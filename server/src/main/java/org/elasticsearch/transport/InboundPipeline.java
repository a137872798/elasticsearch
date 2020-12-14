/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * 以管道模式消费数据
 * 这套模型感觉像是netty的 channelInboundHandler链
 * 每个pipeline 都绑定一个channel 对象 并在单线程环境处理接受到的消息
 */
public class InboundPipeline implements Releasable {

    private static final ThreadLocal<ArrayList<Object>> fragmentList = ThreadLocal.withInitial(ArrayList::new);
    private static final InboundMessage PING_MESSAGE = new InboundMessage(null, true);

    private final LongSupplier relativeTimeInMillis;

    /**
     * 统计写入/读取的数据量
     */
    private final StatsTracker statsTracker;
    /**
     * 该对象的职能就是解析消息
     */
    private final InboundDecoder decoder;
    /**
     * 该对象通过粘包将数据恢复
     */
    private final InboundAggregator aggregator;

    /**
     * param1 代表从哪条channel接受到消息  param2 代表接受到的消息本身
     * 实际上就是 Transport::inboundMessage
     */
    private final BiConsumer<TcpChannel, InboundMessage> messageHandler;
    private Exception uncaughtException;
    private ArrayDeque<ReleasableBytesReference> pending = new ArrayDeque<>(2);
    private boolean isClosed = false;

    /**
     * 当接受到一条新的消息时会通过pipeline进行处理  就跟netty的contextPipeline一样
     * @param version
     * @param statsTracker
     * @param recycler 通过该对象反复创建数组可以节省开销 利用了对象池技术
     * @param relativeTimeInMillis
     * @param circuitBreaker  该对象负责获取熔断器
     * @param registryFunction  通过actionName 可以获取到对应的请求处理器
     * @param messageHandler   定义了如何处理消息的逻辑
     */
    public InboundPipeline(Version version, StatsTracker statsTracker, PageCacheRecycler recycler, LongSupplier relativeTimeInMillis,
                           Supplier<CircuitBreaker> circuitBreaker,
                           Function<String, RequestHandlerRegistry<TransportRequest>> registryFunction,
                           BiConsumer<TcpChannel, InboundMessage> messageHandler) {
        this(statsTracker, relativeTimeInMillis, new InboundDecoder(version, recycler),
            new InboundAggregator(circuitBreaker, registryFunction), messageHandler);
    }

    public InboundPipeline(StatsTracker statsTracker, LongSupplier relativeTimeInMillis, InboundDecoder decoder,
                           InboundAggregator aggregator, BiConsumer<TcpChannel, InboundMessage> messageHandler) {
        this.relativeTimeInMillis = relativeTimeInMillis;
        this.statsTracker = statsTracker;
        this.decoder = decoder;
        this.aggregator = aggregator;
        this.messageHandler = messageHandler;
    }

    @Override
    public void close() {
        isClosed = true;
        Releasables.closeWhileHandlingException(decoder, aggregator);
        Releasables.closeWhileHandlingException(pending);
        pending.clear();
    }

    public void handleBytes(TcpChannel channel, ReleasableBytesReference reference) throws IOException {
        if (uncaughtException != null) {
            throw new IllegalStateException("Pipeline state corrupted by uncaught exception", uncaughtException);
        }
        try {
            doHandleBytes(channel, reference);
        } catch (Exception e) {
            uncaughtException = e;
            throw e;
        }
    }

    /**
     * 当某个channel 接收到某条消息时 进行处理     原本每次发送一个请求应该是对应一个消息 但是可能会发生拆包粘包 所以接收端就会存在一些pending对象  需要等待下次收到的数据一起处理
     * @param channel
     * @param reference
     * @throws IOException
     */
    public void doHandleBytes(TcpChannel channel, ReleasableBytesReference reference) throws IOException {
        // 更新该channel 最后收到消息的时间戳
        channel.getChannelStats().markAccessed(relativeTimeInMillis.getAsLong());
        // 增加收到的消息总长度
        statsTracker.markBytesRead(reference.length());
        // 消息先存储到 pending中
        pending.add(reference.retain());

        final ArrayList<Object> fragments = fragmentList.get();
        boolean continueHandling = true;

        while (continueHandling && isClosed == false) {
            boolean continueDecoding = true;
            // 将pending中所有消息 按照 header/body 拆分后设置到fragmentList中
            while (continueDecoding && pending.isEmpty() == false) {

                try (ReleasableBytesReference toDecode = getPendingBytes()) {
                    // 将合并后的数据进行解码  并将结果存储到 fragments中
                    final int bytesDecoded = decoder.decode(toDecode, fragments::add);
                    if (bytesDecoded != 0) {
                        // 代表成功解析数据后  将pending 移除
                        releasePendingBytes(bytesDecoded);
                        if (fragments.isEmpty() == false && endOfMessage(fragments.get(fragments.size() - 1))) {
                            continueDecoding = false;
                        }
                    } else {
                        // 本次无法获取任何数据 代表发生了拆包 需要等待下次收到新的数据后再处理
                        continueDecoding = false;
                    }
                }
            }

            // 进入这里时消息体已经完成了拆解 和解压
            if (fragments.isEmpty()) {
                // 代表没有数据需要处理了
                continueHandling = false;
            } else {
                try {
                    // 进一步处理所有数据
                    forwardFragments(channel, fragments);
                } finally {
                    for (Object fragment : fragments) {
                        if (fragment instanceof ReleasableBytesReference) {
                            ((ReleasableBytesReference) fragment).close();
                        }
                    }
                    fragments.clear();
                }
            }
        }
    }

    /**
     * 处理拆解出的各部分消息
     * @param channel
     * @param fragments
     * @throws IOException
     */
    private void forwardFragments(TcpChannel channel, ArrayList<Object> fragments) throws IOException {
        for (Object fragment : fragments) {
            // 处理消息头
            if (fragment instanceof Header) {
                assert aggregator.isAggregating() == false;
                aggregator.headerReceived((Header) fragment);
            // 处理心跳包
            } else if (fragment == InboundDecoder.PING) {
                assert aggregator.isAggregating() == false;
                messageHandler.accept(channel, PING_MESSAGE);
            // 代表已经解析到某条消息的末尾   将拼接完成后的消息交由 messageHandler 处理
            } else if (fragment == InboundDecoder.END_CONTENT) {
                assert aggregator.isAggregating();
                try (InboundMessage aggregated = aggregator.finishAggregation()) {
                    statsTracker.markMessageReceived();
                    messageHandler.accept(channel, aggregated);
                }
            // 处理body
            } else {
                assert aggregator.isAggregating();
                assert fragment instanceof ReleasableBytesReference;
                aggregator.aggregate((ReleasableBytesReference) fragment);
            }
        }
    }

    /**
     * 检测是否解析完所有消息
     * @param fragment
     * @return
     */
    private boolean endOfMessage(Object fragment) {
        return fragment == InboundDecoder.PING || fragment == InboundDecoder.END_CONTENT || fragment instanceof Exception;
    }

    /**
     * 获取之前待处理的所有 ref 对象  这里取出数据后并没有从pending中移除
     * @return
     */
    private ReleasableBytesReference getPendingBytes() {
        if (pending.size() == 1) {
            return pending.peekFirst().retain();
        } else {
            final ReleasableBytesReference[] bytesReferences = new ReleasableBytesReference[pending.size()];
            int index = 0;
            for (ReleasableBytesReference pendingReference : pending) {
                bytesReferences[index] = pendingReference.retain();
                ++index;
            }
            final Releasable releasable = () -> Releasables.closeWhileHandlingException(bytesReferences);
            return new ReleasableBytesReference(new CompositeBytesReference(bytesReferences), releasable);
        }
    }

    /**
     * 根据解析的大小 移除合适数量的pending
     * @param bytesConsumed
     */
    private void releasePendingBytes(int bytesConsumed) {
        int bytesToRelease = bytesConsumed;
        while (bytesToRelease != 0) {
            try (ReleasableBytesReference reference = pending.pollFirst()) {
                assert reference != null;
                if (bytesToRelease < reference.length()) {
                    pending.addFirst(reference.retainedSlice(bytesToRelease, reference.length() - bytesToRelease));
                    bytesToRelease -= bytesToRelease;
                } else {
                    bytesToRelease -= reference.length();
                }
            }
        }
    }
}
