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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * 这个是解决拆包粘包问题的
 */
public class InboundAggregator implements Releasable {

    /**
     * 通过该函数获取熔断器
     */
    private final Supplier<CircuitBreaker> circuitBreaker;
    private final Predicate<String> requestCanTripBreaker;

    private ReleasableBytesReference firstContent;
    private ArrayList<ReleasableBytesReference> contentAggregation;

    /**
     * 当前处理的消息头
     */
    private Header currentHeader;

    /**
     * 代表在聚合期间产生的异常
     */
    private Exception aggregationException;
    /**
     * 代表本次处理的消息 可能会触发熔断
     */
    private boolean canTripBreaker = true;
    private boolean isClosed = false;

    public InboundAggregator(Supplier<CircuitBreaker> circuitBreaker,
                             Function<String, RequestHandlerRegistry<TransportRequest>> registryFunction) {
        this(circuitBreaker, (Predicate<String>) actionName -> {
            // 先根据action 获取对应的请求处理器
            final RequestHandlerRegistry<TransportRequest> reg = registryFunction.apply(actionName);
            if (reg == null) {
                throw new ActionNotFoundTransportException(actionName);
            } else {
                // 检测该处理器是否会触发熔断  某些action 可能不会触发熔断逻辑
                return reg.canTripCircuitBreaker();
            }
        });
    }

    // Visible for testing
    InboundAggregator(Supplier<CircuitBreaker> circuitBreaker, Predicate<String> requestCanTripBreaker) {
        this.circuitBreaker = circuitBreaker;
        this.requestCanTripBreaker = requestCanTripBreaker;
    }

    public void headerReceived(Header header) {
        ensureOpen();
        assert isAggregating() == false;
        assert firstContent == null && contentAggregation == null;
        currentHeader = header;
        // 当本次接受到一个 req 且不需要读取变长请求头 触发state的初始化
        if (currentHeader.isRequest() && currentHeader.needsToReadVariableHeader() == false) {
            initializeRequestState();
        }
    }

    /**
     * 将某个消息聚合到当前对象
     * @param content
     */
    public void aggregate(ReleasableBytesReference content) {
        ensureOpen();
        assert isAggregating();
        // 代表之前已经出现了异常 也就是发生了短路
        if (isShortCircuited() == false) {
            // 设置第一个消息体
            if (isFirstContent()) {
                firstContent = content.retain();
            } else {
                // 代表由多个消息合并
                if (contentAggregation == null) {
                    contentAggregation = new ArrayList<>(4);
                    assert firstContent != null;

                    contentAggregation.add(firstContent);
                    firstContent = null;
                }
                contentAggregation.add(content.retain());
            }
        }
    }

    /**
     * 当所有消息收集成功时 进行聚合 不过比起在接收端进行聚合 肯定不如在发送时进行聚合 比如kafka
     * @return
     * @throws IOException
     */
    public InboundMessage finishAggregation() throws IOException {
        ensureOpen();
        final ReleasableBytesReference releasableContent;
        // 代表还未采集到任何消息  生成一个空消息体
        if (isFirstContent()) {
            releasableContent = ReleasableBytesReference.wrap(BytesArray.EMPTY);
        } else if (contentAggregation == null) {
            releasableContent = firstContent;
        } else {
            // 将消息合并
            final ReleasableBytesReference[] references = contentAggregation.toArray(new ReleasableBytesReference[0]);
            final CompositeBytesReference content = new CompositeBytesReference(references);
            releasableContent = new ReleasableBytesReference(content, () -> Releasables.close(references));
        }

        final BreakerControl breakerControl = new BreakerControl(circuitBreaker);
        final InboundMessage aggregated = new InboundMessage(currentHeader, releasableContent, breakerControl);
        boolean success = false;
        try {
            // 读取一些额外数据
            if (aggregated.getHeader().needsToReadVariableHeader()) {
                aggregated.getHeader().finishParsingHeader(aggregated.openOrGetStreamInput());
                if (aggregated.getHeader().isRequest()) {
                    initializeRequestState();
                }
            }
            if (isShortCircuited() == false) {
                // 检验是否触发了熔断
                checkBreaker(aggregated.getHeader(), aggregated.getContentLength(), breakerControl);
            }
            // 代表已经发生了熔断 拒绝处理该请求
            if (isShortCircuited()) {
                // 这里会触发 BreakerControl.close 释放之前使用的bytes
                aggregated.close();
                success = true;
                // 返回一个携带异常的消息 代表处理失败
                return new InboundMessage(aggregated.getHeader(), aggregationException);
            } else {
                success = true;
                return aggregated;
            }
        } finally {
            // 每当处理完一个对象后 重置内部属性  那不就是一个对象池么   这个对象本身要求应该是在单线程中处理
            resetCurrentAggregation();
            if (success == false) {
                aggregated.close();
            }
        }
    }

    public boolean isAggregating() {
        return currentHeader != null;
    }

    private void shortCircuit(Exception exception) {
        this.aggregationException = exception;
    }

    private boolean isShortCircuited() {
        return aggregationException != null;
    }

    private boolean isFirstContent() {
        return firstContent == null && contentAggregation == null;
    }

    @Override
    public void close() {
        isClosed = true;
        closeCurrentAggregation();
    }

    private void closeCurrentAggregation() {
        releaseContent();
        resetCurrentAggregation();
    }

    private void releaseContent() {
        if (contentAggregation == null) {
            Releasables.close(firstContent);
        } else {
            Releasables.close(contentAggregation);
        }
    }

    private void resetCurrentAggregation() {
        firstContent = null;
        contentAggregation = null;
        currentHeader = null;
        aggregationException = null;
        canTripBreaker = true;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Aggregator is already closed");
        }
    }

    /**
     * 初始化请求状态
     */
    private void initializeRequestState() {
        assert currentHeader.needsToReadVariableHeader() == false;
        assert currentHeader.isRequest();
        // 忽略握手请求
        if (currentHeader.isHandshake()) {
            canTripBreaker = false;
            return;
        }

        // 检测是否需要触发熔断
        final String actionName = currentHeader.getActionName();
        try {
            canTripBreaker = requestCanTripBreaker.test(actionName);
        } catch (ActionNotFoundTransportException e) {
            shortCircuit(e);
        }
    }

    /**
     * 检验是否触发了熔断 触发条件大概就是当前节点上同时处理的数据流过多
     * @param header  本次消息对应的请求头
     * @param contentLength  本次消息对应要处理的数据流
     * @param breakerControl
     */
    private void checkBreaker(final Header header, final int contentLength, final BreakerControl breakerControl) {
        if (header.isRequest() == false) {
            return;
        }
        assert header.needsToReadVariableHeader() == false;

        if (canTripBreaker) {
            try {
                // 增加可能会触发熔断的 bytes
                circuitBreaker.get().addEstimateBytesAndMaybeBreak(contentLength, header.getActionName());
                breakerControl.setReservedBytes(contentLength);
            } catch (CircuitBreakingException e) {
                shortCircuit(e);
            }
        } else {
            // 增加此时要处理的数据  但是此时不检测是否熔断 也就是处理这类请求时永远不会触发熔断
            circuitBreaker.get().addWithoutBreaking(contentLength);
            breakerControl.setReservedBytes(contentLength);
        }
    }

    /**
     * 该对象就是修改熔断器的  在处理消息时设置此时使用的bytes 当使用完毕时 释放熔断器记录的数据
     */
    private static class BreakerControl implements Releasable {

        private static final int CLOSED = -1;

        private final Supplier<CircuitBreaker> circuitBreaker;
        private final AtomicInteger bytesToRelease = new AtomicInteger(0);

        private BreakerControl(Supplier<CircuitBreaker> circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }

        private void setReservedBytes(int reservedBytes) {
            final boolean set = bytesToRelease.compareAndSet(0, reservedBytes);
            assert set : "Expected bytesToRelease to be 0, found " + bytesToRelease.get();
        }

        @Override
        public void close() {
            final int toRelease = bytesToRelease.getAndSet(CLOSED);
            assert toRelease != CLOSED;
            if (toRelease > 0) {
                circuitBreaker.get().addWithoutBreaking(-toRelease);
            }
        }
    }
}
