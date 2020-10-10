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

package org.elasticsearch.transport.nio;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.nio.BytesWriteHandler;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.Page;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.InboundPipeline;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * 该对象定义了消费channel中读取的数据 以及会针对即将写入channel的数据做转换 (WriteOp -> FlushOp)
 */
public class TcpReadWriteHandler extends BytesWriteHandler {

    /**
     * 关联的channel
     */
    private final NioTcpChannel channel;

    /**
     * 基于管道模式消费数据
     */
    private final InboundPipeline pipeline;

    /**
     *
     * @param channel
     * @param recycler 对象池 就是重复利用各种数组对象
     * @param transport
     */
    public TcpReadWriteHandler(NioTcpChannel channel, PageCacheRecycler recycler, TcpTransport transport) {
        this.channel = channel;
        // 从传输层对象获取各种参数
        final ThreadPool threadPool = transport.getThreadPool();
        final Supplier<CircuitBreaker> breaker = transport.getInflightBreaker();
        // 存储了所有请求处理器
        final Transport.RequestHandlers requestHandlers = transport.getRequestHandlers();
        this.pipeline = new InboundPipeline(transport.getVersion(), transport.getStatsTracker(), recycler, threadPool::relativeTimeInMillis,
            breaker, requestHandlers::getHandler, transport::inboundMessage);
    }

    /**
     * 当准备好read事件时 这里读取数据块 并在上层通过 pipeline将数据块转换成可以被处理的消息
     * @param channelBuffer of bytes read from the network
     * @return
     * @throws IOException
     */
    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
        Page[] pages = channelBuffer.sliceAndRetainPagesTo(channelBuffer.getIndex());
        BytesReference[] references = new BytesReference[pages.length];
        for (int i = 0; i < pages.length; ++i) {
            references[i] = BytesReference.fromByteBuffer(pages[i].byteBuffer());
        }
        Releasable releasable = () -> IOUtils.closeWhileHandlingException(pages);
        try (ReleasableBytesReference reference = new ReleasableBytesReference(new CompositeBytesReference(references), releasable)) {
            // 在这里会处理完请求
            pipeline.handleBytes(channel, reference);
            return reference.length();
        }
    }

    @Override
    public void close() {
        Releasables.closeWhileHandlingException(pipeline);
        super.close();
    }
}
