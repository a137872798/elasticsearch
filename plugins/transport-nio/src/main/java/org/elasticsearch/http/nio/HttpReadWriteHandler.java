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

package org.elasticsearch.http.nio;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.http.CorsHandler;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.HttpPipelinedRequest;
import org.elasticsearch.http.HttpReadTimeoutException;
import org.elasticsearch.http.nio.cors.NioCorsHandler;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioChannelHandler;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.TaskScheduler;
import org.elasticsearch.nio.WriteOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * http层处理读请求与TCP层不一致
 */
public class HttpReadWriteHandler implements NioChannelHandler {

    private final NettyAdaptor adaptor;
    private final NioHttpChannel nioHttpChannel;
    private final NioHttpServerTransport transport;
    private final TaskScheduler taskScheduler;
    private final LongSupplier nanoClock;
    private final long readTimeoutNanos;
    private boolean channelActive = false;
    private boolean requestSinceReadTimeoutTrigger = false;
    private int inFlightRequests = 0;


    /**
     *
     * @param nioHttpChannel  httpChannel
     * @param transport  使用的是http的传输层对象
     * @param settings
     * @param corsConfig  TODO 先忽略跨域相关的
     * @param taskScheduler  就是一个可以执行定时任务的对象
     * @param nanoClock  获取当前时间
     */
    public HttpReadWriteHandler(NioHttpChannel nioHttpChannel, NioHttpServerTransport transport, HttpHandlingSettings settings,
                                CorsHandler.Config corsConfig, TaskScheduler taskScheduler, LongSupplier nanoClock) {
        this.nioHttpChannel = nioHttpChannel;
        this.transport = transport;
        this.taskScheduler = taskScheduler;
        this.nanoClock = nanoClock;
        this.readTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(settings.getReadTimeoutMillis());

        List<ChannelHandler> handlers = new ArrayList<>(5);
        // 在这里将数据流解析成http 就不细看了
        HttpRequestDecoder decoder = new HttpRequestDecoder(settings.getMaxInitialLineLength(), settings.getMaxHeaderSize(),
            settings.getMaxChunkSize());
        decoder.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
        handlers.add(decoder);
        handlers.add(new HttpContentDecompressor());
        handlers.add(new HttpResponseEncoder());
        handlers.add(new HttpObjectAggregator(settings.getMaxContentLength()));
        if (settings.isCompression()) {
            handlers.add(new HttpContentCompressor(settings.getCompressionLevel()));
        }
        if (settings.isCorsEnabled()) {
            handlers.add(new NioCorsHandler(corsConfig));
        }
        handlers.add(new NioHttpPipeliningHandler(transport.getLogger(), settings.getPipeliningMaxEvents()));

        adaptor = new NettyAdaptor(handlers.toArray(new ChannelHandler[0]));
        adaptor.addCloseListener((v, e) -> nioHttpChannel.close());
    }

    /**
     * 当channel 被注册到 selector时 就会触发该方法了
     */
    @Override
    public void channelActive() {
        channelActive = true;
        if (readTimeoutNanos > 0) {
            scheduleReadTimeout();
        }
    }

    /**
     * 当收到消息时就是通过该方法进行处理  最终会委托给 dispatcher
     * @param channelBuffer of bytes read from the network
     * @return
     */
    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) {
        assert channelActive : "channelActive should have been called";
        int bytesConsumed = adaptor.read(channelBuffer.sliceAndRetainPagesTo(channelBuffer.getIndex()));
        Object message;
        while ((message = adaptor.pollInboundMessage()) != null) {
            ++inFlightRequests;
            requestSinceReadTimeoutTrigger = true;
            handleRequest(message);
        }

        return bytesConsumed;
    }

    // 写入操作分为3个阶段  第一阶段将消息体包装设置到 selector的列表中
    // 第二阶段 当事件循环发现有未写入的writeOp 会将起设置到 writerHandler的flush队列中   这个flush队列是检测缓冲区是否准备好的 因为写事件没准备完成才会有flushOp残留
    // 第三阶段 才是真正将数据写入到缓冲区
    // 而httpHandler只是重写了前面2步

    @Override
    public WriteOperation createWriteOperation(SocketChannelContext context, Object message, BiConsumer<Void, Exception> listener) {
        assert message instanceof NioHttpResponse : "This channel only supports messages that are of type: "
            + NioHttpResponse.class + ". Found type: " + message.getClass() + ".";
        return new HttpWriteOperation(context, (NioHttpResponse) message, listener);
    }

    @Override
    public List<FlushOperation> writeToBytes(WriteOperation writeOperation) {
        assert writeOperation.getObject() instanceof NioHttpResponse : "This channel only supports messages that are of type: "
            + NioHttpResponse.class + ". Found type: " + writeOperation.getObject().getClass() + ".";
        assert channelActive : "channelActive should have been called";
        --inFlightRequests;
        assert inFlightRequests >= 0 : "Inflight requests should never drop below zero, found: " + inFlightRequests;
        adaptor.write(writeOperation);
        return pollFlushOperations();
    }

    @Override
    public List<FlushOperation> pollFlushOperations() {
        ArrayList<FlushOperation> copiedOperations = new ArrayList<>(adaptor.getOutboundCount());
        FlushOperation flushOperation;
        while ((flushOperation = adaptor.pollOutboundOperation()) != null) {
            copiedOperations.add(flushOperation);
        }
        return copiedOperations;
    }

    @Override
    public boolean closeNow() {
        return false;
    }

    @Override
    public void close() throws IOException {
        try {
            adaptor.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * 先接收外部用户传入的请求信息
     * @param msg
     */
    @SuppressWarnings("unchecked")
    private void handleRequest(Object msg) {
        final HttpPipelinedRequest<FullHttpRequest> pipelinedRequest = (HttpPipelinedRequest<FullHttpRequest>) msg;
        FullHttpRequest request = pipelinedRequest.getRequest();
        boolean success = false;
        NioHttpRequest httpRequest = new NioHttpRequest(request, pipelinedRequest.getSequence());
        try {
            if (request.decoderResult().isFailure()) {
                Throwable cause = request.decoderResult().cause();
                if (cause instanceof Error) {
                    ExceptionsHelper.maybeDieOnAnotherThread(cause);
                    transport.incomingRequestError(httpRequest, nioHttpChannel, new Exception(cause));
                } else {
                    transport.incomingRequestError(httpRequest, nioHttpChannel, (Exception) cause);
                }
            } else {
                // 转发到 httpTransport
                transport.incomingRequest(httpRequest, nioHttpChannel);
            }
            success = true;
        } finally {
            if (success == false) {
                request.release();
            }
        }
    }

    /**
     * 当连接建立后 会开启一个定时任务 检测是否有读取到数据 如果没有读取到数据自动抛出异常
     * 因为在http协议中认为只有要发送数据时才会建立连接
     */
    private void maybeReadTimeout() {
        // 既没有发送数据 也没有收到数据
        if (requestSinceReadTimeoutTrigger == false && inFlightRequests == 0) {
            // 自动断开连接
            transport.onException(nioHttpChannel, new HttpReadTimeoutException(TimeValue.nsecToMSec(readTimeoutNanos)));
        } else {
            requestSinceReadTimeoutTrigger = false;
            scheduleReadTimeout();
        }
    }

    private void scheduleReadTimeout() {
        taskScheduler.scheduleAtRelativeTime(this::maybeReadTimeout, nanoClock.getAsLong() + readTimeoutNanos);
    }
}
