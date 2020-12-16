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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 这里大体上就是使用 resHandler/ reqHandler 处理 InboundMessage
 */
public class InboundHandler {

    private static final Logger logger = LogManager.getLogger(InboundHandler.class);

    private final ThreadPool threadPool;
    /**
     * 该对象负责将数据通过channel 发送到对端
     */
    private final OutboundHandler outboundHandler;
    /**
     * 该对象维护了 class 与 reader的映射关系
     */
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TransportHandshaker handshaker;
    /**
     * 这里也包含维护心跳的对象
     */
    private final TransportKeepAlive keepAlive;
    /**
     * 类似于命令池 内部维护的res都是针对某个请求的 当发送成功时 将res从命令池中移除
     */
    private final Transport.ResponseHandlers responseHandlers;
    /**
     * 维护了处理不同action的req的 handler
     */
    private final Transport.RequestHandlers requestHandlers;

    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    InboundHandler(ThreadPool threadPool, OutboundHandler outboundHandler, NamedWriteableRegistry namedWriteableRegistry,
                   TransportHandshaker handshaker, TransportKeepAlive keepAlive, Transport.RequestHandlers requestHandlers,
                   Transport.ResponseHandlers responseHandlers) {
        this.threadPool = threadPool;
        this.outboundHandler = outboundHandler;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.handshaker = handshaker;
        this.keepAlive = keepAlive;
        this.requestHandlers = requestHandlers;
        this.responseHandlers = responseHandlers;
    }

    /**
     * 仅允许设置一次
     * @param listener
     */
    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    /**
     * 接收到一个新消息 并进行处理
     * @param channel
     * @param message
     * @throws Exception
     */
    void inboundMessage(TcpChannel channel, InboundMessage message) throws Exception {
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        TransportLogger.logInboundMessage(channel, message);

        if (message.isPing()) {
            // 当服务端接收到心跳包时 会返回一个响应结果
            keepAlive.receiveKeepAlive(channel);
        } else {
            messageReceived(channel, message);
        }
    }

    /**
     * 代表接收到一条具备业务含义的消息
     * @param channel
     * @param message
     * @throws IOException
     */
    private void messageReceived(TcpChannel channel, InboundMessage message) throws IOException {
        final InetSocketAddress remoteAddress = channel.getRemoteAddress();
        final Header header = message.getHeader();
        assert header.needsToReadVariableHeader() == false;

        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
            // Place the context with the headers from the message
            // 将相关信息填充到上下文对象中  该上下文在同一线程中可以传递
            threadContext.setHeaders(header.getHeaders());
            threadContext.putTransient("_remote_address", remoteAddress);
            // 从消息头的 status中判断本次消息是 req/res
            if (header.isRequest()) {
                handleRequest(channel, header, message);
            } else {
                // 代表接收到一个响应结果   响应结果的处理本身无法触发熔断
                // Responses do not support short circuiting currently
                assert message.isShortCircuit() == false;
                final StreamInput streamInput = namedWriteableStream(message.openOrGetStreamInput());
                assertRemoteVersion(streamInput, header.getVersion());
                final TransportResponseHandler<?> handler;
                long requestId = header.getRequestId();
                // 当接收到一个握手的响应结果时 从命令池中移除等待的请求 并处理
                if (header.isHandshake()) {
                    handler = handshaker.removeHandlerForHandshake(requestId);
                } else {
                    // 返回res的处理器对象
                    TransportResponseHandler<? extends TransportResponse> theHandler =
                        responseHandlers.onResponseReceived(requestId, messageListener);
                    if (theHandler == null && header.isError()) {
                        handler = handshaker.removeHandlerForHandshake(requestId);
                    } else {
                        handler = theHandler;
                    }
                }
                // ignore if its null, the service logs it
                // 使用处理器处理res
                if (handler != null) {
                    if (header.isError()) {
                        handlerResponseError(streamInput, handler);
                    } else {
                        handleResponse(remoteAddress, streamInput, handler);
                    }
                    // Check the entire message has been read
                    final int nextByte = streamInput.read();
                    // calling read() is useful to make sure the message is fully read, even if there is an EOS marker
                    if (nextByte != -1) {
                        throw new IllegalStateException("Message not fully read (response) for requestId [" + requestId + "], handler ["
                            + handler + "], error [" + header.isError() + "]; resetting");
                    }
                }
            }
        }
    }

    /**
     * 处理一个接收到的 req 对象
     * @param channel
     * @param header
     * @param message
     * @param <T>
     * @throws IOException
     */
    private <T extends TransportRequest> void handleRequest(TcpChannel channel, Header header, InboundMessage message) throws IOException {
        final String action = header.getActionName();
        final long requestId = header.getRequestId();
        final Version version = header.getVersion();
        // 代表接收到的是一个握手请求 也就是刚连接到某个节点时会发送探测请求
        if (header.isHandshake()) {
            messageListener.onRequestReceived(requestId, action);
            // Cannot short circuit handshakes
            assert message.isShortCircuit() == false;
            final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
            assertRemoteVersion(stream, header.getVersion());
            final TransportChannel transportChannel = new TcpTransportChannel(outboundHandler, channel, action, requestId, version,
                header.isCompressed(), header.isHandshake(), message.takeBreakerReleaseControl());
            try {
                handshaker.handleHandshake(transportChannel, requestId, stream);
            } catch (Exception e) {
                sendErrorResponse(action, transportChannel, e);
            }
        } else {
            // TransportChannel 相比TcpChannel api精确到了 req/res的级别 贴近业务层面  该对象仅包含发送res的api
            final TransportChannel transportChannel = new TcpTransportChannel(outboundHandler, channel, action, requestId, version,
                header.isCompressed(), header.isHandshake(), message.takeBreakerReleaseControl());
            try {
                // 触发监听器 TransportService就是一个messageListener 需要设置handleIncomingRequests 标识才能正常处理请求 代表本节点此时完成了基础的信息初始化
                messageListener.onRequestReceived(requestId, action);
                // 该消息在处理前由于触发了熔断 不需要处理
                if (message.isShortCircuit()) {
                    // 将异常通过 channel发出
                    sendErrorResponse(action, transportChannel, message.getException());
                } else {
                    // 生成消息体包含的数据流
                    final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
                    assertRemoteVersion(stream, header.getVersion());
                    // 获取对应的handler 对象
                    final RequestHandlerRegistry<T> reg = requestHandlers.getHandler(action);
                    assert reg != null;
                    final T request = reg.newRequest(stream);
                    // 设置req的远端地址
                    request.remoteAddress(new TransportAddress(channel.getRemoteAddress()));
                    // in case we throw an exception, i.e. when the limit is hit, we don't want to verify
                    final int nextByte = stream.read();
                    // calling read() is useful to make sure the message is fully read, even if there some kind of EOS marker
                    if (nextByte != -1) {
                        throw new IllegalStateException("Message not fully read (request) for requestId [" + requestId + "], action ["
                            + action + "], available [" + stream.available() + "]; resetting");
                    }
                    // 使用每个action对应的线程池处理请求
                    // RequestHandler 定义了一套模板 将处理后的结果 通过channel发送
                    threadPool.executor(reg.getExecutor()).execute(new RequestHandler<>(reg, request, transportChannel));
                }
            } catch (Exception e) {
                sendErrorResponse(action, transportChannel, e);
            }

        }
    }

    /**
     * 将异常返回到对端
     * @param actionName
     * @param transportChannel
     * @param e
     */
    private static void sendErrorResponse(String actionName, TransportChannel transportChannel, Exception e) {
        try {
            transportChannel.sendResponse(e);
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.warn(() -> new ParameterizedMessage("Failed to send error message back to client for action [{}]", actionName), inner);
        }
    }

    /**
     * 使用指定的处理器处理 res
     * @param remoteAddress
     * @param stream
     * @param handler
     * @param <T>
     */
    private <T extends TransportResponse> void handleResponse(InetSocketAddress remoteAddress, final StreamInput stream,
                                                              final TransportResponseHandler<T> handler) {
        final T response;
        try {
            // 将数据流还原成 res对象 并设置远端地址
            response = handler.read(stream);
            response.remoteAddress(new TransportAddress(remoteAddress));
        } catch (Exception e) {
            handleException(handler, new TransportSerializationException(
                "Failed to deserialize response from handler [" + handler.getClass().getName() + "]", e));
            return;
        }
        // 交由线程池处理res
        threadPool.executor(handler.executor()).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                handleException(handler, new ResponseHandlerFailureTransportException(e));
            }

            @Override
            protected void doRun() {
                handler.handleResponse(response);
            }
        });
    }

    private void handlerResponseError(StreamInput stream, final TransportResponseHandler<?> handler) {
        Exception error;
        try {
            error = stream.readException();
        } catch (Exception e) {
            error = new TransportSerializationException("Failed to deserialize exception response from stream", e);
        }
        handleException(handler, error);
    }

    private void handleException(final TransportResponseHandler<?> handler, Throwable error) {
        if (!(error instanceof RemoteTransportException)) {
            error = new RemoteTransportException(error.getMessage(), error);
        }
        final RemoteTransportException rtx = (RemoteTransportException) error;
        threadPool.executor(handler.executor()).execute(() -> {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("failed to handle exception response [{}]", handler), e);
            }
        });
    }

    /**
     * 将输入流 与 reader注册器 包装成一个对象
     * @param delegate
     * @return
     */
    private StreamInput namedWriteableStream(StreamInput delegate) {
        return new NamedWriteableAwareStreamInput(delegate, namedWriteableRegistry);
    }

    static void assertRemoteVersion(StreamInput in, Version version) {
        assert version.equals(in.getVersion()) : "Stream version [" + in.getVersion() + "] does not match version [" + version + "]";
    }

    /**
     * 该对象定义了一个处理请求并通过 channel发送结果的模板
     * @param <T>
     */
    private static class RequestHandler<T extends TransportRequest> extends AbstractRunnable {
        private final RequestHandlerRegistry<T> reg;
        private final T request;
        private final TransportChannel transportChannel;

        RequestHandler(RequestHandlerRegistry<T> reg, T request, TransportChannel transportChannel) {
            this.reg = reg;
            this.request = request;
            this.transportChannel = transportChannel;
        }

        @Override
        protected void doRun() throws Exception {
            reg.processMessageReceived(request, transportChannel);
        }

        @Override
        public boolean isForceExecution() {
            return reg.isForceExecution();
        }

        @Override
        public void onFailure(Exception e) {
            sendErrorResponse(reg.getAction(), transportChannel, e);
        }
    }
}
