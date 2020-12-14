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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NotifyOnceListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * 定义了将数据发送到 channel的逻辑
 */
final class OutboundHandler {

    private static final Logger logger = LogManager.getLogger(OutboundHandler.class);

    private final String nodeName;
    private final Version version;
    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    /**
     *
     * @param nodeName   对应当前节点的名字
     * @param version     节点的版本号
     * @param statsTracker
     * @param threadPool
     * @param bigArrays
     */
    OutboundHandler(String nodeName, Version version, StatsTracker statsTracker, ThreadPool threadPool, BigArrays bigArrays) {
        this.nodeName = nodeName;
        this.version = version;
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
    }

    /**
     * 往某个channel 发送bytes 同时使用listener监听结果
     * @param channel
     * @param bytes
     * @param listener
     */
    void sendBytes(TcpChannel channel, BytesReference bytes, ActionListener<Void> listener) {
        // 将相关参数封装成一个上下文对象
        SendContext sendContext = new SendContext(channel, () -> bytes, listener);
        try {
            internalSend(channel, sendContext);
        } catch (IOException e) {
            // This should not happen as the bytes are already serialized
            throw new AssertionError(e);
        }
    }

    /**
     * Sends the request to the given channel. This method should be used to send {@link TransportRequest}
     * objects back to the caller.
     * 往某个指定的节点发送消息
     * @param isHandshake  本次发送的请求是否是握手请求
     */
    void sendRequest(final DiscoveryNode node, final TcpChannel channel, final long requestId, final String action,
                     final TransportRequest request, final TransportRequestOptions options, final Version channelVersion,
                     final boolean compressRequest, final boolean isHandshake) throws IOException, TransportException {
        Version version = Version.min(this.version, channelVersion);
        // 根据相关信息生成 message对象
        OutboundMessage.Request message =
            new OutboundMessage.Request(threadPool.getThreadContext(), request, version, action, requestId, isHandshake, compressRequest);
        // 将messageListener 包装成监听器
        ActionListener<Void> listener = ActionListener.wrap(() ->
            messageListener.onRequestSent(node, requestId, action, request, options));
        // 使用channel 发送消息
        sendMessage(channel, message, listener);
    }

    /**
     * Sends the response to the given channel. This method should be used to send {@link TransportResponse}
     * objects back to the caller.
     *
     * @see #sendErrorResponse(Version, TcpChannel, long, String, Exception) for sending error responses
     */
    void sendResponse(final Version nodeVersion, final TcpChannel channel, final long requestId, final String action,
                      final TransportResponse response, final boolean compress, final boolean isHandshake) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        OutboundMessage.Response message = new OutboundMessage.Response(threadPool.getThreadContext(), response, version,
            requestId, isHandshake, compress);
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onResponseSent(requestId, action, response));
        sendMessage(channel, message, listener);
    }

    /**
     * Sends back an error response to the caller via the given channel
     * 发送一个异常对象到对端
     */
    void sendErrorResponse(final Version nodeVersion, final TcpChannel channel, final long requestId, final String action,
                           final Exception error) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        TransportAddress address = new TransportAddress(channel.getLocalAddress());
        RemoteTransportException tx = new RemoteTransportException(nodeName, address, action, error);
        OutboundMessage.Response message = new OutboundMessage.Response(threadPool.getThreadContext(), tx, version, requestId,
            false, false);
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onResponseSent(requestId, action, error));
        sendMessage(channel, message, listener);
    }

    /**
     * 将消息经过特殊处理后发送
     * @param channel
     * @param networkMessage
     * @param listener
     * @throws IOException
     */
    private void sendMessage(TcpChannel channel, OutboundMessage networkMessage, ActionListener<Void> listener) throws IOException {
        MessageSerializer serializer = new MessageSerializer(networkMessage, bigArrays);
        SendContext sendContext = new SendContext(channel, serializer, listener, serializer);
        internalSend(channel, sendContext);
    }

    /**
     * 将数据通过channel 发送
     * @param channel
     * @param sendContext
     * @throws IOException
     */
    private void internalSend(TcpChannel channel, SendContext sendContext) throws IOException {
        // 更新该channel的最后活跃时间 这样在心跳任务对象中就可以节省部分开销
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        // 获取要发送的数据流
        BytesReference reference = sendContext.get();
        try {
            channel.sendMessage(reference, sendContext);
        } catch (RuntimeException ex) {
            sendContext.onFailure(ex);
            CloseableChannel.closeChannel(channel);
            throw ex;
        }

    }

    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    /**
     * 该对象负责将消息序列化
     */
    private static class MessageSerializer implements CheckedSupplier<BytesReference, IOException>, Releasable {

        private final OutboundMessage message;
        private final BigArrays bigArrays;
        private volatile ReleasableBytesStreamOutput bytesStreamOutput;

        private MessageSerializer(OutboundMessage message, BigArrays bigArrays) {
            this.message = message;
            this.bigArrays = bigArrays;
        }

        @Override
        public BytesReference get() throws IOException {
            bytesStreamOutput = new ReleasableBytesStreamOutput(bigArrays);
            return message.serialize(bytesStreamOutput);
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(bytesStreamOutput);
        }
    }

    /**
     * 在某次发送操作中维护了各种需要的信息
     */
    private class SendContext extends NotifyOnceListener<Void> implements CheckedSupplier<BytesReference, IOException> {

        /**
         *  本次发送数据的channel
         */
        private final TcpChannel channel;
        /**
         * 获取待发送的数据
         */
        private final CheckedSupplier<BytesReference, IOException> messageSupplier;
        /**
         * 发送结果由该监听器处理
         */
        private final ActionListener<Void> listener;
        private final Releasable optionalReleasable;
        private long messageSize = -1;

        private SendContext(TcpChannel channel, CheckedSupplier<BytesReference, IOException> messageSupplier,
                            ActionListener<Void> listener) {
            this(channel, messageSupplier, listener, null);
        }

        private SendContext(TcpChannel channel, CheckedSupplier<BytesReference, IOException> messageSupplier,
                            ActionListener<Void> listener, Releasable optionalReleasable) {
            this.channel = channel;
            this.messageSupplier = messageSupplier;
            this.listener = listener;
            this.optionalReleasable = optionalReleasable;
        }

        /**
         * 获取需要发送的数据
         * @return
         * @throws IOException
         */
        public BytesReference get() throws IOException {
            BytesReference message;
            try {
                message = messageSupplier.get();
                messageSize = message.length();
                TransportLogger.logOutboundMessage(channel, message);
                return message;
            } catch (Exception e) {
                onFailure(e);
                throw e;
            }
        }

        // 当产生结果时触发下面的函数

        @Override
        protected void innerOnResponse(Void v) {
            assert messageSize != -1 : "If onResponse is being called, the message should have been serialized";
            statsTracker.markBytesWritten(messageSize);
            // 转发到创建该对象时的listener
            closeAndCallback(() -> listener.onResponse(v));
        }

        @Override
        protected void innerOnFailure(Exception e) {
            if (NetworkExceptionHelper.isCloseConnectionException(e)) {
                logger.debug(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
            } else {
                logger.warn(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
            }
            closeAndCallback(() -> listener.onFailure(e));
        }

        private void closeAndCallback(Runnable runnable) {
            Releasables.close(optionalReleasable, runnable::run);
        }
    }
}
