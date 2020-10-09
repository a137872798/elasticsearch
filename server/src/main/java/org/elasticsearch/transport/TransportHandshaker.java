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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sends and receives transport-level connection handshakes. This class will send the initial handshake,
 * manage state/timeouts while the handshake is in transit, and handle the eventual response.
 * 这个握手请求更像是一个探测请求 检测节点能否正常处理
 */
final class TransportHandshaker {

    static final String HANDSHAKE_ACTION_NAME = "internal:tcp/handshake";
    /**
     * 相当于命令池  每个发出的请求都会等待对端返回响应结果
     */
    private final ConcurrentMap<Long, HandshakeResponseHandler> pendingHandshakes = new ConcurrentHashMap<>();
    private final CounterMetric numHandshakes = new CounterMetric();

    private final Version version;
    private final ThreadPool threadPool;
    /**
     * 该对象负责发送握手请求
     */
    private final HandshakeRequestSender handshakeRequestSender;

    /**
     *
     * @param version  当前节点的版本号
     * @param threadPool
     * @param handshakeRequestSender  实际上就是借助outboundHandler 发送消息
     */
    TransportHandshaker(Version version, ThreadPool threadPool, HandshakeRequestSender handshakeRequestSender) {
        this.version = version;
        this.threadPool = threadPool;
        this.handshakeRequestSender = handshakeRequestSender;
    }

    /**
     * 发送一个握手请求到目标节点
     * @param requestId
     * @param node   目标节点
     * @param channel
     * @param timeout
     * @param listener
     */
    void sendHandshake(long requestId, DiscoveryNode node, TcpChannel channel, TimeValue timeout, ActionListener<Version> listener) {
        numHandshakes.inc();
        // 生成对应的 响应结果处理器
        final HandshakeResponseHandler handler = new HandshakeResponseHandler(requestId, version, listener);
        pendingHandshakes.put(requestId, handler);
        // 设置关闭监听器
        channel.addCloseListener(ActionListener.wrap(
            () -> handler.handleLocalException(new TransportException("handshake failed because connection reset"))));
        boolean success = false;
        try {
            // for the request we use the minCompatVersion since we don't know what's the version of the node we talk to
            // we also have no payload on the request but the response will contain the actual version of the node we talk
            // to as the payload.
            // 获取兼容的最小版本号
            final Version minCompatVersion = version.minimumCompatibilityVersion();
            // version 将被包装成 handshakeReq对象 并被发往对端
            handshakeRequestSender.sendRequest(node, channel, requestId, minCompatVersion);

            // 超时异常同时还会触发listener
            threadPool.schedule(
                () -> handler.handleLocalException(new ConnectTransportException(node, "handshake_timeout[" + timeout + "]")),
                timeout,
                ThreadPool.Names.GENERIC);
            success = true;
        } catch (Exception e) {
            handler.handleLocalException(new ConnectTransportException(node, "failure to send " + HANDSHAKE_ACTION_NAME, e));
        } finally {
            if (success == false) {
                TransportResponseHandler<?> removed = pendingHandshakes.remove(requestId);
                assert removed == null : "Handshake should not be pending if exception was thrown";
            }
        }
    }

    /**
     * 针对 server 接收到 client的握手请求 生成对应的res对象并通过channel 返回
     * @param channel
     * @param requestId
     * @param stream
     * @throws IOException
     */
    void handleHandshake(TransportChannel channel, long requestId, StreamInput stream) throws IOException {
        // Must read the handshake request to exhaust the stream
        HandshakeRequest handshakeRequest = new HandshakeRequest(stream);
        final int nextByte = stream.read();
        if (nextByte != -1) {
            throw new IllegalStateException("Handshake request not fully read for requestId [" + requestId + "], action ["
                + TransportHandshaker.HANDSHAKE_ACTION_NAME + "], available [" + stream.available() + "]; resetting");
        }
        // 就是将当前节点的版本号返回 用于判断兼容性
        channel.sendResponse(new HandshakeResponse(this.version));
    }

    /**
     * 在处理res时 将req从命令池中移除
     * @param requestId
     * @return
     */
    TransportResponseHandler<HandshakeResponse> removeHandlerForHandshake(long requestId) {
        return pendingHandshakes.remove(requestId);
    }

    int getNumPendingHandshakes() {
        return pendingHandshakes.size();
    }

    long getNumHandshakes() {
        return numHandshakes.count();
    }

    /**
     * 该对象专门处理握手的响应结果
     */
    private class HandshakeResponseHandler implements TransportResponseHandler<HandshakeResponse> {

        private final long requestId;
        private final Version currentVersion;
        private final ActionListener<Version> listener;
        /**
         * 该结果仅能被处理一次
         */
        private final AtomicBoolean isDone = new AtomicBoolean(false);

        private HandshakeResponseHandler(long requestId, Version currentVersion, ActionListener<Version> listener) {
            this.requestId = requestId;
            this.currentVersion = currentVersion;
            this.listener = listener;
        }

        /**
         * 将对端数据流转换成 res对象
         * @param in Input to read the value from
         * @return
         * @throws IOException
         */
        @Override
        public HandshakeResponse read(StreamInput in) throws IOException {
            return new HandshakeResponse(in);
        }

        @Override
        public void handleResponse(HandshakeResponse response) {
            if (isDone.compareAndSet(false, true)) {
                Version version = response.responseVersion;
                if (currentVersion.isCompatible(version) == false) {
                    listener.onFailure(new IllegalStateException("Received message from unsupported version: [" + version
                        + "] minimal compatible version is: [" + currentVersion.minimumCompatibilityVersion() + "]"));
                } else {
                    listener.onResponse(version);
                }
            }
        }

        /**
         * 触发监听器的onFailure 钩子
         * @param e
         */
        @Override
        public void handleException(TransportException e) {
            if (isDone.compareAndSet(false, true)) {
                listener.onFailure(new IllegalStateException("handshake failed", e));
            }
        }

        void handleLocalException(TransportException e) {
            if (removeHandlerForHandshake(requestId) != null && isDone.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    /**
     * 握手请求体  仅包含当前节点的版本号
     */
    static final class HandshakeRequest extends TransportRequest {

        private final Version version;

        HandshakeRequest(Version version) {
            this.version = version;
        }

        HandshakeRequest(StreamInput streamInput) throws IOException {
            super(streamInput);
            BytesReference remainingMessage;
            try {
                remainingMessage = streamInput.readBytesReference();
            } catch (EOFException e) {
                remainingMessage = null;
            }
            if (remainingMessage == null) {
                version = null;
            } else {
                try (StreamInput messageStreamInput = remainingMessage.streamInput()) {
                    this.version = Version.readVersion(messageStreamInput);
                }
            }
        }

        @Override
        public void writeTo(StreamOutput streamOutput) throws IOException {
            super.writeTo(streamOutput);
            assert version != null;
            try (BytesStreamOutput messageStreamOutput = new BytesStreamOutput(4)) {
                Version.writeVersion(version, messageStreamOutput);
                BytesReference reference = messageStreamOutput.bytes();
                streamOutput.writeBytesReference(reference);
            }
        }
    }

    /**
     * 对应握手的响应结果
     */
    static final class HandshakeResponse extends TransportResponse {

        /**
         * 实际上就是返回了对端节点的版本号  这样就可以判断对端节点能否支持一些请求
         */
        private final Version responseVersion;

        HandshakeResponse(Version responseVersion) {
            this.responseVersion = responseVersion;
        }

        private HandshakeResponse(StreamInput in) throws IOException {
            super(in);
            responseVersion = Version.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert responseVersion != null;
            Version.writeVersion(responseVersion, out);
        }

        Version getResponseVersion() {
            return responseVersion;
        }
    }

    @FunctionalInterface
    interface HandshakeRequestSender {

        void sendRequest(DiscoveryNode node, TcpChannel channel, long requestId, Version version) throws IOException;
    }
}
