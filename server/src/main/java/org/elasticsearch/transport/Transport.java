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
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * 传输层对象 实际上节点间的连接就是由该对象完成的
 */
public interface Transport extends LifecycleComponent {

    /**
     * Registers a new request handler
     * 为传输层注册一个请求处理器  也就是任务的分发是做在传输层的
     */
    default <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        getRequestHandlers().registerHandler(reg);
    }

    /**
     * 监听整个传输层发送/接收的req/res
     * @param listener
     */
    void setMessageListener(TransportMessageListener listener);

    /**
     * 协议层是否需要加密
     * @return
     */
    default boolean isSecure() {
        return false;
    }

    /**
     * The address the transport is bound on.
     * 服务器绑定的所有地址 每个地址都允许接收新的连接  不同的profileSettings会定义不同的地址
     */
    BoundTransportAddress boundAddress();

    /**
     * Further profile bound addresses
     * @return <code>null</code> iff profiles are unsupported, otherwise a map with name of profile and its bound transport address
     * 通过profile进行分组
     */
    Map<String, BoundTransportAddress> profileBoundAddresses();

    /**
     * Returns an address from its string representation.
     * 返回某个地址关联的所有 address
     */
    TransportAddress[] addressesFromString(String address) throws UnknownHostException;

    /**
     * Returns a list of all local addresses for this transport
     * 当前节点可选择的一组地址
     */
    List<String> getDefaultSeedAddresses();

    /**
     * Opens a new connection to the given node. When the connection is fully connected, the listener is called.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     * @param profile 描述连接的详情
     * 连接到某个节点
     */
    void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Transport.Connection> listener);

    /**
     * 描述统计信息
     * @return
     */
    TransportStats getStats();

    /**
     * 相当于请求池  他处理的维度是细化到connect的  而 requestHandler是细化到 api层
     * @return
     */
    ResponseHandlers getResponseHandlers();

    /**
     * 该对象存储了所有的请求处理器
     * @return
     */
    RequestHandlers getRequestHandlers();

    /**
     * A unidirectional connection to a {@link DiscoveryNode}
     * 描述连接到某个节点的信息
     */
    interface Connection extends Closeable {
        /**
         * The node this connection is associated with
         * 目标节点
         */
        DiscoveryNode getNode();

        /**
         * Sends the request to the node this connection is associated with
         * @param requestId see {@link ResponseHandlers#add(ResponseContext)} for details   请求的id
         * @param action the action to execute     请求的api
         * @param request the request to send    请求体
         * @param options request options to apply   描述请求体的特殊信息
         * @throws NodeNotConnectedException if the given node is not connected
         * 往目标节点发送请求
         */
        void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) throws
            IOException, TransportException;

        /**
         * The listener's {@link ActionListener#onResponse(Object)} method will be called when this
         * connection is closed. No implementations currently throw an exception during close, so
         * {@link ActionListener#onFailure(Exception)} will not be called.
         *
         * @param listener to be called
         *                 该监听器负责处理 res 或者 请求失败
         */
        void addCloseListener(ActionListener<Void> listener);

        /**
         * 当前连接是否已经被关闭
         * @return
         */
        boolean isClosed();

        /**
         * Returns the version of the node this connection was established with.
         */
        default Version getVersion() {
            return getNode().getVersion();
        }

        /**
         * Returns a key that this connection can be cached on. Delegating subclasses must delegate method call to
         * the original connection.
         */
        default Object getCacheKey() {
            return this;
        }

        @Override
        void close();
    }

    /**
     * This class represents a response context that encapsulates the actual response handler, the action and the connection it was
     * executed on.
     * 对普通的 res对象进行了增强
     */
    final class ResponseContext<T extends TransportResponse> {

        /**
         * 该对象定义了如何处理res
         */
        private final TransportResponseHandler<T> handler;

        /**
         * 描述与目标节点的连接信息 可以设置处理res/fail的监听器 以及包含close 关闭连接的api   看来集群内部节点通信是基于 TCP连接 而es应用服务器对外暴露的是http端口
         */
        private final Connection connection;

        /**
         * 本次针对哪种api请求的结果
         */
        private final String action;

        ResponseContext(TransportResponseHandler<T> handler, Connection connection, String action) {
            this.handler = handler;
            this.connection = connection;
            this.action = action;
        }

        public TransportResponseHandler<T> handler() {
            return handler;
        }

        public Connection connection() {
            return this.connection;
        }

        public String action() {
            return this.action;
        }
    }

    /**
     * This class is a registry that allows
     * 维护所有注册的响应处理器
     * ResponseContext 同时包装了连接对象 以及TransportResponseHandler
     */
    final class ResponseHandlers {

        /**
         * 每个请求id 会绑定一个resContext 内部包含了如何处理res的逻辑
         */
        private final ConcurrentMapLong<ResponseContext<? extends TransportResponse>> handlers = ConcurrentCollections
            .newConcurrentMapLongWithAggressiveConcurrency();
        private final AtomicLong requestIdGenerator = new AtomicLong();

        /**
         * Returns <code>true</code> if the give request ID has a context associated with it.
         */
        public boolean contains(long requestId) {
            return handlers.containsKey(requestId);
        }

        /**
         * Removes and return the {@link ResponseContext} for the given request ID or returns
         * <code>null</code> if no context is associated with this request ID.
         * 代表某个请求已经处理完毕了
         */
        public ResponseContext<? extends TransportResponse> remove(long requestId) {
            return handlers.remove(requestId);
        }

        /**
         * Adds a new response context and associates it with a new request ID.
         * @return the new request ID
         * @see Connection#sendRequest(long, String, TransportRequest, TransportRequestOptions)
         */
        public long add(ResponseContext<? extends TransportResponse> holder) {
            long requestId = newRequestId();
            ResponseContext<? extends TransportResponse> existing = handlers.put(requestId, holder);
            assert existing == null : "request ID already in use: " + requestId;
            return requestId;
        }

        /**
         * Returns a new request ID to use when sending a message via {@link Connection#sendRequest(long, String,
         * TransportRequest, TransportRequestOptions)}
         */
        long newRequestId() {
            return requestIdGenerator.incrementAndGet();
        }

        /**
         * Removes and returns all {@link ResponseContext} instances that match the predicate
         * 将满足谓语条件的 context移除 推测是超时之类的
         */
        public List<ResponseContext<? extends TransportResponse>> prune(Predicate<ResponseContext<? extends TransportResponse>> predicate) {
            final List<ResponseContext<? extends TransportResponse>> holders = new ArrayList<>();
            for (Map.Entry<Long, ResponseContext<? extends TransportResponse>> entry : handlers.entrySet()) {
                ResponseContext<? extends TransportResponse> holder = entry.getValue();
                if (predicate.test(holder)) {
                    ResponseContext<? extends TransportResponse> remove = handlers.remove(entry.getKey());
                    if (remove != null) {
                        holders.add(holder);
                    }
                }
            }
            return holders;
        }

        /**
         * called by the {@link Transport} implementation when a response or an exception has been received for a previously
         * sent request (before any processing or deserialization was done). Returns the appropriate response handler or null if not
         * found.
         * 当某个请求接收到匹配的res时 从handlers中移除
         */
        public TransportResponseHandler<? extends TransportResponse> onResponseReceived(final long requestId,
                                                                                        final TransportMessageListener listener) {
            ResponseContext<? extends TransportResponse> context = handlers.remove(requestId);
            listener.onResponseReceived(requestId, context);
            if (context == null) {
                return null;
            } else {
                return context.handler();
            }
        }
    }

    /**
     * 该对象以 action为key 存储了所有的请求处理器
     */
    final class RequestHandlers {

        private volatile Map<String, RequestHandlerRegistry<? extends TransportRequest>> requestHandlers = Collections.emptyMap();

        /**
         * 避免重复注册同一action的处理器
         * @param reg
         * @param <Request>
         */
        synchronized <Request extends TransportRequest> void registerHandler(RequestHandlerRegistry<Request> reg) {
            if (requestHandlers.containsKey(reg.getAction())) {
                throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
            }
            requestHandlers = Maps.copyMapWithAddedEntry(requestHandlers, reg.getAction(), reg);
        }

        // TODO: Only visible for testing. Perhaps move StubbableTransport from
        //  org.elasticsearch.test.transport to org.elasticsearch.transport
        // 这里采用覆盖的方式进行注册
        public synchronized <Request extends TransportRequest> void forceRegister(RequestHandlerRegistry<Request> reg) {
            requestHandlers = Maps.copyMapWithAddedOrReplacedEntry(requestHandlers, reg.getAction(), reg);
        }

        @SuppressWarnings("unchecked")
        public <T extends TransportRequest> RequestHandlerRegistry<T> getHandler(String action) {
            return (RequestHandlerRegistry<T>) requestHandlers.get(action);
        }
    }
}
