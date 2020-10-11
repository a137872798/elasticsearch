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
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * 传输层服务  该对象本身实现了监听连接状态变化的接口 以及处理接受请求/发送响应结果时的api
 */
public class TransportService extends AbstractLifecycleComponent implements ReportingService<TransportInfo>, TransportMessageListener,
    TransportConnectionListener {
    private static final Logger logger = LogManager.getLogger(TransportService.class);

    public static final String DIRECT_RESPONSE_PROFILE = ".direct";
    public static final String HANDSHAKE_ACTION_NAME = "internal:transport/handshake";

    /**
     * 该标识代表是否允许处理请求  ap模式???  这种套路有点像是eureka的 在节点数据同步前无法处理请求
     */
    private final AtomicBoolean handleIncomingRequests = new AtomicBoolean();

    /**
     * 将一组监听器隐藏在一个监听器下
     */
    private final DelegatingTransportMessageListener messageListener = new DelegatingTransportMessageListener();

    /**
     * 该对象负责建立连接 以及处理发送/接收数据
     */
    protected final Transport transport;
    protected final ConnectionManager connectionManager;
    protected final ThreadPool threadPool;
    /**
     * 该节点所在集群的名字
     */
    protected final ClusterName clusterName;

    /**
     * 下层接收到的req都会转换成某种action 并交由taskManager处理
     */
    protected final TaskManager taskManager;

    /**
     * 代表将某个请求 通过某条connection 发送到目标节点  这里指定了操作类型 以及对应的 resHandler
     * 对应 sendRequestInternal()
     */
    private final TransportInterceptor.AsyncSender asyncSender;

    /**
     * 将地址转换成 node
     */
    private final Function<BoundTransportAddress, DiscoveryNode> localNodeFactory;

    /**
     * 是否需要访问远端集群  如果需要的话 还支持从远端集群监听配置的变化
     */
    private final boolean remoteClusterClient;

    /**
     * 维护了所有待处理的req 以及对应的res处理器
     */
    private final Transport.ResponseHandlers responseHandlers;

    private final TransportInterceptor interceptor;

    // An LRU (don't really care about concurrency here) that holds the latest timed out requests so if they
    // do show up, we can print more descriptive information about them
    // 这里会维护最近超时的一些请求    value包含了超时信息
    final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers =
        Collections.synchronizedMap(new LinkedHashMap<Long, TimeoutInfoHolder>(100, .75F, true) {

            /**
             * 最多仅维护 100 个数据
             * @param eldest
             * @return
             */
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > 100;
            }
        });

    public static final TransportInterceptor NOOP_TRANSPORT_INTERCEPTOR = new TransportInterceptor() {};

    // tracer log

    private final Logger tracerLog;

    /**
     * 可能某些不需要打印日志
     */
    volatile String[] tracerLogInclude;
    volatile String[] tracerLogExclude;

    /**
     * 管理与远端集群的通讯
     */
    private final RemoteClusterService remoteClusterService;

    /**
     * if set will call requests sent to this id to shortcut and executed locally
     * 本地节点 如果需要将请求发往本地节点 会直接调用相关指令 而不需要通过网络  在某些node即作为es集群中的服务节点又作为请求节点时使用
     * */
    volatile DiscoveryNode localNode = null;

    /**
     * 模拟连接到本地node
     */
    private final Transport.Connection localNodeConnection = new Transport.Connection() {
        @Override
        public DiscoveryNode getNode() {
            return localNode;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws TransportException {
            // 直接交由本地的 reqHandler处理请求
            sendLocalRequest(requestId, action, request, options);
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
        }
    };

    /**
     * Build the service.
     *
     * @param clusterSettings if non null, the {@linkplain TransportService} will register with the {@link ClusterSettings} for settings
     *    updates for {@link TransportSettings#TRACE_LOG_EXCLUDE_SETTING} and {@link TransportSettings#TRACE_LOG_INCLUDE_SETTING}.
     */
    public TransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor transportInterceptor,
                            Function<BoundTransportAddress, DiscoveryNode> localNodeFactory, @Nullable ClusterSettings clusterSettings,
                            Set<String> taskHeaders) {
        this(settings, transport, threadPool, transportInterceptor, localNodeFactory, clusterSettings, taskHeaders,
            new ClusterConnectionManager(settings, transport));
    }

    /**
     * 初始化传输层服务对象
     * @param settings
     * @param transport
     * @param threadPool
     * @param transportInterceptor
     * @param localNodeFactory
     * @param clusterSettings
     * @param taskHeaders
     * @param connectionManager
     */
    public TransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor transportInterceptor,
                            Function<BoundTransportAddress, DiscoveryNode> localNodeFactory, @Nullable ClusterSettings clusterSettings,
                            Set<String> taskHeaders, ConnectionManager connectionManager) {
        this.transport = transport;
        this.threadPool = threadPool;
        this.localNodeFactory = localNodeFactory;
        this.connectionManager = connectionManager;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        setTracerLogInclude(TransportSettings.TRACE_LOG_INCLUDE_SETTING.get(settings));
        setTracerLogExclude(TransportSettings.TRACE_LOG_EXCLUDE_SETTING.get(settings));
        tracerLog = Loggers.getLogger(logger, ".tracer");

        // 创建任务管理器
        taskManager = createTaskManager(settings, threadPool, taskHeaders);
        this.interceptor = transportInterceptor;

        // sendRequestInternal 就是一个发送请求的模板
        this.asyncSender = interceptor.interceptSender(this::sendRequestInternal);
        this.remoteClusterClient = Node.NODE_REMOTE_CLUSTER_CLIENT.get(settings);
        remoteClusterService = new RemoteClusterService(settings, this);

        // 该对象存储了所有 发送的req对应的 resHandler
        responseHandlers = transport.getResponseHandlers();

        // 监听支持动态更新的配置
        if (clusterSettings != null) {
            clusterSettings.addSettingsUpdateConsumer(TransportSettings.TRACE_LOG_INCLUDE_SETTING, this::setTracerLogInclude);
            clusterSettings.addSettingsUpdateConsumer(TransportSettings.TRACE_LOG_EXCLUDE_SETTING, this::setTracerLogExclude);
            if (remoteClusterClient) {
                remoteClusterService.listenForUpdates(clusterSettings);
            }
        }
        // 注册有关握手action的处理器
        registerRequestHandler(
            HANDSHAKE_ACTION_NAME,
            ThreadPool.Names.SAME,
            false, false,
            HandshakeRequest::new,
            // 针对握手请求的请求处理器  可以看到 直接将当前节点信息返回
            (request, channel, task) -> channel.sendResponse(
                new HandshakeResponse(localNode, clusterName, localNode.getVersion())));
    }

    public RemoteClusterService getRemoteClusterService() {
        return remoteClusterService;
    }

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    protected TaskManager createTaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
        return new TaskManager(settings, threadPool, taskHeaders);
    }

    /**
     * The executor service for this transport service.
     *
     * @return the executor service
     */
    private ExecutorService getExecutorService() {
        return threadPool.generic();
    }

    void setTracerLogInclude(List<String> tracerLogInclude) {
        this.tracerLogInclude = tracerLogInclude.toArray(Strings.EMPTY_ARRAY);
    }

    void setTracerLogExclude(List<String> tracerLogExclude) {
        this.tracerLogExclude = tracerLogExclude.toArray(Strings.EMPTY_ARRAY);
    }

    /**
     * 启动传输层服务
     */
    @Override
    protected void doStart() {
        // 使用当前对象监听接受发送的 req/res
        transport.setMessageListener(this);
        connectionManager.addListener(this);
        // 这里其实就是开启客户端channel 组装工厂和 服务端channel组装工厂  (其中包含了将channel注册到事件循环对应的selector的逻辑)
        transport.start();
        if (transport.boundAddress() != null && logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
            for (Map.Entry<String, BoundTransportAddress> entry : transport.profileBoundAddresses().entrySet()) {
                logger.info("profile [{}]: {}", entry.getKey(), entry.getValue());
            }
        }
        // 通过该函数生成本地 node
        localNode = localNodeFactory.apply(transport.boundAddress());

        // TODO 先忽略有关远端集群的
        if (remoteClusterClient) {
            // here we start to connect to the remote clusters
            remoteClusterService.initializeRemoteClusters();
        }
    }

    @Override
    protected void doStop() {
        try {
            IOUtils.close(connectionManager, remoteClusterService, transport::stop);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            // in case the transport is not connected to our local node (thus cleaned on node disconnect)
            // make sure to clean any leftover on going handles
            // 以特殊方式处理之前所有囤积的请求
            for (final Transport.ResponseContext holderToNotify : responseHandlers.prune(h -> true)) {
                // callback that an exception happened, but on a different thread since we don't
                // want handlers to worry about stack overflows
                getExecutorService().execute(new AbstractRunnable() {
                    @Override
                    public void onRejection(Exception e) {
                        // if we get rejected during node shutdown we don't wanna bubble it up
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on rejection, action: {}",
                                holderToNotify.action()),
                            e);
                    }
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on exception, action: {}",
                                holderToNotify.action()),
                            e);
                    }
                    @Override
                    public void doRun() {
                        // 以异常方式触发之前resHandler
                        TransportException ex = new SendRequestTransportException(holderToNotify.connection().getNode(),
                            holderToNotify.action(), new NodeClosedException(localNode));
                        holderToNotify.handler().handleException(ex);
                    }
                });
            }
        }
    }

    @Override
    protected void doClose() throws IOException {
        transport.close();
    }

    /**
     * start accepting incoming requests.
     * when the transport layer starts up it will block any incoming requests until
     * this method is called
     */
    public final void acceptIncomingRequests() {
        handleIncomingRequests.set(true);
    }

    @Override
    public TransportInfo info() {
        BoundTransportAddress boundTransportAddress = boundAddress();
        if (boundTransportAddress == null) {
            return null;
        }
        return new TransportInfo(boundTransportAddress, transport.profileBoundAddresses());
    }

    public TransportStats stats() {
        return transport.getStats();
    }

    public boolean isTransportSecure() {
        return transport.isSecure();
    }

    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public List<String> getDefaultSeedAddresses() {
        return transport.getDefaultSeedAddresses();
    }

    /**
     * Returns <code>true</code> iff the given node is already connected.
     */
    public boolean nodeConnected(DiscoveryNode node) {
        return isLocalNode(node) || connectionManager.nodeConnected(node);
    }

    /**
     * Connect to the specified node with the given connection profile.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param node the node to connect to
     * @param listener the action listener to notify
     */
    public void connectToNode(DiscoveryNode node, ActionListener<Void> listener) throws ConnectTransportException {
        connectToNode(node, null, listener);
    }

    /**
     * Connect to the specified node with the given connection profile.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use when connecting to this node
     * @param listener the action listener to notify
     *                 通过传输层建立与某个node 的连接
     */
    public void connectToNode(final DiscoveryNode node, ConnectionProfile connectionProfile, ActionListener<Void> listener) {
        if (isLocalNode(node)) {
            listener.onResponse(null);
            return;
        }
        // 当生成与某个node的 connection时 再进行一次校验
        connectionManager.connectToNode(node, connectionProfile, connectionValidator(node), listener);
    }

    /**
     * 为了鉴别连接是否有效 需要一个连接检测器 这个对象就是基于握手请求实现的  而在传输层也有一个握手请求 那个时候是检测节点间版本是否兼容
     * @param node
     * @return
     */
    public ConnectionManager.ConnectionValidator connectionValidator(DiscoveryNode node) {
        return (newConnection, actualProfile, listener) -> {
            // We don't validate cluster names to allow for CCS connections.
            // 这里确保不是连接到本节点
            handshake(newConnection, actualProfile.getHandshakeTimeout().millis(), cn -> true, ActionListener.map(listener, resp -> {
                final DiscoveryNode remote = resp.discoveryNode;
                if (node.equals(remote) == false) {
                    throw new ConnectTransportException(node, "handshake failed. unexpected remote node " + remote);
                }
                return null;
            }));
        };
    }

    /**
     * Establishes a new connection to the given node. The connection is NOT maintained by this service, it's the callers
     * responsibility to close the connection once it goes out of scope.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     * @param node the node to connect to
     * @param connectionProfile the connection profile to use
     * @param listener the action listener to notify
     *                 创建与目标节点的连接
     */
    public void openConnection(final DiscoveryNode node, ConnectionProfile connectionProfile,
                               ActionListener<Transport.Connection> listener) {
        if (isLocalNode(node)) {
            // 连接本地节点会绕过握手请求
            listener.onResponse(localNodeConnection);
        } else {
            connectionManager.openConnection(node, connectionProfile, listener);
        }
    }

    /**
     * Executes a high-level handshake using the given connection
     * and returns the discovery node of the node the connection
     * was established with. The handshake will fail if the cluster
     * name on the target node mismatches the local cluster name.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param connection       the connection to a specific node
     * @param handshakeTimeout handshake timeout
     * @param listener         action listener to notify
     * @throws ConnectTransportException if the connection failed
     * @throws IllegalStateException if the handshake failed
     */
    public void handshake(
        final Transport.Connection connection,
        final long handshakeTimeout,
        final ActionListener<DiscoveryNode> listener) {
        handshake(connection, handshakeTimeout, clusterName.getEqualityPredicate(),
            ActionListener.map(listener, HandshakeResponse::getDiscoveryNode));
    }

    /**
     * Executes a high-level handshake using the given connection
     * and returns the discovery node of the node the connection
     * was established with. The handshake will fail if the cluster
     * name on the target node doesn't match the local cluster name.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     *
     * @param connection       the connection to a specific node
     * @param handshakeTimeout handshake timeout
     * @param clusterNamePredicate cluster name validation predicate  代表只有符合条件的集群才会处理这个请求
     * @param listener         action listener to notify
     * @throws IllegalStateException if the handshake failed
     * 当建立与某个node的connection 时 需要在一次探测 也就是握手请求
     */
    public void handshake(
        final Transport.Connection connection,
        final long handshakeTimeout, Predicate<ClusterName> clusterNamePredicate,
        final ActionListener<HandshakeResponse> listener) {
        final DiscoveryNode node = connection.getNode();
        // 发送请求到目标节点
        sendRequest(connection, HANDSHAKE_ACTION_NAME, HandshakeRequest.INSTANCE,
            TransportRequestOptions.builder().withTimeout(handshakeTimeout).build(),
            new ActionListenerResponseHandler<>(
                new ActionListener<>() {
                    @Override
                    public void onResponse(HandshakeResponse response) {
                        if (clusterNamePredicate.test(response.clusterName) == false) {
                            listener.onFailure(new IllegalStateException("handshake with [" + node + "] failed: remote cluster name ["
                                + response.clusterName.value() + "] does not match " + clusterNamePredicate));
                        } else if (response.version.isCompatible(localNode.getVersion()) == false) {
                            listener.onFailure(new IllegalStateException("handshake with [" + node + "] failed: remote node version ["
                                + response.version + "] is incompatible with local node version [" + localNode.getVersion() + "]"));
                        } else {
                            listener.onResponse(response);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                }
                , HandshakeResponse::new, ThreadPool.Names.GENERIC
            ));
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    /**
     * 该握手请求与 Transport的 握手请求不同 那个对象携带了一个版本号 而该对象没有存储任何信息  针对这一层的握手请求仅检测连接的节点是否是本节点
     */
    static class HandshakeRequest extends TransportRequest {

        public static final HandshakeRequest INSTANCE = new HandshakeRequest();

        HandshakeRequest(StreamInput in) throws IOException {
            super(in);
        }

        private HandshakeRequest() {
        }

    }

    /**
     * 针对握手请求的响应结果
     */
    public static class HandshakeResponse extends TransportResponse {

        /**
         * 将当前节点信息返回给对端
         */
        private final DiscoveryNode discoveryNode;
        private final ClusterName clusterName;
        private final Version version;

        public HandshakeResponse(DiscoveryNode discoveryNode, ClusterName clusterName, Version version) {
            this.discoveryNode = discoveryNode;
            this.version = version;
            this.clusterName = clusterName;
        }

        public HandshakeResponse(StreamInput in) throws IOException {
            super(in);
            discoveryNode = in.readOptionalWriteable(DiscoveryNode::new);
            clusterName = new ClusterName(in);
            version = Version.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(discoveryNode);
            clusterName.writeTo(out);
            Version.writeVersion(version, out);
        }

        public DiscoveryNode getDiscoveryNode() {
            return discoveryNode;
        }

        public ClusterName getClusterName() {
            return clusterName;
        }
    }

    /**
     * 实际上就是关闭所有channel
     * @param node
     */
    public void disconnectFromNode(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return;
        }
        connectionManager.disconnectFromNode(node);
    }

    public void addMessageListener(TransportMessageListener listener) {
        messageListener.listeners.add(listener);
    }

    public boolean removeMessageListener(TransportMessageListener listener) {
        return messageListener.listeners.remove(listener);
    }

    public void addConnectionListener(TransportConnectionListener listener) {
        connectionManager.addListener(listener);
    }

    public void removeConnectionListener(TransportConnectionListener listener) {
        connectionManager.removeListener(listener);
    }

    /**
     * 发送请求 并使用预备好的 resHandler处理结果
     * @param node
     * @param action
     * @param request
     * @param handler
     * @param <T>
     */
    public <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action,
                                                                final TransportRequest request,
                                                                final TransportResponseHandler<T> handler) {
        final Transport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (final NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
            return;
        }
        sendRequest(connection, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public final <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action,
                                                                final TransportRequest request,
                                                                final TransportRequestOptions options,
                                                                TransportResponseHandler<T> handler) {
        final Transport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (final NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
            return;
        }
        sendRequest(connection, action, request, options, handler);
    }

    /**
     * Sends a request on the specified connection. If there is a failure sending the request, the specified handler is invoked.
     *
     * @param connection the connection to send the request on
     * @param action     the name of the action
     * @param request    the request
     * @param options    the options for this request
     * @param handler    the response handler
     * @param <T>        the type of the transport response
     *           发送请求到目标节点
     */
    public final <T extends TransportResponse> void sendRequest(final Transport.Connection connection, final String action,
                                                                final TransportRequest request,
                                                                final TransportRequestOptions options,
                                                                TransportResponseHandler<T> handler) {
        try {
            // 本次请求是由某个父任务触发的
            if (request.getParentTask().isSet()) {
                // TODO: capture the connection instead so that we can cancel child tasks on the remote connections.
                // 返回的对象用于在接受到子任务结果时调用 主要是注销之前注册的任务
                final Releasable unregisterChildNode = taskManager.registerChildNode(request.getParentTask().getId(), connection.getNode());
                final TransportResponseHandler<T> delegate = handler;
                handler = new TransportResponseHandler<>() {
                    @Override
                    public void handleResponse(T response) {
                        unregisterChildNode.close();
                        delegate.handleResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        unregisterChildNode.close();
                        delegate.handleException(exp);
                    }

                    @Override
                    public String executor() {
                        return delegate.executor();
                    }

                    @Override
                    public T read(StreamInput in) throws IOException {
                        return delegate.read(in);
                    }
                };
            }
            // 这里实际上就是通过 transport发送请求
            asyncSender.sendRequest(connection, action, request, options, handler);
        } catch (final Exception ex) {
            // the caller might not handle this so we invoke the handler
            final TransportException te;
            if (ex instanceof TransportException) {
                te = (TransportException) ex;
            } else {
                te = new TransportException("failure to send", ex);
            }
            handler.handleException(te);
        }
    }

    /**
     * Returns either a real transport connection or a local node connection if we are using the local node optimization.
     * @throws NodeNotConnectedException if the given node is not connected
     */
    public Transport.Connection getConnection(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return localNodeConnection;
        } else {
            return connectionManager.getConnection(node);
        }
    }

    /**
     * 发送一个子请求 可能在某些比较复杂的逻辑中无法通过一次交互完成  而此时又需要做链路追踪 所以需要在task中做关联吧
     * @param node
     * @param action
     * @param request
     * @param parentTask    代表本次task 是由哪个父任务创建的
     * @param options
     * @param handler
     * @param <T>
     */
    public final <T extends TransportResponse> void sendChildRequest(final DiscoveryNode node, final String action,
                                                                     final TransportRequest request, final Task parentTask,
                                                                     final TransportRequestOptions options,
                                                                     final TransportResponseHandler<T> handler) {
        final Transport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (final NodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
            return;
        }
        sendChildRequest(connection, action, request, parentTask, options, handler);
    }

    public <T extends TransportResponse> void sendChildRequest(final Transport.Connection connection, final String action,
                                                               final TransportRequest request, final Task parentTask,
                                                               final TransportResponseHandler<T> handler) {
        sendChildRequest(connection, action, request, parentTask, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> void sendChildRequest(final Transport.Connection connection, final String action,
                                                               final TransportRequest request, final Task parentTask,
                                                               final TransportRequestOptions options,
                                                               final TransportResponseHandler<T> handler) {
        // 设置父任务
        request.setParentTask(localNode.getId(), parentTask.getId());
        sendRequest(connection, action, request, options, handler);
    }

    /**
     * 异步发送某个请求
     * @param connection  本次使用的连接对象  连接对象代表某个目标node 以及多条channel
     * @param action
     * @param request
     * @param options
     * @param handler
     * @param <T>
     */
    private <T extends TransportResponse> void sendRequestInternal(final Transport.Connection connection, final String action,
                                                                   final TransportRequest request,
                                                                   final TransportRequestOptions options,
                                                                   TransportResponseHandler<T> handler) {
        if (connection == null) {
            throw new IllegalStateException("can't send request to a null connection");
        }
        DiscoveryNode node = connection.getNode();

        // 创建一个新的上下文对象
        Supplier<ThreadContext.StoredContext> storedContextSupplier = threadPool.getThreadContext().newRestorableContext(true);
        // 将上下文与resHandler包装在一起
        ContextRestoreResponseHandler<T> responseHandler = new ContextRestoreResponseHandler<>(storedContextSupplier, handler);
        // TODO we can probably fold this entire request ID dance into connection.sendRequest but it will be a bigger refactoring
        final long requestId = responseHandlers.add(new Transport.ResponseContext<>(responseHandler, connection, action));
        final TimeoutHandler timeoutHandler;
        // 本次请求支持超时的情况 创建超时对象
        if (options.timeout() != null) {
            timeoutHandler = new TimeoutHandler(requestId, connection.getNode(), action);
            responseHandler.setTimeoutHandler(timeoutHandler);
        } else {
            timeoutHandler = null;
        }
        try {
            if (lifecycle.stoppedOrClosed()) {
                /*
                 * If we are not started the exception handling will remove the request holder again and calls the handler to notify the
                 * caller. It will only notify if toStop hasn't done the work yet.
                 */
                throw new NodeClosedException(localNode);
            }
            if (timeoutHandler != null) {
                assert options.timeout() != null;
                // 启动定时任务 在一定延迟后以超时异常触发 resHandler
                timeoutHandler.scheduleTimeout(options.timeout());
            }
            connection.sendRequest(requestId, action, request, options); // local node optimization happens upstream
        } catch (final Exception e) {
            // usually happen either because we failed to connect to the node
            // or because we failed serializing the message
            final Transport.ResponseContext<? extends TransportResponse> contextToNotify = responseHandlers.remove(requestId);
            // If holderToNotify == null then handler has already been taken care of.
            if (contextToNotify != null) {
                if (timeoutHandler != null) {
                    timeoutHandler.cancel();
                }
                // callback that an exception happened, but on a different thread since we don't
                // want handlers to worry about stack overflows. In the special case of running into a closing node we run on the current
                // thread on a best effort basis though.
                final SendRequestTransportException sendRequestException = new SendRequestTransportException(node, action, e);
                final String executor = lifecycle.stoppedOrClosed() ? ThreadPool.Names.SAME : ThreadPool.Names.GENERIC;
                threadPool.executor(executor).execute(new AbstractRunnable() {
                    @Override
                    public void onRejection(Exception e) {
                        // if we get rejected during node shutdown we don't wanna bubble it up
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on rejection, action: {}",
                                contextToNotify.action()),
                            e);
                    }
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on exception, action: {}",
                                contextToNotify.action()),
                            e);
                    }
                    @Override
                    protected void doRun() throws Exception {
                        contextToNotify.handler().handleException(sendRequestException);
                    }
                });
            } else {
                logger.debug("Exception while sending request, handler likely already notified due to timeout", e);
            }
        }
    }

    /**
     * 直接在本地处理请求
     * @param requestId
     * @param action
     * @param request
     * @param options
     */
    private void sendLocalRequest(long requestId, final String action, final TransportRequest request, TransportRequestOptions options) {
        final DirectResponseChannel channel = new DirectResponseChannel(localNode, action, requestId, this, threadPool);
        try {
            // 触发钩子
            onRequestSent(localNode, requestId, action, request, options);
            onRequestReceived(requestId, action);
            // 根据action 获取对应的请求处理器
            final RequestHandlerRegistry reg = getRequestHandler(action);
            if (reg == null) {
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
            }
            final String executor = reg.getExecutor();
            if (ThreadPool.Names.SAME.equals(executor)) {
                //noinspection unchecked
                reg.processMessageReceived(request, channel);
            } else {
                // 通过线程池异步执行任务   当前线程应该是事件循环线程
                threadPool.executor(executor).execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() throws Exception {
                        //noinspection unchecked
                        reg.processMessageReceived(request, channel);
                    }

                    @Override
                    public boolean isForceExecution() {
                        return reg.isForceExecution();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            // 当出现异常时 将异常信息发送到对端
                            channel.sendResponse(e);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            logger.warn(() -> new ParameterizedMessage(
                                    "failed to notify channel of error message for action [{}]", action), inner);
                        }
                    }

                    @Override
                    public String toString() {
                        return "processing of [" + requestId + "][" + action + "]: " + request;
                    }
                });
            }

        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.warn(
                    () -> new ParameterizedMessage(
                        "failed to notify channel of error message for action [{}]", action), inner);
            }
        }
    }

    private boolean shouldTraceAction(String action) {
        return shouldTraceAction(action, tracerLogInclude, tracerLogExclude);
    }

    public static boolean shouldTraceAction(String action, String[] include, String[] exclude) {
        if (include.length > 0) {
            if (Regex.simpleMatch(include, action) == false) {
                return false;
            }
        }
        if (exclude.length > 0) {
            return !Regex.simpleMatch(exclude, action);
        }
        return true;
    }

    /**
     * 将格式化后的地址通过 地址解析器转换成一组传输层地址
     * @param address
     * @return
     * @throws UnknownHostException
     */
    public TransportAddress[] addressesFromString(String address) throws UnknownHostException {
        return transport.addressesFromString(address);
    }

    /**
     * A set of all valid action prefixes.
     * 每种action 会有对应的前缀
     */
    public static final Set<String> VALID_ACTION_PREFIXES = Set.of(
            "indices:admin",
            "indices:monitor",
            "indices:data/write",
            "indices:data/read",
            "indices:internal",
            "cluster:admin",
            "cluster:monitor",
            "cluster:internal",
            "internal:");

    private void validateActionName(String actionName) {
        // TODO we should makes this a hard validation and throw an exception but we need a good way to add backwards layer
        // for it. Maybe start with a deprecation layer
        if (isValidActionName(actionName) == false) {
            logger.warn("invalid action name [" + actionName + "] must start with one of: " +
                TransportService.VALID_ACTION_PREFIXES );
        }
    }

    /**
     * Returns <code>true</code> iff the action name starts with a valid prefix.
     *
     * @see #VALID_ACTION_PREFIXES
     */
    public static boolean isValidActionName(String actionName) {
        for (String prefix : VALID_ACTION_PREFIXES) {
            if (actionName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Registers a new request handler
     *
     * @param action         The action the request handler is associated with
     * @param requestReader  a callable to be used construct new instances for streaming
     * @param executor       The executor the request handling will be executed on
     * @param handler        The handler itself that implements the request handling
     *                       注册某种action的req处理器
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action, String executor,
                                                                          Writeable.Reader<Request> requestReader,
                                                                          TransportRequestHandler<Request> handler) {
        // 检验actionName是否有效
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, false, handler);
        // 包装相关对象 并注册
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, requestReader, taskManager, handler, executor, false, true);
        transport.registerRequestHandler(reg);
    }

    /**
     * Registers a new request handler
     *
     * @param action                The action the request handler is associated with
     * @param requestReader               The request class that will be used to construct new instances for streaming
     *                                    该对象定义了如何将输入流转换成 请求对象
     * @param executor              The executor the request handling will be executed on
     * @param forceExecution        Force execution on the executor queue and never reject it
     * @param canTripCircuitBreaker Check the request size and raise an exception in case the limit is breached.
     * @param handler               The handler itself that implements the request handling
     *                              注册某种请求处理器
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action,
                                                                          String executor, boolean forceExecution,
                                                                          boolean canTripCircuitBreaker,
                                                                          Writeable.Reader<Request> requestReader,
                                                                          TransportRequestHandler<Request> handler) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, forceExecution, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(
            action, requestReader, taskManager, handler, executor, forceExecution, canTripCircuitBreaker);
        transport.registerRequestHandler(reg);
    }

    /**
     * called by the {@link Transport} implementation when an incoming request arrives but before
     * any parsing of it has happened (with the exception of the requestId and action)
     * 该对象会作为监听器注册到 transport上  每当接受到一个请求时 触发下面所有的监听器
     */
    @Override
    public void onRequestReceived(long requestId, String action) {
        if (handleIncomingRequests.get() == false) {
            throw new IllegalStateException("transport not ready yet to handle incoming requests");
        }
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] received request", requestId, action);
        }
        messageListener.onRequestReceived(requestId, action);
    }

    /** called by the {@link Transport} implementation once a request has been sent */
    @Override
    public void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request,
                              TransportRequestOptions options) {
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] sent to [{}] (timeout: [{}])", requestId, action, node, options.timeout());
        }
        messageListener.onRequestSent(node, requestId, action, request, options);
    }

    @Override
    public void onResponseReceived(long requestId, Transport.ResponseContext holder) {
        if (holder == null) {
            // 当收到某个请求对应的结果时 检测请求是否超时
            checkForTimeout(requestId);
        } else if (tracerLog.isTraceEnabled() && shouldTraceAction(holder.action())) {
            tracerLog.trace("[{}][{}] received response from [{}]", requestId, holder.action(), holder.connection().getNode());
        }
        messageListener.onResponseReceived(requestId, holder);
    }

    /** called by the {@link Transport} implementation once a response was sent to calling node */
    @Override
    public void onResponseSent(long requestId, String action, TransportResponse response) {
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] sent response", requestId, action);
        }
        messageListener.onResponseSent(requestId, action, response);
    }

    /** called by the {@link Transport} implementation after an exception was sent as a response to an incoming request */
    @Override
    public void onResponseSent(long requestId, String action, Exception e) {
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace(() -> new ParameterizedMessage("[{}][{}] sent error response", requestId, action), e);
        }
        messageListener.onResponseSent(requestId, action, e);
    }

    public RequestHandlerRegistry<? extends TransportRequest> getRequestHandler(String action) {
        return transport.getRequestHandlers().getHandler(action);
    }

    /**
     * 检测某个请求的响应结果是否超时  这里只是打印日志
     * @param requestId
     */
    private void checkForTimeout(long requestId) {
        // lets see if its in the timeout holder, but sync on mutex to make sure any ongoing timeout handling has finished
        final DiscoveryNode sourceNode;
        final String action;
        assert responseHandlers.contains(requestId) == false;
        TimeoutInfoHolder timeoutInfoHolder = timeoutInfoHandlers.remove(requestId);
        // 代表已经超时了
        if (timeoutInfoHolder != null) {
            long time = threadPool.relativeTimeInMillis();
            logger.warn("Received response for a request that has timed out, sent [{}ms] ago, timed out [{}ms] ago, " +
                    "action [{}], node [{}], id [{}]", time - timeoutInfoHolder.sentTime(), time - timeoutInfoHolder.timeoutTime(),
                timeoutInfoHolder.action(), timeoutInfoHolder.node(), requestId);
            action = timeoutInfoHolder.action();
            sourceNode = timeoutInfoHolder.node();
        } else {
            logger.warn("Transport response handler not found of id [{}]", requestId);
            action = null;
            sourceNode = null;
        }
        // call tracer out of lock
        if (tracerLog.isTraceEnabled() == false) {
            return;
        }
        if (action == null) {
            assert sourceNode == null;
            tracerLog.trace("[{}] received response but can't resolve it to a request", requestId);
        } else if (shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] received response from [{}]", requestId, action, sourceNode);
        }
    }

    /**
     * 当某个连接被关闭时触发   将相关的等待响应结果的对象都以异常触发
     * @param connection the closed connection
     */
    @Override
    public void onConnectionClosed(Transport.Connection connection) {
        try {
            List<Transport.ResponseContext<? extends TransportResponse>> pruned =
                responseHandlers.prune(h -> h.connection().getCacheKey().equals(connection.getCacheKey()));
            // callback that an exception happened, but on a different thread since we don't
            // want handlers to worry about stack overflows
            getExecutorService().execute(new Runnable() {
                @Override
                public void run() {
                    for (Transport.ResponseContext holderToNotify : pruned) {
                        holderToNotify.handler().handleException(
                            new NodeDisconnectedException(connection.getNode(), holderToNotify.action()));
                    }
                }

                @Override
                public String toString() {
                    return "onConnectionClosed(" + connection.getNode() + ")";
                }
            });
        } catch (EsRejectedExecutionException ex) {
            logger.debug("Rejected execution on onConnectionClosed", ex);
        }
    }

    /**
     * 当请求处理超时时触发
     */
    final class TimeoutHandler implements Runnable {

        /**
         * 本次需要监控超时的请求对象
         */
        private final long requestId;
        /**
         * 请求发送时间
         */
        private final long sentTime = threadPool.relativeTimeInMillis();
        /**
         * 指令类型
         */
        private final String action;
        private final DiscoveryNode node;
        /**
         * 定时任务可以通过该对象关闭 这样就不会触发超时异常
         */
        volatile Scheduler.Cancellable cancellable;

        TimeoutHandler(long requestId, DiscoveryNode node, String action) {
            this.requestId = requestId;
            this.node = node;
            this.action = action;
        }

        /**
         * 在一定延迟后触发该方法
         */
        @Override
        public void run() {
            // 确保此时没有从响应池中移除
            if (responseHandlers.contains(requestId)) {
                long timeoutTime = threadPool.relativeTimeInMillis();
                // 将超时信息抽取出来存到handler中
                timeoutInfoHandlers.put(requestId, new TimeoutInfoHolder(node, action, sentTime, timeoutTime));
                // now that we have the information visible via timeoutInfoHandlers, we try to remove the request id
                final Transport.ResponseContext<? extends TransportResponse> holder = responseHandlers.remove(requestId);
                if (holder != null) {
                    assert holder.action().equals(action);
                    assert holder.connection().getNode().equals(node);
                    // 处理超时异常
                    holder.handler().handleException(
                        new ReceiveTimeoutTransportException(holder.connection().getNode(), holder.action(),
                            "request_id [" + requestId + "] timed out after [" + (timeoutTime - sentTime) + "ms]"));
                } else {
                    // response was processed, remove timeout info.
                    timeoutInfoHandlers.remove(requestId);
                }
            }
        }

        /**
         * cancels timeout handling. this is a best effort only to avoid running it. remove the requestId from {@link #responseHandlers}
         * to make sure this doesn't run.
         * 从外部关闭超时任务
         */
        public void cancel() {
            assert responseHandlers.contains(requestId) == false :
                "cancel must be called after the requestId [" + requestId + "] has been removed from clientHandlers";
            if (cancellable != null) {
                cancellable.cancel();
            }
        }

        @Override
        public String toString() {
            return "timeout handler for [" + requestId + "][" + action + "]";
        }

        /**
         * 开始超时任务
         * @param timeout
         */
        private void scheduleTimeout(TimeValue timeout) {
            this.cancellable = threadPool.schedule(this, timeout, ThreadPool.Names.GENERIC);
        }
    }

    /**
     * 描述某次请求的action 以及超时信息
     */
    static class TimeoutInfoHolder {

        /**
         * 本次请求的目标节点
         */
        private final DiscoveryNode node;
        /**
         * 本次发起的请求类型
         */
        private final String action;
        private final long sentTime;
        private final long timeoutTime;

        TimeoutInfoHolder(DiscoveryNode node, String action, long sentTime, long timeoutTime) {
            this.node = node;
            this.action = action;
            this.sentTime = sentTime;
            this.timeoutTime = timeoutTime;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String action() {
            return action;
        }

        public long sentTime() {
            return sentTime;
        }

        public long timeoutTime() {
            return timeoutTime;
        }
    }

    /**
     * This handler wrapper ensures that the response thread executes with the correct thread context. Before any of the handle methods
     * are invoked we restore the context.
     * 将resHandler 与上下文对象组合在一起
     */
    public static final class ContextRestoreResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        /**
         * 处理响应结果的对象
         */
        private final TransportResponseHandler<T> delegate;
        private final Supplier<ThreadContext.StoredContext> contextSupplier;
        /**
         * 当本次请求响应超时时 使用该对象处理
         */
        private volatile TimeoutHandler handler;

        public ContextRestoreResponseHandler(Supplier<ThreadContext.StoredContext> contextSupplier, TransportResponseHandler<T> delegate) {
            this.delegate = delegate;
            this.contextSupplier = contextSupplier;
        }

        @Override
        public T read(StreamInput in) throws IOException {
            return delegate.read(in);
        }

        @Override
        public void handleResponse(T response) {
            if(handler != null) {
                handler.cancel();
            }
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleResponse(response);
            }
        }

        @Override
        public void handleException(TransportException exp) {
            if(handler != null) {
                handler.cancel();
            }
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleException(exp);
            }
        }

        @Override
        public String executor() {
            return delegate.executor();
        }

        @Override
        public String toString() {
            return getClass().getName() + "/" + delegate.toString();
        }

        void setTimeoutHandler(TimeoutHandler handler) {
            this.handler = handler;
        }

    }

    /**
     * 模拟一条可以向对端发送结果的通道  实质上直接在本地处理
     */
    static class DirectResponseChannel implements TransportChannel {
        final DiscoveryNode localNode;
        private final String action;
        private final long requestId;
        final TransportService service;
        final ThreadPool threadPool;

        /**
         *
         * @param localNode  本地节点
         * @param action  本次请求的指令类型
         * @param requestId  请求id
         * @param service   生成该channel的服务对象
         * @param threadPool
         */
        DirectResponseChannel(DiscoveryNode localNode, String action, long requestId, TransportService service, ThreadPool threadPool) {
            this.localNode = localNode;
            this.action = action;
            this.requestId = requestId;
            this.service = service;
            this.threadPool = threadPool;
        }

        @Override
        public String getProfileName() {
            return DIRECT_RESPONSE_PROFILE;
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            service.onResponseSent(requestId, action, response);
            final TransportResponseHandler handler = service.responseHandlers.onResponseReceived(requestId, service);
            // ignore if its null, the service logs it
            if (handler != null) {
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processResponse(handler, response);
                } else {
                    threadPool.executor(executor).execute(new Runnable() {
                        @Override
                        public void run() {
                            processResponse(handler, response);
                        }

                        @Override
                        public String toString() {
                            return "delivery of response to [" + requestId + "][" + action + "]: " + response;
                        }
                    });
                }
            }
        }

        @SuppressWarnings("unchecked")
        protected void processResponse(TransportResponseHandler handler, TransportResponse response) {
            try {
                handler.handleResponse(response);
            } catch (Exception e) {
                processException(handler, wrapInRemote(new ResponseHandlerFailureTransportException(e)));
            }
        }

        @Override
        public void sendResponse(Exception exception) throws IOException {
            service.onResponseSent(requestId, action, exception);
            final TransportResponseHandler handler = service.responseHandlers.onResponseReceived(requestId, service);
            // ignore if its null, the service logs it
            if (handler != null) {
                // 将本地异常模拟成一个远端异常
                final RemoteTransportException rtx = wrapInRemote(exception);
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processException(handler, rtx);
                } else {
                    threadPool.executor(handler.executor()).execute(new Runnable() {
                        @Override
                        public void run() {
                            processException(handler, rtx);
                        }

                        @Override
                        public String toString() {
                            return "delivery of failure response to [" + requestId + "][" + action + "]: " + exception;
                        }
                    });
                }
            }
        }

        protected RemoteTransportException wrapInRemote(Exception e) {
            if (e instanceof RemoteTransportException) {
                return (RemoteTransportException) e;
            }
            return new RemoteTransportException(localNode.getName(), localNode.getAddress(), action, e);
        }

        protected void processException(final TransportResponseHandler handler, final RemoteTransportException rtx) {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error(
                    () -> new ParameterizedMessage(
                        "failed to handle exception for action [{}], handler [{}]", action, handler), e);
            }
        }

        @Override
        public String getChannelType() {
            return "direct";
        }

        @Override
        public Version getVersion() {
            return localNode.getVersion();
        }
    }


    /**
     * Returns the internal thread pool
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    private boolean isLocalNode(DiscoveryNode discoveryNode) {
        return Objects.requireNonNull(discoveryNode, "discovery node must not be null").equals(localNode);
    }

    /**
     * 使用代理模式完成任务
     */
    private static final class DelegatingTransportMessageListener implements TransportMessageListener {

        /**
         * 内部隐藏了一组监听器
         */
        private final List<TransportMessageListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onRequestReceived(long requestId, String action) {
            for (TransportMessageListener listener : listeners) {
                listener.onRequestReceived(requestId, action);
            }
        }

        @Override
        public void onResponseSent(long requestId, String action, TransportResponse response) {
            for (TransportMessageListener listener : listeners) {
                listener.onResponseSent(requestId, action, response);
            }
        }

        @Override
        public void onResponseSent(long requestId, String action, Exception error) {
            for (TransportMessageListener listener : listeners) {
                listener.onResponseSent(requestId, action, error);
            }
        }

        @Override
        public void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request,
                                  TransportRequestOptions finalOptions) {
            for (TransportMessageListener listener : listeners) {
                listener.onRequestSent(node, requestId, action, request, finalOptions);
            }
        }

        @Override
        public void onResponseReceived(long requestId, Transport.ResponseContext holder) {
            for (TransportMessageListener listener : listeners) {
                listener.onResponseReceived(requestId, holder);
            }
        }
    }
}
