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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.internal.io.IOUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class manages node connections within a cluster. The connection is opened by the underlying transport.
 * Once the connection is opened, this class manages the connection. This includes closing the connection when
 * the connection manager is closed.
 * 管理当前集群中与其他节点的所有连接
 * 被 TransportService持有
 */
public class ClusterConnectionManager implements ConnectionManager {

    private static final Logger logger = LogManager.getLogger(ClusterConnectionManager.class);

    /**
     * key 代表本节点连接的目标节点
     * Connection代表与该节点的连接  在底层是由多条channel组合成的  每次发送请求时会随机选择一个channel发送
     * 当NodeConnectionsService建立了与某个node的连接后就会存储到这个容器中
     */
    private final ConcurrentMap<DiscoveryNode, Transport.Connection> connectedNodes = ConcurrentCollections.newConcurrentMap();

    /**
     * 代表此时还在连接中  同时future对象上还挂载了一组监听器
     */
    private final ConcurrentMap<DiscoveryNode, ListenableFuture<Void>> pendingConnections = ConcurrentCollections.newConcurrentMap();

    /**
     * 当引用计数为0时 用户定义关闭逻辑  这里就是将所有连接关闭
     */
    private final AbstractRefCounted connectingRefCounter = new AbstractRefCounted("connection manager") {
        @Override
        protected void closeInternal() {
            Iterator<Map.Entry<DiscoveryNode, Transport.Connection>> iterator = connectedNodes.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<DiscoveryNode, Transport.Connection> next = iterator.next();
                try {
                    IOUtils.closeWhileHandlingException(next.getValue());
                } finally {
                    iterator.remove();
                }
            }
            closeLatch.countDown();
        }
    };

    /**
     * 传输层是一套网络框架 实现节点间发送/读取消息  建立新连接等能力  (包含channel的心跳检测 还有握手请求)
     */
    private final Transport transport;
    /**
     * 描述每种类型各有多少连接数  在发起请求时 会轮询不同的channel
     */
    private final ConnectionProfile defaultProfile;
    private final AtomicBoolean closing = new AtomicBoolean(false);

    /**
     * 关闭该对象时使用的锁
     */
    private final CountDownLatch closeLatch = new CountDownLatch(1);

    /**
     * 主要是统一管理所有监听器 这组监听器专门负责监听连接的创建和关闭
     */
    private final DelegatingNodeConnectionListener connectionListener = new DelegatingNodeConnectionListener();

    public ClusterConnectionManager(Settings settings, Transport transport) {
        this(ConnectionProfile.buildDefaultConnectionProfile(settings), transport);
    }

    /**
     * @param connectionProfile
     * @param transport
     */
    public ClusterConnectionManager(ConnectionProfile connectionProfile, Transport transport) {
        this.transport = transport;
        this.defaultProfile = connectionProfile;
    }

    @Override
    public void addListener(TransportConnectionListener listener) {
        this.connectionListener.addListener(listener);
    }

    @Override
    public void removeListener(TransportConnectionListener listener) {
        this.connectionListener.removeListener(listener);
    }

    /**
     * 开启一条通往指定node 的连接
     * @param node
     * @param connectionProfile  使用传入的profile信息去填充默认信息  并使用新的信息去创建连接
     * @param listener
     */
    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile connectionProfile, ActionListener<Transport.Connection> listener) {
        // 某些未设置的信息 使用default中的数据
        ConnectionProfile resolvedProfile = ConnectionProfile.resolveConnectionProfile(connectionProfile, defaultProfile);
        internalOpenConnection(node, resolvedProfile, listener);
    }

    /**
     * Connects to a node with the given connection profile. If the node is already connected this method has no effect.
     * Once a successful is established, it can be validated before being exposed.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     * 创建通往指定node的连接  这里针对单个node 仅允许存在一条连接
     * 这里触发的钩子以node为级别 同时对应某个node的连接已经存在时 无法继续添加连接
     */
    @Override
    public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                              ConnectionValidator connectionValidator,
                              ActionListener<Void> listener) throws ConnectTransportException {
        ConnectionProfile resolvedProfile = ConnectionProfile.resolveConnectionProfile(connectionProfile, defaultProfile);
        if (node == null) {
            listener.onFailure(new ConnectTransportException(null, "can't connect to a null node"));
            return;
        }

        // 引用计数默认为1 当数值为0 时 代表manager被关闭 此时该值返回false
        if (connectingRefCounter.tryIncRef() == false) {
            listener.onFailure(new IllegalStateException("connection manager is closed"));
            return;
        }

        if (connectedNodes.containsKey(node)) {
            // 如果此时连接已经存在 对冲上面的增加操作
            connectingRefCounter.decRef();
            listener.onResponse(null);
            return;
        }

        final ListenableFuture<Void> currentListener = new ListenableFuture<>();
        final ListenableFuture<Void> existingListener = pendingConnections.putIfAbsent(node, currentListener);
        // 追加新的监听器
        if (existingListener != null) {
            try {
                // wait on previous entry to complete connection attempt
                existingListener.addListener(listener, EsExecutors.newDirectExecutorService());
            } finally {
                // 因为针对该节点已经有监听器就代表已经发起了连接动作  通过修改引用计数对冲之前的影响
                connectingRefCounter.decRef();
            }
            return;
        }

        currentListener.addListener(listener, EsExecutors.newDirectExecutorService());

        // 当任务执行完毕时 恢复引用计数 这里使用引用计数的目的是为了避免还有正在尝试连接的节点 程序却被直接停止
        final RunOnce releaseOnce = new RunOnce(connectingRefCounter::decRef);

        // 连接本身是委派给 transport对象的
        internalOpenConnection(node, resolvedProfile,
            // 处理连接结果的对象
            ActionListener.wrap(conn -> {
            connectionValidator.validate(conn, resolvedProfile,
                // 校验完成后继续流程
                ActionListener.wrap(
                ignored -> {
                    assert Transports.assertNotTransportThread("connection validator success");
                    try {
                        // 针对该方法的调用 不允许为某个node创建多个连接
                        if (connectedNodes.putIfAbsent(node, conn) != null) {
                            logger.debug("existing connection to node [{}], closing new redundant connection", node);
                            IOUtils.closeWhileHandlingException(conn);
                        } else {
                            logger.debug("connected to node [{}]", node);
                            try {
                                // 注意这个钩子和 internalOpenConnection 中触发的钩子是不一样的  先触发 onConnectionOpened 后触发 onNodeConnected
                                connectionListener.onNodeConnected(node, conn);
                            } finally {
                                final Transport.Connection finalConnection = conn;
                                conn.addCloseListener(ActionListener.wrap(() -> {
                                    logger.trace("unregistering {} after connection close and marking as disconnected", node);
                                    connectedNodes.remove(node, finalConnection);
                                    // 在 internalOpenConnection 对应的钩子为  onConnectionClosed
                                    connectionListener.onNodeDisconnected(node, conn);
                                }));
                            }
                        }
                    } finally {
                        // 在处理完后唤醒等待连接结果的对象
                        ListenableFuture<Void> future = pendingConnections.remove(node);
                        assert future == currentListener : "Listener in pending map is different than the expected listener";
                        releaseOnce.run();
                        future.onResponse(null);
                    }
                }, e -> {
                    assert Transports.assertNotTransportThread("connection validator failure");
                    IOUtils.closeWhileHandlingException(conn);
                    failConnectionListeners(node, releaseOnce, e, currentListener);
                }));
        }, e -> {
            assert Transports.assertNotTransportThread("internalOpenConnection failure");
            failConnectionListeners(node, releaseOnce, e, currentListener);
        }));
    }

    /**
     * Returns a connection for the given node if the node is connected.
     * Connections returned from this method must not be closed. The lifecycle of this connection is
     * maintained by this connection manager
     *
     * @throws NodeNotConnectedException if the node is not connected
     * @see #connectToNode(DiscoveryNode, ConnectionProfile, ConnectionValidator, ActionListener)
     */
    @Override
    public Transport.Connection getConnection(DiscoveryNode node) {
        Transport.Connection connection = connectedNodes.get(node);
        if (connection == null) {
            throw new NodeNotConnectedException(node, "Node not connected");
        }
        return connection;
    }

    /**
     * Returns {@code true} if the node is connected.
     */
    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return connectedNodes.containsKey(node);
    }

    /**
     * Disconnected from the given node, if not connected, will do nothing.
     * 断开与某个节点的连接 就是切断所有的channel  这样会从selector上注销channel
     */
    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        Transport.Connection nodeChannels = connectedNodes.remove(node);
        if (nodeChannels != null) {
            // if we found it and removed it we close
            nodeChannels.close();
        }
    }

    /**
     * Returns the number of nodes this manager is connected to.
     */
    @Override
    public int size() {
        return connectedNodes.size();
    }

    @Override
    public Set<DiscoveryNode> getAllConnectedNodes() {
        return Collections.unmodifiableSet(connectedNodes.keySet());
    }

    @Override
    public void close() {
        internalClose(true);
    }

    @Override
    public void closeNoBlock() {
        internalClose(false);
    }

    private void internalClose(boolean waitForPendingConnections) {
        assert Transports.assertNotTransportThread("Closing ConnectionManager");
        if (closing.compareAndSet(false, true)) {
            connectingRefCounter.decRef();
            if (waitForPendingConnections) {
                try {
                    // 阻塞直到所有连接完成 此时引用计数为0 触发关闭函数 同时当引用计数为0时 无法创建新的连接
                    closeLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    /**
     * 创建连接的实现   这里针对的钩子是connection级别
     * @param node
     * @param connectionProfile
     * @param listener
     */
    private void internalOpenConnection(DiscoveryNode node, ConnectionProfile connectionProfile,
                                        ActionListener<Transport.Connection> listener) {
        // 通过transport创建连接
        transport.openConnection(node, connectionProfile, ActionListener.map(listener,
            // 在触发listener之前会先走下面的逻辑
            connection -> {
            assert Transports.assertNotTransportThread("internalOpenConnection success");
            try {
                connectionListener.onConnectionOpened(connection);
            } finally {
                // 添加一个监听连接关闭的监听器
                connection.addCloseListener(ActionListener.wrap(() -> connectionListener.onConnectionClosed(connection)));
            }
            if (connection.isClosed()) {
                throw new ConnectTransportException(node, "a channel closed while connecting");
            }
            return connection;
        }));
    }

    private void failConnectionListeners(DiscoveryNode node, RunOnce releaseOnce, Exception e, ListenableFuture<Void> expectedListener) {
        ListenableFuture<Void> future = pendingConnections.remove(node);
        releaseOnce.run();
        if (future != null) {
            assert future == expectedListener : "Listener in pending map is different than the expected listener";
            future.onFailure(e);
        }
    }

    @Override
    public ConnectionProfile getConnectionProfile() {
        return defaultProfile;
    }

}
