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

package org.elasticsearch.discovery;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.coordination.PeersResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * 该对象是用于探测集群中某节点的状态的
 */
public abstract class PeerFinder {

    private static final Logger logger = LogManager.getLogger(PeerFinder.class);

    public static final String REQUEST_PEERS_ACTION_NAME = "internal:discovery/request_peers";

    // the time between attempts to find all peers
    public static final Setting<TimeValue> DISCOVERY_FIND_PEERS_INTERVAL_SETTING =
        Setting.timeSetting("discovery.find_peers_interval",
            TimeValue.timeValueMillis(1000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    public static final Setting<TimeValue> DISCOVERY_REQUEST_PEERS_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.request_peers_timeout",
            TimeValue.timeValueMillis(3000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    /**
     * 每隔多少时间查询一次其他节点
     */
    private final TimeValue findPeersInterval;
    private final TimeValue requestPeersTimeout;

    private final Object mutex = new Object();
    private final TransportService transportService;

    /**
     * 该节点单独将连接到 master节点的逻辑抽取出来
     */
    private final TransportAddressConnector transportAddressConnector;

    /**
     * 解析域名对象
     */
    private final ConfiguredHostsResolver configuredHostsResolver;

    /**
     * 当前节点感知到的任期  如果与其他节点的任期发生了同步 也要对该属性进行同步
     */
    private volatile long currentTerm;

    /**
     * 该组件此时暂停使用
     */
    private boolean active;

    /**
     * 最近一次集群节点的信息
     * 该数据本身存在滞后性 所以当其他节点如果能感知到不存在于 lastAcceptedNodes 的节点 需要同时到本节点 并尝试建立连接
     */
    private DiscoveryNodes lastAcceptedNodes;

    /**
     * 存储集群中除了本节点外的其他 master节点
     */
    private final Map<TransportAddress, Peer> peersByAddress = new LinkedHashMap<>();

    /**
     * 集群中的 leader节点
     * 当触发 active时 代表需要检测其他节点中记录的leader信息 进而判断是leader掉线
     * 还是连接被断开 如果掉线 需要重新选举 如果仅是连接假死 只要当前节点重新建立与leader的连接即可
     */
    private Optional<DiscoveryNode> leader = Optional.empty();
    private volatile List<TransportAddress> lastResolvedAddresses = emptyList();

    public PeerFinder(Settings settings, TransportService transportService, TransportAddressConnector transportAddressConnector,
                      ConfiguredHostsResolver configuredHostsResolver) {
        findPeersInterval = DISCOVERY_FIND_PEERS_INTERVAL_SETTING.get(settings);
        requestPeersTimeout = DISCOVERY_REQUEST_PEERS_TIMEOUT_SETTING.get(settings);
        this.transportService = transportService;
        this.transportAddressConnector = transportAddressConnector;
        this.configuredHostsResolver = configuredHostsResolver;

        transportService.registerRequestHandler(REQUEST_PEERS_ACTION_NAME, Names.GENERIC, false, false,
            PeersRequest::new,
            (request, channel, task) -> channel.sendResponse(handlePeersRequest(request)));
    }

    /**
     * 参选的节点通过该对象检测其他节点的状态
     * @param lastAcceptedNodes 最近一次集群中存在的所有节点
     */
    public void activate(final DiscoveryNodes lastAcceptedNodes) {
        logger.trace("activating with {}", lastAcceptedNodes);

        synchronized (mutex) {
            assert assertInactiveWithNoKnownPeers();
            // 首先激活这个组件
            active = true;
            this.lastAcceptedNodes = lastAcceptedNodes;
            leader = Optional.empty();
            // 与此时集群中其他节点建立连接  注意是异步的 此时无法确保是否有连接真正完成
            handleWakeUp(); // return value discarded: there are no known peers, so none can be disconnected
        }

        onFoundPeersUpdated(); // trigger a check for a quorum already
    }

    /**
     * 这里的意思是既然已经知道了leader节点 那么直接让请求端访问leader就好  必须要再进入preVote阶段了
     * 并且本节点也不需要继续探测外部的节点了
     * @param leader
     */
    public void deactivate(DiscoveryNode leader) {
        final boolean peersRemoved;
        synchronized (mutex) {
            logger.trace("deactivating and setting leader to {}", leader);
            active = false;
            // 因为active被设置成false 所以所有node的wakeup方法都会返回true
            peersRemoved = handleWakeUp();
            this.leader = Optional.of(leader);
            assert assertInactiveWithNoKnownPeers();
        }
        // 一般来说必然会触发该方法
        if (peersRemoved) {
            onFoundPeersUpdated();
        }
    }

    // exposed to subclasses for testing
    protected final boolean holdsLock() {
        return Thread.holdsLock(mutex);
    }

    private boolean assertInactiveWithNoKnownPeers() {
        assert holdsLock() : "PeerFinder mutex not held";
        assert active == false;
        assert peersByAddress.isEmpty() : peersByAddress.keySet();
        return true;
    }

    /**
     * 当收到其他节点的探测请求时 将本节点认为的集群最新的节点快照返回
     * @param peersRequest
     * @return
     */
    PeersResponse handlePeersRequest(PeersRequest peersRequest) {
        synchronized (mutex) {
            assert peersRequest.getSourceNode().equals(getLocalNode()) == false;
            final List<DiscoveryNode> knownPeers;
            // 只要leader节点还未确认  finder就还处于激活状态
            if (active) {
                assert leader.isPresent() == false : leader;
                // 建立与对端节点的连接
                if (peersRequest.getSourceNode().isMasterNode()) {
                    // 这里就是将 地址包装成 Peer对象 并存储到容器中
                    startProbe(peersRequest.getSourceNode().getAddress());
                }

                // 这样集群中所有节点都会尝试连接 不过此时新旧集群的节点会混在一起
                peersRequest.getKnownPeers().stream().map(DiscoveryNode::getAddress).forEach(this::startProbe);

                // 将本对象此时能感知到的所有masterNode返回
                knownPeers = getFoundPeersUnderLock();
            } else {
                // 失活代表已经确定leader节点了 不需要继续探测
                assert leader.isPresent() || lastAcceptedNodes == null;
                knownPeers = emptyList();
            }
            // 在这个结果中  leader 和 knownPeers是不可能同时设置的  设置了leader就代表这个节点有认同的leader 否则就返回能参与选举的所有node
            return new PeersResponse(leader, knownPeers, currentTerm);
        }
    }

    // exposed for checking invariant in o.e.c.c.Coordinator (public since this is a different package)
    public Optional<DiscoveryNode> getLeader() {
        synchronized (mutex) {
            return leader;
        }
    }

    // exposed for checking invariant in o.e.c.c.Coordinator (public since this is a different package)
    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    private DiscoveryNode getLocalNode() {
        final DiscoveryNode localNode = transportService.getLocalNode();
        assert localNode != null;
        return localNode;
    }

    /**
     * Invoked on receipt of a PeersResponse from a node that believes it's an active leader, which this node should therefore try and join.
     * Note that invocations of this method are not synchronised. By the time it is called we may have been deactivated.
     */
    protected abstract void onActiveMasterFound(DiscoveryNode masterNode, long term);

    /**
     * Invoked when the set of found peers changes. Note that invocations of this method are not fully synchronised, so we only guarantee
     * that the change to the set of found peers happens before this method is invoked. If there are multiple concurrent changes then there
     * will be multiple concurrent invocations of this method, with no guarantee as to their order. For this reason we do not pass the
     * updated set of peers as an argument to this method, leaving it to the implementation to call getFoundPeers() with appropriate
     * synchronisation to avoid lost updates. Also, by the time this method is invoked we may have been deactivated.
     */
    protected abstract void onFoundPeersUpdated();

    public List<TransportAddress> getLastResolvedAddresses() {
        return lastResolvedAddresses;
    }

    public interface TransportAddressConnector {
        /**
         * Identify the node at the given address and, if it is a master node and not the local node then establish a full connection to it.
         */
        void connectToRemoteMasterNode(TransportAddress transportAddress, ActionListener<DiscoveryNode> listener);
    }

    public interface ConfiguredHostsResolver {
        /**
         * Attempt to resolve the configured unicast hosts list to a list of transport addresses.
         *
         * @param consumer Consumer for the resolved list. May not be called if an error occurs or if another resolution attempt is in
         *                 progress.
         */
        void resolveConfiguredHosts(Consumer<List<TransportAddress>> consumer);
    }

    public Iterable<DiscoveryNode> getFoundPeers() {
        synchronized (mutex) {
            return getFoundPeersUnderLock();
        }
    }

    /**
     * 返回至少成功连接过一次的node  包含之后断开连接的
     * @return
     */
    private List<DiscoveryNode> getFoundPeersUnderLock() {
        assert holdsLock() : "PeerFinder mutex not held";
        return peersByAddress.values().stream()
            .map(Peer::getDiscoveryNode).filter(Objects::nonNull).distinct().collect(Collectors.toList());
    }

    /**
     * 将某个节点对应的地址 包装成 peer对象  并建立连接
     * @param transportAddress
     * @return
     */
    private Peer createConnectingPeer(TransportAddress transportAddress) {
        Peer peer = new Peer(transportAddress);
        peer.establishConnection();
        return peer;
    }

    /**
     * 因为本节点之前持久化的 clusterState 不一定是最新的 所以其他能够观测到的节点上携带的集群信息也要参考
     * @return whether any peers were removed due to disconnection
     */
    private boolean handleWakeUp() {
        assert holdsLock() : "PeerFinder mutex not held";

        // 与某个地址对应的node 断开了连接
        final boolean peersRemoved = peersByAddress.values().removeIf(Peer::handleWakeUp);

        // 当前节点已经确定leader的情况下就可以 停止探测外部节点了
        if (active == false) {
            logger.trace("not active");
            return peersRemoved;
        }


        logger.trace("probing master nodes from cluster state: {}", lastAcceptedNodes);
        // 这里只探测master节点  因为只有master节点参与选举
        // 这里已经在与新的地址建立连接了  注意是异步的
        for (ObjectCursor<DiscoveryNode> discoveryNodeObjectCursor : lastAcceptedNodes.getMasterNodes().values()) {
            startProbe(discoveryNodeObjectCursor.value.getAddress());
        }

        // 如果从其他途径获取了一些地址信息 也通过 startProbe 加入到 peerByAddress中
        configuredHostsResolver.resolveConfiguredHosts(providedAddresses -> {
            synchronized (mutex) {
                lastResolvedAddresses = providedAddresses;
                logger.trace("probing resolved transport addresses {}", providedAddresses);
                providedAddresses.forEach(this::startProbe);
            }
        });

        // 启动定时任务
        transportService.getThreadPool().scheduleUnlessShuttingDown(findPeersInterval, Names.GENERIC, new AbstractRunnable() {
            @Override
            public boolean isForceExecution() {
                return true;
            }

            @Override
            public void onFailure(Exception e) {
                assert false : e;
                logger.debug("unexpected exception in wakeup", e);
            }

            @Override
            protected void doRun() {
                synchronized (mutex) {
                    // 如果本对象长时间未建立连接 不需要处理 或者此时确认了leader 节点
                    if (handleWakeUp() == false) {
                        return;
                    }
                }
                // 在至少有某个node 的连接断开时 触发该方法
                // 下次的检测任务依旧会执行
                onFoundPeersUpdated();
            }

            @Override
            public String toString() {
                return "PeerFinder handling wakeup";
            }
        });

        return peersRemoved;
    }

    /**
     * 向某个节点发起探测
     * @param transportAddress  发起 peersRequest的节点
     */
    protected void startProbe(TransportAddress transportAddress) {
        assert holdsLock() : "PeerFinder mutex not held";
        if (active == false) {
            logger.trace("startProbe({}) not running", transportAddress);
            return;
        }

        // 如果探测的节点地址与本节点一致 就不需要探测了 本次是一个无意义的操作
        if (transportAddress.equals(getLocalNode().getAddress())) {
            logger.trace("startProbe({}) not probing local node", transportAddress);
            return;
        }

        // key 对应需要探测的某个地址   value 就是建立于某个地址的连接后创建的 peer对象
        peersByAddress.computeIfAbsent(transportAddress, this::createConnectingPeer);
    }

    /**
     * 将某个master节点包装成Peer对象
     */
    private class Peer {

        /**
         * 对应探测的节点的地址
         */
        private final TransportAddress transportAddress;

        /**
         * 如果设置了这个值 就代表目标节点此时是有效的 并且是master节点
         */
        private SetOnce<DiscoveryNode> discoveryNode = new SetOnce<>();

        /**
         * 代表此时有一个飞行中的 探测请求
         */
        private volatile boolean peersRequestInFlight;

        Peer(TransportAddress transportAddress) {
            this.transportAddress = transportAddress;
        }

        @Nullable
        DiscoveryNode getDiscoveryNode() {
            return discoveryNode.get();
        }

        /**
         * 该方法本身是周期性触发 检测是否有需要关闭的连接
         * @return 返回true
         */
        boolean handleWakeUp() {
            assert holdsLock() : "PeerFinder mutex not held";

            // 此时已经确定了集群中的leader节点 不需要保持连接了
            if (active == false) {
                return true;
            }

            final DiscoveryNode discoveryNode = getDiscoveryNode();
            // may be null if connection not yet established

            // 此时已经连接到目标节点了
            if (discoveryNode != null) {
                // 建立连接本身是一个异步过程  当连接建立完毕后会设置到manager中
                if (transportService.nodeConnected(discoveryNode)) {
                    if (peersRequestInFlight == false) {
                        requestPeers();
                    }
                    // 代表长时间没有完成连接 放弃该节点的探测
                } else {
                    // 代表连接断开了
                    logger.trace("{} no longer connected", this);
                    return true;
                }
            }

            // 首次发起建立连接 必然是返回false
            return false;
        }

        /**
         * 建立与目标节点的连接
         * 通过各种途径获取需要探测的地址后 会先调用该方法 与目标地址建立连接
         */
        void establishConnection() {
            assert holdsLock() : "PeerFinder mutex not held";
            assert getDiscoveryNode() == null : "unexpectedly connected to " + getDiscoveryNode();
            assert active;

            logger.trace("{} attempting connection", this);

            // 只有当目标节点确实是master节点时才会触发 onResponse  主要是为了避免本地clusterState信息已经过期 比如A节点之前是masterNode 后来不是了 所以要进行握手校验
            transportAddressConnector.connectToRemoteMasterNode(transportAddress, new ActionListener<DiscoveryNode>() {

                /**
                 * 只有当握手成功后才触发 onResponse
                 * @param remoteNode
                 */
                @Override
                public void onResponse(DiscoveryNode remoteNode) {
                    assert remoteNode.isMasterNode() : remoteNode + " is not master-eligible";
                    assert remoteNode.equals(getLocalNode()) == false : remoteNode + " is the local node";
                    synchronized (mutex) {
                        if (active == false) {
                            return;
                        }

                        assert discoveryNode.get() == null : "discoveryNode unexpectedly already set to " + discoveryNode.get();
                        discoveryNode.set(remoteNode);
                        // 探测对端节点 此时认为的集群节点  同时将自身认为的集群节点发送过去 本身是异步操作
                        requestPeers();
                    }

                    assert holdsLock() == false : "PeerFinder mutex is held in error";
                    // 每当与某个node的连接建立时 也会触发该方法
                    onFoundPeersUpdated();
                }

                /**
                 * 因为这个节点不是能够参与选举的节点 或者与该节点的连接失败   所以从peersByAddress 中移除  代表不再参考这个节点的leader信息
                 * @param e
                 */
                @Override
                public void onFailure(Exception e) {
                    logger.debug(() -> new ParameterizedMessage("{} connection failed", Peer.this), e);
                    synchronized (mutex) {
                        peersByAddress.remove(transportAddress);
                    }
                }
            });
        }

        /**
         * 此时已经连接到 其他master节点了 需要从该节点获取其他节点信息
         */
        private void requestPeers() {
            assert holdsLock() : "PeerFinder mutex not held";
            assert peersRequestInFlight == false : "PeersRequest already in flight";
            assert active;

            final DiscoveryNode discoveryNode = getDiscoveryNode();
            assert discoveryNode != null : "cannot request peers without first connecting";

            if (discoveryNode.equals(getLocalNode())) {
                logger.trace("{} not requesting peers from local node", this);
                return;
            }

            logger.trace("{} requesting peers", this);
            peersRequestInFlight = true;

            // 此时集群中所有已知的 master节点   因为不同节点能感知到的master节点不一定相同
            // 某些节点可能是从离线状态恢复的 那么集群快照就是旧的  这时数据就会出现不一致的情况
            final List<DiscoveryNode> knownNodes = getFoundPeersUnderLock();

            // 其他节点感知到本节点认为的集群中节点 并进行处理后 会将它认为的集群参选节点返回
            final TransportResponseHandler<PeersResponse> peersResponseHandler = new TransportResponseHandler<PeersResponse>() {

                @Override
                public PeersResponse read(StreamInput in) throws IOException {
                    return new PeersResponse(in);
                }

                /**
                 * 处理从其他节点返回的peers结果
                 * @param response
                 */
                @Override
                public void handleResponse(PeersResponse response) {
                    logger.trace("{} received {}", Peer.this, response);
                    synchronized (mutex) {
                        // 本节点已经确认leader节点了 不需要再处理其他探测结果
                        if (active == false) {
                            return;
                        }

                        peersRequestInFlight = false;

                        // 如果存在leader 就与leader连接 如果存在普通节点就连接普通节点
                        response.getMasterNode().map(DiscoveryNode::getAddress).ifPresent(PeerFinder.this::startProbe);
                        response.getKnownPeers().stream().map(DiscoveryNode::getAddress).forEach(PeerFinder.this::startProbe);
                    }

                    // 认为目标节点本身就是leader节点   当连接到leader节点后 必然会触发该逻辑 代表已经与集群中的leader节点完成连接了
                    if (response.getMasterNode().equals(Optional.of(discoveryNode))) {
                        // Must not hold lock here to avoid deadlock
                        assert holdsLock() == false : "PeerFinder mutex is held in error";
                        onActiveMasterFound(discoveryNode, response.getTerm());
                    }
                }

                /**
                 * 当探测某个节点失败时 只能打印日志 因为没有解决办法
                 * @param exp
                 */
                @Override
                public void handleException(TransportException exp) {
                    peersRequestInFlight = false;
                    logger.debug(new ParameterizedMessage("{} peers request failed", Peer.this), exp);
                }

                @Override
                public String executor() {
                    return Names.GENERIC;
                }
            };

            // 到了这里 开始发送探测请求了  同时将自身记录的集群节点发送过去
            transportService.sendRequest(discoveryNode, REQUEST_PEERS_ACTION_NAME,
                new PeersRequest(getLocalNode(), knownNodes),
                TransportRequestOptions.builder().withTimeout(requestPeersTimeout).build(),
                peersResponseHandler);
        }

        @Override
        public String toString() {
            return "Peer{" +
                "transportAddress=" + transportAddress +
                ", discoveryNode=" + discoveryNode.get() +
                ", peersRequestInFlight=" + peersRequestInFlight +
                '}';
        }
    }
}
