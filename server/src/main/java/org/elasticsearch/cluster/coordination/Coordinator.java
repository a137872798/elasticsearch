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
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalClusterUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper.ClusterFormationState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.coordination.FollowersChecker.FollowerCheckRequest;
import org.elasticsearch.cluster.coordination.JoinHelper.InitialJoinAccumulator;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplier.ClusterApplyListener;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.HandshakingTransportAddressConnector;
import org.elasticsearch.discovery.PeerFinder;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.discovery.SeedHostsResolver;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_ID;
import static org.elasticsearch.gateway.ClusterStateUpdaters.hideStateIfNotRecovered;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

/**
 * 整个集群的协调者 内部应该使用了某种一致性算法
 * 并且应该是只有role为master的节点才会参与   跟leader不是一个概念 (leader是这些master节点通过一致性算法后选举出来的)
 * 如果要实现线性一致性 应该有一个与 leader节点同步数据的过程
 */
public class Coordinator extends AbstractLifecycleComponent implements Discovery {

    private static final Logger logger = LogManager.getLogger(Coordinator.class);

    // the timeout before emitting an info log about a slow-running publication
    public static final Setting<TimeValue> PUBLISH_INFO_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.publish.info_timeout",
            TimeValue.timeValueMillis(10000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    // the timeout for the publication of each value
    // 将统一后的集群信息发布到其他节点的超时时间
    public static final Setting<TimeValue> PUBLISH_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.publish.timeout",
            TimeValue.timeValueMillis(30000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final Settings settings;

    /**
     * 代表集群中只有一个节点
     */
    private final boolean singleNodeDiscovery;
    /**
     * 使用的选举策略
     */
    private final ElectionStrategy electionStrategy;
    /**
     * 传输层服务 通过该对象可以在节点之间建立连接 以及发送数据包
     */
    private final TransportService transportService;

    /**
     * MasterService 是要当前节点作为master时才可以使用
     * 主要功能就是接收update请求更新集群状态 并将更新事件通知到集群中所有节点
     */
    private final MasterService masterService;

    /**
     * 该对象负责为分片分配 节点 还包含根据节点负载重分配分片等能力
     */
    private final AllocationService allocationService;

    /**
     * 该对象负责处理加入集群的逻辑
     */
    private final JoinHelper joinHelper;
    /**
     * 处理节点从集群移除的请求
     */
    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;

    /**
     * TODO 从实现类中可以看到 LUCENE???
     */
    private final Supplier<CoordinationState.PersistedState> persistedStateSupplier;

    /**
     * 针对非master节点 采用的阻塞策略
     */
    private final NoMasterBlockService noMasterBlockService;
    final Object mutex = new Object(); // package-private to allow tests to call methods that assert that the mutex is held

    /**
     * 内部存储了当前节点 选举策略 以及持久化状态
     */
    private final SetOnce<CoordinationState> coordinationState = new SetOnce<>(); // initialized on start-up (see doStart)

    /**
     * 当前集群状态   在该对象重启时 仅包含localNode
     */
    private volatile ClusterState applierState; // the state that should be exposed to the cluster state applier

    /**
     * 什么玩意???  可以往其他节点发请求 其他节点会将集群此时所有已知的节点地址返回
     */
    private final PeerFinder peerFinder;

    /**
     * 该对象负责往目标节点发送preVote请求
     */
    private final PreVoteCollector preVoteCollector;

    /**
     * 生成选举的浮动时间
     */
    private final Random random;

    /**
     * 该对象提供了创建选举触发器实例的逻辑
     * 每经过多少时间 就要在集群内发起一次新的选举
     */
    private final ElectionSchedulerFactory electionSchedulerFactory;

    /**
     * 主机地址解析器
     */
    private final SeedHostsResolver configuredHostsResolver;
    private final TimeValue publishTimeout;
    private final TimeValue publishInfoTimeout;

    /**
     * 该对象处理有关发布/提交的请求
     */
    private final PublicationTransportHandler publicationHandler;

    /**
     * 该对象检查当前节点是否还是leader 节点
     */
    private final LeaderChecker leaderChecker;

    /**
     * 定期检查 follower对象
     */
    private final FollowersChecker followersChecker;

    /**
     * 该对象会感知集群的状态变化
     */
    private final ClusterApplier clusterApplier;
    private final Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators;
    @Nullable
    private Releasable electionScheduler;
    @Nullable
    private Releasable prevotingRound;

    /**
     * 某次选举中在预投票过程中接受到的最大term
     */
    private long maxTermSeen;

    /**
     * 这个是更新配置对象么
     */
    private final Reconfigurator reconfigurator;

    /**
     * 集群引导服务
     */
    private final ClusterBootstrapService clusterBootstrapService;

    /**
     * 滞后探测器
     */
    private final LagDetector lagDetector;
    /**
     * 将失败信息格式化
     */
    private final ClusterFormationFailureHelper clusterFormationFailureHelper;

    private Mode mode;

    /**
     * 已知最新的leader节点
     */
    private Optional<DiscoveryNode> lastKnownLeader;
    /**
     * 最近一次发起的join请求 代表在集群中发现任期更新了 需要加入到最新的集群中
     * 或者是收到了 startJoin时 就会往对端发送join请求 同时设置该属性
     */
    private Optional<Join> lastJoin;

    /**
     * 该对象在初始化时对应 InitialJoinAccumulator 此时拒绝任何join请求
     */
    private JoinHelper.JoinAccumulator joinAccumulator;

    /**
     * 代表当前正在执行一个发布任务
     */
    private Optional<CoordinatorPublication> currentPublication = Optional.empty();

    /**
     * @param nodeName The name of the node, used to name the {@link java.util.concurrent.ExecutorService} of the {@link SeedHostsResolver}.
     * @param onJoinValidators A collection of join validators to restrict which nodes may join the cluster.
     *                         在构造函数中只是做了一些赋值操作
     */
    public Coordinator(String nodeName, Settings settings, ClusterSettings clusterSettings, TransportService transportService,
                       NamedWriteableRegistry namedWriteableRegistry, AllocationService allocationService, MasterService masterService,
                       Supplier<CoordinationState.PersistedState> persistedStateSupplier, SeedHostsProvider seedHostsProvider,
                       ClusterApplier clusterApplier, Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators, Random random,
                       RerouteService rerouteService, ElectionStrategy electionStrategy) {
        this.settings = settings;
        this.transportService = transportService;
        this.masterService = masterService;
        this.allocationService = allocationService;
        // 追加2个检测版本是否兼容的钩子
        this.onJoinValidators = JoinTaskExecutor.addBuiltInJoinValidators(onJoinValidators);

        // 代表集群中只有一个节点
        this.singleNodeDiscovery = DiscoveryModule.isSingleNodeDiscovery(settings);
        this.electionStrategy = electionStrategy;

        // 生成处理join流程相关的组件
        this.joinHelper = new JoinHelper(settings, allocationService, masterService, transportService,
            this::getCurrentTerm, this::getStateForMasterService, this::handleJoinRequest, this::joinLeaderInTerm, this.onJoinValidators,
            rerouteService);
        this.persistedStateSupplier = persistedStateSupplier;
        this.noMasterBlockService = new NoMasterBlockService(settings, clusterSettings);
        this.lastKnownLeader = Optional.empty();
        this.lastJoin = Optional.empty();
        this.joinAccumulator = new InitialJoinAccumulator();
        this.publishTimeout = PUBLISH_TIMEOUT_SETTING.get(settings);
        this.publishInfoTimeout = PUBLISH_INFO_TIMEOUT_SETTING.get(settings);
        this.random = random;
        this.electionSchedulerFactory = new ElectionSchedulerFactory(settings, random, transportService.getThreadPool());
        this.preVoteCollector = new PreVoteCollector(transportService, this::startElection, this::updateMaxTermSeen, electionStrategy);
        configuredHostsResolver = new SeedHostsResolver(nodeName, settings, transportService, seedHostsProvider);

        // 集群节点探测器
        this.peerFinder = new CoordinatorPeerFinder(settings, transportService,
            // 该连接对象在连接到某个地址时 会校验是否是master节点 如果不是的话 以失败的方式触发监听器
            new HandshakingTransportAddressConnector(settings, transportService), configuredHostsResolver);
        this.publicationHandler = new PublicationTransportHandler(transportService, namedWriteableRegistry,
            this::handlePublishRequest, this::handleApplyCommit);
        this.leaderChecker = new LeaderChecker(settings, transportService, this::onLeaderFailure);
        this.followersChecker = new FollowersChecker(settings, transportService, this::onFollowerCheckRequest, this::removeNode);
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        this.clusterApplier = clusterApplier;
        masterService.setClusterStateSupplier(this::getStateForMasterService);
        this.reconfigurator = new Reconfigurator(settings, clusterSettings);
        this.clusterBootstrapService = new ClusterBootstrapService(settings, transportService, this::getFoundPeers,
            this::isInitialConfigurationSet, this::setInitialConfiguration);
        this.lagDetector = new LagDetector(settings, transportService.getThreadPool(), n -> removeNode(n, "lagging"),
            transportService::getLocalNode);
        this.clusterFormationFailureHelper = new ClusterFormationFailureHelper(settings, this::getClusterFormationState,
            transportService.getThreadPool(), joinHelper::logLastFailedJoinAttempt);
    }

    private ClusterFormationState getClusterFormationState() {
        return new ClusterFormationState(settings, getStateForMasterService(), peerFinder.getLastResolvedAddresses(),
            Stream.concat(Stream.of(getLocalNode()), StreamSupport.stream(peerFinder.getFoundPeers().spliterator(), false))
                    .collect(Collectors.toList()), getCurrentTerm(), electionStrategy);
    }

    /**
     * 集群中的所有节点只要有一个检测到 master节点下线了 就将自身修改成candidate 并发起预投票 在这个过程中会检测其他节点是否还能连接到master上 只要超过半数无法连接到 就通过了预投票 并且发起startJoin 开始拉票
     * @param e
     */
    private void onLeaderFailure(Exception e) {
        synchronized (mutex) {
            if (mode != Mode.CANDIDATE) {
                assert lastKnownLeader.isPresent();
                logger.info(new ParameterizedMessage("master node [{}] failed, restarting discovery", lastKnownLeader.get()), e);
            }
            becomeCandidate("onLeaderFailure");
        }
    }

    private void removeNode(DiscoveryNode discoveryNode, String reason) {
        synchronized (mutex) {
            if (mode == Mode.LEADER) {
                masterService.submitStateUpdateTask("node-left",
                    new NodeRemovalClusterStateTaskExecutor.Task(discoveryNode, reason),
                    ClusterStateTaskConfig.build(Priority.IMMEDIATE),
                    nodeRemovalExecutor,
                    nodeRemovalExecutor);
            }
        }
    }

    /**
     * 检测当前节点是否是 follower节点
     * @param followerCheckRequest
     */
    void onFollowerCheckRequest(FollowerCheckRequest followerCheckRequest) {
        synchronized (mutex) {
            ensureTermAtLeast(followerCheckRequest.getSender(), followerCheckRequest.getTerm());

            if (getCurrentTerm() != followerCheckRequest.getTerm()) {
                logger.trace("onFollowerCheckRequest: current term is [{}], rejecting {}", getCurrentTerm(), followerCheckRequest);
                throw new CoordinationStateRejectedException("onFollowerCheckRequest: current term is ["
                    + getCurrentTerm() + "], rejecting " + followerCheckRequest);
            }

            // check if node has accepted a state in this term already. If not, this node has never committed a cluster state in this
            // term and therefore never removed the NO_MASTER_BLOCK for this term. This logic ensures that we quickly turn a node
            // into follower, even before receiving the first cluster state update, but also don't have to deal with the situation
            // where we would possibly have to remove the NO_MASTER_BLOCK from the applierState when turning a candidate back to follower.
            if (getLastAcceptedState().term() < getCurrentTerm()) {
                becomeFollower("onFollowerCheckRequest", followerCheckRequest.getSender());
            } else if (mode == Mode.FOLLOWER) {
                logger.trace("onFollowerCheckRequest: responding successfully to {}", followerCheckRequest);
            } else if (joinHelper.isJoinPending()) {
                logger.trace("onFollowerCheckRequest: rejoining master, responding successfully to {}", followerCheckRequest);
            } else {
                logger.trace("onFollowerCheckRequest: received check from faulty master, rejecting {}", followerCheckRequest);
                throw new CoordinationStateRejectedException(
                    "onFollowerCheckRequest: received check from faulty master, rejecting " + followerCheckRequest);
            }
        }
    }

    private void handleApplyCommit(ApplyCommitRequest applyCommitRequest, ActionListener<Void> applyListener) {
        synchronized (mutex) {
            logger.trace("handleApplyCommit: applying commit {}", applyCommitRequest);

            coordinationState.get().handleCommit(applyCommitRequest);
            final ClusterState committedState = hideStateIfNotRecovered(coordinationState.get().getLastAcceptedState());
            applierState = mode == Mode.CANDIDATE ? clusterStateWithNoMasterBlock(committedState) : committedState;
            if (applyCommitRequest.getSourceNode().equals(getLocalNode())) {
                // master node applies the committed state at the end of the publication process, not here.
                applyListener.onResponse(null);
            } else {
                clusterApplier.onNewClusterState(applyCommitRequest.toString(), () -> applierState,
                    new ClusterApplyListener() {

                        @Override
                        public void onFailure(String source, Exception e) {
                            applyListener.onFailure(e);
                        }

                        @Override
                        public void onSuccess(String source) {
                            applyListener.onResponse(null);
                        }
                    });
            }
        }
    }

    /**
     * 当接受到集群中的 发布请求时触发
     * @param publishRequest
     * @return
     */
    PublishWithJoinResponse handlePublishRequest(PublishRequest publishRequest) {
        assert publishRequest.getAcceptedState().nodes().getLocalNode().equals(getLocalNode()) :
            publishRequest.getAcceptedState().nodes().getLocalNode() + " != " + getLocalNode();

        synchronized (mutex) {
            // 获取当前记录的集群中的master节点
            final DiscoveryNode sourceNode = publishRequest.getAcceptedState().nodes().getMasterNode();
            logger.trace("handlePublishRequest: handling [{}] from [{}]", publishRequest, sourceNode);

            // 如果是发往自己的请求 那么当前节点必须是master(leader)节点  因为只有leader节点才有发布权力
            if (sourceNode.equals(getLocalNode()) && mode != Mode.LEADER) {
                // Rare case in which we stood down as leader between starting this publication and receiving it ourselves. The publication
                // is already failed so there is no point in proceeding.
                throw new CoordinationStateRejectedException("no longer leading this publication's term: " + publishRequest);
            }

            // 上个集群快照
            final ClusterState localState = coordinationState.get().getLastAcceptedState();


            // 看来每个集群使用同一个uuid
            if (localState.metadata().clusterUUIDCommitted() &&
                localState.metadata().clusterUUID().equals(publishRequest.getAcceptedState().metadata().clusterUUID()) == false) {
                logger.warn("received cluster state from {} with a different cluster uuid {} than local cluster uuid {}, rejecting",
                    sourceNode, publishRequest.getAcceptedState().metadata().clusterUUID(), localState.metadata().clusterUUID());
                throw new CoordinationStateRejectedException("received cluster state from " + sourceNode +
                    " with a different cluster uuid " + publishRequest.getAcceptedState().metadata().clusterUUID() +
                    " than local cluster uuid " + localState.metadata().clusterUUID() + ", rejecting");
            }

            // 代表集群发生了更替 也就是leader节点变化了
            if (publishRequest.getAcceptedState().term() > localState.term()) {
                // only do join validation if we have not accepted state from this master yet
                // TODO 目前只看到有关兼容性检测的逻辑
                onJoinValidators.forEach(a -> a.accept(getLocalNode(), publishRequest.getAcceptedState()));
            }

            // 更新当前节点的任期
            ensureTermAtLeast(sourceNode, publishRequest.getAcceptedState().term());
            // 处理发布请求 主要就是更新节点此时存储的 clusterState 这里还涉及到对clusterState的持久化
            final PublishResponse publishResponse = coordinationState.get().handlePublishRequest(publishRequest);

            if (sourceNode.equals(getLocalNode())) {
                // 顺便更新 preVote中的leader节点   在发起预投票过程中 其他节点就是根据该属性判断 当前集群中leader节点是哪个
                preVoteCollector.update(getPreVoteResponse(), getLocalNode());
            } else {
                // 其余节点只要接收到了发布请求 就代表在当前任期中 自身不是master节点 并且此时集群中已经产生了master节点 就可以将自身转换成 follower节点
                becomeFollower("handlePublishRequest", sourceNode); // also updates preVoteCollector
            }

            return new PublishWithJoinResponse(publishResponse,
                joinWithDestination(lastJoin, sourceNode, publishRequest.getAcceptedState().term()));
        }
    }

    /**
     * 检测最近一次发出的join请求目标地点是否就是 本次收到publish请求的leader节点  也就代表本次leader是否有该节点贡献的一票
     * @param lastJoin
     * @param leader
     * @param term
     * @return
     */
    private static Optional<Join> joinWithDestination(Optional<Join> lastJoin, DiscoveryNode leader, long term) {
        if (lastJoin.isPresent()
            && lastJoin.get().targetMatches(leader)
            && lastJoin.get().getTerm() == term) {
            return lastJoin;
        }

        return Optional.empty();
    }

    private void closePrevotingAndElectionScheduler() {
        // 因为选举已经完成 所以关闭预投票处理对象
        if (prevotingRound != null) {
            prevotingRound.close();
            prevotingRound = null;
        }

        if (electionScheduler != null) {
            // 该对象只有在当前是 candidate时才会起作用  当本节点转换成 follower/master时 就不会继续运行了
            electionScheduler.close();
            electionScheduler = null;
        }
    }

    /**
     * 当发送预投票请求时  目标节点会通过req 触发该函数
     * 这时返回的res 又会在源节点上触发该函数
     * @param term
     */
    private void updateMaxTermSeen(final long term) {
        synchronized (mutex) {
            maxTermSeen = Math.max(maxTermSeen, term);
            final long currentTerm = getCurrentTerm();
            // 如果当前节点是master节点  并且收到了比当前更大的任期 代表发生了脑裂
            if (mode == Mode.LEADER && maxTermSeen > currentTerm) {
                // Bump our term. However if there is a publication in flight then doing so would cancel the publication, so don't do that
                // since we check whether a term bump is needed at the end of the publication too.
                // TODO 如果此时正处于发布状态 该节点很快就会意识到自己过期了 所以不需要处理
                if (publicationInProgress()) {
                    logger.debug("updateMaxTermSeen: maxTermSeen = {} > currentTerm = {}, enqueueing term bump", maxTermSeen, currentTerm);
                } else {
                    try {
                        logger.debug("updateMaxTermSeen: maxTermSeen = {} > currentTerm = {}, bumping term", maxTermSeen, currentTerm);
                        // 这里还不知道要将请求发往哪里
                        ensureTermAtLeast(getLocalNode(), maxTermSeen);
                        startElection();
                    } catch (Exception e) {
                        logger.warn(new ParameterizedMessage("failed to bump term to {}", maxTermSeen), e);
                        becomeCandidate("updateMaxTermSeen");
                    }
                }
            }
        }
    }

    /**
     * 在满足了预投票的要求后 开始选举
     */
    private void startElection() {
        synchronized (mutex) {
            // The preVoteCollector is only active while we are candidate, but it does not call this method with synchronisation, so we have
            // to check our mode again here.
            if (mode == Mode.CANDIDATE) {
                // 忽略选举失败的情况
                if (localNodeMayWinElection(getLastAcceptedState()) == false) {
                    logger.trace("skip election as local node may not win it: {}", getLastAcceptedState().coordinationMetadata());
                    return;
                }

                // 向所有节点发出 startJoin请求  这里将任期+1  startJoin的请求是某个通过预投票的候选者发往集群其他节点的
                final StartJoinRequest startJoinRequest
                    = new StartJoinRequest(getLocalNode(), Math.max(getCurrentTerm(), maxTermSeen) + 1);
                logger.debug("starting election with {}", startJoinRequest);
                getDiscoveredNodes().forEach(node -> joinHelper.sendStartJoinRequest(startJoinRequest, node));
            }
        }
    }

    private void abdicateTo(DiscoveryNode newMaster) {
        assert Thread.holdsLock(mutex);
        assert mode == Mode.LEADER : "expected to be leader on abdication but was " + mode;
        assert newMaster.isMasterNode() : "should only abdicate to master-eligible node but was " + newMaster;
        final StartJoinRequest startJoinRequest = new StartJoinRequest(newMaster, Math.max(getCurrentTerm(), maxTermSeen) + 1);
        logger.info("abdicating to {} with term {}", newMaster, startJoinRequest.getTerm());
        getLastAcceptedState().nodes().mastersFirstStream().forEach(node -> joinHelper.sendStartJoinRequest(startJoinRequest, node));
        // handling of start join messages on the local node will be dispatched to the generic thread-pool
        assert mode == Mode.LEADER : "should still be leader after sending abdication messages " + mode;
        // explicitly move node to candidate state so that the next cluster state update task yields an onNoLongerMaster event
        becomeCandidate("after abdicating to " + newMaster);
    }

    /**
     *  检测本地节点是否在选举中获胜
     * @param lastAcceptedState
     * @return
     */
    private static boolean localNodeMayWinElection(ClusterState lastAcceptedState) {
        final DiscoveryNode localNode = lastAcceptedState.nodes().getLocalNode();
        assert localNode != null;
        return nodeMayWinElection(lastAcceptedState, localNode);
    }

    /**
     * 检测本次节点是否在选举中获胜
     * @param lastAcceptedState
     * @param node
     * @return
     */
    private static boolean nodeMayWinElection(ClusterState lastAcceptedState, DiscoveryNode node) {
        final String nodeId = node.getId();
        // TODO 下面都是node列表啊 难道有多个节点会同时选举成功 ???  并且这2个列表是什么时候填充的
        return lastAcceptedState.getLastCommittedConfiguration().getNodeIds().contains(nodeId)
            || lastAcceptedState.getLastAcceptedConfiguration().getNodeIds().contains(nodeId)
            || lastAcceptedState.getVotingConfigExclusions().stream().noneMatch(vce -> vce.getNodeId().equals(nodeId));
    }


    /**
     * 检测目标节点的任期是否是最新的
     * @param sourceNode
     * @param targetTerm
     * @return
     */
    private Optional<Join> ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        if (getCurrentTerm() < targetTerm) {
            return Optional.of(joinLeaderInTerm(new StartJoinRequest(sourceNode, targetTerm)));
        }
        return Optional.empty();
    }

    /**
     * 此时作为集群中 没有通过预投票的其他节点  接受到 startJoin请求后 触发该方法
     * @param startJoinRequest
     * @return
     */
    private Join joinLeaderInTerm(StartJoinRequest startJoinRequest) {
        synchronized (mutex) {
            logger.debug("joinLeaderInTerm: for [{}] with term {}", startJoinRequest.getSourceNode(), startJoinRequest.getTerm());
            // 将本节点信息包装成join对象
            final Join join = coordinationState.get().handleStartJoin(startJoinRequest);

            // 更新最近一次发出的join请求
            lastJoin = Optional.of(join);
            // 更新任期后 在处理Peer请求时 也能返回最新的任期
            peerFinder.setCurrentTerm(getCurrentTerm());

            // 在 startJoin的阶段 可以将除了通过预投票的其余节点强制修改成 候选节点   也就是预投票阶段就是检测是否是master节点无效了
            if (mode != Mode.CANDIDATE) {
                becomeCandidate("joinLeaderInTerm"); // updates followersChecker and preVoteCollector
            } else {
                // 更新当前节点在集群中被感知到的任期
                followersChecker.updateFastResponseState(getCurrentTerm(), mode);
                preVoteCollector.update(getPreVoteResponse(), null);
            }
            return join;
        }
    }


    /**
     * 当收到某个节点发来的join请求 触发该方法
     * @param joinRequest
     * @param joinCallback
     */
    private void handleJoinRequest(JoinRequest joinRequest, JoinHelper.JoinCallback joinCallback) {
        assert Thread.holdsLock(mutex) == false;
        assert getLocalNode().isMasterNode() : getLocalNode() + " received a join but is not master-eligible";
        logger.trace("handleJoinRequest: as {}, handling {}", mode, joinRequest);

        // 忽略单节点集群
        if (singleNodeDiscovery && joinRequest.getSourceNode().equals(getLocalNode()) == false) {
            joinCallback.onFailure(new IllegalStateException("cannot join node with [" + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey() +
                "] set to [" + DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE  + "] discovery"));
            return;
        }

        // 此时作为leader节点 在接受到join请求后 就尝试与这些节点建立连接
        transportService.connectToNode(joinRequest.getSourceNode(), ActionListener.wrap(ignore -> {
            final ClusterState stateForJoinValidation = getStateForMasterService();


            // TODO 一开始通过预投票的节点此时应该还没有变成master节点
            if (stateForJoinValidation.nodes().isLocalNodeElectedMaster()) {
                onJoinValidators.forEach(a -> a.accept(joinRequest.getSourceNode(), stateForJoinValidation));
                if (stateForJoinValidation.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                    // we do this in a couple of places including the cluster update thread. This one here is really just best effort
                    // to ensure we fail as fast as possible.
                    JoinTaskExecutor.ensureMajorVersionBarrier(joinRequest.getSourceNode().getVersion(),
                        stateForJoinValidation.getNodes().getMinNodeVersion());
                }
                sendValidateJoinRequest(stateForJoinValidation, joinRequest, joinCallback);
            } else {
                //  成功连接到节点后发送处理join
                processJoinRequest(joinRequest, joinCallback);
            }
        }, joinCallback::onFailure));
    }

    // package private for tests
    void sendValidateJoinRequest(ClusterState stateForJoinValidation, JoinRequest joinRequest,
                                 JoinHelper.JoinCallback joinCallback) {
        // validate the join on the joining node, will throw a failure if it fails the validation
        joinHelper.sendValidateJoinRequest(joinRequest.getSourceNode(), stateForJoinValidation, new ActionListener<Empty>() {
            @Override
            public void onResponse(Empty empty) {
                try {
                    processJoinRequest(joinRequest, joinCallback);
                } catch (Exception e) {
                    joinCallback.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to validate incoming join request from node [{}]",
                    joinRequest.getSourceNode()), e);
                joinCallback.onFailure(new IllegalStateException("failure when sending a validation request to node", e));
            }
        });
    }

    /**
     * 当通过候选的节点 连接到发送join请求的节点后 才会处理之前收到的请求
     * @param joinRequest
     * @param joinCallback
     */
    private void processJoinRequest(JoinRequest joinRequest, JoinHelper.JoinCallback joinCallback) {
        final Optional<Join> optionalJoin = joinRequest.getOptionalJoin();
        synchronized (mutex) {
            updateMaxTermSeen(joinRequest.getTerm());

            final CoordinationState coordState = coordinationState.get();
            // 上次选举是否成功 ???
            final boolean prevElectionWon = coordState.electionWon();

            // 处理本次join请求内部的join
            optionalJoin.ifPresent(this::handleJoin);

            // 将join请求存储到 accumulator中
            joinAccumulator.handleJoinRequest(joinRequest.getSourceNode(), joinCallback);

            // 如果此时发现当前节点已经获取了足够的选票 晋升成leader
            if (prevElectionWon == false && coordState.electionWon()) {
                becomeLeader("handleJoinRequest");
            }
        }
    }

    /**
     * 将当前节点变成候选者节点 尝试通过选举生成leader
     * @param method
     */
    void becomeCandidate(String method) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        logger.debug("{}: coordinator becoming CANDIDATE in term {} (was {}, lastKnownLeader was [{}])",
            method, getCurrentTerm(), mode, lastKnownLeader);

        if (mode != Mode.CANDIDATE) {
            final Mode prevMode = mode;
            // 将当前节点的角色变成候选者
            mode = Mode.CANDIDATE;
            // 在节点首次启动时 还没有开启发布任务
            // 如果至少发布过一次 那么就会初始化发布任务  此时如果变回了候选者 那么就会终止对外发布的任务
            cancelActivePublication("become candidate: " + method);
            joinAccumulator.close(mode);
            // 关闭之前的join结果采集器  同时将当前对象修改成在候选者场景下 处理join请求的采集器
            joinAccumulator = joinHelper.new CandidateJoinAccumulator();

            // 此时开始探测之前记录的集群中的节点   这里初步操作就是找到所有master节点并发送探测请求 非master节点的探测将被忽略
            peerFinder.activate(coordinationState.get().getLastAcceptedState().nodes());

            // 该对象启动后 会定期执行joinHelper::logLastFailedJoinAttempt   也就是定期打印设置到 JoinHelper内部的失败的join
            clusterFormationFailureHelper.start();

            // 此时还不知道集群中的leader节点 所以将leaderChecker 内部的待检查节点滞空  以及设置一个空的节点群
            leaderChecker.setCurrentNodes(DiscoveryNodes.EMPTY_NODES);
            leaderChecker.updateLeader(null);

            // 同上
            followersChecker.clearCurrentNodes();
            // 更新当前结果状态 这时针对别的节点的请求 会快速返回该结果
            followersChecker.updateFastResponseState(getCurrentTerm(), mode);
            // 此时不再对任何节点进行滞后探测
            lagDetector.clearTrackedNodes();

            // 如果此前该节点是一个leader节点 那么就要清理masterService  那么果然leader与master是同一含义 ???
            if (prevMode == Mode.LEADER) {
                cleanMasterService();
            }

            // 如果当前集群存在master节点  将masterId 从集群中移除 同时追加一个block   因为集群状态发生了变化 还会触发监听器
            if (applierState.nodes().getMasterNodeId() != null) {
                applierState = clusterStateWithNoMasterBlock(applierState);
                clusterApplier.onNewClusterState("becoming candidate: " + method, () -> applierState, (source, e) -> {
                });
            }
        }

        preVoteCollector.update(getPreVoteResponse(), null);
    }

    /**
     * 将当前节点晋升成leader
     * @param method
     */
    void becomeLeader(String method) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;
        assert getLocalNode().isMasterNode() : getLocalNode() + " became a leader but is not master-eligible";

        logger.debug("{}: coordinator becoming LEADER in term {} (was {}, lastKnownLeader was [{}])",
            method, getCurrentTerm(), mode, lastKnownLeader);

        mode = Mode.LEADER;
        // 这里会生成一组join 任务 并交由 JoinTaskExecutor执行  同时将本节点暴露给集群中的其他节点 其他节点就会自动变成follower
        joinAccumulator.close(mode);
        // 将累加器更新成 leaderAccu
        joinAccumulator = joinHelper.new LeaderJoinAccumulator();

        // 更新已知的leader节点
        lastKnownLeader = Optional.of(getLocalNode());

        // 停止对外探测
        peerFinder.deactivate(getLocalNode());
        // 因为选举阶段已经结束了 所以不需要再打印 失败的join信息了
        clusterFormationFailureHelper.stop();
        closePrevotingAndElectionScheduler();
        // 因为当前节点是leader节点 设置结果到  preVoteCollector 中
        preVoteCollector.update(getPreVoteResponse(), getLocalNode());

        assert leaderChecker.leader() == null : leaderChecker.leader();
        followersChecker.updateFastResponseState(getCurrentTerm(), mode);
    }

    /**
     * 当某个候选节点 或者 master节点 接受到一个新的发布请求时 将自身转换成follower节点
     * @param method
     * @param leaderNode
     */
    void becomeFollower(String method, DiscoveryNode leaderNode) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert leaderNode.isMasterNode() : leaderNode + " became a leader but is not master-eligible";
        assert mode != Mode.LEADER : "do not switch to follower from leader (should be candidate first)";

        if (mode == Mode.FOLLOWER && Optional.of(leaderNode).equals(lastKnownLeader)) {
            logger.trace("{}: coordinator remaining FOLLOWER of [{}] in term {}",
                method, leaderNode, getCurrentTerm());
        } else {
            logger.debug("{}: coordinator becoming FOLLOWER of [{}] in term {} (was {}, lastKnownLeader was [{}])",
                method, leaderNode, getCurrentTerm(), mode, lastKnownLeader);
        }

        // 当leader节点发生了变化 或者是当前节点首次设置成follower 那么就可以重启 masterChecker对象了   masterCheck对象通过定期检测master节点是否下线 尝试发起新一轮选举
        final boolean restartLeaderChecker = (mode == Mode.FOLLOWER && Optional.of(leaderNode).equals(lastKnownLeader)) == false;

        // 其他节点什么时候变成候选者  只有在发现master节点下线时么
        if (mode != Mode.FOLLOWER) {
            mode = Mode.FOLLOWER;
            // 当前如果是candidate节点转换成follower 将会触发关闭累加器的逻辑 这时会处理该节点在startJoin期间 收到的所有join请求
            joinAccumulator.close(mode);
            // 将累加器切换成follower角色相关的  (这样在接受到新的join请求时就会走不同的逻辑)
            joinAccumulator = new JoinHelper.FollowerJoinAccumulator();
            // 在 leaderChecker中 只要当前节点不是master节点 实际上并不会用到 discoveryNodes
            leaderChecker.setCurrentNodes(DiscoveryNodes.EMPTY_NODES);
        }

        // 更新此时已知的集群中leader节点
        lastKnownLeader = Optional.of(leaderNode);
        peerFinder.deactivate(leaderNode);
        // 因为本轮选举已经结束了 所以不再需要打印join失败的信息了
        clusterFormationFailureHelper.stop();
        closePrevotingAndElectionScheduler();

        // 如果此时该对象正在进行一个发布动作 也就是当前节点之前还是master节点  在某次选举后生成了新的master节点 并且通知到旧的master节点 这时取消publish任务
        cancelActivePublication("become follower: " + method);
        // 更新为了处理预投票请求的 结果对象 (也就是在收到预投票请求时 会将此时的leader返回)
        preVoteCollector.update(getPreVoteResponse(), leaderNode);

        // 开始定期检测新的leader是否变化
        if (restartLeaderChecker) {
            leaderChecker.updateLeader(leaderNode);
        }

        // 应该是只有 master节点才会开启这个对象
        followersChecker.clearCurrentNodes();
        followersChecker.updateFastResponseState(getCurrentTerm(), mode);
        // follower不需要检测 滞后的节点
        lagDetector.clearTrackedNodes();
    }

    /**
     * 某个节点从leader 降级成 candidate时 触发该函数
     */
    private void cleanMasterService() {

        // 提交一个更新任务 TODO 这里也是异步执行啊  这个任务意味着什么
        masterService.submitStateUpdateTask("clean-up after stepping down as master",
            new LocalClusterUpdateTask() {
                @Override
                public void onFailure(String source, Exception e) {
                    // ignore
                    logger.trace("failed to clean-up after stepping down as master", e);
                }

                @Override
                public ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) {
                    // 当前节点不是master节点时 清除缓存
                    if (currentState.nodes().isLocalNodeElectedMaster() == false) {
                        allocationService.cleanCaches();
                    }
                    // 这里返回了一个空结果对象啊
                    return unchanged();
                }

            });
    }


    /**
     * 基于当前term 以及最后一次集群中的节点生成一个 res 对象
     * @return
     */
    private PreVoteResponse getPreVoteResponse() {
        return new PreVoteResponse(getCurrentTerm(), coordinationState.get().getLastAcceptedTerm(),
            coordinationState.get().getLastAcceptedState().version());
    }

    // package-visible for testing
    // 获取当前节点的任期   当接受到startJoin时 需要检测本地任期与预投票任期的大小
    long getCurrentTerm() {
        synchronized (mutex) {
            return coordinationState.get().getCurrentTerm();
        }
    }

    // package-visible for testing
    Mode getMode() {
        synchronized (mutex) {
            return mode;
        }
    }

    // visible for testing
    DiscoveryNode getLocalNode() {
        return transportService.getLocalNode();
    }

    // package-visible for testing
    boolean publicationInProgress() {
        synchronized (mutex) {
            return currentPublication.isPresent();
        }
    }

    /**
     * 整个中枢对象 通过该函数启动
     */
    @Override
    protected void doStart() {
        // 确保单线程执行
        synchronized (mutex) {
            // 应该是获取之前有关集群信息的持久化数据
            CoordinationState.PersistedState persistedState = persistedStateSupplier.get();
            // TODO 有没有可能多个节点的选举策略不一样
            coordinationState.set(new CoordinationState(getLocalNode(), persistedState, electionStrategy));

            // 从持久化数据中获取当前任期
            peerFinder.setCurrentTerm(getCurrentTerm());

            // 初始化地址解析对象内部的线程池
            configuredHostsResolver.start();

            // 获取之前持久化的最近一次集群数据  (每次某个节点重启时肯定是根据之前持久化的数据尝试恢复)
            final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();

            // TODO
            if (lastAcceptedState.metadata().clusterUUIDCommitted()) {
                logger.info("cluster UUID [{}]", lastAcceptedState.metadata().clusterUUID());
            }

            // 获取持久化数据中 描述最后一次选举的相关配置  也就是本次选举有多少节点参与
            final VotingConfiguration votingConfiguration = lastAcceptedState.getLastCommittedConfiguration();

            // 在单节点集群下 如果votingConfiguration.nodeIds >=2 那么无法完成选举动作 处于异常状态
            if (singleNodeDiscovery &&
                votingConfiguration.isEmpty() == false &&
                votingConfiguration.hasQuorum(Collections.singleton(getLocalNode().getId())) == false) {
                throw new IllegalStateException("cannot start with [" + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey() + "] set to [" +
                    DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE + "] when local node " + getLocalNode() +
                    " does not have quorum in voting configuration " + votingConfiguration);
            }
            ClusterState initialState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
                .blocks(ClusterBlocks.builder()
                    // TODO 这里加入了2个特殊的阻塞对象
                    .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
                    .addGlobalBlock(noMasterBlockService.getNoMasterBlock()))
                // 此时集群对象中 只有localNode
                .nodes(DiscoveryNodes.builder().add(getLocalNode()).localNodeId(getLocalNode().getId()))
                // 加集群状态初始化时 会生成一个 uuid
                .build();
            applierState = initialState;
            // 将初始化的集群状态 设置到 ClusterApplierService中
            clusterApplier.setInitialState(initialState);
        }
    }

    @Override
    public DiscoveryStats stats() {
        return new DiscoveryStats(new PendingClusterStateStats(0, 0, 0), publicationHandler.stats());
    }

    /**
     * 开始初始化整个集群对象
     */
    @Override
    public void startInitialJoin() {
        synchronized (mutex) {
            becomeCandidate("startInitialJoin");
        }
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
    }

    @Override
    protected void doStop() {
        configuredHostsResolver.stop();
    }

    @Override
    protected void doClose() throws IOException {
        final CoordinationState coordinationState = this.coordinationState.get();
        if (coordinationState != null) {
            // This looks like a race that might leak an unclosed CoordinationState if it's created while execution is here, but this method
            // is synchronized on AbstractLifecycleComponent#lifestyle, as is the doStart() method that creates the CoordinationState, so
            // it's all ok.
            synchronized (mutex) {
                coordinationState.close();
            }
        }
    }

    public void invariant() {
        synchronized (mutex) {
            final Optional<DiscoveryNode> peerFinderLeader = peerFinder.getLeader();
            assert peerFinder.getCurrentTerm() == getCurrentTerm();
            assert followersChecker.getFastResponseState().term == getCurrentTerm() : followersChecker.getFastResponseState();
            assert followersChecker.getFastResponseState().mode == getMode() : followersChecker.getFastResponseState();
            assert (applierState.nodes().getMasterNodeId() == null) == applierState.blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID);
            assert preVoteCollector.getPreVoteResponse().equals(getPreVoteResponse())
                : preVoteCollector + " vs " + getPreVoteResponse();

            assert lagDetector.getTrackedNodes().contains(getLocalNode()) == false : lagDetector.getTrackedNodes();
            assert followersChecker.getKnownFollowers().equals(lagDetector.getTrackedNodes())
                : followersChecker.getKnownFollowers() + " vs " + lagDetector.getTrackedNodes();

            if (mode == Mode.LEADER) {
                final boolean becomingMaster = getStateForMasterService().term() != getCurrentTerm();

                assert coordinationState.get().electionWon();
                assert lastKnownLeader.isPresent() && lastKnownLeader.get().equals(getLocalNode());
                assert joinAccumulator instanceof JoinHelper.LeaderJoinAccumulator;
                assert peerFinderLeader.equals(lastKnownLeader) : peerFinderLeader;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
                assert becomingMaster || getStateForMasterService().nodes().getMasterNodeId() != null : getStateForMasterService();
                assert leaderChecker.leader() == null : leaderChecker.leader();
                assert getLocalNode().equals(applierState.nodes().getMasterNode()) ||
                    (applierState.nodes().getMasterNodeId() == null && applierState.term() < getCurrentTerm());
                assert preVoteCollector.getLeader() == getLocalNode() : preVoteCollector;
                assert clusterFormationFailureHelper.isRunning() == false;

                final boolean activePublication = currentPublication.map(CoordinatorPublication::isActiveForCurrentLeader).orElse(false);
                if (becomingMaster && activePublication == false) {
                    // cluster state update task to become master is submitted to MasterService, but publication has not started yet
                    assert followersChecker.getKnownFollowers().isEmpty() : followersChecker.getKnownFollowers();
                } else {
                    final ClusterState lastPublishedState;
                    if (activePublication) {
                        // active publication in progress: followersChecker is up-to-date with nodes that we're actively publishing to
                        lastPublishedState = currentPublication.get().publishedState();
                    } else {
                        // no active publication: followersChecker is up-to-date with the nodes of the latest publication
                        lastPublishedState = coordinationState.get().getLastAcceptedState();
                    }
                    final Set<DiscoveryNode> lastPublishedNodes = new HashSet<>();
                    lastPublishedState.nodes().forEach(lastPublishedNodes::add);
                    assert lastPublishedNodes.remove(getLocalNode()); // followersChecker excludes local node
                    assert lastPublishedNodes.equals(followersChecker.getKnownFollowers()) :
                        lastPublishedNodes + " != " + followersChecker.getKnownFollowers();
                }

                assert becomingMaster || activePublication ||
                    coordinationState.get().getLastAcceptedConfiguration().equals(coordinationState.get().getLastCommittedConfiguration())
                    : coordinationState.get().getLastAcceptedConfiguration() + " != "
                    + coordinationState.get().getLastCommittedConfiguration();
            } else if (mode == Mode.FOLLOWER) {
                assert coordinationState.get().electionWon() == false : getLocalNode() + " is FOLLOWER so electionWon() should be false";
                assert lastKnownLeader.isPresent() && (lastKnownLeader.get().equals(getLocalNode()) == false);
                assert joinAccumulator instanceof JoinHelper.FollowerJoinAccumulator;
                assert peerFinderLeader.equals(lastKnownLeader) : peerFinderLeader;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
                assert getStateForMasterService().nodes().getMasterNodeId() == null : getStateForMasterService();
                assert leaderChecker.currentNodeIsMaster() == false;
                assert lastKnownLeader.equals(Optional.of(leaderChecker.leader()));
                assert followersChecker.getKnownFollowers().isEmpty();
                assert lastKnownLeader.get().equals(applierState.nodes().getMasterNode()) ||
                    (applierState.nodes().getMasterNodeId() == null &&
                        (applierState.term() < getCurrentTerm() || applierState.version() < getLastAcceptedState().version()));
                assert currentPublication.map(Publication::isCommitted).orElse(true);
                assert preVoteCollector.getLeader().equals(lastKnownLeader.get()) : preVoteCollector;
                assert clusterFormationFailureHelper.isRunning() == false;
            } else {
                assert mode == Mode.CANDIDATE;
                assert joinAccumulator instanceof JoinHelper.CandidateJoinAccumulator;
                assert peerFinderLeader.isPresent() == false : peerFinderLeader;
                assert prevotingRound == null || electionScheduler != null;
                assert getStateForMasterService().nodes().getMasterNodeId() == null : getStateForMasterService();
                assert leaderChecker.currentNodeIsMaster() == false;
                assert leaderChecker.leader() == null : leaderChecker.leader();
                assert followersChecker.getKnownFollowers().isEmpty();
                assert applierState.nodes().getMasterNodeId() == null;
                assert currentPublication.map(Publication::isCommitted).orElse(true);
                assert preVoteCollector.getLeader() == null : preVoteCollector;
                assert clusterFormationFailureHelper.isRunning();
            }
        }
    }

    public boolean isInitialConfigurationSet() {
        return getStateForMasterService().getLastAcceptedConfiguration().isEmpty() == false;
    }

    /**
     * Sets the initial configuration to the given {@link VotingConfiguration}. This method is safe to call
     * more than once, as long as the argument to each call is the same.
     *
     * @param votingConfiguration The nodes that should form the initial configuration.    参与选举的所有node???
     * @return whether this call successfully set the initial configuration - if false, the cluster has already been bootstrapped.
     * 开始发起投票
     */
    public boolean setInitialConfiguration(final VotingConfiguration votingConfiguration) {
        synchronized (mutex) {
            // TODO 只看到如果存在 masterId 就去除 其余还不明白
            final ClusterState currentState = getStateForMasterService();

            // 如果之前的clusterState中存在 VotingConf 不进行处理
            if (isInitialConfigurationSet()) {
                logger.debug("initial configuration already set, ignoring {}", votingConfiguration);
                return false;
            }

            // TODO 当前节点不是master节点 抛出异常 啥玩意
            if (getLocalNode().isMasterNode() == false) {
                logger.debug("skip setting initial configuration as local node is not a master-eligible node");
                throw new CoordinationStateRejectedException(
                    "this node is not master-eligible, but cluster bootstrapping can only happen on a master-eligible node");
            }

            // 如果本次参与选举的节点中不存在本节点 抛出异常
            if (votingConfiguration.getNodeIds().contains(getLocalNode().getId()) == false) {
                logger.debug("skip setting initial configuration as local node is not part of initial configuration");
                throw new CoordinationStateRejectedException("local node is not part of initial configuration");
            }

            final List<DiscoveryNode> knownNodes = new ArrayList<>();
            knownNodes.add(getLocalNode());
            // 将找到的所有节点加入到 knownNodes中
            peerFinder.getFoundPeers().forEach(knownNodes::add);

            // knownNodes的数量必须超过总节点数的一半
            if (votingConfiguration.hasQuorum(knownNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toList())) == false) {
                logger.debug("skip setting initial configuration as not enough nodes discovered to form a quorum in the " +
                    "initial configuration [knownNodes={}, {}]", knownNodes, votingConfiguration);
                throw new CoordinationStateRejectedException("not enough nodes discovered to form a quorum in the initial configuration " +
                    "[knownNodes=" + knownNodes + ", " + votingConfiguration + "]");
            }

            logger.info("setting initial configuration to {}", votingConfiguration);
            final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder(currentState.coordinationMetadata())
                .lastAcceptedConfiguration(votingConfiguration)
                .lastCommittedConfiguration(votingConfiguration)
                .build();

            Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
            // automatically generate a UID for the metadata if we need to
            metadataBuilder.generateClusterUuidIfNeeded();
            metadataBuilder.coordinationMetadata(coordinationMetadata);

            coordinationState.get().setInitialState(ClusterState.builder(currentState).metadata(metadataBuilder).build());
            assert localNodeMayWinElection(getLastAcceptedState()) :
                "initial state does not allow local node to win election: " + getLastAcceptedState().coordinationMetadata();
            preVoteCollector.update(getPreVoteResponse(), null); // pick up the change to last-accepted version
            startElectionScheduler();
            return true;
        }
    }

    // Package-private for testing
    ClusterState improveConfiguration(ClusterState clusterState) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert validVotingConfigExclusionState(clusterState) : clusterState;

        // exclude any nodes whose ID is in the voting config exclusions list ...
        final Stream<String> excludedNodeIds = clusterState.getVotingConfigExclusions().stream().map(VotingConfigExclusion::getNodeId);
        // ... and also automatically exclude the node IDs of master-ineligible nodes that were previously master-eligible and are still in
        // the voting config. We could exclude all the master-ineligible nodes here, but there could be quite a few of them and that makes
        // the logging much harder to follow.
        final Stream<String> masterIneligibleNodeIdsInVotingConfig = StreamSupport.stream(clusterState.nodes().spliterator(), false)
            .filter(n -> n.isMasterNode() == false
                && (clusterState.getLastAcceptedConfiguration().getNodeIds().contains(n.getId())
                || clusterState.getLastCommittedConfiguration().getNodeIds().contains(n.getId())))
            .map(DiscoveryNode::getId);

        final Set<DiscoveryNode> liveNodes = StreamSupport.stream(clusterState.nodes().spliterator(), false)
            .filter(DiscoveryNode::isMasterNode).filter(coordinationState.get()::containsJoinVoteFor).collect(Collectors.toSet());
        final VotingConfiguration newConfig = reconfigurator.reconfigure(liveNodes,
            Stream.concat(masterIneligibleNodeIdsInVotingConfig, excludedNodeIds).collect(Collectors.toSet()),
            getLocalNode(), clusterState.getLastAcceptedConfiguration());

        if (newConfig.equals(clusterState.getLastAcceptedConfiguration()) == false) {
            assert coordinationState.get().joinVotesHaveQuorumFor(newConfig);
            return ClusterState.builder(clusterState).metadata(Metadata.builder(clusterState.metadata())
                .coordinationMetadata(CoordinationMetadata.builder(clusterState.coordinationMetadata())
                    .lastAcceptedConfiguration(newConfig).build())).build();
        }
        return clusterState;
    }

    /*
    * Valid Voting Configuration Exclusion state criteria:
    * 1. Every voting config exclusion with an ID of _absent_ should not match any nodes currently in the cluster by name
    * 2. Every voting config exclusion with a name of _absent_ should not match any nodes currently in the cluster by ID
     */
    static boolean validVotingConfigExclusionState(ClusterState clusterState) {
        Set<VotingConfigExclusion> votingConfigExclusions = clusterState.getVotingConfigExclusions();
        Set<String> nodeNamesWithAbsentId = votingConfigExclusions.stream()
                                                .filter(e -> e.getNodeId().equals(VotingConfigExclusion.MISSING_VALUE_MARKER))
                                                .map(VotingConfigExclusion::getNodeName)
                                                .collect(Collectors.toSet());
        Set<String> nodeIdsWithAbsentName = votingConfigExclusions.stream()
                                                .filter(e -> e.getNodeName().equals(VotingConfigExclusion.MISSING_VALUE_MARKER))
                                                .map(VotingConfigExclusion::getNodeId)
                                                .collect(Collectors.toSet());
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (node.isMasterNode() &&
                (nodeIdsWithAbsentName.contains(node.getId()) || nodeNamesWithAbsentId.contains(node.getName()))) {
                    return false;
            }
        }

        return true;
    }

    private AtomicBoolean reconfigurationTaskScheduled = new AtomicBoolean();

    private void scheduleReconfigurationIfNeeded() {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert mode == Mode.LEADER : mode;
        assert currentPublication.isPresent() == false : "Expected no publication in progress";

        final ClusterState state = getLastAcceptedState();
        if (improveConfiguration(state) != state && reconfigurationTaskScheduled.compareAndSet(false, true)) {
            logger.trace("scheduling reconfiguration");
            masterService.submitStateUpdateTask("reconfigure", new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    reconfigurationTaskScheduled.set(false);
                    synchronized (mutex) {
                        return improveConfiguration(currentState);
                    }
                }

                @Override
                public void onFailure(String source, Exception e) {
                    reconfigurationTaskScheduled.set(false);
                    logger.debug("reconfiguration failed", e);
                }
            });
        }
    }

    // exposed for tests
    boolean missingJoinVoteFrom(DiscoveryNode node) {
        return node.isMasterNode() && coordinationState.get().containsJoinVoteFor(node) == false;
    }

    /**
     * 处理某个join
     * @param join
     */
    private void handleJoin(Join join) {
        synchronized (mutex) {
            // 处理join请求 就是将sourceNode加入到投票箱中
            ensureTermAtLeast(getLocalNode(), join.getTerm()).ifPresent(this::handleJoin);

            if (coordinationState.get().electionWon()) {
                // If we have already won the election then the actual join does not matter for election purposes, so swallow any exception
                final boolean isNewJoinFromMasterEligibleNode = handleJoinIgnoringExceptions(join);

                // If we haven't completely finished becoming master then there's already a publication scheduled which will, in turn,
                // schedule a reconfiguration if needed. It's benign to schedule a reconfiguration anyway, but it might fail if it wins the
                // race against the election-winning publication and log a big error message, which we can prevent by checking this here:
                final boolean establishedAsMaster = mode == Mode.LEADER && getLastAcceptedState().term() == getCurrentTerm();
                if (isNewJoinFromMasterEligibleNode && establishedAsMaster && publicationInProgress() == false) {
                    scheduleReconfigurationIfNeeded();
                }
            } else {
                // 处理join请求 当某个发起startJoin的节点收到超过半数的join时 就代表选举成功了  在预投票阶段到选举出leader的时间中 可能会有其他节点也通过预投票  所以这个节点可能有多个节点发送startJoin
                coordinationState.get().handleJoin(join); // this might fail and bubble up the exception
            }
        }
    }

    /**
     * @return true iff the join was from a new node and was successfully added
     */
    private boolean handleJoinIgnoringExceptions(Join join) {
        try {
            return coordinationState.get().handleJoin(join);
        } catch (CoordinationStateRejectedException e) {
            logger.debug(new ParameterizedMessage("failed to add {} - ignoring", join), e);
            return false;
        }
    }

    public ClusterState getLastAcceptedState() {
        synchronized (mutex) {
            return coordinationState.get().getLastAcceptedState();
        }
    }

    @Nullable
    public ClusterState getApplierState() {
        return applierState;
    }

    private List<DiscoveryNode> getDiscoveredNodes() {
        final List<DiscoveryNode> nodes = new ArrayList<>();
        nodes.add(getLocalNode());
        peerFinder.getFoundPeers().forEach(nodes::add);
        return nodes;
    }

    /**
     *
     * @return
     */
    ClusterState getStateForMasterService() {
        synchronized (mutex) {
            // expose last accepted cluster state as base state upon which the master service
            // speculatively calculates the next cluster state update
            // 获取最新的集群状态对象
            final ClusterState clusterState = coordinationState.get().getLastAcceptedState();
            if (mode != Mode.LEADER || clusterState.term() != getCurrentTerm()) {
                // the master service checks if the local node is the master node in order to fail execution of the state update early
                return clusterStateWithNoMasterBlock(clusterState);
            }
            return clusterState;
        }
    }

    /**
     * 在原有的集群状态下 追加一个非master的阻塞对象 同时将masterId 移除
     * @param clusterState
     * @return
     */
    private ClusterState clusterStateWithNoMasterBlock(ClusterState clusterState) {
        // 确保此时集群中存在一个 master节点
        if (clusterState.nodes().getMasterNodeId() != null) {
            // remove block if it already exists before adding new one
            assert clusterState.blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID) == false :
                "NO_MASTER_BLOCK should only be added by Coordinator";
            final ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(clusterState.blocks()).addGlobalBlock(
                noMasterBlockService.getNoMasterBlock()).build();

            // 这里去掉了 masterId 并且增加了一个block
            final DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder(clusterState.nodes()).masterNodeId(null).build();
            return ClusterState.builder(clusterState).blocks(clusterBlocks).nodes(discoveryNodes).build();
        } else {
            return clusterState;
        }
    }

    /**
     * 将集群变化的事件通知到所有节点  比如当前节点晋升成了master节点
     * @param clusterChangedEvent
     * @param publishListener
     * @param ackListener
     */
    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener) {
        try {
            synchronized (mutex) {
                // 如果当前节点不是leader节点 或者当前节点的任期 不一致 是没有发布到集群的权力的
                if (mode != Mode.LEADER || getCurrentTerm() != clusterChangedEvent.state().term()) {
                    logger.debug(() -> new ParameterizedMessage("[{}] failed publication as node is no longer master for term {}",
                        clusterChangedEvent.source(), clusterChangedEvent.state().term()));
                    publishListener.onFailure(new FailedToCommitClusterStateException("node is no longer master for term " +
                        clusterChangedEvent.state().term() + " while handling publication"));
                    return;
                }

                // 当前正在处理一个发布事件 无法继续发布
                if (currentPublication.isPresent()) {
                    assert false : "[" + currentPublication.get() + "] in progress, cannot start new publication";
                    logger.warn(() -> new ParameterizedMessage("[{}] failed publication as already publication in progress",
                        clusterChangedEvent.source()));
                    publishListener.onFailure(new FailedToCommitClusterStateException("publication " + currentPublication.get() +
                        " already in progress"));
                    return;
                }

                assert assertPreviousStateConsistency(clusterChangedEvent);

                final ClusterState clusterState = clusterChangedEvent.state();

                assert getLocalNode().equals(clusterState.getNodes().get(getLocalNode().getId())) :
                    getLocalNode() + " should be in published " + clusterState;

                // 基于要发布的内容 创建一个上下文对象
                final PublicationTransportHandler.PublicationContext publicationContext =
                    publicationHandler.newPublicationContext(clusterChangedEvent);

                // 将此时最新的集群状态包装成req 对象并发往集群中的其他节点
                final PublishRequest publishRequest = coordinationState.get().handleClientValue(clusterState);
                final CoordinatorPublication publication = new CoordinatorPublication(publishRequest, publicationContext,
                    new ListenableFuture<>(), ackListener, publishListener);
                currentPublication = Optional.of(publication);

                // 将此时集群中所有可观测到的节点设置到2个checker对象中
                final DiscoveryNodes publishNodes = publishRequest.getAcceptedState().nodes();
                leaderChecker.setCurrentNodes(publishNodes);
                followersChecker.setCurrentNodes(publishNodes);
                lagDetector.setTrackedNodes(publishNodes);
                // 开始发布任务
                publication.start(followersChecker.getFaultyNodes());
            }
        } catch (Exception e) {
            logger.debug(() -> new ParameterizedMessage("[{}] publishing failed", clusterChangedEvent.source()), e);
            publishListener.onFailure(new FailedToCommitClusterStateException("publishing failed", e));
        }
    }

    // there is no equals on cluster state, so we just serialize it to XContent and compare Maps
    // deserialized from the resulting JSON
    private boolean assertPreviousStateConsistency(ClusterChangedEvent event) {
        assert event.previousState() == coordinationState.get().getLastAcceptedState() ||
            XContentHelper.convertToMap(
                JsonXContent.jsonXContent, Strings.toString(event.previousState()), false
            ).equals(
                XContentHelper.convertToMap(
                    JsonXContent.jsonXContent,
                    Strings.toString(clusterStateWithNoMasterBlock(coordinationState.get().getLastAcceptedState())),
                    false))
            : Strings.toString(event.previousState()) + " vs "
            + Strings.toString(clusterStateWithNoMasterBlock(coordinationState.get().getLastAcceptedState()));
        return true;
    }

    private <T> ActionListener<T> wrapWithMutex(ActionListener<T> listener) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T t) {
                synchronized (mutex) {
                    listener.onResponse(t);
                }
            }

            @Override
            public void onFailure(Exception e) {
                synchronized (mutex) {
                    listener.onFailure(e);
                }
            }
        };
    }

    /**
     * 在当前节点变成候选者时 停止对外发布的动作
     * @param reason
     */
    private void cancelActivePublication(String reason) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        if (currentPublication.isPresent()) {
            currentPublication.get().cancel(reason);
        }
    }

    public Collection<BiConsumer<DiscoveryNode, ClusterState>> getOnJoinValidators() {
        return onJoinValidators;
    }

    public enum Mode {
        CANDIDATE, LEADER, FOLLOWER
    }

    private class CoordinatorPeerFinder extends PeerFinder {

        /**
         *
         * @param settings
         * @param transportService
         * @param transportAddressConnector  该对象连接到某个地址时会检测是否是master节点 如果不是会触发失败的钩子
         * @param configuredHostsResolver
         */
        CoordinatorPeerFinder(Settings settings, TransportService transportService, TransportAddressConnector transportAddressConnector,
                              ConfiguredHostsResolver configuredHostsResolver) {
            super(settings, transportService, transportAddressConnector,
                singleNodeDiscovery ? hostsResolver -> Collections.emptyList() : configuredHostsResolver);
        }

        @Override
        protected void onActiveMasterFound(DiscoveryNode masterNode, long term) {
            synchronized (mutex) {
                ensureTermAtLeast(masterNode, term);
                joinHelper.sendJoinRequest(masterNode, getCurrentTerm(), joinWithDestination(lastJoin, masterNode, term));
            }
        }

        /**
         * 确保非单节点集群的环境才会进行探测
         * @param transportAddress  发起 peersRequest的节点
         */
        @Override
        protected void startProbe(TransportAddress transportAddress) {
            if (singleNodeDiscovery == false) {
                super.startProbe(transportAddress);
            }
        }

        @Override
        protected void onFoundPeersUpdated() {
            synchronized (mutex) {
                final Iterable<DiscoveryNode> foundPeers = getFoundPeers();
                if (mode == Mode.CANDIDATE) {
                    final VoteCollection expectedVotes = new VoteCollection();
                    foundPeers.forEach(expectedVotes::addVote);
                    expectedVotes.addVote(Coordinator.this.getLocalNode());
                    final boolean foundQuorum = coordinationState.get().isElectionQuorum(expectedVotes);

                    if (foundQuorum) {
                        if (electionScheduler == null) {
                            startElectionScheduler();
                        }
                    } else {
                        closePrevotingAndElectionScheduler();
                    }
                }
            }

            clusterBootstrapService.onFoundPeersUpdated();
        }
    }

    /**
     * 开启选举任务
     */
    private void startElectionScheduler() {
        assert electionScheduler == null : electionScheduler;

        // master节点不需要参与选举
        if (getLocalNode().isMasterNode() == false) {
            return;
        }

        final TimeValue gracePeriod = TimeValue.ZERO;
        electionScheduler = electionSchedulerFactory.startElectionScheduler(gracePeriod, new Runnable() {
            @Override
            public void run() {
                synchronized (mutex) {
                    // 只有在当前节点是 候选人时 才会触发选举
                    if (mode == Mode.CANDIDATE) {
                        final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();

                        // 当前节点如果没有在选举中获胜 结束本次选举
                        if (localNodeMayWinElection(lastAcceptedState) == false) {
                            logger.trace("skip prevoting as local node may not win election: {}",
                                lastAcceptedState.coordinationMetadata());
                            return;
                        }

                        // 关闭上一次投票 ???
                        if (prevotingRound != null) {
                            prevotingRound.close();
                        }
                        // 针对从 peerFinder收集到的node 开始选举  预投票是发往集群中除了当前节点外其他所有节点
                        prevotingRound = preVoteCollector.start(lastAcceptedState, getDiscoveredNodes());
                    }
                }
            }

            @Override
            public String toString() {
                return "scheduling of new prevoting round";
            }
        });
    }

    public Iterable<DiscoveryNode> getFoundPeers() {
        return peerFinder.getFoundPeers();
    }

    /**
     * If there is any current committed publication, this method cancels it.
     * This method is used exclusively by tests.
     * @return true if publication was cancelled, false if there is no current committed publication.
     */
    boolean cancelCommittedPublication() {
        synchronized (mutex) {
            if (currentPublication.isPresent()) {
                final CoordinatorPublication publication = currentPublication.get();
                if (publication.isCommitted()) {
                    publication.cancel("cancelCommittedPublication");
                    logger.debug("Cancelled publication of [{}].", publication);
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * 每当要执行一次发布动作就会生成一个该对象
     */
    class CoordinatorPublication extends Publication {

        private final PublishRequest publishRequest;

        /**
         * 当发布的目标节点是本地节点时 使用这个对象处理
         */
        private final ListenableFuture<Void> localNodeAckEvent;

        /**
         * 当发布的节点是其他节点时  接收其他节点的ack 或者异常信息 并处理
         */
        private final AckListener ackListener;
        private final ActionListener<Void> publishListener;

        /**
         * 该对象定义了如何发送 PublishRequest
         */
        private final PublicationTransportHandler.PublicationContext publicationContext;

        @Nullable // if using single-node discovery
        private final Scheduler.ScheduledCancellable timeoutHandler;
        private final Scheduler.Cancellable infoTimeoutHandler;

        // We may not have accepted our own state before receiving a join from another node, causing its join to be rejected (we cannot
        // safely accept a join whose last-accepted term/version is ahead of ours), so store them up and process them at the end.
        // 在某次发布过程中 每当有某个节点在本轮选举中 选择了该leader节点 就会返回一个join 对象   在收到res时 将join对象添加到list中
        private final List<Join> receivedJoins = new ArrayList<>();
        private boolean receivedJoinsProcessed;

        CoordinatorPublication(PublishRequest publishRequest, PublicationTransportHandler.PublicationContext publicationContext,
                               ListenableFuture<Void> localNodeAckEvent, AckListener ackListener, ActionListener<Void> publishListener) {
            super(publishRequest,
                new AckListener() {
                    @Override
                    public void onCommit(TimeValue commitTime) {
                        ackListener.onCommit(commitTime);
                    }


                    /**
                     * 往target节点暴露自己 并收到了确定信息(ack)
                     * @param node the node  本次目标节点
                     * @param e the optional exception   代表本次处理结果失败
                     */
                    @Override
                    public void onNodeAck(DiscoveryNode node, Exception e) {
                        // acking and cluster state application for local node is handled specially
                        // 如果本次发布的目标节点是自己  将结果设置到 future对象中
                        if (node.equals(getLocalNode())) {
                            synchronized (mutex) {
                                if (e == null) {
                                    localNodeAckEvent.onResponse(null);
                                } else {
                                    localNodeAckEvent.onFailure(e);
                                }
                            }
                        } else {
                            ackListener.onNodeAck(node, e);
                            // 本次成功收到目标节点的ack信息
                            if (e == null) {
                                // 更新该节点的版本号
                                lagDetector.setAppliedVersion(node, publishRequest.getAcceptedState().version());
                            }
                        }
                    }
                },
                transportService.getThreadPool()::relativeTimeInMillis);
            this.publishRequest = publishRequest;
            this.publicationContext = publicationContext;
            this.localNodeAckEvent = localNodeAckEvent;
            this.ackListener = ackListener;
            this.publishListener = publishListener;

            this.timeoutHandler = singleNodeDiscovery ? null : transportService.getThreadPool().schedule(new Runnable() {
                @Override
                public void run() {
                    synchronized (mutex) {
                        cancel("timed out after " + publishTimeout);
                    }
                }

                @Override
                public String toString() {
                    return "scheduled timeout for " + CoordinatorPublication.this;
                }
            }, publishTimeout, Names.GENERIC);

            this.infoTimeoutHandler = transportService.getThreadPool().schedule(new Runnable() {
                @Override
                public void run() {
                    synchronized (mutex) {
                        logIncompleteNodes(Level.INFO);
                    }
                }

                @Override
                public String toString() {
                    return "scheduled timeout for reporting on " + CoordinatorPublication.this;
                }
            }, publishInfoTimeout, Names.GENERIC);
        }

        private void removePublicationAndPossiblyBecomeCandidate(String reason) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

            assert currentPublication.get() == this;
            currentPublication = Optional.empty();
            logger.debug("publication ended unsuccessfully: {}", this);

            // check if node has not already switched modes (by bumping term)
            if (isActiveForCurrentLeader()) {
                becomeCandidate(reason);
            }
        }

        boolean isActiveForCurrentLeader() {
            // checks if this publication can still influence the mode of the current publication
            return mode == Mode.LEADER && publishRequest.getAcceptedState().term() == getCurrentTerm();
        }


        /**
         * 代表本次发布任务已经中止
         * 每次将当前节点修改成候选者时 就会停止向外部发布的任务
         * @param committed
         */
        @Override
        protected void onCompletion(boolean committed) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

            localNodeAckEvent.addListener(new ActionListener<Void>() {
                @Override
                public void onResponse(Void ignore) {
                    assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                    assert committed;

                    receivedJoins.forEach(CoordinatorPublication.this::handleAssociatedJoin);
                    assert receivedJoinsProcessed == false;
                    receivedJoinsProcessed = true;

                    clusterApplier.onNewClusterState(CoordinatorPublication.this.toString(), () -> applierState,
                        new ClusterApplyListener() {
                            @Override
                            public void onFailure(String source, Exception e) {
                                synchronized (mutex) {
                                    removePublicationAndPossiblyBecomeCandidate("clusterApplier#onNewClusterState");
                                }
                                cancelTimeoutHandlers();
                                ackListener.onNodeAck(getLocalNode(), e);
                                publishListener.onFailure(e);
                            }

                            @Override
                            public void onSuccess(String source) {
                                synchronized (mutex) {
                                    assert currentPublication.get() == CoordinatorPublication.this;
                                    currentPublication = Optional.empty();
                                    logger.debug("publication ended successfully: {}", CoordinatorPublication.this);
                                    // trigger term bump if new term was found during publication
                                    updateMaxTermSeen(getCurrentTerm());

                                    if (mode == Mode.LEADER) {
                                        // if necessary, abdicate to another node or improve the voting configuration
                                        boolean attemptReconfiguration = true;
                                        final ClusterState state = getLastAcceptedState(); // committed state
                                        if (localNodeMayWinElection(state) == false) {
                                            final List<DiscoveryNode> masterCandidates = completedNodes().stream()
                                                .filter(DiscoveryNode::isMasterNode)
                                                .filter(node -> nodeMayWinElection(state, node))
                                                .filter(node -> {
                                                    // check if master candidate would be able to get an election quorum if we were to
                                                    // abdicate to it. Assume that every node that completed the publication can provide
                                                    // a vote in that next election and has the latest state.
                                                    final long futureElectionTerm = state.term() + 1;
                                                    final VoteCollection futureVoteCollection = new VoteCollection();
                                                    completedNodes().forEach(completedNode -> futureVoteCollection.addJoinVote(
                                                        new Join(completedNode, node, futureElectionTerm, state.term(), state.version())));
                                                    return electionStrategy.isElectionQuorum(node, futureElectionTerm,
                                                        state.term(), state.version(), state.getLastCommittedConfiguration(),
                                                        state.getLastAcceptedConfiguration(), futureVoteCollection);
                                                })
                                                .collect(Collectors.toList());
                                            if (masterCandidates.isEmpty() == false) {
                                                abdicateTo(masterCandidates.get(random.nextInt(masterCandidates.size())));
                                                attemptReconfiguration = false;
                                            }
                                        }
                                        if (attemptReconfiguration) {
                                            scheduleReconfigurationIfNeeded();
                                        }
                                    }
                                    lagDetector.startLagDetector(publishRequest.getAcceptedState().version());
                                    logIncompleteNodes(Level.WARN);
                                }
                                cancelTimeoutHandlers();
                                ackListener.onNodeAck(getLocalNode(), null);
                                publishListener.onResponse(null);
                            }
                        });
                }

                @Override
                public void onFailure(Exception e) {
                    assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                    removePublicationAndPossiblyBecomeCandidate("Publication.onCompletion(false)");
                    cancelTimeoutHandlers();

                    final FailedToCommitClusterStateException exception = new FailedToCommitClusterStateException("publication failed", e);
                    ackListener.onNodeAck(getLocalNode(), exception); // other nodes have acked, but not the master.
                    publishListener.onFailure(exception);
                }
            }, EsExecutors.newDirectExecutorService(), transportService.getThreadPool().getThreadContext());
        }

        private void cancelTimeoutHandlers() {
            if (timeoutHandler != null) {
                timeoutHandler.cancel();
            }
            infoTimeoutHandler.cancel();
        }

        private void handleAssociatedJoin(Join join) {
            if (join.getTerm() == getCurrentTerm() && missingJoinVoteFrom(join.getSourceNode())) {
                logger.trace("handling {}", join);
                handleJoin(join);
            }
        }

        @Override
        protected boolean isPublishQuorum(VoteCollection votes) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            return coordinationState.get().isPublishQuorum(votes);
        }

        /**
         * 处理由某个节点返回的 publish 结果对象
         * @param sourceNode
         * @param publishResponse
         * @return
         */
        @Override
        protected Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode,
                                                                     PublishResponse publishResponse) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            assert getCurrentTerm() >= publishResponse.getTerm();
            return coordinationState.get().handlePublishResponse(sourceNode, publishResponse);
        }

        /**
         * 代表本次发布的目标节点刚好是本次leader的投票节点
         * @param join
         */
        @Override
        protected void onJoin(Join join) {
            // TODO
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            // TODO
            if (receivedJoinsProcessed) {
                // a late response may arrive after the state has been locally applied, meaning that receivedJoins has already been
                // processed, so we have to handle this late response here.
                handleAssociatedJoin(join);
            } else {
                receivedJoins.add(join);
            }
        }

        /**
         * 当 返回的res中没有携带join时 触发该方法
         * @param discoveryNode
         */
        @Override
        protected void onMissingJoin(DiscoveryNode discoveryNode) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            // The remote node did not include a join vote in its publish response. We do not persist joins, so it could be that the remote
            // node voted for us and then rebooted, or it could be that it voted for a different node in this term. If we don't have a copy
            // of a join from this node then we assume the latter and bump our term to obtain a vote from this node.
            // 是这样 本次为该leader投票的节点肯定是正常的 但是如果没有投票  可能这个节点的状态不正常 这时就更新任期
            if (missingJoinVoteFrom(discoveryNode)) {
                final long term = publishRequest.getAcceptedState().term();
                logger.debug("onMissingJoin: no join vote from {}, bumping term to exceed {}", discoveryNode, term);
                updateMaxTermSeen(term + 1);
            }
        }

        @Override
        protected void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                          ActionListener<PublishWithJoinResponse> responseActionListener) {
            publicationContext.sendPublishRequest(destination, publishRequest, wrapWithMutex(responseActionListener));
        }

        @Override
        protected void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommit,
                                       ActionListener<Empty> responseActionListener) {
            publicationContext.sendApplyCommit(destination, applyCommit, wrapWithMutex(responseActionListener));
        }
    }
}
