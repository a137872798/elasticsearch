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

import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_ID;
import static org.elasticsearch.gateway.ClusterStateUpdaters.hideStateIfNotRecovered;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

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

/**
 * 该对象使用的选举算法不同于raft
 * 针对选举节点会灵活变化的情况进行了改良 )
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
     * 该对象负责处理将节点移除集群  当master节点检测到集群中某些节点滞后 就会被移除  判别条件是version
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
     * 当前集群状态   在该对象重启时 仅包含localNode  相当于是一层缓存  因为集群状态会持久化到文件中
     */
    private volatile ClusterState applierState; // the state that should be exposed to the cluster state applier

    /**
     * 感知master集群中的其他节点 并建立连接  preVote的目标节点就是它们
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
     * 只有在某个节点成为leader后 并在publish时根据此时最新的clusterState 才会检测这些节点
     */
    private final FollowersChecker followersChecker;

    /**
     * 该对象会感知集群的状态变化
     */
    private final ClusterApplier clusterApplier;
    private final Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators;

    /**
     * 当该属性被设置时 代表此时正在执行一个选举任务 选举任务从 preVote开始
     */
    @Nullable
    private Releasable electionScheduler;
    @Nullable
    private Releasable prevotingRound;

    /**
     * 此时从集群中探测到的最大任期
     */
    private long maxTermSeen;

    /**
     * 这个是更新配置对象么
     */
    private final Reconfigurator reconfigurator;

    /**
     * 这个对象是在启动节点时没有传入有关配置时 尝试从其他节点获取的对象 可以先忽略 与选举本身无密切关系
     */
    private final ClusterBootstrapService clusterBootstrapService;

    /**
     * 滞后探测器
     * 该对象同followerChecker对象一样 也是首先要求当前节点是leader节点 并且每次在publish时 更新要检测的节点列表 因为在触发publish时 入参是此时集群中最新的node信息
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
     * 在处理最近一轮选举时 收到startJoin请求时返回的 join对象  一轮中只能设置一次
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
     * @param nodeName         The name of the node, used to name the {@link java.util.concurrent.ExecutorService} of the {@link SeedHostsResolver}.
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

        // 只有在预投票阶段需要执行 updateMaxTermSeen
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
     * 集群中的所有节点只要有一个检测到 master节点下线了
     * 就将自身修改成candidate 并发起预投票 在这个过程中会检测其他节点是否还能连接到master上 只要超过半数无法连接到 就通过了预投票 并且发起startJoin 开始拉票
     *
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

    /**
     * 目前只有master节点有这个权利  就是当它管理下面所有node时 发现某个节点一直无法访问 就将其从集群中剔除
     * 这也会触发pub  该任务本身不是强制的 如果本次没有执行 下次pub会从faultyNodes中找到未处理的任务 并执行
     * @param discoveryNode
     * @param reason
     */
    private void removeNode(DiscoveryNode discoveryNode, String reason) {
        synchronized (mutex) {
            // 确保此时是leader节点
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
     * 当某个leader出现时 会通知clusterState中所有的节点 将他们转换成follower
     *
     * @param followerCheckRequest
     */
    void onFollowerCheckRequest(FollowerCheckRequest followerCheckRequest) {
        synchronized (mutex) {
            // 这里更新当前term 并将自己修改成candidate 并尝试通过finder发现leader 以及加入
            ensureTermAtLeast(followerCheckRequest.getSender(), followerCheckRequest.getTerm());

            // 忽略 在followerChecker中应该已经挡掉这种可能了
            if (getCurrentTerm() != followerCheckRequest.getTerm()) {
                logger.trace("onFollowerCheckRequest: current term is [{}], rejecting {}", getCurrentTerm(), followerCheckRequest);
                throw new CoordinationStateRejectedException("onFollowerCheckRequest: current term is ["
                    + getCurrentTerm() + "], rejecting " + followerCheckRequest);
            }

            // check if node has accepted a state in this term already. If not, this node has never committed a cluster state in this
            // term and therefore never removed the NO_MASTER_BLOCK for this term. This logic ensures that we quickly turn a node
            // into follower, even before receiving the first cluster state update, but also don't have to deal with the situation
            // where we would possibly have to remove the NO_MASTER_BLOCK from the applierState when turning a candidate back to follower.
            // TODO 如果任期一样 代表已经处理过pub了 那么重启的candidate 无法 自动变成follower吗   但是发起选举 如果集群中leader还是有效的 那么选举也会失败
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

    /**
     * 当集群选出 leader节点后 会向所有节点发送 pub请求 当收到超过半数候选节点的有效响应时 会对这些节点发起 commit请求
     * 超过半数后 又收到pubRes时 也会发送commitReq请求
     *
     * @param applyCommitRequest
     * @param applyListener
     */
    private void handleApplyCommit(ApplyCommitRequest applyCommitRequest, ActionListener<Void> applyListener) {
        synchronized (mutex) {
            logger.trace("handleApplyCommit: applying commit {}", applyCommitRequest);

            coordinationState.get().handleCommit(applyCommitRequest);
            // 移除一些recovery相关的blocks 先忽略
            final ClusterState committedState = hideStateIfNotRecovered(coordinationState.get().getLastAcceptedState());
            // 在收到publish请求时 不是应该已经将当前节点修改成follower了么
            applierState = mode == Mode.CANDIDATE ? clusterStateWithNoMasterBlock(committedState) : committedState;
            if (applyCommitRequest.getSourceNode().equals(getLocalNode())) {
                // master node applies the committed state at the end of the publication process, not here.
                // 在本地节点持久化最新的集群数据后 并没有直接暴露到应用层
                applyListener.onResponse(null);
            } else {
                // 只有当pub成功 将数据写入到超过半数的候选节点后 在整个集群范围内这个最新状态才是有效的  此时才应该暴露到应用层
                // 其余无法连接上的节点 在重启后会自动找到leader 并拉取最新的clusterState 并设置到应用层
                // 只要pub成功 其他节点的commit是否成功实际上已经不重要了
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
     * leader会将每次clusterState的变化 通知到其他节点  这里包含普通节点
     *
     * @param publishRequest
     * @return
     */
    PublishWithJoinResponse handlePublishRequest(PublishRequest publishRequest) {
        assert publishRequest.getAcceptedState().nodes().getLocalNode().equals(getLocalNode()) :
            publishRequest.getAcceptedState().nodes().getLocalNode() + " != " + getLocalNode();

        synchronized (mutex) {
            // 找到leader节点
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

            if (publishRequest.getAcceptedState().term() > localState.term()) {
                // only do join validation if we have not accepted state from this master yet
                // TODO 目前只看到有关兼容性检测的逻辑 先忽略  不兼容的话 就会抛出异常 使得pub失败
                onJoinValidators.forEach(a -> a.accept(getLocalNode(), publishRequest.getAcceptedState()));
            }

            // 某个刚启动还没有加入到集群的节点 会通过leader的pub 被动更新term
            ensureTermAtLeast(sourceNode, publishRequest.getAcceptedState().term());
            // 这里才是处理发布请求 并生成结果的步骤
            final PublishResponse publishResponse = coordinationState.get().handlePublishRequest(publishRequest);

            if (sourceNode.equals(getLocalNode())) {
                // 主要是为了更新预投票阶段的version  只有最新的term + version 对应的节点才能发起预投票
                preVoteCollector.update(getPreVoteResponse(), getLocalNode());
            } else {
                // 其余节点只要接收到了发布请求 就代表在当前任期中 自身不是master节点 并且此时集群中已经产生了master节点 就可以将自身转换成 follower节点
                becomeFollower("handlePublishRequest", sourceNode); // also updates preVoteCollector
            }

            // 如果是本轮支持它的node 就会在处理pub请求时 返回join
            return new PublishWithJoinResponse(publishResponse,
                joinWithDestination(lastJoin, sourceNode, publishRequest.getAcceptedState().term()));
        }
    }

    /**
     * 检测最近一次发出的join请求目标地点是否就是 本次收到publish请求的leader节点  也就代表本次leader是否有该节点贡献的一票
     *
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

    /**
     * 关闭预投票任务 有2种情况 一种是已经确定了集群中的leader节点  一种是此时能够探测到的节点已经不足半数了 即使发送了preVote 也无法达到要求
     * 那么当前节点就处于停滞状态 等待 finder对象连接到更多的node
     */
    private void closePrevotingAndElectionScheduler() {
        // 关闭预投票处理对象
        if (prevotingRound != null) {
            prevotingRound.close();
            prevotingRound = null;
        }

        // 如果此时开启了 定时创建 prevotingRound 的任务 那么也关闭
        if (electionScheduler != null) {
            electionScheduler.close();
            electionScheduler = null;
        }
    }

    /**
     * 1.当本节点收到其他节点的 preVote请求时 会先比较任期 检测当前节点是否滞后  如果当前节点不是leader节点不需要处理
     * 因为已经有先发现leader掉线的节点了 所以尽可能将票聚集在它身上  针对直接发现leader的情况 会直接同步任期
     *
     * 2.当发起预投票的节点收到其他节点的preVoteRes时 也会触发该方法 尽可能同步2个节点间的任期   但是这时候没有对任期进行持久化 仅仅是更新了 maxTermSeen
     * 应该是想在一轮预投票中仅同步一次
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
                // 处于发布阶段的话 最后会发现自己已经落后 会自己降级 所以不需要处理
                if (publicationInProgress()) {
                    logger.debug("updateMaxTermSeen: maxTermSeen = {} > currentTerm = {}, enqueueing term bump", maxTermSeen, currentTerm);
                } else {
                    try {
                        logger.debug("updateMaxTermSeen: maxTermSeen = {} > currentTerm = {}, bumping term", maxTermSeen, currentTerm);
                        // 这里只是修改自身的任期 以及降级成candidate 并刷新 lastJoin
                        // 并且随着降级会使得 follower 都检测不到leader 进而将follower也转换成candidate
                        ensureTermAtLeast(getLocalNode(), maxTermSeen);
                        // 低任期发起的startJoin请求肯定会失败 这步应该是没必要的
                        startElection();
                    } catch (Exception e) {
                        logger.warn(new ParameterizedMessage("failed to bump term to {}", maxTermSeen), e);
                        // 处理失败时至少要将自己修改成candidate
                        becomeCandidate("updateMaxTermSeen");
                    }
                }
            }
        }
    }

    /**
     * 当满足了预投票的请求后
     * 开始进行选举 通过预投票的基本要求是 收到超过半数的节点 任期至少与当前节点相同 也就是如果当前节点本身任期小 是无法通过预投票阶段的 只能通过自主发现leader
     */
    private void startElection() {
        synchronized (mutex) {
            // The preVoteCollector is only active while we are candidate, but it does not call this method with synchronisation, so we have
            // to check our mode again here.
            if (mode == Mode.CANDIDATE) {
                // 在配置中发现 当前节点不满足成为leader的条件
                // 最新一期任期选举节点的范围是在上一次提交的VoteConf中 也就是在一次选举中实际上范围是不会变化的
                // 这样预投票还是有它的意义 它可以扫描其他节点 找到落后的节点
                // 这应该是防御性编程吧 应该不会发生
                if (localNodeMayWinElection(getLastAcceptedState()) == false) {
                    logger.trace("skip election as local node may not win it: {}", getLastAcceptedState().coordinationMetadata());
                    return;
                }

                // 这里发起startJoin请求  当发起预投票时 接收端如果是脑裂的leader 也会感知到进而向下面的节点发起startJoin请求
                final StartJoinRequest startJoinRequest
                    = new StartJoinRequest(getLocalNode(), Math.max(getCurrentTerm(), maxTermSeen) + 1);
                logger.debug("starting election with {}", startJoinRequest);
                // 获取此时被连接的所有节点 并发起startJoin请求
                // 这时有可能连接上的节点数不满足超过半数的条件 还是会发起 startJoin  不过不会成功
                getDiscoveredNodes().forEach(node -> joinHelper.sendStartJoinRequest(startJoinRequest, node));
            }
        }
    }

    /**
     * 尝试将某个节点推选为leader
     * @param newMaster
     */
    private void abdicateTo(DiscoveryNode newMaster) {
        assert Thread.holdsLock(mutex);
        assert mode == Mode.LEADER : "expected to be leader on abdication but was " + mode;
        assert newMaster.isMasterNode() : "should only abdicate to master-eligible node but was " + newMaster;

        // 模拟该节点发起 startJoin请求 注意这里使用新的任期  (最大任期+1)
        final StartJoinRequest startJoinRequest = new StartJoinRequest(newMaster, Math.max(getCurrentTerm(), maxTermSeen) + 1);
        logger.info("abdicating to {} with term {}", newMaster, startJoinRequest.getTerm());
        // 通过该node 往所有节点发送 startJoin请求  也就是startJoin请求本身不一定要有本节点发起 他只是代表一个期望竞选的节点
        getLastAcceptedState().nodes().mastersFirstStream().forEach(node -> joinHelper.sendStartJoinRequest(startJoinRequest, node));
        // handling of start join messages on the local node will be dispatched to the generic thread-pool
        assert mode == Mode.LEADER : "should still be leader after sending abdication messages " + mode;
        // explicitly move node to candidate state so that the next cluster state update task yields an onNoLongerMaster event
        // 将本节点降级成candidate
        becomeCandidate("after abdicating to " + newMaster);
    }

    /**
     * 检测本地节点是否有可能在选举中获胜
     *
     * @param lastAcceptedState
     * @return
     */
    private static boolean localNodeMayWinElection(ClusterState lastAcceptedState) {
        final DiscoveryNode localNode = lastAcceptedState.nodes().getLocalNode();
        assert localNode != null;
        return nodeMayWinElection(lastAcceptedState, localNode);
    }

    /**
     * 检测目标节点是否有可能在选举中获胜
     *
     * @param lastAcceptedState 最近一次集群中所有节点的快照数据
     * @param node              目标节点
     * @return
     */
    private static boolean nodeMayWinElection(ClusterState lastAcceptedState, DiscoveryNode node) {
        final String nodeId = node.getId();
        // 这里只是在确保node 必须在某些list中 且没有被排除于选举之外
        return lastAcceptedState.getLastCommittedConfiguration().getNodeIds().contains(nodeId)
            || lastAcceptedState.getLastAcceptedConfiguration().getNodeIds().contains(nodeId)
            || lastAcceptedState.getVotingConfigExclusions().stream().noneMatch(vce -> vce.getNodeId().equals(nodeId));
    }


    /**
     * 有些探测请求中 有可能对端的term 比当前节点大  那么模拟从探测节点收到startJoin请求 这样就可以共用加入集群的请求了
     *
     * @param sourceNode 目标任期对应的leader节点
     * @param targetTerm 目标任期
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
     * 当收到startJoin请求时 根据携带的term信息  生成Join对象
     *
     * @param startJoinRequest
     * @return
     */
    private Join joinLeaderInTerm(StartJoinRequest startJoinRequest) {
        // 在处理过程中是加锁的
        synchronized (mutex) {
            logger.debug("joinLeaderInTerm: for [{}] with term {}", startJoinRequest.getSourceNode(), startJoinRequest.getTerm());

            // 当确定startJoin的任期比当前节点高 返回一个申请加入目标节点的join对象  这里同时还会更新本地的任期 重置投票箱 重置 lastPubVersion等信息
            final Join join = coordinationState.get().handleStartJoin(startJoinRequest);

            // 更新最近一次发出的join请求
            lastJoin = Optional.of(join);
            // 更新任期后 在处理Peer请求时 也能返回最新的任期  这样在尝试选举的阶段  每个节点就能尽可能获取准确的数据
            peerFinder.setCurrentTerm(getCurrentTerm());

            if (mode != Mode.CANDIDATE) {
                becomeCandidate("joinLeaderInTerm"); // updates followersChecker and preVoteCollector
            } else {

                // 先假设当前节点在重启时作为候选节点  并通过finder对象感知到leader节点 且leader节点的term大于当前节点时的场景 就是模拟一个leader节点的startJoin请求
                // 并使用该方法处理请求   更新followerChecker内的数据 这样当leader发起探测请求时就可以感知到这个节点

                followersChecker.updateFastResponseState(getCurrentTerm(), mode);
                // 此时虽然收到了更新的任期信息 但是并不能确保leader就一定是准确的 因为还有可能发生脑裂 所以这里能做的就是更新在预投票阶段对外暴露的任期信息
                // 既然handleStartJoin 中没有抛出异常 那么至少能确保的就是当前节点的任期必然更新了
                preVoteCollector.update(getPreVoteResponse(), null);
            }
            return join;
        }
    }


    /**
     * 当收到某个节点发来的join请求时触发该方法
     * 后启动的节点在感知到集群中存在term超过该节点的leader时 会往leader发起join请求
     * @param joinRequest
     * @param joinCallback 该对象就是适配了 channel   调用该方法就是将结果通过channel 返回给对端
     */
    private void handleJoinRequest(JoinRequest joinRequest, JoinHelper.JoinCallback joinCallback) {
        assert Thread.holdsLock(mutex) == false;
        assert getLocalNode().isMasterNode() : getLocalNode() + " received a join but is not master-eligible";
        logger.trace("handleJoinRequest: as {}, handling {}", mode, joinRequest);

        // 忽略单节点集群
        if (singleNodeDiscovery && joinRequest.getSourceNode().equals(getLocalNode()) == false) {
            joinCallback.onFailure(new IllegalStateException("cannot join node with [" + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey() +
                "] set to [" + DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE + "] discovery"));
            return;
        }

        // 有些连接是通过startJoin 后再发送的  所以不会重复建立连接， 而向是节点主动向leader发起join请求的场景 连接不一定创建了
        transportService.connectToNode(joinRequest.getSourceNode(), ActionListener.wrap(ignore -> {
            // 能接收join请求的 一种是通过预投票的 且在startJoin中检测到任期超过投票者的
            //                  第二种就是leader接收重启的参选节点
            //                  第三种就是非参选节点 只有在leader确定后 会通过finder找到leader 并发起join请求
            // 根据当前情况选择是否去除leader的相关信息
            final ClusterState stateForJoinValidation = getStateForMasterService();

            // 新的节点主动加入到leader 触发的是上面的分支  这里只对应第二三种情况
            if (stateForJoinValidation.nodes().isLocalNodeElectedMaster()) {
                // onJoinValidators 除了最基础的校验兼容性外 用户可以自己定义兼容逻辑
                onJoinValidators.forEach(a -> a.accept(joinRequest.getSourceNode(), stateForJoinValidation));
                // 这里也是兼容性相关的
                if (stateForJoinValidation.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                    // we do this in a couple of places including the cluster update thread. This one here is really just best effort
                    // to ensure we fail as fast as possible.
                    JoinTaskExecutor.ensureMajorVersionBarrier(joinRequest.getSourceNode().getVersion(),
                        stateForJoinValidation.getNodes().getMinNodeVersion());
                }
                // 因为对端可能也设置了自定义检验逻辑  所以要发送一个校验join
                sendValidateJoinRequest(stateForJoinValidation, joinRequest, joinCallback);
            } else {
                // 成功连接到节点后发送处理join
                processJoinRequest(joinRequest, joinCallback);
            }
        }, joinCallback::onFailure));
    }

    /**
     * 当本节点作为leader时 后启动的节点通过finder主动找到了这个节点并尝试加入时 发起join请求后会触发该方法
     * @param stateForJoinValidation
     * @param joinRequest
     * @param joinCallback
     */
    void sendValidateJoinRequest(ClusterState stateForJoinValidation, JoinRequest joinRequest,
                                 JoinHelper.JoinCallback joinCallback) {
        // validate the join on the joining node, will throw a failure if it fails the validation
        // 此时只知道对端的地址信息 将当前leader节点最新的clusterState发送到对端 并且对端可能定义了自己的onJoinValidators 所以二次校验是有必要的
        joinHelper.sendValidateJoinRequest(joinRequest.getSourceNode(), stateForJoinValidation, new ActionListener<Empty>() {
            @Override
            public void onResponse(Empty empty) {
                try {
                    processJoinRequest(joinRequest, joinCallback);
                } catch (Exception e) {
                    joinCallback.onFailure(e);
                }
            }

            /**
             * 对端的校验未通过自然就忽略这个申请join的节点了 触发join失败的回调
             * @param e
             */
            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to validate incoming join request from node [{}]",
                    joinRequest.getSourceNode()), e);
                joinCallback.onFailure(new IllegalStateException("failure when sending a validation request to node", e));
            }
        });
    }

    /**
     * 本节点作为候选者发起startJoin时 收到join请求
     * 或者是当前节点已经是leader了 集群中有新加入的节点在通过finder对象发现leader后 也发起join请求尝试加入集群（在这种情况下还需要在2端分别执行一次校验器）
     * @param joinRequest
     * @param joinCallback
     */
    private void processJoinRequest(JoinRequest joinRequest, JoinHelper.JoinCallback joinCallback) {
        // 只有一种情况没有join  那就是重启的节点的任期 与当前集群中leader节点的任期一样
        final Optional<Join> optionalJoin = joinRequest.getOptionalJoin();
        synchronized (mutex) {
            // 在startJoin阶段 如果本节点传过去的任期低 是不会发起join请求的 而在之后的处理中 会更新join内部的任期 所以这个的任期应该等于当前任期 如果当前节点又增加了任期
            // 那么至少也是 >= 的关系 req的任期不可能更大
            updateMaxTermSeen(joinRequest.getTerm());

            final CoordinationState coordState = coordinationState.get();

            final boolean prevElectionWon = coordState.electionWon();

            // 处理本次joinReq内部的join   当票数足够时  会将coordinationState中的win修改成true
            // 如果join为空 代表目标节点的任期与当前节点一致  实际上只要过了票选阶段这个join对象就没作用了
            optionalJoin.ifPresent(this::handleJoin);

            // 当此节点 已经成为leader后 再处理join请求时 会将该节点加入到clusterState中 之后触发pub 将最新集群状态通知到clusterState下的所有node
            joinAccumulator.handleJoinRequest(joinRequest.getSourceNode(), joinCallback);

            // 如果此时发现当前节点已经获取了足够的选票 晋升成leader
            if (prevElectionWon == false && coordState.electionWon()) {
                becomeLeader("handleJoinRequest");
            }
        }
    }


    /**
     * 成为选举者
     */
    void becomeCandidate(String method) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        logger.debug("{}: coordinator becoming CANDIDATE in term {} (was {}, lastKnownLeader was [{}])",
            method, getCurrentTerm(), mode, lastKnownLeader);

        // 初始状态下 mode为null
        if (mode != Mode.CANDIDATE) {
            final Mode prevMode = mode;
            // 将当前节点的角色变成候选者
            mode = Mode.CANDIDATE;
            // 可能当前节点之前是 master 因为某种原因变成了candidate  并且此时正在往外部发送 pub请求 这里就要提前关闭
            cancelActivePublication("become candidate: " + method);

            // 除了candidate外其余joinAccumulator.close 都是noop 所以不用看
            joinAccumulator.close(mode);
            // 将该对象修改成 candidate角色对应的 accumulator
            joinAccumulator = joinHelper.new CandidateJoinAccumulator();

            // 根据之前持久化的集群状态 访问他们并尝试获取此时集群中所有 参选的节点
            peerFinder.activate(coordinationState.get().getLastAcceptedState().nodes());

            // 该对象启动后 会定期执行joinHelper::logLastFailedJoinAttempt   也就是定期打印设置到 JoinHelper内部的失败的join
            clusterFormationFailureHelper.start();

            // 如果当前节点是从leader 降级成candidate 那么通过置空内部nodes 之后在处理follower的探测请求时 就会返回非leader信息 对象就会尝试发起选举
            leaderChecker.setCurrentNodes(DiscoveryNodes.EMPTY_NODES);
            // 如果当前节点是follower 那么关闭对当前leader节点的检测
            leaderChecker.updateLeader(null);

            // 当从leader变成candidate时 不需要检测其他节点了
            followersChecker.clearCurrentNodes();
            // 更新当前结果状态 这时针对别的节点的请求 会快速返回该结果  此时的任期还是从之前持久化的数据中取出的
            followersChecker.updateFastResponseState(getCurrentTerm(), mode);
            // 当节点从leader降级后 此时不再对任何节点进行滞后探测
            lagDetector.clearTrackedNodes();

            // 如果当前节点是从leader降级
            if (prevMode == Mode.LEADER) {
                cleanMasterService();
            }

            // 如果在初始化信息中包含了masterId信息 这个信息此时是不可靠的 将masterId清除
            // 或者此时master降级成candidate masterId 也是不可靠的 移除
            // 或者此时是follower 升级成candidate 代表检测到master不可靠 也要移除
            if (applierState.nodes().getMasterNodeId() != null) {
                applierState = clusterStateWithNoMasterBlock(applierState);

                // 这里只是触发一组监听器 还不知道监听器具体能做什么   应该跟选举没有太大关系
                clusterApplier.onNewClusterState("becoming candidate: " + method, () -> applierState, (source, e) -> {
                });
            }
        }

        preVoteCollector.update(getPreVoteResponse(), null);
    }

    /**
     * 将当前节点晋升成leader
     *
     * @param method
     */
    void becomeLeader(String method) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;
        assert getLocalNode().isMasterNode() : getLocalNode() + " became a leader but is not master-eligible";

        logger.debug("{}: coordinator becoming LEADER in term {} (was {}, lastKnownLeader was [{}])",
            method, getCurrentTerm(), mode, lastKnownLeader);

        mode = Mode.LEADER;
        // 因为follower是不能变成leader的所以这里一定是 candidate
        // 在每轮选举中 发出join请求时 callback都没有直接触发 而是等待该节点变成了leader后 或者follower后触发close 这时会回调之前的callback通知其他节点选举完成
        // 之后成为master的节点会通知其他节点
        joinAccumulator.close(mode);
        // 将累加器更新成 LeaderJoinAccumulator
        joinAccumulator = joinHelper.new LeaderJoinAccumulator();

        // 更新已知的leader节点
        lastKnownLeader = Optional.of(getLocalNode());

        // 停止对外探测 但是还是接收其他节点的 PeerReq
        peerFinder.deactivate(getLocalNode());
        // 因为选举阶段已经结束了 所以不需要再打印 失败的join信息了
        clusterFormationFailureHelper.stop();

        // 因为选举已经完成  关闭定时器 和预投票   只有当选举结果产生(也就是当前节点转换成leader/follower) 或者当前节点能感知到的节点已经不足半数时 才会关闭选举任务
        closePrevotingAndElectionScheduler();
        // 因为当前节点是leader节点 设置结果到  preVoteCollector 中  这样其他节点发送preVote时 本节点不予通过
        preVoteCollector.update(getPreVoteResponse(), getLocalNode());

        assert leaderChecker.leader() == null : leaderChecker.leader();
        followersChecker.updateFastResponseState(getCurrentTerm(), mode);
    }

    /**
     * 只有在收到 leader的发布请求时 才会将自身修改成follower
     *
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

        // 当leader节点发生了变化 或者是当前节点首次设置成follower
        // 那么就可以重启 masterChecker对象了   masterCheck对象通过定期检测master节点是否下线 尝试发起新一轮选举
        final boolean restartLeaderChecker = (mode == Mode.FOLLOWER && Optional.of(leaderNode).equals(lastKnownLeader)) == false;

        if (mode != Mode.FOLLOWER) {
            mode = Mode.FOLLOWER;
            // 当前如果是candidate节点转换成follower 将会触发关闭累加器的逻辑 会回复之前所有发送join请求的节点异常信息  这里仅提示join失败
            // 之后那些节点会通过finder找到leader 并向leader发起join请求
            joinAccumulator.close(mode);
            // 将累加器切换成follower角色相关的
            joinAccumulator = new JoinHelper.FollowerJoinAccumulator();
            // 这时代表本节点不再是master节点 其他检测的节点就会感知到
            leaderChecker.setCurrentNodes(DiscoveryNodes.EMPTY_NODES);
        }

        // 更新此时已知的集群中leader节点
        lastKnownLeader = Optional.of(leaderNode);
        // 每当一轮的选举结束时就是关闭finder对象的时候
        peerFinder.deactivate(leaderNode);
        // 因为本轮选举已经结束了 所以不再需要打印join失败的信息了  在一轮中到底可以往几个节点发送join请求
        clusterFormationFailureHelper.stop();
        // 关闭预投票和触发预投票的定时任务
        closePrevotingAndElectionScheduler();

        // 如果此时该对象正在进行一个发布动作 也就是当前节点之前还是master节点  在某次选举后生成了新的master节点 并且通知到旧的master节点 这时取消publish任务
        cancelActivePublication("become follower: " + method);
        // 更新此时的 term + version 确保旧的节点无法发起预投票 除非集群中有半数以上的旧节点
        preVoteCollector.update(getPreVoteResponse(), leaderNode);

        // 只有当节点收到 leader的pub请求时 才会更新要检测的节点
        if (restartLeaderChecker) {
            leaderChecker.updateLeader(leaderNode);
        }

        // 应该是只有leader 才要使用该对象
        followersChecker.clearCurrentNodes();
        // 当感知到 checkFollower请求时将当前状态返回
        followersChecker.updateFastResponseState(getCurrentTerm(), mode);
        // follower不需要检测 滞后的节点
        lagDetector.clearTrackedNodes();
    }

    /**
     * 某个节点从leader 降级成 candidate时 触发该函数
     */
    private void cleanMasterService() {

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
                    // 此时返回的 clusterState 中没有node信息 也就是本次不需要发布到任何节点 仅仅针对leader节点做一些清理工作
                    return unchanged();
                }

            });
    }


    /**
     * 该对象是负责处理其他节点的preVote请求的
     *
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

            coordinationState.set(new CoordinationState(getLocalNode(), persistedState, electionStrategy));

            // 从持久化数据中获取当前任期
            peerFinder.setCurrentTerm(getCurrentTerm());

            // 初始化地址解析对象内部的线程池
            configuredHostsResolver.start();

            // 获取之前持久化的最近一次集群数据  (每次某个节点重启时肯定是根据之前持久化的数据尝试恢复)
            final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();

            // 只是打印日志
            if (lastAcceptedState.metadata().clusterUUIDCommitted()) {
                logger.info("cluster UUID [{}]", lastAcceptedState.metadata().clusterUUID());
            }

            // 尝试获取本节点最后一次集群状态中commit阶段的数据 如果没有的话应该是空容器
            final VotingConfiguration votingConfiguration = lastAcceptedState.getLastCommittedConfiguration();

            // 单节点环境先忽略
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
     * 通过该方法启动后 首先依赖与 finder找到尽可能多的master节点  如果直接找到了leader直接发起join请求就好
     * 如果收到超过集群半数的节点没有leader信息 那么leader必然是无效的 开始preVote
     * 反之 必然可以获取到leader信息 这时模拟startJoin请求 更新任期 并往leader节点发起join请求
     * 因为集群是弹性化的  随时可能增加/减少master节点  这时 finderReq的探测功能就显示出来了 会尽可能的感知到最新的master节点集
     * 当集群不可用时 无法增加新的节点数 也就可以确保在这个时刻节点不会再发生变化 而之前提交成功的集群状态应该至少会在半数节点+1上包含完整
     * 而预投票满足的前提就是直到访问到1/2+1的master节点数 那么此时必然能够得知集群下所有的节点数
     */
    @Override
    public void startInitialJoin() {
        synchronized (mutex) {
            becomeCandidate("startInitialJoin");
        }

        // TODO 引导程序本身不影响选举 忽略
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

    /**
     * 代表之前有持久化数据
     *
     * @return
     */
    public boolean isInitialConfigurationSet() {
        return getStateForMasterService().getLastAcceptedConfiguration().isEmpty() == false;
    }

    /**
     * Sets the initial configuration to the given {@link VotingConfiguration}. This method is safe to call
     * more than once, as long as the argument to each call is the same.
     *
     * @param votingConfiguration The nodes that should form the initial configuration.
     * @return whether this call successfully set the initial configuration - if false, the cluster has already been bootstrapped.
     * 这是初始化参与选举的节点配置的  每当选举是否成功时 都要检测票数是否满足
     */
    public boolean setInitialConfiguration(final VotingConfiguration votingConfiguration) {
        synchronized (mutex) {
            // 如果当前节点不是leader 就追加一个noMasterBlocks
            final ClusterState currentState = getStateForMasterService();

            // 如果之前的clusterState中存在 VotingConf 不进行处理
            if (isInitialConfigurationSet()) {
                logger.debug("initial configuration already set, ignoring {}", votingConfiguration);
                return false;
            }

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
    // 更新集群配置  当某个节点加入到leader后也会触发该方法
    ClusterState improveConfiguration(ClusterState clusterState) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert validVotingConfigExclusionState(clusterState) : clusterState;

        // exclude any nodes whose ID is in the voting config exclusions list ...
        // 找到被排除在选举之外的所有nodeId
        final Stream<String> excludedNodeIds = clusterState.getVotingConfigExclusions().stream().map(VotingConfigExclusion::getNodeId);
        // ... and also automatically exclude the node IDs of master-ineligible nodes that were previously master-eligible and are still in
        // the voting config. We could exclude all the master-ineligible nodes here, but there could be quite a few of them and that makes
        // the logging much harder to follow.
        // 找到所有不参与选举的节点
        final Stream<String> masterIneligibleNodeIdsInVotingConfig = StreamSupport.stream(clusterState.nodes().spliterator(), false)
            .filter(n -> n.isMasterNode() == false
                && (clusterState.getLastAcceptedConfiguration().getNodeIds().contains(n.getId())
                || clusterState.getLastCommittedConfiguration().getNodeIds().contains(n.getId())))
            .map(DiscoveryNode::getId);

        // 能够检测到该节点发出的join请求代表着 这个节点此时是存活的 同时他也作为下次选举的候选leader
        final Set<DiscoveryNode> liveNodes = StreamSupport.stream(clusterState.nodes().spliterator(), false)
            .filter(DiscoveryNode::isMasterNode).filter(coordinationState.get()::containsJoinVoteFor).collect(Collectors.toSet());
        // 生成修改后的配置对象
        final VotingConfiguration newConfig = reconfigurator.reconfigure(liveNodes,
            Stream.concat(masterIneligibleNodeIdsInVotingConfig, excludedNodeIds).collect(Collectors.toSet()),
            getLocalNode(), clusterState.getLastAcceptedConfiguration());

        // 代表选举配置发生了变化 返回新的clusterState对象
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

    /**
     * 根据当前存活节点的情况 尝试调整VoteConf 并通知到其他节点
     */
    private void scheduleReconfigurationIfNeeded() {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert mode == Mode.LEADER : mode;
        assert currentPublication.isPresent() == false : "Expected no publication in progress";

        // 当收到pub请求时 该属性就会发生变化
        final ClusterState state = getLastAcceptedState();
        // 检测到选举配置发生了变化 提交一个集群状态 更新的任务 它将会再发起一次publish操作 同时在执行任务前 避免插入重复任务
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
                    // 失败时也不能阻止新的线程继续检测配置
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
     * 处理某个节点加入到集群的请求 这个节点可以是 非参与选举的节点（但是非参选节点只有在确定leader后才会发送 在选举阶段不会发起join请求）
     * 在节点启动阶段无论任何节点都会通过 finder对象找到leader节点 并进行连接 目的就是为了告知leader节点 当前节点存活
     *
     * @param join
     */
    private void handleJoin(Join join) {
        synchronized (mutex) {
            // 如果join的任期比当前任期大 则更新本地任期后继续处理join  这种情况应该是不可能发生的
            ensureTermAtLeast(getLocalNode(), join.getTerm()).ifPresent(this::handleJoin);

            // 当本节点已经竞选成功的情况下 又收到了其他join  也就是后启动的落后的节点感知到leader后主动申请加入  但是当前leader的任期必须比落后的节点高
            // 还有非参选的节点在知道leader节点后也会加入到集群中
            if (coordinationState.get().electionWon()) {
                // If we have already won the election then the actual join does not matter for election purposes, so swallow any exception
                // 返回true 代表该join已经加入到leader的投票箱中了 如果出现异常该节点被否认 同时返回false  加入到投票箱就代表leader认为该节点存活
                final boolean isNewJoinFromMasterEligibleNode = handleJoinIgnoringExceptions(join);

                // If we haven't completely finished becoming master then there's already a publication scheduled which will, in turn,
                // schedule a reconfiguration if needed. It's benign to schedule a reconfiguration anyway, but it might fail if it wins the
                // race against the election-winning publication and log a big error message, which we can prevent by checking this here:
                // 当此时已经成为leader节点时
                final boolean establishedAsMaster = mode == Mode.LEADER && getLastAcceptedState().term() == getCurrentTerm();
                // 此时有一个新的节点加入到集群中 且此时当前节点是leader节点 并且此时没有其他pub任务正在执行
                if (isNewJoinFromMasterEligibleNode && establishedAsMaster && publicationInProgress() == false) {
                    // 检测配置是否发生了变化 如果变化则触发pub 将最新的集群配置同步到所有节点上
                    scheduleReconfigurationIfNeeded();
                }
            } else {
                // 处理join请求 增加票数并尝试晋升
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

    /**
     * 当收到 pub请求时 当前节点的 lastAcceptedState就会变化
     * @return
     */
    public ClusterState getLastAcceptedState() {
        synchronized (mutex) {
            return coordinationState.get().getLastAcceptedState();
        }
    }

    @Nullable
    public ClusterState getApplierState() {
        return applierState;
    }

    /**
     * 获取所有此时连接上的节点数
     *
     * @return
     */
    private List<DiscoveryNode> getDiscoveredNodes() {
        final List<DiscoveryNode> nodes = new ArrayList<>();
        nodes.add(getLocalNode());
        peerFinder.getFoundPeers().forEach(nodes::add);
        return nodes;
    }

    /**
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
     * 当前集群状态如果不存在leader 就会添加2个特殊的block 以及清除 masterId
     *
     * @param clusterState
     * @return
     */
    private ClusterState clusterStateWithNoMasterBlock(ClusterState clusterState) {
        // 此时指定了leader节点
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
     * 将集群变化的事件通知到所有节点  比如当前节点晋升成了master节点  又或者 集群下感知到了新的node并加入到clusterState (通过join请求感知到)
     * 成为leader节点最基本的要求就是首先将最新的集群状态发布到超过半数节点  如果连这次通知都失败了 实际上本次选举就是失败的 所以leader会直接降级成candidate
     *
     * @param clusterChangedEvent 存储了集群的前后状态 以及新增的node
     * @param publishListener     当任务完成时往future中设置结果
     * @param ackListener         桥接到一组 Ack监听器上 可能为空列表 这样就是NOOP
     */
    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener) {
        try {
            synchronized (mutex) {
                // 如果当前节点不是leader节点 或者当前节点的任期 不一致 是没有发布的权力的
                if (mode != Mode.LEADER || getCurrentTerm() != clusterChangedEvent.state().term()) {
                    logger.debug(() -> new ParameterizedMessage("[{}] failed publication as node is no longer master for term {}",
                        clusterChangedEvent.source(), clusterChangedEvent.state().term()));
                    publishListener.onFailure(new FailedToCommitClusterStateException("node is no longer master for term " +
                        clusterChangedEvent.state().term() + " while handling publication"));
                    return;
                }

                // 因为发布任务本身会将最新的集群状态持久化 如果同时执行2个发布任务 且写入到不同的节点中 那么此时clusteState是不可控的 会产生混乱
                // 所以最好的方式就是确保每次的集群配置变化后  才进行持久化 这里指的集群状态配置主要是说参与投票的节点数发生变化 因为每个节点进行选举时都是根据本地的选举配置发起选举的
                // 在发布过程中 其他join都会失败 但是其他节点可以通过finder对象持续不断的发起join请求 直到join成功
                // 原本就在集群中的node 重启时 由于集群状态没有发生变化 所以join会直接成功
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

                // 基于要发布的内容 创建一个上下文对象 该对象已经定义了publish/commit怎么处理
                final PublicationTransportHandler.PublicationContext publicationContext =
                    publicationHandler.newPublicationContext(clusterChangedEvent);

                // 将此时最新的集群状态包装成req对象
                final PublishRequest publishRequest = coordinationState.get().handleClientValue(clusterState);

                // 生成一个包含完整发布逻辑的对象
                final CoordinatorPublication publication = new CoordinatorPublication(publishRequest, publicationContext,
                    new ListenableFuture<>(), ackListener, publishListener);

                currentPublication = Optional.of(publication);

                // 如果本次插入了新的节点 那么本次要通知的节点也包括那个节点 主要是为了计算 pubVote
                final DiscoveryNodes publishNodes = publishRequest.getAcceptedState().nodes();
                // 将当前节点作为leader 暴露给集群的其他节点
                leaderChecker.setCurrentNodes(publishNodes);
                // 因为持有最新的集群节点列表 主动的通知所有节点变成follower
                followersChecker.setCurrentNodes(publishNodes);
                // 设置需要探测的节点
                lagDetector.setTrackedNodes(publishNodes);
                // 开始发布任务  followersChecker.getFaultyNodes() 中包含了因为连接不上从集群中被移除的节点
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
     * 当前节点不再是leader节点时 如果存在某个发布任务 直接关闭
     * 针对脑裂的情况(当前节点是一个旧的leader)
     *
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
         * @param settings
         * @param transportService
         * @param transportAddressConnector 该对象连接到某个地址时会检测是否是master节点 如果不是会触发失败的钩子
         * @param configuredHostsResolver
         */
        CoordinatorPeerFinder(Settings settings, TransportService transportService, TransportAddressConnector transportAddressConnector,
                              ConfiguredHostsResolver configuredHostsResolver) {
            super(settings, transportService, transportAddressConnector,
                singleNodeDiscovery ? hostsResolver -> Collections.emptyList() : configuredHostsResolver);
        }

        /**
         * 在处于candidate阶段时 从集群中其他节点获取leader的位置 当成功连接到leader节点后会触发该方法  由于在同一时刻可能几个节点对应的leader不同 所以还需要传入term信息
         *
         * @param masterNode
         * @param term
         */
        @Override
        protected void onActiveMasterFound(DiscoveryNode masterNode, long term) {
            synchronized (mutex) {
                // 更新此时集群中最新的任期
                ensureTermAtLeast(masterNode, term);
                // 在ensureTermAtLeast中会成功生成一个通往masterNode的join对象  这里发送一个join请求
                // 如果当前启动的节点任期与此时的leader一样 那么 lastJoin为empty
                joinHelper.sendJoinRequest(masterNode, getCurrentTerm(), joinWithDestination(lastJoin, masterNode, term));
            }
        }

        /**
         * 确保非单节点集群的环境才会进行探测
         *
         * @param transportAddress 发起 peersRequest的节点
         */
        @Override
        protected void startProbe(TransportAddress transportAddress) {
            if (singleNodeDiscovery == false) {
                super.startProbe(transportAddress);
            }
        }

        /**
         * 每当与新的节点建立连接或断开连接  都会触发该方法  只有当确保能连接到足够的节点时才会发起预投票
         */
        @Override
        protected void onFoundPeersUpdated() {
            synchronized (mutex) {
                // 获取此时总计连接上的节点数
                final Iterable<DiscoveryNode> foundPeers = getFoundPeers();

                if (mode == Mode.CANDIDATE) {
                    // 将所有连接到的节点  包含自身设置到投票箱中
                    final VoteCollection expectedVotes = new VoteCollection();
                    foundPeers.forEach(expectedVotes::addVote);
                    expectedVotes.addVote(Coordinator.this.getLocalNode());
                    // 如果当前节点之前持久化的选举配置是最新的 那么开始就可以开始预投票了
                    // 如果之前选举配置是过时的
                    // TODO 在极端情况下可能会出现2个leader 比如旧的配置 只有3个节点  新的配置是5个节点
                    // TODO 旧的节点启动只要求2个节点就能开始选举   新的节点中产生脑裂有2个节点脱节  这样就有可能在同一时间开启2个选举 并都成功 这个问题该怎么解决???
                    final boolean foundQuorum = coordinationState.get().isElectionQuorum(expectedVotes);

                    if (foundQuorum) {
                        // 从这里可以看出 只要存在一个正在选举中的工作对象 那么即使之后又有新的节点连接 也不会连续触发选举  也就是做到了去重
                        // electionScheduler 会每隔一段时间根据 peerFinder获取到的节点信息 发送perVote 并直到获取大多数节点的认同
                        if (electionScheduler == null) {
                            startElectionScheduler();
                        }
                    } else {
                        // 一旦检测到此时的节点数不可能满足选举条件时 自动放弃选举 这样也就不会有后续的startJoin操作了
                        closePrevotingAndElectionScheduler();
                    }
                }
            }

            // TODO 该对象本身不影响选举
            clusterBootstrapService.onFoundPeersUpdated();
        }
    }

    /**
     * 开启选举任务
     * 看到的第一个能够触发这里的场景是 某个节点变成候选者后 通过PeerFinder探测集群中所有节点  并且此时与半数的节点连接成功  满足了发送perVote的条件
     * 就会由 candidate 开始触发选举
     * 选举任务本身和 在finder对象中检测到leader并发起join请求是不冲突的
     * 因为在preVote阶段也具备着发现leader的职责  如果leader有效 那么在访问到的半数以上的节点时必然能够知道此时的leader节点
     * 也就是在finder阶段寻找只是顺便的事情
     */
    private void startElectionScheduler() {
        assert electionScheduler == null : electionScheduler;

        // 因为除了master节点外 其他节点也是走这套流程的
        // (通过finder对象找到所有可能成为leader的节点 也就是参与选举的节点 然后从他们那找到leader 并发起join请求
        // 这时有可能会连接到一个即将下线的节点 但是还是选择加入 并且会触发pub  如果pub失败leader节点就会自动让位)
        // 所以需要判断当前节点能否参与选举
        if (getLocalNode().isMasterNode() == false) {
            return;
        }

        final TimeValue gracePeriod = TimeValue.ZERO;
        // 定时执行下面的任务  也就是说在得出一个结果前 (收到第一个startJoin) 会不断的重试 preVote
        // 是这样的 满足半数节点是进行preVote的最低要求 (此时必须要求这些节点都同意当前节点发起startJoin请求)
        // 但是当能感知到更多的节点时  通过preVote的成功率会更高 这种情况会一直持续到本节点确定leader为止
        electionScheduler = electionSchedulerFactory.startElectionScheduler(gracePeriod, new Runnable() {
            @Override
            public void run() {
                synchronized (mutex) {
                    // 只有在当前节点是 候选人时 才会触发选举
                    if (mode == Mode.CANDIDATE) {
                        // 获取最后一次集群的状态信息
                        final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();

                        // 1.如果lastAcceptedState 是旧的 也就是下面的逻辑没有参考价值 那么不满足条件自然是好  如果满足条件  在预投票阶段也无法达到满足半数的条件
                        // 还是要等待finder找到最新的leader节点 并同步数据
                        // 2.如果当前节点处于最新的任期 这时lastAcceptedState是准确的 而当前节点不满足选举条件 自然就无法发起预投票
                        if (localNodeMayWinElection(lastAcceptedState) == false) {
                            logger.trace("skip prevoting as local node may not win election: {}",
                                lastAcceptedState.coordinationMetadata());
                            return;
                        }

                        // 关闭上次的预投票round  这里主要是避免2次触发时间间隔短 上次任务还没有完成
                        if (prevotingRound != null) {
                            prevotingRound.close();
                        }

                        // 在这里会获取一次最新的finder连接上的节点
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
     *
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
     * 每当要执行一次发布动作就会生成一个该对象  同时该对象内部的PublicationContext 定义了处理请求的逻辑
     */
    class CoordinatorPublication extends Publication {


        /**
         * 发布请求本身 包含了最新的集群状态
         */
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


        /**
         * 在处理因为join导致的 clusterState更新 并发起发布请求的场景下   publishListener 对应一个future对象 publish就是在阻塞等待future的结果
         *
         * @param publishRequest
         * @param publicationContext
         * @param localNodeAckEvent  没有对象监听该future
         * @param ackListener        桥接到一组ACK监听器 可能是一个空列表
         * @param publishListener    这个监听器才是关联到join的响应结果的
         */
        CoordinatorPublication(PublishRequest publishRequest, PublicationTransportHandler.PublicationContext publicationContext,
                               ListenableFuture<Void> localNodeAckEvent, AckListener ackListener, ActionListener<Void> publishListener) {
            super(publishRequest,
                new AckListener() {

                    /**
                     * 当从发起某个 pub请求 到超过半数节点成功处理时 触发该方法
                     * @param commitTime the time it took to commit the cluster state  总计消耗了多少时间
                     */
                    @Override
                    public void onCommit(TimeValue commitTime) {
                        ackListener.onCommit(commitTime);
                    }


                    /**
                     * 将此时最新的clusterState发送到目标节点上 并收到了确定信息(ack)
                     * @param node the node  本次目标节点
                     * @param e the optional exception   代表本次处理结果失败
                     */
                    @Override
                    public void onNodeAck(DiscoveryNode node, Exception e) {
                        // acking and cluster state application for local node is handled specially
                        // 如果本次发布的目标节点是自己  将结果设置到 localNodeAckEvent对象中
                        if (node.equals(getLocalNode())) {
                            synchronized (mutex) {
                                if (e == null) {
                                    localNodeAckEvent.onResponse(null);
                                } else {
                                    localNodeAckEvent.onFailure(e);
                                }
                            }
                        } else {
                            // 此时ackListener是NOOP 可以忽略
                            ackListener.onNodeAck(node, e);
                            // 本次成功收到目标节点的ack信息
                            if (e == null) {
                                // 代表此时信息已经发布到对端节点了 所以针对这个节点的探测版本也要更新
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

            /**
             * 发布有一个超时时间 超过时间时 会关闭本次发布任务
             */
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

            // 打印日志的不管
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

        /**
         * 这里代表任务已经失败了
         *
         * @param reason
         */
        private void removePublicationAndPossiblyBecomeCandidate(String reason) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

            assert currentPublication.get() == this;
            currentPublication = Optional.empty();
            logger.debug("publication ended unsuccessfully: {}", this);

            // check if node has not already switched modes (by bumping term)
            // 如果当前是leader 强制降级成candidate
            if (isActiveForCurrentLeader()) {
                becomeCandidate(reason);
            }
        }

        boolean isActiveForCurrentLeader() {
            // checks if this publication can still influence the mode of the current publication
            return mode == Mode.LEADER && publishRequest.getAcceptedState().term() == getCurrentTerm();
        }


        /**
         * 当发布任务完全结束时触发  可能执行成功也可能执行失败
         *
         * @param committed 代表产生了 commit对象  至于有多少commit请求是被成功处理的 这里看不出来
         */
        @Override
        protected void onCompletion(boolean committed) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

            // 因为当前任务已经完成加入的监听器会立即执行
            localNodeAckEvent.addListener(new ActionListener<Void>() {


                /**
                 * 能够进入到onResponse 代表至少进入到commit阶段 也就是至少pub超过半数
                 * 并且本地节点的commit一定会成功这里就是commit成功了
                 * @param ignore
                 */
                @Override
                public void onResponse(Void ignore) {
                    assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                    assert committed;

                    // 内部的join代表此时有多少投票者成功处理了pub请求
                    receivedJoins.forEach(CoordinatorPublication.this::handleAssociatedJoin);
                    assert receivedJoinsProcessed == false;

                    // 这个标记可以先忽略 因为即使收到了 pubRes 但是任务已经被标记成结束了 不会触发onJoin
                    receivedJoinsProcessed = true;

                    // 使用此时最新的集群状态更新到应用层 触发比如配置变化 重新分配分片等逻辑   这里是异步的
                    clusterApplier.onNewClusterState(CoordinatorPublication.this.toString(), () -> applierState,
                        new ClusterApplyListener() {

                            /**
                             * 在处理commit失败 还是会降级
                             * @param source information where the cluster state came from
                             * @param e exception that occurred
                             */
                            @Override
                            public void onFailure(String source, Exception e) {
                                synchronized (mutex) {
                                    removePublicationAndPossiblyBecomeCandidate("clusterApplier#onNewClusterState");
                                }
                                cancelTimeoutHandlers();
                                ackListener.onNodeAck(getLocalNode(), e);
                                publishListener.onFailure(e);
                            }

                            /**
                             * 代表最新的集群状态成功commit到本地
                             * @param source information where the cluster state came from
                             */
                            @Override
                            public void onSuccess(String source) {
                                synchronized (mutex) {
                                    assert currentPublication.get() == CoordinatorPublication.this;
                                    currentPublication = Optional.empty();
                                    logger.debug("publication ended successfully: {}", CoordinatorPublication.this);
                                    // trigger term bump if new term was found during publication
                                    // 在这期间 可能任期又发生了变化
                                    updateMaxTermSeen(getCurrentTerm());

                                    // 当前节点还是leader节点 就代表本节点此时还有有效的 否则会在updateMaxTermSeen中发现自己已经是一个过期节点了
                                    if (mode == Mode.LEADER) {
                                        // if necessary, abdicate to another node or improve the voting configuration
                                        boolean attemptReconfiguration = true;
                                        final ClusterState state = getLastAcceptedState(); // committed state
                                        // 如果当前节点已经不可能在选举中成功   比如选举配置发生了变化 这个节点突然失去了成为leader的资格
                                        if (localNodeMayWinElection(state) == false) {
                                            // 该列表中每个node 都有成为leader的资格
                                            final List<DiscoveryNode> masterCandidates = completedNodes().stream()
                                                .filter(DiscoveryNode::isMasterNode)
                                                // 找到所有有可能选举成功的节点
                                                .filter(node -> nodeMayWinElection(state, node))
                                                .filter(node -> {
                                                    // check if master candidate would be able to get an election quorum if we were to
                                                    // abdicate to it. Assume that every node that completed the publication can provide
                                                    // a vote in that next election and has the latest state.
                                                    // 生成最新的任期
                                                    final long futureElectionTerm = state.term() + 1;
                                                    final VoteCollection futureVoteCollection = new VoteCollection();

                                                    // 每个节点 都将此时所有成功通过commit的包装成join 并加入到投票箱中
                                                    completedNodes().forEach(completedNode -> futureVoteCollection.addJoinVote(
                                                        new Join(completedNode, node, futureElectionTerm, state.term(), state.version())));

                                                    // 只有当选举成功时 这个node才会返回
                                                    return electionStrategy.isElectionQuorum(node, futureElectionTerm,
                                                        state.term(), state.version(), state.getLastCommittedConfiguration(),
                                                        state.getLastAcceptedConfiguration(), futureVoteCollection);
                                                })
                                                .collect(Collectors.toList());
                                            if (masterCandidates.isEmpty() == false) {
                                                // 这里随机选择了一个节点 并尝试将它推举为leader
                                                abdicateTo(masterCandidates.get(random.nextInt(masterCandidates.size())));
                                                attemptReconfiguration = false;
                                            }
                                        }
                                        // 在经过一段时间直到触发回调时 还是leader节点 此时尝试检测选举配置是否更新  变更时还是要将最新的集群状态发布到其他节点上(不仅仅是role为master的节点)
                                        if (attemptReconfiguration) {
                                            scheduleReconfigurationIfNeeded();
                                        }
                                    }
                                    // 开始检测其他节点是否滞后
                                    // 在调用publish时 会更新需要检测滞后的所有节点  也就是那个时刻集群中所有的节点
                                    lagDetector.startLagDetector(publishRequest.getAcceptedState().version());
                                    logIncompleteNodes(Level.WARN);
                                }
                                cancelTimeoutHandlers();
                                ackListener.onNodeAck(getLocalNode(), null);
                                publishListener.onResponse(null);
                            }
                        });
                }

                /**
                 * 失败原本包含3种情况
                 * 1.单个节点上的pub失败
                 * 2.pub没有达到半数所导致的其他节点间接失败
                 * 3.单个节点上的commit失败
                 * 但是leader对本地发起publish/commit时必然会成功 所以进入到这个分支就代表第二种情况  也就是pub失败
                 * @param e
                 */
                @Override
                public void onFailure(Exception e) {
                    assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                    // 因为pub没有达到半数要求 本节点此时无法连接到大部分的节点   即使成为leader 也要降级成candidate
                    removePublicationAndPossiblyBecomeCandidate("Publication.onCompletion(false)");
                    // 关闭2个定时器
                    cancelTimeoutHandlers();

                    final FailedToCommitClusterStateException exception = new FailedToCommitClusterStateException("publication failed", e);
                    // 正常触发ack  因为一开始针对本地节点的ack 会被localNodeAckEvent拦截
                    ackListener.onNodeAck(getLocalNode(), exception); // other nodes have acked, but not the master.

                    // 将结果通知到所有发送join请求的节点上
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

        /**
         * 当某个leader发起pub请求时 在处理pub阶段成功时会将本次选择它的所有投票人的join请求存储下来
         *
         * @param join
         */
        private void handleAssociatedJoin(Join join) {
            // TODO 可能会出现这种情况么  ???
            if (join.getTerm() == getCurrentTerm() && missingJoinVoteFrom(join.getSourceNode())) {
                logger.trace("handling {}", join);
                handleJoin(join);
            }
        }

        /**
         * 检测此时成功响应pub的节点数是否达到半数
         *
         * @param votes
         * @return
         */
        @Override
        protected boolean isPublishQuorum(VoteCollection votes) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            return coordinationState.get().isPublishQuorum(votes);
        }

        /**
         * 处理由某个节点返回的 publish 结果对象
         *
         * @param sourceNode
         * @param publishResponse
         * @return 当此时master节点 收到超过半数的赞同票时 每次收到publishRes 都会返回一个新的commitReq
         */
        @Override
        protected Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode,
                                                                     PublishResponse publishResponse) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            assert getCurrentTerm() >= publishResponse.getTerm();
            return coordinationState.get().handlePublishResponse(sourceNode, publishResponse);
        }

        /**
         * 代表本次发布的目标节点在本轮选举中join了本节点
         *
         * @param join
         */
        @Override
        protected void onJoin(Join join) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            if (receivedJoinsProcessed) {
                // a late response may arrive after the state has been locally applied, meaning that receivedJoins has already been
                // processed, so we have to handle this late response here.
                handleAssociatedJoin(join);
            } else {
                // 在publish的所有target对象还没有全部失活时receivedJoinsProcessed 为false 此时将join加入到容器中
                receivedJoins.add(join);
            }
        }

        /**
         * 本次发布的节点没有在本轮中选举当前节点
         *
         * @param discoveryNode
         */
        @Override
        protected void onMissingJoin(DiscoveryNode discoveryNode) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            // The remote node did not include a join vote in its publish response. We do not persist joins, so it could be that the remote
            // node voted for us and then rebooted, or it could be that it voted for a different node in this term. If we don't have a copy
            // of a join from this node then we assume the latter and bump our term to obtain a vote from this node.
            // 这时有2种情况  第一种该节点在本轮中选择了其他节点 第二种选择了本节点 但是重启了 之后term一样 不会生成join
            // missingJoinVoteFrom 确保当前节点是参与选举的节点 同时这个节点没有投票给当前leader (就是检查joinVote)
            if (missingJoinVoteFrom(discoveryNode)) {
                final long term = publishRequest.getAcceptedState().term();
                logger.debug("onMissingJoin: no join vote from {}, bumping term to exceed {}", discoveryNode, term);
                // TODO 只要出现没有选择自己的节点 就增加任期???
                updateMaxTermSeen(term + 1);
            }
        }

        @Override
        protected void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                          ActionListener<PublishWithJoinResponse> responseActionListener) {
            // wrapWithMutex 确保处理发布结果时 在锁下执行
            publicationContext.sendPublishRequest(destination, publishRequest, wrapWithMutex(responseActionListener));
        }

        @Override
        protected void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommit,
                                       ActionListener<Empty> responseActionListener) {
            publicationContext.sendApplyCommit(destination, applyCommit, wrapWithMutex(responseActionListener));
        }
    }
}
