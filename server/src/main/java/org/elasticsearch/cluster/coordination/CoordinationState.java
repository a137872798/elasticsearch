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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The core class of the cluster state coordination algorithm, directly implementing the
 * <a href="https://github.com/elastic/elasticsearch-formal-models/blob/master/ZenWithTerms/tla/ZenWithTerms.tla">formal model</a>
 * 描述此时的协调状态
 */
public class CoordinationState {

    private static final Logger logger = LogManager.getLogger(CoordinationState.class);

    private final DiscoveryNode localNode;

    private final ElectionStrategy electionStrategy;

    // persisted state
    private final PersistedState persistedState;

    // transient state
    /**
     * 专门用于选举leader的投票箱
     */
    private VoteCollection joinVotes;

    /**
     * 标记此时收到了startJoin 请求
     */
    private boolean startedJoinSinceLastReboot;
    /**
     * 当收到 startJoin 请求时 的代表本次选举已经失败了
     */
    private boolean electionWon;

    /**
     * 当该属性首次被设置时 代表本节点晋升成了leader
     */
    private long lastPublishedVersion;

    /**
     * 在初始化阶段 由最新一次持久化的数据中获得 记录最后一次选举有哪些node加入
     */
    private VotingConfiguration lastPublishedConfiguration;
    private VoteCollection publishVotes;


    /**
     *
     * @param localNode
     * @param persistedState  集群信息的持久化数据
     * @param electionStrategy   采用的选举策略
     */
    public CoordinationState(DiscoveryNode localNode, PersistedState persistedState, ElectionStrategy electionStrategy) {
        this.localNode = localNode;

        // persisted state
        this.persistedState = persistedState;
        this.electionStrategy = electionStrategy;

        // transient state
        this.joinVotes = new VoteCollection();
        this.startedJoinSinceLastReboot = false;
        this.electionWon = false;
        this.lastPublishedVersion = 0L;
        this.lastPublishedConfiguration = persistedState.getLastAcceptedState().getLastAcceptedConfiguration();
        this.publishVotes = new VoteCollection();
    }

    public long getCurrentTerm() {
        return persistedState.getCurrentTerm();
    }

    public ClusterState getLastAcceptedState() {
        return persistedState.getLastAcceptedState();
    }

    public long getLastAcceptedTerm() {
        return getLastAcceptedState().term();
    }

    public long getLastAcceptedVersion() {
        return getLastAcceptedState().version();
    }

    public VotingConfiguration getLastCommittedConfiguration() {
        return getLastAcceptedState().getLastCommittedConfiguration();
    }

    public VotingConfiguration getLastAcceptedConfiguration() {
        return getLastAcceptedState().getLastAcceptedConfiguration();
    }

    public long getLastPublishedVersion() {
        return lastPublishedVersion;
    }

    public boolean electionWon() {
        return electionWon;
    }

    /**
     * 此时 voteCollection中的nodes数 是否达到集群中的半数以上
     * @param joinVotes
     * @return
     */
    public boolean isElectionQuorum(VoteCollection joinVotes) {
        // 通过选举策略对象来决定是否满足条件
        return electionStrategy.isElectionQuorum(localNode, getCurrentTerm(), getLastAcceptedTerm(), getLastAcceptedVersion(),
            getLastCommittedConfiguration(), getLastAcceptedConfiguration(), joinVotes);
    }

    public boolean isPublishQuorum(VoteCollection votes) {
        return votes.isQuorum(getLastCommittedConfiguration()) && votes.isQuorum(lastPublishedConfiguration);
    }

    public boolean containsJoinVoteFor(DiscoveryNode node) {
        return joinVotes.containsVoteFor(node);
    }

    // used for tests
    boolean containsJoin(Join join) {
        return joinVotes.getJoins().contains(join);
    }

    public boolean joinVotesHaveQuorumFor(VotingConfiguration votingConfiguration) {
        return joinVotes.isQuorum(votingConfiguration);
    }

    /**
     * Used to bootstrap a cluster by injecting the initial state and configuration.
     *
     * @param initialState The initial state to use. Must have term 0, version equal to the last-accepted version, and non-empty
     *                     configurations.
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     */
    public void setInitialState(ClusterState initialState) {

        final VotingConfiguration lastAcceptedConfiguration = getLastAcceptedConfiguration();
        if (lastAcceptedConfiguration.isEmpty() == false) {
            logger.debug("setInitialState: rejecting since last-accepted configuration is nonempty: {}", lastAcceptedConfiguration);
            throw new CoordinationStateRejectedException(
                "initial state already set: last-accepted configuration now " + lastAcceptedConfiguration);
        }

        assert getLastAcceptedTerm() == 0 : getLastAcceptedTerm();
        assert getLastCommittedConfiguration().isEmpty() : getLastCommittedConfiguration();
        assert lastPublishedVersion == 0 : lastPublishedVersion;
        assert lastPublishedConfiguration.isEmpty() : lastPublishedConfiguration;
        assert electionWon == false;
        assert joinVotes.isEmpty() : joinVotes;
        assert publishVotes.isEmpty() : publishVotes;

        assert initialState.term() == 0 : initialState + " should have term 0";
        assert initialState.version() == getLastAcceptedVersion() : initialState + " should have version " + getLastAcceptedVersion();
        assert initialState.getLastAcceptedConfiguration().isEmpty() == false;
        assert initialState.getLastCommittedConfiguration().isEmpty() == false;

        persistedState.setLastAcceptedState(initialState);
    }

    /**
     * May be safely called at any time to move this instance to a new term.
     *
     * @param startJoinRequest The startJoinRequest, specifying the node requesting the join.
     * @return A Join that should be sent to the target node of the join.
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     * 处理 startJoin请求
     */
    public Join handleStartJoin(StartJoinRequest startJoinRequest) {
        // 既然要发起一个join请求 那么任期必须比该节点此时的要高
        // 在下面的逻辑中因为更新了任期使得之后的startJoin请求会在这里被拒绝 也就是每个节点还是只能在一轮中回复一个Join请求
        if (startJoinRequest.getTerm() <= getCurrentTerm()) {
            logger.debug("handleStartJoin: ignoring [{}] as term provided is not greater than current term [{}]",
                startJoinRequest, getCurrentTerm());
            throw new CoordinationStateRejectedException("incoming term " + startJoinRequest.getTerm() +
                " not greater than current term " + getCurrentTerm());
        }

        logger.debug("handleStartJoin: leaving term [{}] due to {}", getCurrentTerm(), startJoinRequest);

        // joinVotes 填充了数据应该就代表本节点也正好发起了startJoin请求   之后该容器将会被清空 所以这里打印日志
        if (joinVotes.isEmpty() == false) {
            final String reason;
            if (electionWon == false) {
                reason = "failed election";
            } else if (startJoinRequest.getSourceNode().equals(localNode)) {
                reason = "bumping term";
            } else {
                reason = "standing down as leader";
            }
            logger.debug("handleStartJoin: discarding {}: {}", joinVotes, reason);
        }

        // currentTerm 应该是代表还没有确定leader 仅在startJoin 阶段使用的任期 当确定leader后才会最终确认term
        persistedState.setCurrentTerm(startJoinRequest.getTerm());
        assert getCurrentTerm() == startJoinRequest.getTerm();
        lastPublishedVersion = 0;
        lastPublishedConfiguration = getLastAcceptedConfiguration();
        startedJoinSinceLastReboot = true;
        // 收到startJoin请求的节点 在本次选举中已经失败了 所以重置相关属性
        electionWon = false;
        joinVotes = new VoteCollection();
        publishVotes = new VoteCollection();

        return new Join(localNode, startJoinRequest.getSourceNode(), getCurrentTerm(), getLastAcceptedTerm(),
            getLastAcceptedVersion());
    }

    /**
     * May be called on receipt of a Join.
     *
     * @param join The Join received.
     * @return true iff this instance does not already have a join vote from the given source node for this term
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     * 当收到其他节点发来的join请求时触发
     * 这里主要是为了选举出leader
     */
    public boolean handleJoin(Join join) {
        assert join.targetMatches(localNode) : "handling join " + join + " for the wrong node " + localNode;

        if (join.getTerm() != getCurrentTerm()) {
            logger.debug("handleJoin: ignored join due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), join.getTerm());
            throw new CoordinationStateRejectedException(
                "incoming term " + join.getTerm() + " does not match current term " + getCurrentTerm());
        }

        // TODO
        if (startedJoinSinceLastReboot == false) {
            logger.debug("handleJoin: ignored join as term was not incremented yet after reboot");
            throw new CoordinationStateRejectedException("ignored join as term has not been incremented yet after reboot");
        }

        // 持久化的term应该一致
        final long lastAcceptedTerm = getLastAcceptedTerm();
        if (join.getLastAcceptedTerm() > lastAcceptedTerm) {
            logger.debug("handleJoin: ignored join as joiner has a better last accepted term (expected: <=[{}], actual: [{}])",
                lastAcceptedTerm, join.getLastAcceptedTerm());
            throw new CoordinationStateRejectedException("incoming last accepted term " + join.getLastAcceptedTerm() +
                " of join higher than current last accepted term " + lastAcceptedTerm);
        }

        if (join.getLastAcceptedTerm() == lastAcceptedTerm && join.getLastAcceptedVersion() > getLastAcceptedVersion()) {
            logger.debug(
                "handleJoin: ignored join as joiner has a better last accepted version (expected: <=[{}], actual: [{}]) in term {}",
                getLastAcceptedVersion(), join.getLastAcceptedVersion(), lastAcceptedTerm);
            throw new CoordinationStateRejectedException("incoming last accepted version " + join.getLastAcceptedVersion() +
                " of join higher than current last accepted version " + getLastAcceptedVersion()
                + " in term " + lastAcceptedTerm);
        }

        if (getLastAcceptedConfiguration().isEmpty()) {
            // We do not check for an election won on setting the initial configuration, so it would be possible to end up in a state where
            // we have enough join votes to have won the election immediately on setting the initial configuration. It'd be quite
            // complicated to restore all the appropriate invariants when setting the initial configuration (it's not just electionWon)
            // so instead we just reject join votes received prior to receiving the initial configuration.
            logger.debug("handleJoin: rejecting join since this node has not received its initial configuration yet");
            throw new CoordinationStateRejectedException("rejecting join since this node has not received its initial configuration yet");
        }

        // 将当前的join请求加入到投票箱中
        boolean added = joinVotes.addJoinVote(join);
        boolean prevElectionWon = electionWon;
        // 每当接受到一个新的join 请求时 都要根据此时最新的joinVotes 检测是否完成选举   也就是极端情况可能会有多个node 同时通过预投票 并通过startJoin采集票数
        electionWon = isElectionQuorum(joinVotes);
        assert !prevElectionWon || electionWon : // we cannot go from won to not won
            "locaNode= " + localNode + ", join=" + join + ", joinVotes=" + joinVotes;
        logger.debug("handleJoin: added join {} from [{}] for election, electionWon={} lastAcceptedTerm={} lastAcceptedVersion={}", join,
            join.getSourceNode(), electionWon, lastAcceptedTerm, getLastAcceptedVersion());

        // 代表本次接受join请求使得本节点晋升成了leader
        if (electionWon && prevElectionWon == false) {
            logger.debug("handleJoin: election won in term [{}] with {}", getCurrentTerm(), joinVotes);
            lastPublishedVersion = getLastAcceptedVersion();
        }
        return added;
    }

    /**
     * May be called in order to prepare publication of the given cluster state
     *
     * @param clusterState The cluster state to publish.  这个对象是在当前节点作为leader后接收各种join请求 生成的
     * @return A PublishRequest to publish the given cluster state
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     * 基于当前的集群状态 生成一个发布状态的请求对象
     */
    public PublishRequest handleClientValue(ClusterState clusterState) {
        // 选举失败的节点不具备发布的资格
        if (electionWon == false) {
            logger.debug("handleClientValue: ignored request as election not won");
            throw new CoordinationStateRejectedException("election not won");
        }
        // 这里只是看到 当本节点变成leader节点时这2个值肯定是一致的
        if (lastPublishedVersion != getLastAcceptedVersion()) {
            logger.debug("handleClientValue: cannot start publishing next value before accepting previous one");
            throw new CoordinationStateRejectedException("cannot start publishing next value before accepting previous one");
        }

        // 当任期不匹配时 不能发布数据
        if (clusterState.term() != getCurrentTerm()) {
            logger.debug("handleClientValue: ignored request due to term mismatch " +
                    "(expected: [term {} version >{}], actual: [term {} version {}])",
                getCurrentTerm(), lastPublishedVersion, clusterState.term(), clusterState.version());
            throw new CoordinationStateRejectedException("incoming term " + clusterState.term() + " does not match current term " +
                getCurrentTerm());
        }
        if (clusterState.version() <= lastPublishedVersion) {
            logger.debug("handleClientValue: ignored request due to version mismatch " +
                    "(expected: [term {} version >{}], actual: [term {} version {}])",
                getCurrentTerm(), lastPublishedVersion, clusterState.term(), clusterState.version());
            throw new CoordinationStateRejectedException("incoming cluster state version " + clusterState.version() +
                " lower or equal to last published version " + lastPublishedVersion);
        }

        if (clusterState.getLastAcceptedConfiguration().equals(getLastAcceptedConfiguration()) == false
            && getLastCommittedConfiguration().equals(getLastAcceptedConfiguration()) == false) {
            logger.debug("handleClientValue: only allow reconfiguration while not already reconfiguring");
            throw new CoordinationStateRejectedException("only allow reconfiguration while not already reconfiguring");
        }
        // TODO
        if (joinVotesHaveQuorumFor(clusterState.getLastAcceptedConfiguration()) == false) {
            logger.debug("handleClientValue: only allow reconfiguration if joinVotes have quorum for new config");
            throw new CoordinationStateRejectedException("only allow reconfiguration if joinVotes have quorum for new config");
        }

        assert clusterState.getLastCommittedConfiguration().equals(getLastCommittedConfiguration()) :
            "last committed configuration should not change";

        // 将当前集群的数据标记成最近一次发布的属性
        lastPublishedVersion = clusterState.version();
        lastPublishedConfiguration = clusterState.getLastAcceptedConfiguration();
        // 在发布过程中 会设置 publishVotes
        publishVotes = new VoteCollection();

        logger.trace("handleClientValue: processing request for version [{}] and term [{}]", lastPublishedVersion, getCurrentTerm());

        return new PublishRequest(clusterState);
    }

    /**
     * May be called on receipt of a PublishRequest.
     *
     * @param publishRequest The publish request received.
     * @return A PublishResponse which can be sent back to the sender of the PublishRequest.
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     * 当收到 master节点的发布请求时触发
     */
    public PublishResponse handlePublishRequest(PublishRequest publishRequest) {
        final ClusterState clusterState = publishRequest.getAcceptedState();
        // 在调用该方法之前已经更新过当前节点的任期了 所以此时如果任期不匹配 抛出异常
        if (clusterState.term() != getCurrentTerm()) {
            logger.debug("handlePublishRequest: ignored publish request due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), clusterState.term());
            throw new CoordinationStateRejectedException("incoming term " + clusterState.term() + " does not match current term " +
                getCurrentTerm());
        }

        // 版本号匹配失败的时候抛出异常
        if (clusterState.term() == getLastAcceptedTerm() && clusterState.version() <= getLastAcceptedVersion()) {
            logger.debug("handlePublishRequest: ignored publish request due to version mismatch (expected: >[{}], actual: [{}])",
                getLastAcceptedVersion(), clusterState.version());
            throw new CoordinationStateRejectedException("incoming version " + clusterState.version() +
                " lower or equal to current version " + getLastAcceptedVersion());
        }

        logger.trace("handlePublishRequest: accepting publish request for version [{}] and term [{}]",
            clusterState.version(), clusterState.term());
        // 为集群状态信息做持久化
        persistedState.setLastAcceptedState(clusterState);
        assert getLastAcceptedState() == clusterState;

        // 将此时集群中的 term version作为结果返回
        return new PublishResponse(clusterState.term(), clusterState.version());
    }

    /**
     * May be called on receipt of a PublishResponse from the given sourceNode.
     *
     * @param sourceNode      The sender of the PublishResponse received.
     * @param publishResponse The PublishResponse received.
     * @return An optional ApplyCommitRequest which, if present, may be broadcast to all peers, indicating that this publication
     * has been accepted at a quorum of peers and is therefore committed.
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     * 当master节点收到某个节点返回的publishRes时 触发该方法
     */
    public Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {

        // 非master节点不应该处理该请求
        if (electionWon == false) {
            logger.debug("handlePublishResponse: ignored response as election not won");
            throw new CoordinationStateRejectedException("election not won");
        }

        // 在发布时使用的任期 与当前任期不一致的情况下 已经无法处理这个res了
        if (publishResponse.getTerm() != getCurrentTerm()) {
            logger.debug("handlePublishResponse: ignored publish response due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), publishResponse.getTerm());
            throw new CoordinationStateRejectedException("incoming term " + publishResponse.getTerm()
                + " does not match current term " + getCurrentTerm());
        }

        // 版本号发生变化时 也拒绝处理res
        if (publishResponse.getVersion() != lastPublishedVersion) {
            logger.debug("handlePublishResponse: ignored publish response due to version mismatch (expected: [{}], actual: [{}])",
                lastPublishedVersion, publishResponse.getVersion());
            throw new CoordinationStateRejectedException("incoming version " + publishResponse.getVersion() +
                " does not match current version " + lastPublishedVersion);
        }

        logger.trace("handlePublishResponse: accepted publish response for version [{}] and term [{}] from [{}]",
            publishResponse.getVersion(), publishResponse.getTerm(), sourceNode);

        // 代表发布成功 将结果加入到投票箱中
        publishVotes.addVote(sourceNode);

        // 代表此时已经有超过半数的节点 更新了集群最新的信息
        // 超过半数后 每个publishRes 都可以生成一个 commitReq
        if (isPublishQuorum(publishVotes)) {
            logger.trace("handlePublishResponse: value committed for version [{}] and term [{}]",
                publishResponse.getVersion(), publishResponse.getTerm());
            // 这里生成了一个提交请求
            return Optional.of(new ApplyCommitRequest(localNode, publishResponse.getTerm(), publishResponse.getVersion()));
        }

        return Optional.empty();
    }

    /**
     * May be called on receipt of an ApplyCommitRequest. Updates the committed configuration accordingly.
     *
     * @param applyCommit The ApplyCommitRequest received.
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     * 处理收到的commit请求
     */
    public void handleCommit(ApplyCommitRequest applyCommit) {
        // 代表在收到该请求前 任期已经发生变化 之前的leader已经失效了 忽略结果
        if (applyCommit.getTerm() != getCurrentTerm()) {
            logger.debug("handleCommit: ignored commit request due to term mismatch " +
                    "(expected: [term {} version {}], actual: [term {} version {}])",
                getLastAcceptedTerm(), getLastAcceptedVersion(), applyCommit.getTerm(), applyCommit.getVersion());
            throw new CoordinationStateRejectedException("incoming term " + applyCommit.getTerm() + " does not match current term " +
                getCurrentTerm());
        }
        if (applyCommit.getTerm() != getLastAcceptedTerm()) {
            logger.debug("handleCommit: ignored commit request due to term mismatch " +
                    "(expected: [term {} version {}], actual: [term {} version {}])",
                getLastAcceptedTerm(), getLastAcceptedVersion(), applyCommit.getTerm(), applyCommit.getVersion());
            throw new CoordinationStateRejectedException("incoming term " + applyCommit.getTerm() + " does not match last accepted term " +
                getLastAcceptedTerm());
        }
        if (applyCommit.getVersion() != getLastAcceptedVersion()) {
            logger.debug("handleCommit: ignored commit request due to version mismatch (term {}, expected: [{}], actual: [{}])",
                getLastAcceptedTerm(), getLastAcceptedVersion(), applyCommit.getVersion());
            throw new CoordinationStateRejectedException("incoming version " + applyCommit.getVersion() +
                " does not match current version " + getLastAcceptedVersion());
        }

        logger.trace("handleCommit: applying commit request for term [{}] and version [{}]", applyCommit.getTerm(),
            applyCommit.getVersion());

        persistedState.markLastAcceptedStateAsCommitted();
        assert getLastCommittedConfiguration().equals(getLastAcceptedConfiguration());
    }

    public void invariant() {
        assert getLastAcceptedTerm() <= getCurrentTerm();
        assert electionWon() == isElectionQuorum(joinVotes);
        if (electionWon()) {
            assert getLastPublishedVersion() >= getLastAcceptedVersion();
        } else {
            assert getLastPublishedVersion() == 0L;
        }
        assert electionWon() == false || startedJoinSinceLastReboot;
        assert publishVotes.isEmpty() || electionWon();
    }

    public void close() throws IOException {
        persistedState.close();
    }

    /**
     * Pluggable persistence layer for {@link CoordinationState}.
     */
    public interface PersistedState extends Closeable {

        /**
         * Returns the current term
         */
        long getCurrentTerm();

        /**
         * Returns the last accepted cluster state
         */
        ClusterState getLastAcceptedState();

        /**
         * Sets a new current term.
         * After a successful call to this method, {@link #getCurrentTerm()} should return the last term that was set.
         * The value returned by {@link #getLastAcceptedState()} should not be influenced by calls to this method.
         */
        void setCurrentTerm(long currentTerm);

        /**
         * Sets a new last accepted cluster state.
         * After a successful call to this method, {@link #getLastAcceptedState()} should return the last cluster state that was set.
         * The value returned by {@link #getCurrentTerm()} should not be influenced by calls to this method.
         * 更新此时从master节点收到的最新的集群数据
         */
        void setLastAcceptedState(ClusterState clusterState);

        /**
         * Marks the last accepted cluster state as committed.
         * After a successful call to this method, {@link #getLastAcceptedState()} should return the last cluster state that was set,
         * with the last committed configuration now corresponding to the last accepted configuration, and the cluster uuid, if set,
         * marked as committed.
         * 主要就是为之前持久化的集群状态 设置一个 accepted的标识位    只有确保接收到commit请求  之前的clusterState才能生效
         * 这种情况就是为了避免 在每个节点收到 pub的过程中 leader节点失效  所以在超过半数节点确认ack 并且此时leader能够正常处理的这个状态下 需要一个通知 就是commit
         * 那么如果 节点没有收到commit 会怎么样呢
         */
        default void markLastAcceptedStateAsCommitted() {
            final ClusterState lastAcceptedState = getLastAcceptedState();
            Metadata.Builder metadataBuilder = null;
            if (lastAcceptedState.getLastAcceptedConfiguration().equals(lastAcceptedState.getLastCommittedConfiguration()) == false) {
                final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder(lastAcceptedState.coordinationMetadata())
                        .lastCommittedConfiguration(lastAcceptedState.getLastAcceptedConfiguration())
                        .build();
                metadataBuilder = Metadata.builder(lastAcceptedState.metadata());
                metadataBuilder.coordinationMetadata(coordinationMetadata);
            }
            assert lastAcceptedState.metadata().clusterUUID().equals(Metadata.UNKNOWN_CLUSTER_UUID) == false :
                "received cluster state with empty cluster uuid: " + lastAcceptedState;
            if (lastAcceptedState.metadata().clusterUUID().equals(Metadata.UNKNOWN_CLUSTER_UUID) == false &&
                lastAcceptedState.metadata().clusterUUIDCommitted() == false) {
                if (metadataBuilder == null) {
                    metadataBuilder = Metadata.builder(lastAcceptedState.metadata());
                }
                metadataBuilder.clusterUUIDCommitted(true);
                logger.info("cluster UUID set to [{}]", lastAcceptedState.metadata().clusterUUID());
            }
            if (metadataBuilder != null) {
                setLastAcceptedState(ClusterState.builder(lastAcceptedState).metadata(metadataBuilder).build());
            }
        }

        default void close() throws IOException {
        }
    }

    /**
     * A collection of votes, used to calculate quorums. Optionally records the Joins as well.
     * 包含了所有参与选举的node
     */
    public static class VoteCollection {

        /**
         * 此时已经投票的所有节点
         */
        private final Map<String, DiscoveryNode> nodes;
        private final Set<Join> joins;

        public boolean addVote(DiscoveryNode sourceNode) {
            // 确保此时参与的节点必须是master节点
            // master是不是有2层含义  一层是针对所有node 只有role包含master的node 才能参与投票
            // 还有一层就是针对 所有参与投票的节点(master就是leader)
            return sourceNode.isMasterNode() && nodes.put(sourceNode.getId(), sourceNode) == null;
        }

        /**
         * 获取某个节点发出的投票请求
         * @param join
         * @return
         */
        public boolean addJoinVote(Join join) {
            final boolean added = addVote(join.getSourceNode());
            if (added) {
                joins.add(join);
            }
            return added;
        }

        public VoteCollection() {
            nodes = new HashMap<>();
            joins = new HashSet<>();
        }

        public boolean isQuorum(VotingConfiguration configuration) {
            return configuration.hasQuorum(nodes.keySet());
        }

        public boolean containsVoteFor(DiscoveryNode node) {
            return nodes.containsKey(node.getId());
        }

        public boolean isEmpty() {
            return nodes.isEmpty();
        }

        public Collection<DiscoveryNode> nodes() {
            return Collections.unmodifiableCollection(nodes.values());
        }

        public Set<Join> getJoins() {
            return Collections.unmodifiableSet(joins);
        }

        @Override
        public String toString() {
            return "VoteCollection{votes=" + nodes.keySet() + ", joins=" + joins + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof VoteCollection)) return false;

            VoteCollection that = (VoteCollection) o;

            if (!nodes.equals(that.nodes)) return false;
            return joins.equals(that.joins);
        }

        @Override
        public int hashCode() {
            int result = nodes.hashCode();
            result = 31 * result + joins.hashCode();
            return result;
        }
    }
}
