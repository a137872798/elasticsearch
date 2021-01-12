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
 * 该对象决定了选举的状态  比如当前采集到多少票数  条件是否允许触发某种动作 比如晋升
 */
public class CoordinationState {

    private static final Logger logger = LogManager.getLogger(CoordinationState.class);

    private final DiscoveryNode localNode;

    private final ElectionStrategy electionStrategy;

    // persisted state
    private final PersistedState persistedState;

    // transient state
    /**
     * 接收其他节点返回的 join请求
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
     * 当某个节点晋升成leader时 会设置成 getLastAcceptedVersion
     */
    private long lastPublishedVersion;

    /**
     * 每当更新任期时 会记录之前所有参与选举的节点配置
     */
    private VotingConfiguration lastPublishedConfiguration;
    private VoteCollection publishVotes;


    /**
     *
     * @param localNode 本节点信息
     * @param persistedState  该对象具备将clusterState持久化的能力   在masterNode对应直接持久化对象  在dataNode中则是先将数据暂存到内存中
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
     * 检测当前感受到的选举节点数能否触发选举
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
     * 当收到某个节点的  加团申请    本节点不一定会通过
     */
    public Join handleStartJoin(StartJoinRequest startJoinRequest) {
        // 当本节点收到落后的请求时 直接拒绝  但是不影响发起startJoin的节点  只要超半数收到请求 还是可以晋升成leader
        // 通过预投票的节点在 发起startJoin时 会将term+1
        if (startJoinRequest.getTerm() <= getCurrentTerm()) {
            logger.debug("handleStartJoin: ignoring [{}] as term provided is not greater than current term [{}]",
                startJoinRequest, getCurrentTerm());
            throw new CoordinationStateRejectedException("incoming term " + startJoinRequest.getTerm() +
                " not greater than current term " + getCurrentTerm());
        }

        logger.debug("handleStartJoin: leaving term [{}] due to {}", getCurrentTerm(), startJoinRequest);

        // joinVotes 填充了数据就代表本节点也正好发起了startJoin请求
        // 但是由于收到的新请求的任期更大 所以直接放弃了本次选举   这里仅是打印日志 还没有做清理操作
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

        // 这里同步任期
        persistedState.setCurrentTerm(startJoinRequest.getTerm());
        assert getCurrentTerm() == startJoinRequest.getTerm();
        // 每一个term不一定会选举出leader 但是每个term至多对应一个leader 并且每发起一次pub请求都会增加 pubVersion
        lastPublishedVersion = 0;

        // 最后一次发布的配置项会被记录下来 注意不是 commit
        lastPublishedConfiguration = getLastAcceptedConfiguration();

        // 当收到某节点发送的startJoin时 会修改该标识
        startedJoinSinceLastReboot = true;

        // 因为收到了任期更大的节点的startJoin请求 所以本次选举自动失败
        // 注意 在极端情况下允许多个节点同时进入预投票阶段 每个节点都会拒绝其他节点的startJoin请求 但是本轮很有可能每个节点各自拉拢了一些跟随着 但是没有到1/2的界限导致本轮选举无结果
        // 甚至多个不同的term 也可能同时发起 startJoin请求 但是低term的接受到高term的就会自动放弃 并尝试加入到发起startJoin的节点
        electionWon = false;

        // 本节点在本轮已经无法胜出 选择跟随发起节点
        joinVotes = new VoteCollection();
        publishVotes = new VoteCollection();

        // 生成一个加入到目标节点的join请求
        return new Join(localNode, startJoinRequest.getSourceNode(), getCurrentTerm(), getLastAcceptedTerm(),
            getLastAcceptedVersion());
    }

    /**
     * May be called on receipt of a Join.
     *
     * @param join The Join received.
     * @return true iff this instance does not already have a join vote from the given source node for this term
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     * 收到某节点发送的join请求
     */
    public boolean handleJoin(Join join) {
        assert join.targetMatches(localNode) : "handling join " + join + " for the wrong node " + localNode;

        // 要求处理join时  join.term 与本节点的term必须相同
        if (join.getTerm() != getCurrentTerm()) {
            logger.debug("handleJoin: ignored join due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), join.getTerm());
            throw new CoordinationStateRejectedException(
                "incoming term " + join.getTerm() + " does not match current term " + getCurrentTerm());
        }

        // 发起startJoin的节点必然能保证在处理其他节点的join请求前会先处理投选自己的join请求  同时还会重置投票箱
        if (startedJoinSinceLastReboot == false) {
            logger.debug("handleJoin: ignored join as term was not incremented yet after reboot");
            throw new CoordinationStateRejectedException("ignored join as term has not been incremented yet after reboot");
        }

        final long lastAcceptedTerm = getLastAcceptedTerm();
        // join请求应该比本节点 的数据要旧
        if (join.getLastAcceptedTerm() > lastAcceptedTerm) {
            logger.debug("handleJoin: ignored join as joiner has a better last accepted term (expected: <=[{}], actual: [{}])",
                lastAcceptedTerm, join.getLastAcceptedTerm());
            throw new CoordinationStateRejectedException("incoming last accepted term " + join.getLastAcceptedTerm() +
                " of join higher than current last accepted term " + lastAcceptedTerm);
        }

        // 如果在同一个任期中 但是join 此时集群的集群状态版本更新 也是异常情况
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
        // 每当接受到一个新的join 请求时 都要根据此时最新的joinVotes 检测是否完成选举   极端情况可能会有多个node 同时通过预投票 并通过startJoin采集票数
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
     * 当收到leader节点的发布请求时触发
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

        // 代表在同一任期中 触发了不止一次的pub 每次version都会更新 收到的新的version 必须比之前的新
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
     * @param sourceNode      The sender of the PublishResponse received.     该res是由哪个节点发出的
     * @param publishResponse The PublishResponse received.
     * @return An optional ApplyCommitRequest which, if present, may be broadcast to all peers, indicating that this publication
     * has been accepted at a quorum of peers and is therefore committed.
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     * 当leader节点收到某个节点返回的publishRes时 触发该方法
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

        // pub本身是串行执行的 执行完一个 增加一次版本号 所以旧的任务不予处理
        if (publishResponse.getVersion() != lastPublishedVersion) {
            logger.debug("handlePublishResponse: ignored publish response due to version mismatch (expected: [{}], actual: [{}])",
                lastPublishedVersion, publishResponse.getVersion());
            throw new CoordinationStateRejectedException("incoming version " + publishResponse.getVersion() +
                " does not match current version " + lastPublishedVersion);
        }

        logger.trace("handlePublishResponse: accepted publish response for version [{}] and term [{}] from [{}]",
            publishResponse.getVersion(), publishResponse.getTerm(), sourceNode);

        // 代表发布成功 将结果加入到投票箱中   注意这里加入投票箱的前提是必须是参与选举的节点  也就是pub虽然针对的是所有节点 但是只有写入超过半数的参选节点 才算pub成功
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
        // 处理commit是在处理pub之后的 在pub时 getLastAcceptedTerm 已经与getCurrentTerm 做同步了 所以应该是一致的
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

        // 在收到  pub请求的节点是更新 lastAcceptedState  而在commit阶段则是将它标记成 committed
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
                    // 主要就是在这里一步设置了一个 lastCommittedConf
                        .lastCommittedConfiguration(lastAcceptedState.getLastAcceptedConfiguration())
                        .build();
                metadataBuilder = Metadata.builder(lastAcceptedState.metadata());
                metadataBuilder.coordinationMetadata(coordinationMetadata);
            }
            assert lastAcceptedState.metadata().clusterUUID().equals(Metadata.UNKNOWN_CLUSTER_UUID) == false :
                "received cluster state with empty cluster uuid: " + lastAcceptedState;
            // 这里才是代表本次集群状态是提交过的么
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
     * 代表某次选举的投票箱 比如 预投票
     */
    public static class VoteCollection {

        /**
         * 当前感知到的集群中所有masterNode
         */
        private final Map<String, DiscoveryNode> nodes;
        private final Set<Join> joins;

        /**
         * 增加一票
         * @param sourceNode  支持者的节点
         * @return
         */
        public boolean addVote(DiscoveryNode sourceNode) {
            // 要求支持者必须是参选节点 并且是首次加入
            return sourceNode.isMasterNode() && nodes.put(sourceNode.getId(), sourceNode) == null;
        }

        /**
         * 本节点获得了一个支持者
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
