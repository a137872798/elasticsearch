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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 * 预投票容器
 * 预投票本身是为了防止某个节点发生网络分区时 发起无效投票
 */
public class PreVoteCollector {

    private static final Logger logger = LogManager.getLogger(PreVoteCollector.class);

    public static final String REQUEST_PRE_VOTE_ACTION_NAME = "internal:cluster/request_pre_vote";

    private final TransportService transportService;

    /**
     * 选举逻辑
     */
    private final Runnable startElection;

    /**
     * 该函数的主要作用就是当某个节点是leader节点时 检测集群中是否有更高的term 有的话就代表发生了脑裂
     */
    private final LongConsumer updateMaxTermSeen;
    private final ElectionStrategy electionStrategy;

    // Tuple for simple atomic updates. null until the first call to `update()`.
    // 该对象作为一个响应结果的缓存
    private volatile Tuple<DiscoveryNode, PreVoteResponse> state; // DiscoveryNode component is null if there is currently no known leader.

    /**
     *
     * @param transportService
     * @param startElection  开始选举的函数
     * @param updateMaxTermSeen   更新当前集群最大term的函数
     * @param electionStrategy
     */
    PreVoteCollector(final TransportService transportService, final Runnable startElection, final LongConsumer updateMaxTermSeen,
                     final ElectionStrategy electionStrategy) {
        this.transportService = transportService;
        this.startElection = startElection;
        this.updateMaxTermSeen = updateMaxTermSeen;
        this.electionStrategy = electionStrategy;

        // 注册处理预投票请求
        transportService.registerRequestHandler(REQUEST_PRE_VOTE_ACTION_NAME, Names.GENERIC, false, false,
            PreVoteRequest::new,
            (request, channel, task) -> channel.sendResponse(handlePreVoteRequest(request)));
    }

    /**
     * Start a new pre-voting round.
     *
     * @param clusterState   the last-accepted cluster state   最近一次集群快照中的所有节点
     * @param broadcastNodes the nodes from whom to request pre-votes   本次接收预投票请求的所有节点  包括自身
     * @return the pre-voting round, which can be closed to end the round early.
     * 执行预投票任务 通过向相关节点发送 preVote请求 探测各个节点此时的状况
     */
    public Releasable start(final ClusterState clusterState, final Iterable<DiscoveryNode> broadcastNodes) {
        // 将此时最新的任期信息发送到其他节点
        PreVotingRound preVotingRound = new PreVotingRound(clusterState, state.v2().getCurrentTerm());
        preVotingRound.start(broadcastNodes);
        return preVotingRound;
    }

    // only for testing
    PreVoteResponse getPreVoteResponse() {
        return state.v2();
    }

    // only for testing
    @Nullable
    DiscoveryNode getLeader() {
        return state.v1();
    }


    /**
     * 更新针对其他节点 preVote 的返回结果
     * @param preVoteResponse
     * @param leader
     */
    public void update(final PreVoteResponse preVoteResponse, @Nullable final DiscoveryNode leader) {
        logger.trace("updating with preVoteResponse={}, leader={}", preVoteResponse, leader);
        state = new Tuple<>(leader, preVoteResponse);
    }

    /**
     * 处理预投票请求
     * 也许本节点此时有认定的leader节点
     * @param request
     * @return
     */
    private PreVoteResponse handlePreVoteRequest(final PreVoteRequest request) {
        // 当前节点收到其他节点的请求 并藉此检测当前节点是否落后 当本节点是leader 且落后时 会降级成candidate 并直接向其他节点发起startJoin请求
        updateMaxTermSeen.accept(request.getCurrentTerm());

        // 无论哪个节点在启动阶段都通过持久化的数据复原了 state 所以该属性必然被设置
        Tuple<DiscoveryNode, PreVoteResponse> state = this.state;
        assert state != null : "received pre-vote request before fully initialised";

        // TODO 当本节点就是leader节点 并且发现自己落后时 应该会更新state
        final DiscoveryNode leader = state.v1();
        final PreVoteResponse response = state.v2();

        // 如果此时本节点没有观测到leader 直接返回res
        if (leader == null) {
            return response;
        }

        // 这是特殊情况 leader下线 当前节点还没有检测到下线的时候 leader又上线了 这时可以选择通过 其实就跟 leader == null是一样的
        if (leader.equals(request.getSourceNode())) {
            // This is a _rare_ case where our leader has detected a failure and stepped down, but we are still a follower. It's possible
            // that the leader lost its quorum, but while we're still a follower we will not offer joins to any other node so there is no
            // major drawback in offering a join to our old leader. The advantage of this is that it makes it slightly more likely that the
            // leader won't change, and also that its re-election will happen more quickly than if it had to wait for a quorum of followers
            // to also detect its failure.
            return response;
        }

        // 只要其他节点认为leader节点还存活 就会抛出异常 但是这时并没有告诉发起者 哪个节点是leader
        throw new CoordinationStateRejectedException("rejecting " + request + " as there is already a leader");
    }

    @Override
    public String toString() {
        return "PreVoteCollector{" +
            "state=" + state +
            '}';
    }

    /**
     * 该对象可以向多个节点发起 preVote请求
     */
    private class PreVotingRound implements Releasable {

        /**
         * 存储所有term/version 小于本节点的node  并且这些节点未感知到leader节点
         */
        private final Map<DiscoveryNode, PreVoteResponse> preVotesReceived = newConcurrentMap();

        /**
         * 当满足预投票的条件时 设置该标识为true
         */
        private final AtomicBoolean electionStarted = new AtomicBoolean();
        /**
         * 因为发往每个节点的预投票请求都是一样的 所以直接用单例就好
         */
        private final PreVoteRequest preVoteRequest;
        private final ClusterState clusterState;
        private final AtomicBoolean isClosed = new AtomicBoolean();

        PreVotingRound(final ClusterState clusterState, final long currentTerm) {
            this.clusterState = clusterState;
            // 每次发起预投票请求时 都会以当前节点最新的任期去触发   注意在预投票阶段任期还没有+1
            preVoteRequest = new PreVoteRequest(transportService.getLocalNode(), currentTerm);
        }

        /**
         * @param broadcastNodes   指定本次要通知的所有节点 包含了自身    这些节点是通过finder对象找到的
         */
        void start(final Iterable<DiscoveryNode> broadcastNodes) {
            logger.debug("{} requesting pre-votes from {}", this, broadcastNodes);
            broadcastNodes.forEach(n -> transportService.sendRequest(n, REQUEST_PRE_VOTE_ACTION_NAME, preVoteRequest,
                new TransportResponseHandler<PreVoteResponse>() {
                    @Override
                    public PreVoteResponse read(StreamInput in) throws IOException {
                        return new PreVoteResponse(in);
                    }

                    /**
                     * 当其他节点没有确定leader节点时触发该方法  就是正常情况 因为在第一轮 finder阶段 有足够多的node感知不到leader才会进入预投票阶段
                     * 这里主要的作用就是感知其他节点的term
                     * @param response
                     */
                    @Override
                    public void handleResponse(PreVoteResponse response) {
                        handlePreVoteResponse(response, n);
                    }

                    /**
                     * 如果对端节点已经检测到某个leader节点 就会返回异常信息  并且在 finder对象中 探测对端节点时 会将已知的leader节点返回 并在coordinator中处理 所以这里可以不做处理
                     * @param exp
                     */
                    @Override
                    public void handleException(TransportException exp) {
                        logger.debug(new ParameterizedMessage("{} failed", this), exp);
                    }

                    @Override
                    public String executor() {
                        return Names.GENERIC;
                    }

                    @Override
                    public String toString() {
                        return "TransportResponseHandler{" + PreVoteCollector.this + ", node=" + n + '}';
                    }
                }));
        }

        /**
         * 处理预投票结果  代表此时对端未检测到启动的leader节点
         * @param response
         * @param sender
         */
        private void handlePreVoteResponse(final PreVoteResponse response, final DiscoveryNode sender) {
            // 本对象已经被关闭了 忽略res 比如已经选出leader 或者此时节点数又不满足 预投票要求
            if (isClosed.get()) {
                logger.debug("{} is closed, ignoring {} from {}", this, response, sender);
                return;
            }

            // 因为当前不是leader节点 所以该函数的作用 仅仅是更新maxTermSeen
            // 预投票阶段 仅检查leader是否过期 并没有对新的任期进行持久化
            updateMaxTermSeen.accept(response.getCurrentTerm());

            // 如果对端的请求比自身要大 必然不会认可本节点 所以不需要继续处理  但是只要满足 超半数的节点认可本对象即可
            if (response.getLastAcceptedTerm() > clusterState.term()
                || (response.getLastAcceptedTerm() == clusterState.term()
                && response.getLastAcceptedVersion() > clusterState.version())) {
                logger.debug("{} ignoring {} from {} as it is fresher", this, response, sender);
                return;
            }

            // 存在一个任期比当前节点小的节点  并且该节点未感知到leader
            preVotesReceived.put(sender, response);

            // create a fake VoteCollection based on the pre-votes and check if there is an election quorum
            final VoteCollection voteCollection = new VoteCollection();
            final DiscoveryNode localNode = clusterState.nodes().getLocalNode();

            // 该对象包含了本节点最新的任期信息
            final PreVoteResponse localPreVoteResponse = getPreVoteResponse();

            // 每收到一次请求时 都尝试将之前收到的所有请求加入到投票箱中
            // 预投票的意义就是采集足够多的 支持者
            preVotesReceived.forEach((node, preVoteResponse) -> voteCollection.addJoinVote(
                new Join(node, localNode, preVoteResponse.getCurrentTerm(),
                preVoteResponse.getLastAcceptedTerm(), preVoteResponse.getLastAcceptedVersion())));

            // 本次支持者数量未达到要求
            if (electionStrategy.isElectionQuorum(clusterState.nodes().getLocalNode(), localPreVoteResponse.getCurrentTerm(),
                localPreVoteResponse.getLastAcceptedTerm(), localPreVoteResponse.getLastAcceptedVersion(),
                clusterState.getLastCommittedConfiguration(), clusterState.getLastAcceptedConfiguration(), voteCollection) == false) {
                logger.debug("{} added {} from {}, no quorum yet", this, response, sender);
                return;
            }

            // 每一轮预投票 只要有一次节点数满足要求就可以了  实际上并没有要求在这一轮明确直到哪些节点支持  只是需要这个数量而已
            // 但是通过finder探测到的所有节点会借着这个时候检测双端的任期
            if (electionStarted.compareAndSet(false, true) == false) {
                logger.debug("{} added {} from {} but election has already started", this, response, sender);
                return;
            }

            logger.debug("{} added {} from {}, starting election", this, response, sender);
            // 满足选举条件 此时开始选举
            startElection.run();
        }

        @Override
        public String toString() {
            return "PreVotingRound{" +
                "preVotesReceived=" + preVotesReceived +
                ", electionStarted=" + electionStarted +
                ", preVoteRequest=" + preVoteRequest +
                ", isClosed=" + isClosed +
                '}';
        }

        @Override
        public void close() {
            final boolean isNotAlreadyClosed = isClosed.compareAndSet(false, true);
            assert isNotAlreadyClosed;
        }
    }
}
