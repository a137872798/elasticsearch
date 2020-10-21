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
     * @param broadcastNodes the nodes from whom to request pre-votes   需要发起预投票的所有节点
     * @return the pre-voting round, which can be closed to end the round early.
     * 对所有节点发出一个 preVote请求
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
     * @param request
     * @return
     */
    private PreVoteResponse handlePreVoteRequest(final PreVoteRequest request) {
        // 当前节点收到其他节点的请求 并藉此检测当前节点是否落后 当本节点是leader 且落后时 会降级成candidate 并直接向其他节点发起startJoin请求
        updateMaxTermSeen.accept(request.getCurrentTerm());

        // 无论哪个节点在启动阶段都通过持久化的数据复原了 state 所以该属性必然被设置
        Tuple<DiscoveryNode, PreVoteResponse> state = this.state;
        assert state != null : "received pre-vote request before fully initialised";

        final DiscoveryNode leader = state.v1();
        final PreVoteResponse response = state.v2();

        // 此时不知道集群中的leader节点  返回res 这里包含本节点的term和version  这时可能会比选举的节点高 也可能会比选举的节点低 它会怎么做呢???
        // 在预投票阶段实际上对term是没有要求的
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
         * 在某一轮发起的预投票请求中  每个节点的res都会保存在这里   这些节点将会支持本节点发起startJoin请求
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
         * 往这组节点发送 preVote请求
         * 因为本任务本身就是一个不断循环发起的任务 只要满足半数条件的node能被感知到 就可以开始预投票了 极端情况下 一开始的半数节点全部支持 那么直接可以开始startJoin
         * 只要条件没有满足 会随着定时任务不断地重复检测 以及当新的节点被感知到时 他们会加入到preVote的目标节点内
         * @param broadcastNodes
         */
        void start(final Iterable<DiscoveryNode> broadcastNodes) {
            logger.debug("{} requesting pre-votes from {}", this, broadcastNodes);
            broadcastNodes.forEach(n -> transportService.sendRequest(n, REQUEST_PRE_VOTE_ACTION_NAME, preVoteRequest,
                new TransportResponseHandler<PreVoteResponse>() {
                    @Override
                    public PreVoteResponse read(StreamInput in) throws IOException {
                        return new PreVoteResponse(in);
                    }

                    @Override
                    public void handleResponse(PreVoteResponse response) {
                        handlePreVoteResponse(response, n);
                    }

                    /**
                     * 返回异常信息 代表对端是有leader的  本节点能做的就是2件事
                     * 1.等待finder找到leader节点 并加入集群
                     * 2.在等待一定时间后 继续发起preVote请求 直到通过预投票
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
         * 处理预投票结果
         * @param response
         * @param sender
         */
        private void handlePreVoteResponse(final PreVoteResponse response, final DiscoveryNode sender) {
            if (isClosed.get()) {
                logger.debug("{} is closed, ignoring {} from {}", this, response, sender);
                return;
            }

            // 因为当前不是leader节点 所以该函数的作用 仅仅是更新maxTermSeen
            updateMaxTermSeen.accept(response.getCurrentTerm());

            // 因为对端节点 最后一次持久化的任期 比当前节点的大  也就是本节点的数据是落后与目标节点的  这样是无法通过选举的
            // 目标就是找到超过半数的节点 任期数据比当前节点小 或者相等
            if (response.getLastAcceptedTerm() > clusterState.term()
                || (response.getLastAcceptedTerm() == clusterState.term()
                && response.getLastAcceptedVersion() > clusterState.version())) {
                logger.debug("{} ignoring {} from {} as it is fresher", this, response, sender);
                return;
            }

            // 在term。version 满足条件的情况下 加入到投票箱中
            preVotesReceived.put(sender, response);

            // create a fake VoteCollection based on the pre-votes and check if there is an election quorum
            final VoteCollection voteCollection = new VoteCollection();
            final DiscoveryNode localNode = clusterState.nodes().getLocalNode();

            // 该对象包含了当前节点持久化的最新任期 当前尚未持久化的任期等信息   (当前任期可以理解为在还未选举出leader节点使用的临时任期
            // 而当选出leader后 会在pub中将任期进行持久化)
            final PreVoteResponse localPreVoteResponse = getPreVoteResponse();

            // 将票数放入投票箱中 并检测此时是否满足半数节点要求
            preVotesReceived.forEach((node, preVoteResponse) -> voteCollection.addJoinVote(
                new Join(node, localNode, preVoteResponse.getCurrentTerm(),
                preVoteResponse.getLastAcceptedTerm(), preVoteResponse.getLastAcceptedVersion())));

            // 未满足条件 忽略本次处理
            if (electionStrategy.isElectionQuorum(clusterState.nodes().getLocalNode(), localPreVoteResponse.getCurrentTerm(),
                localPreVoteResponse.getLastAcceptedTerm(), localPreVoteResponse.getLastAcceptedVersion(),
                clusterState.getLastCommittedConfiguration(), clusterState.getLastAcceptedConfiguration(), voteCollection) == false) {
                logger.debug("{} added {} from {}, no quorum yet", this, response, sender);
                return;
            }

            // 代表并发收到2个res 只需要处理一次
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
