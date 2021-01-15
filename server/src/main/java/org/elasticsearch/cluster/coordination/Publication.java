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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher.AckListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * 每当clusterState 发生变化时 会发布到其他所有的节点
 */
public abstract class Publication {

    protected final Logger logger = LogManager.getLogger(getClass());

    /**
     * 代表本次发布相关的所有目标节点
     */
    private final List<PublicationTarget> publicationTargets;
    private final PublishRequest publishRequest;
    private final AckListener ackListener;

    /**
     * 该函数负责获取当前时间戳
     */
    private final LongSupplier currentTimeSupplier;
    private final long startTime;

    /**
     * 准备往确认的节点发送commit请求
     */
    private Optional<ApplyCommitRequest> applyCommitRequest; // set when state is committed

    // 代表本次发布动作完成或关闭
    private boolean isCompleted; // set when publication is completed
    private boolean cancelled; // set when publication is cancelled


    /**
     *
     * @param publishRequest   本次要发布的数据 内部包含了最新的 clusterState
     * @param ackListener      当每个节点接收到请求时 要触发ack函数
     * @param currentTimeSupplier  提供当前时间的函数
     */
    public Publication(PublishRequest publishRequest, AckListener ackListener, LongSupplier currentTimeSupplier) {
        this.publishRequest = publishRequest;
        this.ackListener = ackListener;
        this.currentTimeSupplier = currentTimeSupplier;
        startTime = currentTimeSupplier.getAsLong();
        applyCommitRequest = Optional.empty();
        // 在选举阶段  只要收到超半数的masterNode 就可以晋升成功  这个全数指的是 voteConfiguration
        // 而在发布阶段 除了通知 masterNode  普通的node 也需要通知 所以就将请求发往之前clusterState中记录的所有node
        // 应该是要发布到 其中 masterNode的一半以上才算成功  至于普通节点 成功再多也不作数
        publicationTargets = new ArrayList<>(publishRequest.getAcceptedState().getNodes().getNodes().size());
        // 注意这里发布的节点包含非master节点    将每个节点包装成 target 对象
        publishRequest.getAcceptedState().getNodes().mastersFirstStream().forEach(n -> publicationTargets.add(new PublicationTarget(n)));
    }

    /**
     * 开始执行发布任务
     * @param faultyNodes  这组node 代表follower长期连接不上 自动被踢出cluster的节点
     */
    public void start(Set<DiscoveryNode> faultyNodes) {
        logger.trace("publishing {} to {}", publishRequest, publicationTargets);

        for (final DiscoveryNode faultyNode : faultyNodes) {
            // 未长时间无法连接上的节点设置 失败
            onFaultyNode(faultyNode);
        }
        // 检测此时是否已经有超1/2节点失败 是的话 提前结束任务
        onPossibleCommitFailure();
        publicationTargets.forEach(PublicationTarget::sendPublishRequest);
    }

    /**
     * 处于某种原因关闭该对象
     * 比如master节点降级成 candidate
     * 又或者 发布任务超时
     * @param reason
     */
    public void cancel(String reason) {
        if (isCompleted) {
            return;
        }

        assert cancelled == false;
        cancelled = true;
        // 此时还不满足 commit条件
        if (applyCommitRequest.isPresent() == false) {
            logger.debug("cancel: [{}] cancelled before committing (reason: {})", this, reason);
            // fail all current publications
            final Exception e = new ElasticsearchException("publication cancelled before committing: " + reason);
            // 立即关闭所有节点对应的对象
            publicationTargets.stream().filter(PublicationTarget::isActive).forEach(pt -> pt.setFailed(e));
        }
        // 代表本对象完成任务
        onPossibleCompletion();
    }

    /**
     * 本节点已经脱离集群了 本次请求必然失败
     * @param faultyNode
     */
    public void onFaultyNode(DiscoveryNode faultyNode) {
        // 这会触发ackListener
        publicationTargets.forEach(t -> t.onFaultyNode(faultyNode));
        // 顺带检测是否已经通知到所有节点上了
        onPossibleCompletion();
    }

    public List<DiscoveryNode> completedNodes() {
        return publicationTargets.stream()
            .filter(PublicationTarget::isSuccessfullyCompleted)
            .map(PublicationTarget::getDiscoveryNode)
            .collect(Collectors.toList());
    }

    public boolean isCommitted() {
        return applyCommitRequest.isPresent();
    }

    /**
     * 检测本次任务是否已经完成
     */
    private void onPossibleCompletion() {
        if (isCompleted) {
            return;
        }

        // 只要还有某个节点未收到结果 就认为任务未结束
        if (cancelled == false) {
            for (final PublicationTarget target : publicationTargets) {
                if (target.isActive()) {
                    return;
                }
            }
        }

        // 当所有节点都收到结果时   如果没有生成commitReq对象 代表不满足commit条件
        if (applyCommitRequest.isPresent() == false) {
            logger.debug("onPossibleCompletion: [{}] commit failed", this);
            assert isCompleted == false;
            isCompleted = true;
            // 触发钩子
            onCompletion(false);
            return;
        }

        // 以满足commit的情况触发钩子
        assert isCompleted == false;
        isCompleted = true;
        onCompletion(true);
        assert applyCommitRequest.isPresent();
        logger.trace("onPossibleCompletion: [{}] was successful", this);
    }

    // For assertions only: verify that this invariant holds
    private boolean publicationCompletedIffAllTargetsInactiveOrCancelled() {
        if (cancelled == false) {
            for (final PublicationTarget target : publicationTargets) {
                if (target.isActive()) {
                    return isCompleted == false;
                }
            }
        }
        return isCompleted;
    }

    // For assertions
    ClusterState publishedState() {
        return publishRequest.getAcceptedState();
    }

    /**
     * 发送流程分为2步 第一步要求确认的节点数要超过 1/2 成功后 代表发布成功 之后将 commit请求发往集群
     */
    private void onPossibleCommitFailure() {
        if (applyCommitRequest.isPresent()) {
            // 必须要确保请求发往 所有target  并根据是否设置commitReq 触发不同的 onCompletion
            onPossibleCompletion();
            return;
        }

        final CoordinationState.VoteCollection possiblySuccessfulNodes = new CoordinationState.VoteCollection();
        for (PublicationTarget publicationTarget : publicationTargets) {
            // 这里检测失败数量是否已经超过了 1/2 这样可以提前失败
            if (publicationTarget.mayCommitInFuture()) {
                possiblySuccessfulNodes.addVote(publicationTarget.discoveryNode);
            } else {
                assert publicationTarget.isFailed() : publicationTarget;
            }
        }

        // 本次pub 必然失败
        if (isPublishQuorum(possiblySuccessfulNodes) == false) {
            logger.debug("onPossibleCommitFailure: non-failed nodes {} do not form a quorum, so {} cannot succeed",
                possiblySuccessfulNodes, this);
            Exception e = new FailedToCommitClusterStateException("non-failed nodes do not form a quorum");
            publicationTargets.stream().filter(PublicationTarget::isActive).forEach(pt -> pt.setFailed(e));
            onPossibleCompletion();
        }
    }

    protected abstract void onCompletion(boolean committed);

    protected abstract boolean isPublishQuorum(CoordinationState.VoteCollection votes);

    protected abstract Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse);

    protected abstract void onJoin(Join join);

    protected abstract void onMissingJoin(DiscoveryNode discoveryNode);

    protected abstract void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                               ActionListener<PublishWithJoinResponse> responseActionListener);

    protected abstract void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommit,
                                            ActionListener<TransportResponse.Empty> responseActionListener);

    @Override
    public String toString() {
        return "Publication{term=" + publishRequest.getAcceptedState().term() +
            ", version=" + publishRequest.getAcceptedState().version() + '}';
    }

    void logIncompleteNodes(Level level) {
        final String message = publicationTargets.stream().filter(PublicationTarget::isActive).map(publicationTarget ->
            publicationTarget.getDiscoveryNode() + " [" + publicationTarget.getState() + "]").collect(Collectors.joining(", "));
        if (message.isEmpty() == false) {
            final TimeValue elapsedTime = TimeValue.timeValueMillis(currentTimeSupplier.getAsLong() - startTime);
            logger.log(level, "after [{}] publication of cluster state version [{}] is still waiting for {}", elapsedTime,
                publishRequest.getAcceptedState().version(), message);
        }
    }

    /**
     * 描述 pubReq 发往某个节点进行处理的整个流程的某个状态
     */
    enum PublicationTargetState {
        NOT_STARTED,
        FAILED,
        SENT_PUBLISH_REQUEST,
        /**
         * 代表至少已经接受到某个发布响应结果
         * 此时整个publish流程中成功的节点还没有达到半数
         */
        WAITING_FOR_QUORUM,
        /**
         * 当超过半数的follower 发送了pubRes时
         * 由 leader节点向他们发送 commit请求  并等待结果
         */
        SENT_APPLY_COMMIT,
        APPLIED_COMMIT,
    }

    /**
     * 最新的clusterState 需要发布到所有类型的node 上
     */
    class PublicationTarget {
        private final DiscoveryNode discoveryNode;
        /**
         * 是否正在等待ack信息中
         */
        private boolean ackIsPending = true;

        /**
         * 描述此时往目标节点发布数据的状态
         */
        private PublicationTargetState state = PublicationTargetState.NOT_STARTED;

        PublicationTarget(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        PublicationTargetState getState() {
            return state;
        }

        @Override
        public String toString() {
            return "PublicationTarget{" +
                "discoveryNode=" + discoveryNode +
                ", state=" + state +
                ", ackIsPending=" + ackIsPending +
                '}';
        }

        /**
         * 整个publish 流程分为2步  这里是第一步 发送pub请求
         */
        void sendPublishRequest() {
            // 比如该节点已经下线就不需要处理了
            if (isFailed()) {
                return;
            }
            assert state == PublicationTargetState.NOT_STARTED : state + " -> " + PublicationTargetState.SENT_PUBLISH_REQUEST;

            // 更新成发布中的状态
            state = PublicationTargetState.SENT_PUBLISH_REQUEST;
            // 发送逻辑由子类实现
            Publication.this.sendPublishRequest(discoveryNode, publishRequest, new PublishResponseHandler());
            assert publicationCompletedIffAllTargetsInactiveOrCancelled();
        }

        /**
         * 处理发往某个节点后得到的 pubResp
         * @param publishResponse
         */
        void handlePublishResponse(PublishResponse publishResponse) {
            assert isWaitingForQuorum() : this;
            logger.trace("handlePublishResponse: handling [{}] from [{}])", publishResponse, discoveryNode);

            // 之后每收到一个 pubRes后 立即发送commit请求
            if (applyCommitRequest.isPresent()) {
                sendApplyCommit();
            } else {
                try {

                    // 交由实现类 处理res   当某次满足条件生成commit对象时 将它设置到applyCommitRequest上  这里的条件就是pub成功发布到超过半数的候选节点
                    // 因为在选举出leadr时就已经获得了超半数候选节点的同意  所以正常情况下在pub时 也能获取超半数候选节点的认同
                    // commit 针对的范围也是所有节点
                    Publication.this.handlePublishResponse(discoveryNode, publishResponse).ifPresent(applyCommit -> {
                        assert applyCommitRequest.isPresent() == false;
                        applyCommitRequest = Optional.of(applyCommit);

                        // 代表从发起publish 任务 到超过半数节点认同 总计花费多少时间    然而ack请求还是要求必须发布到所有节点才算成功
                        ackListener.onCommit(TimeValue.timeValueMillis(currentTimeSupplier.getAsLong() - startTime));
                        // 找到所有刚确认pub的节点  发送 applyCommit请求
                        publicationTargets.stream().filter(PublicationTarget::isWaitingForQuorum)
                            .forEach(PublicationTarget::sendApplyCommit);
                    });
                } catch (Exception e) {
                    // 处理失败时检测失败数量是否超过半数 是的话结束任务
                    setFailed(e);
                    onPossibleCommitFailure();
                }
            }
        }

        /**
         * 当pub 投票成功时 往这些节点发送commit请求
         */
        void sendApplyCommit() {
            assert state == PublicationTargetState.WAITING_FOR_QUORUM : state + " -> " + PublicationTargetState.SENT_APPLY_COMMIT;
            state = PublicationTargetState.SENT_APPLY_COMMIT;
            assert applyCommitRequest.isPresent();
            Publication.this.sendApplyCommit(discoveryNode, applyCommitRequest.get(), new ApplyCommitResponseHandler());
            assert publicationCompletedIffAllTargetsInactiveOrCancelled();
        }

        void setAppliedCommit() {
            assert state == PublicationTargetState.SENT_APPLY_COMMIT : state + " -> " + PublicationTargetState.APPLIED_COMMIT;
            state = PublicationTargetState.APPLIED_COMMIT;
            ackOnce(null);
        }

        /**
         * 标记本次针对该node的发布任务失败
         * @param e
         */
        void setFailed(Exception e) {
            assert state != PublicationTargetState.APPLIED_COMMIT : state + " -> " + PublicationTargetState.FAILED;
            state = PublicationTargetState.FAILED;
            ackOnce(e);
        }

        /**
         * 直接用异常信息通知监听器
         * @param faultyNode
         */
        void onFaultyNode(DiscoveryNode faultyNode) {
            if (isActive() && discoveryNode.equals(faultyNode)) {
                logger.debug("onFaultyNode: [{}] is faulty, failing target in publication {}", faultyNode, Publication.this);
                setFailed(new ElasticsearchException("faulty node"));
                // 判断能否提前结束任务
                onPossibleCommitFailure();
            }
        }

        DiscoveryNode getDiscoveryNode() {
            return discoveryNode;
        }

        /**
         * 代表以失败方式接收到了某个回复消息
         * @param e
         */
        private void ackOnce(Exception e) {
            // ackIsPending 该属性在初始状态就是true 即使以失败的形式结束任务 还是会触发 ackOnce
            if (ackIsPending) {
                ackIsPending = false;
                ackListener.onNodeAck(discoveryNode, e);
            }
        }

        /**
         * 代表已经产生了结果 比如失败 或者已提交
         * @return
         */
        boolean isActive() {
            return state != PublicationTargetState.FAILED
                && state != PublicationTargetState.APPLIED_COMMIT;
        }

        boolean isSuccessfullyCompleted() {
            return state == PublicationTargetState.APPLIED_COMMIT;
        }

        boolean isWaitingForQuorum() {
            return state == PublicationTargetState.WAITING_FOR_QUORUM;
        }

        boolean mayCommitInFuture() {
            return (state == PublicationTargetState.NOT_STARTED
                || state == PublicationTargetState.SENT_PUBLISH_REQUEST
                || state == PublicationTargetState.WAITING_FOR_QUORUM);
        }

        boolean isFailed() {
            return state == PublicationTargetState.FAILED;
        }

        /**
         * 该对象负责处理发布的结果
         */
        private class PublishResponseHandler implements ActionListener<PublishWithJoinResponse> {

            /**
             * 处理某个发布结果  该方法在外部已经套锁了
             * @param response
             */
            @Override
            public void onResponse(PublishWithJoinResponse response) {
                // 比如前一个leader 收到了后一个leader的发布请求 前一个leader关闭发布任务 之后又收到了发布结果 这时候拒绝处理
                if (isFailed()) {
                    logger.debug("PublishResponseHandler.handleResponse: already failed, ignoring response from [{}]", discoveryNode);
                    assert publicationCompletedIffAllTargetsInactiveOrCancelled();
                    return;
                }

                // 代表发送的目标节点  在本轮选举中为当前节点投了一票
                if (response.getJoin().isPresent()) {
                    final Join join = response.getJoin().get();
                    assert discoveryNode.equals(join.getSourceNode());
                    assert join.getTerm() == response.getPublishResponse().getTerm() : response;
                    logger.trace("handling join within publish response: {}", join);
                    onJoin(join);
                } else {
                    logger.trace("publish response from {} contained no join", discoveryNode);
                    onMissingJoin(discoveryNode);
                }

                assert state == PublicationTargetState.SENT_PUBLISH_REQUEST : state + " -> " + PublicationTargetState.WAITING_FOR_QUORUM;

                // 完成该target的发布阶段 进入等待判决阶段
                state = PublicationTargetState.WAITING_FOR_QUORUM;
                handlePublishResponse(response.getPublishResponse());

                assert publicationCompletedIffAllTargetsInactiveOrCancelled();
            }

            /**
             * 当尝试将最新的 clusterState 发布到某个节点上失败时
             * @param e
             */
            @Override
            public void onFailure(Exception e) {
                assert e instanceof TransportException;
                final TransportException exp = (TransportException) e;
                logger.debug(() -> new ParameterizedMessage("PublishResponseHandler: [{}] failed", discoveryNode), exp);
                assert ((TransportException) e).getRootCause() instanceof Exception;
                setFailed((Exception) exp.getRootCause());
                onPossibleCommitFailure();
                assert publicationCompletedIffAllTargetsInactiveOrCancelled();
            }

        }

        /**
         * 处理commit的结果
         */
        private class ApplyCommitResponseHandler implements ActionListener<TransportResponse.Empty> {

            @Override
            public void onResponse(TransportResponse.Empty ignored) {
                // pub任务已经被提前关闭
                if (isFailed()) {
                    logger.debug("ApplyCommitResponseHandler.handleResponse: already failed, ignoring response from [{}]",
                        discoveryNode);
                    return;
                }
                setAppliedCommit();
                onPossibleCompletion();
                assert publicationCompletedIffAllTargetsInactiveOrCancelled();
            }

            @Override
            public void onFailure(Exception e) {
                assert e instanceof TransportException;
                final TransportException exp = (TransportException) e;
                logger.debug(() -> new ParameterizedMessage("ApplyCommitResponseHandler: [{}] failed", discoveryNode), exp);
                assert ((TransportException) e).getRootCause() instanceof Exception;

                // 这时认为结果是失败的
                setFailed((Exception) exp.getRootCause());
                onPossibleCompletion();
                assert publicationCompletedIffAllTargetsInactiveOrCancelled();
            }
        }
    }
}
