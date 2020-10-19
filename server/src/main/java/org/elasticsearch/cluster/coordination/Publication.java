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
     * @param publishRequest
     * @param ackListener
     * @param currentTimeSupplier  提供当前时间的函数
     */
    public Publication(PublishRequest publishRequest, AckListener ackListener, LongSupplier currentTimeSupplier) {
        this.publishRequest = publishRequest;
        this.ackListener = ackListener;
        this.currentTimeSupplier = currentTimeSupplier;
        startTime = currentTimeSupplier.getAsLong();
        applyCommitRequest = Optional.empty();
        publicationTargets = new ArrayList<>(publishRequest.getAcceptedState().getNodes().getNodes().size());
        // 注意这里发布的节点包含非master节点
        publishRequest.getAcceptedState().getNodes().mastersFirstStream().forEach(n -> publicationTargets.add(new PublicationTarget(n)));
    }

    /**
     * 针对这组失败的节点 触发  PublicationTarget.onFaultyNode 钩子
     * 并且针对所有 target对象发送 publishRequest
     * @param faultyNodes
     */
    public void start(Set<DiscoveryNode> faultyNodes) {
        logger.trace("publishing {} to {}", publishRequest, publicationTargets);

        for (final DiscoveryNode faultyNode : faultyNodes) {
            // 每当往某个节点发送错误消息时 顺带检测能否提前结束pub任务 因为往其他节点发送本身是一个比较重的操作 当失败的节点数已经确定超过半数时 可以提前结束任务
            onFaultyNode(faultyNode);
        }
        // 在faultyNodes为空的时候 就有检测这个的必要了  如果一开始票数就是达不到要求的就可以直接结束pub任务 而不浪费时间
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
     * 代表本次发布任务完成 可能以成功形式完成 也可能是被关闭
     */
    private void onPossibleCompletion() {
        if (isCompleted) {
            return;
        }

        // 在被手动关闭的情况下 只要还有一个target 处于活跃状态 就无法执行 completion
        if (cancelled == false) {
            for (final PublicationTarget target : publicationTargets) {
                if (target.isActive()) {
                    return;
                }
            }
        }

        // 此时还没有满足commit条件
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
     * 这里是在检测失败的节点是否已经达到1/2了 如果达到了就没有必要继续发送pub请求了 可以提前结束
     */
    private void onPossibleCommitFailure() {
        // 如果已经生成了commit 对象 可以检测是否完成任务
        if (applyCommitRequest.isPresent()) {
            onPossibleCompletion();
            return;
        }

        final CoordinationState.VoteCollection possiblySuccessfulNodes = new CoordinationState.VoteCollection();
        for (PublicationTarget publicationTarget : publicationTargets) {
            // 这里将还未发送publish请求的节点 或者说还不能明确会失败的节点先加入到 投票箱中
            if (publicationTarget.mayCommitInFuture()) {
                possiblySuccessfulNodes.addVote(publicationTarget.discoveryNode);
            } else {
                assert publicationTarget.isFailed() : publicationTarget;
            }
        }

        // 如果此时票数不够 会将所有活跃节点以失败状态结束 并触发本对象的onComplete
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
     * 代表一个发布过程此时正处的阶段
     */
    enum PublicationTargetState {
        NOT_STARTED,
        FAILED,
        SENT_PUBLISH_REQUEST,
        /**
         * 代表至少已经接受到某个发布响应结果 但是还没有达到半数以上
         */
        WAITING_FOR_QUORUM,
        /**
         * 当超过半数的follower 发送了pubRes时
         * 由 master节点向他们发送 commit请求
         */
        SENT_APPLY_COMMIT,
        APPLIED_COMMIT,
    }

    /**
     * 代表一个发布请求的目标节点
     * 为什么不排除掉非master节点啊     也就是同步数据是全范围 而选举仅master节点范围???  那么发送join请求的节点也应该是全节点???
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
         * 当发布任务启动时 会往集群中所有节点发送pub请求
         */
        void sendPublishRequest() {
            // 如果在执行任务前已经判断pub无法成功了 也就是无法满足半数节点 那么会提前将任务设置成失败 也就不需要发送数据了
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
         * 处理某次收到的发布结果
         * @param publishResponse
         */
        void handlePublishResponse(PublishResponse publishResponse) {
            assert isWaitingForQuorum() : this;
            logger.trace("handlePublishResponse: handling [{}] from [{}])", publishResponse, discoveryNode);
            // 当收到超过半数节点的 publishRes 时  生成一个commitReq 对象 并且会设置到该字段中
            // 之后能收到 pub的请求 必然是在isWaitingForQuorum 阶段  所以不需要走
            // publicationTargets.stream().filter(PublicationTarget::isWaitingForQuorum)
            //                            .forEach(PublicationTarget::sendApplyCommit); 的判断逻辑
            if (applyCommitRequest.isPresent()) {
                sendApplyCommit();
            } else {
                try {
                    Publication.this.handlePublishResponse(discoveryNode, publishResponse).ifPresent(applyCommit -> {
                        // 只有第一次会设置 applyCommitRequest
                        assert applyCommitRequest.isPresent() == false;
                        applyCommitRequest = Optional.of(applyCommit);

                        // 代表从发起publish 任务 到超过半数节点认同 总计花费多少时间
                        ackListener.onCommit(TimeValue.timeValueMillis(currentTimeSupplier.getAsLong() - startTime));
                        // 找到所有刚确认pub的节点  发送 applyCommit请求
                        publicationTargets.stream().filter(PublicationTarget::isWaitingForQuorum)
                            .forEach(PublicationTarget::sendApplyCommit);
                    });
                } catch (Exception e) {
                    setFailed(e);
                    onPossibleCommitFailure();
                }
            }
        }

        /**
         * 当此时pub 满足半数条件时 开始向之前确认的节点 发送commit请求
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
         * 本次发布任务因为某个异常而停止
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
            // 只有确保请求发出去的情况 再失败时才会触发ack的钩子 如果请求还没发出去由其他 原因得知这个node不可靠 那么不需要触发ack
            if (ackIsPending) {
                ackIsPending = false;
                ackListener.onNodeAck(discoveryNode, e);
            }
        }

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
             *
             * @param response
             */
            @Override
            public void onResponse(PublishWithJoinResponse response) {
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
                state = PublicationTargetState.WAITING_FOR_QUORUM;
                handlePublishResponse(response.getPublishResponse());

                assert publicationCompletedIffAllTargetsInactiveOrCancelled();
            }

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
         * 该对象负责处理 commit的响应结果
         */
        private class ApplyCommitResponseHandler implements ActionListener<TransportResponse.Empty> {

            @Override
            public void onResponse(TransportResponse.Empty ignored) {
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
                setFailed((Exception) exp.getRootCause());
                onPossibleCompletion();
                assert publicationCompletedIffAllTargetsInactiveOrCancelled();
            }
        }
    }
}
