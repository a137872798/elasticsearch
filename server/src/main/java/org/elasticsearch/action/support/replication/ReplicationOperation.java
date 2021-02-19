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
package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * 通过该对象完成分片级别的某些任务 包含primary/replica
 * 该对象是在主分片上被创建的
 * @param <Request>
 * @param <ReplicaRequest>
 * @param <PrimaryResultT>
 */
public class ReplicationOperation<
            Request extends ReplicationRequest<Request>,
            ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
            PrimaryResultT extends ReplicationOperation.PrimaryResult<ReplicaRequest>
        > {
    private final Logger logger;
    private final ThreadPool threadPool;
    private final Request request;
    private final String opType;

    /**
     * 记录此时已经处理了多少分片
     */
    private final AtomicInteger totalShards = new AtomicInteger();
    /**
     * The number of pending sub-operations in this operation. This is incremented when the following operations start and decremented when
     * they complete:
     * <ul>
     * <li>The operation on the primary</li>
     * <li>The operation on each replica</li>
     * <li>Coordination of the operation as a whole. This prevents the operation from terminating early if we haven't started any replica
     * operations and the primary finishes.</li>
     * </ul>
     */
    private final AtomicInteger pendingActions = new AtomicInteger();
    private final AtomicInteger successfulShards = new AtomicInteger();
    private final Primary<Request, ReplicaRequest, PrimaryResultT> primary;
    private final Replicas<ReplicaRequest> replicasProxy;
    private final AtomicBoolean finished = new AtomicBoolean();
    private final TimeValue initialRetryBackoffBound;
    private final TimeValue retryTimeout;
    private final long primaryTerm;

    // exposed for tests
    private final ActionListener<PrimaryResultT> resultListener;

    /**
     * 本次操作对应的结果
     */
    private volatile PrimaryResultT primaryResult = null;

    /**
     * 存储在分片上执行失败的数量
     */
    private final List<ReplicationResponse.ShardInfo.Failure> shardReplicaFailures = Collections.synchronizedList(new ArrayList<>());

    /**
     *
     * @param request
     * @param primary  这是一个分片的包装对象
     * @param listener  当操作完成后触发该监听器
     * @param replicas
     * @param logger
     * @param threadPool
     * @param opType
     * @param primaryTerm
     * @param initialRetryBackoffBound
     * @param retryTimeout
     */
    public ReplicationOperation(Request request, Primary<Request, ReplicaRequest, PrimaryResultT> primary,
                                ActionListener<PrimaryResultT> listener,
                                Replicas<ReplicaRequest> replicas,
                                Logger logger, ThreadPool threadPool, String opType, long primaryTerm, TimeValue initialRetryBackoffBound,
                                TimeValue retryTimeout) {
        this.replicasProxy = replicas;
        this.primary = primary;
        this.resultListener = listener;
        this.logger = logger;
        this.threadPool = threadPool;
        this.request = request;
        this.opType = opType;
        this.primaryTerm = primaryTerm;
        this.initialRetryBackoffBound = initialRetryBackoffBound;
        this.retryTimeout = retryTimeout;
    }

    /**
     * 开始处理请求
     * @throws Exception
     */
    public void execute() throws Exception {
        final String activeShardCountFailure = checkActiveShardCount();
        final ShardRouting primaryRouting = primary.routingEntry();
        final ShardId primaryId = primaryRouting.shardId();
        // 代表当前分片数不足 无法正常执行任务
        if (activeShardCountFailure != null) {
            finishAsFailed(new UnavailableShardsException(primaryId,
                "{} Timeout: [{}], request: [{}]", activeShardCountFailure, request.timeout(), request));
            return;
        }

        // 当前已经处理到第几个分片了
        totalShards.incrementAndGet();
        // 当整个流程结束后才会-1
        pendingActions.incrementAndGet(); // increase by 1 until we finish all primary coordination
        // 当接收到结果时  使用handlePrimaryResult 进行处理
        primary.perform(request, ActionListener.wrap(this::handlePrimaryResult, resultListener::onFailure));
    }

    /**
     * 当处理主分片并产生了结果时  会先调用该方法
     * @param primaryResult
     */
    private void handlePrimaryResult(final PrimaryResultT primaryResult) {
        this.primaryResult = primaryResult;

        // result中 同时包含了本次接收的请求req 以及处理的结果res
        final ReplicaRequest replicaRequest = primaryResult.replicaRequest();

        if (replicaRequest != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("[{}] op [{}] completed on primary for request [{}]", primary.routingEntry().shardId(), opType, request);
            }
            // we have to get the replication group after successfully indexing into the primary in order to honour recovery semantics.
            // we have to make sure that every operation indexed into the primary after recovery start will also be replicated
            // to the recovery target. If we used an old replication group, we may miss a recovery that has started since then.
            // we also have to make sure to get the global checkpoint before the replication group, to ensure that the global checkpoint
            // is valid for this replication group. If we would sample in the reverse, the global checkpoint might be based on a subset
            // of the sampled replication group, and advanced further than what the given replication group would allow it to.
            // This would entail that some shards could learn about a global checkpoint that would be higher than its local checkpoint.
            // 获取主分片此时的全局检查点
            final long globalCheckpoint = primary.computedGlobalCheckpoint();
            // we have to capture the max_seq_no_of_updates after this request was completed on the primary to make sure the value of
            // max_seq_no_of_updates on replica when this request is executed is at least the value on the primary when it was executed
            // on.

            // 每当发现执行的index是update操作 或者执行delete操作时 会记录此时的时间戳
            final long maxSeqNoOfUpdatesOrDeletes = primary.maxSeqNoOfUpdatesOrDeletes();
            assert maxSeqNoOfUpdatesOrDeletes != SequenceNumbers.UNASSIGNED_SEQ_NO : "seqno_of_updates still uninitialized";
            // 副本组描述了当前分片的状态
            final ReplicationGroup replicationGroup = primary.getReplicationGroup();
            // 该对象本身会监听 replicationGroup的变化 并抽取信息  只维护tracked相关的数据  针对peerRecovery的分片 在完成了safeCommit的索引文件传输 以及创建engine后 就会标记成tracked
            final PendingReplicationActions pendingReplicationActions = primary.getPendingReplicationActions();
            // 根据条件判断 是否要将此时不可用的分片关闭
            markUnavailableShardsAsStale(replicaRequest, replicationGroup);

            // 开始在副本上进行处理
            performOnReplicas(replicaRequest, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes, replicationGroup, pendingReplicationActions);
        }

        // 当发送副本请求后 触发该函数   此时action可能还没有在副本上执行完   在子类中这里就是对 lucene/事务日志进行持久化
        primaryResult.runPostReplicationActions(new ActionListener<>() {

            @Override
            public void onResponse(Void aVoid) {
                successfulShards.incrementAndGet();
                try {
                    // 在执行完刷盘后 此时localCheckpoint 对应事务日志此时的seqNo
                    // 全局检查点则是每个副本的localCheckpoint 都会上报给主分片  当中的最小值会作为globalCheckpoint
                    // 而当事务日志要持久化时 就会将此时最新的 globalCheckpoint 写入到ckp文件中
                    updateCheckPoints(primary.routingEntry(), primary::localCheckpoint, primary::globalCheckpoint);
                } finally {
                    // 对 pending计数-1 当归0的时候 触发finish方法
                    decPendingAndFinishIfNeeded();
                }
            }

            /**
             * 如果在写入主分片的过程中就已经失败了 触发该方法   以失败的情况触发监听器
             * @param e
             */
            @Override
            public void onFailure(Exception e) {
                logger.trace("[{}] op [{}] post replication actions failed for [{}]", primary.routingEntry().shardId(), opType, request);
                // TODO: fail shard? This will otherwise have the local / global checkpoint info lagging, or possibly have replicas
                // go out of sync with the primary
                finishAsFailed(e);
            }
        });
    }

    /**
     * 将某些分片不可用的信息上报给 leader节点
     * @param replicaRequest
     * @param replicationGroup
     */
    private void markUnavailableShardsAsStale(ReplicaRequest replicaRequest, ReplicationGroup replicationGroup) {
        // if inSyncAllocationIds contains allocation ids of shards that don't exist in RoutingTable, mark copies as stale
        // 还未处于 in-sync 容器中的分片
        for (String allocationId : replicationGroup.getUnavailableInSyncShards()) {
            // 增加一个此时正在运行的任务
            pendingActions.incrementAndGet();

            // 根据情况选择是否将分片无效的信息上报给leader节点  这时leader节点一般会重新选择一个节点去创建分片
            replicasProxy.markShardCopyAsStaleIfNeeded(replicaRequest.shardId(), allocationId, primaryTerm,
                // decPendingAndFinishIfNeeded 对冲刚才增加的pendingActions
                ActionListener.wrap(r -> decPendingAndFinishIfNeeded(), ReplicationOperation.this::onNoLongerPrimary));
        }
    }

    /**
     * 在副本上处理请求
     * @param replicaRequest  本次传入的请求对象
     * @param globalCheckpoint   主分片此时的全局检查点
     * @param maxSeqNoOfUpdatesOrDeletes    更新或者删除操作对应的最大的seqNo
     * @param replicationGroup
     * @param pendingReplicationActions
     */
    private void performOnReplicas(final ReplicaRequest replicaRequest, final long globalCheckpoint,
                                   final long maxSeqNoOfUpdatesOrDeletes, final ReplicationGroup replicationGroup,
                                   final PendingReplicationActions pendingReplicationActions) {
        // for total stats, add number of unassigned shards and
        // number of initializing shards that are not ready yet to receive operations (recovery has not opened engine yet on the target)
        // 某些分片此时处于初始化未结束的状态 无法将请求发往这些分片 所以需要跳过
        totalShards.addAndGet(replicationGroup.getSkippedShards().size());

        final ShardRouting primaryRouting = primary.routingEntry();

        // 这些副本是需要同步数据的目标分片
        for (final ShardRouting shard : replicationGroup.getReplicationTargets()) {
            // 跳过主分片自身
            if (shard.isSameAllocation(primaryRouting) == false) {
                performOnReplica(shard, replicaRequest, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes, pendingReplicationActions);
            }
        }
    }

    /**
     * 处理某个副本
     * @param shard  副本对应的分片
     * @param replicaRequest   本次发起的原请求
     * @param globalCheckpoint     此时主分片的全局检查点
     * @param maxSeqNoOfUpdatesOrDeletes    最近的一次更新/删除操作对应的seqNo
     * @param pendingReplicationActions
     */
    private void performOnReplica(final ShardRouting shard, final ReplicaRequest replicaRequest,
                                  final long globalCheckpoint, final long maxSeqNoOfUpdatesOrDeletes,
                                  final PendingReplicationActions pendingReplicationActions) {
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] sending op [{}] to replica {} for request [{}]", shard.shardId(), opType, shard, replicaRequest);
        }
        totalShards.incrementAndGet();
        // 增加这个就类似于 CountDown 的意图  增加计数 并在产生结果的回调中减少计数 这样就不会提前触发 最终的处理逻辑了  而是必须要等待所有副本返回结果后才处理
        pendingActions.incrementAndGet();


        // 每当成功作用在某个副本后 返回的结果会触发该函数
        // ReplicaResponse 中包含了副本此时最新的事务日志文件op的seqNo  (persistedCheckpoint)
        // 以及事务日志中记录的最新的全局检查点 都是此时已经明确持久化的值
        final ActionListener<ReplicaResponse> replicationListener = new ActionListener<>() {
            @Override
            public void onResponse(ReplicaResponse response) {
                // 每当成功处理一个分片后 增加成功数
                successfulShards.incrementAndGet();
                try {
                    // 根据副本响应结果中的 localCheckpoint/globalCheckpoint  更新主分片维护的副本相关信息
                    updateCheckPoints(shard, response::localCheckpoint, response::globalCheckpoint);
                } finally {
                    // 对冲增加的 pendingActions
                    decPendingAndFinishIfNeeded();
                }
            }

            /**
             * 当在某个分片上执行写入操作 并失败时
             * @param replicaException
             */
            @Override
            public void onFailure(Exception replicaException) {
                logger.trace(() -> new ParameterizedMessage(
                    "[{}] failure while performing [{}] on replica {}, request [{}]",
                    shard.shardId(), opType, shard, replicaRequest), replicaException);
                // Only report "critical" exceptions - TODO: Reach out to the master node to get the latest shard state then report.
                if (TransportActions.isShardNotAvailableException(replicaException) == false) {
                    RestStatus restStatus = ExceptionsHelper.status(replicaException);
                    shardReplicaFailures.add(new ReplicationResponse.ShardInfo.Failure(
                        shard.shardId(), shard.currentNodeId(), replicaException, restStatus, false));
                }
                String message = String.format(Locale.ROOT, "failed to perform %s on replica %s", opType, shard);

                // 当处理失败时 是否需要通知到leader节点
                replicasProxy.failShardIfNeeded(shard, primaryTerm, message, replicaException,
                    ActionListener.wrap(r -> decPendingAndFinishIfNeeded(), ReplicationOperation.this::onNoLongerPrimary));
            }

            @Override
            public String toString() {
                return "[" + replicaRequest + "][" + shard + "]";
            }
        };

        // 获取当前分片的分配id
        final String allocationId = shard.allocationId().getId();

        // 对应一个可以进行重试的模板
        final RetryableAction<ReplicaResponse> replicationAction = new RetryableAction<>(logger, threadPool, initialRetryBackoffBound,
                retryTimeout, replicationListener) {

            /**
             * 在副本上处理请求
             * @param listener
             */
            @Override
            public void tryAction(ActionListener<ReplicaResponse> listener) {
                replicasProxy.performOn(shard, replicaRequest, primaryTerm, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes, listener);
            }

            @Override
            public void onFinished() {
                super.onFinished();
                pendingReplicationActions.removeReplicationAction(allocationId, this);
            }

            @Override
            public boolean shouldRetry(Exception e) {
                final Throwable cause = ExceptionsHelper.unwrapCause(e);
                return cause instanceof CircuitBreakingException ||
                    cause instanceof EsRejectedExecutionException ||
                    cause instanceof ConnectTransportException;
            }
        };

        // 通过 pendingReplicationActions 维护此时正在执行的索引任务
        pendingReplicationActions.addPendingAction(allocationId, replicationAction);
        // 执行副本任务  这里是通过线程池执行的
        replicationAction.run();
    }

    /**
     * 更新某个分片的 localCheckpoint/globalCheckpoint   注意这里的2个检查点都是已经确定持久化到副本的
     * 比如localCheckpoint就是此时写入到事务日志中最新的checkpoint  而global对应事务日志中最新的 checkpoint对象对应的globalCheckpoint
     * @param shard
     * @param localCheckpointSupplier   将此时持久化的检查点同步到 tracer上
     * @param globalCheckpointSupplier
     */
    private void updateCheckPoints(ShardRouting shard, LongSupplier localCheckpointSupplier, LongSupplier globalCheckpointSupplier) {
        try {
            primary.updateLocalCheckpointForShard(shard.allocationId().getId(), localCheckpointSupplier.getAsLong());
            primary.updateGlobalCheckpointForShard(shard.allocationId().getId(), globalCheckpointSupplier.getAsLong());
        } catch (final AlreadyClosedException e) {
            // the index was deleted or this shard was never activated after a relocation; fall through and finish normally
        } catch (final Exception e) {
            // fail the primary but fall through and let the rest of operation processing complete
            final String message = String.format(Locale.ROOT, "primary failed updating local checkpoint for replica %s", shard);
            primary.failShard(message, e);
        }
    }

    private void onNoLongerPrimary(Exception failure) {
        final Throwable cause = ExceptionsHelper.unwrapCause(failure);
        final boolean nodeIsClosing = cause instanceof NodeClosedException;
        final String message;
        if (nodeIsClosing) {
            message = String.format(Locale.ROOT,
                "node with primary [%s] is shutting down while failing replica shard", primary.routingEntry());
            // We prefer not to fail the primary to avoid unnecessary warning log
            // when the node with the primary shard is gracefully shutting down.
        } else {
            if (Assertions.ENABLED) {
                if (failure instanceof ShardStateAction.NoLongerPrimaryShardException == false) {
                    throw new AssertionError("unexpected failure", failure);
                }
            }
            // we are no longer the primary, fail ourselves and start over
            message = String.format(Locale.ROOT, "primary shard [%s] was demoted while failing replica shard", primary.routingEntry());
            primary.failShard(message, failure);
        }
        finishAsFailed(new RetryOnPrimaryException(primary.routingEntry().shardId(), message, failure));
    }

    /**
     * Checks whether we can perform a write based on the required active shard count setting.
     * Returns **null* if OK to proceed, or a string describing the reason to stop
     * 某些请求可能有此时活跃的分片数量限制  当不满足条件时不应该继续执行任务   这样比如设置是ALL 然后有正在恢复数据的分片这时就应该拒绝写入
     */
    protected String checkActiveShardCount() {
        // 获取该主分片的 shardId
        final ShardId shardId = primary.routingEntry().shardId();
        // 代表本次请求需要等待直到多少分片才开始处理
        final ActiveShardCount waitForActiveShards = request.waitForActiveShards();
        // 如果没有限制 返回null
        if (waitForActiveShards == ActiveShardCount.NONE) {
            return null;  // not waiting for any shards
        }
        // 获取该分片此时所有副本的路由信息
        final IndexShardRoutingTable shardRoutingTable = primary.getReplicationGroup().getRoutingTable();
        // 当满足要求时返回null
        if (waitForActiveShards.enoughShardsActive(shardRoutingTable)) {
            return null;
        } else {
            // 当此时活跃分片数不足的时候 返回一个提示信息
            final String resolvedShards = waitForActiveShards == ActiveShardCount.ALL ? Integer.toString(shardRoutingTable.shards().size())
                                              : waitForActiveShards.toString();
            logger.trace("[{}] not enough active copies to meet shard count of [{}] (have {}, needed {}), scheduling a retry. op [{}], " +
                         "request [{}]", shardId, waitForActiveShards, shardRoutingTable.activeShards().size(),
                         resolvedShards, opType, request);
            return "Not enough active copies to meet shard count of [" + waitForActiveShards + "] (have " +
                       shardRoutingTable.activeShards().size() + ", needed " + resolvedShards + ").";
        }
    }

    /**
     * 减少 pending的计数值 如果刚好归0了 触发finish
     */
    private void decPendingAndFinishIfNeeded() {
        assert pendingActions.get() > 0 : "pending action count goes below 0 for request [" + request + "]";
        if (pendingActions.decrementAndGet() == 0) {
            finish();
        }
    }

    /**
     * 所有主分片/副本已经写入完成
     */
    private void finish() {
        if (finished.compareAndSet(false, true)) {
            final ReplicationResponse.ShardInfo.Failure[] failuresArray;
            if (shardReplicaFailures.isEmpty()) {
                failuresArray = ReplicationResponse.EMPTY;
            } else {
                failuresArray = new ReplicationResponse.ShardInfo.Failure[shardReplicaFailures.size()];
                shardReplicaFailures.toArray(failuresArray);
            }
            primaryResult.setShardInfo(new ReplicationResponse.ShardInfo(
                    // 总计处理了多少分片 成功了多少分片 本次结果是否成功
                    totalShards.get(),
                    successfulShards.get(),
                    failuresArray
                )
            );
            resultListener.onResponse(primaryResult);
        }
    }

    private void finishAsFailed(Exception exception) {
        if (finished.compareAndSet(false, true)) {
            resultListener.onFailure(exception);
        }
    }

    /**
     * An encapsulation of an operation that is to be performed on the primary shard
     */
    public interface Primary<
                RequestT extends ReplicationRequest<RequestT>,
                ReplicaRequestT extends ReplicationRequest<ReplicaRequestT>,
                PrimaryResultT extends PrimaryResult<ReplicaRequestT>
            > {

        /**
         * routing entry for this primary
         */
        ShardRouting routingEntry();

        /**
         * Fail the primary shard.
         *
         * @param message   the failure message
         * @param exception the exception that triggered the failure
         */
        void failShard(String message, Exception exception);

        /**
         * Performs the given request on this primary. Yes, this returns as soon as it can with the request for the replicas and calls a
         * listener when the primary request is completed. Yes, the primary request might complete before the method returns. Yes, it might
         * also complete after. Deal with it.
         *
         * @param request the request to perform
         * @param listener result listener
         */
        void perform(RequestT request, ActionListener<PrimaryResultT> listener);

        /**
         * Notifies the primary of a local checkpoint for the given allocation.
         *
         * Note: The primary will use this information to advance the global checkpoint if possible.
         *
         * @param allocationId allocation ID of the shard corresponding to the supplied local checkpoint
         * @param checkpoint the *local* checkpoint for the shard
         */
        void updateLocalCheckpointForShard(String allocationId, long checkpoint);

        /**
         * Update the local knowledge of the global checkpoint for the specified allocation ID.
         *
         * @param allocationId     the allocation ID to update the global checkpoint for
         * @param globalCheckpoint the global checkpoint
         *
         */
        void updateGlobalCheckpointForShard(String allocationId, long globalCheckpoint);

        /**
         * Returns the persisted local checkpoint on the primary shard.
         *
         * @return the local checkpoint
         */
        long localCheckpoint();

        /**
         * Returns the global checkpoint computed on the primary shard.
         *
         * @return the computed global checkpoint
         */
        long computedGlobalCheckpoint();

        /**
         * Returns the persisted global checkpoint on the primary shard.
         *
         * @return the persisted global checkpoint
         */
        long globalCheckpoint();

        /**
         * Returns the maximum seq_no of updates (index operations overwrite Lucene) or deletes on the primary.
         * This value must be captured after the execution of a replication request on the primary is completed.
         */
        long maxSeqNoOfUpdatesOrDeletes();

        /**
         * Returns the current replication group on the primary shard
         *
         * @return the replication group
         */
        ReplicationGroup getReplicationGroup();

        /**
         * Returns the pending replication actions on the primary shard
         *
         * @return the pending replication actions
         */
        PendingReplicationActions getPendingReplicationActions();
    }

    /**
     * An encapsulation of an operation that will be executed on the replica shards, if present.
     * 代表一个将会在副本执行的操作
     */
    public interface Replicas<RequestT extends ReplicationRequest<RequestT>> {

        /**
         * Performs the specified request on the specified replica.
         *
         * @param replica                    the shard this request should be executed on
         * @param replicaRequest             the operation to perform
         * @param primaryTerm                the primary term
         * @param globalCheckpoint           the global checkpoint on the primary
         * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates (index operations overwriting Lucene) or deletes on primary
         *                                   after this replication was executed on it.
         * @param listener                   callback for handling the response or failure
         */
        void performOn(ShardRouting replica, RequestT replicaRequest,
                       long primaryTerm, long globalCheckpoint, long maxSeqNoOfUpdatesOrDeletes, ActionListener<ReplicaResponse> listener);

        /**
         * Fail the specified shard if needed, removing it from the current set
         * of active shards. Whether a failure is needed is left up to the
         * implementation.
         *
         * @param replica      shard to fail
         * @param primaryTerm  the primary term
         * @param message      a (short) description of the reason
         * @param exception    the original exception which caused the ReplicationOperation to request the shard to be failed
         * @param listener     a listener that will be notified when the failing shard has been removed from the in-sync set
         */
        void failShardIfNeeded(ShardRouting replica, long primaryTerm, String message, Exception exception, ActionListener<Void> listener);

        /**
         * Marks shard copy as stale if needed, removing its allocation id from
         * the set of in-sync allocation ids. Whether marking as stale is needed
         * is left up to the implementation.
         *
         * @param shardId      shard id
         * @param allocationId allocation id to remove from the set of in-sync allocation ids
         * @param primaryTerm  the primary term
         * @param listener     a listener that will be notified when the failing shard has been removed from the in-sync set
         */
        void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, long primaryTerm, ActionListener<Void> listener);
    }

    /**
     * An interface to encapsulate the metadata needed from replica shards when they respond to operations performed on them.
     */
    public interface ReplicaResponse {

        /**
         * The persisted local checkpoint for the shard.
         *
         * @return the persisted local checkpoint
         **/
        long localCheckpoint();

        /**
         * The persisted global checkpoint for the shard.
         *
         * @return the persisted global checkpoint
         **/
        long globalCheckpoint();

    }

    public static class RetryOnPrimaryException extends ElasticsearchException {
        RetryOnPrimaryException(ShardId shardId, String msg) {
            this(shardId, msg, null);
        }

        RetryOnPrimaryException(ShardId shardId, String msg, Throwable cause) {
            super(msg, cause);
            setShard(shardId);
        }

        public RetryOnPrimaryException(StreamInput in) throws IOException {
            super(in);
        }
    }

    public interface PrimaryResult<RequestT extends ReplicationRequest<RequestT>> {

        /**
         * @return null if no operation needs to be sent to a replica
         * (for example when the operation failed on the primary due to a parsing exception)
         */
        @Nullable RequestT replicaRequest();

        void setShardInfo(ReplicationResponse.ShardInfo shardInfo);

        /**
         * Run actions to be triggered post replication
         * @param listener calllback that is invoked after post replication actions have completed
         * */
        void runPostReplicationActions(ActionListener<Void> listener);
    }
}
