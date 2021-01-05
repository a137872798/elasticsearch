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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for transport actions that modify data in some shard like index, delete, and shardBulk.
 * Allows performing async actions (e.g. refresh) after performing write operations on primary and replica shards
 * 这是一个写入操作
 * 继承自 TransportReplicationAction  代表会先作用在 primary上 之后作用到所有replica
 */
public abstract class TransportWriteAction<
    Request extends ReplicatedWriteRequest<Request>,
    ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>,
    Response extends ReplicationResponse & WriteResponse
    > extends TransportReplicationAction<Request, ReplicaRequest, Response> {


    /**
     * WriteAction 在写入到主副本后 会要求同步全局检查点
     * @param settings
     * @param actionName
     * @param transportService
     * @param clusterService
     * @param indicesService
     * @param threadPool
     * @param shardStateAction
     * @param actionFilters
     * @param request
     * @param replicaRequest
     * @param executor
     * @param forceExecutionOnPrimary
     */
    protected TransportWriteAction(Settings settings, String actionName, TransportService transportService,
                                   ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                   ShardStateAction shardStateAction, ActionFilters actionFilters, Writeable.Reader<Request> request,
                                   Writeable.Reader<ReplicaRequest> replicaRequest, String executor, boolean forceExecutionOnPrimary) {
        super(settings, actionName, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
            // 注意操作完成后需要同步全局检查点
            request, replicaRequest, executor, true, forceExecutionOnPrimary);
    }

    /**
     * Syncs operation result to the translog or throws a shard not available failure
     *
     * @param operationResult 本次操作的结果
     * @param currentLocation 本次操作生成的结果在事务日志中的位置
     */
    protected static Location syncOperationResultOrThrow(final Engine.Result operationResult,
                                                         final Location currentLocation) throws Exception {
        final Location location;

        // 如果本次处理已经产生异常了  那么不应该调用该方法
        if (operationResult.getFailure() != null) {
            // check if any transient write operation failures should be bubbled up
            Exception failure = operationResult.getFailure();
            assert failure instanceof MapperParsingException : "expected mapper parsing failures. got " + failure;
            throw failure;
        } else {
            // 返回本次结果携带的 location信息
            location = locationToSync(currentLocation, operationResult.getTranslogLocation());
        }
        return location;
    }

    public static Location locationToSync(Location current, Location next) {
        /* here we are moving forward in the translog with each operation. Under the hood this might
         * cross translog files which is ok since from the user perspective the translog is like a
         * tape where only the highest location needs to be fsynced in order to sync all previous
         * locations even though they are not in the same file. When the translog rolls over files
         * the previous file is fsynced on after closing if needed.*/
        assert next != null : "next operation can't be null";
        assert current == null || current.compareTo(next) < 0 :
            "translog locations are not increasing";
        return next;
    }

    @Override
    protected ReplicationOperation.Replicas<ReplicaRequest> newReplicasProxy() {
        return new WriteActionReplicasProxy();
    }

    /**
     * Called on the primary with a reference to the primary {@linkplain IndexShard} to modify.
     *
     * @param listener listener for the result of the operation on primary, including current translog location and operation response
     *                 and failure async refresh is performed on the <code>primary</code> shard according to the <code>Request</code> refresh policy
     */
    @Override
    protected abstract void shardOperationOnPrimary(
        Request request, IndexShard primary, ActionListener<PrimaryResult<ReplicaRequest, Response>> listener);

    /**
     * Called once per replica with a reference to the replica {@linkplain IndexShard} to modify.
     *
     * @return the result of the operation on replica, including current translog location and operation response and failure
     * async refresh is performed on the <code>replica</code> shard according to the <code>ReplicaRequest</code> refresh policy
     */
    @Override
    protected abstract WriteReplicaResult<ReplicaRequest> shardOperationOnReplica(
        ReplicaRequest request, IndexShard replica) throws Exception;

    /**
     * Result of taking the action on the primary.
     * <p>
     * NOTE: public for testing
     * 代表某个写入到主分片的操作结果
     */
    public static class WritePrimaryResult<ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>,
        Response extends ReplicationResponse & WriteResponse> extends PrimaryResult<ReplicaRequest, Response> {
        public final Location location;
        public final IndexShard primary;
        private final Logger logger;

        /**
         * @param request          本次写入任务的请求体
         * @param finalResponse    处理结果
         * @param location         此时事务日志所在的位置 应当要确保这个位置及之前的事务日志持久化
         * @param operationFailure
         * @param primary
         * @param logger
         */
        public WritePrimaryResult(ReplicaRequest request, @Nullable Response finalResponse,
                                  @Nullable Location location, @Nullable Exception operationFailure,
                                  IndexShard primary, Logger logger) {
            super(request, finalResponse, operationFailure);
            this.location = location;
            this.primary = primary;
            this.logger = logger;
            assert location == null || operationFailure == null
                : "expected either failure to be null or translog location to be null, " +
                "but found: [" + location + "] translog location and [" + operationFailure + "] failure";
        }

        /**
         * 当主分片完成写入操作 并往相关副本发起写入操作后  触发该方法
         *
         * @param listener calllback that is invoked after post replication actions have completed
         */
        @Override
        public void runPostReplicationActions(ActionListener<Void> listener) {
            // 代表在第一阶段就出现异常了
            if (finalFailure != null) {
                listener.onFailure(finalFailure);
            } else {
                /*
                 * We call this after replication because this might wait for a refresh and that can take a while.
                 * This way we wait for the refresh in parallel on the primary and on the replica.
                 */
                new AsyncAfterWriteAction(primary, replicaRequest, location, new RespondingWriteResult() {

                    /**
                     * 当执行刷盘任务时 是否执行了 refresh
                     * @param forcedRefresh <code>true</code> iff this write has caused a refresh
                     */
                    @Override
                    public void onSuccess(boolean forcedRefresh) {
                        finalResponseIfSuccessful.setForcedRefresh(forcedRefresh);
                        listener.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception ex) {
                        listener.onFailure(ex);
                    }
                }, logger).run();
            }
        }
    }

    /**
     * Result of taking the action on the replica.
     */
    public static class WriteReplicaResult<ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>> extends ReplicaResult {
        public final Location location;
        private final ReplicaRequest request;
        private final IndexShard replica;
        private final Logger logger;

        public WriteReplicaResult(ReplicaRequest request, @Nullable Location location,
                                  @Nullable Exception operationFailure, IndexShard replica, Logger logger) {
            super(operationFailure);
            this.location = location;
            this.request = request;
            this.replica = replica;
            this.logger = logger;
        }

        /**
         * 在副本上执行完操作后触发该方法
         * @param listener
         */
        @Override
        public void runPostReplicaActions(ActionListener<Void> listener) {
            if (finalFailure != null) {
                listener.onFailure(finalFailure);
            } else {
                new AsyncAfterWriteAction(replica, request, location,
                    // 当写入完成后 通过该对象处理
                    new RespondingWriteResult() {
                        @Override
                        public void onSuccess(boolean forcedRefresh) {
                            listener.onResponse(null);
                        }

                        @Override
                        public void onFailure(Exception ex) {
                            listener.onFailure(ex);
                        }
                    }, logger).run();
            }
        }
    }

    @Override
    protected ClusterBlockLevel globalBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    /**
     * callback used by {@link AsyncAfterWriteAction} to notify that all post
     * process actions have been executed
     */
    interface RespondingWriteResult {
        /**
         * Called on successful processing of all post write actions
         *
         * @param forcedRefresh <code>true</code> iff this write has caused a refresh
         */
        void onSuccess(boolean forcedRefresh);

        /**
         * Called on failure if a post action failed.
         */
        void onFailure(Exception ex);
    }

    /**
     * This class encapsulates post write actions like async waits for
     * translog syncs or waiting for a refresh to happen making the write operation
     * visible.
     */
    static final class AsyncAfterWriteAction {
        private final Location location;
        private final boolean waitUntilRefresh;
        private final boolean sync;
        private final AtomicInteger pendingOps = new AtomicInteger(1);
        private final AtomicBoolean refreshed = new AtomicBoolean(false);
        private final AtomicReference<Exception> syncFailure = new AtomicReference<>(null);
        private final RespondingWriteResult respond;
        private final IndexShard indexShard;
        private final WriteRequest<?> request;
        private final Logger logger;


        /**
         * @param indexShard
         * @param request    本次在所有分片中执行的req
         * @param location   最后一个操作在事务日志中对应的位置
         * @param respond    负责处理写入结果的回调对象
         * @param logger
         */
        AsyncAfterWriteAction(final IndexShard indexShard,
                              final WriteRequest<?> request,
                              @Nullable final Translog.Location location,
                              final RespondingWriteResult respond,
                              final Logger logger) {
            this.indexShard = indexShard;
            this.request = request;
            boolean waitUntilRefresh = false;

            // 写入请求本身可以指定刷盘策略
            switch (request.getRefreshPolicy()) {
                // 这里是针对lucene数据进行刷盘  但是这个操作并不会更新userData   这时用户可以直接观测到数据变化
                case IMMEDIATE:
                    indexShard.refresh("refresh_flag_index");
                    refreshed.set(true);
                    break;
                // 阻塞直到检测到 refresh时 才会从索引操作返回
                case WAIT_UNTIL:
                    if (location != null) {
                        waitUntilRefresh = true;
                        pendingOps.incrementAndGet();
                    }
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalArgumentException("unknown refresh policy: " + request.getRefreshPolicy());
            }
            this.waitUntilRefresh = waitUntilRefresh;
            this.respond = respond;
            this.location = location;

            // 当采用 req作为事务日志持久化策略 那么每次索引操作都会引起事务日志的刷盘  如果采用异步刷盘策略 那么通过后台线程刷盘 索引操作不会强制刷盘
            if ((sync = indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null)) {
                pendingOps.incrementAndGet();
            }
            this.logger = logger;
            assert pendingOps.get() >= 0 && pendingOps.get() <= 3 : "pendingOpts was: " + pendingOps.get();
        }

        /**
         * calls the response listener if all pending operations have returned otherwise it just decrements the pending opts counter.
         */
        private void maybeFinish() {
            final int numPending = pendingOps.decrementAndGet();
            if (numPending == 0) {
                if (syncFailure.get() != null) {
                    respond.onFailure(syncFailure.get());
                } else {
                    respond.onSuccess(refreshed.get());
                }
            }
            assert numPending >= 0 && numPending <= 2 : "numPending must either 2, 1 or 0 but was " + numPending;
        }

        /**
         * 开始执行刷盘任务
         */
        void run() {
            /*
             * We either respond immediately (i.e., if we do not fsync per request or wait for
             * refresh), or we there are past async operations and we wait for them to return to
             * respond.
             * 这里只是尝试性刷盘  多副本实际上已经实现了高可用 不同于选举算法 任何操作都必须立即刷盘 并确保写入1/2以上
             * 这里只要能确保最终刷盘成功就可以
             */
            indexShard.afterWriteOperation();
            // decrement pending by one, if there is nothing else to do we just respond with success
            maybeFinish();
            // 如果设置等待直到refresh 就是追加一个刷新的监听器
            if (waitUntilRefresh) {
                assert pendingOps.get() > 0;
                indexShard.addRefreshListener(location, forcedRefresh -> {
                    if (forcedRefresh) {
                        logger.warn(
                            "block until refresh ran out of slots and forced a refresh: [{}]",
                            request);
                    }
                    refreshed.set(forcedRefresh);
                    maybeFinish();
                });
            }
            // 如果指定的刷盘策略为 req 也就是不具备后台线程刷盘功能  那么还是会强制刷盘  仅针对事务日志
            if (sync) {
                assert pendingOps.get() > 0;
                indexShard.sync(location, (ex) -> {
                    syncFailure.set(ex);
                    maybeFinish();
                });
            }
        }
    }

    /**
     * A proxy for <b>write</b> operations that need to be performed on the
     * replicas, where a failure to execute the operation should fail
     * the replica shard and/or mark the replica as stale.
     * <p>
     * This extends {@code TransportReplicationAction.ReplicasProxy} to do the
     * failing and stale-ing.
     */
    class WriteActionReplicasProxy extends ReplicasProxy {


        /**
         * 当在某个分片上执行任务失败 是否需要通知给leader节点进行关闭
         *
         * @param replica
         * @param primaryTerm
         * @param message
         * @param exception
         * @param listener
         */
        @Override
        public void failShardIfNeeded(ShardRouting replica, long primaryTerm, String message, Exception exception,
                                      ActionListener<Void> listener) {
            if (TransportActions.isShardNotAvailableException(exception) == false) {
                logger.warn(new ParameterizedMessage("[{}] {}", replica.shardId(), message), exception);
            }
            shardStateAction.remoteShardFailed(
                replica.shardId(), replica.allocationId().getId(), primaryTerm, true, message, exception, listener);
        }

        /**
         * 在这个阶段  原本应该将请求 发往所有副本  但是某些分片此时还未处于 in-sync 容器中  就不需要发送了
         * 同时上报给leader节点 该分片启动失败 那么leader节点就会选择其他节点初始化分片
         *
         * @param shardId
         * @param allocationId
         * @param primaryTerm
         * @param listener
         */
        @Override
        public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, long primaryTerm, ActionListener<Void> listener) {
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, true, "mark copy as stale", null, listener);
        }
    }
}
