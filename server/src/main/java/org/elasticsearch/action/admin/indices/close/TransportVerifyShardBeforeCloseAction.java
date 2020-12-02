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
package org.elasticsearch.action.admin.indices.close;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * 在某个shard 被关闭前 需要做认证工作
 * TODO 只看到在phase1时 是对分片做刷盘操作 还没有找到体现认证的地方
 */
public class TransportVerifyShardBeforeCloseAction extends TransportReplicationAction<
    TransportVerifyShardBeforeCloseAction.ShardRequest, TransportVerifyShardBeforeCloseAction.ShardRequest, ReplicationResponse> {

    public static final String NAME = CloseIndexAction.NAME + "[s]";
    public static final ActionType<ReplicationResponse> TYPE = new ActionType<>(NAME, ReplicationResponse::new);
    protected Logger logger = LogManager.getLogger(getClass());

    @Inject
    public TransportVerifyShardBeforeCloseAction(final Settings settings, final TransportService transportService,
                                                 final ClusterService clusterService, final IndicesService indicesService,
                                                 final ThreadPool threadPool, final ShardStateAction stateAction,
                                                 final ActionFilters actionFilters) {
        super(settings, NAME, transportService, clusterService, indicesService, threadPool, stateAction, actionFilters,
            ShardRequest::new, ShardRequest::new, ThreadPool.Names.MANAGEMENT);
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    /**
     * 获取主分片的操作许可
     * @param primary
     * @param request
     * @param onAcquired
     */
    @Override
    protected void acquirePrimaryOperationPermit(final IndexShard primary,
                                                 final ShardRequest request,
                                                 final ActionListener<Releasable> onAcquired) {
        primary.acquireAllPrimaryOperationsPermits(onAcquired, request.timeout());
    }

    /**
     * 在操作副本时 也需要获取所有的许可证
     * @param replica
     * @param request
     * @param onAcquired
     * @param primaryTerm
     * @param globalCheckpoint
     * @param maxSeqNoOfUpdateOrDeletes
     */
    @Override
    protected void acquireReplicaOperationPermit(final IndexShard replica,
                                                 final ShardRequest request,
                                                 final ActionListener<Releasable> onAcquired,
                                                 final long primaryTerm,
                                                 final long globalCheckpoint,
                                                 final long maxSeqNoOfUpdateOrDeletes) {
        replica.acquireAllReplicaOperationsPermits(primaryTerm, globalCheckpoint, maxSeqNoOfUpdateOrDeletes, onAcquired, request.timeout());
    }

    /**
     * 处理主分片的请求最后会委托给该方法
     * @param shardRequest the request to the primary shard   代表本次处理是针对哪个主分片
     * @param primary      the primary shard to perform the operation on
     * @param listener
     */
    @Override
    protected void shardOperationOnPrimary(final ShardRequest shardRequest, final IndexShard primary,
            ActionListener<PrimaryResult<ShardRequest, ReplicationResponse>> listener) {
        ActionListener.completeWith(listener,
            // 通过该函数获取结果 并触发监听器
            () -> {
            executeShardOperation(shardRequest, primary);
            return new PrimaryResult<>(shardRequest, new ReplicationResponse());
        });
    }

    /**
     * 当在该分片的副本上处理数据时 触发该方法
     * @param shardRequest the request to the replica shard
     * @param replica      the replica shard to perform the operation on
     * @return
     * @throws IOException
     */
    @Override
    protected ReplicaResult shardOperationOnReplica(final ShardRequest shardRequest, final IndexShard replica) throws IOException {
        executeShardOperation(shardRequest, replica);
        return new ReplicaResult();
    }

    /**
     * 根据请求处理某个分片
     * @param request
     * @param indexShard
     * @throws IOException
     */
    private void executeShardOperation(final ShardRequest request, final IndexShard indexShard) throws IOException {
        final ShardId shardId = indexShard.shardId();
        // 在执行close请求前 应该确保该分片的所有许可证都已经被抢占
        if (indexShard.getActiveOperationsCount() != IndexShard.OPERATIONS_BLOCKED) {
            throw new IllegalStateException("Index shard " + shardId + " is not blocking all operations during closing");
        }

        final ClusterBlocks clusterBlocks = clusterService.state().blocks();
        // 请求体如果有携带block 应该要加入到 clusterBlocks
        if (clusterBlocks.hasIndexBlock(shardId.getIndexName(), request.clusterBlock()) == false) {
            throw new IllegalStateException("Index shard " + shardId + " must be blocked by " + request.clusterBlock() + " before closing");
        }

        // 当此时还处于第1阶段时  应该就是通知集群中所有该shardId 对应的分片进行刷盘
        if (request.isPhase1()) {
            // in order to advance the global checkpoint to the maximum sequence number, the (persisted) local checkpoint needs to be
            // advanced first, which, when using async translog syncing, does not automatically hold at the time where we have acquired
            // all operation permits. Instead, this requires and explicit sync, which communicates the updated (persisted) local checkpoint
            // to the primary (we call this phase1), and phase2 can then use the fact that the global checkpoint has moved to the maximum
            // sequence number to pass the verifyShardBeforeIndexClosing check and create a safe commit where the maximum sequence number
            // is equal to the global checkpoint.
            // 将分片的所有数据刷盘
            indexShard.sync();
        } else {
            // 代表进入了第二阶段   这里是确保全局检查点 与 maxSeq相同
            indexShard.verifyShardBeforeIndexClosing();
            // 将lucene内的数据刷盘
            indexShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            logger.trace("{} shard is ready for closing", shardId);
        }
    }

    /**
     * 创建一个副本代理对象
     * @return
     */
    @Override
    protected ReplicationOperation.Replicas<ShardRequest> newReplicasProxy() {
        return new VerifyShardBeforeCloseActionReplicasProxy();
    }

    /**
     * A {@link ReplicasProxy} that marks as stale the shards that are unavailable during the verification
     * and the flush of the shard. This is done to ensure that such shards won't be later promoted as primary
     * or reopened in an unverified state with potential non flushed translog operations.
     */
    class VerifyShardBeforeCloseActionReplicasProxy extends ReplicasProxy {

        /**
         * 这个时候 当某个shard.primary所在的节点在处理完一个请求后  会检查当前副本组中是否有落后的分片  落后的分片存储在2个容器中 一个是 unavailableInSyncShards   一个是 inSyncShards
         *
         * 将某些分片标记成无效状态
         * @param shardId  本次需要被标记的分片id
         * @param allocationId   这个id才是作为唯一性的标识 一个shardId下的多个 primary/replica 会使用不同的allocationId
         * @param primaryTerm
         * @param listener   当处理完成后会触发监听器
         */
        @Override
        public void markShardCopyAsStaleIfNeeded(final ShardId shardId, final String allocationId, final long primaryTerm,
                                                 final ActionListener<Void> listener) {
            // 本次操作会发送到 leader节点上
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, true, "mark copy as stale", null, listener);
        }
    }

    public static class ShardRequest extends ReplicationRequest<ShardRequest> {

        private final ClusterBlock clusterBlock;

        private final boolean phase1;

        ShardRequest(StreamInput in) throws IOException {
            super(in);
            clusterBlock = new ClusterBlock(in);
            if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
                phase1 = in.readBoolean();
            } else {
                phase1 = false;
            }
        }

        public ShardRequest(final ShardId shardId, final ClusterBlock clusterBlock, final boolean phase1, final TaskId parentTaskId) {
            super(shardId);
            this.clusterBlock = Objects.requireNonNull(clusterBlock);
            this.phase1 = phase1;
            // 设置父级任务的id
            setParentTask(parentTaskId);
        }

        @Override
        public String toString() {
            return "verify shard " + shardId + " before close with block " + clusterBlock;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            clusterBlock.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
                out.writeBoolean(phase1);
            }
        }

        public ClusterBlock clusterBlock() {
            return clusterBlock;
        }

        public boolean isPhase1() {
            return phase1;
        }
    }
}
