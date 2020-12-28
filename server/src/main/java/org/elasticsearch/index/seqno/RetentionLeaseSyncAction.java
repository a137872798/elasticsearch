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

package org.elasticsearch.index.seqno;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.ReplicationTask;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.gateway.WriteStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Write action responsible for syncing retention leases to replicas. This action is deliberately a write action so that if a replica misses
 * a retention lease sync then that shard will be marked as stale.
 * 同步续约信息
 * 该任务在执行完成后 需要同步检查点
 */
public class RetentionLeaseSyncAction extends
        TransportWriteAction<RetentionLeaseSyncAction.Request, RetentionLeaseSyncAction.Request, RetentionLeaseSyncAction.Response> {

    public static final String ACTION_NAME = "indices:admin/seq_no/retention_lease_sync";
    private static final Logger LOGGER = LogManager.getLogger(RetentionLeaseSyncAction.class);

    protected Logger getLogger() {
        return LOGGER;
    }

    /**
     *
     * @param settings
     * @param transportService
     * @param clusterService
     * @param indicesService
     * @param threadPool
     * @param shardStateAction  该对象上注册了一个启动分片和关闭分片的action
     * @param actionFilters
     */
    @Inject
    public RetentionLeaseSyncAction(
            final Settings settings,
            final TransportService transportService,
            final ClusterService clusterService,
            final IndicesService indicesService,
            final ThreadPool threadPool,
            final ShardStateAction shardStateAction,
            final ActionFilters actionFilters) {
        super(
                settings,
                ACTION_NAME,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                actionFilters,
                RetentionLeaseSyncAction.Request::new,
                RetentionLeaseSyncAction.Request::new,
                ThreadPool.Names.MANAGEMENT, false);
    }

    @Override
    protected void doExecute(Task parentTask, Request request, ActionListener<Response> listener) {
        assert false : "use RetentionLeaseSyncAction#sync";
    }


    /**
     * 代表一个针对续约信息进行同步的请求
     * @param shardId
     * @param primaryAllocationId
     * @param primaryTerm
     * @param retentionLeases  此时本节点最新的续约集信息
     * @param listener   处理结果的监听器
     */
    final void sync(ShardId shardId, String primaryAllocationId, long primaryTerm, RetentionLeases retentionLeases,
                    ActionListener<ReplicationResponse> listener) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we have to execute under the system context so that if security is enabled the sync is authorized
            threadContext.markAsSystemContext();
            final Request request = new Request(shardId, retentionLeases);
            final ReplicationTask task = (ReplicationTask) taskManager.register("transport", "retention_lease_sync", request);
            // 请求会先发送到本地节点
            transportService.sendChildRequest(clusterService.localNode(), transportPrimaryAction,
                new ConcreteShardRequest<>(request, primaryAllocationId, primaryTerm),
                task,
                transportOptions,
                new TransportResponseHandler<ReplicationResponse>() {
                    @Override
                    public ReplicationResponse read(StreamInput in) throws IOException {
                        return newResponseInstance(in);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(ReplicationResponse response) {
                        task.setPhase("finished");
                        taskManager.unregister(task);
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException e) {
                        if (ExceptionsHelper.unwrap(e, AlreadyClosedException.class, IndexShardClosedException.class) == null) {
                            getLogger().warn(new ParameterizedMessage("{} retention lease sync failed", shardId), e);
                        }
                        task.setPhase("finished");
                        taskManager.unregister(task);
                        listener.onFailure(e);
                    }
                });
        }
    }

    /**
     * 同步续约信息的请求是由primary发起的 先由自身处理
     * @param request
     * @param primary      the primary shard to perform the operation on
     * @param listener listener for the result of the operation on primary, including current translog location and operation response
     */
    @Override
    protected void shardOperationOnPrimary(Request request, IndexShard primary,
            ActionListener<PrimaryResult<Request, Response>> listener) {
        ActionListener.completeWith(listener, () -> {
            assert request.waitForActiveShards().equals(ActiveShardCount.NONE) : request.waitForActiveShards();
            Objects.requireNonNull(request);
            Objects.requireNonNull(primary);
            // 这里就是将续约信息持久化
            primary.persistRetentionLeases();
            return new WritePrimaryResult<>(request, new Response(), null, null, primary, getLogger());
        });
    }

    /**
     * 在副本上执行任务
     * @param request
     * @param replica      the replica shard to perform the operation on
     * @return
     * @throws WriteStateException
     */
    @Override
    protected WriteReplicaResult<Request> shardOperationOnReplica(
            final Request request,
            final IndexShard replica) throws WriteStateException {
        Objects.requireNonNull(request);
        Objects.requireNonNull(replica);
        // 更新内存中的续约信息
        replica.updateRetentionLeasesOnReplica(request.getRetentionLeases());
        // 对续约信息进行持久化
        replica.persistRetentionLeases();
        return new WriteReplicaResult<>(request, null, null, replica, getLogger());
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
        return null;
    }

    public static final class Request extends ReplicatedWriteRequest<Request> {

        private RetentionLeases retentionLeases;

        public RetentionLeases getRetentionLeases() {
            return retentionLeases;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            retentionLeases = new RetentionLeases(in);
        }

        public Request(final ShardId shardId, final RetentionLeases retentionLeases) {
            super(Objects.requireNonNull(shardId));
            this.retentionLeases = Objects.requireNonNull(retentionLeases);
            // 注意默认情况下 在需要等待的活跃分片数为0
            waitForActiveShards(ActiveShardCount.NONE);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(Objects.requireNonNull(out));
            retentionLeases.writeTo(out);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new ReplicationTask(id, type, action, "retention_lease_sync shardId=" + shardId, parentTaskId, headers);
        }

        @Override
        public String toString() {
            return "RetentionLeaseSyncAction.Request{" +
                    "retentionLeases=" + retentionLeases +
                    ", shardId=" + shardId +
                    ", timeout=" + timeout +
                    ", index='" + index + '\'' +
                    ", waitForActiveShards=" + waitForActiveShards +
                    '}';
        }

    }

    public static final class Response extends ReplicationResponse implements WriteResponse {

        public Response() {}

        Response(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void setForcedRefresh(final boolean forcedRefresh) {
            // ignore
        }

    }

    @Override
    protected Response newResponseInstance(StreamInput in) throws IOException {
        return new Response(in);
    }

}
