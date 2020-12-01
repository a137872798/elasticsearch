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

package org.elasticsearch.cluster.action.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.MasterNodeChangePredicate;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.StaleShard;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestDeduplicator;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * 对应一个获取分片状态的请求
 */
public class ShardStateAction {

    private static final Logger logger = LogManager.getLogger(ShardStateAction.class);

    public static final String SHARD_STARTED_ACTION_NAME = "internal:cluster/shard/started";
    public static final String SHARD_FAILED_ACTION_NAME = "internal:cluster/shard/failure";

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    // a list of shards that failed during replication
    // we keep track of these shards in order to avoid sending duplicate failed shard requests for a single failing shard.
    // 该对象会维护发往相关节点的监听器
    private final TransportRequestDeduplicator<FailedShardEntry> remoteFailedShardsDeduplicator = new TransportRequestDeduplicator<>();

    @Inject
    public ShardStateAction(ClusterService clusterService, TransportService transportService,
                            AllocationService allocationService, RerouteService rerouteService, ThreadPool threadPool) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        // 分片在启动和关闭 会注册不同的处理器
        transportService.registerRequestHandler(SHARD_STARTED_ACTION_NAME, ThreadPool.Names.SAME, StartedShardEntry::new,
            new ShardStartedTransportHandler(clusterService,
                new ShardStartedClusterStateTaskExecutor(allocationService, rerouteService, logger),
                logger));
        transportService.registerRequestHandler(SHARD_FAILED_ACTION_NAME, ThreadPool.Names.SAME, FailedShardEntry::new,
            new ShardFailedTransportHandler(clusterService,
                new ShardFailedClusterStateTaskExecutor(allocationService, rerouteService, logger),
                logger));
    }

    /**
     * 将某个请求发送到相关节点上
     * @param actionName    本次操作的名称 用于匹配action对象
     * @param currentState   当前集群状态
     * @param request   发送的请求体
     * @param listener   处理成功后触发的监听器
     */
    private void sendShardAction(final String actionName, final ClusterState currentState,
                                 final TransportRequest request, final ActionListener<Void> listener) {
        ClusterStateObserver observer =
            new ClusterStateObserver(currentState, clusterService, null, logger, threadPool.getThreadContext());
        // 获取leader节点 看来某个分片的相关操作都是发送到主控节点去处理的
        DiscoveryNode masterNode = currentState.nodes().getMasterNode();

        // 检测leader节点是否发生变化的谓语
        Predicate<ClusterState> changePredicate = MasterNodeChangePredicate.build(currentState);
        // 如果当前主节点未确定 比如还处于选举阶段 那么就需要等待 clusterState 发生变化
        if (masterNode == null) {
            logger.warn("no master known for action [{}] for shard entry [{}]", actionName, request);
            waitForNewMasterAndRetry(actionName, observer, request, listener, changePredicate);
        } else {
            logger.debug("sending [{}] to [{}] for shard entry [{}]", actionName, masterNode.getId(), request);
            // 将请求发往leader 节点 将某个shard关闭    对应的传输层处理器为  ShardFailedTransportHandler
            transportService.sendRequest(masterNode,
                actionName, request, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        listener.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (isMasterChannelException(exp)) {
                            waitForNewMasterAndRetry(actionName, observer, request, listener, changePredicate);
                        } else {
                            logger.warn(new ParameterizedMessage("unexpected failure while sending request [{}]" +
                                " to [{}] for shard entry [{}]", actionName, masterNode, request), exp);
                            listener.onFailure(exp instanceof RemoteTransportException ?
                                (Exception) (exp.getCause() instanceof Exception ? exp.getCause() :
                                    new ElasticsearchException(exp.getCause())) : exp);
                        }
                    }
                });
        }
    }

    private static Class[] MASTER_CHANNEL_EXCEPTIONS = new Class[]{
        NotMasterException.class,
        ConnectTransportException.class,
        FailedToCommitClusterStateException.class
    };

    private static boolean isMasterChannelException(TransportException exp) {
        return ExceptionsHelper.unwrap(exp, MASTER_CHANNEL_EXCEPTIONS) != null;
    }

    /**
     * Send a shard failed request to the master node to update the cluster state with the failure of a shard on another node. This means
     * that the shard should be failed because a write made it into the primary but was not replicated to this shard copy. If the shard
     * does not exist anymore but still has an entry in the in-sync set, remove its allocation id from the in-sync set.
     *
     * @param shardId            shard id of the shard to fail
     * @param allocationId       allocation id of the shard to fail
     * @param primaryTerm        the primary term associated with the primary shard that is failing the shard. Must be strictly positive.
     * @param markAsStale        whether or not to mark a failing shard as stale (eg. removing from in-sync set) when failing the shard.
     * @param message            the reason for the failure
     * @param failure            the underlying cause of the failure
     * @param listener           callback upon completion of the request
     * 将某个分片标记成失败状态
     */
    public void remoteShardFailed(final ShardId shardId, String allocationId, long primaryTerm, boolean markAsStale, final String message,
                                  @Nullable final Exception failure, ActionListener<Void> listener) {
        assert primaryTerm > 0L : "primary term should be strictly positive";
        remoteFailedShardsDeduplicator.executeOnce(
            // 将相关信息包装成 entry对象
            new FailedShardEntry(shardId, allocationId, primaryTerm, message, failure, markAsStale), listener,
            // 回调对象 当该 failedShardEntry被首次加入到内部的管理容器时 会触发该回调
            (req, reqListener) -> sendShardAction(SHARD_FAILED_ACTION_NAME, clusterService.state(), req, reqListener));
    }

    int remoteShardFailedCacheSize() {
        return remoteFailedShardsDeduplicator.size();
    }

    /**
     * Send a shard failed request to the master node to update the cluster state when a shard on the local node failed.
     */
    public void localShardFailed(final ShardRouting shardRouting, final String message,
                                 @Nullable final Exception failure, ActionListener<Void> listener) {
        localShardFailed(shardRouting, message, failure, listener, clusterService.state());
    }

    /**
     * Send a shard failed request to the master node to update the cluster state when a shard on the local node failed.
     */
    public void localShardFailed(final ShardRouting shardRouting, final String message, @Nullable final Exception failure,
                                 ActionListener<Void> listener, final ClusterState currentState) {
        FailedShardEntry shardEntry = new FailedShardEntry(shardRouting.shardId(), shardRouting.allocationId().getId(),
            0L, message, failure, true);
        sendShardAction(SHARD_FAILED_ACTION_NAME, currentState, shardEntry, listener);
    }

    /**
     * 等待leader节点发生变化
     * @param actionName
     * @param observer
     * @param request
     * @param listener
     * @param changePredicate
     */
    protected void waitForNewMasterAndRetry(String actionName, ClusterStateObserver observer,
                                            TransportRequest request, ActionListener<Void> listener,
                                            Predicate<ClusterState> changePredicate) {
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                if (logger.isTraceEnabled()) {
                    logger.trace("new cluster state [{}] after waiting for master election for shard entry [{}]", state, request);
                }
                // 当确定了leader节点后 触发该函数
                sendShardAction(actionName, state, request, listener);
            }

            @Override
            public void onClusterServiceClose() {
                logger.warn("node closed while execution action [{}] for shard entry [{}]", actionName, request);
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                // we wait indefinitely for a new master
                assert false;
            }
        }, changePredicate);
    }

    /**
     * 当某个分片被标记成无效时 需要在leader节点进行处理
     * 是这样 先找到shard.primary所在的node 发送请求 获取primary对应replicaGroup  找到在 unavailable容器中的分片 向leader节点发起关闭的请求
     * 而该对象就是处理关闭请求的
     */
    private static class ShardFailedTransportHandler implements TransportRequestHandler<FailedShardEntry> {

        /**
         * 集群服务对象
         */
        private final ClusterService clusterService;
        private final ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor;
        private final Logger logger;

        ShardFailedTransportHandler(ClusterService clusterService,
                                    ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor, Logger logger) {
            this.clusterService = clusterService;
            this.shardFailedClusterStateTaskExecutor = shardFailedClusterStateTaskExecutor;
            this.logger = logger;
        }

        /**
         * 接收到要关闭某个shard的请求
         * @param request  本次的请求对象
         * @param channel   使用的通道信息
         * @param task   本次相关的任务信息
         * @throws Exception
         */
        @Override
        public void messageReceived(FailedShardEntry request, TransportChannel channel, Task task) throws Exception {
            logger.debug(() -> new ParameterizedMessage("{} received shard failed for {}",
                request.shardId, request), request.failure);

            // 提交一个集群更新的任务
            clusterService.submitStateUpdateTask(
                "shard-failed",
                request,
                ClusterStateTaskConfig.build(Priority.HIGH),
                shardFailedClusterStateTaskExecutor,
                new ClusterStateTaskListener() {
                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(() -> new ParameterizedMessage("{} unexpected failure while failing shard [{}]",
                            request.shardId, request), e);
                        try {
                            channel.sendResponse(e);
                        } catch (Exception channelException) {
                            channelException.addSuppressed(e);
                            logger.warn(() ->
                                new ParameterizedMessage("{} failed to send failure [{}] while failing shard [{}]",
                                    request.shardId, e, request), channelException);
                        }
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        logger.error("{} no longer master while failing shard [{}]", request.shardId, request);
                        try {
                            channel.sendResponse(new NotMasterException(source));
                        } catch (Exception channelException) {
                            logger.warn(() ->
                                new ParameterizedMessage("{} failed to send no longer master while failing shard [{}]",
                                    request.shardId, request), channelException);
                        }
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        try {
                            channel.sendResponse(TransportResponse.Empty.INSTANCE);
                        } catch (Exception channelException) {
                            logger.warn(() ->
                                new ParameterizedMessage("{} failed to send response while failing shard [{}]",
                                    request.shardId, request), channelException);
                        }
                    }
                }
            );
        }
    }

    /**
     * 任务处理器对象  定义了如何处理更新任务
     */
    public static class ShardFailedClusterStateTaskExecutor implements ClusterStateTaskExecutor<FailedShardEntry> {
        private final AllocationService allocationService;
        private final RerouteService rerouteService;
        private final Logger logger;

        public ShardFailedClusterStateTaskExecutor(AllocationService allocationService, RerouteService rerouteService, Logger logger) {
            this.allocationService = allocationService;
            this.rerouteService = rerouteService;
            this.logger = logger;
        }

        /**
         * 当接收到任务后 开始处理请求
         * @param currentState
         * @param tasks
         * @return
         * @throws Exception
         */
        @Override
        public ClusterTasksResult<FailedShardEntry> execute(ClusterState currentState, List<FailedShardEntry> tasks) throws Exception {

            // 该对象用于处理某个集群状态更新的结果
            ClusterTasksResult.Builder<FailedShardEntry> batchResultBuilder = ClusterTasksResult.builder();
            List<FailedShardEntry> tasksToBeApplied = new ArrayList<>();

            // 能够在路由表中找到的分片 就会被划分到这个列表中
            List<FailedShard> failedShardsToBeApplied = new ArrayList<>();

            // 检测到未完成数据同步的分片存储到这个列表中
            List<StaleShard> staleShardsToBeApplied = new ArrayList<>();

            // 遍历处理 分片失败任务
            for (FailedShardEntry task : tasks) {
                IndexMetadata indexMetadata = currentState.metadata().index(task.shardId.getIndex());
                if (indexMetadata == null) {
                    // tasks that correspond to non-existent indices are marked as successful
                    logger.debug("{} ignoring shard failed task [{}] (unknown index {})",
                        task.shardId, task, task.shardId.getIndex());
                    // 当对应的索引元数据不存在时 静默处理 认为已经执行成功
                    batchResultBuilder.success(task);
                } else {
                    // The primary term is 0 if the shard failed itself. It is > 0 if a write was done on a primary but was failed to be
                    // replicated to the shard copy with the provided allocation id. In case where the shard failed itself, it's ok to just
                    // remove the corresponding routing entry from the routing table. In case where a write could not be replicated,
                    // however, it is important to ensure that the shard copy with the missing write is considered as stale from that point
                    // on, which is implemented by removing the allocation id of the shard copy from the in-sync allocations set.
                    // We check here that the primary to which the write happened was not already failed in an earlier cluster state update.
                    // This prevents situations where a new primary has already been selected and replication failures from an old stale
                    // primary unnecessarily fail currently active shards.
                    // 如果此时发起任务时 任期已经发生了变化了 也就是ABA 的问题 那么本次任务执行失败
                    if (task.primaryTerm > 0) {
                        long currentPrimaryTerm = indexMetadata.primaryTerm(task.shardId.id());
                        if (currentPrimaryTerm != task.primaryTerm) {
                            assert currentPrimaryTerm > task.primaryTerm : "received a primary term with a higher term than in the " +
                                "current cluster state (received [" + task.primaryTerm + "] but current is [" + currentPrimaryTerm + "])";
                            logger.debug("{} failing shard failed task [{}] (primary term {} does not match current term {})", task.shardId,
                                task, task.primaryTerm, indexMetadata.primaryTerm(task.shardId.id()));
                            batchResultBuilder.failure(task, new NoLongerPrimaryShardException(
                                task.shardId,
                                "primary term [" + task.primaryTerm + "] did not match current primary term [" + currentPrimaryTerm + "]"));
                            continue;
                        }
                    }

                    // 找到匹配的路由信息  路由表会在集群内所有节点做同步
                    ShardRouting matched = currentState.getRoutingTable().getByAllocationId(task.shardId, task.allocationId);

                    // 当没有找到匹配信息时
                    if (matched == null) {
                        Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(task.shardId.id());
                        // mark shard copies without routing entries that are in in-sync allocations set only as stale if the reason why
                        // they were failed is because a write made it into the primary but not to this copy (which corresponds to
                        // the check "primaryTerm > 0").
                        // 应该是同步完成后的分片 才会进行分配 才会到路由表中  所以上面没有分配信息 就从同步队列中查找
                        if (task.primaryTerm > 0 && inSyncAllocationIds.contains(task.allocationId)) {
                            logger.debug("{} marking shard {} as stale (shard failed task: [{}])",
                                task.shardId, task.allocationId, task);

                            // 这种情况代表这个分片是一个过期的分片  也就是未完成数据同步的分片
                            tasksToBeApplied.add(task);
                            // 将分片包装成 stale对象并存储到队列中
                            staleShardsToBeApplied.add(new StaleShard(task.shardId, task.allocationId));
                        } else {
                            // tasks that correspond to non-existent shards are marked as successful
                            logger.debug("{} ignoring shard failed task [{}] (shard does not exist anymore)", task.shardId, task);
                            // 静默处理
                            batchResultBuilder.success(task);
                        }
                    } else {
                        // failing a shard also possibly marks it as stale (see IndexMetadataUpdater)
                        logger.debug("{} failing shard {} (shard failed task: [{}])", task.shardId, matched, task);
                        // 如果在路由表中找到了这个分片 就标记成失败的分片
                        tasksToBeApplied.add(task);
                        failedShardsToBeApplied.add(new FailedShard(matched, task.message, task.failure, task.markAsStale));
                    }
                }
            }
            assert tasksToBeApplied.size() == failedShardsToBeApplied.size() + staleShardsToBeApplied.size();

            ClusterState maybeUpdatedState = currentState;
            try {
                // 根据这些失败/过期的分片  更新集群状态  其中会涉及到某些分片被剔除  以及更新集群内分片的分布情况
                maybeUpdatedState = applyFailedShards(currentState, failedShardsToBeApplied, staleShardsToBeApplied);
                // 这组任务都成功执行
                batchResultBuilder.successes(tasksToBeApplied);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to apply failed shards {}", failedShardsToBeApplied), e);
                // failures are communicated back to the requester
                // cluster state will not be updated in this case
                batchResultBuilder.failures(tasksToBeApplied, e);
            }

            // 通过最新的集群状态来生成 ClusterTasksResult
            return batchResultBuilder.build(maybeUpdatedState);
        }

        /**
         * 某些分片此时被标记成无效分片了 可能其他相关分片会发生 reroute
         * @param currentState
         * @param failedShards
         * @param staleShards
         * @return
         */
        ClusterState applyFailedShards(ClusterState currentState, List<FailedShard> failedShards, List<StaleShard> staleShards) {
            return allocationService.applyFailedShards(currentState, failedShards, staleShards);
        }

        /**
         * 将状态变化的事件发布到集群中
         * @param clusterChangedEvent the change event for this cluster state change, containing
         */
        @Override
        public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
            int numberOfUnassignedShards = clusterChangedEvent.state().getRoutingNodes().unassigned().size();
            if (numberOfUnassignedShards > 0) {
                // The reroute called after failing some shards will not assign any shard back to the node on which it failed. If there were
                // no other options for a failed shard then it is left unassigned. However, absent other options it's better to try and
                // assign it again, even if that means putting it back on the node on which it previously failed:
                final String reason = String.format(Locale.ROOT, "[%d] unassigned shards after failing shards", numberOfUnassignedShards);
                logger.trace("{}, scheduling a reroute", reason);
                // 执行重路由 并监听结果
                rerouteService.reroute(reason, Priority.NORMAL, ActionListener.wrap(
                    r -> logger.trace("{}, reroute completed", reason),
                    e -> logger.debug(new ParameterizedMessage("{}, reroute failed", reason), e)));
            }
        }
    }

    /**
     * 该请求代表某个分片被标记成了无效状态
     */
    public static class FailedShardEntry extends TransportRequest {

        /**
         * 代表本次针对的分片
         */
        final ShardId shardId;
        /**
         * 分片 + targetNode 对应一个唯一的allocationId
         */
        final String allocationId;
        final long primaryTerm;
        final String message;
        final Exception failure;
        final boolean markAsStale;

        FailedShardEntry(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            allocationId = in.readString();
            primaryTerm = in.readVLong();
            message = in.readString();
            failure = in.readException();
            markAsStale = in.readBoolean();
        }

        public FailedShardEntry(ShardId shardId, String allocationId, long primaryTerm,
                                String message, Exception failure, boolean markAsStale) {
            this.shardId = shardId;
            this.allocationId = allocationId;
            this.primaryTerm = primaryTerm;
            this.message = message;
            this.failure = failure;
            this.markAsStale = markAsStale;
        }

        public ShardId getShardId() {
            return shardId;
        }

        public String getAllocationId() {
            return allocationId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(allocationId);
            out.writeVLong(primaryTerm);
            out.writeString(message);
            out.writeException(failure);
            out.writeBoolean(markAsStale);
        }

        @Override
        public String toString() {
            List<String> components = new ArrayList<>(6);
            components.add("shard id [" + shardId + "]");
            components.add("allocation id [" + allocationId + "]");
            components.add("primary term [" + primaryTerm + "]");
            components.add("message [" + message + "]");
            components.add("markAsStale [" + markAsStale + "]");
            if (failure != null) {
                components.add("failure [" + ExceptionsHelper.stackTrace(failure) + "]");
            }
            return String.join(", ", components);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FailedShardEntry that = (FailedShardEntry) o;
            // Exclude message and exception from equals and hashCode
            return Objects.equals(this.shardId, that.shardId) &&
                Objects.equals(this.allocationId, that.allocationId) &&
                primaryTerm == that.primaryTerm &&
                markAsStale == that.markAsStale;
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, allocationId, primaryTerm, markAsStale);
        }
    }

    public void shardStarted(final ShardRouting shardRouting,
                             final long primaryTerm,
                             final String message,
                             final ActionListener<Void> listener) {
        shardStarted(shardRouting, primaryTerm, message, listener, clusterService.state());
    }

    public void shardStarted(final ShardRouting shardRouting,
                             final long primaryTerm,
                             final String message,
                             final ActionListener<Void> listener,
                             final ClusterState currentState) {
        StartedShardEntry entry = new StartedShardEntry(shardRouting.shardId(), shardRouting.allocationId().getId(), primaryTerm, message);
        sendShardAction(SHARD_STARTED_ACTION_NAME, currentState, entry, listener);
    }

    private static class ShardStartedTransportHandler implements TransportRequestHandler<StartedShardEntry> {
        private final ClusterService clusterService;
        private final ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor;
        private final Logger logger;

        ShardStartedTransportHandler(ClusterService clusterService,
                                     ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor, Logger logger) {
            this.clusterService = clusterService;
            this.shardStartedClusterStateTaskExecutor = shardStartedClusterStateTaskExecutor;
            this.logger = logger;
        }

        @Override
        public void messageReceived(StartedShardEntry request, TransportChannel channel, Task task) throws Exception {
            logger.debug("{} received shard started for [{}]", request.shardId, request);
            clusterService.submitStateUpdateTask(
                "shard-started " + request,
                request,
                ClusterStateTaskConfig.build(Priority.URGENT),
                shardStartedClusterStateTaskExecutor,
                shardStartedClusterStateTaskExecutor);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    public static class ShardStartedClusterStateTaskExecutor
            implements ClusterStateTaskExecutor<StartedShardEntry>, ClusterStateTaskListener {
        private final AllocationService allocationService;
        private final Logger logger;
        private final RerouteService rerouteService;

        public ShardStartedClusterStateTaskExecutor(AllocationService allocationService, RerouteService rerouteService, Logger logger) {
            this.allocationService = allocationService;
            this.logger = logger;
            this.rerouteService = rerouteService;
        }

        @Override
        public ClusterTasksResult<StartedShardEntry> execute(ClusterState currentState, List<StartedShardEntry> tasks) throws Exception {
            ClusterTasksResult.Builder<StartedShardEntry> builder = ClusterTasksResult.builder();
            List<StartedShardEntry> tasksToBeApplied = new ArrayList<>();
            List<ShardRouting> shardRoutingsToBeApplied = new ArrayList<>(tasks.size());
            Set<ShardRouting> seenShardRoutings = new HashSet<>(); // to prevent duplicates
            for (StartedShardEntry task : tasks) {
                final ShardRouting matched = currentState.getRoutingTable().getByAllocationId(task.shardId, task.allocationId);
                if (matched == null) {
                    // tasks that correspond to non-existent shards are marked as successful. The reason is that we resend shard started
                    // events on every cluster state publishing that does not contain the shard as started yet. This means that old stale
                    // requests might still be in flight even after the shard has already been started or failed on the master. We just
                    // ignore these requests for now.
                    logger.debug("{} ignoring shard started task [{}] (shard does not exist anymore)", task.shardId, task);
                    builder.success(task);
                } else {
                    if (matched.primary() && task.primaryTerm > 0) {
                        final IndexMetadata indexMetadata = currentState.metadata().index(task.shardId.getIndex());
                        assert indexMetadata != null;
                        final long currentPrimaryTerm = indexMetadata.primaryTerm(task.shardId.id());
                        if (currentPrimaryTerm != task.primaryTerm) {
                            assert currentPrimaryTerm > task.primaryTerm : "received a primary term with a higher term than in the " +
                                "current cluster state (received [" + task.primaryTerm + "] but current is [" + currentPrimaryTerm + "])";
                            logger.debug("{} ignoring shard started task [{}] (primary term {} does not match current term {})",
                                task.shardId, task, task.primaryTerm, currentPrimaryTerm);
                            builder.success(task);
                            continue;
                        }
                    }
                    if (matched.initializing() == false) {
                        assert matched.active() : "expected active shard routing for task " + task + " but found " + matched;
                        // same as above, this might have been a stale in-flight request, so we just ignore.
                        logger.debug("{} ignoring shard started task [{}] (shard exists but is not initializing: {})",
                            task.shardId, task, matched);
                        builder.success(task);
                    } else {
                        // remove duplicate actions as allocation service expects a clean list without duplicates
                        if (seenShardRoutings.contains(matched)) {
                            logger.trace("{} ignoring shard started task [{}] (already scheduled to start {})",
                                task.shardId, task, matched);
                            tasksToBeApplied.add(task);
                        } else {
                            logger.debug("{} starting shard {} (shard started task: [{}])", task.shardId, matched, task);
                            tasksToBeApplied.add(task);
                            shardRoutingsToBeApplied.add(matched);
                            seenShardRoutings.add(matched);
                        }
                    }
                }
            }
            assert tasksToBeApplied.size() >= shardRoutingsToBeApplied.size();

            ClusterState maybeUpdatedState = currentState;
            try {
                maybeUpdatedState = allocationService.applyStartedShards(currentState, shardRoutingsToBeApplied);
                builder.successes(tasksToBeApplied);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to apply started shards {}", shardRoutingsToBeApplied), e);
                builder.failures(tasksToBeApplied, e);
            }

            return builder.build(maybeUpdatedState);
        }

        @Override
        public void onFailure(String source, Exception e) {
            if (e instanceof FailedToCommitClusterStateException || e instanceof NotMasterException) {
                logger.debug(() -> new ParameterizedMessage("failure during [{}]", source), e);
            } else {
                logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
            }
        }

        @Override
        public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
            rerouteService.reroute("reroute after starting shards", Priority.NORMAL, ActionListener.wrap(
                r -> logger.trace("reroute after starting shards succeeded"),
                e -> logger.debug("reroute after starting shards failed", e)));
        }
    }

    public static class StartedShardEntry extends TransportRequest {
        final ShardId shardId;
        final String allocationId;
        final long primaryTerm;
        final String message;

        StartedShardEntry(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            allocationId = in.readString();
            primaryTerm = in.readVLong();
            this.message = in.readString();
        }

        public StartedShardEntry(final ShardId shardId, final String allocationId, final long primaryTerm, final String message) {
            this.shardId = shardId;
            this.allocationId = allocationId;
            this.primaryTerm = primaryTerm;
            this.message = message;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(allocationId);
            out.writeVLong(primaryTerm);
            out.writeString(message);
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT,  "StartedShardEntry{shardId [%s], allocationId [%s], primary term [%d], message [%s]}",
                shardId, allocationId, primaryTerm, message);
        }
    }

    public static class NoLongerPrimaryShardException extends ElasticsearchException {

        public NoLongerPrimaryShardException(ShardId shardId, String msg) {
            super(msg);
            setShard(shardId);
        }

        public NoLongerPrimaryShardException(StreamInput in) throws IOException {
            super(in);
        }

    }

}
