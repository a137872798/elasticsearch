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

package org.elasticsearch.indices.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource.Type;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardRelocatedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.CLOSED;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.FAILURE;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.REOPENED;

/**
 * 通过监听集群状态的变化  操控indicesService修改索引
 * 因为交互逻辑比较复杂就没有让 indicesService去直接监听CS
 */
public class IndicesClusterStateService extends AbstractLifecycleComponent implements ClusterStateApplier {
    private static final Logger logger = LogManager.getLogger(IndicesClusterStateService.class);

    /**
     * 就是 indicesService
     */
    final AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService;

    /**
     * 集群服务主要是提供监听CS变化 以及更新CS的 本对象会被注册到该对象中 以监听集群变化
     */
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    /**
     * 从其他节点拉取数据 用于恢复本节点数据
     */
    private final PeerRecoveryTargetService recoveryTargetService;
    private final ShardStateAction shardStateAction;
    private final NodeMappingRefreshAction nodeMappingRefreshAction;

    private static final ActionListener<Void> SHARD_STATE_ACTION_LISTENER = ActionListener.wrap(() -> {});

    private final Settings settings;
    // a list of shards that failed during recovery
    // we keep track of these shards in order to prevent repeated recovery of these shards on each cluster state update
    // 记录一些处理失败的shard
    final ConcurrentMap<ShardId, ShardRouting> failedShardsCache = ConcurrentCollections.newConcurrentMap();
    private final RepositoriesService repositoriesService;

    private final FailedShardHandler failedShardHandler = new FailedShardHandler();

    private final boolean sendRefreshMapping;

    /**
     * 处理索引的相关事件 比如某个索引被创建/删除
     * 几个核心服务都实现了  indexEventListener接口
     */
    private final List<IndexEventListener> buildInIndexListener;
    private final PrimaryReplicaSyncer primaryReplicaSyncer;
    private final RetentionLeaseSyncer retentionLeaseSyncer;
    private final NodeClient client;

    @Inject
    public IndicesClusterStateService(
            final Settings settings,
            final IndicesService indicesService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final PeerRecoveryTargetService recoveryTargetService,
            final ShardStateAction shardStateAction,
            final NodeMappingRefreshAction nodeMappingRefreshAction,
            final RepositoriesService repositoriesService,
            final SearchService searchService,
            final PeerRecoverySourceService peerRecoverySourceService,
            final SnapshotShardsService snapshotShardsService,
            final PrimaryReplicaSyncer primaryReplicaSyncer,
            final RetentionLeaseSyncer retentionLeaseSyncer,
            final NodeClient client) {
        this(
                settings,
                (AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>>) indicesService,
                clusterService,
                threadPool,
                recoveryTargetService,
                shardStateAction,
                nodeMappingRefreshAction,
                repositoriesService,
                searchService,
                peerRecoverySourceService,
                snapshotShardsService,
                primaryReplicaSyncer,
                retentionLeaseSyncer,
                client);
    }

    /**
     * 初始化只是简单的赋值
     * @param settings
     * @param indicesService
     * @param clusterService
     * @param threadPool
     * @param recoveryTargetService
     * @param shardStateAction
     * @param nodeMappingRefreshAction
     * @param repositoriesService
     * @param searchService
     * @param peerRecoverySourceService
     * @param snapshotShardsService
     * @param primaryReplicaSyncer
     * @param retentionLeaseSyncer
     * @param client
     */
    IndicesClusterStateService(
            final Settings settings,
            final AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final PeerRecoveryTargetService recoveryTargetService,
            final ShardStateAction shardStateAction,
            final NodeMappingRefreshAction nodeMappingRefreshAction,
            final RepositoriesService repositoriesService,
            final SearchService searchService,
            final PeerRecoverySourceService peerRecoverySourceService,
            final SnapshotShardsService snapshotShardsService,
            final PrimaryReplicaSyncer primaryReplicaSyncer,
            final RetentionLeaseSyncer retentionLeaseSyncer,
            final NodeClient client) {
        this.settings = settings;
        // 这几个服务都会监听 shard/index的创建 关闭事件
        this.buildInIndexListener = Arrays.asList(peerRecoverySourceService, recoveryTargetService, searchService, snapshotShardsService);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryTargetService = recoveryTargetService;
        this.shardStateAction = shardStateAction;
        this.nodeMappingRefreshAction = nodeMappingRefreshAction;
        this.repositoriesService = repositoriesService;
        this.primaryReplicaSyncer = primaryReplicaSyncer;
        this.retentionLeaseSyncer = retentionLeaseSyncer;
        this.sendRefreshMapping = settings.getAsBoolean("indices.cluster.send_refresh_mapping", true);
        this.client = client;
    }

    /**
     * 将自身添加到集群服务中 这样CS发生变化会通知到该对象
     */
    @Override
    protected void doStart() {
        // Doesn't make sense to manage shards on non-master and non-data nodes
        if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isMasterNode(settings)) {
            clusterService.addHighPriorityApplier(this);
        }
    }

    @Override
    protected void doStop() {
        if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isMasterNode(settings)) {
            clusterService.removeApplier(this);
        }
    }

    @Override
    protected void doClose() {
    }

    /**
     * 当感知到集群变化后触发该钩子
     * @param event
     */
    @Override
    public synchronized void applyClusterState(final ClusterChangedEvent event) {
        if (!lifecycle.started()) {
            return;
        }

        final ClusterState state = event.state();

        // we need to clean the shards and indices we have on this node, since we
        // are going to recover them again once state persistence is disabled (no master / not recovered)
        // TODO: feels hacky, a block disables state persistence, and then we clean the allocated shards, maybe another flag in blocks?
        // 当此时不允许持久化state信息时   这时将管理的所有索引移除
        if (state.blocks().disableStatePersistence()) {
            for (AllocatedIndex<? extends Shard> indexService : indicesService) {
                // also cleans shards
                indicesService.removeIndex(indexService.index(), NO_LONGER_ASSIGNED, "cleaning index (disabled block persistence)");
            }
            return;
        }

        // 在处理分片的过程中可能会失败 这时候检测这个分片有没有在cluster中被重新分配 如果没有 则通知leader节点 提醒它进行重分配
        updateFailedShardsCache(state);

        // 如果某些index不再分配到该节点 进行移除  同时还要清理残留数据
        deleteIndices(event); // also deletes shards of deleted indices

        // 索引状态可能被更新了 比如从OPEN->CLOSED 那么要关闭indexService
        removeIndices(event); // also removes shards of removed indices

        // 某些分片不存在  通知leader节点
        failMissingShards(state);

        // 某些分片可能已经被移动
        removeShards(state);   // removes any local shards that doesn't match what the master expects

        // 更新indexMetadata的信息
        updateIndices(event); // can also fail shards, but these are then guaranteed to be in failedShardsCache

        // 处理新增的index
        createIndices(state);

        // 处理新增/更新的shard
        createOrUpdateShards(state);
    }

    /**
     * Removes shard entries from the failed shards cache that are no longer allocated to this node by the master.
     * Sends shard failures for shards that are marked as actively allocated to this node but don't actually exist on the node.
     * Resends shard failures for shards that are still marked as allocated to this node but previously failed.
     *
     * @param state new cluster state
     *              之前处理失败的某些分片   检测此时是否还需要维护
     */
    private void updateFailedShardsCache(final ClusterState state) {
        // 获取本节点下所有分片的路由信息
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        // 如果此时本节点已经不再维护任何分片信息 那么清空容器
        if (localRoutingNode == null) {
            failedShardsCache.clear();
            return;
        }

        DiscoveryNode masterNode = state.nodes().getMasterNode();

        // remove items from cache which are not in our routing table anymore and resend failures that have not executed on master yet
        for (Iterator<Map.Entry<ShardId, ShardRouting>> iterator = failedShardsCache.entrySet().iterator(); iterator.hasNext(); ) {
            ShardRouting failedShardRouting = iterator.next().getValue();

            // 获取对应shardId 在本节点上维护的最新路由信息
            ShardRouting matchedRouting = localRoutingNode.getByShardId(failedShardRouting.shardId());
            // 找不到分片 或者分片第二次被分配到当前节点 (allocationId已经变化) 那么忽略之前失败的信息
            if (matchedRouting == null || matchedRouting.isSameAllocation(failedShardRouting) == false) {
                iterator.remove();
            } else {
                // 代表leader节点本次CS更新没有解决掉该失败分片的问题   将失败信息通知给leader节点
                if (masterNode != null) { // TODO: can we remove this? Is resending shard failures the responsibility of shardStateAction?
                    String message = "master " + masterNode + " has not removed previously failed shard. resending shard failure";
                    logger.trace("[{}] re-sending failed shard [{}], reason [{}]", matchedRouting.shardId(), matchedRouting, message);
                    shardStateAction.localShardFailed(matchedRouting, message, null, SHARD_STATE_ACTION_LISTENER, state);
                }
            }
        }
    }

    /**
     * 更新某个分片的全局检查点
     * 具体操作就是将 syncedCheckpoint 同步到 lastKnowCheckpoint  同时对数据做刷盘操作
     * @param shardId
     */
    protected void updateGlobalCheckpointForShard(final ShardId shardId) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.markAsSystemContext();
            client.executeLocally(GlobalCheckpointSyncAction.TYPE, new GlobalCheckpointSyncAction.Request(shardId),
                ActionListener.wrap(r -> {
                }, e -> {
                    if (ExceptionsHelper.unwrap(e, AlreadyClosedException.class, IndexShardClosedException.class) == null) {
                        getLogger().info(new ParameterizedMessage("{} global checkpoint sync failed", shardId), e);
                    }
                }));
        }
    }

    // overrideable by tests
    Logger getLogger() {
        return logger;
    }

    /**
     * Deletes indices (with shard data).
     *
     * @param event cluster change event
     *              比较集群状态 将不需要继续维护的索引信息移除
     */
    private void deleteIndices(final ClusterChangedEvent event) {
        final ClusterState previousState = event.previousState();
        final ClusterState state = event.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;

        for (Index index : event.indicesDeleted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] cleaning index, no longer part of the metadata", index);
            }

            // 获取某个分配到本node的索引
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(index);
            final IndexSettings indexSettings;

            // 如果存在该信息 就进行移除 (本节点不需要再维护该index信息)
            if (indexService != null) {
                indexSettings = indexService.getIndexSettings();
                indicesService.removeIndex(index, DELETED, "index no longer part of the metadata");
                // 代表index之前分配在该节点上  尝试删除一些残留数据
            } else if (previousState.metadata().hasIndex(index.getName())) {
                // The deleted index was part of the previous cluster state, but not loaded on the local node
                final IndexMetadata metadata = previousState.metadata().index(index);
                indexSettings = new IndexSettings(metadata, settings);
                indicesService.deleteUnassignedIndex("deleted index was not assigned to local node", metadata, state);
            } else {
                // The previous cluster state's metadata also does not contain the index,
                // which is what happens on node startup when an index was deleted while the
                // node was not part of the cluster.  In this case, try reading the index
                // metadata from disk.  If its not there, there is nothing to delete.
                // First, though, verify the precondition for applying this case by
                // asserting that the previous cluster state is not initialized/recovered.
                // TODO 这种情况是怎么发生的 ???
                assert previousState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
                final IndexMetadata metadata = indicesService.verifyIndexIsDeleted(index, event.state());
                if (metadata != null) {
                    indexSettings = new IndexSettings(metadata, settings);
                } else {
                    indexSettings = null;
                }
            }

            // 有关该索引之前的配置信息
            if (indexSettings != null) {
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(() -> new ParameterizedMessage("[{}] failed to complete pending deletion for index", index), e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        try {
                            // we are waiting until we can lock the index / all shards on the node and then we ack the delete of the store
                            // to the master. If we can't acquire the locks here immediately there might be a shard of this index still
                            // holding on to the lock due to a "currently canceled recovery" or so. The shard will delete itself BEFORE the
                            // lock is released so it's guaranteed to be deleted by the time we get the lock
                            // 清除残留数据
                            indicesService.processPendingDeletes(index, indexSettings, new TimeValue(30, TimeUnit.MINUTES));
                        } catch (ShardLockObtainFailedException exc) {
                            logger.warn("[{}] failed to lock all shards for index - timed out after 30 seconds", index);
                        } catch (InterruptedException e) {
                            logger.warn("[{}] failed to lock all shards for index - interrupted", index);
                        }
                    }
                });
            }
        }
    }

    /**
     * Removes indices that have no shards allocated to this node or indices whose state has changed. This does not delete the shard data
     * as we wait for enough shard copies to exist in the cluster before deleting shard data (triggered by
     * {@link org.elasticsearch.indices.store.IndicesStore}).
     *
     * @param event the cluster changed event
     *              对应index还分配在该节点  但是状态可能被修改成关闭
     */
    private void removeIndices(final ClusterChangedEvent event) {
        final ClusterState state = event.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;

        // 将此时节点下存在的所有分片 按照index存储
        final Set<Index> indicesWithShards = new HashSet<>();
        RoutingNode localRoutingNode = state.getRoutingNodes().node(localNodeId);
        if (localRoutingNode != null) { // null e.g. if we are not a data node
            for (ShardRouting shardRouting : localRoutingNode) {
                indicesWithShards.add(shardRouting.index());
            }
        }

        // 遍历下面的 indexService
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            final Index index = indexService.index();
            final IndexMetadata indexMetadata = state.metadata().index(index);
            final IndexMetadata existingMetadata = indexService.getIndexSettings().getIndexMetadata();

            AllocatedIndices.IndexRemovalReason reason = null;
            // 代表该索引的元数据信息发生了变化   索引可能被关闭 或者被重新开启
            if (indexMetadata != null && indexMetadata.getState() != existingMetadata.getState()) {
                reason = indexMetadata.getState() == IndexMetadata.State.CLOSE ? CLOSED : REOPENED;
            } else if (indicesWithShards.contains(index) == false) {
                // if the cluster change indicates a brand new cluster, we only want
                // to remove the in-memory structures for the index and not delete the
                // contents on disk because the index will later be re-imported as a
                // dangling index
                assert indexMetadata != null || event.isNewCluster() :
                    "index " + index + " does not exist in the cluster state, it should either " +
                        "have been deleted or the cluster must be new";
                reason = indexMetadata != null && indexMetadata.getState() == IndexMetadata.State.CLOSE ? CLOSED : NO_LONGER_ASSIGNED;
            }

            if (reason != null) {
                logger.debug("{} removing index ({})", index, reason);
                indicesService.removeIndex(index, reason, "removing index (" + reason + ")");
            }
        }
    }

    /**
     * Notifies master about shards that don't exist but are supposed to be active on this node.
     *
     * @param state new cluster state
     */
    private void failMissingShards(final ClusterState state) {
        // 获取这个节点上所有分片信息
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }
        for (final ShardRouting shardRouting : localRoutingNode) {
            ShardId shardId = shardRouting.shardId();
            // 非init状态 却无法在failedShards 和 indicesService上找到
            if (shardRouting.initializing() == false &&
                failedShardsCache.containsKey(shardId) == false &&
                indicesService.getShardOrNull(shardId) == null) {
                // the master thinks we are active, but we don't have this shard at all, mark it as failed
                sendFailShard(shardRouting, "master marked shard as active, but shard has not been created, mark shard as failed", null,
                    state);
            }
        }
    }

    /**
     * Removes shards that are currently loaded by indicesService but have disappeared from the routing table of the current node.
     * This method does not delete the shard data.
     *
     * @param state new cluster state
     *              某些分片可能新增或者从该节点移除
     */
    private void removeShards(final ClusterState state) {
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;

        // remove shards based on routing nodes (no deletion of data)
        RoutingNode localRoutingNode = state.getRoutingNodes().node(localNodeId);

        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            for (Shard shard : indexService) {

                // 检查索引级的每个分片是否还存在于本节点上 如果不存在 则移除
                ShardRouting currentRoutingEntry = shard.routingEntry();
                ShardId shardId = currentRoutingEntry.shardId();
                ShardRouting newShardRouting = localRoutingNode == null ? null : localRoutingNode.getByShardId(shardId);

                // 代表这个分片此时不应该存在于该节点  进行移除
                if (newShardRouting == null) {
                    // we can just remove the shard without cleaning it locally, since we will clean it in IndicesStore
                    // once all shards are allocated
                    logger.debug("{} removing shard (not allocated)", shardId);
                    indexService.removeShard(shardId.id(), "removing shard (not allocated)");
                    // 代表该分片是重新分配的 将旧的分片移除
                } else if (newShardRouting.isSameAllocation(currentRoutingEntry) == false) {
                    logger.debug("{} removing shard (stale allocation id, stale {}, new {})", shardId,
                        currentRoutingEntry, newShardRouting);
                    indexService.removeShard(shardId.id(), "removing shard (stale copy)");
                    // 该分片处于 init状态  但是在当前节点确实活跃状态  代表这个分片是重新加入到集群的 也先移除旧的分片
                } else if (newShardRouting.initializing() && currentRoutingEntry.active()) {
                    // this can happen if the node was isolated/gc-ed, rejoins the cluster and a new shard with the same allocation id
                    // is assigned to it. Batch cluster state processing or if shard fetching completes before the node gets a new cluster
                    // state may result in a new shard being initialized while having the same allocation id as the currently started shard.
                    logger.debug("{} removing shard (not active, current {}, new {})", shardId, currentRoutingEntry, newShardRouting);
                    indexService.removeShard(shardId.id(), "removing shard (stale copy)");
                    // 当前节点的分片晋升成主分片
                } else if (newShardRouting.primary() && currentRoutingEntry.primary() == false && newShardRouting.initializing()) {
                    assert currentRoutingEntry.initializing() : currentRoutingEntry; // see above if clause
                    // this can happen when cluster state batching batches activation of the shard, closing an index, reopening it
                    // and assigning an initializing primary to this node
                    logger.debug("{} removing shard (not active, current {}, new {})", shardId, currentRoutingEntry, newShardRouting);
                    indexService.removeShard(shardId.id(), "removing shard (stale copy)");
                }
            }
        }
    }

    /**
     * 创建新索引
     * @param state
     */
    private void createIndices(final ClusterState state) {
        // we only create indices for shards that are allocated
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }
        // create map of indices to create with shards to fail if index creation fails
        final Map<Index, List<ShardRouting>> indicesToCreate = new HashMap<>();
        for (ShardRouting shardRouting : localRoutingNode) {
            // 确保这个分片本身没有被记录在失败容器中
            if (failedShardsCache.containsKey(shardRouting.shardId()) == false) {
                final Index index = shardRouting.index();
                if (indicesService.indexService(index) == null) {
                    indicesToCreate.computeIfAbsent(index, k -> new ArrayList<>()).add(shardRouting);
                }
            }
        }

        // 创建索引信息
        for (Map.Entry<Index, List<ShardRouting>> entry : indicesToCreate.entrySet()) {
            final Index index = entry.getKey();
            final IndexMetadata indexMetadata = state.metadata().index(index);
            logger.debug("[{}] creating index", index);

            AllocatedIndex<? extends Shard> indexService = null;
            try {
                // 创建indexService 同时触发监听器
                indexService = indicesService.createIndex(indexMetadata, buildInIndexListener, true);
                // 更新mapping信息 同时通知到leader节点
                if (indexService.updateMapping(null, indexMetadata) && sendRefreshMapping) {
                    nodeMappingRefreshAction.nodeMappingRefresh(state.nodes().getMasterNode(),
                        new NodeMappingRefreshAction.NodeMappingRefreshRequest(indexMetadata.getIndex().getName(),
                            indexMetadata.getIndexUUID(), state.nodes().getLocalNodeId())
                    );
                }
            } catch (Exception e) {
                final String failShardReason;
                if (indexService == null) {
                    failShardReason = "failed to create index";
                } else {
                    failShardReason = "failed to update mapping for index";
                    indicesService.removeIndex(index, FAILURE, "removing index (mapping update failed)");
                }
                for (ShardRouting shardRouting : entry.getValue()) {
                    sendFailShard(shardRouting, failShardReason, e, state);
                }
            }
        }
    }

    /**
     * 更新索引信息
     * @param event
     */
    private void updateIndices(ClusterChangedEvent event) {
        // 如果元数据本身没有变化 直接返回
        if (!event.metadataChanged()) {
            return;
        }
        final ClusterState state = event.state();
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            final Index index = indexService.index();
            final IndexMetadata currentIndexMetadata = indexService.getIndexSettings().getIndexMetadata();
            final IndexMetadata newIndexMetadata = state.metadata().index(index);
            assert newIndexMetadata != null : "index " + index + " should have been removed by deleteIndices";
            // 检测元数据是否发生变化
            if (ClusterChangedEvent.indexMetadataChanged(currentIndexMetadata, newIndexMetadata)) {
                String reason = null;
                try {
                    reason = "metadata update failed";
                    try {
                        // 更新元数据信息
                        indexService.updateMetadata(currentIndexMetadata, newIndexMetadata);
                    } catch (Exception e) {
                        assert false : e;
                        throw e;
                    }

                    reason = "mapping update failed";
                    // 使用最新的元数据去更新 mapping   当更新成功后默认要通知leader节点
                    if (indexService.updateMapping(currentIndexMetadata, newIndexMetadata) && sendRefreshMapping) {
                        nodeMappingRefreshAction.nodeMappingRefresh(state.nodes().getMasterNode(),
                            new NodeMappingRefreshAction.NodeMappingRefreshRequest(newIndexMetadata.getIndex().getName(),
                                newIndexMetadata.getIndexUUID(), state.nodes().getLocalNodeId())
                        );
                    }
                } catch (Exception e) {
                    indicesService.removeIndex(indexService.index(), FAILURE, "removing index (" + reason + ")");

                    // fail shards that would be created or updated by createOrUpdateShards
                    RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
                    if (localRoutingNode != null) {
                        for (final ShardRouting shardRouting : localRoutingNode) {
                            if (shardRouting.index().equals(index) && failedShardsCache.containsKey(shardRouting.shardId()) == false) {
                                sendFailShard(shardRouting, "failed to update index (" + reason + ")", e, state);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 创建新分片/更新分片
     * @param state
     */
    private void createOrUpdateShards(final ClusterState state) {
        // 每个index对应一个模板 被称为mapping  然后数据又被细化成shard
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }

        DiscoveryNodes nodes = state.nodes();
        RoutingTable routingTable = state.routingTable();

        // 这里需要找到在整个集群中 只分配到本节点上的所有路由信息   该shardRouting 可能是primary 也可能是replica
        for (final ShardRouting shardRouting : localRoutingNode) {
            ShardId shardId = shardRouting.shardId();
            if (failedShardsCache.containsKey(shardId) == false) {
                AllocatedIndex<? extends Shard> indexService = indicesService.indexService(shardId.getIndex());
                assert indexService != null : "index " + shardId.getIndex() + " should have been created by createIndices";
                Shard shard = indexService.getShardOrNull(shardId.id());
                if (shard == null) {
                    assert shardRouting.initializing() : shardRouting + " should have been removed by failMissingShards";
                    createShard(nodes, routingTable, shardRouting, state);
                } else {
                    updateShard(nodes, shardRouting, shard, routingTable, state);
                }
            }
        }
    }

    /**
     * 创建新分片
     * @param nodes  当前集群内所有节点
     * @param routingTable  全局路由表
     * @param shardRouting  某个分片的路由信息
     * @param state
     */
    private void createShard(DiscoveryNodes nodes, RoutingTable routingTable, ShardRouting shardRouting, ClusterState state) {
        assert shardRouting.initializing() : "only allow shard creation for initializing shard but was " + shardRouting;

        DiscoveryNode sourceNode = null;
        // 如果分片从其他节点获取数据
        if (shardRouting.recoverySource().getType() == Type.PEER)  {
            // 寻找primary分片所在的节点
            sourceNode = findSourceNodeForPeerRecovery(logger, routingTable, nodes, shardRouting);
            // 找不到恢复源就不创建分片了吗
            if (sourceNode == null) {
                logger.trace("ignoring initializing shard {} - no source node can be found.", shardRouting.shardId());
                return;
            }
        }

        try {
            final long primaryTerm = state.metadata().index(shardRouting.index()).primaryTerm(shardRouting.id());
            logger.debug("{} creating shard with primary term [{}]", shardRouting.shardId(), primaryTerm);
            // 该对象描述了恢复源的状态信息
            RecoveryState recoveryState = new RecoveryState(shardRouting, nodes.getLocalNode(), sourceNode);
            // 创建分片
            indicesService.createShard(
                    shardRouting,
                    recoveryState,
                    recoveryTargetService,
                    new RecoveryListener(shardRouting, primaryTerm),
                    repositoriesService,
                    failedShardHandler, // 当某个shard处理失败时 使用该handler进行处理
                    this::updateGlobalCheckpointForShard,
                    retentionLeaseSyncer);
        } catch (Exception e) {
            failAndRemoveShard(shardRouting, true, "failed to create shard", e, state);
        }
    }

    /**
     * 更新分片信息
     * @param nodes
     * @param shardRouting  此时从CS中获取的最新分片信息
     * @param shard  本地分片信息
     * @param routingTable
     * @param clusterState
     */
    private void updateShard(DiscoveryNodes nodes, ShardRouting shardRouting, Shard shard, RoutingTable routingTable,
                             ClusterState clusterState) {
        final ShardRouting currentRoutingEntry = shard.routingEntry();
        assert currentRoutingEntry.isSameAllocation(shardRouting) :
            "local shard has a different allocation id but wasn't cleaned by removeShards. "
                + "cluster state: " + shardRouting + " local: " + currentRoutingEntry;

        final long primaryTerm;
        try {
            final IndexMetadata indexMetadata = clusterState.metadata().index(shard.shardId().getIndex());
            primaryTerm = indexMetadata.primaryTerm(shard.shardId().id());
            // 获取处于同步队列中的所有nodeId
            final Set<String> inSyncIds = indexMetadata.inSyncAllocationIds(shard.shardId().id());
            final IndexShardRoutingTable indexShardRoutingTable = routingTable.shardRoutingTable(shardRouting.shardId());

            // 使用相关信息  更新分片状态
            shard.updateShardState(shardRouting, primaryTerm, primaryReplicaSyncer::resync, clusterState.version(),
                inSyncIds, indexShardRoutingTable);
        } catch (Exception e) {
            failAndRemoveShard(shardRouting, true, "failed updating shard routing entry", e, clusterState);
            return;
        }

        // 代表分片此时
        final IndexShardState state = shard.state();
        // 代表状态从启动变回了 init
        if (shardRouting.initializing() && (state == IndexShardState.STARTED || state == IndexShardState.POST_RECOVERY)) {
            // the master thinks we are initializing, but we are already started or on POST_RECOVERY and waiting
            // for master to confirm a shard started message (either master failover, or a cluster event before
            // we managed to tell the master we started), mark us as started
            if (logger.isTraceEnabled()) {
                logger.trace("{} master marked shard as initializing, but shard has state [{}], resending shard started to {}",
                    shardRouting.shardId(), state, nodes.getMasterNode());
            }
            if (nodes.getMasterNode() != null) {
                // 原本以为分片还处于 init状态  但是在该节点上分片已经处于启动状态了 要通知leader 在createShard中 如果数据恢复完成 应该也会通知
                shardStateAction.shardStarted(shardRouting, primaryTerm, "master " + nodes.getMasterNode() +
                        " marked shard as initializing, but shard state is [" + state + "], mark shard as started",
                    SHARD_STATE_ACTION_LISTENER, clusterState);
            }
        }
    }

    /**
     * Finds the routing source node for peer recovery, return null if its not found. Note, this method expects the shard
     * routing to *require* peer recovery, use {@link ShardRouting#recoverySource()} to check if its needed or not.
     * 在生成某个分片时 如果 recoverySource是 Peer 就要寻找 primary分片所在的节点
     */
    private static DiscoveryNode findSourceNodeForPeerRecovery(Logger logger, RoutingTable routingTable, DiscoveryNodes nodes,
                                                               ShardRouting shardRouting) {
        DiscoveryNode sourceNode = null;
        // 当本节点对应的分片不是主分片时
        if (!shardRouting.primary()) {
            // 找到主分片所在的node  如果主分片此时处于非活跃状态 返回null
            ShardRouting primary = routingTable.shardRoutingTable(shardRouting.shardId()).primaryShard();
            // only recover from started primary, if we can't find one, we will do it next round
            if (primary.active()) {
                sourceNode = nodes.get(primary.currentNodeId());
                if (sourceNode == null) {
                    logger.trace("can't find replica source node because primary shard {} is assigned to an unknown node.", primary);
                }
            } else {
                logger.trace("can't find replica source node because primary shard {} is not active.", primary);
            }
            // 代表当前分片是主分片  并且是由于重分配跑到这个节点的 那么是可以从原节点拷贝数据的
        } else if (shardRouting.relocatingNodeId() != null) {
            sourceNode = nodes.get(shardRouting.relocatingNodeId());
            if (sourceNode == null) {
                logger.trace("can't find relocation source node for shard {} because it is assigned to an unknown node [{}].",
                    shardRouting.shardId(), shardRouting.relocatingNodeId());
            }
        } else {
            // 其余情况不合法
            throw new IllegalStateException("trying to find source node for peer recovery when routing state means no peer recovery: " +
                shardRouting);
        }
        return sourceNode;
    }

    /**
     * 监听shard的数据恢复情况  在完成时触发相关函数
     */
    private class RecoveryListener implements PeerRecoveryTargetService.RecoveryListener {

        /**
         * ShardRouting with which the shard was created
         */
        private final ShardRouting shardRouting;

        /**
         * Primary term with which the shard was created
         */
        private final long primaryTerm;

        private RecoveryListener(final ShardRouting shardRouting, final long primaryTerm) {
            this.shardRouting = shardRouting;
            this.primaryTerm = primaryTerm;
        }

        @Override
        public void onRecoveryDone(final RecoveryState state) {
            // 当分片数据恢复完成后 发送一个该分片可以已启动的通知到leader
            shardStateAction.shardStarted(shardRouting, primaryTerm, "after " + state.getRecoverySource(), SHARD_STATE_ACTION_LISTENER);
        }

        /**
         * 恢复失败 将分片添加到失败容器中  在下一次接收CS状态后 会检测分片是否被重新分配 如果没有则会通知leader节点
         * @param state
         * @param e
         * @param sendShardFailure
         */
        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
            handleRecoveryFailure(shardRouting, sendShardFailure, e);
        }
    }

    // package-private for testing
    synchronized void handleRecoveryFailure(ShardRouting shardRouting, boolean sendShardFailure, Exception failure) {
        failAndRemoveShard(shardRouting, sendShardFailure, "failed recovery", failure, clusterService.state());
    }

    /**
     * 将某个分片从 indexService中移除
     * @param shardRouting
     * @param sendShardFailure  代表当该分片处理失败时 是否需要通知leader
     * @param message
     * @param failure
     * @param state
     */
    private void failAndRemoveShard(ShardRouting shardRouting, boolean sendShardFailure, String message, @Nullable Exception failure,
                                    ClusterState state) {
        try {
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(shardRouting.shardId().getIndex());
            if (indexService != null) {
                Shard shard = indexService.getShardOrNull(shardRouting.shardId().id());
                if (shard != null && shard.routingEntry().isSameAllocation(shardRouting)) {
                    indexService.removeShard(shardRouting.shardId().id(), message);
                }
            }
        } catch (ShardNotFoundException e) {
            // the node got closed on us, ignore it
        } catch (Exception inner) {
            inner.addSuppressed(failure);
            logger.warn(() -> new ParameterizedMessage(
                    "[{}][{}] failed to remove shard after failure ([{}])",
                    shardRouting.getIndexName(),
                    shardRouting.getId(),
                    message),
                inner);
        }
        if (sendShardFailure) {
            sendFailShard(shardRouting, message, failure, state);
        }
    }

    /**
     * 将某个分片的失败信息发送给leader节点
     * @param shardRouting
     * @param message
     * @param failure
     * @param state
     */
    private void sendFailShard(ShardRouting shardRouting, String message, @Nullable Exception failure, ClusterState state) {
        try {
            logger.warn(() -> new ParameterizedMessage(
                    "{} marking and sending shard failed due to [{}]", shardRouting.shardId(), message), failure);
            // 只要在下次CS更新中 leader没有处理该shard 还会继续发送请求
            failedShardsCache.put(shardRouting.shardId(), shardRouting);
            shardStateAction.localShardFailed(shardRouting, message, failure, SHARD_STATE_ACTION_LISTENER, state);
        } catch (Exception inner) {
            if (failure != null) inner.addSuppressed(failure);
            logger.warn(() -> new ParameterizedMessage(
                    "[{}][{}] failed to mark shard as failed (because of [{}])",
                    shardRouting.getIndexName(),
                    shardRouting.getId(),
                    message),
                inner);
        }
    }

    /**
     * 当某个分片处理失败时 通过该函数进行处理
     */
    private class FailedShardHandler implements Consumer<IndexShard.ShardFailure> {
        @Override
        public void accept(final IndexShard.ShardFailure shardFailure) {
            final ShardRouting shardRouting = shardFailure.routing;
            threadPool.generic().execute(() -> {
                synchronized (IndicesClusterStateService.this) {
                    failAndRemoveShard(shardRouting, true, "shard failure, reason [" + shardFailure.reason + "]", shardFailure.cause,
                        clusterService.state());
                }
            });
        }
    }

    /**
     * 分片接口是定义在这里的
     */
    public interface Shard {

        /**
         * Returns the shard id of this shard.
         */
        ShardId shardId();

        /**
         * Returns the latest cluster routing entry received with this shard.
         */
        ShardRouting routingEntry();

        /**
         * Returns the latest internal shard state.
         */
        IndexShardState state();

        /**
         * Returns the recovery state associated with this shard.
         */
        RecoveryState recoveryState();

        /**
         * Updates the shard state based on an incoming cluster state:
         * - Updates and persists the new routing value.
         * - Updates the primary term if this shard is a primary.
         * - Updates the allocation ids that are tracked by the shard if it is a primary.
         *   See {@link ReplicationTracker#updateFromMaster(long, Set, IndexShardRoutingTable)} for details.
         *
         * @param shardRouting                the new routing entry
         * @param primaryTerm                 the new primary term
         * @param primaryReplicaSyncer        the primary-replica resync action to trigger when a term is increased on a primary
         * @param applyingClusterStateVersion the cluster state version being applied when updating the allocation IDs from the master
         * @param inSyncAllocationIds         the allocation ids of the currently in-sync shard copies
         * @param routingTable                the shard routing table
         * @throws IndexShardRelocatedException if shard is marked as relocated and relocation aborted
         * @throws IOException                  if shard state could not be persisted
         */
        void updateShardState(ShardRouting shardRouting,
                              long primaryTerm,
                              BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer,
                              long applyingClusterStateVersion,
                              Set<String> inSyncAllocationIds,
                              IndexShardRoutingTable routingTable) throws IOException;
    }

    /**
     * 代表分配到当前节点的索引  该index内部可能有多个shardId的分片被同时分配到当前节点
     * @param <T>
     */
    public interface AllocatedIndex<T extends Shard> extends Iterable<T>, IndexComponent {

        /**
         * Returns the index settings of this index.
         * 获取该索引相关的索引配置
         */
        IndexSettings getIndexSettings();

        // 当元数据发生变化时 会影响到这个已经分配好的索引

        /**
         * Updates the metadata of this index. Changes become visible through {@link #getIndexSettings()}.
         *
         * @param currentIndexMetadata the current index metadata
         * @param newIndexMetadata the new index metadata
         */
        void updateMetadata(IndexMetadata currentIndexMetadata, IndexMetadata newIndexMetadata);

        /**
         * Checks if index requires refresh from master.
         */
        boolean updateMapping(IndexMetadata currentIndexMetadata, IndexMetadata newIndexMetadata) throws IOException;

        /**
         * Returns shard with given id.
         * 获取该索引下某个分片
         */
        @Nullable T getShardOrNull(int shardId);

        /**
         * Removes shard with given id.
         * 移除某个分片
         */
        void removeShard(int shardId, String message);
    }

    /**
     * 记录此时所有已经分配好的index  实际上就是 indicesService
     * @param <T>
     * @param <U>
     */
    public interface AllocatedIndices<T extends Shard, U extends AllocatedIndex<T>> extends Iterable<U> {

        /**
         * Creates a new {@link IndexService} for the given metadata.
         *
         * @param indexMetadata          the index metadata to create the index for
         * @param builtInIndexListener   a list of built-in lifecycle {@link IndexEventListener} that should should be used along side with
         *                               the per-index listeners    构建完成后会触发这组监听器
         * @param writeDanglingIndices   whether dangling indices information should be written  使用允许写入 dangling的索引
         * @throws ResourceAlreadyExistsException if the index already exists.
         * 根据某个索引元数据创建一个对应的 indexService
         */
        U createIndex(IndexMetadata indexMetadata,
                      List<IndexEventListener> builtInIndexListener,
                      boolean writeDanglingIndices) throws IOException;

        /**
         * Verify that the contents on disk for the given index is deleted; if not, delete the contents.
         * This method assumes that an index is already deleted in the cluster state and/or explicitly
         * through index tombstones.
         * @param index {@code Index} to make sure its deleted from disk
         * @param clusterState {@code ClusterState} to ensure the index is not part of it
         * @return IndexMetadata for the index loaded from disk
         */
        IndexMetadata verifyIndexIsDeleted(Index index, ClusterState clusterState);


        /**
         * Deletes an index that is not assigned to this node. This method cleans up all disk folders relating to the index
         * but does not deal with in-memory structures. For those call {@link #removeIndex(Index, IndexRemovalReason, String)}
         */
        void deleteUnassignedIndex(String reason, IndexMetadata metadata, ClusterState clusterState);

        /**
         * Removes the given index from this service and releases all associated resources. Persistent parts of the index
         * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
         * @param index the index to remove
         * @param reason the reason to remove the index
         * @param extraInfo extra information that will be used for logging and reporting
         */
        void removeIndex(Index index, IndexRemovalReason reason, String extraInfo);

        /**
         * Returns an IndexService for the specified index if exists otherwise returns <code>null</code>.
         */
        @Nullable U indexService(Index index);

        /**
         * Creates a shard for the specified shard routing and starts recovery.
         *
         * @param shardRouting           the shard routing
         * @param recoveryState          the recovery state
         * @param recoveryTargetService  recovery service for the target
         * @param recoveryListener       a callback when recovery changes state (finishes or fails)
         * @param repositoriesService    service responsible for snapshot/restore
         * @param onShardFailure         a callback when this shard fails
         * @param globalCheckpointSyncer a callback when this shard syncs the global checkpoint
         * @param retentionLeaseSyncer   a callback when this shard syncs retention leases
         * @return a new shard
         * @throws IOException if an I/O exception occurs when creating the shard
         */
        T createShard(
                ShardRouting shardRouting,
                RecoveryState recoveryState,
                PeerRecoveryTargetService recoveryTargetService,
                PeerRecoveryTargetService.RecoveryListener recoveryListener,
                RepositoriesService repositoriesService,
                Consumer<IndexShard.ShardFailure> onShardFailure,
                Consumer<ShardId> globalCheckpointSyncer,
                RetentionLeaseSyncer retentionLeaseSyncer) throws IOException;

        /**
         * Returns shard for the specified id if it exists otherwise returns <code>null</code>.
         * 每个节点上包含一个 IndexService  内部包含了本节点维护的所有index 以及本节点在该index上维护的所有shard 而且同一shard 仅会存在一份数据 (一个replicate 或者一个primary)
         */
        default T getShardOrNull(ShardId shardId) {
            U indexRef = indexService(shardId.getIndex());
            if (indexRef != null) {
                return indexRef.getShardOrNull(shardId.id());
            }
            return null;
        }

        void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeValue)
            throws IOException, InterruptedException, ShardLockObtainFailedException;

        enum IndexRemovalReason {
            /**
             * Shard of this index were previously assigned to this node but all shards have been relocated.
             * The index should be removed and all associated resources released. Persistent parts of the index
             * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
             */
            NO_LONGER_ASSIGNED,
            /**
             * The index is deleted. Persistent parts of the index  like the shards files, state and transaction logs are removed once
             * all resources are released.
             */
            DELETED,

            /**
             * The index has been closed. The index should be removed and all associated resources released. Persistent parts of the index
             * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
             */
            CLOSED,

            /**
             * Something around index management has failed and the index should be removed.
             * Persistent parts of the index like the shards files, state and transaction logs are kept around in the
             * case of a disaster recovery.
             */
            FAILURE,

            /**
             * The index has been reopened. The index should be removed and all associated resources released. Persistent parts of the index
             * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
             */
            REOPENED,
        }
    }
}
