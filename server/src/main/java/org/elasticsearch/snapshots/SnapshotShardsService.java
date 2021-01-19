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

package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus.Stage;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestDeduplicator;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.cluster.SnapshotsInProgress.completed;

/**
 * This service runs on data and master nodes and controls currently snapshotted shards on these nodes. It is responsible for
 * starting and stopping shard level snapshots
 * 在发起快照任务后 当检测满足执行快照任务的基础条件后 会将快照任务修改成started状态
 * 分片快照任务实际上由该类来执行
 */
public class SnapshotShardsService extends AbstractLifecycleComponent implements ClusterStateListener, IndexEventListener {
    private static final Logger logger = LogManager.getLogger(SnapshotShardsService.class);

    private static final String UPDATE_SNAPSHOT_STATUS_ACTION_NAME = "internal:cluster/snapshot/update_snapshot_status";

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final RepositoriesService repositoriesService;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private final Map<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> shardSnapshots = new HashMap<>();

    // A map of snapshots to the shardIds that we already reported to the master as failed
    private final TransportRequestDeduplicator<UpdateIndexShardSnapshotStatusRequest> remoteFailedRequestDeduplicator =
        new TransportRequestDeduplicator<>();

    /**
     * 该对象定义了如何更新CS
     */
    private final SnapshotStateExecutor snapshotStateExecutor = new SnapshotStateExecutor();

    /**
     * 该对象内部包含了如何更新快照状态
     */
    private final UpdateSnapshotStatusAction updateSnapshotStatusHandler;

    /**
     *
     * @param settings
     * @param clusterService
     * @param repositoriesService
     * @param threadPool
     * @param transportService
     * @param indicesService
     * @param actionFilters  该对象内部存储了一组 ActionFilter
     * @param indexNameExpressionResolver
     */
    public SnapshotShardsService(Settings settings, ClusterService clusterService, RepositoriesService repositoriesService,
                                 ThreadPool threadPool, TransportService transportService, IndicesService indicesService,
                                 ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indicesService = indicesService;
        this.repositoriesService = repositoriesService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // 当本节点是数据节点时 会监听ClusterState变化
        if (DiscoveryNode.isDataNode(settings)) {
            // this is only useful on the nodes that can hold data
            clusterService.addListener(this);
        }

        // The constructor of UpdateSnapshotStatusAction will register itself to the TransportService.
        // 将更新快照状态的请求注册到 请求处理器上
        this.updateSnapshotStatusHandler =
            new UpdateSnapshotStatusAction(transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver);
    }

    @Override
    protected void doStart() {
        assert this.updateSnapshotStatusHandler != null;
        assert transportService.getRequestHandler(UPDATE_SNAPSHOT_STATUS_ACTION_NAME) != null;
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
        clusterService.removeListener(this);
    }


    /**
     * 当本节点作为数据节点 感知到集群发生变化时触发
     * @param event
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            SnapshotsInProgress previousSnapshots = event.previousState().custom(SnapshotsInProgress.TYPE);
            SnapshotsInProgress currentSnapshots = event.state().custom(SnapshotsInProgress.TYPE);
            // 当描述所有快照任务的 inProgress 发生了变化 需要执行快照操作
            if ((previousSnapshots == null && currentSnapshots != null)
                || (previousSnapshots != null && previousSnapshots.equals(currentSnapshots) == false)) {
                synchronized (shardSnapshots) {
                    // 当leader发生变化时 新leader可能没有同步到快照任务 并发布了clusterState
                    // 这些快照任务就需要被终止 这里将他们的状态修改成aborted
                    cancelRemoved(currentSnapshots);
                    if (currentSnapshots != null) {
                        // 检测是否有新建的快照任务 并执行  如果发现被终止的任务 则进行关闭
                        startNewSnapshots(currentSnapshots);
                    }
                }
            }

            // 当leader发生变化时 有2种情况  第一种 快照任务被同步过去了  那么本节点需要将快照结果通知到新的leader上 实际上新leader本地数据与repositoryData是对不上的
            // 但是这样可以触发快照任务的移除
            // 第二种情况 快照任务本身没有同步过去 那么即使上报 新leader节点也不会处理这个信息 因为对应的entry已经不在了 并且因为快照任务的丢失 也就不需要发起移除快照的clusterState更新任务了
            String previousMasterNodeId = event.previousState().nodes().getMasterNodeId();
            String currentMasterNodeId = event.state().nodes().getMasterNodeId();
            if (currentMasterNodeId != null && currentMasterNodeId.equals(previousMasterNodeId) == false) {
                syncShardStatsOnNewMaster(event);
            }

        } catch (Exception e) {
            logger.warn("Failed to update snapshot state ", e);
        }
    }

    /**
     * 当某个分片在关闭前触发
     * @param shardId
     * @param indexShard The index shard
     * @param indexSettings
     */
    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        // abort any snapshots occurring on the soon-to-be closed shard
        synchronized (shardSnapshots) {
            for (Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> snapshotShards : shardSnapshots.entrySet()) {
                Map<ShardId, IndexShardSnapshotStatus> shards = snapshotShards.getValue();
                if (shards.containsKey(shardId)) {
                    logger.debug("[{}] shard closing, abort snapshotting for snapshot [{}]",
                        shardId, snapshotShards.getKey().getSnapshotId());
                    // 将快照标记成禁止状态
                    shards.get(shardId).abortIfNotCompleted("shard is closing, aborting");
                }
            }
        }
    }

    /**
     * Returns status of shards that are snapshotted on the node and belong to the given snapshot
     * <p>
     * This method is executed on data node
     * </p>
     *
     * @param snapshot  snapshot
     * @return map of shard id to snapshot status
     * 简单的map操作
     */
    public Map<ShardId, IndexShardSnapshotStatus> currentSnapshotShards(Snapshot snapshot) {
        synchronized (shardSnapshots) {
            final Map<ShardId, IndexShardSnapshotStatus> current = shardSnapshots.get(snapshot);
            return current == null ? null : new HashMap<>(current);
        }
    }

    /**
     * 当leader发生变化时  这里是考虑到ES的选举可能会出现clusterState被覆盖的情况  如果某些快照任务没有发布到新的leader上
     * 其他节点选择终止之前的快照任务
     * @param snapshotsInProgress
     */
    private void cancelRemoved(@Nullable SnapshotsInProgress snapshotsInProgress) {
        // First, remove snapshots that are no longer there
        // 实际上本地留存map 就是一种变相的续约机制  通过对比集群中发布的clusterState 与本地缓存的数据 可以知道之前是否开启了快照任务 并且可以关闭任务
        // 如果是采用类似raft的线性一致性实现的 那么只要获取更新前后的clusterState 就可以知道哪些任务被移除 就不需要借助这个map了
        Iterator<Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>>> it = shardSnapshots.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> entry = it.next();
            final Snapshot snapshot = entry.getKey();
            // 在新的clusterState中没有之前的快照任务信息 就可以强制关闭本次快照任务
            if (snapshotsInProgress == null || snapshotsInProgress.snapshot(snapshot) == null) {
                // abort any running snapshots of shards for the removed entry;
                // this could happen if for some reason the cluster state update for aborting
                // running shards is missed, then the snapshot is removed is a subsequent cluster
                // state update, which is being processed here
                it.remove();
                for (IndexShardSnapshotStatus snapshotStatus : entry.getValue().values()) {
                    // 将本次快照涉及到的所有索引级任务关闭  本地快照任务在执行是会检测是否被修改成 abort
                    snapshotStatus.abortIfNotCompleted("snapshot has been removed in cluster state, aborting");
                }
            }
        }
    }

    /**
     * 发现新增的快照任务 并开始执行
     * @param snapshotsInProgress
     */
    private void startNewSnapshots(SnapshotsInProgress snapshotsInProgress) {
        // For now we will be mostly dealing with a single snapshot at a time but might have multiple simultaneously running
        // snapshots in the future
        // Now go through all snapshots and update existing or create missing
        final String localNodeId = clusterService.localNode().getId();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            final State entryState = entry.state();

            // 发起快照任务后 并确保任务可以正常执行时 会将其修改成start  并通过snapshotsService发布到集群中 之后该服务就可以感知到 并进行处理
            if (entryState == State.STARTED) {
                // 本轮中开启了有关多少个分片的快照任务
                Map<ShardId, IndexShardSnapshotStatus> startedShards = null;
                final Snapshot snapshot = entry.snapshot();
                // 本地缓存一份任务 就可以做很多控制 可以感知到多了哪个shard的任务 或者是否第一次执行该任务
                Map<ShardId, IndexShardSnapshotStatus> snapshotShards = shardSnapshots.getOrDefault(snapshot, emptyMap());

                // 检测此时最新的快照任务与之前相比是否发生了变化
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shard : entry.shards()) {
                    // Add all new shards to start processing on
                    final ShardId shardId = shard.key;
                    final ShardSnapshotStatus shardSnapshotStatus = shard.value;
                    // 检测本节点是否为本shard.primary 所在的节点
                    if (localNodeId.equals(shardSnapshotStatus.nodeId())
                        // 当分片任务处于未启动状态才能处理
                        && shardSnapshotStatus.state() == ShardState.INIT
                        // 如果本地开始执行了 就会存储到map中
                        && snapshotShards.containsKey(shardId) == false) {
                        logger.trace("[{}] - Adding shard to the queue", shardId);
                        if (startedShards == null) {
                            startedShards = new HashMap<>();
                        }
                        // 生成一个处于初始节点的status对象 并填充到容器中
                        startedShards.put(shardId, IndexShardSnapshotStatus.newInitializing(shardSnapshotStatus.generation()));
                    }
                }

                // 代表本次开启了一个新的快照任务
                if (startedShards != null && startedShards.isEmpty() == false) {
                    shardSnapshots.computeIfAbsent(snapshot, s -> new HashMap<>()).putAll(startedShards);
                    // 这里执行新的快照任务
                    startNewShards(entry, startedShards);
                }

                // 由于发起了快照删除任务 并且此时快照任务还在执行过程中 检测到任务被中断后 会将所有shard级别的快照任务标记成终止
            } else if (entryState == State.ABORTED) {
                // Abort all running shards for this snapshot
                final Snapshot snapshot = entry.snapshot();
                Map<ShardId, IndexShardSnapshotStatus> snapshotShards = shardSnapshots.getOrDefault(snapshot, emptyMap());
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shard : entry.shards()) {
                    final IndexShardSnapshotStatus snapshotStatus = snapshotShards.get(shard.key);
                    if (snapshotStatus == null) {
                        // due to CS batching we might have missed the INIT state and straight went into ABORTED
                        // notify master that abort has completed by moving to FAILED
                        // 处于预期外情况 认为本分片的快照任务失败
                        if (shard.value.state() == ShardState.ABORTED) {
                            notifyFailedSnapshotShard(snapshot, shard.key, shard.value.reason());
                        }
                    } else {
                        // 将此时存在的status 设置成abort
                        snapshotStatus.abortIfNotCompleted("snapshot has been aborted");
                    }
                }
            }
        }
    }

    /**
     * 需要针对某些分片 开启快照任务
     * @param entry  该快照任务描述了本次针对哪些index/shard
     * @param startedShards  本节点下哪些分片 以及快照运行状态
     */
    private void startNewShards(SnapshotsInProgress.Entry entry, Map<ShardId, IndexShardSnapshotStatus> startedShards) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            final Snapshot snapshot = entry.snapshot();

            // 拆解成index级别
            final Map<String, IndexId> indicesMap =
                entry.indices().stream().collect(Collectors.toMap(IndexId::getName, Function.identity()));
            for (final Map.Entry<ShardId, IndexShardSnapshotStatus> shardEntry : startedShards.entrySet()) {
                final ShardId shardId = shardEntry.getKey();
                final IndexShardSnapshotStatus snapshotStatus = shardEntry.getValue();
                // 本次分片对应的索引
                final IndexId indexId = indicesMap.get(shardId.getIndexName());
                assert indexId != null;
                assert SnapshotsService.useShardGenerations(entry.version()) || snapshotStatus.generation() == null :
                    "Found non-null shard generation [" + snapshotStatus.generation() + "] for snapshot with old-format compatibility";

                // 执行快照任务   实际上就是commit此时存储在lucene中的数据 并根据这份索引文件生成一份副本存储在repository中
                snapshot(shardId, snapshot, indexId, entry.userMetadata(), snapshotStatus, entry.version(),
                    // 每当针对shard的快照操作完成后 会生成一个新的随机数作为shardGen 并将本次快照以及以往快照的元数据以该shardGen命名并存储
                    new ActionListener<>() {
                        @Override
                        public void onResponse(String newGeneration) {
                            assert newGeneration != null;
                            assert newGeneration.equals(snapshotStatus.generation());
                            if (logger.isDebugEnabled()) {
                                final IndexShardSnapshotStatus.Copy lastSnapshotStatus = snapshotStatus.asCopy();
                                logger.debug("snapshot [{}] completed to [{}] with [{}] at generation [{}]",
                                    snapshot, snapshot.getRepository(), lastSnapshotStatus, snapshotStatus.generation());
                            }
                            // 将本次快照结果上报给leader节点
                            notifySuccessfulSnapshotShard(snapshot, shardId, newGeneration);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            final String failure = ExceptionsHelper.stackTrace(e);
                            snapshotStatus.moveToFailed(threadPool.absoluteTimeInMillis(), failure);
                            logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to snapshot shard", shardId, snapshot), e);
                            // 将本次快照结果上报给leader节点
                            notifyFailedSnapshotShard(snapshot, shardId, failure);
                        }
                    });
            }
        });
    }

    /**
     * Creates shard snapshot
     *
     * @param snapshot       snapshot
     * @param snapshotStatus snapshot status
     *                       开始执行快照任务了  需要借助 repository
     */
    private void snapshot(final ShardId shardId, final Snapshot snapshot, final IndexId indexId, final Map<String, Object> userMetadata,
                          final IndexShardSnapshotStatus snapshotStatus, Version version, ActionListener<String> listener) {
        try {
            // indicesService 内部管理了每个索引 以及他们下面的所有分片
            final IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).getShardOrNull(shardId.id());
            // 只有主分片才有生成快照的必要
            if (indexShard.routingEntry().primary() == false) {
                throw new IndexShardSnapshotFailedException(shardId, "snapshot should be performed only on primary");
            }
            // 如果正处于重定位的状态 无法生成快照  实际上在创建快照任务时 应该已经检测过了
            if (indexShard.routingEntry().relocating()) {
                // do not snapshot when in the process of relocation of primaries so we won't get conflicts
                throw new IndexShardSnapshotFailedException(shardId, "cannot snapshot while relocating");
            }

            final IndexShardState indexShardState = indexShard.state();
            // 实际上对应 indexShard.init 状态  init和relocating 都不应该执行快照任务
            if (indexShardState == IndexShardState.CREATED || indexShardState == IndexShardState.RECOVERING) {
                // shard has just been created, or still recovering
                throw new IndexShardSnapshotFailedException(shardId, "shard didn't fully recover yet");
            }

            // 借助仓库执行快照任务
            final Repository repository = repositoriesService.repository(snapshot.getRepository());
            Engine.IndexCommitRef snapshotRef = null;
            try {
                // we flush first to make sure we get the latest writes snapshotted
                // 每次索引文件的提交都会生成一个commit对象 但是此时不一定包含了全部的数据  除非发生了merge
                // flushFirst == true 会触发事务日志的刷盘 以及 lucene数据的刷盘
                // lucene本身是支持 commit 与 writer并行执行的  通过将 perThread从pool中移除 实现隔离 这也体现了高性能
                snapshotRef = indexShard.acquireLastIndexCommit(true);
                final IndexCommit snapshotIndexCommit = snapshotRef.getIndexCommit();

                // 这个commit不一定包含全部数据呀  这个快照是否有意义 还是说es做了封装能够确保每次 commit都包含了所有数据 ???
                repository.snapshotShard(indexShard.store(), indexShard.mapperService(), snapshot.getSnapshotId(), indexId,
                    snapshotRef.getIndexCommit(), getShardStateId(indexShard, snapshotIndexCommit), snapshotStatus, version, userMetadata,
                    ActionListener.runBefore(listener, snapshotRef::close));
            } catch (Exception e) {
                IOUtils.close(snapshotRef);
                throw e;
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Generates an identifier from the current state of a shard that can be used to detect whether a shard's contents
     * have changed between two snapshots.
     * A shard is assumed to have unchanged contents if its global- and local checkpoint are equal, its maximum
     * sequence number has not changed and its history- and force-merge-uuid have not changed.
     * The method returns {@code null} if global and local checkpoint are different for a shard since no safe unique
     * shard state id can be used in this case because of the possibility of a primary failover leading to different
     * shard content for the same sequence number on a subsequent snapshot.
     *
     * @param indexShard          Shard
     * @param snapshotIndexCommit IndexCommit for shard
     * @return shard state id or {@code null} if none can be used
     * 生成一个唯一性的字符串     可以判断同一个shard在2次快照中是否发生了变化
     */
    @Nullable
    private static String getShardStateId(IndexShard indexShard, IndexCommit snapshotIndexCommit) throws IOException {
        final Map<String, String> userCommitData = snapshotIndexCommit.getUserData();
        // 获取此时shard此时的localCheckpoint 和 maxSeq  实际上这2个值 应该是等价的
        final SequenceNumbers.CommitInfo seqNumInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userCommitData.entrySet());
        final long maxSeqNo = seqNumInfo.maxSeqNo;
        // TODO 要求maxSeq == getLastSyncedGlobalCheckpoint  就代表所有副本同时进行持久化 并将自己最新的localCheckpoint上报给primary
        if (maxSeqNo != seqNumInfo.localCheckpoint || maxSeqNo != indexShard.getLastSyncedGlobalCheckpoint()) {
            return null;
        }
        // TODO 这里利用了 historyUUID 和 forceMergeUUID ???
        return userCommitData.get(Engine.HISTORY_UUID_KEY) + "-" +
            userCommitData.getOrDefault(Engine.FORCE_MERGE_UUID_KEY, "na") + "-" + maxSeqNo;
    }

    /**
     * Checks if any shards were processed that the new master doesn't know about
     * 当leader节点发生变化时 需要自动的将当前节点的分片快照状态上报给leader  这也说明了ES的clusterState更新会出现丢失 所以每个节点还需要具备续约机制
     * 如果是raft算法应该是实现了线性一致性 任何masterNode的数据都是完全同步的 就没有从dataNode发送数据去同步leader的必要了
     */
    private void syncShardStatsOnNewMaster(ClusterChangedEvent event) {
        // 如果任务本身不存在 代表新上线的leader节点 并没有同步到快照数据 或者原本就没有快照任务 如果leader节点本身都已经丢失了快照数据的情况 就不需要上报快照状态了
        SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
        if (snapshotsInProgress == null) {
            return;
        }

        // Clear request deduplicator since we need to send all requests that were potentially not handled by the previous
        // master again
        // 因为leader节点发生了变化 所以之前发往leader的请求就不需要处理了
        remoteFailedRequestDeduplicator.clear();
        for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {

            // 只有这2种类型有处理的必要
            if (snapshot.state() == State.STARTED || snapshot.state() == State.ABORTED) {
                // 找到本次快照所有的分片
                Map<ShardId, IndexShardSnapshotStatus> localShards = currentSnapshotShards(snapshot.snapshot());
                if (localShards != null) {
                    ImmutableOpenMap<ShardId, ShardSnapshotStatus> masterShards = snapshot.shards();
                    for(Map.Entry<ShardId, IndexShardSnapshotStatus> localShard : localShards.entrySet()) {
                        ShardId shardId = localShard.getKey();
                        ShardSnapshotStatus masterShard = masterShards.get(shardId);
                        // 找到 leader节点上 还未完成的快照状态
                        if (masterShard != null && masterShard.state().completed() == false) {
                            final IndexShardSnapshotStatus.Copy indexShardSnapshotStatus = localShard.getValue().asCopy();
                            final Stage stage = indexShardSnapshotStatus.getStage();
                            // Master knows about the shard and thinks it has not completed
                            // 将本地的状态上报给新leader
                            if (stage == Stage.DONE) {
                                // but we think the shard is done - we need to make new master know that the shard is done
                                logger.debug("[{}] new master thinks the shard [{}] is not completed but the shard is done locally, " +
                                    "updating status on the master", snapshot.snapshot(), shardId);
                                notifySuccessfulSnapshotShard(snapshot.snapshot(), shardId, localShard.getValue().generation());

                            } else if (stage == Stage.FAILURE) {
                                // but we think the shard failed - we need to make new master know that the shard failed
                                logger.debug("[{}] new master thinks the shard [{}] is not completed but the shard failed locally, " +
                                    "updating status on master", snapshot.snapshot(), shardId);
                                notifyFailedSnapshotShard(snapshot.snapshot(), shardId, indexShardSnapshotStatus.getFailure());
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Internal request that is used to send changes in snapshot status to master
     * 这个一个更新集群状态的请求
     */
    public static class UpdateIndexShardSnapshotStatusRequest extends MasterNodeRequest<UpdateIndexShardSnapshotStatusRequest> {
        private final Snapshot snapshot;
        private final ShardId shardId;
        /**
         * 将leader上的快照状态更新成该值
         */
        private final ShardSnapshotStatus status;

        public UpdateIndexShardSnapshotStatusRequest(StreamInput in) throws IOException {
            super(in);
            snapshot = new Snapshot(in);
            shardId = new ShardId(in);
            status = new ShardSnapshotStatus(in);
        }

        public UpdateIndexShardSnapshotStatusRequest(Snapshot snapshot, ShardId shardId, ShardSnapshotStatus status) {
            this.snapshot = snapshot;
            this.shardId = shardId;
            this.status = status;
            // By default, we keep trying to post snapshot status messages to avoid snapshot processes getting stuck.
            this.masterNodeTimeout = TimeValue.timeValueNanos(Long.MAX_VALUE);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            snapshot.writeTo(out);
            shardId.writeTo(out);
            status.writeTo(out);
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        public ShardId shardId() {
            return shardId;
        }

        public ShardSnapshotStatus status() {
            return status;
        }

        @Override
        public String toString() {
            return snapshot + ", shardId [" + shardId + "], status [" + status.state() + "]";
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final UpdateIndexShardSnapshotStatusRequest that = (UpdateIndexShardSnapshotStatusRequest) o;
            return snapshot.equals(that.snapshot) && shardId.equals(that.shardId) && status.equals(that.status);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshot, shardId, status);
        }
    }

    /**
     * Notify the master node that the given shard has been successfully snapshotted
     * 本次快照任务成功执行 需要通知给leader节点
     */
    private void notifySuccessfulSnapshotShard(final Snapshot snapshot, final ShardId shardId, String generation) {
        assert generation != null;
        sendSnapshotShardUpdate(snapshot, shardId,
            new ShardSnapshotStatus(clusterService.localNode().getId(), ShardState.SUCCESS, generation));
    }

    /**
     * Notify the master node that the given shard failed to be snapshotted
     * @param snapshot 本次失败的快照信息
     * @param shardId  具体是哪个分片失败
     * @param failure  失败原因
     * 主节点通知某个快照失败了
     */
    private void notifyFailedSnapshotShard(final Snapshot snapshot, final ShardId shardId, final String failure) {
        sendSnapshotShardUpdate(snapshot, shardId,
            new ShardSnapshotStatus(clusterService.localNode().getId(), ShardState.FAILED, failure, null));
    }

    /**
     * Updates the shard snapshot status by sending a {@link UpdateIndexShardSnapshotStatusRequest} to the master node
     * 将本节点上某个分片的快照结果上报给leader
     */
    private void sendSnapshotShardUpdate(final Snapshot snapshot, final ShardId shardId, final ShardSnapshotStatus status) {
        // 这里发送更新请求
        remoteFailedRequestDeduplicator.executeOnce(
            new UpdateIndexShardSnapshotStatusRequest(snapshot, shardId, status),
            // 当任务完成时触发监听器  只是打印日志
            new ActionListener<>() {
                @Override
                public void onResponse(Void aVoid) {
                    logger.trace("[{}] [{}] updated snapshot state", snapshot, status);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage("[{}] [{}] failed to update snapshot state", snapshot, status), e);
                }
            },
            // 先向自己发送一个 updateSnapshotStatus请求 之后会转发到leader上
            (req, reqListener) -> transportService.sendRequest(transportService.getLocalNode(), UPDATE_SNAPSHOT_STATUS_ACTION_NAME, req,

                // 当更新快照任务结束后 触发监听器  实际上假设在整个快照流程中 leader都没有修改 因为一旦修改 之前的快照任务就要整个取消
                new TransportResponseHandler<UpdateIndexShardSnapshotStatusResponse>() {
                    @Override
                    public UpdateIndexShardSnapshotStatusResponse read(StreamInput in) throws IOException {
                        return new UpdateIndexShardSnapshotStatusResponse(in);
                    }

                    @Override
                    public void handleResponse(UpdateIndexShardSnapshotStatusResponse response) {
                        reqListener.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        reqListener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                })
        );
    }

    /**
     * Updates the shard status on master node
     *
     * @param request update shard status request
     *                leader节点更新 snapshotStatus的方法
     */
    private void innerUpdateSnapshotState(final UpdateIndexShardSnapshotStatusRequest request,
                                          ActionListener<UpdateIndexShardSnapshotStatusResponse> listener) {
        logger.trace("received updated snapshot restore state [{}]", request);
        clusterService.submitStateUpdateTask(
            "update snapshot state",
            request,
            ClusterStateTaskConfig.build(Priority.NORMAL),
            snapshotStateExecutor,
            new ClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new UpdateIndexShardSnapshotStatusResponse());
                }
            });
    }

    /**
     * 该对象定义了如何更新快照状态
     */
    private static class SnapshotStateExecutor implements ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest> {

        @Override
        public ClusterTasksResult<UpdateIndexShardSnapshotStatusRequest> execute(ClusterState currentState, List<UpdateIndexShardSnapshotStatusRequest> tasks) {
            final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
            if (snapshots != null) {
                int changedCount = 0;
                final List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                // 找到匹配的快照任务 中匹配的shard数据 之后更新
                for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                    ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
                    boolean updated = false;

                    // 可以看到当leader已经丢失了某个快照任务 这里的更新分片状态会被忽略
                    for (UpdateIndexShardSnapshotStatusRequest updateSnapshotState : tasks) {
                        if (entry.snapshot().equals(updateSnapshotState.snapshot())) {
                            logger.trace("[{}] Updating shard [{}] with status [{}]", updateSnapshotState.snapshot(),
                                updateSnapshotState.shardId(), updateSnapshotState.status().state());
                            if (updated == false) {
                                shards.putAll(entry.shards());
                                updated = true;
                            }
                            // 这里是覆盖操作
                            shards.put(updateSnapshotState.shardId(), updateSnapshotState.status());
                            changedCount++;
                        }
                    }

                    // 代表有某个分片的快照状态发生了变化
                    if (updated) {
                        // 判断此时是否所有快照任务都已经完成
                        if (completed(shards.values()) == false) {
                            entries.add(new SnapshotsInProgress.Entry(entry, shards.build()));
                        } else {
                            // Snapshot is finished - mark it as done
                            // TODO: Add PARTIAL_SUCCESS status?
                            // 本次快照任务下所有分片都已经完成 更新快照任务本身
                            SnapshotsInProgress.Entry updatedEntry = new SnapshotsInProgress.Entry(entry, State.SUCCESS, shards.build());
                            entries.add(updatedEntry);
                        }
                    } else {
                        entries.add(entry);
                    }
                }
                if (changedCount > 0) {
                    logger.trace("changed cluster state triggered by {} snapshot state updates", changedCount);
                    return ClusterTasksResult.<UpdateIndexShardSnapshotStatusRequest>builder().successes(tasks)
                        .build(ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE,
                            new SnapshotsInProgress(unmodifiableList(entries))).build());
                }
            }
            return ClusterTasksResult.<UpdateIndexShardSnapshotStatusRequest>builder().successes(tasks).build(currentState);
        }
    }

    static class UpdateIndexShardSnapshotStatusResponse extends ActionResponse {

        UpdateIndexShardSnapshotStatusResponse() {}

        UpdateIndexShardSnapshotStatusResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    /**
     * 更新快照状态
     */
    private class UpdateSnapshotStatusAction
        extends TransportMasterNodeAction<UpdateIndexShardSnapshotStatusRequest, UpdateIndexShardSnapshotStatusResponse> {
        UpdateSnapshotStatusAction(TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(
                SnapshotShardsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME, false, transportService, clusterService, threadPool,
                actionFilters, UpdateIndexShardSnapshotStatusRequest::new, indexNameExpressionResolver
            );
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected UpdateIndexShardSnapshotStatusResponse read(StreamInput in) throws IOException {
            return new UpdateIndexShardSnapshotStatusResponse(in);
        }

        @Override
        protected void masterOperation(Task task, UpdateIndexShardSnapshotStatusRequest request, ClusterState state,
                                       ActionListener<UpdateIndexShardSnapshotStatusResponse> listener) {
            innerUpdateSnapshotState(request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(UpdateIndexShardSnapshotStatusRequest request, ClusterState state) {
            return null;
        }
    }

}
