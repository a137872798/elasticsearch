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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.cluster.SnapshotsInProgress.completed;

/**
 * Service responsible for creating snapshots. See package level documentation of {@link org.elasticsearch.snapshots}
 * for details.
 * 快照服务
 */
public class SnapshotsService extends AbstractLifecycleComponent implements ClusterStateApplier {

    public static final Version SHARD_GEN_IN_REPO_DATA_VERSION = Version.V_7_6_0;

    public static final Version OLD_SNAPSHOT_FORMAT = Version.V_7_5_0;

    public static final Version MULTI_DELETE_VERSION = Version.V_8_0_0;

    private static final Logger logger = LogManager.getLogger(SnapshotsService.class);

    /**
     * 集群服务 负责执行一些更新clusterState的任务 并将最新的集群状态发布给其它节点
     */
    private final ClusterService clusterService;

    /**
     * 这个解析器先不管吧 也就是在name和格式化字符串之间做映射 与ES本身的核心功能无关
     */
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * 存储服务
     */
    private final RepositoriesService repositoriesService;

    private final ThreadPool threadPool;

    private final Map<Snapshot, List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>>> snapshotCompletionListeners =
        new ConcurrentHashMap<>();

    // 这2个容器存储了不同时期的snapshot

    // Set of snapshots that are currently being initialized by this node
    private final Set<Snapshot> initializingSnapshots = Collections.synchronizedSet(new HashSet<>());

    // Set of snapshots that are currently being ended by this node
    private final Set<Snapshot> endingSnapshots = Collections.synchronizedSet(new HashSet<>());

    /**
     * 快照服务
     * @param settings
     * @param clusterService
     * @param indexNameExpressionResolver
     * @param repositoriesService
     * @param threadPool
     */
    public SnapshotsService(Settings settings, ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver,
                            RepositoriesService repositoriesService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;

        // 如果当前节点是一个参与选举的节点
        if (DiscoveryNode.isMasterNode(settings)) {
            // addLowPriorityApplier to make sure that Repository will be created before snapshot
            clusterService.addLowPriorityApplier(this);
        }
    }

    /**
     * Same as {@link #createSnapshot(CreateSnapshotRequest, ActionListener)} but invokes its callback on completion of
     * the snapshot.
     * 接收创建快照的请求 并执行一次快照操作
     *
     * @param request snapshot request
     * @param listener snapshot completion listener
     */
    public void executeSnapshot(final CreateSnapshotRequest request, final ActionListener<SnapshotInfo> listener) {
        createSnapshot(request,
            ActionListener.wrap(snapshot -> addListener(snapshot, ActionListener.map(listener, Tuple::v2)), listener::onFailure));
    }

    /**
     * Initializes the snapshotting process.
     * <p>
     * This method is used by clients to start snapshot. It makes sure that there is no snapshots are currently running and
     * creates a snapshot record in cluster state metadata.
     *
     * @param request  snapshot request   申请创建快照
     * @param listener snapshot creation listener   生成的快照结果会触发监听器
     */
    public void createSnapshot(final CreateSnapshotRequest request, final ActionListener<Snapshot> listener) {

        // 本次依靠于哪个存储实例创建快照
        final String repositoryName = request.repository();
        // 解析成快照名
        final String snapshotName = indexNameExpressionResolver.resolveDateMathExpression(request.snapshot());
        // 校验名称有效性
        validate(repositoryName, snapshotName);
        final SnapshotId snapshotId = new SnapshotId(snapshotName, UUIDs.randomBase64UUID()); // new UUID for the snapshot

        // 通过指定的存储实例名 寻找实例对象
        Repository repository = repositoriesService.repository(request.repository());
        // 存储实例 处理一些用户传入的元数据 并尝试修改成自身可以处理的数据  默认情况下就是返回原数据
        final Map<String, Object> userMeta = repository.adaptUserMetadata(request.userMetadata());

        // 更新clusterState 并通知到其他节点
        clusterService.submitStateUpdateTask("create_snapshot [" + snapshotName + ']', new ClusterStateUpdateTask() {

            // SnapshotsInProgress 负责管理所有的生成快照任务   而每个entry则代表一个快照任务
            private SnapshotsInProgress.Entry newSnapshot = null;

            private List<String> indices;

            @Override
            public ClusterState execute(ClusterState currentState) {
                validate(repositoryName, snapshotName, currentState);

                // 找到此时正在执行的所有 快照删除任务
                SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);
                // 当正在执行删除快照的任务时 无法生成快照
                if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(repositoryName, snapshotName,
                        "cannot snapshot while a snapshot deletion is in-progress in [" + deletionsInProgress + "]");
                }
                // 什么是 repositoryCleanup  它跟删除快照有什么联系
                final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(RepositoryCleanupInProgress.TYPE);
                if (repositoryCleanupInProgress != null && repositoryCleanupInProgress.hasCleanupInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(repositoryName, snapshotName,
                        "cannot snapshot while a repository cleanup is in-progress in [" + repositoryCleanupInProgress + "]");
                }
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                // Fail if there are any concurrently running snapshots. The only exception to this being a snapshot in INIT state from a
                // previous master that we can simply ignore and remove from the cluster state because we would clean it up from the
                // cluster state anyway in #applyClusterState.
                // 找到所有正在初始化阶段的快照任务 并且该entry内的快照处于 initializingSnapshots 阶段  那么无法执行快照任务 应该是代表此时正在执行某个快照任务吧
                if (snapshots != null && snapshots.entries().stream().anyMatch(entry ->
                    (entry.state() == State.INIT && initializingSnapshots.contains(entry.snapshot()) == false) == false)) {
                    throw new ConcurrentSnapshotExecutionException(repositoryName, snapshotName, " a snapshot is already running");
                }
                // Store newSnapshot here to be processed in clusterStateProcessed
                // 反正就是生成了一组索引名
                indices = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(currentState,
                    request.indicesOptions(), request.indices()));
                logger.trace("[{}][{}] creating snapshot for indices [{}]", repositoryName, snapshotName, indices);

                // 照理说不是可以存储一组快照么
                newSnapshot = new SnapshotsInProgress.Entry(
                    // 快照名称和存储名称是指定的   snapshotId 内部的uuid是随机生成的
                    new Snapshot(repositoryName, snapshotId),
                    // 是否包含globalState 和  partial 是什么意思???
                    request.includeGlobalState(), request.partial(),
                    // 新插入的集群状态是 init
                    State.INIT,
                    Collections.emptyList(), // We'll resolve the list of indices when moving to the STARTED state in #beginSnapshot
                    threadPool.absoluteTimeInMillis(),
                    RepositoryData.UNKNOWN_REPO_GEN,
                    null,
                    userMeta, Version.CURRENT
                );
                // 将初始化的快照存储到 initializingSnapshots中
                initializingSnapshots.add(newSnapshot.snapshot());
                snapshots = new SnapshotsInProgress(newSnapshot);
                // 将最新的快照发布到集群中
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to create snapshot", repositoryName, snapshotName), e);
                if (newSnapshot != null) {
                    initializingSnapshots.remove(newSnapshot.snapshot());
                }
                newSnapshot = null;
                listener.onFailure(e);
            }

            /**
             * 在es中 很多操作都要求尽可能发布到多的节点后才执行
             * @param source
             * @param oldState
             * @param newState
             */
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                if (newSnapshot != null) {
                    final Snapshot current = newSnapshot.snapshot();
                    assert initializingSnapshots.contains(current);
                    assert indices != null;
                    // 开始生成快照
                    beginSnapshot(newState, newSnapshot, request.partial(), indices, repository, new ActionListener<>() {
                        @Override
                        public void onResponse(final Snapshot snapshot) {
                            initializingSnapshots.remove(snapshot);
                            listener.onResponse(snapshot);
                        }

                        @Override
                        public void onFailure(final Exception e) {
                            initializingSnapshots.remove(current);
                            listener.onFailure(e);
                        }
                    });
                }
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }
        });
    }

    /**
     * Validates snapshot request
     *
     * @param repositoryName repository name
     * @param snapshotName snapshot name
     * @param state   current cluster state
     */
    private static void validate(String repositoryName, String snapshotName, ClusterState state) {
        RepositoriesMetadata repositoriesMetadata = state.getMetadata().custom(RepositoriesMetadata.TYPE);
        if (repositoriesMetadata == null || repositoriesMetadata.repository(repositoryName) == null) {
            throw new RepositoryMissingException(repositoryName);
        }
        validate(repositoryName, snapshotName);
    }

    private static void validate(final String repositoryName, final String snapshotName) {
        if (Strings.hasLength(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "cannot be empty");
        }
        if (snapshotName.contains(" ")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain whitespace");
        }
        if (snapshotName.contains(",")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain ','");
        }
        if (snapshotName.contains("#")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain '#'");
        }
        if (snapshotName.charAt(0) == '_') {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not start with '_'");
        }
        if (snapshotName.toLowerCase(Locale.ROOT).equals(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must be lowercase");
        }
        if (Strings.validFileName(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName,
                snapshotName,
                "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
    }

    /**
     * Starts snapshot.
     * <p>
     * Creates snapshot in repository and updates snapshot metadata record with list of shards that needs to be processed.
     *
     * @param clusterState               cluster state  此时最新的集群状态
     * @param snapshot                   snapshot meta data   本次生成快照时 相关的信息
     * @param partial                    allow partial snapshots   是否允许部分生成快照
     * @param repository                 本次生成快照使用的存储实例
     * @param userCreateSnapshotListener listener
     */
    private void beginSnapshot(final ClusterState clusterState,
                               final SnapshotsInProgress.Entry snapshot,
                               final boolean partial,
                               final List<String> indices,
                               final Repository repository,
                               final ActionListener<Snapshot> userCreateSnapshotListener) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {

            /**
             * 代表检测到 本次快照对应的entry已经被标记成Aborted了
             */
            boolean hadAbortedInitializations;

            @Override
            protected void doRun() {
                assert initializingSnapshots.contains(snapshot.snapshot());
                // 如果该存储对象是一个只读对象 那么无法生成快照
                if (repository.isReadOnly()) {
                    throw new RepositoryException(repository.getMetadata().name(), "cannot create snapshot in a readonly repository");
                }
                final String snapshotName = snapshot.snapshot().getSnapshotId().getName();
                final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
                // 获取此时存储实例对应的 repositoryData
                repository.getRepositoryData(repositoryDataListener);
                repositoryDataListener.whenComplete(repositoryData -> {
                    // check if the snapshot name already exists in the repository
                    // 先检查repositoryData 中是否已经出现过这个快照了  不能重复添加
                    if (repositoryData.getSnapshotIds().stream().anyMatch(s -> s.getName().equals(snapshotName))) {
                        throw new InvalidSnapshotNameException(
                            repository.getMetadata().name(), snapshotName, "snapshot with the same name already exists");
                    }

                    logger.info("snapshot [{}] started", snapshot.snapshot());
                    // 这里就是从所有快照中找到最小的版本
                    final Version version =
                        minCompatibleVersion(clusterState.nodes().getMinNodeVersion(), snapshot.repository(), repositoryData, null);
                    if (indices.isEmpty()) {
                        // No indices in this snapshot - we are done
                        // 该快照不需要包含任何索引 那么就可以直接触发了  在监听器中 会触发  initializingSnapshots.remove(snapshot);
                        userCreateSnapshotListener.onResponse(snapshot.snapshot());
                        // 这时又创建了一个新的快照对象去触发 endSnapshot
                        endSnapshot(new SnapshotsInProgress.Entry(
                            snapshot, State.STARTED, Collections.emptyList(), repositoryData.getGenId(), null, version,
                            null), clusterState.metadata());
                        return;
                    }

                    // 正常的处理流程在这里
                    clusterService.submitStateUpdateTask("update_snapshot [" + snapshot.snapshot() + "]", new ClusterStateUpdateTask() {

                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            // 获取正在处理中的快照
                            SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                            List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                            for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                                if (entry.snapshot().equals(snapshot.snapshot()) == false) {
                                    entries.add(entry);
                                    continue;
                                }

                                // 找到快照一致的entry
                                if (entry.state() == State.ABORTED) {
                                    entries.add(entry);
                                    assert entry.shards().isEmpty();
                                    hadAbortedInitializations = true;
                                } else {
                                    // 将index转换成indexId
                                    final List<IndexId> indexIds = repositoryData.resolveNewIndices(indices);
                                    // Replace the snapshot that was just initialized
                                    // 将分片的信息抽取出来设置到ShardSnapshotStatus
                                    ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards =
                                        shards(currentState, indexIds, useShardGenerations(version), repositoryData);
                                    // 非部分的情况
                                    if (!partial) {
                                        // v1 代表没有分片的索引  v2 代表已经被关闭的索引
                                        Tuple<Set<String>, Set<String>> indicesWithMissingShards = indicesWithMissingShards(shards,
                                            currentState.metadata());
                                        Set<String> missing = indicesWithMissingShards.v1();
                                        Set<String> closed = indicesWithMissingShards.v2();
                                        // 也就是说出现了miss 和close的本次处理就算是失败了
                                        if (missing.isEmpty() == false || closed.isEmpty() == false) {
                                            final StringBuilder failureMessage = new StringBuilder();
                                            if (missing.isEmpty() == false) {
                                                failureMessage.append("Indices don't have primary shards ");
                                                failureMessage.append(missing);
                                            }
                                            if (closed.isEmpty() == false) {
                                                if (failureMessage.length() > 0) {
                                                    failureMessage.append("; ");
                                                }
                                                failureMessage.append("Indices are closed ");
                                                failureMessage.append(closed);
                                            }
                                            // 代表本次生成快照失败了
                                            entries.add(new SnapshotsInProgress.Entry(entry, State.FAILED, indexIds,
                                                repositoryData.getGenId(), shards, version, failureMessage.toString()));
                                            continue;
                                        }
                                    }
                                    entries.add(new SnapshotsInProgress.Entry(entry, State.STARTED, indexIds, repositoryData.getGenId(),
                                        shards, version, null));
                                }
                            }
                            // 使用新的entries 覆盖之前的数据
                            return ClusterState.builder(currentState)
                                .putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(unmodifiableList(entries)))
                                .build();
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            logger.warn(() -> new ParameterizedMessage("[{}] failed to create snapshot",
                                snapshot.snapshot().getSnapshotId()), e);
                            removeSnapshotFromClusterState(snapshot.snapshot(), e,
                                new CleanupAfterErrorListener(userCreateSnapshotListener, e));
                        }

                        @Override
                        public void onNoLongerMaster(String source) {
                            // We are not longer a master - we shouldn't try to do any cleanup
                            // The new master will take care of it
                            logger.warn("[{}] failed to create snapshot - no longer a master", snapshot.snapshot().getSnapshotId());
                            userCreateSnapshotListener.onFailure(
                                new SnapshotException(snapshot.snapshot(), "master changed during snapshot initialization"));
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            // The userCreateSnapshotListener.onResponse() notifies caller that the snapshot was accepted
                            // for processing. If client wants to wait for the snapshot completion, it can register snapshot
                            // completion listener in this method. For the snapshot completion to work properly, the snapshot
                            // should still exist when listener is registered.
                            userCreateSnapshotListener.onResponse(snapshot.snapshot());

                            if (hadAbortedInitializations) {
                                final SnapshotsInProgress snapshotsInProgress = newState.custom(SnapshotsInProgress.TYPE);
                                assert snapshotsInProgress != null;
                                final SnapshotsInProgress.Entry entry = snapshotsInProgress.snapshot(snapshot.snapshot());
                                assert entry != null;
                                endSnapshot(entry, newState.metadata());
                            }
                        }
                    });
                }, this::onFailure);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to create snapshot [{}]",
                    snapshot.snapshot().getSnapshotId()), e);
                removeSnapshotFromClusterState(snapshot.snapshot(), e,
                    new CleanupAfterErrorListener(userCreateSnapshotListener, e));
            }
        });
    }

    private static class CleanupAfterErrorListener {

        private final ActionListener<Snapshot> userCreateSnapshotListener;
        private final Exception e;

        CleanupAfterErrorListener(ActionListener<Snapshot> userCreateSnapshotListener, Exception e) {
            this.userCreateSnapshotListener = userCreateSnapshotListener;
            this.e = e;
        }

        public void onFailure(@Nullable Exception e) {
            userCreateSnapshotListener.onFailure(ExceptionsHelper.useOrSuppress(e, this.e));
        }

        /**
         * 当本节点不再是leader时 无法继续处理
         */
        public void onNoLongerMaster() {
            userCreateSnapshotListener.onFailure(e);
        }
    }

    /**
     * 生成分片gen数据
     * @param snapshot
     * @param metadata
     * @return
     */
    private static ShardGenerations buildGenerations(SnapshotsInProgress.Entry snapshot, Metadata metadata) {
        ShardGenerations.Builder builder = ShardGenerations.builder();
        final Map<String, IndexId> indexLookup = new HashMap<>();
        // 将快照中所有的索引信息都插入到 map中
        snapshot.indices().forEach(idx -> indexLookup.put(idx.getName(), idx));
        snapshot.shards().forEach(c -> {
            // 把分片的id 和 gen存起来
            if (metadata.index(c.key.getIndex()) == null) {
                assert snapshot.partial() :
                    "Index [" + c.key.getIndex() + "] was deleted during a snapshot but snapshot was not partial.";
                return;
            }
            final IndexId indexId = indexLookup.get(c.key.getIndexName());
            if (indexId != null) {
                builder.put(indexId, c.key.id(), c.value.generation());
            }
        });
        return builder.build();
    }

    private static Metadata metadataForSnapshot(SnapshotsInProgress.Entry snapshot, Metadata metadata) {
        // 如果不包含全局state   这里只是将该快照下所有的索引对应的 IndexMetadata 包装成新的metadata
        if (snapshot.includeGlobalState() == false) {
            // Remove global state from the cluster state
            Metadata.Builder builder = Metadata.builder();
            for (IndexId index : snapshot.indices()) {
                final IndexMetadata indexMetadata = metadata.index(index.getName());
                if (indexMetadata == null) {
                    assert snapshot.partial() : "Index [" + index + "] was deleted during a snapshot but snapshot was not partial.";
                } else {
                    builder.put(indexMetadata, false);
                }
            }
            metadata = builder.build();
        }
        return metadata;
    }

    /**
     * Returns status of the currently running snapshots
     * <p>
     * This method is executed on master node
     * </p>
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repository          repository id
     * @param snapshots           list of snapshots that will be used as a filter, empty list means no snapshots are filtered
     * @return list of metadata for currently running snapshots
     * 获取当前快照
     */
    public static List<SnapshotsInProgress.Entry> currentSnapshots(@Nullable SnapshotsInProgress snapshotsInProgress, String repository,
                                                                   List<String> snapshots) {
        if (snapshotsInProgress == null || snapshotsInProgress.entries().isEmpty()) {
            return Collections.emptyList();
        }
        if ("_all".equals(repository)) {
            return snapshotsInProgress.entries();
        }
        // 代表此时只有一个正在执行的快照任务
        if (snapshotsInProgress.entries().size() == 1) {
            // Most likely scenario - one snapshot is currently running
            // Check this snapshot against the query
            // 获取快照实体
            SnapshotsInProgress.Entry entry = snapshotsInProgress.entries().get(0);
            // 如果快照使用的repository 与传入的不一致
            if (entry.snapshot().getRepository().equals(repository) == false) {
                return Collections.emptyList();
            }
            if (snapshots.isEmpty() == false) {
                for (String snapshot : snapshots) {
                    // 只要命中了任意一个快照就返回所有entries
                    if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                        return snapshotsInProgress.entries();
                    }
                }
                return Collections.emptyList();
            } else {
                // 返回所有快照entry
                return snapshotsInProgress.entries();
            }
        }
        // 这里将 inprogress.entry 中的快照 与 传入的 snapshots做匹配 将命中的返回
        List<SnapshotsInProgress.Entry> builder = new ArrayList<>();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.snapshot().getRepository().equals(repository) == false) {
                continue;
            }
            if (snapshots.isEmpty() == false) {
                for (String snapshot : snapshots) {
                    if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                        builder.add(entry);
                        break;
                    }
                }
            } else {
                builder.add(entry);
            }
        }
        return unmodifiableList(builder);
    }

    /**
     * 当集群状态发生变化时触发
     * @param event
     */
    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            // 只有当本节点成为leader时才进行处理
            if (event.localNodeMaster()) {
                // We don't remove old master when master flips anymore. So, we need to check for change in master
                // 找到最新的 clusterState中 描述执行的快照任务
                final SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
                final boolean newMaster = event.previousState().nodes().isLocalNodeElectedMaster() == false;
                if (snapshotsInProgress != null) {
                    // 如果是此次刚成为leader   或者某些节点被移除了
                    if (newMaster || removedNodesCleanupNeeded(snapshotsInProgress, event.nodesDelta().removedNodes())) {
                        // 从当前clusterState中移除掉已经无效的node 之后将最新的集群状态发布到集群中
                        processSnapshotsOnRemovedNodes();
                    }
                    // 如果本次快照相关的所有索引 刚好与本次变动有关 并且处于 started或者unassigned状态
                    if (event.routingTableChanged() && waitingShardsStartedOrUnassigned(snapshotsInProgress, event)) {
                        processStartedShards();
                    }
                    // Cleanup all snapshots that have no more work left:
                    // 1. Completed snapshots
                    // 2. Snapshots in state INIT that the previous master failed to start
                    // 3. Snapshots in any other state that have all their shard tasks completed
                    snapshotsInProgress.entries().stream().filter(
                        entry -> entry.state().completed()
                            || initializingSnapshots.contains(entry.snapshot()) == false
                            && (entry.state() == State.INIT || completed(entry.shards().values()))
                    ).forEach(entry -> endSnapshot(entry, event.state().metadata()));
                }
                if (newMaster) {
                    finalizeSnapshotDeletionFromPreviousMaster(event.state());
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to update snapshot state ", e);
        }
        assert assertConsistentWithClusterState(event.state());
    }

    private boolean assertConsistentWithClusterState(ClusterState state) {
        final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
        if (snapshotsInProgress != null && snapshotsInProgress.entries().isEmpty() == false) {
            final Set<Snapshot> runningSnapshots =
                snapshotsInProgress.entries().stream().map(SnapshotsInProgress.Entry::snapshot).collect(Collectors.toSet());
            final Set<Snapshot> snapshotListenerKeys = snapshotCompletionListeners.keySet();
            assert runningSnapshots.containsAll(snapshotListenerKeys) : "Saw completion listeners for unknown snapshots in "
                + snapshotListenerKeys + " but running snapshots are " + runningSnapshots;
        }
        return true;
    }

    /**
     * Finalizes a snapshot deletion in progress if the current node is the master but it
     * was not master in the previous cluster state and there is still a lingering snapshot
     * deletion in progress in the cluster state.  This means that the old master failed
     * before it could clean up an in-progress snapshot deletion.  We attempt to delete the
     * snapshot files and remove the deletion from the cluster state.  It is possible that the
     * old master was in a state of long GC and then it resumes and tries to delete the snapshot
     * that has already been deleted by the current master.  This is acceptable however, since
     * the old master's snapshot deletion will just respond with an error but in actuality, the
     * snapshot was deleted and a call to GET snapshots would reveal that the snapshot no longer exists.
     */
    private void finalizeSnapshotDeletionFromPreviousMaster(ClusterState state) {
        SnapshotDeletionsInProgress deletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
        if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
            assert deletionsInProgress.getEntries().size() == 1 : "only one in-progress deletion allowed per cluster";
            SnapshotDeletionsInProgress.Entry entry = deletionsInProgress.getEntries().get(0);
            deleteSnapshotsFromRepository(entry.repository(), entry.getSnapshots(), null, entry.repositoryStateId(),
                state.nodes().getMinNodeVersion());
        }
    }

    /**
     * Cleans up shard snapshots that were running on removed nodes
     * 当某些node被移除时 相关的快照需要被清理
     */
    private void processSnapshotsOnRemovedNodes() {
        clusterService.submitStateUpdateTask("update snapshot state after node removal", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                DiscoveryNodes nodes = currentState.nodes();
                // 如果此时没有正在处理的快照返回原CS
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots == null) {
                    return currentState;
                }
                boolean changed = false;
                ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                for (final SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                    SnapshotsInProgress.Entry updatedSnapshot = snapshot;
                    // 如果快照处于刚启动或者禁止状态
                    if (snapshot.state() == State.STARTED || snapshot.state() == State.ABORTED) {
                        ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
                        boolean snapshotChanged = false;
                        for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : snapshot.shards()) {
                            // 该快照下描述每个分片的状态
                            final ShardSnapshotStatus shardStatus = shardEntry.value;
                            final ShardId shardId = shardEntry.key;
                            // 只处理还未完成 且nodeId不为空的分片status
                            if (!shardStatus.state().completed() && shardStatus.nodeId() != null) {
                                // 代表这个分片对象的node 还存在于集群中
                                if (nodes.nodeExists(shardStatus.nodeId())) {
                                    shards.put(shardId, shardStatus);
                                } else {
                                    // TODO: Restart snapshot on another node?
                                    // 当该快照对应的node 已经从CS中移除时
                                    snapshotChanged = true;
                                    logger.warn("failing snapshot of shard [{}] on closed node [{}]",
                                        shardId, shardStatus.nodeId());
                                    shards.put(shardId,
                                        new ShardSnapshotStatus(shardStatus.nodeId(), ShardState.FAILED, "node shutdown",
                                            shardStatus.generation()));
                                }
                            } else {
                                // 完成的或者 未设置nodeId的也会被加入到shards中
                                shards.put(shardId, shardStatus);
                            }
                        }
                        // 确实发生了变化 需要重新生成
                        if (snapshotChanged) {
                            changed = true;
                            ImmutableOpenMap<ShardId, ShardSnapshotStatus> shardsMap = shards.build();
                            if (!snapshot.state().completed() && completed(shardsMap.values())) {
                                updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, State.SUCCESS, shardsMap);
                            } else {
                                updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, snapshot.state(), shardsMap);
                            }
                        }
                        entries.add(updatedSnapshot);
                    } else if (snapshot.state() == State.INIT && initializingSnapshots.contains(snapshot.snapshot()) == false) {
                        changed = true;
                        // A snapshot in INIT state hasn't yet written anything to the repository so we simply remove it
                        // from the cluster state  without any further cleanup
                    }
                    assert updatedSnapshot.shards().size() == snapshot.shards().size()
                        : "Shard count changed during snapshot status update from [" + snapshot + "] to [" + updatedSnapshot + "]";
                }
                if (changed) {
                    return ClusterState.builder(currentState)
                        .putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(unmodifiableList(entries))).build();
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to update snapshot state after node removal");
            }
        });
    }

    /**
     * 处理所有处于 started 的分片
     */
    private void processStartedShards() {
        clusterService.submitStateUpdateTask("update snapshot state after shards started", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                RoutingTable routingTable = currentState.routingTable();
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots != null) {
                    boolean changed = false;
                    ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                    for (final SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                        SnapshotsInProgress.Entry updatedSnapshot = snapshot;
                        // 当该快照处于started状态时
                        if (snapshot.state() == State.STARTED) {
                            // 更新分片状态
                            ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards = processWaitingShards(snapshot.shards(),
                                routingTable);

                            // 代表分片发生了变化
                            if (shards != null) {
                                changed = true;
                                if (!snapshot.state().completed() && completed(shards.values())) {
                                    updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, State.SUCCESS, shards);
                                } else {
                                    updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, shards);
                                }
                            }
                            entries.add(updatedSnapshot);
                        }
                    }
                    if (changed) {
                        return ClusterState.builder(currentState)
                            .putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(unmodifiableList(entries))).build();
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() ->
                    new ParameterizedMessage("failed to update snapshot state after shards started from [{}] ", source), e);
            }
        });
    }

    /**
     * 相当于遍历当前snapshotShards 下所有的snapshotStatus 并在路由表中找到当前分片的状态
     * 根据状态更新snapshotStatus 并重新生成ImmutableOpenMap<ShardId, ShardSnapshotStatus> 对象
     * @param snapshotShards
     * @param routingTable
     * @return
     */
    private static ImmutableOpenMap<ShardId, ShardSnapshotStatus> processWaitingShards(
        ImmutableOpenMap<ShardId, ShardSnapshotStatus> snapshotShards, RoutingTable routingTable) {
        boolean snapshotChanged = false;
        ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
        for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : snapshotShards) {
            // 获取某个分片当前的快照状态
            ShardSnapshotStatus shardStatus = shardEntry.value;
            ShardId shardId = shardEntry.key;
            // 如果当前分片处于等待状态
            if (shardStatus.state() == ShardState.WAITING) {
                IndexRoutingTable indexShardRoutingTable = routingTable.index(shardId.getIndex());
                if (indexShardRoutingTable != null) {
                    // 通过索引和分片定位到这个 IndexShardRoutingTable
                    IndexShardRoutingTable shardRouting = indexShardRoutingTable.shard(shardId.id());
                    // 上面都是在查找 state == waiting 的分片
                    if (shardRouting != null && shardRouting.primaryShard() != null) {
                        // 代表某个分片处于已启动状态
                        if (shardRouting.primaryShard().started()) {
                            // Shard that we were waiting for has started on a node, let's process it
                            snapshotChanged = true;
                            logger.trace("starting shard that we were waiting for [{}] on node [{}]", shardId, shardStatus.nodeId());
                            shards.put(shardId,
                                new ShardSnapshotStatus(shardRouting.primaryShard().currentNodeId(), shardStatus.generation()));
                            continue;
                        } else if (shardRouting.primaryShard().initializing() || shardRouting.primaryShard().relocating()) {
                            // Shard that we were waiting for hasn't started yet or still relocating - will continue to wait
                            shards.put(shardId, shardStatus);
                            continue;
                        }
                    }
                }
                // 进入到这里代表已经失败了
                // Shard that we were waiting for went into unassigned state or disappeared - giving up
                snapshotChanged = true;
                logger.warn("failing snapshot of shard [{}] on unassigned shard [{}]", shardId, shardStatus.nodeId());
                shards.put(shardId, new ShardSnapshotStatus(
                    shardStatus.nodeId(), ShardState.FAILED, "shard is unassigned", shardStatus.generation()));
            } else {
                shards.put(shardId, shardStatus);
            }
        }
        if (snapshotChanged) {
            return shards.build();
        } else {
            return null;
        }
    }

    /**
     * 检测在 entry.waitingIndices 中是否有任意一个参与本次变化的索引 处于started|unassigned
     * @param snapshotsInProgress
     * @param event
     * @return
     */
    private static boolean waitingShardsStartedOrUnassigned(SnapshotsInProgress snapshotsInProgress, ClusterChangedEvent event) {
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            // 首先确保当前快照处于started状态
            if (entry.state() == State.STARTED) {
                // 找到所有等待中的索引
                for (ObjectCursor<String> index : entry.waitingIndices().keys()) {
                    // 如果等待中的索引正好发生了变化
                    if (event.indexRoutingTableChanged(index.value)) {
                        IndexRoutingTable indexShardRoutingTable = event.state().getRoutingTable().index(index.value);
                        // 找到所有相关的分片
                        for (ShardId shardId : entry.waitingIndices().get(index.value)) {
                            // 找到该 shardId 对应所有分片 包含主副本
                            ShardRouting shardRouting = indexShardRoutingTable.shard(shardId.id()).primaryShard();
                            if (shardRouting != null && (shardRouting.started() || shardRouting.unassigned())) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * 代表这些快照对应的node 在本次CS更新时 被移除了
     * @param snapshotsInProgress
     * @param removedNodes
     * @return
     */
    private static boolean removedNodesCleanupNeeded(SnapshotsInProgress snapshotsInProgress, List<DiscoveryNode> removedNodes) {
        // If at least one shard was running on a removed node - we need to fail it
        return removedNodes.isEmpty() == false && snapshotsInProgress.entries().stream().flatMap(snapshot ->
            StreamSupport.stream(((Iterable<ShardSnapshotStatus>) () -> snapshot.shards().valuesIt()).spliterator(), false)
                .filter(s -> s.state().completed() == false).map(ShardSnapshotStatus::nodeId))
            .anyMatch(removedNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet())::contains);
    }

    /**
     * Returns list of indices with missing shards, and list of indices that are closed
     *
     * @param shards list of shard statuses
     * @return list of failed and closed indices
     * 返回没有分片的索引 以及被关闭的索引
     */
    private static Tuple<Set<String>, Set<String>> indicesWithMissingShards(
        ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards, Metadata metadata) {
        Set<String> missing = new HashSet<>();
        Set<String> closed = new HashSet<>();
        for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> entry : shards) {
            // 找到status是miss的
            if (entry.value.state() == ShardState.MISSING) {
                if (metadata.hasIndex(entry.key.getIndex().getName()) &&
                    // 如果发现元数据是关闭的 加入到close的容器中
                    metadata.getIndexSafe(entry.key.getIndex()).getState() == IndexMetadata.State.CLOSE) {
                    closed.add(entry.key.getIndex().getName());
                } else {
                    missing.add(entry.key.getIndex().getName());
                }
            }
        }
        return new Tuple<>(missing, closed);
    }

    /**
     * Finalizes the shard in repository and then removes it from cluster state
     * <p>
     * This is non-blocking method that runs on a thread from SNAPSHOT thread pool
     *
     * @param entry snapshot
     *              当某个快照生成完毕时触发
     */
    private void endSnapshot(SnapshotsInProgress.Entry entry, Metadata metadata) {
        if (endingSnapshots.add(entry.snapshot()) == false) {
            return;
        }
        final Snapshot snapshot = entry.snapshot();
        // 当完成时 stateId 不能为unknownGen
        if (entry.repositoryStateId() == RepositoryData.UNKNOWN_REPO_GEN) {
            logger.debug("[{}] was aborted before starting", snapshot);
            removeSnapshotFromClusterState(entry.snapshot(), new SnapshotException(snapshot, "Aborted on initialization"), null);
            return;
        }
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                final Repository repository = repositoriesService.repository(snapshot.getRepository());
                final String failure = entry.failure();
                logger.trace("[{}] finalizing snapshot in repository, state: [{}], failure[{}]", snapshot, entry.state(), failure);
                ArrayList<SnapshotShardFailure> shardFailures = new ArrayList<>();
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardStatus : entry.shards()) {
                    ShardId shardId = shardStatus.key;
                    ShardSnapshotStatus status = shardStatus.value;
                    final ShardState state = status.state();
                    if (state.failed()) {
                        shardFailures.add(new SnapshotShardFailure(status.nodeId(), shardId, status.reason()));
                    } else if (state.completed() == false) {
                        shardFailures.add(new SnapshotShardFailure(status.nodeId(), shardId, "skipped"));
                    } else {
                        assert state == ShardState.SUCCESS;
                    }
                }
                final ShardGenerations shardGenerations = buildGenerations(entry, metadata);
                repository.finalizeSnapshot(
                    snapshot.getSnapshotId(),
                    shardGenerations,
                    entry.startTime(),
                    failure,
                    entry.partial() ? shardGenerations.totalShards() : entry.shards().size(),
                    unmodifiableList(shardFailures),
                    entry.repositoryStateId(),
                    entry.includeGlobalState(),
                    metadataForSnapshot(entry, metadata),
                    entry.userMetadata(),
                    entry.version(),
                    state -> stateWithoutSnapshot(state, snapshot),
                    ActionListener.wrap(result -> {
                        final List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>> completionListeners =
                            snapshotCompletionListeners.remove(snapshot);
                        if (completionListeners != null) {
                            try {
                                ActionListener.onResponse(completionListeners, result);
                            } catch (Exception e) {
                                logger.warn("Failed to notify listeners", e);
                            }
                        }
                        endingSnapshots.remove(snapshot);
                        logger.info("snapshot [{}] completed with state [{}]", snapshot, result.v2().state());
                    }, this::onFailure));
            }

            @Override
            public void onFailure(final Exception e) {
                Snapshot snapshot = entry.snapshot();
                if (ExceptionsHelper.unwrap(e, NotMasterException.class, FailedToCommitClusterStateException.class) != null) {
                    // Failure due to not being master any more, don't try to remove snapshot from cluster state the next master
                    // will try ending this snapshot again
                    logger.debug(() -> new ParameterizedMessage(
                        "[{}] failed to update cluster state during snapshot finalization", snapshot), e);
                    failSnapshotCompletionListeners(snapshot,
                        new SnapshotException(snapshot, "Failed to update cluster state during snapshot finalization", e));
                } else {
                    logger.warn(() -> new ParameterizedMessage("[{}] failed to finalize snapshot", snapshot), e);
                    removeSnapshotFromClusterState(snapshot, e, null);
                }
            }
        });
    }

    /**
     * 从当前CS中 剔除掉指定的snapshot
     * @param state
     * @param snapshot
     * @return
     */
    private static ClusterState stateWithoutSnapshot(ClusterState state, Snapshot snapshot) {
        SnapshotsInProgress snapshots = state.custom(SnapshotsInProgress.TYPE);
        if (snapshots != null) {
            boolean changed = false;
            ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
            for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                if (entry.snapshot().equals(snapshot)) {
                    changed = true;
                } else {
                    entries.add(entry);
                }
            }
            if (changed) {
                return ClusterState.builder(state).putCustom(
                    SnapshotsInProgress.TYPE, new SnapshotsInProgress(unmodifiableList(entries))).build();
            }
        }
        return state;
    }

    /**
     * Removes record of running snapshot from cluster state and notifies the listener when this action is complete
     * @param snapshot   snapshot
     * @param failure    exception if snapshot failed
     * @param listener   listener to notify when snapshot information is removed from the cluster state
     *                   因为出现了异常情况 需要将某个快照从 clusterState中移除
     */
    private void removeSnapshotFromClusterState(final Snapshot snapshot, Exception failure,
                                                @Nullable CleanupAfterErrorListener listener) {
        assert failure != null : "Failure must be supplied";
        clusterService.submitStateUpdateTask("remove snapshot metadata", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) {
                return stateWithoutSnapshot(currentState, snapshot);
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to remove snapshot metadata", snapshot), e);
                failSnapshotCompletionListeners(
                    snapshot, new SnapshotException(snapshot, "Failed to remove snapshot from cluster state", e));
                if (listener != null) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onNoLongerMaster(String source) {
                failSnapshotCompletionListeners(
                    snapshot, ExceptionsHelper.useOrSuppress(failure, new SnapshotException(snapshot, "no longer master")));
                if (listener != null) {
                    listener.onNoLongerMaster();
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                failSnapshotCompletionListeners(snapshot, failure);
                if (listener != null) {
                    listener.onFailure(null);
                }
            }
        });
    }

    /**
     * 当某个处理失败的快照从CS移除后触发的监听器
     * @param snapshot
     * @param e
     */
    private void failSnapshotCompletionListeners(Snapshot snapshot, Exception e) {
        // 从相关容器中移除快照
        final List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>> completionListeners = snapshotCompletionListeners.remove(snapshot);
        if (completionListeners != null) {
            try {
                ActionListener.onFailure(completionListeners, e);
            } catch (Exception ex) {
                logger.warn("Failed to notify listeners", ex);
            }
        }
        endingSnapshots.remove(snapshot);
    }

    /**
     * Deletes snapshots from the repository or aborts a running snapshot.
     * If deleting a single snapshot, first checks if a snapshot is still running and if so cancels the snapshot and then deletes it from
     * the repository.
     * If the snapshot is not running or multiple snapshot names are given, moves to trying to find a matching {@link Snapshot}s for the
     * given names in the repository and deletes them.
     *
     * @param request         delete snapshot request
     * @param listener        listener
     */
    public void deleteSnapshots(final DeleteSnapshotRequest request, final ActionListener<Void> listener) {

        final String[] snapshotNames = request.snapshots();
        final String repositoryName = request.repository();
        logger.info(() -> new ParameterizedMessage("deleting snapshots [{}] from repository [{}]",
            Strings.arrayToCommaDelimitedString(snapshotNames), repositoryName));

        clusterService.submitStateUpdateTask("delete snapshot", new ClusterStateUpdateTask(Priority.NORMAL) {

            Snapshot runningSnapshot;

            boolean abortedDuringInit = false;

            @Override
            public ClusterState execute(ClusterState currentState) {
                if (snapshotNames.length > 1 && currentState.nodes().getMinNodeVersion().before(MULTI_DELETE_VERSION)) {
                    throw new IllegalArgumentException("Deleting multiple snapshots in a single request is only supported in version [ "
                        + MULTI_DELETE_VERSION + "] but cluster contained node of version [" + currentState.nodes().getMinNodeVersion()
                        + "]");
                }
                final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                final SnapshotsInProgress.Entry snapshotEntry;
                if (snapshotNames.length == 1) {
                    final String snapshotName = snapshotNames[0];
                    if (Regex.isSimpleMatchPattern(snapshotName)) {
                        snapshotEntry = null;
                    } else {
                        snapshotEntry = findInProgressSnapshot(snapshots, snapshotName, repositoryName);
                    }
                } else {
                    snapshotEntry = null;
                }
                if (snapshotEntry == null) {
                    return currentState;
                }
                runningSnapshot = snapshotEntry.snapshot();
                final ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards;

                final State state = snapshotEntry.state();
                final String failure;
                if (state == State.INIT) {
                    // snapshot is still initializing, mark it as aborted
                    shards = snapshotEntry.shards();
                    assert shards.isEmpty();
                    failure = "Snapshot was aborted during initialization";
                    abortedDuringInit = true;
                } else if (state == State.STARTED) {
                    // snapshot is started - mark every non completed shard as aborted
                    final ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shardsBuilder = ImmutableOpenMap.builder();
                    for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : snapshotEntry.shards()) {
                        ShardSnapshotStatus status = shardEntry.value;
                        if (status.state().completed() == false) {
                            status = new ShardSnapshotStatus(
                                status.nodeId(), ShardState.ABORTED, "aborted by snapshot deletion", status.generation());
                        }
                        shardsBuilder.put(shardEntry.key, status);
                    }
                    shards = shardsBuilder.build();
                    failure = "Snapshot was aborted by deletion";
                } else {
                    boolean hasUncompletedShards = false;
                    // Cleanup in case a node gone missing and snapshot wasn't updated for some reason
                    for (ObjectCursor<ShardSnapshotStatus> shardStatus : snapshotEntry.shards().values()) {
                        // Check if we still have shard running on existing nodes
                        if (shardStatus.value.state().completed() == false && shardStatus.value.nodeId() != null
                            && currentState.nodes().get(shardStatus.value.nodeId()) != null) {
                            hasUncompletedShards = true;
                            break;
                        }
                    }
                    if (hasUncompletedShards) {
                        // snapshot is being finalized - wait for shards to complete finalization process
                        logger.debug("trying to delete completed snapshot - should wait for shards to finalize on all nodes");
                        return currentState;
                    } else {
                        // no shards to wait for but a node is gone - this is the only case
                        // where we force to finish the snapshot
                        logger.debug("trying to delete completed snapshot with no finalizing shards - can delete immediately");
                        shards = snapshotEntry.shards();
                    }
                    failure = snapshotEntry.failure();
                }
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE,
                    new SnapshotsInProgress(snapshots.entries().stream().map(existing -> {
                        if (existing.equals(snapshotEntry)) {
                            return new SnapshotsInProgress.Entry(snapshotEntry, State.ABORTED, shards, failure);
                        }
                        return existing;
                    }).collect(Collectors.toUnmodifiableList()))).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (runningSnapshot == null) {
                    try {
                        repositoriesService.repository(repositoryName).executeConsistentStateUpdate(repositoryData ->
                                createDeleteStateUpdate(matchingSnapshotIds(repositoryData, snapshotNames, repositoryName), repositoryName,
                                    repositoryData.getGenId(), request.masterNodeTimeout(), Priority.NORMAL, listener),
                            "delete completed snapshots", listener::onFailure);
                    } catch (RepositoryMissingException e) {
                        listener.onFailure(e);
                    }
                    return;
                }
                logger.trace("adding snapshot completion listener to wait for deleted snapshot to finish");
                addListener(runningSnapshot, ActionListener.wrap(
                    result -> {
                        logger.debug("deleted snapshot completed - deleting files");
                        clusterService.submitStateUpdateTask("delete snapshot",
                            createDeleteStateUpdate(Collections.singletonList(result.v2().snapshotId()), repositoryName,
                                result.v1().getGenId(), null, Priority.IMMEDIATE, listener));
                    },
                    e -> {
                        if (abortedDuringInit) {
                            logger.info("Successfully aborted snapshot [{}]", runningSnapshot);
                            listener.onResponse(null);
                        } else {
                            if (ExceptionsHelper.unwrap(e, NotMasterException.class, FailedToCommitClusterStateException.class)
                                != null) {
                                logger.warn("master failover before deleted snapshot could complete", e);
                                // Just pass the exception to the transport handler as is so it is retried on the new master
                                listener.onFailure(e);
                            } else {
                                logger.warn("deleted snapshot failed", e);
                                listener.onFailure(
                                    new SnapshotMissingException(runningSnapshot.getRepository(), runningSnapshot.getSnapshotId(), e));
                            }
                        }
                    }
                ));
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }
        });
    }

    private static List<SnapshotId> matchingSnapshotIds(RepositoryData repositoryData, String[] snapshotsOrPatterns,
                                                        String repositoryName) {
        final Map<String, SnapshotId> allSnapshotIds = repositoryData.getSnapshotIds().stream().collect(
            Collectors.toMap(SnapshotId::getName, Function.identity()));
        final Set<SnapshotId> foundSnapshots = new HashSet<>();
        for (String snapshotOrPattern : snapshotsOrPatterns) {
            if (Regex.isSimpleMatchPattern(snapshotOrPattern) == false) {
                final SnapshotId foundId = allSnapshotIds.get(snapshotOrPattern);
                if (foundId == null) {
                    throw new SnapshotMissingException(repositoryName, snapshotOrPattern);
                } else {
                    foundSnapshots.add(allSnapshotIds.get(snapshotOrPattern));
                }
            } else {
                for (Map.Entry<String, SnapshotId> entry : allSnapshotIds.entrySet()) {
                    if (Regex.simpleMatch(snapshotOrPattern, entry.getKey())) {
                        foundSnapshots.add(entry.getValue());
                    }
                }
            }
        }
        return List.copyOf(foundSnapshots);
    }

    // Return in-progress snapshot entry by name and repository in the given cluster state or null if none is found
    @Nullable
    private static SnapshotsInProgress.Entry findInProgressSnapshot(@Nullable SnapshotsInProgress snapshots, String snapshotName,
                                                                    String repositoryName) {
        if (snapshots == null) {
            return null;
        }
        SnapshotsInProgress.Entry snapshotEntry = null;
        for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
            if (entry.repository().equals(repositoryName)
                && entry.snapshot().getSnapshotId().getName().equals(snapshotName)) {
                snapshotEntry = entry;
                break;
            }
        }
        return snapshotEntry;
    }

    private ClusterStateUpdateTask createDeleteStateUpdate(List<SnapshotId> snapshotIds, String repoName, long repositoryStateId,
                                                           @Nullable TimeValue timeout, Priority priority, ActionListener<Void> listener) {
        // Short circuit to noop state update if there isn't anything to delete
        if (snapshotIds.isEmpty()) {
            return new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(null);
                }

                @Override
                public TimeValue timeout() {
                    return timeout;
                }
            };
        }
        return new ClusterStateUpdateTask(priority) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);
                if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(new Snapshot(repoName, snapshotIds.get(0)),
                        "cannot delete - another snapshot is currently being deleted in [" + deletionsInProgress + "]");
                }
                final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(RepositoryCleanupInProgress.TYPE);
                if (repositoryCleanupInProgress != null && repositoryCleanupInProgress.hasCleanupInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(new Snapshot(repoName, snapshotIds.get(0)),
                        "cannot delete snapshots while a repository cleanup is in-progress in [" + repositoryCleanupInProgress + "]");
                }
                RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE);
                if (restoreInProgress != null) {
                    // don't allow snapshot deletions while a restore is taking place,
                    // otherwise we could end up deleting a snapshot that is being restored
                    // and the files the restore depends on would all be gone

                    for (RestoreInProgress.Entry entry : restoreInProgress) {
                        if (repoName.equals(entry.snapshot().getRepository()) && snapshotIds.contains(entry.snapshot().getSnapshotId())) {
                            throw new ConcurrentSnapshotExecutionException(new Snapshot(repoName, snapshotIds.get(0)),
                                "cannot delete snapshot during a restore in progress in [" + restoreInProgress + "]");
                        }
                    }
                }
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots != null && snapshots.entries().isEmpty() == false) {
                    // However other snapshots are running - cannot continue
                    throw new ConcurrentSnapshotExecutionException(
                        repoName, snapshotIds.toString(), "another snapshot is currently running cannot delete");
                }
                // add the snapshot deletion to the cluster state
                SnapshotDeletionsInProgress.Entry entry = new SnapshotDeletionsInProgress.Entry(
                    snapshotIds,
                    repoName,
                    threadPool.absoluteTimeInMillis(),
                    repositoryStateId
                );
                if (deletionsInProgress != null) {
                    deletionsInProgress = deletionsInProgress.withAddedEntry(entry);
                } else {
                    deletionsInProgress = SnapshotDeletionsInProgress.newInstance(entry);
                }
                return ClusterState.builder(currentState).putCustom(SnapshotDeletionsInProgress.TYPE, deletionsInProgress).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                deleteSnapshotsFromRepository(repoName, snapshotIds, listener, repositoryStateId, newState.nodes().getMinNodeVersion());
            }
        };
    }

    /**
     * Determines the minimum {@link Version} that the snapshot repository must be compatible with from the current nodes in the cluster
     * and the contents of the repository. The minimum version is determined as the lowest version found across all snapshots in the
     * repository and all nodes in the cluster.
     *
     * @param minNodeVersion minimum node version in the cluster
     * @param repositoryName name of the repository to modify
     * @param repositoryData current {@link RepositoryData} of that repository
     * @param excluded       snapshot id to ignore when computing the minimum version
     *                       (used to use newer metadata version after a snapshot delete)    某些是需要被排除的
     * @return minimum node version that must still be able to read the repository metadata
     * 获取最小的兼容版本
     */
    public Version minCompatibleVersion(Version minNodeVersion, String repositoryName, RepositoryData repositoryData,
                                        @Nullable Collection<SnapshotId> excluded) {
        // 记录这么多快照中最低的版本号
        Version minCompatVersion = minNodeVersion;
        // 获取此时所有的快照
        final Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        final Repository repository = repositoriesService.repository(repositoryName);
        for (SnapshotId snapshotId : snapshotIds.stream().filter(excluded == null ? sn -> true : Predicate.not(excluded::contains))
            .collect(Collectors.toList())) {
            // 找到每个快照对应的版本号
            final Version known = repositoryData.getVersion(snapshotId);
            // If we don't have the version cached in the repository data yet we load it from the snapshot info blobs
            if (known == null) {
                assert repositoryData.shardGenerations().totalShards() == 0 :
                    "Saw shard generations [" + repositoryData.shardGenerations() +
                        "] but did not have versions tracked for snapshot [" + snapshotId + "]";
                try {
                    final Version foundVersion = repository.getSnapshotInfo(snapshotId).version();
                    if (useShardGenerations(foundVersion) == false) {
                        // We don't really care about the exact version if its before 7.6 as the 7.5 metadata is the oldest we are able
                        // to write out so we stop iterating here and just use 7.5.0 as a placeholder.
                        return OLD_SNAPSHOT_FORMAT;
                    }
                    minCompatVersion = minCompatVersion.before(foundVersion) ? minCompatVersion : foundVersion;
                } catch (SnapshotMissingException e) {
                    logger.warn("Failed to load snapshot metadata, assuming repository is in old format", e);
                    return OLD_SNAPSHOT_FORMAT;
                }
            } else {
                minCompatVersion = minCompatVersion.before(known) ? minCompatVersion : known;
            }
        }
        return minCompatVersion;
    }

    /**
     * Checks whether the metadata version supports writing {@link ShardGenerations} to the repository.
     *
     * @param repositoryMetaVersion version to check
     * @return true if version supports {@link ShardGenerations}
     * 检测是否需要将版本号写入到存储层中
     */
    public static boolean useShardGenerations(Version repositoryMetaVersion) {
        return repositoryMetaVersion.onOrAfter(SHARD_GEN_IN_REPO_DATA_VERSION);
    }

    /**
     * Deletes snapshot from repository
     *
     * @param repoName          repository name
     * @param snapshotIds       snapshot ids
     * @param listener          listener
     * @param repositoryStateId the unique id representing the state of the repository at the time the deletion began
     * @param minNodeVersion    minimum node version in the cluster
     */
    private void deleteSnapshotsFromRepository(String repoName, Collection<SnapshotId> snapshotIds, @Nullable ActionListener<Void> listener,
                                               long repositoryStateId, Version minNodeVersion) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(listener, l -> {
            Repository repository = repositoriesService.repository(repoName);
            repository.getRepositoryData(ActionListener.wrap(repositoryData -> repository.deleteSnapshots(snapshotIds,
                repositoryStateId,
                minCompatibleVersion(minNodeVersion, repoName, repositoryData, snapshotIds),
                ActionListener.wrap(v -> {
                        logger.info("snapshots {} deleted", snapshotIds);
                        removeSnapshotDeletionFromClusterState(snapshotIds, null, l);
                    }, ex -> removeSnapshotDeletionFromClusterState(snapshotIds, ex, l)
                )), ex -> removeSnapshotDeletionFromClusterState(snapshotIds, ex, l)));
        }));
    }

    /**
     * Removes the snapshot deletion from {@link SnapshotDeletionsInProgress} in the cluster state.
     */
    private void removeSnapshotDeletionFromClusterState(final Collection<SnapshotId> snapshotIds, @Nullable final Exception failure,
                                                        @Nullable final ActionListener<Void> listener) {
        clusterService.submitStateUpdateTask("remove snapshot deletion metadata", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                SnapshotDeletionsInProgress deletions = currentState.custom(SnapshotDeletionsInProgress.TYPE);
                if (deletions != null) {
                    boolean changed = false;
                    if (deletions.hasDeletionsInProgress()) {
                        assert deletions.getEntries().size() == 1 : "should have exactly one deletion in progress";
                        SnapshotDeletionsInProgress.Entry entry = deletions.getEntries().get(0);
                        deletions = deletions.withRemovedEntry(entry);
                        changed = true;
                    }
                    if (changed) {
                        return ClusterState.builder(currentState).putCustom(SnapshotDeletionsInProgress.TYPE, deletions).build();
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("{} failed to remove snapshot deletion metadata", snapshotIds), e);
                if (listener != null) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (listener != null) {
                    if (failure != null) {
                        listener.onFailure(failure);
                    } else {
                        logger.info("Successfully deleted snapshots {}", snapshotIds);
                        listener.onResponse(null);
                    }
                }
            }
        });
    }

    /**
     * Calculates the list of shards that should be included into the current snapshot
     *
     * @param clusterState        cluster state   当前集群状态
     * @param indices             Indices to snapshot          本次快照相关的所有索引
     * @param useShardGenerations whether to write {@link ShardGenerations} during the snapshot   是否需要将分片的gen写入快照 新版本都是true
     * @return list of shard to be included into current snapshot
     */
    private static ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards(ClusterState clusterState,
                                                                                             List<IndexId> indices,
                                                                                             boolean useShardGenerations,
                                                                                             RepositoryData repositoryData) {
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableOpenMap.builder();
        Metadata metadata = clusterState.metadata();
        // 每个分片的gen 代表着什么???
        final ShardGenerations shardGenerations = repositoryData.shardGenerations();
        for (IndexId index : indices) {
            final String indexName = index.getName();
            // 代表本次快照中某个索引是之前没有的
            final boolean isNewIndex = repositoryData.getIndices().containsKey(indexName) == false;
            IndexMetadata indexMetadata = metadata.index(indexName);
            if (indexMetadata == null) {
                // The index was deleted before we managed to start the snapshot - mark it as missing.
                // 创建了一个内部包含未知索引的shardId
                builder.put(new ShardId(indexName, IndexMetadata.INDEX_UUID_NA_VALUE, 0),
                    new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.MISSING, "missing index", null));
            } else {
                // 获取该索引相关的所有分片
                IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(indexName);
                // 根据分片数量创建多个shardId
                for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                    ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
                    // 有关分片的gen
                    final String shardRepoGeneration;
                    if (useShardGenerations) {
                        if (isNewIndex) {
                            assert shardGenerations.getShardGen(index, shardId.getId()) == null
                                : "Found shard generation for new index [" + index + "]";
                            shardRepoGeneration = ShardGenerations.NEW_SHARD_GEN;
                        } else {
                            // 如果是旧的索引 可以直接通过 索引 + 分片id进行定位
                            shardRepoGeneration = shardGenerations.getShardGen(index, shardId.getId());
                        }
                    } else {
                        shardRepoGeneration = null;
                    }
                    // TODO 构建了这个对象又怎么了呢
                    if (indexRoutingTable != null) {
                        // 每个分片 还有主副关系 这里是只获取主分片
                        ShardRouting primary = indexRoutingTable.shard(i).primaryShard();
                        if (primary == null || !primary.assignedToNode()) {
                            builder.put(shardId,
                                new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.MISSING, "primary shard is not allocated",
                                    shardRepoGeneration));
                        } else if (primary.relocating() || primary.initializing()) {
                            builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(
                                primary.currentNodeId(), ShardState.WAITING, shardRepoGeneration));
                        } else if (!primary.started()) {
                            builder.put(shardId,
                                new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId(), ShardState.MISSING,
                                    "primary shard hasn't been started yet", shardRepoGeneration));
                        } else {
                            builder.put(shardId,
                                new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId(), shardRepoGeneration));
                        }
                    } else {
                        builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.MISSING,
                            "missing routing table", shardRepoGeneration));
                    }
                }
            }
        }

        return builder.build();
    }

    /**
     * Returns the indices that are currently being snapshotted (with partial == false) and that are contained in the indices-to-check set.
     */
    public static Set<Index> snapshottingIndices(final ClusterState currentState, final Set<Index> indicesToCheck) {
        final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
        if (snapshots == null) {
            return emptySet();
        }

        final Set<Index> indices = new HashSet<>();
        for (final SnapshotsInProgress.Entry entry : snapshots.entries()) {
            if (entry.partial() == false) {
                for (IndexId index : entry.indices()) {
                    IndexMetadata indexMetadata = currentState.metadata().index(index.getName());
                    if (indexMetadata != null && indicesToCheck.contains(indexMetadata.getIndex())) {
                        indices.add(indexMetadata.getIndex());
                    }
                }
            }
        }
        return indices;
    }

    /**
     * Adds snapshot completion listener
     *
     * @param snapshot Snapshot to listen for
     * @param listener listener
     */
    private void addListener(Snapshot snapshot, ActionListener<Tuple<RepositoryData, SnapshotInfo>> listener) {
        snapshotCompletionListeners.computeIfAbsent(snapshot, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        clusterService.removeApplier(this);
    }
}

