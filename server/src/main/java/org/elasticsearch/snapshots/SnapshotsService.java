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

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.cluster.SnapshotsInProgress.completed;

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

/**
 * Service responsible for creating snapshots. See package level documentation of {@link org.elasticsearch.snapshots}
 * for details.
 * 快照服务  内部需要基于存储服务来实现  并且某些shard 就可以基于快照进行数据恢复
 * 本对象也需要监听 clusterState 的变化
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
     * 存储服务 通过该对象可以获取仓库实例
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
     *
     * @param settings
     * @param clusterService
     * @param indexNameExpressionResolver  可以将包含通配符的indexName 定位到准确的索引名
     * @param repositoriesService   快照服务 依赖存储服务
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
     *
     * @param request  snapshot request
     * @param listener snapshot completion listener
     *                 执行快照任务 直到整个流程完成时才触发监听器
     */
    public void executeSnapshot(final CreateSnapshotRequest request, final ActionListener<SnapshotInfo> listener) {
        createSnapshot(request,
            // 当创建快照任务成功时  将监听器设置到最终的监听器列表中  并等待整个快照任务完成后 触发监听器
            ActionListener.wrap(snapshot -> addListener(snapshot, ActionListener.map(listener, Tuple::v2)), listener::onFailure));
    }

    /**
     * Initializes the snapshotting process.
     * <p>
     * This method is used by clients to start snapshot. It makes sure that there is no snapshots are currently running and
     * creates a snapshot record in cluster state metadata.
     *
     * @param request  snapshot request
     * @param listener snapshot creation listener
     *                 分片可以选择从本地 lucene索引文件 + 事务日志进行数据恢复
     *                 或者从远端节点传输数据  peer
     *                 也可以通过快照方式进行数据恢复   这里就是创建用于数据恢复的快照
     */
    public void createSnapshot(final CreateSnapshotRequest request, final ActionListener<Snapshot> listener) {

        // 不同的repository 有不同的快照生成逻辑 这里是指定某个仓库
        final String repositoryName = request.repository();
        // 使用 dataFormat解析器 对快照名进行解析
        final String snapshotName = indexNameExpressionResolver.resolveDateMathExpression(request.snapshot());
        // 校验名称有效性
        validate(repositoryName, snapshotName);

        // 每个快照会有一个唯一id
        final SnapshotId snapshotId = new SnapshotId(snapshotName, UUIDs.randomBase64UUID()); // new UUID for the snapshot

        // 通过指定的存储实例名 寻找实例对象  当仓库不存在时 会抛出异常
        Repository repository = repositoriesService.repository(request.repository());
        // 存储实例 处理一些用户传入的元数据 并尝试修改成自身可以处理的数据  默认情况下就是返回原数据
        final Map<String, Object> userMeta = repository.adaptUserMetadata(request.userMetadata());

        // 更新clusterState 并通知到其他节点
        // 向leader节点发起快照请求  leader节点会更新clusterState 并发布到集群的其他节点上
        clusterService.submitStateUpdateTask("create_snapshot [" + snapshotName + ']', new ClusterStateUpdateTask() {

            // SnapshotsInProgress 负责管理所有的生成快照任务   而每个entry则代表一个快照任务
            private SnapshotsInProgress.Entry newSnapshot = null;

            private List<String> indices;

            @Override
            public ClusterState execute(ClusterState currentState) {
                // 要求clusterState要存在该 repository 之后就是对快照名称做简单的格式校验
                validate(repositoryName, snapshotName, currentState);

                // 看来 SnapshotsInProgress/RepositoryCleanupInProgress/SnapshotDeletionsInProgress  这3个任务彼此都是互斥的
                // 一次只能执行一个任务 即使针对不同的repository 也不行 因为原始数据都是同一份吧

                // 快照任务和删除快照任务 不能同时执行
                SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);
                if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(repositoryName, snapshotName,
                        "cannot snapshot while a snapshot deletion is in-progress in [" + deletionsInProgress + "]");
                }
                // 清理仓库的任务 与快照任务也不能同时执行
                final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(RepositoryCleanupInProgress.TYPE);
                if (repositoryCleanupInProgress != null && repositoryCleanupInProgress.hasCleanupInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(repositoryName, snapshotName,
                        "cannot snapshot while a repository cleanup is in-progress in [" + repositoryCleanupInProgress + "]");
                }
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                // Fail if there are any concurrently running snapshots. The only exception to this being a snapshot in INIT state from a
                // previous master that we can simply ignore and remove from the cluster state because we would clean it up from the
                // cluster state anyway in #applyClusterState.

                // 在并发执行更新任务时  实际上是通过写时拷贝技术来避免并发问题的  每个线程都是获取到 clusterState的副本
                // 这里是要求所有快照都处于 init状态 并且必须存在于 initializingSnapshots 容器中
                if (snapshots != null && snapshots.entries().stream().anyMatch(entry ->
                    // 等价于 (entry.state() == State.INIT && initializingSnapshots.contains(entry.snapshot()) 全部是false  就抛出异常
                    (entry.state() == State.INIT && initializingSnapshots.contains(entry.snapshot()) == false) == false)) {
                    throw new ConcurrentSnapshotExecutionException(repositoryName, snapshotName, " a snapshot is already running");
                }
                // Store newSnapshot here to be processed in clusterStateProcessed
                // 针对哪些索引开启快照任务
                indices = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(currentState,
                    request.indicesOptions(), request.indices()));
                logger.trace("[{}][{}] creating snapshot for indices [{}]", repositoryName, snapshotName, indices);

                // 创建一个新的快照实体   为什么在这个阶段不传入 index/shard信息
                newSnapshot = new SnapshotsInProgress.Entry(
                    // 快照名称和存储名称是指定的   snapshotId 内部的uuid是随机生成的
                    new Snapshot(repositoryName, snapshotId),

                    // 在创建快照的时候 可以要求是否包含globalState  以及是否是部分(partial)的
                    request.includeGlobalState(), request.partial(),
                    // 当刚创建快照任务时 此时状态为 init
                    State.INIT,
                    // 在初始化entry时  可以看到这里还没有传入 index信息
                    Collections.emptyList(), // We'll resolve the list of indices when moving to the STARTED state in #beginSnapshot
                    threadPool.absoluteTimeInMillis(),
                    // 刚创建快照任务时 未指定仓库的gen
                    RepositoryData.UNKNOWN_REPO_GEN,
                    null,
                    userMeta, Version.CURRENT
                );
                // 将初始化的快照存储到 initializingSnapshots中
                initializingSnapshots.add(newSnapshot.snapshot());
                // 这里进行了覆盖操作 所以之前才会要求内部的所有entry必须处于init状态 否则无法进行覆盖
                snapshots = new SnapshotsInProgress(newSnapshot);
                // 将最新的快照发布到集群中
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
            }

            /**
             * 发布某个快照失败时 将任务移除
             * 发布任务不能短期内连续执行   从这一层面上解决了rest接口并发调用问题
             * 一旦上一个发布完成 必然是获取到最新的clusterState  但是像类似发布冲突这种异常是属于 可重试异常
             * @param source
             * @param e
             */
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
             * 只有当集群中确定了唯一的leader后 才能执行其他更新clusterState的操作
             * 当在所有的节点上都产生结果时 并且要满足voteConfiguration条件的节点数成功 才可以触发该方法 失败的情况就代表很多节点宕机了
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

                        // 当任务完成或者失败 触发 用户执行命令时传入的监听器

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
     * @param snapshotName   snapshot name
     * @param state          current cluster state
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
     * @param clusterState               cluster state
     * @param snapshot                   snapshot meta data
     * @param partial                    allow partial snapshots
     * @param repository
     * @param userCreateSnapshotListener
     * 当成功将创建快照的clusterState 更新到集群其他节点后 开始执行快照任务
     */
    private void beginSnapshot(final ClusterState clusterState,
                               final SnapshotsInProgress.Entry snapshot,
                               final boolean partial,
                               final List<String> indices,
                               final Repository repository,
                               final ActionListener<Snapshot> userCreateSnapshotListener) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {

            /**
             * 代表本快照任务已经被标记成禁止了
             */
            boolean hadAbortedInitializations;

            /**
             * 主要是根据当前的分片状态 索引信息检测本次是否有足够的数据以便生成快照  不满足条件则修改成failed  满足条件修改成started
             */
            @Override
            protected void doRun() {
                assert initializingSnapshots.contains(snapshot.snapshot());
                // 仓库本身是只读对象 无法生成快照
                if (repository.isReadOnly()) {
                    throw new RepositoryException(repository.getMetadata().name(), "cannot create snapshot in a readonly repository");
                }
                final String snapshotName = snapshot.snapshot().getSnapshotId().getName();
                final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
                // 从仓库中最新的 index-gen 文件加载数据 会转换成 repositoryData  该对象本身相当于是 仓库的描述信息
                repository.getRepositoryData(repositoryDataListener);

                // 当加载到仓库数据后 触发函数
                repositoryDataListener.whenComplete(repositoryData -> {
                    // check if the snapshot name already exists in the repository
                    // 仓库中已经存在某个快照了 拒绝执行
                    if (repositoryData.getSnapshotIds().stream().anyMatch(s -> s.getName().equals(snapshotName))) {
                        throw new InvalidSnapshotNameException(
                            repository.getMetadata().name(), snapshotName, "snapshot with the same name already exists");
                    }

                    logger.info("snapshot [{}] started", snapshot.snapshot());
                    // 这里就是从所有快照中找到最小的版本  TODO 兼容性相关的忽略
                    final Version version =
                        minCompatibleVersion(clusterState.nodes().getMinNodeVersion(), snapshot.repository(), repositoryData, null);
                    // 本次快照没有选定的索引信息 直接结束任务
                    if (indices.isEmpty()) {
                        // No indices in this snapshot - we are done
                        // 触发监听器 代表本次索引操作完成
                        userCreateSnapshotListener.onResponse(snapshot.snapshot());
                        // 快照任务结束
                        endSnapshot(new SnapshotsInProgress.Entry(
                            snapshot, State.STARTED, Collections.emptyList(), repositoryData.getGenId(), null, version,
                            null), clusterState.metadata());
                        return;
                    }

                    // 正常的处理流程在这里
                    // 第一步先是将追加了快照信息的 clusterState发布到集群 并成功后 才触发第二步
                    // 第二步将合适的shard级快照任务 发布到其他节点上 如果某节点包含该shard信息 就会开始创建快照
                    // 快照实际上就是先将lucene数据刷盘 并根据最新的索引文件生成一份副本文件 并存储在repository上
                    // 当每个节点的快照任务完成后 会上报给leader节点
                    clusterService.submitStateUpdateTask("update_snapshot [" + snapshot.snapshot() + "]", new ClusterStateUpdateTask() {


                        /**
                         * @param currentState
                         * @return
                         */
                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            // 获取正在处理中的快照
                            SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                            List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                            for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                                // 其余快照entry 保持不变
                                if (entry.snapshot().equals(snapshot.snapshot()) == false) {
                                    entries.add(entry);
                                    continue;
                                }

                                // 如果当前任务已经被标记成禁止   比如在createSnapshot 与 beginSnapshot的时间差内 执行了快照删除任务
                                if (entry.state() == State.ABORTED) {
                                    entries.add(entry);
                                    assert entry.shards().isEmpty();
                                    hadAbortedInitializations = true;
                                } else {

                                    // 将快照任务拆解到index级别
                                    // 将 indexName 转换成 indexId   如果之前的repositoryData中没有索引信息 就需要创建新的索引对象
                                    final List<IndexId> indexIds = repositoryData.resolveNewIndices(indices);
                                    // Replace the snapshot that was just initialized
                                    // 以分片为单位提取快照信息  ShardSnapshotStatus 是根据对应主分片的状态来生成的
                                    ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards =
                                        shards(currentState, indexIds, useShardGenerations(version), repositoryData);

                                    // 因为某些分片此时可能无法正常生成快照 比如处于未恢复 或者未分配的状态 那么通过是否 允许针对部分数据生成快照 走不同逻辑

                                    // 当并非所有shard都可以生成快照的情况下
                                    if (!partial) {
                                        // v1 对应未找到分片信息
                                        // v2 代表分片对应的索引已经被关闭
                                        Tuple<Set<String>, Set<String>> indicesWithMissingShards = indicesWithMissingShards(shards,
                                            currentState.metadata());
                                        Set<String> missing = indicesWithMissingShards.v1();
                                        Set<String> closed = indicesWithMissingShards.v2();

                                        // 因为本次快照任务要求包含所有分片 所以任务失败
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
                                            // 将本次快照任务标记成失败
                                            entries.add(new SnapshotsInProgress.Entry(entry, State.FAILED, indexIds,
                                                repositoryData.getGenId(), shards, version, failureMessage.toString()));
                                            continue;
                                        }
                                    }
                                    // 如果本次快照设置了 部分模式 那么允许忽略部分未确定的分片
                                    entries.add(new SnapshotsInProgress.Entry(entry, State.STARTED, indexIds, repositoryData.getGenId(),
                                        shards, version, null));
                                }
                            }
                            // 使用新的entries 覆盖之前的数据
                            return ClusterState.builder(currentState)
                                .putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(unmodifiableList(entries)))
                                .build();
                        }

                        /**
                         * 更新快照任务失败时 从clusterState中移除
                         * @param source
                         * @param e
                         */
                        @Override
                        public void onFailure(String source, Exception e) {
                            logger.warn(() -> new ParameterizedMessage("[{}] failed to create snapshot",
                                snapshot.snapshot().getSnapshotId()), e);
                            removeSnapshotFromClusterState(snapshot.snapshot(), e,
                                // 直接触发这个监听器
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

                        /**
                         * 快照第一阶段 将一个init状态的快照任务发布到集群中
                         * 第二阶段 找到本次快照任务相关的所有shard 并生成status 之后 将快照任务更改成start后发布到集群中
                         * 如果本次快照任务已经失败了 也要发布到集群中
                         */

                        /**
                         * 发布成功后 触发该方法
                         * @param source
                         * @param oldState
                         * @param newState
                         */
                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            // The userCreateSnapshotListener.onResponse() notifies caller that the snapshot was accepted
                            // for processing. If client wants to wait for the snapshot completion, it can register snapshot
                            // completion listener in this method. For the snapshot completion to work properly, the snapshot
                            // should still exist when listener is registered.
                            // 如果用户在生成快照时 是等待快照完成 那么这一步 就是将监听器存储到一个list中
                            // 如果用户仅仅是发起创建快照的请求 那么这一步已经完成创建了
                            userCreateSnapshotListener.onResponse(snapshot.snapshot());

                            // 代表本次任务被终止了 触发endSnapshot 将本次快照任务的相关信息 持久化到repositoryData中
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
     * 当某个快照任务完成时 触发该函数
     *
     * @param snapshot
     * @param metadata
     * @return
     */
    private static ShardGenerations buildGenerations(SnapshotsInProgress.Entry snapshot, Metadata metadata) {
        ShardGenerations.Builder builder = ShardGenerations.builder();

        // 本次该快照相关的所有索引信息都会存储到该容器中
        final Map<String, IndexId> indexLookup = new HashMap<>();
        // 将快照中所有的索引信息都插入到 map中
        snapshot.indices().forEach(idx -> indexLookup.put(idx.getName(), idx));
        snapshot.shards().forEach(c -> {
            // 如果此时元数据下已经没有这个索引信息了 忽略
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
     * @param snapshots           list of snapshots that will be used as a filter, empty list means no snapshots are filtered  只有命中的才会返回
     * @return list of metadata for currently running snapshots
     * 获取指定repository下所有的快照信息
     */
    public static List<SnapshotsInProgress.Entry> currentSnapshots(@Nullable SnapshotsInProgress snapshotsInProgress, String repository,
                                                                   List<String> snapshots) {
        if (snapshotsInProgress == null || snapshotsInProgress.entries().isEmpty()) {
            return Collections.emptyList();
        }
        if ("_all".equals(repository)) {
            return snapshotsInProgress.entries();
        }
        // 当前只有一个快照任务信息
        if (snapshotsInProgress.entries().size() == 1) {
            // Most likely scenario - one snapshot is currently running
            // Check this snapshot against the query
            // 获取快照实体
            SnapshotsInProgress.Entry entry = snapshotsInProgress.entries().get(0);
            // 如果快照使用的repository 与传入的不一致  返回空列表
            if (entry.snapshot().getRepository().equals(repository) == false) {
                return Collections.emptyList();
            }
            // 如果有一个选取范围  那么只有匹配的才会返回
            if (snapshots.isEmpty() == false) {
                for (String snapshot : snapshots) {
                    // 代表匹配成功 因为当前entry数量为1 并且entry与snapshot为一一对应关系
                    if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                        return snapshotsInProgress.entries();
                    }
                }
                return Collections.emptyList();
            } else {
                // 如果没有指定范围  直接返回entry即可
                return snapshotsInProgress.entries();
            }
        }

        // 找到所有匹配的entry并返回
        List<SnapshotsInProgress.Entry> builder = new ArrayList<>();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            // 跳过不匹配的repository
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
     * 只有当集群中的leader收到其他所有节点的支持(join) 时 才能开始接收别的clusterState更新请求
     *
     * 该函数中应该包含了处理快照任务完成的逻辑
     * @param event
     */
    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            // 整个快照流程都是由 leader来统筹的
            if (event.localNodeMaster()) {
                // We don't remove old master when master flips anymore. So, we need to check for change in master
                // 获取描述当前运行的快照信息的对象
                final SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);

                // es选举可能会发生clusterState的更新被覆盖  只要leader更换了就有覆盖的可能
                // 但是如果快照任务同步到了新的leader 那么每个执行shard快照任务的节点会将结果信息发送到新的leader上
                final boolean newMaster = event.previousState().nodes().isLocalNodeElectedMaster() == false;

                if (snapshotsInProgress != null) {
                    // 当leader节点变化必然伴随着节点的下线   或者检测到其他节点下线 都要触发该方法
                    if (newMaster || removedNodesCleanupNeeded(snapshotsInProgress, event.nodesDelta().removedNodes())) {
                        processSnapshotsOnRemovedNodes();
                    }

                    // 更新路由表的任务与更新leader的任务本身是无法同时执行的
                    // 路由表发生变化即是某些分片发生了重路由 找到了更合适的节点
                    if (event.routingTableChanged() && waitingShardsStartedOrUnassigned(snapshotsInProgress, event)) {
                        // 启动init的分片快照任务 或者标记为失败
                        processStartedShards();
                    }
                    // Cleanup all snapshots that have no more work left:
                    // 1. Completed snapshots
                    // 2. Snapshots in state INIT that the previous master failed to start
                    // 3. Snapshots in any other state that have all their shard tasks completed

                    // 检测哪些快照已经完成 需要触发 endSnapshot
                    snapshotsInProgress.entries().stream().filter(
                        entry ->
                            // 代表该快照任务已经完成
                            entry.state().completed()
                                ||
                                // 或者本次快照任务还处于init状态/已完成 并且更换了leader
                                (initializingSnapshots.contains(entry.snapshot()) == false && (entry.state() == State.INIT || completed(entry.shards().values())))
                    ).forEach(entry -> endSnapshot(entry, event.state().metadata()));
                }

                // 可能本节点刚晋升 而之前刚提交一个快照删除任务 这里要继续执行删除任务
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
     * 当leader发生更替时 发现之前存在一个快照删除任务 那么继续执行
     */
    private void finalizeSnapshotDeletionFromPreviousMaster(ClusterState state) {
        SnapshotDeletionsInProgress deletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
        if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
            assert deletionsInProgress.getEntries().size() == 1 : "only one in-progress deletion allowed per cluster";
            SnapshotDeletionsInProgress.Entry entry = deletionsInProgress.getEntries().get(0);
            // 执行删除操作
            deleteSnapshotsFromRepository(entry.repository(), entry.getSnapshots(), null, entry.repositoryStateId(),
                state.nodes().getMinNodeVersion());
        }
    }

    /**
     * Cleans up shard snapshots that were running on removed nodes
     * 在执行快照任务时 会根据index 找到执行快照任务的所有node (index包含所有shard信息 每个shard对应不同的node)
     * 而某次clusterState的更新 可能会有某个node下线了  这里进行处理
     */
    private void processSnapshotsOnRemovedNodes() {

        // 在提交任务并执行时  会判断自己是否还是leader节点  当本节点降级后无法执行任务
        clusterService.submitStateUpdateTask("update snapshot state after node removal", new ClusterStateUpdateTask() {

            /**
             * 只有在集群确定了唯一的那个leader后才能正常执行更新操作
             * @param currentState
             * @return
             */
            @Override
            public ClusterState execute(ClusterState currentState) {
                DiscoveryNodes nodes = currentState.nodes();
                // 如果没有快照任务 不需要做处理
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots == null) {
                    return currentState;
                }
                boolean changed = false;
                ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                // 每个entry 代表一次快照任务  TODO 目前只能看到一次存在一个entry 每次都是覆盖操作 这里多个entry是怎么产生的???
                for (final SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                    SnapshotsInProgress.Entry updatedSnapshot = snapshot;

                    // init状态的快照任务 还没有设置shard 所以不需要处理   started/aborted 代表快照任务还未完成
                    if (snapshot.state() == State.STARTED || snapshot.state() == State.ABORTED) {
                        ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
                        boolean snapshotChanged = false;
                        // 每个快照任务会作用到多个分片
                        for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : snapshot.shards()) {

                            // 对应在每个分片处理的状态
                            final ShardSnapshotStatus shardStatus = shardEntry.value;
                            final ShardId shardId = shardEntry.key;
                            // 主要针对未完成的快照 检测它们的node是否在本次变化中被移除
                            if (!shardStatus.state().completed() && shardStatus.nodeId() != null) {
                                // 节点还存在 不需要做处理
                                if (nodes.nodeExists(shardStatus.nodeId())) {
                                    shards.put(shardId, shardStatus);
                                } else {
                                    // TODO: Restart snapshot on another node?
                                    // 这个意思就是  在执行快照任务的过程中 在某些分片上执行的任务由于node下线了 从集群中被自动移除 导致针对该分片的快照任务也失败了
                                    // 但是其他已经完成了的分片快照任务 即使此时节点下线了也不会有影响

                                    // 目标节点都已经下线了 快照任务就自然终止了
                                    snapshotChanged = true;
                                    logger.warn("failing snapshot of shard [{}] on closed node [{}]",
                                        shardId, shardStatus.nodeId());

                                    // 将status 更新成 failed
                                    shards.put(shardId,
                                        new ShardSnapshotStatus(shardStatus.nodeId(), ShardState.FAILED, "node shutdown",
                                            shardStatus.generation()));
                                }
                            } else {

                                // 其余的原样存储
                                shards.put(shardId, shardStatus);
                            }
                        }

                        // 有某个分片的快照任务被标记成失败了  更新本次snapshot.entry
                        if (snapshotChanged) {
                            changed = true;
                            ImmutableOpenMap<ShardId, ShardSnapshotStatus> shardsMap = shards.build();
                            // 此时所有分片快照任务都已经产生了结果 所以将快照任务修改成 success
                            if (!snapshot.state().completed() && completed(shardsMap.values())) {
                                updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, State.SUCCESS, shardsMap);
                            } else {
                                // 保持原state
                                updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, snapshot.state(), shardsMap);
                            }
                        }
                        entries.add(updatedSnapshot);

                        // 代表leader发生变化 并且快照任务同步过去了 这个时候就忽略这次快照任务
                        // 实际上如果某个新选举成功的leader没有同步到快照任务 那么在发布clusterState时 snapshotShardsService 就会自动关闭这些分片快照任务
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
     * 根据路由表信息 更新某些分片此时的状态
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

                        // 当快照任务被标记成 started后 才设置index/shard 信息
                        if (snapshot.state() == State.STARTED) {
                            // 找到之前处于等待状态的分片 通过路由表检测分片是否解除了 init/relocation状态 并修改它们的state(这样就可以开始生成快照了)
                            ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards = processWaitingShards(snapshot.shards(),
                                routingTable);

                            // 代表分片发生了变化
                            if (shards != null) {
                                changed = true;
                                if (!snapshot.state().completed() && completed(shards.values())) {
                                    // 当所有的分片都完成快照任务后 外层entry的状态变成了success
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
     * 根据最新的路由表信息 更新分片的状态
     *
     * @param snapshotShards
     * @param routingTable
     * @return
     */
    private static ImmutableOpenMap<ShardId, ShardSnapshotStatus> processWaitingShards(
        ImmutableOpenMap<ShardId, ShardSnapshotStatus> snapshotShards, RoutingTable routingTable) {
        boolean snapshotChanged = false;
        ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();

        // 遍历所有待处理的分片
        for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : snapshotShards) {
            ShardSnapshotStatus shardStatus = shardEntry.value;
            ShardId shardId = shardEntry.key;

            // 只处于 waiting的分片
            if (shardStatus.state() == ShardState.WAITING) {
                IndexRoutingTable indexShardRoutingTable = routingTable.index(shardId.getIndex());
                if (indexShardRoutingTable != null) {
                    // 通过索引和分片定位到这个 IndexShardRoutingTable
                    IndexShardRoutingTable shardRouting = indexShardRoutingTable.shard(shardId.id());
                    // 快照只针对主分片
                    if (shardRouting != null && shardRouting.primaryShard() != null) {
                        // 主分片已经完成了数据恢复  可以开始生成快照了
                        if (shardRouting.primaryShard().started()) {
                            // Shard that we were waiting for has started on a node, let's process it
                            snapshotChanged = true;
                            logger.trace("starting shard that we were waiting for [{}] on node [{}]", shardId, shardStatus.nodeId());
                            shards.put(shardId,
                                new ShardSnapshotStatus(shardRouting.primaryShard().currentNodeId(), shardStatus.generation()));
                            continue;
                            // 保持原样
                        } else if (shardRouting.primaryShard().initializing() || shardRouting.primaryShard().relocating()) {
                            // Shard that we were waiting for hasn't started yet or still relocating - will continue to wait
                            shards.put(shardId, shardStatus);
                            continue;
                        }
                    }
                }
                // 代表分片又重新回到 unassigned状态 那么该分片的快照任务就失败了
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
     * 在创建快照任务时 某些分片可能处于init/relocation状态  此时无法执行任务 当这些分片完成数据恢复后 就可以重新开始快照任务了
     *
     * @param snapshotsInProgress
     * @param event
     * @return
     */
    private static boolean waitingShardsStartedOrUnassigned(SnapshotsInProgress snapshotsInProgress, ClusterChangedEvent event) {
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {

            // 快照任务在开始前 是不会设置index/shard属性的  只有处于start阶段 才有必要处理
            if (entry.state() == State.STARTED) {
                // 找到处于init/relocation的分片对应的index
                for (ObjectCursor<String> index : entry.waitingIndices().keys()) {
                    // 如果等待中的索引正好发生了变化
                    if (event.indexRoutingTableChanged(index.value)) {
                        IndexRoutingTable indexShardRoutingTable = event.state().getRoutingTable().index(index.value);
                        // 找到所有相关的分片   因为快照只针对 primary 所以只需要主分片的信息就可以了
                        for (ShardId shardId : entry.waitingIndices().get(index.value)) {
                            // 找到该 shardId 对应所有分片 包含主副本
                            ShardRouting shardRouting = indexShardRoutingTable.shard(shardId.id()).primaryShard();
                            // 代表从waiting状态解除   如果处于started就可以开始任务了 如果处于 unassigned 就要将本次分片快照任务标记成失败
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
     * 某些快照任务尚未完成的节点在本次clusterState更新中被移除了
     *
     * @param snapshotsInProgress
     * @param removedNodes        某次clusterState变化时 相较之前被移除的node
     * @return
     */
    private static boolean removedNodesCleanupNeeded(SnapshotsInProgress snapshotsInProgress, List<DiscoveryNode> removedNodes) {
        // If at least one shard was running on a removed node - we need to fail it
        return removedNodes.isEmpty() == false && snapshotsInProgress.entries().stream().flatMap(snapshot ->
            // 获取快照在每个分片上的任务  如果快照还处在 init阶段 是没有设置shardId信息的
            StreamSupport.stream(((Iterable<ShardSnapshotStatus>) () -> snapshot.shards().valuesIt()).spliterator(), false)
                // 找到所有未完成的分片任务  并获取他们所在的节点
                .filter(s -> s.state().completed() == false).map(ShardSnapshotStatus::nodeId))
            .anyMatch(removedNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet())::contains);
    }

    /**
     * Returns list of indices with missing shards, and list of indices that are closed
     *
     * @param shards list of shard statuses
     * @return list of failed and closed indices
     * 找到所有失效的分片
     */
    private static Tuple<Set<String>, Set<String>> indicesWithMissingShards(
        ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards, Metadata metadata) {
        Set<String> missing = new HashSet<>();
        Set<String> closed = new HashSet<>();
        for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> entry : shards) {
            // miss的情况有很多  比如没有该索引的元数据  或者索引路由表不存在  或者分片本身处于unassigned状态
            if (entry.value.state() == ShardState.MISSING) {
                if (metadata.hasIndex(entry.key.getIndex().getName()) &&
                    // 关闭索引会怎样
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
        // 去重
        if (endingSnapshots.add(entry.snapshot()) == false) {
            return;
        }
        final Snapshot snapshot = entry.snapshot();
        // entry.repositoryStateId() 对应 repositoryData.genId()  如果本次快照信息未知 直接移除就好
        // 默认情况下 某个仓库对象被创建时 会加载此时最新的仓库gen  最小为 RepositoryData.EMPTY_REPO_GEN (-1  unknown是-2) -2 就是异常情况
        if (entry.repositoryStateId() == RepositoryData.UNKNOWN_REPO_GEN) {
            logger.debug("[{}] was aborted before starting", snapshot);
            removeSnapshotFromClusterState(entry.snapshot(), new SnapshotException(snapshot, "Aborted on initialization"), null);
            return;
        }
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {
            @Override
            protected void doRun() {

                // 获取对应的仓库对象
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

                // 每个分片的快照任务执行完后 都会生成一个 shardGen 分片目录下对应本次快照的数据  然后在repositoryData中就要保存本次快照所有的shardGen信息
                // 如果本次任务被打断 那么gen对应的还是上次的记录
                final ShardGenerations shardGenerations = buildGenerations(entry, metadata);
                // 快照完成后 触发仓库的 finalize方法  将最新的repositoryData 同步到集群的其他节点
                repository.finalizeSnapshot(
                    snapshot.getSnapshotId(),
                    shardGenerations,
                    entry.startTime(),
                    failure,
                    entry.partial() ? shardGenerations.totalShards() : entry.shards().size(),
                    unmodifiableList(shardFailures),
                    // 每当执行一次 finalizeSnapshot repositoryData 就会更新一次 同时 repositoryStateId 也会发生变化
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
                        // 当快照任务完成时 却发现仓库本身发生了变化 触发该函数
                    }, this::onFailure));
            }

            @Override
            public void onFailure(final Exception e) {
                Snapshot snapshot = entry.snapshot();

                // 如果将快照信息写入到本地repositoryData/发布到集群中失败时

                // 用户在创建快照时 如果没有监听快照执行结果 那么只要成功创建快照 就返回了  如果要求等待整个快照任务完成 那么就会这里失败时 就会通知到用户
                if (ExceptionsHelper.unwrap(e, NotMasterException.class, FailedToCommitClusterStateException.class) != null) {
                    // Failure due to not being master any more, don't try to remove snapshot from cluster state the next master
                    // will try ending this snapshot again
                    logger.debug(() -> new ParameterizedMessage(
                        "[{}] failed to update cluster state during snapshot finalization", snapshot), e);

                    // 通知用户本次快照任务执行失败了  因为这里返回的异常被包装了 action 不会进行重试
                    // 一旦新的leader节点初始化后 该对象还是会监听快照任务 并且只要快照任务完成还是会走更新repositoryData的逻辑
                    // TODO 问题是 其他节点repositoryData.gen 与之前leader的不一样啊 数据对不上没办法正常结束快照任务
                    failSnapshotCompletionListeners(snapshot,
                        new SnapshotException(snapshot, "Failed to update cluster state during snapshot finalization", e));
                } else {
                    // 如果是其他异常信息  比如快照任务完成时  repositoryGen 已经发生了变化
                    // 将本次快照任务从 clusterState中移除 同时发布到其他节点上 无论是否成功 都会触发 failSnapshotCompletionListeners 代表本次快照任务失败了
                    logger.warn(() -> new ParameterizedMessage("[{}] failed to finalize snapshot", snapshot), e);
                    removeSnapshotFromClusterState(snapshot, e, null);
                }
            }
        });
    }

    /**
     * 从当前CS中 剔除掉指定的snapshot
     *
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
     *
     * @param snapshot snapshot
     * @param failure  exception if snapshot failed
     * @param listener listener to notify when snapshot information is removed from the cluster state
     *                 某个快照任务结束后 需要从clusterState中移除
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
     * 如果用户监听了某个快照任务的完成结果 那么触发监听器
     *
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
     * @param request  delete snapshot request
     * @param listener listener
     *                 发起一个删除快照的任务
     */
    public void deleteSnapshots(final DeleteSnapshotRequest request, final ActionListener<Void> listener) {

        // 本次要删除的所有快照
        final String[] snapshotNames = request.snapshots();
        // 这个快照是创建在哪个仓库下的
        final String repositoryName = request.repository();
        logger.info(() -> new ParameterizedMessage("deleting snapshots [{}] from repository [{}]",
            Strings.arrayToCommaDelimitedString(snapshotNames), repositoryName));

        clusterService.submitStateUpdateTask("delete snapshot", new ClusterStateUpdateTask(Priority.NORMAL) {

            /**
             * 当前正在执行的快照任务
             */
            Snapshot runningSnapshot;

            /**
             * 对应的快照任务在init阶段就被关闭
             */
            boolean abortedDuringInit = false;

            /**
             * 创建一个删除快照的任务
             * @param currentState
             * @return
             */
            @Override
            public ClusterState execute(ClusterState currentState) {
                if (snapshotNames.length > 1 && currentState.nodes().getMinNodeVersion().before(MULTI_DELETE_VERSION)) {
                    throw new IllegalArgumentException("Deleting multiple snapshots in a single request is only supported in version [ "
                        + MULTI_DELETE_VERSION + "] but cluster contained node of version [" + currentState.nodes().getMinNodeVersion()
                        + "]");
                }
                final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                final SnapshotsInProgress.Entry snapshotEntry;

                // 如果是针对多个快照的删除 就不更改快照状态了
                // 如果传入的快照名包含通配符 代表可能会匹配到多个快照 那么也不更改快照状态了
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
                // 不修改clusterState 但是还是会触发监听器
                if (snapshotEntry == null) {
                    return currentState;
                }

                // entry 相当于只是一个包装对象
                runningSnapshot = snapshotEntry.snapshot();
                final ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards;

                final State state = snapshotEntry.state();
                final String failure;
                // 如果要删除的快照任务还未启动  因为clusterState的更新是串行的 所以这里确定是否是在init后立即被标记成中断是有意义的
                if (state == State.INIT) {
                    // snapshot is still initializing, mark it as aborted
                    shards = snapshotEntry.shards();
                    assert shards.isEmpty();
                    failure = "Snapshot was aborted during initialization";
                    abortedDuringInit = true;

                    // 如果任务已经启动就修改成 aborted snapshotShardsService 会感知到状态的变化 并终止快照任务
                } else if (state == State.STARTED) {
                    // snapshot is started - mark every non completed shard as aborted
                    final ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shardsBuilder = ImmutableOpenMap.builder();
                    for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : snapshotEntry.shards()) {
                        ShardSnapshotStatus status = shardEntry.value;
                        // 找到哪些还未执行完成的shard 直接修改成aborted
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
                    // 代表此时快照任务处于aborted状态 并且某些分片也处于aborted状态 还没有修改成failure 只需要等待即可
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
                // 将某些分片快照任务修改成 aborted后 发布到集群中
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

            /**
             * 删除快照首先尝试关闭已经启动的快照任务  即修改成aborted状态
             * 如果快照任务已经完成 就尝试从repository中删除对应快照数据
             * @param source
             * @param oldState
             * @param newState
             */
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                // 代表快照任务已经结束 repositoryData已经同步到集群
                // 或者本次针对的是多个快照
                if (runningSnapshot == null) {
                    try {
                        repositoriesService.repository(repositoryName).executeConsistentStateUpdate(
                            // 发布一个快照删除任务 并从本地仓库删除相关数据 当数据删除完毕后 更新repositoryData 并发布到集群中
                            // 如果快照未完成 这里发布快照删除任务就会失败
                            repositoryData ->
                                createDeleteStateUpdate(matchingSnapshotIds(repositoryData, snapshotNames, repositoryName), repositoryName,
                                    repositoryData.getGenId(), request.masterNodeTimeout(), Priority.NORMAL, listener),
                            "delete completed snapshots", listener::onFailure);
                    } catch (RepositoryMissingException e) {
                        listener.onFailure(e);
                    }
                    return;
                }
                logger.trace("adding snapshot completion listener to wait for deleted snapshot to finish");

                // 因为本次快照任务还未完成 无法立即执行快照删除任务 这里要监听快照任务
                addListener(runningSnapshot, ActionListener.wrap(
                    result -> {
                        logger.debug("deleted snapshot completed - deleting files");
                        // 创建一个 快照删除任务 并在发布完成后执行删除操作
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

    /**
     * 从 repository中找到符合条件的快照数据
     * @param repositoryData
     * @param snapshotsOrPatterns
     * @param repositoryName
     * @return
     */
    private static List<SnapshotId> matchingSnapshotIds(RepositoryData repositoryData, String[] snapshotsOrPatterns,
                                                        String repositoryName) {
        final Map<String, SnapshotId> allSnapshotIds = repositoryData.getSnapshotIds().stream().collect(
            Collectors.toMap(SnapshotId::getName, Function.identity()));
        final Set<SnapshotId> foundSnapshots = new HashSet<>();
        for (String snapshotOrPattern : snapshotsOrPatterns) {
            // 如果采用精确匹配的方式
            if (Regex.isSimpleMatchPattern(snapshotOrPattern) == false) {
                final SnapshotId foundId = allSnapshotIds.get(snapshotOrPattern);
                if (foundId == null) {
                    // 没有找到会抛出异常
                    throw new SnapshotMissingException(repositoryName, snapshotOrPattern);
                } else {
                    // 找到则加入到队列中
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

    /**
     * 通过快照名/仓库名 定位到某个具体的entry
     *
     * @param snapshots
     * @param snapshotName
     * @param repositoryName
     * @return
     */
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

    /**
     * 创建一个 会删除某些快照的更新任务
     * @param snapshotIds  本次会波及到的快照
     * @param repoName    关联的repository
     * @param repositoryStateId   repository对应的最新gen
     * @param timeout
     * @param priority
     * @param listener
     * @return
     */
    private ClusterStateUpdateTask createDeleteStateUpdate(List<SnapshotId> snapshotIds, String repoName, long repositoryStateId,
                                                           @Nullable TimeValue timeout, Priority priority, ActionListener<Void> listener) {
        // Short circuit to noop state update if there isn't anything to delete
        // 本次没有命中快照   不会修改clusterState
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

                // 删除快照任务也是串行执行
                if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(new Snapshot(repoName, snapshotIds.get(0)),
                        "cannot delete - another snapshot is currently being deleted in [" + deletionsInProgress + "]");
                }

                // 该任务不能与清理仓库同时进行
                final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(RepositoryCleanupInProgress.TYPE);
                if (repositoryCleanupInProgress != null && repositoryCleanupInProgress.hasCleanupInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(new Snapshot(repoName, snapshotIds.get(0)),
                        "cannot delete snapshots while a repository cleanup is in-progress in [" + repositoryCleanupInProgress + "]");
                }

                // 也不能与数据恢复同时进行
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
                // 不能与创建快照的进程冲突
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots != null && snapshots.entries().isEmpty() == false) {
                    // However other snapshots are running - cannot continue
                    throw new ConcurrentSnapshotExecutionException(
                        repoName, snapshotIds.toString(), "another snapshot is currently running cannot delete");
                }

                // 因为更新clusterState 是串行执行的 所以每个变化都可以察觉到之前的变化 就不会导致状态不一致 同时出现多个inProgress等情况了

                // add the snapshot deletion to the cluster state
                SnapshotDeletionsInProgress.Entry entry = new SnapshotDeletionsInProgress.Entry(
                    snapshotIds,
                    repoName,
                    threadPool.absoluteTimeInMillis(),
                    repositoryStateId
                );

                // 生成 InProgress并插入到 clusterState中
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

            /**
             * 当删除进程成功发布到集群后 从仓库中删除对应的快照数据
             * @param source
             * @param oldState
             * @param newState
             */
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
     * 通过版本号来判断是否需要将 shardGen 写入
     */
    public static boolean useShardGenerations(Version repositoryMetaVersion) {
        return repositoryMetaVersion.onOrAfter(SHARD_GEN_IN_REPO_DATA_VERSION);
    }

    /**
     * Deletes snapshot from repository
     *
     * @param repoName          repository name
     * @param snapshotIds       snapshot ids   本次要删除的所有快照id
     * @param listener          listener
     * @param repositoryStateId the unique id representing the state of the repository at the time the deletion began
     * @param minNodeVersion    minimum node version in the cluster
     *                          从仓库中删除之前的快照数据
     */
    private void deleteSnapshotsFromRepository(String repoName, Collection<SnapshotId> snapshotIds, @Nullable ActionListener<Void> listener,
                                               long repositoryStateId, Version minNodeVersion) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(listener, l -> {
            Repository repository = repositoriesService.repository(repoName);
            repository.getRepositoryData(ActionListener.wrap(repositoryData ->
                // 当找到仓库数据时 执行删除快照的操作
                repository.deleteSnapshots(snapshotIds,
                    repositoryStateId,
                    // 兼容性相关的先忽略
                    minCompatibleVersion(minNodeVersion, repoName, repositoryData, snapshotIds),
                    // 实际上无论成功与否 关键是要将本次删除进程从CS中移除 避免其他操作被阻塞 无法正常执行
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
     * @param repositoryData      当前repository的元数据信息  如果本次快照中的某些index不存在 此时还不会添加到 repositoryData中
     * @return list of shard to be included into current snapshot
     * 每次生成快照是有作用范围的  一般是指定对某些index生成快照  这里进一步细化到shard级别
     */
    private static ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards(ClusterState clusterState,
                                                                                             List<IndexId> indices,
                                                                                             boolean useShardGenerations,
                                                                                             RepositoryData repositoryData) {
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableOpenMap.builder();
        Metadata metadata = clusterState.metadata();
        // 这是之前的shardGen 信息
        final ShardGenerations shardGenerations = repositoryData.shardGenerations();

        // 遍历每个索引
        for (IndexId index : indices) {
            final String indexName = index.getName();
            // 代表之前 repository 并没有存储该index相关的数据
            final boolean isNewIndex = repositoryData.getIndices().containsKey(indexName) == false;
            IndexMetadata indexMetadata = metadata.index(indexName);
            if (indexMetadata == null) {
                // The index was deleted before we managed to start the snapshot - mark it as missing.
                // 这里插入了一个特殊的分片对象  分片id为0 且index元数据为  na_value  代表这个分片本身是不存在的
                builder.put(new ShardId(indexName, IndexMetadata.INDEX_UUID_NA_VALUE, 0),
                    new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.MISSING, "missing index", null));
            } else {
                IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(indexName);
                // 这里代表该index下有多少个shardId  实际上快照应该就是针对主分片来做
                for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                    ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
                    // shardGen 应该是代表某次快照对应的分片的状态id吧 便于在反查快照信息时快速定位
                    final String shardRepoGeneration;

                    // 如果本次索引是新建的 那么shardGen 都是一个统一值 _new_
                    if (useShardGenerations) {
                        if (isNewIndex) {
                            assert shardGenerations.getShardGen(index, shardId.getId()) == null
                                : "Found shard generation for new index [" + index + "]";
                            shardRepoGeneration = ShardGenerations.NEW_SHARD_GEN;
                        } else {
                            // 这里获取的shardGen信息是之前存储在 repositoryData中的
                            shardRepoGeneration = shardGenerations.getShardGen(index, shardId.getId());
                        }
                    } else {
                        shardRepoGeneration = null;
                    }

                    // 快照本身就应该只针对主分片 这里是检测主分片是否可用

                    if (indexRoutingTable != null) {
                        // 每个分片 还有主副关系 这里是只获取主分片
                        ShardRouting primary = indexRoutingTable.shard(i).primaryShard();
                        // 如果本次快照对应的某个index.shard 此时还未分配到某个节点上 添加一个失败的状态
                        if (primary == null || !primary.assignedToNode()) {
                            builder.put(shardId,
                                new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.MISSING, "primary shard is not allocated",
                                    shardRepoGeneration));
                            // 代表这时还无法为该分片生成快照信息 因为此时分片不可用 需要等待
                        } else if (primary.relocating() || primary.initializing()) {
                            builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(
                                primary.currentNodeId(), ShardState.WAITING, shardRepoGeneration));
                            // 除了 init/unassigned/relocation/start外应该没有其他情况了啊  其余情况都当作missing
                        } else if (!primary.started()) {
                            builder.put(shardId,
                                new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId(), ShardState.MISSING,
                                    "primary shard hasn't been started yet", shardRepoGeneration));
                        } else {
                            // 当主分片处于start状态时  可以针对该shard开启一个init的快照任务
                            builder.put(shardId,
                                new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId(), shardRepoGeneration));
                        }
                        // 当路由表不存在时 每个分片都标记成missing
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
     * @param indicesToCheck 将候选的索引中 正处于生成快照阶段的索引返回
     *                       注意这个检测只针对 partial 为 false的快照
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
     *                 当快照任务整个流程结束时 才触发该监听器
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

