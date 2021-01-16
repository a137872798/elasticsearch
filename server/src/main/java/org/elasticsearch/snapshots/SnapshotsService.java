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

                                // 如果当前任务已经被标记成禁止   比如在createSnapshot 与 beginSnapshot的时间差内 快照任务被关闭了
                                if (entry.state() == State.ABORTED) {
                                    entries.add(entry);
                                    assert entry.shards().isEmpty();
                                    hadAbortedInitializations = true;
                                } else {

                                    // 将快照任务拆解到index级别
                                    // 将 indexName 转换成 indexId
                                    final List<IndexId> indexIds = repositoryData.resolveNewIndices(indices);
                                    // Replace the snapshot that was just initialized
                                    // 以分片为单位提取快照信息  ShardSnapshotStatus 是根据对应主分片的状态来生成的
                                    ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards =
                                        shards(currentState, indexIds, useShardGenerations(version), repositoryData);

                                    // 如果要求非部分模式 就是指该索引下的所有分片.primary 都应该至少处于assigned的状态  并且每个索引对应的indexMetadata都应该是已知的
                                    // 如果是部分模式 仅会按照此时有效的分片生成快照数据
                                    if (!partial) {
                                        // v1 代表分片对应的索引没有在metadata中找到对应的indexMetadata
                                        // v2 代表indexMetadata 已经标记该index被关闭
                                        Tuple<Set<String>, Set<String>> indicesWithMissingShards = indicesWithMissingShards(shards,
                                            currentState.metadata());
                                        Set<String> missing = indicesWithMissingShards.v1();
                                        Set<String> closed = indicesWithMissingShards.v2();
                                        // 无法满足 !partial的条件
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
                                    // 允许部分模式 或者indices下的所有分片都处于 assigned状态 那么将快照修改成started状态
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
                         * 当entry的状态修改为started后 为什么不在这里直接处理所有shardId 是因为某些分片此时可能处于waiting状态
                         * 必须要等到所有shardId对应的state 都处于init状态后 才可以处理
                         * 所以处理的逻辑放在了  clusterStateApplier中执行 监听集群状态的变化 并在满足条件时真正开始生成快照
                         */

                        /**
                         * 当将快照任务启动的 clusterState 发布到集群中并成功后
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
                            // 这里实际上是将任务存储到 complete队列中
                            userCreateSnapshotListener.onResponse(snapshot.snapshot());

                            // 代表本次任务被终止了 触发endSnapshot 将快照从clusterState中移除
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
     * @param event
     */
    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            // leader节点才可以处理
            if (event.localNodeMaster()) {
                // We don't remove old master when master flips anymore. So, we need to check for change in master
                // 获取描述当前运行的快照信息的对象
                final SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);

                // 正常操作下 更新leader的任务不会修改快照元数据 这2个任务是彼此隔离的
                // 又因为在确定集群唯一的leader前无法提交其他执行其他更新clusterState的任务 所以所有节点的元数据信息肯定是相同的
                // leader节点在处理快照时 会将entry存储在initializingSnapshots中 而当leader发生变化 新leader对应的这个容器肯定是空的
                // 但是本次本节点选举成功不代表唯一确定了 可能本次有2个节点选举成功 还需要之后确认唯一的那个
                // 而被淘汰的那个leader无法执行其他更新clusterState的任务 所以实际上只有唯一的leader可以正常处理请求
                final boolean newMaster = event.previousState().nodes().isLocalNodeElectedMaster() == false;

                if (snapshotsInProgress != null) {
                    // 当检测到leader变化 或者某些参与生成快照的节点被移除了 就要将最新的集群状态发布到所有节点上
                    // 针对已启动的快照任务就是尝试将 下线节点对应的分片快照任务关闭
                    // 如果是init的快照任务 因为leader发生变化 在initializingSnapshots 中快照任务不存在 则关闭任务
                    if (newMaster || removedNodesCleanupNeeded(snapshotsInProgress, event.nodesDelta().removedNodes())) {
                        processSnapshotsOnRemovedNodes();
                    }

                    // 更新路由表的任务与更新leader的任务本身是无法同时执行的
                    // 路由表发生变化即是某些分片发生了重路由 找到了更合适的节点
                    if (event.routingTableChanged() && waitingShardsStartedOrUnassigned(snapshotsInProgress, event)) {
                        // 如果之前是waiting状态的某些shard 此时路由信息发生了变化 那么修改成 init状态
                        processStartedShards();
                    }
                    // Cleanup all snapshots that have no more work left:
                    // 1. Completed snapshots
                    // 2. Snapshots in state INIT that the previous master failed to start
                    // 3. Snapshots in any other state that have all their shard tasks completed
                    // 将某些满足条件的快照状态清除
                    snapshotsInProgress.entries().stream().filter(
                        entry ->
                            // 1.首先如果这个分片已经完成
                            // 2.其次如果快照已经从init容器中被移除  并且entry处于 init 或者已经完成的状态
                            entry.state().completed() || initializingSnapshots.contains(entry.snapshot()) == false && (entry.state() == State.INIT || completed(entry.shards().values()))
                        // 处理这些完成的快照操作
                    ).forEach(entry -> endSnapshot(entry, event.state().metadata()));
                }
                // 如果是本次刚晋升的leader节点  且集群中设置了删除快照的任务  那么要执行删除任务
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
     * 当本节点刚晋升成leader节点时触发
     */
    private void finalizeSnapshotDeletionFromPreviousMaster(ClusterState state) {
        // 如果此时 clusterState 中包含了 清理快照的相关信息 继续执行删除操作
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

                    // 针对已经启动的快照任务 或者被禁止的快照任务 需要找到本次移除的node
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
                            // 快照任务由 原本的副本未完成  因为node 的下线导致一些分片快照任务强制失败后  整个快照任务完成 此时标记成success
                            // TODO 那么什么情况下 快照任务会标记成失败
                            if (!snapshot.state().completed() && completed(shardsMap.values())) {
                                updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, State.SUCCESS, shardsMap);
                            } else {
                                // 保持原state
                                updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, snapshot.state(), shardsMap);
                            }
                        }
                        entries.add(updatedSnapshot);

                        // 当leader发生变化时 新leader的initializingSnapshots上并没有 原本存储的init的snapshot对象
                        // 这种情况下去除掉一开始提交的快照任务
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
     * 更新处于started状态的分片
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
                        // 只要要处于 started状态的才有处理的必要  代表通过了第一层校验
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
     * 找到内部处于waiting状态的分片 并进行处理
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
                    // 上面都是在查找 state == waiting 的分片
                    if (shardRouting != null && shardRouting.primaryShard() != null) {
                        // 代表可以结束等待状态
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
     * 由于某些分片的路由信息发生了变化
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
                            // 代表从waiting状态解除
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

                // 该对象描述了本次快照相关的所有索引的 分片信息
                final ShardGenerations shardGenerations = buildGenerations(entry, metadata);
                // 快照完成后 触发仓库的 finalize方法
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

                    // 通知用户本次快照任务执行失败了
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
        // 从哪个repository下查找快照
        final String repositoryName = request.repository();
        logger.info(() -> new ParameterizedMessage("deleting snapshots [{}] from repository [{}]",
            Strings.arrayToCommaDelimitedString(snapshotNames), repositoryName));

        clusterService.submitStateUpdateTask("delete snapshot", new ClusterStateUpdateTask(Priority.NORMAL) {

            /**
             * 包含 repository/snapshotId  方便定位哪个快照
             */
            Snapshot runningSnapshot;

            /**
             * 本次终止的快照任务是在 init阶段 还是started阶段
             */
            boolean abortedDuringInit = false;

            /**
             * 更新集群状态
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
                if (snapshotNames.length == 1) {
                    final String snapshotName = snapshotNames[0];
                    // 如果包含通配符 不允许删除
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

                // entry 相当于只是一个包装对象
                runningSnapshot = snapshotEntry.snapshot();
                final ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards;

                final State state = snapshotEntry.state();
                final String failure;
                // 代表此时快照任务还没有开始  并且还没有为每个shard生成对应的state(这个阶段会修改成started)
                if (state == State.INIT) {
                    // snapshot is still initializing, mark it as aborted
                    shards = snapshotEntry.shards();
                    assert shards.isEmpty();
                    failure = "Snapshot was aborted during initialization";
                    abortedDuringInit = true;

                    // 当外层的 Entry处于started状态时  是可以进行强制关闭的
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
                    // 此时应该已经产生结果了  (非init/started阶段)
                } else {
                    boolean hasUncompletedShards = false;
                    // Cleanup in case a node gone missing and snapshot wasn't updated for some reason
                    for (ObjectCursor<ShardSnapshotStatus> shardStatus : snapshotEntry.shards().values()) {
                        // Check if we still have shard running on existing nodes
                        // 照理说 如果entry产生了结果 那么每个state都应该是 completed 但是这里是false 就代表正在被终止
                        if (shardStatus.value.state().completed() == false && shardStatus.value.nodeId() != null
                            && currentState.nodes().get(shardStatus.value.nodeId()) != null) {
                            hasUncompletedShards = true;
                            break;
                        }
                    }
                    // 如果此时检测到存在 未完成的分片 那么很可能当前已经处于关闭中的状态了 比如已经接受到了一个aborted请求  那么不应该重复处理 只要等待结果即可
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
                // 将终止状态发布到集群中    被终止的具体分片可以通过检测 对应的shardState来识别
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
             * 当集群发布状态完成后触发  TODO  在集群范围内通知到其他节点会怎样呢 其他节点会配合着进行一些处理吗
             * @param source
             * @param oldState
             * @param newState
             */
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (runningSnapshot == null) {
                    try {
                        repositoriesService.repository(repositoryName).executeConsistentStateUpdate(
                            // 当执行executeConsistentStateUpdate时 会从最新的index-n 文件中还原 repositoryData 相当于是一个元数据
                            repositoryData ->
                                // 创建updateTask
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
     * 创建一个 clusterStateUpdateTask
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
        // 当没有快照受到影响 返回空对象
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

                // 当发起 deleteSnapshot的请求时   会创建SnapshotDeletionsInProgress对象
                SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);
                // 同一时间只允许存在一个 DeletionsInProgress.Entry 这个跟创建索引不一样   创建索引允许同时存在多个entry 但是要求状态必须是INIT
                if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(new Snapshot(repoName, snapshotIds.get(0)),
                        "cannot delete - another snapshot is currently being deleted in [" + deletionsInProgress + "]");
                }

                // 该任务也不能与 RepositoryCleanupInProgress 同时存在   TODO 这个entry是什么时候生成的  在 DeleteRepository任务中 只是更新了clusterState 并没有修改该对象
                final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(RepositoryCleanupInProgress.TYPE);
                if (repositoryCleanupInProgress != null && repositoryCleanupInProgress.hasCleanupInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(new Snapshot(repoName, snapshotIds.get(0)),
                        "cannot delete snapshots while a repository cleanup is in-progress in [" + repositoryCleanupInProgress + "]");
                }

                // 存储的进程不能与删除冲突
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
                // 创建 deletionsInProgress 插入到clusterState中 并发布到集群中

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
     * @param snapshotIds       snapshot ids   本次要删除的所有快照id
     * @param listener          listener
     * @param repositoryStateId the unique id representing the state of the repository at the time the deletion began
     * @param minNodeVersion    minimum node version in the cluster
     *                          执行删除快照的操作
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
     * @param repositoryData      当前repository的元数据信息
     * @return list of shard to be included into current snapshot
     * ShardSnapshotStatus 对应索引下每个shardId对应的主分片状态  比如当前是否处于运行状态 只有处于运行状态时 才应该生成快照
     */
    private static ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards(ClusterState clusterState,
                                                                                             List<IndexId> indices,
                                                                                             boolean useShardGenerations,
                                                                                             RepositoryData repositoryData) {
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableOpenMap.builder();
        Metadata metadata = clusterState.metadata();
        // 这里存储了此时所有分片的gen
        final ShardGenerations shardGenerations = repositoryData.shardGenerations();

        // 遍历每个索引
        for (IndexId index : indices) {
            final String indexName = index.getName();
            // 代表之前 repository 并没有存储该index相关的数据
            final boolean isNewIndex = repositoryData.getIndices().containsKey(indexName) == false;
            IndexMetadata indexMetadata = metadata.index(indexName);
            if (indexMetadata == null) {
                // The index was deleted before we managed to start the snapshot - mark it as missing.
                // TODO 这是一个哨兵对象吧  因为没有indexMetadata 实际上是异常情况
                builder.put(new ShardId(indexName, IndexMetadata.INDEX_UUID_NA_VALUE, 0),
                    new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.MISSING, "missing index", null));
            } else {
                IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(indexName);
                // 这里代表该index下有多少个分片 (不考虑副本)
                for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                    ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
                    // 有关分片的gen
                    final String shardRepoGeneration;
                    if (useShardGenerations) {
                        if (isNewIndex) {
                            assert shardGenerations.getShardGen(index, shardId.getId()) == null
                                : "Found shard generation for new index [" + index + "]";
                            // 某个本次新建的shard 使用的generation为 new_shard
                            shardRepoGeneration = ShardGenerations.NEW_SHARD_GEN;
                        } else {
                            // 从 repositoryData 中查找该shard之前的 shardGen
                            shardRepoGeneration = shardGenerations.getShardGen(index, shardId.getId());
                        }
                    } else {
                        shardRepoGeneration = null;
                    }

                    // 以上只是为index的每个shard 生成 shardGen

                    if (indexRoutingTable != null) {
                        // 每个分片 还有主副关系 这里是只获取主分片
                        ShardRouting primary = indexRoutingTable.shard(i).primaryShard();
                        if (primary == null || !primary.assignedToNode()) {
                            builder.put(shardId,
                                new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.MISSING, "primary shard is not allocated",
                                    shardRepoGeneration));
                            // 代表这时还无法为该分片生成快照信息 因为此时分片不可用 需要等待
                        } else if (primary.relocating() || primary.initializing()) {
                            builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(
                                primary.currentNodeId(), ShardState.WAITING, shardRepoGeneration));
                            // 处于未分配状态
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

