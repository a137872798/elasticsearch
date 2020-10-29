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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.RestoreInProgress.ShardRestoreStatus;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_UPGRADED;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.snapshots.SnapshotUtils.filterIndices;

/**
 * Service responsible for restoring snapshots
 * <p>
 * Restore operation is performed in several stages.
 * <p>
 * First {@link #restoreSnapshot(RestoreSnapshotRequest, org.elasticsearch.action.ActionListener)}
 * method reads information about snapshot and metadata from repository. In update cluster state task it checks restore
 * preconditions, restores global state if needed, creates {@link RestoreInProgress} record with list of shards that needs
 * to be restored and adds this shard to the routing table using
 * {@link RoutingTable.Builder#addAsRestore(IndexMetadata, SnapshotRecoverySource)} method.
 * <p>
 * Individual shards are getting restored as part of normal recovery process in
 * {@link IndexShard#restoreFromRepository} )}
 * method, which detects that shard should be restored from snapshot rather than recovered from gateway by looking
 * at the {@link ShardRouting#recoverySource()} property.
 * <p>
 * At the end of the successful restore process {@code RestoreService} calls {@link #cleanupRestoreState(ClusterChangedEvent)},
 * which removes {@link RestoreInProgress} when all shards are completed. In case of
 * restore failure a normal recovery fail-over process kicks in.
 *
 * 恢复服务会监听集群状态的变化
 */
public class RestoreService implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(RestoreService.class);

    /**
     * 有关无法修改的配置
     */
    private static final Set<String> UNMODIFIABLE_SETTINGS = unmodifiableSet(newHashSet(
            SETTING_NUMBER_OF_SHARDS,
            SETTING_VERSION_CREATED,
            SETTING_INDEX_UUID,
            SETTING_CREATION_DATE,
            IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey()));

    // It's OK to change some settings, but we shouldn't allow simply removing them
    // 有些配置是允许被移除的 因为在处理restoreReq中 用户可以指定移除掉一些配置
    private static final Set<String> UNREMOVABLE_SETTINGS;

    static {
        Set<String> unremovable = new HashSet<>(UNMODIFIABLE_SETTINGS.size() + 4);
        unremovable.addAll(UNMODIFIABLE_SETTINGS);
        unremovable.add(SETTING_NUMBER_OF_REPLICAS);
        unremovable.add(SETTING_AUTO_EXPAND_REPLICAS);
        unremovable.add(SETTING_VERSION_UPGRADED);
        UNREMOVABLE_SETTINGS = unmodifiableSet(unremovable);
    }


    private final ClusterService clusterService;

    /**
     * 存储服务
     */
    private final RepositoriesService repositoriesService;

    /**
     * 该服务能够为分片分配相关的节点
     */
    private final AllocationService allocationService;

    /**
     * 创建索引的服务
     * 在进行恢复时 可以将之前的某个index 进行rename 这时就要创建新索引
     */
    private final MetadataCreateIndexService createIndexService;

    /**
     * 更新索引的服务
     */
    private final MetadataIndexUpgradeService metadataIndexUpgradeService;

    private final ClusterSettings clusterSettings;

    private final CleanRestoreStateTaskExecutor cleanRestoreStateTaskExecutor;

    public RestoreService(ClusterService clusterService, RepositoriesService repositoriesService,
                          AllocationService allocationService, MetadataCreateIndexService createIndexService,
                          MetadataIndexUpgradeService metadataIndexUpgradeService, ClusterSettings clusterSettings) {
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;
        this.metadataIndexUpgradeService = metadataIndexUpgradeService;
        // 将自己注册到集群服务对象上 这样就可以监听集群的变化
        clusterService.addStateApplier(this);
        this.clusterSettings = clusterSettings;
        this.cleanRestoreStateTaskExecutor = new CleanRestoreStateTaskExecutor();
    }

    /**
     * Restores snapshot specified in the restore request.
     *
     * @param request  restore request
     * @param listener restore listener
     *                 发送从快照中恢复数据的请求
     */
    public void restoreSnapshot(final RestoreSnapshotRequest request, final ActionListener<RestoreCompletionResponse> listener) {
        try {
            // Read snapshot info and metadata from the repository
            // 获取存储实例
            final String repositoryName = request.repository();
            Repository repository = repositoriesService.repository(repositoryName);
            final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
            // RepositoryData 描述当前存储实例内部存放了哪些indices/shard 信息
            repository.getRepositoryData(repositoryDataListener);
            repositoryDataListener.whenComplete(repositoryData -> {

                final String snapshotName = request.snapshot();
                // 如果当前存储实例中并没有存该快照相关的信息
                final Optional<SnapshotId> matchingSnapshotId = repositoryData.getSnapshotIds().stream()
                    .filter(s -> snapshotName.equals(s.getName())).findFirst();
                if (matchingSnapshotId.isPresent() == false) {
                    throw new SnapshotRestoreException(repositoryName, snapshotName, "snapshot does not exist");
                }

                final SnapshotId snapshotId = matchingSnapshotId.get();
                // 获取该快照的详细信息
                final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
                final Snapshot snapshot = new Snapshot(repositoryName, snapshotId);

                // Make sure that we can restore from this snapshot
                // 检验能否从该快照中还原数据
                validateSnapshotRestorable(repositoryName, snapshotInfo);

                // Resolve the indices from the snapshot that need to be restored
                // 找到本次期望恢复的几个索引
                final List<String> indicesInSnapshot = filterIndices(snapshotInfo.indices(), request.indices(), request.indicesOptions());

                final Metadata.Builder metadataBuilder;
                // 如果本次请求包含全局状态  以globalState作为起始模板
                if (request.includeGlobalState()) {
                    metadataBuilder = Metadata.builder(repository.getSnapshotGlobalMetadata(snapshotId));
                } else {
                    metadataBuilder = Metadata.builder();
                }

                // 每个快照名都对应一个 indexId对象
                final List<IndexId> indexIdsInSnapshot = repositoryData.resolveIndices(indicesInSnapshot);
                for (IndexId indexId : indexIdsInSnapshot) {
                    // 将相关的索引元数据设置到 metadata中
                    metadataBuilder.put(repository.getSnapshotIndexMetadata(snapshotId, indexId), false);
                }

                final Metadata metadata = metadataBuilder.build();

                // Apply renaming on index names, returning a map of names where
                // the key is the renamed index and the value is the original name
                // 将这组索引进行重命名  key 对应renamed 后的索引 value对应原名字
                final Map<String, String> indices = renamedIndices(request, indicesInSnapshot);

                // Now we can start the actual restore process by adding shards to be recovered in the cluster state
                // and updating cluster metadata (global and index) as needed
                // TODO 推测是集群中任意一个节点都可以发起restore请求  这时首先会转发到leader节点 并根据leader存储的最新元数据处理后 将任务再转发到集群其他节点上
                clusterService.submitStateUpdateTask("restore_snapshot[" + snapshotName + ']', new ClusterStateUpdateTask() {

                    /**
                     * 每次执行restore时 会分配一个随机id
                     */
                    final String restoreUUID = UUIDs.randomBase64UUID();
                    RestoreInfo restoreInfo = null;

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        // 获取此时的 恢复进度对象 每次恢复应该都会被封装成一个entry对象 并填充到 RestoreInProgress 中
                        RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE);
                        // Check if the snapshot to restore is currently being deleted
                        SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);

                        // 如果此时正好要针对这个快照进行删除操作  抛出异常  TODO 如果在之后执行删除操作了呢  根据什么条件来检测冲突
                        if (deletionsInProgress != null
                            && deletionsInProgress.getEntries().stream().anyMatch(entry -> entry.getSnapshots().contains(snapshotId))) {
                            throw new ConcurrentSnapshotExecutionException(snapshot,
                                "cannot restore a snapshot while a snapshot deletion is in-progress [" +
                                    deletionsInProgress.getEntries().get(0) + "]");
                        }

                        // Updating cluster state
                        // 先将之前CS的各个数据拆出来生成对应的builder对象
                        ClusterState.Builder builder = ClusterState.builder(currentState);
                        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                        RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
                        ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards;
                        Set<String> aliases = new HashSet<>();

                        if (indices.isEmpty() == false) {
                            // We have some indices to restore
                            ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> shardsBuilder =
                                ImmutableOpenMap.builder();
                            // 获取最小的兼容版本
                            final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion()
                                .minimumIndexCompatibilityVersion();
                            for (Map.Entry<String, String> indexEntry : indices.entrySet()) {
                                // 使用未rename的索引
                                String index = indexEntry.getValue();

                                // 当前索引是否是部分的
                                boolean partial = checkPartial(index);

                                // 将本次恢复的相关数据整合成一个 source对象   比如涉及到了哪个快照 针对的索引id
                                SnapshotRecoverySource recoverySource = new SnapshotRecoverySource(restoreUUID, snapshot,
                                    snapshotInfo.version(), repositoryData.resolveIndexId(index));
                                // rename后的索引名
                                String renamedIndexName = indexEntry.getKey();
                                IndexMetadata snapshotIndexMetadata = metadata.index(index);

                                // 请求体中可能包含某些所有相关的配置 可以用来更新索引
                                snapshotIndexMetadata = updateIndexSettings(snapshotIndexMetadata,
                                    request.indexSettings(), request.ignoreIndexSettings());
                                try {
                                    // 这里还要进行升级 应该是处理兼容性问题的  先忽略
                                    snapshotIndexMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(snapshotIndexMetadata,
                                        minIndexCompatibilityVersion);
                                } catch (Exception ex) {
                                    throw new SnapshotRestoreException(snapshot, "cannot restore index [" + index +
                                        "] because it cannot be upgraded", ex);
                                }
                                // Check that the index is closed or doesn't exist
                                // 寻找重命名后的索引对应的元数据
                                IndexMetadata currentIndexMetadata = currentState.metadata().index(renamedIndexName);
                                IntSet ignoreShards = new IntHashSet();
                                final Index renamedIndex;
                                if (currentIndexMetadata == null) {
                                    // Index doesn't exist - create it and start recovery
                                    // Make sure that the index we are about to create has a validate name
                                    // 根据当前索引的元数据来决定是否要进行 hidden  啥意思 ???
                                    boolean isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(snapshotIndexMetadata.getSettings());
                                    createIndexService.validateIndexName(renamedIndexName, currentState);
                                    // TODO
                                    createIndexService.validateDotIndex(renamedIndexName, currentState, isHidden);
                                    // 校验索引配置是否有效  先忽略吧
                                    createIndexService.validateIndexSettings(renamedIndexName, snapshotIndexMetadata.getSettings(), false);

                                    // 为新的index构建一个metadata 对象  并将索引标记成打开状态
                                    IndexMetadata.Builder indexMdBuilder = IndexMetadata.builder(snapshotIndexMetadata)
                                        .state(IndexMetadata.State.OPEN)
                                        .index(renamedIndexName);
                                    // 在设置settings时 在原有的基础上增加了一个 index-uuid
                                    indexMdBuilder.settings(Settings.builder()
                                        .put(snapshotIndexMetadata.getSettings())
                                        .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()));

                                    // 因为即将要创建一个新的index了 会根据此时的情况判断创建多少分片  根据当前集群此时承载的分片数量 与最大承载能力判断 是否超过分片数量限制
                                    MetadataCreateIndexService.checkShardLimit(snapshotIndexMetadata.getSettings(), currentState);
                                    // 如果新创建的索引metadata中不需要别名信息 进行移除
                                    if (!request.includeAliases() && !snapshotIndexMetadata.getAliases().isEmpty()) {
                                        // Remove all aliases - they shouldn't be restored
                                        indexMdBuilder.removeAllAliases();
                                    } else {

                                        // 否则将当前元数据中的所有别名取一份出来存到 aliases容器中
                                        for (ObjectCursor<String> alias : snapshotIndexMetadata.getAliases().keys()) {
                                            aliases.add(alias.value);
                                        }
                                    }
                                    IndexMetadata updatedIndexMetadata = indexMdBuilder.build();
                                    // 如果使用分段方式
                                    if (partial) {
                                        // 将当前快照信息中的 failure信息中 index匹配的数据 添加到ignoreShards 中
                                        populateIgnoredShards(index, ignoreShards);
                                    }
                                    // 路由表中也需要追加新的信息
                                    rtBuilder.addAsNewRestore(updatedIndexMetadata, recoverySource, ignoreShards);
                                    // block也需要追加新的信息
                                    blocks.addBlocks(updatedIndexMetadata);
                                    // 更新当前索引元数据
                                    mdBuilder.put(updatedIndexMetadata, true);
                                    renamedIndex = updatedIndexMetadata.getIndex();
                                } else {

                                    // 如果rename后的索引之前就存在  忽略校验
                                    validateExistingIndex(currentIndexMetadata, snapshotIndexMetadata, renamedIndexName, partial);
                                    // Index exists and it's closed - open it in metadata and start recovery
                                    // 将索引状态修改成打开 并增加版本号
                                    IndexMetadata.Builder indexMdBuilder =
                                        IndexMetadata.builder(snapshotIndexMetadata).state(IndexMetadata.State.OPEN);
                                    indexMdBuilder.version(
                                        Math.max(snapshotIndexMetadata.getVersion(), 1 + currentIndexMetadata.getVersion()));
                                    indexMdBuilder.mappingVersion(
                                        Math.max(snapshotIndexMetadata.getMappingVersion(), 1 + currentIndexMetadata.getMappingVersion()));
                                    indexMdBuilder.settingsVersion(
                                        Math.max(
                                            snapshotIndexMetadata.getSettingsVersion(),
                                            1 + currentIndexMetadata.getSettingsVersion()));
                                    indexMdBuilder.aliasesVersion(
                                        Math.max(snapshotIndexMetadata.getAliasesVersion(), 1 + currentIndexMetadata.getAliasesVersion()));

                                    for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
                                        indexMdBuilder.primaryTerm(shard,
                                            Math.max(snapshotIndexMetadata.primaryTerm(shard), currentIndexMetadata.primaryTerm(shard)));
                                    }

                                    // 代表需要移除掉别名 就进行移除
                                    if (!request.includeAliases()) {
                                        // Remove all snapshot aliases
                                        if (!snapshotIndexMetadata.getAliases().isEmpty()) {
                                            indexMdBuilder.removeAllAliases();
                                        }
                                        // Add existing aliases
                                        // 追加rename 后的索引对应的别名
                                        for (ObjectCursor<AliasMetadata> alias : currentIndexMetadata.getAliases().values()) {
                                            indexMdBuilder.putAlias(alias.value);
                                        }
                                    } else {
                                        for (ObjectCursor<String> alias : snapshotIndexMetadata.getAliases().keys()) {
                                            aliases.add(alias.value);
                                        }
                                    }
                                    // 追加uuid的配置
                                    indexMdBuilder.settings(Settings.builder()
                                        .put(snapshotIndexMetadata.getSettings())
                                        .put(IndexMetadata.SETTING_INDEX_UUID,
                                            currentIndexMetadata.getIndexUUID()));
                                    IndexMetadata updatedIndexMetadata = indexMdBuilder.index(renamedIndexName).build();
                                    rtBuilder.addAsRestore(updatedIndexMetadata, recoverySource);
                                    blocks.updateBlocks(updatedIndexMetadata);
                                    mdBuilder.put(updatedIndexMetadata, true);
                                    renamedIndex = updatedIndexMetadata.getIndex();
                                }

                                for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
                                    if (!ignoreShards.contains(shard)) {
                                        shardsBuilder.put(new ShardId(renamedIndex, shard),
                                            new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId()));
                                    } else {
                                        // 如果该分片是被忽略的分片  插入一个无效的restoreStatus
                                        shardsBuilder.put(new ShardId(renamedIndex, shard),
                                            new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId(),
                                                RestoreInProgress.State.FAILURE));
                                    }
                                }
                            }

                            // 将entry 追加到inProgress中
                            shards = shardsBuilder.build();
                            RestoreInProgress.Entry restoreEntry = new RestoreInProgress.Entry(
                                restoreUUID, snapshot, overallState(RestoreInProgress.State.INIT, shards),
                                List.copyOf(indices.keySet()),
                                shards
                            );
                            RestoreInProgress.Builder restoreInProgressBuilder;
                            if (restoreInProgress != null) {
                                restoreInProgressBuilder = new RestoreInProgress.Builder(restoreInProgress);
                            } else {
                                restoreInProgressBuilder = new RestoreInProgress.Builder();
                            }
                            builder.putCustom(RestoreInProgress.TYPE, restoreInProgressBuilder.add(restoreEntry).build());
                        } else {
                            shards = ImmutableOpenMap.of();
                        }

                        checkAliasNameConflicts(indices, aliases);

                        // Restore global state if needed
                        if (request.includeGlobalState()) {
                            if (metadata.persistentSettings() != null) {
                                // 这个是检测配置是否需要更新么  为什么需要检测 此时获取到的不是新配置么  配置变化时不会主动更新配置么
                                Settings settings = metadata.persistentSettings();
                                clusterSettings.validateUpdate(settings);
                                mdBuilder.persistentSettings(settings);
                            }

                            // 下面就是配置的迁移
                            if (metadata.templates() != null) {
                                // TODO: Should all existing templates be deleted first?
                                for (ObjectCursor<IndexTemplateMetadata> cursor : metadata.templates().values()) {
                                    mdBuilder.put(cursor.value);
                                }
                            }
                            if (metadata.customs() != null) {
                                for (ObjectObjectCursor<String, Metadata.Custom> cursor : metadata.customs()) {
                                    if (!RepositoriesMetadata.TYPE.equals(cursor.key)) {
                                        // Don't restore repositories while we are working with them
                                        // TODO: Should we restore them at the end?
                                        mdBuilder.putCustom(cursor.key, cursor.value);
                                    }
                                }
                            }
                        }

                        // 如果所有 restoreStatus 都为completed 生成一个 restoreInfo 对象
                        if (completed(shards)) {
                            // We don't have any indices to restore - we are done
                            restoreInfo = new RestoreInfo(snapshotId.getName(),
                                List.copyOf(indices.keySet()),
                                shards.size(),
                                shards.size() - failedShards(shards));
                        }

                        // 因为路由表发生了变化 所以要进行重路由
                        RoutingTable rt = rtBuilder.build();
                        ClusterState updatedState = builder.metadata(mdBuilder).blocks(blocks).routingTable(rt).build();
                        return allocationService.reroute(updatedState, "restored snapshot [" + snapshot + "]");
                    }

                    private void checkAliasNameConflicts(Map<String, String> renamedIndices, Set<String> aliases) {
                        for (Map.Entry<String, String> renamedIndex : renamedIndices.entrySet()) {
                            if (aliases.contains(renamedIndex.getKey())) {
                                throw new SnapshotRestoreException(snapshot,
                                    "cannot rename index [" + renamedIndex.getValue() + "] into [" + renamedIndex.getKey()
                                        + "] because of conflict with an alias with the same name");
                            }
                        }
                    }

                    /**
                     * 获取此时快照所有的失败信息 当某条index匹配成功时 将分片id 设置到容器中
                     * @param index
                     * @param ignoreShards
                     */
                    private void populateIgnoredShards(String index, IntSet ignoreShards) {
                        for (SnapshotShardFailure failure : snapshotInfo.shardFailures()) {
                            if (index.equals(failure.index())) {
                                ignoreShards.add(failure.shardId());
                            }
                        }
                    }

                    /**
                     * @param index
                     * @return
                     */
                    private boolean checkPartial(String index) {
                        // Make sure that index was fully snapshotted
                        // 看来如果某个索引本身是partial  那么在生成快照是必然会产生几个 failure???
                        if (failed(snapshotInfo, index)) {
                            if (request.partial()) {
                                return true;
                            } else {
                                throw new SnapshotRestoreException(snapshot, "index [" + index + "] wasn't fully snapshotted - cannot " +
                                    "restore");
                            }
                        } else {
                            return false;
                        }
                    }

                    /**
                     *
                     * @param currentIndexMetadata  重命名的index 对应的元数据
                     * @param snapshotIndexMetadata   重命名前的index 对应的元数据
                     * @param renamedIndex   重命名后的名字
                     * @param partial    未重命名前的index  是否是部分数据
                     */
                    private void validateExistingIndex(IndexMetadata currentIndexMetadata, IndexMetadata snapshotIndexMetadata,
                        String renamedIndex, boolean partial) {
                        // Index exist - checking that it's closed
                        // 要求之前存在的索引必须处于关闭状态  为什么???
                        if (currentIndexMetadata.getState() != IndexMetadata.State.CLOSE) {
                            // TODO: Enable restore for open indices
                            throw new SnapshotRestoreException(snapshot, "cannot restore index [" + renamedIndex
                                + "] because an open index " +
                                "with same name already exists in the cluster. Either close or delete the existing index or restore the " +
                                "index under a different name by providing a rename pattern and replacement name");
                        }
                        // Index exist - checking if it's partial restore
                        // 什么是部分索引
                        if (partial) {
                            throw new SnapshotRestoreException(snapshot, "cannot restore partial index [" + renamedIndex
                                + "] because such index already exists");
                        }
                        // Make sure that the number of shards is the same. That's the only thing that we cannot change
                        if (currentIndexMetadata.getNumberOfShards() != snapshotIndexMetadata.getNumberOfShards()) {
                            throw new SnapshotRestoreException(snapshot,
                                "cannot restore index [" + renamedIndex + "] with [" + currentIndexMetadata.getNumberOfShards()
                                    + "] shards from a snapshot of index [" + snapshotIndexMetadata.getIndex().getName() + "] with [" +
                                    snapshotIndexMetadata.getNumberOfShards() + "] shards");
                        }
                    }

                    /**
                     * Optionally updates index settings in indexMetadata by removing settings listed in ignoreSettings and
                     * merging them with settings in changeSettings.
                     * 更新索引配置
                     */
                    private IndexMetadata updateIndexSettings(IndexMetadata indexMetadata, Settings changeSettings,
                                                              String[] ignoreSettings) {
                        // 将本次change的所有配置追加上 index. 前缀
                        Settings normalizedChangeSettings = Settings.builder()
                            .put(changeSettings)
                            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
                            .build();
                        IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
                        Settings settings = indexMetadata.getSettings();
                        // 当完全命中时 才移除
                        Set<String> keyFilters = new HashSet<>();
                        // 内部的字符串会包含* 也就是可以灵活匹配一些配置
                        List<String> simpleMatchPatterns = new ArrayList<>();
                        for (String ignoredSetting : ignoreSettings) {
                            // 代表setting中没有 通配符 这种处理比较简单 只有完全匹配时才命中
                            if (!Regex.isSimpleMatchPattern(ignoredSetting)) {
                                // 如果本次请求中 尝试移除某条不允许被移除的setting项 抛出异常
                                if (UNREMOVABLE_SETTINGS.contains(ignoredSetting)) {
                                    throw new SnapshotRestoreException(
                                        snapshot, "cannot remove setting [" + ignoredSetting + "] on restore");
                                } else {
                                    keyFilters.add(ignoredSetting);
                                }
                            } else {
                                simpleMatchPatterns.add(ignoredSetting);
                            }
                        }
                        Predicate<String> settingsFilter = k -> {
                            // 如果是 unremovable配置 直接通过
                            if (UNREMOVABLE_SETTINGS.contains(k) == false) {

                                // 进行精准匹配
                                for (String filterKey : keyFilters) {
                                    if (k.equals(filterKey)) {
                                        return false;
                                    }
                                }

                                // 进行正则匹配
                                for (String pattern : simpleMatchPatterns) {
                                    if (Regex.simpleMatch(pattern, k)) {
                                        return false;
                                    }
                                }
                            }
                            return true;
                        };
                        Settings.Builder settingsBuilder = Settings.builder()
                            // 在原配置中 只有符合过滤条件的才能被填充到settings中
                            .put(settings.filter(settingsFilter))
                            // 本次设置的配置会覆盖之前的
                            .put(normalizedChangeSettings.filter(k -> {
                                if (UNMODIFIABLE_SETTINGS.contains(k)) {
                                    throw new SnapshotRestoreException(snapshot, "cannot modify setting [" + k + "] on restore");
                                } else {
                                    return true;
                                }
                            }));
                        // 注意这里移除掉了一个固定的配置 VERIFIED_BEFORE_CLOSE_SETTING
                        settingsBuilder.remove(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey());
                        return builder.settings(settingsBuilder).build();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.warn(() -> new ParameterizedMessage("[{}] failed to restore snapshot", snapshotId), e);
                        listener.onFailure(e);
                    }

                    @Override
                    public TimeValue timeout() {
                        return request.masterNodeTimeout();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        listener.onResponse(new RestoreCompletionResponse(restoreUUID, snapshot, restoreInfo));
                    }
                });
            }, listener::onFailure);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to restore snapshot",
                request.repository() + ":" + request.snapshot()), e);
            listener.onFailure(e);
        }
    }

    /**
     * 更新restoreState
     * @param oldRestore
     * @param deletedIndices
     * @return
     */
    public static RestoreInProgress updateRestoreStateWithDeletedIndices(RestoreInProgress oldRestore, Set<Index> deletedIndices) {
        boolean changesMade = false;
        RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
        for (RestoreInProgress.Entry entry : oldRestore) {
            ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = null;
            // entry 代表一次恢复操作 每个entry下有一组 status 每个对象又对应在哪个节点上进行恢复
            for (ObjectObjectCursor<ShardId, ShardRestoreStatus> cursor : entry.shards()) {
                ShardId shardId = cursor.key;
                // 如果这个索引被包含在删除列表中
                if (deletedIndices.contains(shardId.getIndex())) {
                    changesMade = true;
                    if (shardsBuilder == null) {
                        shardsBuilder = ImmutableOpenMap.builder(entry.shards());
                    }
                    // 将命中的分片标记成失败
                    shardsBuilder.put(shardId,
                        new ShardRestoreStatus(null, RestoreInProgress.State.FAILURE, "index was deleted"));
                }
            }
            // 代表有某些数据命中删除条件被移除
            if (shardsBuilder != null) {
                ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = shardsBuilder.build();
                builder.add(new RestoreInProgress.Entry(entry.uuid(), entry.snapshot(),
                    overallState(RestoreInProgress.State.STARTED, shards), entry.indices(), shards));
            } else {
                builder.add(entry);
            }
        }
        if (changesMade) {
            return builder.build();
        } else {
            return oldRestore;
        }
    }

    /**
     * 某次恢复请求的响应结果
     */
    public static final class RestoreCompletionResponse {
        private final String uuid;

        /**
         * 本次请求时指定的快照
         */
        private final Snapshot snapshot;
        /**
         * 本次恢复涉及到多少个索引 以及分片 并且有多少分片恢复成功了
         */
        private final RestoreInfo restoreInfo;

        private RestoreCompletionResponse(final String uuid, final Snapshot snapshot, final RestoreInfo restoreInfo) {
            this.uuid = uuid;
            this.snapshot = snapshot;
            this.restoreInfo = restoreInfo;
        }

        public String getUuid() {
            return uuid;
        }

        public Snapshot getSnapshot() {
            return snapshot;
        }

        public RestoreInfo getRestoreInfo() {
            return restoreInfo;
        }
    }

    /**
     * 记录某个分片此时的恢复状态
     */
    public static class RestoreInProgressUpdater extends RoutingChangesObserver.AbstractRoutingChangesObserver {
        // Map of RestoreUUID to a of changes to the shards' restore statuses
        // key是每个恢复源的id   ShardRestoreStatus 对应此时主分片的恢复状态   只有主分片才有恢复的概念么???
        private final Map<String, Map<ShardId, ShardRestoreStatus>> shardChanges = new HashMap<>();

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
            // mark snapshot as completed
            // 某个主分片由初始状态变化为 启动状态时
            if (initializingShard.primary()) {
                RecoverySource recoverySource = initializingShard.recoverySource();
                // 如果要求分片从快照中恢复数据  这里直接插入一个success的status对象
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    changes(recoverySource).put(
                        initializingShard.shardId(),
                        new ShardRestoreStatus(initializingShard.currentNodeId(), RestoreInProgress.State.SUCCESS));
                }
            }
        }

        /**
         * 如果某个分片分配失败了
         * @param failedShard
         * @param unassignedInfo
         */
        @Override
        public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
            // 当主分片在初始阶段启动时失败了
            if (failedShard.primary() && failedShard.initializing()) {
                // 如果是基于快照模式
                RecoverySource recoverySource = failedShard.recoverySource();
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    // mark restore entry for this shard as failed when it's due to a file corruption. There is no need wait on retries
                    // to restore this shard on another node if the snapshot files are corrupt. In case where a node just left or crashed,
                    // however, we only want to acknowledge the restore operation once it has been successfully restored on another node.
                    if (unassignedInfo.getFailure() != null && Lucene.isCorruptionException(unassignedInfo.getFailure().getCause())) {
                        changes(recoverySource).put(
                            failedShard.shardId(), new ShardRestoreStatus(failedShard.currentNodeId(),
                                RestoreInProgress.State.FAILURE, unassignedInfo.getFailure().getCause().getMessage()));
                    }
                }
            }
        }

        @Override
        public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
            // if we force an empty primary, we should also fail the restore entry
            if (unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT &&
                initializedShard.recoverySource().getType() != RecoverySource.Type.SNAPSHOT) {
                changes(unassignedShard.recoverySource()).put(
                    unassignedShard.shardId(),
                    new ShardRestoreStatus(null, RestoreInProgress.State.FAILURE,
                        "recovery source type changed from snapshot to " + initializedShard.recoverySource())
                );
            }
        }

        /**
         * 当未分配信息发生变化时触发
         * @param unassignedShard
         * @param newUnassignedInfo
         */
        @Override
        public void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {
            RecoverySource recoverySource = unassignedShard.recoverySource();
            if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                if (newUnassignedInfo.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO) {
                    String reason = "shard could not be allocated to any of the nodes";
                    changes(recoverySource).put(
                        unassignedShard.shardId(),
                        new ShardRestoreStatus(unassignedShard.currentNodeId(), RestoreInProgress.State.FAILURE, reason));
                }
            }
        }

        /**
         * Helper method that creates update entry for the given recovery source's restore uuid
         * if such an entry does not exist yet.
         */
        private Map<ShardId, ShardRestoreStatus> changes(RecoverySource recoverySource) {
            assert recoverySource.getType() == RecoverySource.Type.SNAPSHOT;
            return shardChanges.computeIfAbsent(((SnapshotRecoverySource) recoverySource).restoreUUID(), k -> new HashMap<>());
        }

        public RestoreInProgress applyChanges(final RestoreInProgress oldRestore) {
            if (shardChanges.isEmpty() == false) {
                RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
                for (RestoreInProgress.Entry entry : oldRestore) {
                    Map<ShardId, ShardRestoreStatus> updates = shardChanges.get(entry.uuid());
                    ImmutableOpenMap<ShardId, ShardRestoreStatus> shardStates = entry.shards();
                    if (updates != null && updates.isEmpty() == false) {
                        ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = ImmutableOpenMap.builder(shardStates);
                        for (Map.Entry<ShardId, ShardRestoreStatus> shard : updates.entrySet()) {
                            ShardId shardId = shard.getKey();
                            ShardRestoreStatus status = shardStates.get(shardId);
                            if (status == null || status.state().completed() == false) {
                                shardsBuilder.put(shardId, shard.getValue());
                            }
                        }

                        ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = shardsBuilder.build();
                        RestoreInProgress.State newState = overallState(RestoreInProgress.State.STARTED, shards);
                        builder.add(new RestoreInProgress.Entry(entry.uuid(), entry.snapshot(), newState, entry.indices(), shards));
                    } else {
                        builder.add(entry);
                    }
                }
                return builder.build();
            } else {
                return oldRestore;
            }
        }
    }

    public static RestoreInProgress.Entry restoreInProgress(ClusterState state, String restoreUUID) {
        final RestoreInProgress restoreInProgress = state.custom(RestoreInProgress.TYPE);
        if (restoreInProgress != null) {
            return restoreInProgress.get(restoreUUID);
        }
        return null;
    }

    /**
     * 清理恢复状态的任务
     */
    static class CleanRestoreStateTaskExecutor implements ClusterStateTaskExecutor<CleanRestoreStateTaskExecutor.Task>,
        ClusterStateTaskListener {

        static class Task {
            final String uuid;

            Task(String uuid) {
                this.uuid = uuid;
            }

            @Override
            public String toString() {
                return "clean restore state for restore " + uuid;
            }
        }

        @Override
        public ClusterTasksResult<Task> execute(final ClusterState currentState, final List<Task> tasks) {
            final ClusterTasksResult.Builder<Task> resultBuilder = ClusterTasksResult.<Task>builder().successes(tasks);
            Set<String> completedRestores = tasks.stream().map(e -> e.uuid).collect(Collectors.toSet());
            RestoreInProgress.Builder restoreInProgressBuilder = new RestoreInProgress.Builder();
            final RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE);
            boolean changed = false;
            if (restoreInProgress != null) {
                for (RestoreInProgress.Entry entry : restoreInProgress) {
                    if (completedRestores.contains(entry.uuid())) {
                        changed = true;
                    } else {
                        restoreInProgressBuilder.add(entry);
                    }
                }
            }
            if (changed == false) {
                return resultBuilder.build(currentState);
            }
            ImmutableOpenMap.Builder<String, ClusterState.Custom> builder = ImmutableOpenMap.builder(currentState.getCustoms());
            builder.put(RestoreInProgress.TYPE, restoreInProgressBuilder.build());
            ImmutableOpenMap<String, ClusterState.Custom> customs = builder.build();
            return resultBuilder.build(ClusterState.builder(currentState).customs(customs).build());
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
        }

        @Override
        public void onNoLongerMaster(String source) {
            logger.debug("no longer master while processing restore state update [{}]", source);
        }

    }

    private void cleanupRestoreState(ClusterChangedEvent event) {
        ClusterState state = event.state();

        RestoreInProgress restoreInProgress = state.custom(RestoreInProgress.TYPE);
        if (restoreInProgress != null) {
            for (RestoreInProgress.Entry entry : restoreInProgress) {
                if (entry.state().completed()) {
                    assert completed(entry.shards()) : "state says completed but restore entries are not";
                    clusterService.submitStateUpdateTask(
                        "clean up snapshot restore state",
                        new CleanRestoreStateTaskExecutor.Task(entry.uuid()),
                        ClusterStateTaskConfig.build(Priority.URGENT),
                        cleanRestoreStateTaskExecutor,
                        cleanRestoreStateTaskExecutor);
                }
            }
        }
    }

    private static RestoreInProgress.State overallState(RestoreInProgress.State nonCompletedState,
                                                        ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        boolean hasFailed = false;
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (!status.value.state().completed()) {
                return nonCompletedState;
            }
            if (status.value.state() == RestoreInProgress.State.FAILURE) {
                hasFailed = true;
            }
        }
        if (hasFailed) {
            return RestoreInProgress.State.FAILURE;
        } else {
            return RestoreInProgress.State.SUCCESS;
        }
    }

    public static boolean completed(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (!status.value.state().completed()) {
                return false;
            }
        }
        return true;
    }

    public static int failedShards(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        int failedShards = 0;
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (status.value.state() == RestoreInProgress.State.FAILURE) {
                failedShards++;
            }
        }
        return failedShards;
    }

    /**
     * 将内部的索引进行重命名
     * @param request
     * @param filteredIndices
     * @return
     */
    private static Map<String, String> renamedIndices(RestoreSnapshotRequest request, List<String> filteredIndices) {
        Map<String, String> renamedIndices = new HashMap<>();
        for (String index : filteredIndices) {
            String renamedIndex = index;
            // 替换匹配上的索引内容
            if (request.renameReplacement() != null && request.renamePattern() != null) {
                renamedIndex = index.replaceAll(request.renamePattern(), request.renameReplacement());
            }
            String previousIndex = renamedIndices.put(renamedIndex, index);
            if (previousIndex != null) {
                throw new SnapshotRestoreException(request.repository(), request.snapshot(),
                        "indices [" + index + "] and [" + previousIndex + "] are renamed into the same index [" + renamedIndex + "]");
            }
        }
        return Collections.unmodifiableMap(renamedIndices);
    }

    /**
     * Checks that snapshots can be restored and have compatible version
     *
     * @param repository      repository name
     * @param snapshotInfo    snapshot metadata
     */
    private static void validateSnapshotRestorable(final String repository, final SnapshotInfo snapshotInfo) {
        if (!snapshotInfo.state().restorable()) {
            throw new SnapshotRestoreException(new Snapshot(repository, snapshotInfo.snapshotId()),
                                               "unsupported snapshot state [" + snapshotInfo.state() + "]");
        }
        // 版本不兼容无法恢复数据
        if (Version.CURRENT.before(snapshotInfo.version())) {
            throw new SnapshotRestoreException(new Snapshot(repository, snapshotInfo.snapshotId()),
                                               "the snapshot was created with Elasticsearch version [" + snapshotInfo.version() +
                                                   "] which is higher than the version of this node [" + Version.CURRENT + "]");
        }
    }

    /**
     * 检测在生成快照时 哪些索引处理失败了    好像某些快照 partial为true 就会产生几个failure
     * @param snapshot
     * @param index
     * @return
     */
    private static boolean failed(SnapshotInfo snapshot, String index) {
        for (SnapshotShardFailure failure : snapshot.shardFailures()) {
            if (index.equals(failure.index())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the indices that are currently being restored and that are contained in the indices-to-check set.
     */
    public static Set<Index> restoringIndices(final ClusterState currentState, final Set<Index> indicesToCheck) {
        final RestoreInProgress restore = currentState.custom(RestoreInProgress.TYPE);
        if (restore == null) {
            return emptySet();
        }

        final Set<Index> indices = new HashSet<>();
        for (RestoreInProgress.Entry entry : restore) {
            for (ObjectObjectCursor<ShardId, RestoreInProgress.ShardRestoreStatus> shard : entry.shards()) {
                Index index = shard.key.getIndex();
                if (indicesToCheck.contains(index)
                    && shard.value.state().completed() == false
                    && currentState.getMetadata().index(index) != null) {
                    indices.add(index);
                }
            }
        }
        return indices;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster()) {
                cleanupRestoreState(event);
            }
        } catch (Exception t) {
            logger.warn("Failed to update restore state ", t);
        }
    }
}
