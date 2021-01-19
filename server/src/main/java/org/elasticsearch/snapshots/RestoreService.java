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
 * <p>
 * 基于快照进行数据恢复
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
    // 这里存储了无法删除的配置项
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

    private final MetadataCreateIndexService createIndexService;

    /**
     * index 的升级先不看了
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
     *                 代表需要从某个快照中恢复数据
     */
    public void restoreSnapshot(final RestoreSnapshotRequest request, final ActionListener<RestoreCompletionResponse> listener) {
        try {
            // Read snapshot info and metadata from the repository
            // 第一步 从仓库中加载此时最新的 repositoryData
            final String repositoryName = request.repository();
            Repository repository = repositoriesService.repository(repositoryName);
            final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
            repository.getRepositoryData(repositoryDataListener);


            // 当加载完仓库元数据后触发监听器
            repositoryDataListener.whenComplete(repositoryData -> {
                final String snapshotName = request.snapshot();
                // 检测快照是否存在  并且应该只存在一个同名快照  在执行快照任务时会做检测 如存在重名的快照 那么新的快照任务将无法执行
                final Optional<SnapshotId> matchingSnapshotId = repositoryData.getSnapshotIds().stream()
                    .filter(s -> snapshotName.equals(s.getName())).findFirst();
                if (matchingSnapshotId.isPresent() == false) {
                    throw new SnapshotRestoreException(repositoryName, snapshotName, "snapshot does not exist");
                }

                final SnapshotId snapshotId = matchingSnapshotId.get();
                // 获取本次快照任务的详细信息
                final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
                final Snapshot snapshot = new Snapshot(repositoryName, snapshotId);

                // Make sure that we can restore from this snapshot
                // 如果快照版本不兼容  无法恢复数据  如果分片任务执行失败 比如任务被标记成 aborted  无法进行恢复
                // 但是如果有部分shard生成了快照 认为是可恢复的
                validateSnapshotRestorable(repositoryName, snapshotInfo);

                // Resolve the indices from the snapshot that need to be restored
                // 找到本次请求恢复的index与 快照任务包含的index  的交集
                final List<String> indicesInSnapshot = filterIndices(snapshotInfo.indices(), request.indices(), request.indicesOptions());

                final Metadata.Builder metadataBuilder;
                // 代表本次恢复操作 要连同metadata一起复原
                if (request.includeGlobalState()) {
                    metadataBuilder = Metadata.builder(repository.getSnapshotGlobalMetadata(snapshotId));
                } else {
                    metadataBuilder = Metadata.builder();
                }

                // 通过index的基本信息 兑换到 indexMetadata
                final List<IndexId> indexIdsInSnapshot = repositoryData.resolveIndices(indicesInSnapshot);
                for (IndexId indexId : indexIdsInSnapshot) {
                    // 将相关的索引元数据设置到 metadata中
                    metadataBuilder.put(repository.getSnapshotIndexMetadata(snapshotId, indexId), false);
                }

                final Metadata metadata = metadataBuilder.build();

                // Apply renaming on index names, returning a map of names where
                // the key is the renamed index and the value is the original name
                // req中可以携带替换符 可以将index 重命名
                // v1 代表rename后的indexName  v2 代表原indexName
                final Map<String, String> indices = renamedIndices(request, indicesInSnapshot);

                // Now we can start the actual restore process by adding shards to be recovered in the cluster state
                // and updating cluster metadata (global and index) as needed
                clusterService.submitStateUpdateTask("restore_snapshot[" + snapshotName + ']', new ClusterStateUpdateTask() {

                    /**
                     * 每次执行restore 会分配一个唯一id
                     */
                    final String restoreUUID = UUIDs.randomBase64UUID();
                    RestoreInfo restoreInfo = null;

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        // 恢复任务 与 其他几个 XXXInProgress 是互斥的
                        RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE);
                        // Check if the snapshot to restore is currently being deleted
                        SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);

                        // 如果此时正好要针对这个快照进行删除操作  抛出异常
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
                            // 获取最小的兼容版本  TODO 先忽略有关版本的处理
                            final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion()
                                .minimumIndexCompatibilityVersion();
                            for (Map.Entry<String, String> indexEntry : indices.entrySet()) {
                                // 对应索引的原名
                                String index = indexEntry.getValue();

                                // true代表该index的分片数据是不完整的(上次生成快照有部分数据失败)  false代表快照数据完整
                                boolean partial = checkPartial(index);

                                // 将相关信息包装成 快照恢复源
                                SnapshotRecoverySource recoverySource = new SnapshotRecoverySource(restoreUUID, snapshot,
                                    snapshotInfo.version(), repositoryData.resolveIndexId(index));
                                // rename后的索引名
                                String renamedIndexName = indexEntry.getKey();

                                // 获取在生成该快照时该索引此时的元数据信息
                                IndexMetadata snapshotIndexMetadata = metadata.index(index);

                                // TODO 先忽略indexMetadata的更新
                                snapshotIndexMetadata = updateIndexSettings(snapshotIndexMetadata,
                                    request.indexSettings(), request.ignoreIndexSettings());
                                try {
                                    // TODO 忽略兼容性处理
                                    snapshotIndexMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(snapshotIndexMetadata,
                                        minIndexCompatibilityVersion);
                                } catch (Exception ex) {
                                    throw new SnapshotRestoreException(snapshot, "cannot restore index [" + index +
                                        "] because it cannot be upgraded", ex);
                                }
                                // Check that the index is closed or doesn't exist
                                // 这个就是利用快照恢复index的意思啊  以index-A 的快照作为原始数据 通过req.rename参数 将index-A 转换成 index-B 并开始恢复数据
                                IndexMetadata currentIndexMetadata = currentState.metadata().index(renamedIndexName);
                                IntSet ignoreShards = new IntHashSet();
                                final Index renamedIndex;
                                if (currentIndexMetadata == null) {
                                    // Index doesn't exist - create it and start recovery
                                    // Make sure that the index we are about to create has a validate name
                                    boolean isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(snapshotIndexMetadata.getSettings());
                                    // TODO 先忽略校验代码
                                    createIndexService.validateIndexName(renamedIndexName, currentState);
                                    createIndexService.validateDotIndex(renamedIndexName, currentState, isHidden);
                                    createIndexService.validateIndexSettings(renamedIndexName, snapshotIndexMetadata.getSettings(), false);

                                    // 这里以 index-A 作为模板创建 index-B
                                    IndexMetadata.Builder indexMdBuilder = IndexMetadata.builder(snapshotIndexMetadata)
                                        .state(IndexMetadata.State.OPEN)
                                        .index(renamedIndexName);
                                    indexMdBuilder.settings(Settings.builder()
                                        .put(snapshotIndexMetadata.getSettings())
                                        .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()));

                                    // 检查此时集群是否有承载新index的能力   不满足则抛出异常
                                    MetadataCreateIndexService.checkShardLimit(snapshotIndexMetadata.getSettings(), currentState);

                                    // 如果恢复操作不要求包含别名信息  而此时的index-B 中有别名信息 那么移除别名信息
                                    if (!request.includeAliases() && !snapshotIndexMetadata.getAliases().isEmpty()) {
                                        // Remove all aliases - they shouldn't be restored
                                        indexMdBuilder.removeAllAliases();
                                    } else {
                                        // 否则将该index的所有相关别名取出来
                                        for (ObjectCursor<String> alias : snapshotIndexMetadata.getAliases().keys()) {
                                            aliases.add(alias.value);
                                        }
                                    }
                                    IndexMetadata updatedIndexMetadata = indexMdBuilder.build();
                                    // 代表本次index中可能会有部分shard的数据无法恢复
                                    if (partial) {
                                        // 将无法通过快照恢复数据的shard 加入到特殊容器中
                                        populateIgnoredShards(index, ignoreShards);
                                    }
                                    // 在路由表中添加一个新的index 并且指定recovery为快照恢复
                                    rtBuilder.addAsNewRestore(updatedIndexMetadata, recoverySource, ignoreShards);
                                    // 加入block后 针对该index的相关请求都会被暂停
                                    blocks.addBlocks(updatedIndexMetadata);
                                    // 在当前metadata基础上增加新索引对应的元数据信息
                                    mdBuilder.put(updatedIndexMetadata, true);
                                    renamedIndex = updatedIndexMetadata.getIndex();
                                } else {
                                    // 2种情况 第一种 renamed后的快照还是它自身
                                    // 第二种 renamed后的快照之前已经存在

                                    validateExistingIndex(currentIndexMetadata, snapshotIndexMetadata, renamedIndexName, partial);

                                    // Index exists and it's closed - open it in metadata and start recovery
                                    // 尽可能用快照索引的元数据信息 去覆盖已存在的索引元数据信息
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

                                    // 如果本次不需要操作别名的副本 那么保留原indexMetadata的别名
                                    if (!request.includeAliases()) {
                                        // Remove all snapshot aliases
                                        if (!snapshotIndexMetadata.getAliases().isEmpty()) {
                                            indexMdBuilder.removeAllAliases();
                                        }
                                        // Add existing aliases
                                        for (ObjectCursor<AliasMetadata> alias : currentIndexMetadata.getAliases().values()) {
                                            indexMdBuilder.putAlias(alias.value);
                                        }
                                    } else {
                                        // 操作包含别名 所以就把快照的别名存进去
                                        for (ObjectCursor<String> alias : snapshotIndexMetadata.getAliases().keys()) {
                                            aliases.add(alias.value);
                                        }
                                    }


                                    // 沿用currentIndexMetadata的uuid 其余数据尽量使用 snapshotIndexMetadata的
                                    indexMdBuilder.settings(Settings.builder()
                                        .put(snapshotIndexMetadata.getSettings())
                                        .put(IndexMetadata.SETTING_INDEX_UUID,
                                            currentIndexMetadata.getIndexUUID()));
                                    IndexMetadata updatedIndexMetadata = indexMdBuilder.index(renamedIndexName).build();

                                    // 将indexMetadata的恢复源更新后 插入到路由表中
                                    rtBuilder.addAsRestore(updatedIndexMetadata, recoverySource);
                                    blocks.updateBlocks(updatedIndexMetadata);
                                    mdBuilder.put(updatedIndexMetadata, true);
                                    renamedIndex = updatedIndexMetadata.getIndex();
                                }

                                // 上面是处理index级别 这里要处理shard级别
                                for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {

                                    // 该分片可以正常进行数据恢复
                                    if (!ignoreShards.contains(shard)) {
                                        shardsBuilder.put(new ShardId(renamedIndex, shard),
                                            // 这里status设置了nodeId(为当前节点)  restore任务只能在leader节点发起吧
                                            new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId()));
                                    } else {
                                        // 该分片之前创建快照就已经失败了 所以本次该分片restore操作已经完成了 并且结果是failure
                                        shardsBuilder.put(new ShardId(renamedIndex, shard),
                                            new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId(),
                                                RestoreInProgress.State.FAILURE));
                                    }
                                }
                            }

                            // 到此为止 已经为所有本次需要恢复的所有索引 更新/创建了 indexMetadata 并且设置了每个分片的 restoreStatus
                            shards = shardsBuilder.build();
                            // 生成restore.Entry
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
                            // 代表本次没有可恢复数据的索引
                            shards = ImmutableOpenMap.of();
                        }

                        // aliases 是本次要插入的别名信息  别名不能与indices重复
                        checkAliasNameConflicts(indices, aliases);

                        // Restore global state if needed
                        // 这里是尝试将 metadata 也恢复到生成快照的那个时刻   在执行快照任务时 可以不指定index 通知指定存储metadata 这样就可以只针对metadata 进行还原
                        if (request.includeGlobalState()) {
                            if (metadata.persistentSettings() != null) {
                                Settings settings = metadata.persistentSettings();
                                clusterSettings.validateUpdate(settings);
                                mdBuilder.persistentSettings(settings);
                            }

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

                        // 如果所有分片在生成快照时都失败了 实际上本次restore任务就可以提前退出了
                        if (completed(shards)) {
                            // We don't have any indices to restore - we are done
                            restoreInfo = new RestoreInfo(snapshotId.getName(),
                                List.copyOf(indices.keySet()),
                                shards.size(),
                                shards.size() - failedShards(shards));
                        }

                        RoutingTable rt = rtBuilder.build();
                        ClusterState updatedState = builder.metadata(mdBuilder).blocks(blocks).routingTable(rt).build();
                        // 因为产生了新的分片 所以要进行重路由 而在 gatewayAllocation中 一旦发现某些分片的recoverySource是 restore 就会做特殊处理
                        return allocationService.reroute(updatedState, "restored snapshot [" + snapshot + "]");
                    }

                    /**
                     * 检查别名是否与 索引名重复了
                     * @param renamedIndices
                     * @param aliases
                     */
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
                     * 将该index 下所有生成快照失败的shard 加入到容器中
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
                        // 判断该index在生成快照时 是否失败了
                        if (failed(snapshotInfo, index)) {
                            // 允许只恢复部分数据
                            if (request.partial()) {
                                return true;
                            } else {
                                throw new SnapshotRestoreException(snapshot, "index [" + index + "] wasn't fully snapshotted - cannot " +
                                    "restore");
                            }
                        } else {
                            // 快照数据是完整的
                            return false;
                        }
                    }

                    /**
                     * 当尝试用快照作为恢复源去恢复某个分片时 发现renamed后的索引已经存在
                     * @param currentIndexMetadata
                     * @param snapshotIndexMetadata
                     * @param renamedIndex
                     * @param partial
                     */
                    private void validateExistingIndex(IndexMetadata currentIndexMetadata, // 此时该索引最新的元数据信息
                                                       IndexMetadata snapshotIndexMetadata, // 在生成快照时该索引的快照信息
                                                       String renamedIndex, // 重命名后的索引名
                                                       boolean partial // 是否有部分分片快照创建失败了
                    ) {
                        // Index exist - checking that it's closed
                        // 此时该索引已经被关闭了 那么就无法进行数据恢复了
                        if (currentIndexMetadata.getState() != IndexMetadata.State.CLOSE) {
                            // TODO: Enable restore for open indices
                            throw new SnapshotRestoreException(snapshot, "cannot restore index [" + renamedIndex
                                + "] because an open index " +
                                "with same name already exists in the cluster. Either close or delete the existing index or restore the " +
                                "index under a different name by providing a rename pattern and replacement name");
                        }

                        // 如果想要以快照作为恢复源 且被恢复的index之前已经存在 那么必须完全的覆盖  shard数量必须一致 且所有分片的快照必须都创建成功

                        // Index exist - checking if it's partial restore
                        // TODO 这里还不清楚这样设计的理由  如果index是存在的 且部分快照之前创建失败了  那么就不该继续下去了
                        if (partial) {
                            throw new SnapshotRestoreException(snapshot, "cannot restore partial index [" + renamedIndex
                                + "] because such index already exists");
                        }
                        // Make sure that the number of shards is the same. That's the only thing that we cannot change
                        // 要求分片数量必须一致
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
     * 删除某些index
     *
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
     * 记录在reroute过程中 哪些分片发生了变化 并在reroute流程结束后 针对这些分片进行数据恢复
     */
    public static class RestoreInProgressUpdater extends RoutingChangesObserver.AbstractRoutingChangesObserver {
        // Map of RestoreUUID to a of changes to the shards' restore statuses

        // key1：SnapshotRecoverySource.uuid key2：shardId value：恢复状态(比如在哪个节点上进行恢复，此时执行到恢复的什么阶段)
        private final Map<String, Map<ShardId, ShardRestoreStatus>> shardChanges = new HashMap<>();

        /**
         * 当某个分片从 init->start
         *
         * @param initializingShard
         * @param startedShard
         */
        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
            // mark snapshot as completed
            // 本次切换的是主分片
            if (initializingShard.primary()) {
                RecoverySource recoverySource = initializingShard.recoverySource();
                // TODO 目前只知道主分片从本地事务日志恢复数据  还不清楚别的恢复源是什么时候设置的
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    changes(recoverySource).put(
                        initializingShard.shardId(),
                        new ShardRestoreStatus(initializingShard.currentNodeId(), RestoreInProgress.State.SUCCESS));
                }
            }
        }

        /**
         * 如果某个分片分配失败了
         *
         * @param failedShard
         * @param unassignedInfo
         */
        @Override
        public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
            // 只有主分片在启动时 失败需要处理
            if (failedShard.primary() && failedShard.initializing()) {
                // 同样只有快照模式才进行处理  记录一个失败状态
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
         * 一个分片进行分配需要经过很多流程   在其中 unassignedInfo 也会发生变化
         *
         * @param unassignedShard
         * @param newUnassignedInfo
         */
        @Override
        public void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {
            RecoverySource recoverySource = unassignedShard.recoverySource();
            // TODO 非快照恢复 忽略
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
         * 获取某个shardId相关的 描述恢复状态的对象
         */
        private Map<ShardId, ShardRestoreStatus> changes(RecoverySource recoverySource) {
            assert recoverySource.getType() == RecoverySource.Type.SNAPSHOT;
            return shardChanges.computeIfAbsent(((SnapshotRecoverySource) recoverySource).restoreUUID(), k -> new HashMap<>());
        }

        /**
         * 处理之前监听到的变化   Restore只针对恢复方式为 SNAPSHOT 的
         *
         * @param oldRestore 针对恢复状态进行更新
         * @return
         */
        public RestoreInProgress applyChanges(final RestoreInProgress oldRestore) {
            if (shardChanges.isEmpty() == false) {
                RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
                // 每个entry 好像就是一次恢复操作  而 RestoreInProgress好像包含了多次恢复???
                for (RestoreInProgress.Entry entry : oldRestore) {
                    Map<ShardId, ShardRestoreStatus> updates = shardChanges.get(entry.uuid());
                    ImmutableOpenMap<ShardId, ShardRestoreStatus> shardStates = entry.shards();
                    if (updates != null && updates.isEmpty() == false) {
                        ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = ImmutableOpenMap.builder(shardStates);
                        // 把 shardChanges内的数据都插入到builder中
                        for (Map.Entry<ShardId, ShardRestoreStatus> shard : updates.entrySet()) {
                            ShardId shardId = shard.getKey();
                            ShardRestoreStatus status = shardStates.get(shardId);

                            // 之前oldRestore内部的status 如果未设置 或者之前的status处于未完成状态 那么允许被当前最新的 shardChanges更新
                            if (status == null || status.state().completed() == false) {
                                shardsBuilder.put(shardId, shard.getValue());
                            }
                        }

                        // 此时内部某些shardId 相关的status可能被修改成了 completed状态
                        ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = shardsBuilder.build();
                        // 根据当前所有结果 返回一个状态
                        RestoreInProgress.State newState = overallState(RestoreInProgress.State.STARTED, shards);
                        builder.add(new RestoreInProgress.Entry(entry.uuid(), entry.snapshot(), newState, entry.indices(), shards));
                    } else {
                        // 如果oldRestore中的某个entry 没有被记录到本次发生变化的数据容器中 原样保存
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
     * 每当leader节点发生变化时  检测恢复任务是否完成 并从clusterState中清理
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
                        // 可以看到已完成的任务没有加入到 builder对象中
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

    /**
     * 每当检测到集群状态发生变化 都要触发该方法
     * 实际上就是将 restoreEntry 从 clusterState中移除
     *
     * @param event
     */
    private void cleanupRestoreState(ClusterChangedEvent event) {
        ClusterState state = event.state();

        RestoreInProgress restoreInProgress = state.custom(RestoreInProgress.TYPE);
        if (restoreInProgress != null) {
            for (RestoreInProgress.Entry entry : restoreInProgress) {
                // 实际上就是检测 恢复任务是否已经完成 如果完成则从clusterState中移除
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

    /**
     *
     * @param nonCompletedState 根据此时每个shard级别的恢复状态 判读当前恢复任务是否已经结束
     * @param shards
     * @return
     */
    private static RestoreInProgress.State overallState(RestoreInProgress.State nonCompletedState,
                                                        ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        boolean hasFailed = false;
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            // 只要有某个分片任务未完成 代表整个restore未完成
            if (!status.value.state().completed()) {
                return nonCompletedState;
            }
            // failure 属于 completed  所以不会走上面的分支
            if (status.value.state() == RestoreInProgress.State.FAILURE) {
                hasFailed = true;
            }
        }
        // 当所有分片都已经处理完毕后  只要有某个分片恢复失败了 就认为整个快照流程失败了
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
     *
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
            // 当没有发生替换时 v1与v2是相同的
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
     * @param repository   repository name
     * @param snapshotInfo snapshot metadata
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
     * 在生成快照的过程中 某些分片可能会处理失败
     *
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
     *
     * @param currentState   当前集群状态
     * @param indicesToCheck 本次需要检测的索引
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
                // 此时某个正在恢复的分片正好属于本次检查的某个索引
                if (indicesToCheck.contains(index)
                    && shard.value.state().completed() == false
                    && currentState.getMetadata().index(index) != null) {
                    indices.add(index);
                }
            }
        }
        // 将正在恢复中的索引返回
        return indices;
    }

    /**
     * 当该服务感知到集群状态发生变化时 触发监听器
     *
     * @param event
     */
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
