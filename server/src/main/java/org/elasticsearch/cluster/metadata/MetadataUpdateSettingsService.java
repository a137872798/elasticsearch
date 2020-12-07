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

package org.elasticsearch.cluster.metadata;

import static org.elasticsearch.action.support.ContextPreservingActionListener.wrapPreservingContext;
import static org.elasticsearch.index.IndexSettings.same;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeSettingsClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Service responsible for submitting update index settings requests
 */
public class MetadataUpdateSettingsService {
    private static final Logger logger = LogManager.getLogger(MetadataUpdateSettingsService.class);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final IndexScopedSettings indexScopedSettings;
    private final IndicesService indicesService;
    private final ThreadPool threadPool;

    @Inject
    public MetadataUpdateSettingsService(ClusterService clusterService, AllocationService allocationService,
                                         IndexScopedSettings indexScopedSettings, IndicesService indicesService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.allocationService = allocationService;
        this.indexScopedSettings = indexScopedSettings;
        this.indicesService = indicesService;
    }

    /**
     * 更新配置信息
     *
     * @param request
     * @param listener
     */
    public void updateSettings(final UpdateSettingsClusterStateUpdateRequest request,
                               final ActionListener<ClusterStateUpdateResponse> listener) {

        // 本次配置都追加上 "index." 前缀
        final Settings normalizedSettings =
            Settings.builder().put(request.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();

        // 可能会新增某些配置项 也可能会移除某些配置项
        Settings.Builder settingsForClosedIndices = Settings.builder();
        Settings.Builder settingsForOpenIndices = Settings.builder();
        final Set<String> skippedSettings = new HashSet<>();

        // 校验传入的配置项  settings中可以为每个配置项设置一个 校验器对象
        indexScopedSettings.validate(
            normalizedSettings.filter(s -> Regex.isSimpleMatchPattern(s) == false), // don't validate wildcards
            false, // don't validate dependencies here we check it below never allow to change the number of shards
            true); // validate internal or private index settings
        for (String key : normalizedSettings.keySet()) {
            Setting setting = indexScopedSettings.get(key);
            boolean isWildcard = setting == null && Regex.isSimpleMatchPattern(key);
            assert setting != null // we already validated the normalized settings
                || (isWildcard && normalizedSettings.hasValue(key) == false)
                : "unknown setting: " + key + " isWildcard: " + isWildcard + " hasValue: " + normalizedSettings.hasValue(key);

            // 当前所有项先设置到close容器中
            settingsForClosedIndices.copy(key, normalizedSettings);
            if (isWildcard || setting.isDynamic()) {
                // 有关动态配置 或者包含通配符的配置项 又加入到open容器中
                settingsForOpenIndices.copy(key, normalizedSettings);
            } else {
                // 存储非动态配置项
                skippedSettings.add(key);
            }
        }
        final Settings closedSettings = settingsForClosedIndices.build();
        final Settings openSettings = settingsForOpenIndices.build();
        // 是否保留当前存在的配置项
        final boolean preserveExisting = request.isPreserveExisting();

        clusterService.submitStateUpdateTask("update-settings " + Arrays.toString(request.indices()),
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request,
                wrapPreservingContext(listener, threadPool.getThreadContext())) {

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                /**
                 * 该方法负责将 settings更新到CS中
                 * @param currentState
                 * @return
                 */
                @Override
                public ClusterState execute(ClusterState currentState) {

                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

                    // allow to change any settings to a close index, and only allow dynamic settings to be changed
                    // on an open index
                    Set<Index> openIndices = new HashSet<>();
                    Set<Index> closeIndices = new HashSet<>();
                    final String[] actualIndices = new String[request.indices().length];
                    for (int i = 0; i < request.indices().length; i++) {
                        Index index = request.indices()[i];
                        actualIndices[i] = index.getName();
                        // 先获取本次操作涉及到的所有index
                        final IndexMetadata metadata = currentState.metadata().getIndexSafe(index);
                        // 根据此时index是否处于打开状态 存储到2个容器中
                        if (metadata.getState() == IndexMetadata.State.OPEN) {
                            openIndices.add(index);
                        } else {
                            closeIndices.add(index);
                        }
                    }

                    // skip中存储的是非动态配置 并且此时有某些index处于打开状态
                    // 看来非dynamic配置项不能作用于打开的索引上 必须先等待索引被关闭
                    if (!skippedSettings.isEmpty() && !openIndices.isEmpty()) {
                        throw new IllegalArgumentException(String.format(Locale.ROOT,
                            "Can't update non dynamic settings [%s] for open indices %s", skippedSettings, openIndices));
                    }

                    // 那么此时 要么本次更新的都是动态配置项 要么所有索引均处于关闭状态
                    // 这个配置项是从openSettings中获得的 也就是说这个配置属于动态配置
                    int updatedNumberOfReplicas = openSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, -1);
                    if (updatedNumberOfReplicas != -1 && preserveExisting == false) {

                        // Verify that this won't take us over the cluster shard limit.
                        // 根据最新的副本数 计算这些索引总计应该创建多少分片
                        int totalNewShards = Arrays.stream(request.indices())
                            .mapToInt(i -> getTotalNewShards(i, currentState, updatedNumberOfReplicas))
                            .sum();
                        // 检测这些分片总数是否超过了集群限制
                        Optional<String> error = IndicesService.checkShardLimit(totalNewShards, currentState);
                        if (error.isPresent()) {
                            ValidationException ex = new ValidationException();
                            ex.addValidationError(error.get());
                            throw ex;
                        }

                        // we do *not* update the in sync allocation ids as they will be removed upon the first index
                        // operation which make these copies stale
                        // TODO: update the list once the data is deleted by the node?
                        // 根据副本数调整路由表下所有分片数量
                        routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                        // 更新元数据中有关分片数量的信息
                        metadataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                        logger.info("updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas, actualIndices);
                    }

                    // 因为即将要对index做一些操作 所以这里增加了一些block 避免在意外情况下访问
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_READ_ONLY_BLOCK,
                        IndexMetadata.INDEX_READ_ONLY_SETTING, openSettings);
                    maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK,
                        IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING, openSettings);
                    maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_METADATA_BLOCK,
                        IndexMetadata.INDEX_BLOCKS_METADATA_SETTING, openSettings);
                    maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_WRITE_BLOCK,
                        IndexMetadata.INDEX_BLOCKS_WRITE_SETTING, openSettings);
                    maybeUpdateClusterBlock(actualIndices, blocks, IndexMetadata.INDEX_READ_BLOCK,
                        IndexMetadata.INDEX_BLOCKS_READ_SETTING, openSettings);

                    // 存在打开的索引 那么本次更新配置必须全部都是动态配置
                    if (!openIndices.isEmpty()) {
                        for (Index index : openIndices) {
                            IndexMetadata indexMetadata = metadataBuilder.getSafe(index);
                            Settings.Builder updates = Settings.builder();
                            Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
                            // 更新原有的配置  如果某些值被设置成null 会从indexSettings中移除
                            if (indexScopedSettings.updateDynamicSettings(openSettings, indexSettings, updates, index.getName())) {
                                // 这里又覆盖回去还改毛啊???
                                if (preserveExisting) {
                                    indexSettings.put(indexMetadata.getSettings());
                                }
                                Settings finalSettings = indexSettings.build();
                                indexScopedSettings.validate(
                                    finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
                                // 这里将 finalSettings覆盖到之前的旧配置中
                                metadataBuilder.put(IndexMetadata.builder(indexMetadata).settings(finalSettings));
                            }
                        }
                    }

                    // 针对close的索引 无论是否是动态配置 都可以更新  当openIndex为空 就可以存在普通配置 当openIndex不为空 实际上本次更新的配置还是只有动态配置(过不了上面的判断条件)
                    // 操作跟上面基本相同
                    if (!closeIndices.isEmpty()) {
                        for (Index index : closeIndices) {
                            IndexMetadata indexMetadata = metadataBuilder.getSafe(index);
                            Settings.Builder updates = Settings.builder();
                            Settings.Builder indexSettings = Settings.builder().put(indexMetadata.getSettings());
                            if (indexScopedSettings.updateSettings(closedSettings, indexSettings, updates, index.getName())) {
                                // 理解不了这个标记的意义
                                if (preserveExisting) {
                                    indexSettings.put(indexMetadata.getSettings());
                                }
                                Settings finalSettings = indexSettings.build();
                                indexScopedSettings.validate(
                                    finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
                                metadataBuilder.put(IndexMetadata.builder(indexMetadata).settings(finalSettings));
                            }
                        }
                    }

                    // 如果存在有关事务文件存储的配置  age应该是形容最多保留多久前的文件  而size则是当文件达到多少大小时强制清理
                    // 清理了就代表数据无法恢复  极端情况会导致写入可能还未读取的数据被删除??? 特别是MQ这种被清理了会怎么办 允许出现这种情况吗
                    if (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(normalizedSettings) ||
                        IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(normalizedSettings)) {
                        // 检测这些配置项是否合法
                        for (String index : actualIndices) {
                            // 实际上在8.0版本开始 不再支持这两项配置了 当检测到会抛出异常
                            MetadataCreateIndexService.validateTranslogRetentionSettings(metadataBuilder.get(index).getSettings());
                        }
                    }
                    // increment settings versions
                    for (final String index : actualIndices) {
                        // 如果配置项发生了变化 需要增加版本号
                        if (same(currentState.metadata().index(index).getSettings(), metadataBuilder.get(index).getSettings()) == false) {
                            final IndexMetadata.Builder builder = IndexMetadata.builder(metadataBuilder.get(index));
                            builder.settingsVersion(1 + builder.settingsVersion());
                            metadataBuilder.put(builder);
                        }
                    }

                    // 因为配置项的更新 所以需要为新生成的分片分配路由
                    ClusterState updatedState = ClusterState.builder(currentState).metadata(metadataBuilder)
                        .routingTable(routingTableBuilder.build()).blocks(blocks).build();

                    // now, reroute in case things change that require it (like number of replicas)
                    updatedState = allocationService.reroute(updatedState, "settings update");
                    try {
                        for (Index index : openIndices) {
                            final IndexMetadata currentMetadata = currentState.getMetadata().getIndexSafe(index);
                            final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(index);
                            // 以检测元数据的场景会创建索引服务 确保不会出现异常  并且会关闭索引
                            indicesService.verifyIndexMetadata(currentMetadata, updatedMetadata);
                        }
                        for (Index index : closeIndices) {
                            final IndexMetadata currentMetadata = currentState.getMetadata().getIndexSafe(index);
                            final IndexMetadata updatedMetadata = updatedState.metadata().getIndexSafe(index);
                            // Verifies that the current index settings can be updated with the updated dynamic settings.
                            indicesService.verifyIndexMetadata(currentMetadata, updatedMetadata);
                            // Now check that we can create the index with the updated settings (dynamic and non-dynamic).
                            // This step is mandatory since we allow to update non-dynamic settings on closed indices.
                            indicesService.verifyIndexMetadata(updatedMetadata, updatedMetadata);
                        }
                    } catch (IOException ex) {
                        throw ExceptionsHelper.convertToElastic(ex);
                    }
                    return updatedState;
                }
            });
    }

    /**
     * 计算某个index下总的分片数
     * @param index  当前处理的索引名
     * @param currentState
     * @param updatedNumberOfReplicas
     * @return
     */
    private int getTotalNewShards(Index index, ClusterState currentState, int updatedNumberOfReplicas) {
        IndexMetadata indexMetadata = currentState.metadata().index(index);
        // 有多少shardId
        int shardsInIndex = indexMetadata.getNumberOfShards();
        // 原先有多少副本
        int oldNumberOfReplicas = indexMetadata.getNumberOfReplicas();
        // 通过副本数差值 * shardId 数 得到增加的分片总数 因为primary会对冲 所以不需要考虑
        int replicaIncrease = updatedNumberOfReplicas - oldNumberOfReplicas;
        return replicaIncrease * shardsInIndex;
    }

    /**
     * Updates the cluster block only iff the setting exists in the given settings
     */
    private static void maybeUpdateClusterBlock(String[] actualIndices, ClusterBlocks.Builder blocks, ClusterBlock block,
                                                Setting<Boolean> setting, Settings openSettings) {
        if (setting.exists(openSettings)) {
            final boolean updateBlock = setting.get(openSettings);
            for (String index : actualIndices) {
                if (updateBlock) {
                    blocks.addIndexBlock(index, block);
                } else {
                    blocks.removeIndexBlock(index, block);
                }
            }
        }
    }


    public void upgradeIndexSettings(final UpgradeSettingsClusterStateUpdateRequest request,
                                     final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("update-index-compatibility-versions",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request,
                wrapPreservingContext(listener, threadPool.getThreadContext())) {

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    for (Map.Entry<String, Tuple<Version, String>> entry : request.versions().entrySet()) {
                        String index = entry.getKey();
                        IndexMetadata indexMetadata = metadataBuilder.get(index);
                        if (indexMetadata != null) {
                            if (Version.CURRENT.equals(indexMetadata.getCreationVersion()) == false) {
                                // no reason to pollute the settings, we didn't really upgrade anything
                                metadataBuilder.put(
                                    IndexMetadata
                                        .builder(indexMetadata)
                                        .settings(
                                            Settings
                                                .builder()
                                                .put(indexMetadata.getSettings())
                                                .put(IndexMetadata.SETTING_VERSION_UPGRADED, entry.getValue().v1()))
                                        .settingsVersion(1 + indexMetadata.getSettingsVersion()));
                            }
                        }
                    }
                    return ClusterState.builder(currentState).metadata(metadataBuilder).build();
                }
            });
    }
}
