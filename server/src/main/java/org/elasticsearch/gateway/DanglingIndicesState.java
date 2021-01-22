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

package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * The dangling indices state is responsible for finding new dangling indices (indices that have
 * their state written on disk, but don't exists in the metadata of the cluster), and importing
 * them into the cluster.
 * 检测集群中是否存在摇摆索引 并进行处理
 */
public class DanglingIndicesState implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(DanglingIndicesState.class);

    public static final Setting<Boolean> AUTO_IMPORT_DANGLING_INDICES_SETTING = Setting.boolSetting(
        "gateway.auto_import_dangling_indices",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );

    /**
     * 指明了该节点下所有的数据文件路径
     */
    private final NodeEnvironment nodeEnv;
    private final MetaStateService metaStateService;
    private final LocalAllocateDangledIndices allocateDangledIndices;

    /**
     * 是否自动导入摇摆索引
     * 如果设置为true 会将自己作为监听器注册到 clusterService上
     */
    private final boolean isAutoImportDanglingIndicesEnabled;

    /**
     * 存储所有摇摆的索引
     */
    private final Map<Index, IndexMetadata> danglingIndices = ConcurrentCollections.newConcurrentMap();

    /**
     * 没有看到这些相关实例注入到工厂的动作啊
     * @param nodeEnv
     * @param metaStateService
     * @param allocateDangledIndices
     * @param clusterService
     */
    @Inject
    public DanglingIndicesState(NodeEnvironment nodeEnv, MetaStateService metaStateService,
                                LocalAllocateDangledIndices allocateDangledIndices, ClusterService clusterService) {
        this.nodeEnv = nodeEnv;
        this.metaStateService = metaStateService;
        this.allocateDangledIndices = allocateDangledIndices;

        this.isAutoImportDanglingIndicesEnabled = AUTO_IMPORT_DANGLING_INDICES_SETTING.get(clusterService.getSettings());

        if (this.isAutoImportDanglingIndicesEnabled) {
            clusterService.addListener(this);
        } else {
            logger.warn(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey() + " is disabled, dangling indices will not be detected or imported");
        }
    }

    boolean isAutoImportDanglingIndicesEnabled() {
        return this.isAutoImportDanglingIndicesEnabled;
    }

    /**
     * Process dangling indices based on the provided meta data, handling cleanup, finding
     * new dangling indices, and allocating outstanding ones.
     * 处理摇摆索引
     */
    public void processDanglingIndices(final Metadata metadata) {
        // 如果没有指定数据路径 直接返回
        if (nodeEnv.hasNodeFile() == false) {
            return;
        }
        // 第一步清理之前残留的摇摆索引
        cleanupAllocatedDangledIndices(metadata);
        findNewAndAddDanglingIndices(metadata);
        allocateDanglingIndices();
    }

    /**
     * The current set of dangling indices.
     */
    Map<Index, IndexMetadata> getDanglingIndices() {
        // This might be a good use case for CopyOnWriteHashMap
        return Map.copyOf(danglingIndices);
    }

    /**
     * Cleans dangling indices if they are already allocated on the provided meta data.
     * 检测之前发现的摇摆索引 是否已经在集群中创建了对应的元数据信息
     */
    void cleanupAllocatedDangledIndices(Metadata metadata) {
        for (Index index : danglingIndices.keySet()) {
            // 获取该索引相关的元数据
            final IndexMetadata indexMetadata = metadata.index(index);
            // 已经处理完毕就从集群中移除
            if (indexMetadata != null && indexMetadata.getIndex().getName().equals(index.getName())) {
                if (indexMetadata.getIndex().getUUID().equals(index.getUUID()) == false) {
                    logger.warn("[{}] can not be imported as a dangling index, as there is already another index " +
                        "with the same name but a different uuid. local index will be ignored (but not deleted)", index);
                } else {
                    logger.debug("[{}] no longer dangling (created), removing from dangling list", index);
                }
                danglingIndices.remove(index);
            }
        }
    }

    /**
     * Finds (@{link #findNewAndAddDanglingIndices}) and adds the new dangling indices
     * to the currently tracked dangling indices.
     * 从元数据中找到新的摇摆索引 并保存
     */
    void findNewAndAddDanglingIndices(final Metadata metadata) {
        danglingIndices.putAll(findNewDanglingIndices(metadata));
    }

    /**
     * Finds new dangling indices by iterating over the indices and trying to find indices
     * that have state on disk, but are not part of the provided meta data, or not detected
     * as dangled already.
     * 从当前元数据中找到所有摇摆索引
     * 摇摆索引就是从当前数据目录还原之前可能存在的index 如果明确指定该index已经被删除了 那么就不会恢复
     */
    Map<Index, IndexMetadata> findNewDanglingIndices(final Metadata metadata) {
        final Set<String> excludeIndexPathIds = new HashSet<>(metadata.indices().size() + danglingIndices.size());

        // 首先已经存在元数据的index 以及当前已经保存在容器中的 index是不需要考虑的
        for (ObjectCursor<IndexMetadata> cursor : metadata.indices().values()) {
            excludeIndexPathIds.add(cursor.value.getIndex().getUUID());
        }
        excludeIndexPathIds.addAll(danglingIndices.keySet().stream().map(Index::getUUID).collect(Collectors.toList()));
        try {
            // 将当前node目录下所有index 最新的元数据加载出来
            final List<IndexMetadata> indexMetadataList = metaStateService.loadIndicesStates(excludeIndexPathIds::contains);
            Map<Index, IndexMetadata> newIndices = new HashMap<>(indexMetadataList.size());

            // 这里存储了所有应当被删除的index
            final IndexGraveyard graveyard = metadata.indexGraveyard();
            for (IndexMetadata indexMetadata : indexMetadataList) {
                if (metadata.hasIndex(indexMetadata.getIndex().getName())) {
                    logger.warn("[{}] can not be imported as a dangling index, as index with same name already exists in cluster metadata",
                        indexMetadata.getIndex());

                } else if (graveyard.containsIndex(indexMetadata.getIndex())) {
                    logger.warn("[{}] can not be imported as a dangling index, as an index with the same name and UUID exist in the " +
                                "index tombstones.  This situation is likely caused by copying over the data directory for an index " +
                                "that was previously deleted.", indexMetadata.getIndex());
                } else {
                    logger.info("[{}] dangling index exists on local file system, but not in cluster metadata, " +
                                "auto import to cluster state", indexMetadata.getIndex());
                    newIndices.put(indexMetadata.getIndex(), stripAliases(indexMetadata));
                }
            }
            return newIndices;
        } catch (IOException e) {
            logger.warn("failed to list dangling indices", e);
            return emptyMap();
        }
    }

    /**
     * Dangling importing indices with aliases is dangerous, it could for instance result in inability to write to an existing alias if it
     * previously had only one index with any is_write_index indication.
     * 裁剪掉所有的 别名
     */
    private IndexMetadata stripAliases(IndexMetadata indexMetadata) {
        if (indexMetadata.getAliases().isEmpty()) {
            return indexMetadata;
        } else {
            logger.info("[{}] stripping aliases: {} from index before importing",
                indexMetadata.getIndex(), indexMetadata.getAliases().keys());
            return IndexMetadata.builder(indexMetadata).removeAllAliases().build();
        }
    }

    /**
     * Allocates the provided list of the dangled indices by sending them to the master node
     * for allocation.
     * 处理所有摇摆的索引
     */
    void allocateDanglingIndices() {
        if (danglingIndices.isEmpty()) {
            return;
        }
        try {
            allocateDangledIndices.allocateDangled(Collections.unmodifiableCollection(new ArrayList<>(danglingIndices.values())),
                new ActionListener<>() {
                    @Override
                    public void onResponse(LocalAllocateDangledIndices.AllocateDangledResponse response) {
                        logger.trace("allocated dangled");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.info("failed to send allocated dangled", e);
                    }
                }
            );
        } catch (Exception e) {
            logger.warn("failed to send allocate dangled", e);
        }
    }

    /**
     * 当开启自动检测摇摆索引时 触发该函数
     * @param event
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().disableStatePersistence() == false) {
            processDanglingIndices(event.state().metadata());
        }
    }
}
