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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsService;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Deletes indices.
 * 有关删除索引的功能被单独抽取出来 生成一个服务
 */
public class MetadataDeleteIndexService {

    private static final Logger logger = LogManager.getLogger(MetadataDeleteIndexService.class);

    private final Settings settings;
    private final ClusterService clusterService;

    private final AllocationService allocationService;

    @Inject
    public MetadataDeleteIndexService(Settings settings, ClusterService clusterService, AllocationService allocationService) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
    }

    /**
     * 发起一个删除index的请求  会在cluster中提交更新任务
     * @param request
     * @param listener
     */
    public void deleteIndices(final DeleteIndexClusterStateUpdateRequest request,
            final ActionListener<ClusterStateUpdateResponse> listener) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        clusterService.submitStateUpdateTask("delete-index " + Arrays.toString(request.indices()),
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {
                return deleteIndices(currentState, Sets.newHashSet(request.indices()));
            }
        });
    }

    /**
     * Delete some indices from the cluster state.
     * @param indices 本次需要删除的index
     * 从ClusterState中删除某些index
     */
    public ClusterState deleteIndices(ClusterState currentState, Set<Index> indices) {
        final Metadata meta = currentState.metadata();

        // 包含本次要删除的所有index
        final Set<Index> indicesToDelete = new HashSet<>();

        // 如果本次删除的index 有关联的
        final Map<Index, DataStream> backingIndices = new HashMap<>();
        for (Index index : indices) {
            IndexMetadata im = meta.getIndexSafe(index);
            // 在通过删除dataStream 间接删除index的场景 是不会找到parent的
            // 因为在外层时 一旦删除dataStream 立即重建了 Metadata 这样lookup内的数据也会重建 原本挂载在dataStream下的index 就不会设置parent属性
            IndexAbstraction.DataStream parent = meta.getIndicesLookup().get(im.getIndex().getName()).getParentDataStream();
            // 如果在删除时 还绑定了 dataStream 那么删除粒度会不同
            // 也就是不能直接删除 dataStream下的某个writerIndex  比如通过删除dataStream来达成目的
            if (parent != null) {
                if (parent.getWriteIndex().equals(im)) {
                    throw new IllegalArgumentException("index [" + index.getName() + "] is the write index for data stream [" +
                        parent.getName() + "] and cannot be deleted");
                } else {
                    backingIndices.put(index, parent.getDataStream());
                }
            }
            indicesToDelete.add(im.getIndex());
        }

        // Check if index deletion conflicts with any running snapshots
        Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentState, indicesToDelete);
        // 如果某些索引正在生成快照  此时无法进行删除
        if (snapshottingIndices.isEmpty() == false) {
            throw new SnapshotInProgressException("Cannot delete indices that are being snapshotted: " + snapshottingIndices +
                ". Try again after snapshot finishes or cancel the currently running snapshot.");
        }

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(meta);
        ClusterBlocks.Builder clusterBlocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());

        // 找到metadata下描述 索引墓地的对象   该对象主要是为了避免某些摇摆索引自动创建
        final IndexGraveyard.Builder graveyardBuilder = IndexGraveyard.builder(metadataBuilder.indexGraveyard());

        // 此时已经存放了有关多少个index 的墓碑信息
        final int previousGraveyardSize = graveyardBuilder.tombstones().size();
        for (final Index index : indices) {
            String indexName = index.getName();
            logger.info("{} deleting index", index);
            // 从相关的元数据中移除index信息
            routingTableBuilder.remove(indexName);
            clusterBlocksBuilder.removeIndexBlocks(indexName);
            metadataBuilder.remove(indexName);
            // 找到dataStream 删除子索引
            if (backingIndices.containsKey(index)) {
                DataStream parent = backingIndices.get(index);
                metadataBuilder.put(parent.removeBackingIndex(index));
            }
        }
        // add tombstones to the cluster state for each deleted index
        // 本次被删除的索引会加入到 索引墓地中
        final IndexGraveyard currentGraveyard = graveyardBuilder.addTombstones(indices).build(settings);
        metadataBuilder.indexGraveyard(currentGraveyard); // the new graveyard set on the metadata
        logger.trace("{} tombstones purged from the cluster state. Previous tombstone size: {}. Current tombstone size: {}.",
            graveyardBuilder.getNumPurged(), previousGraveyardSize, currentGraveyard.getTombstones().size());

        Metadata newMetadata = metadataBuilder.build();
        ClusterBlocks blocks = clusterBlocksBuilder.build();

        // update snapshot restore entries
        ImmutableOpenMap<String, ClusterState.Custom> customs = currentState.getCustoms();
        final RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE);
        if (restoreInProgress != null) {
            // 由于某些 index 被删除  要更新 restoreInProgress  (这些index对应的shard的恢复结果会设置成 failure)
            RestoreInProgress updatedRestoreInProgress = RestoreService.updateRestoreStateWithDeletedIndices(restoreInProgress, indices);
            if (updatedRestoreInProgress != restoreInProgress) {
                ImmutableOpenMap.Builder<String, ClusterState.Custom> builder = ImmutableOpenMap.builder(customs);
                builder.put(RestoreInProgress.TYPE, updatedRestoreInProgress);
                customs = builder.build();
            }
        }

        // 因为路由表发生了变化 所以要进行reroute
        return allocationService.reroute(
                ClusterState.builder(currentState)
                    .routingTable(routingTableBuilder.build())
                    .metadata(newMetadata)
                    .blocks(blocks)
                    .customs(customs)
                    .build(),
                "deleted indices [" + indices + "]");
    }
}
