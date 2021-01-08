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

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Observer that tracks changes made to RoutingNodes in order to update the primary terms and in-sync allocation ids in
 * {@link IndexMetadata} once the allocation round has completed.
 *
 * Primary terms are updated on primary initialization or when an active primary fails.
 *
 * Allocation ids are added for shards that become active and removed for shards that stop being active.
 * 记录某次分片变化的信息
 */
public class IndexMetadataUpdater extends RoutingChangesObserver.AbstractRoutingChangesObserver {

    /**
     * 在一次reroute过程中可能会有多个shard发生变化 都会被记录在这个容器中
     * 而当触发applyChanges时 会使用更新后的routingTable 来更新传入的metadata (这个metadata是一个元数据总集 内部包含了各种元数据信息)
     *
     * value 则是记录了在某次 reroute过程中分片的变化信息  比如这次变化是否有涉及到primary等等的关键信息
     */
    private final Map<ShardId, Updates> shardChanges = new HashMap<>();

    /**
     * 感知到某个分片从未分配状态转变成init状态 (完成了分片的分配任务)
     * @param unassignedShard
     * @param initializedShard
     */
    @Override
    public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
        assert initializedShard.isRelocationTarget() == false : "shardInitialized is not called on relocation target: " + initializedShard;

        // 只有主分片的分配需要被记录
        if (initializedShard.primary()) {

            // 每当某个主分片确定了要分配的node时  就可以增加主分片的任期了 代表主分片发生过一次变化
            increasePrimaryTerm(initializedShard.shardId());

            Updates updates = changes(initializedShard.shardId());
            assert updates.initializedPrimary == null : "Primary cannot be initialized more than once in same allocation round: " +
                "(previous: " + updates.initializedPrimary + ", next: " + initializedShard + ")";
            updates.initializedPrimary = initializedShard;
        }
    }

    /**
     * 某个之前处于初始状态的分片此时转换成启动状态
     * 在从 init -> started 的过程中 经历了数据恢复阶段
     * @param initializingShard
     * @param startedShard
     */
    @Override
    public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
        assert Objects.equals(initializingShard.allocationId().getId(), startedShard.allocationId().getId())
            : "initializingShard.allocationId [" + initializingShard.allocationId().getId()
            + "] and startedShard.allocationId [" + startedShard.allocationId().getId() + "] have to have the same";

        // update对象对应某个shardId 在某次操作中所有的变化记录
        Updates updates = changes(startedShard.shardId());

        // 当分片处于启动状态后 就需要进入到 in-sync 队列
        updates.addedAllocationIds.add(startedShard.allocationId().getId());

        // TODO 目前还不清楚什么时候会使用FORCE_STALE_PRIMARY_INSTANCE 这种恢复源
        if (startedShard.primary()
            // started shard has to have null recoverySource; have to pick up recoverySource from its initializing state
            && (initializingShard.recoverySource() == RecoverySource.ExistingStoreRecoverySource.FORCE_STALE_PRIMARY_INSTANCE)) {
            updates.removedAllocationIds.add(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID);
        }
    }

    /**
     * 某个分片的处理失败了
     * @param failedShard  本次处理失败的分片
     * @param unassignedInfo  该对象内部会记录失败的原因
     */
    @Override
    public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {

        // 如果当前分片是主分片才处理
        if (failedShard.active() && failedShard.primary()) {
            Updates updates = changes(failedShard.shardId());
            if (updates.firstFailedPrimary == null) {
                // more than one primary can be failed (because of batching, primary can be failed, replica promoted and then failed...)
                updates.firstFailedPrimary = failedShard;
            }
            // 代表在这次reroute中 主分片发生了一次变化
            increasePrimaryTerm(failedShard.shardId());
        }
    }

    /**
     * 当完成重定位后  会被移出 in-sync集合   应该是代表此时分片处于非start状态吧
     * @param removedRelocationSource
     */
    @Override
    public void relocationCompleted(ShardRouting removedRelocationSource) {
        removeAllocationId(removedRelocationSource);
    }

    /**
     * Updates the current {@link Metadata} based on the changes of this RoutingChangesObserver. Specifically
     * we update {@link IndexMetadata#getInSyncAllocationIds()} and {@link IndexMetadata#primaryTerm(int)} based on
     * the changes made during this allocation.
     *
     * @param oldMetadata {@link Metadata} object from before the routing nodes was changed.   之前的元数据信息
     * @param newRoutingTable {@link RoutingTable} object after routing changes were applied.   此时最新的路由表
     * @return adapted {@link Metadata}, potentially the original one if no change was needed.
     *
     * 根据此时最新的路由表信息更新  Metadata
     */
    public Metadata applyChanges(Metadata oldMetadata, RoutingTable newRoutingTable) {
        // 获取在操作过程中更新的分片
        Map<Index, List<Map.Entry<ShardId, Updates>>> changesGroupedByIndex =
            shardChanges.entrySet().stream().collect(Collectors.groupingBy(e -> e.getKey().getIndex()));

        Metadata.Builder metadataBuilder = null;
        for (Map.Entry<Index, List<Map.Entry<ShardId, Updates>>> indexChanges : changesGroupedByIndex.entrySet()) {
            Index index = indexChanges.getKey();
            // 获取该索引之前的元数据信息
            final IndexMetadata oldIndexMetadata = oldMetadata.getIndexSafe(index);
            IndexMetadata.Builder indexMetadataBuilder = null;
            for (Map.Entry<ShardId, Updates> shardEntry : indexChanges.getValue()) {
                ShardId shardId = shardEntry.getKey();
                Updates updates = shardEntry.getValue();

                // 某些分片变成start状态后 就允许接收此时写入到主分片的数据了 被称为 in-sync
                // 这里是根据分片的状态变化 更新 in-sync中的分片
                indexMetadataBuilder = updateInSyncAllocations(newRoutingTable, oldIndexMetadata, indexMetadataBuilder, shardId, updates);
                // 更新 primaryTerm
                indexMetadataBuilder = updatePrimaryTerm(oldIndexMetadata, indexMetadataBuilder, shardId, updates);
            }

            if (indexMetadataBuilder != null) {
                if (metadataBuilder == null) {
                    metadataBuilder = Metadata.builder(oldMetadata);
                }
                metadataBuilder.put(indexMetadataBuilder);
            }
        }

        if (metadataBuilder != null) {
            return metadataBuilder.build();
        } else {
            return oldMetadata;
        }
    }

    /**
     * Updates in-sync allocations with routing changes that were made to the routing table.
     * @param newRoutingTable   当分片状态修改后此时最新的路由表信息
     * @param oldIndexMetadata   本次待更新的元数据
     * @param indexMetadataBuilder 通过该对象构建新的元数据
     * @param shardId   本次更新的是哪个分片id
     * @param updates   包含了更新的具体数据
     *
     *                更新 in-sync 队列中的分片
     */
    private IndexMetadata.Builder updateInSyncAllocations(RoutingTable newRoutingTable, IndexMetadata oldIndexMetadata,
                                                          IndexMetadata.Builder indexMetadataBuilder, ShardId shardId, Updates updates) {
        assert Sets.haveEmptyIntersection(updates.addedAllocationIds, updates.removedAllocationIds) :
            "allocation ids cannot be both added and removed in the same allocation round, added ids: " +
                updates.addedAllocationIds + ", removed ids: " + updates.removedAllocationIds;

        // 获取此时本分片下 所有处于in-sync的分片
        Set<String> oldInSyncAllocationIds = oldIndexMetadata.inSyncAllocationIds(shardId.id());

        // check if we have been force-initializing an empty primary or a stale primary
        // TODO 目前还不清楚怎么出现主分片为init 并且 inSync不为空
        if (updates.initializedPrimary != null && oldInSyncAllocationIds.isEmpty() == false &&
            oldInSyncAllocationIds.contains(updates.initializedPrimary.allocationId().getId()) == false) {
            // we're not reusing an existing in-sync allocation id to initialize a primary, which means that we're either force-allocating
            // an empty or a stale primary (see AllocateEmptyPrimaryAllocationCommand or AllocateStalePrimaryAllocationCommand).

            // 获取主分片采用的数据恢复策略 (主分片一般都是从磁盘恢复数据)
            RecoverySource recoverySource = updates.initializedPrimary.recoverySource();
            RecoverySource.Type recoverySourceType = recoverySource.getType();
            boolean emptyPrimary = recoverySourceType == RecoverySource.Type.EMPTY_STORE;
            assert updates.addedAllocationIds.isEmpty() : (emptyPrimary ? "empty" : "stale") +
                " primary is not force-initialized in same allocation round where shards are started";

            // 如果此时builder还没有创建  基于旧的元数据构建builder对象
            if (indexMetadataBuilder == null) {
                indexMetadataBuilder = IndexMetadata.builder(oldIndexMetadata);
            }
            // 如果这个主分片内部不包含数据  也就是恢复源为空  这个时候将整个同步集合清空
            if (emptyPrimary) {
                // forcing an empty primary resets the in-sync allocations to the empty set (ShardRouting.allocatedPostIndexCreate)
                // 将这个分片的分配者置空
                indexMetadataBuilder.putInSyncAllocationIds(shardId.id(), Collections.emptySet());
            } else {
                final String allocationId;
                // 如果使用的是 FORCE_STALE_PRIMARY_INSTANCE 这种恢复方式   allocationId 就是一个特殊值
                if (recoverySource == RecoverySource.ExistingStoreRecoverySource.FORCE_STALE_PRIMARY_INSTANCE) {
                    allocationId = RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID;
                } else {
                    assert recoverySource instanceof RecoverySource.SnapshotRecoverySource : recoverySource;
                    allocationId = updates.initializedPrimary.allocationId().getId();
                }
                // forcing a stale primary resets the in-sync allocations to the singleton set with the stale id
                // 当某个分片从init->start后 就会加入到in-sync 队列中
                indexMetadataBuilder.putInSyncAllocationIds(shardId.id(), Collections.singleton(allocationId));
            }

        } else {
            // standard path for updating in-sync ids
            // 更新in-sync 容器
            Set<String> inSyncAllocationIds = new HashSet<>(oldInSyncAllocationIds);
            inSyncAllocationIds.addAll(updates.addedAllocationIds);
            // 当某些位于 inSync的副本写入失败时  会变成unassigned 并等待重新分配 可能在本轮就会变回 init   如果副本此时在inSync中 就会被移除
            inSyncAllocationIds.removeAll(updates.removedAllocationIds);

            assert oldInSyncAllocationIds.contains(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID) == false
                || inSyncAllocationIds.contains(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID) == false :
                "fake allocation id has to be removed, inSyncAllocationIds:" + inSyncAllocationIds;

            // Prevent set of inSyncAllocationIds to grow unboundedly. This can happen for example if we don't write to a primary
            // but repeatedly shut down nodes that have active replicas.
            // We use number_of_replicas + 1 (= possible active shard copies) to bound the inSyncAllocationIds set
            // Only trim the set of allocation ids when it grows, otherwise we might trim too eagerly when the number
            // of replicas was decreased while shards were unassigned.

            // 计算总数
            int maxActiveShards = oldIndexMetadata.getNumberOfReplicas() + 1; // +1 for the primary

            // 获取对应的路由表
            IndexShardRoutingTable newShardRoutingTable = newRoutingTable.shardRoutingTable(shardId);
            assert newShardRoutingTable.assignedShards().stream()
                .filter(ShardRouting::isRelocationTarget).map(s -> s.allocationId().getId()).noneMatch(inSyncAllocationIds::contains)
                : newShardRoutingTable.assignedShards() + " vs " + inSyncAllocationIds;

            // 如果此时in-sync集合中的分片数 超过了 该shardId 对应的分片总数  下面这块代码就是将 in-sync的数量减小到至多 maxActiveShard
            if (inSyncAllocationIds.size() > oldInSyncAllocationIds.size() && inSyncAllocationIds.size() > maxActiveShards) {
                // trim entries that have no corresponding shard routing in the cluster state (i.e. trim unavailable copies)
                List<ShardRouting> assignedShards = newShardRoutingTable.assignedShards()
                    .stream().filter(s -> s.isRelocationTarget() == false).collect(Collectors.toList());
                assert assignedShards.size() <= maxActiveShards :
                    "cannot have more assigned shards " + assignedShards + " than maximum possible active shards " + maxActiveShards;
                Set<String> assignedAllocations = assignedShards.stream().map(s -> s.allocationId().getId()).collect(Collectors.toSet());
                inSyncAllocationIds = inSyncAllocationIds.stream()
                    .sorted(Comparator.comparing(assignedAllocations::contains).reversed()) // values with routing entries first
                    .limit(maxActiveShards)
                    .collect(Collectors.toSet());
            }

            // only remove allocation id of failed active primary if there is at least one active shard remaining. Assume for example that
            // the primary fails but there is no new primary to fail over to. If we were to remove the allocation id of the primary from the
            // in-sync set, this could create an empty primary on the next allocation.
            // 如果仅剩一个失败的主分片 还是保留在 inSync中
            if (newShardRoutingTable.activeShards().isEmpty() && updates.firstFailedPrimary != null) {
                // add back allocation id of failed primary
                inSyncAllocationIds.add(updates.firstFailedPrimary.allocationId().getId());
            }

            assert inSyncAllocationIds.isEmpty() == false || oldInSyncAllocationIds.isEmpty() :
                "in-sync allocations cannot become empty after they have been non-empty: " + oldInSyncAllocationIds;

            // be extra safe here and only update in-sync set if it is non-empty
            // 覆盖之前的in-sync集合
            if (inSyncAllocationIds.isEmpty() == false) {
                if (indexMetadataBuilder == null) {
                    indexMetadataBuilder = IndexMetadata.builder(oldIndexMetadata);
                }
                indexMetadataBuilder.putInSyncAllocationIds(shardId.id(), inSyncAllocationIds);
            }
        }
        return indexMetadataBuilder;
    }

    /**
     * Removes allocation ids from the in-sync set for shard copies for which there is no routing entries in the routing table.
     * This method is called in AllocationService before any changes to the routing table are made.
     * @param clusterState 当前集群状态信息
     * @param staleShards 此时已经过期的分片
     *                    某些分片数据在 indexMetadata中已经过期 需要清理
     *                    通过 shardId + allocationId 进行定位
     */
    public static ClusterState removeStaleIdsWithoutRoutings(ClusterState clusterState, List<StaleShard> staleShards, Logger logger) {
        Metadata oldMetadata = clusterState.metadata();
        RoutingTable oldRoutingTable = clusterState.routingTable();
        Metadata.Builder metadataBuilder = null;
        // group staleShards entries by index
        // 以index 分组进行清理
        for (Map.Entry<Index, List<StaleShard>> indexEntry : staleShards.stream().collect(
            Collectors.groupingBy(fs -> fs.getShardId().getIndex())).entrySet()) {
            final IndexMetadata oldIndexMetadata = oldMetadata.getIndexSafe(indexEntry.getKey());
            IndexMetadata.Builder indexMetadataBuilder = null;
            // group staleShards entries by shard id
            // 之后按照shardId 进行分组
            for (Map.Entry<ShardId, List<StaleShard>> shardEntry : indexEntry.getValue().stream().collect(
                Collectors.groupingBy(staleShard -> staleShard.getShardId())).entrySet()) {
                int shardNumber = shardEntry.getKey().getId();

                // 定位到shardId 组的 inSync 集合
                Set<String> oldInSyncAllocations = oldIndexMetadata.inSyncAllocationIds(shardNumber);

                // 找到所有要移除的分片
                Set<String> idsToRemove = shardEntry.getValue().stream().map(e -> e.getAllocationId()).collect(Collectors.toSet());
                assert idsToRemove.stream().allMatch(id -> oldRoutingTable.getByAllocationId(shardEntry.getKey(), id) == null) :
                    "removing stale ids: " + idsToRemove + ", some of which have still a routing entry: " + oldRoutingTable;

                // 移除后最新的in-sync 容器
                Set<String> remainingInSyncAllocations = Sets.difference(oldInSyncAllocations, idsToRemove);
                assert remainingInSyncAllocations.isEmpty() == false : "Set of in-sync ids cannot become empty for shard " +
                    shardEntry.getKey() + " (before: " + oldInSyncAllocations + ", ids to remove: " + idsToRemove + ")";
                // be extra safe here: if the in-sync set were to become empty, this would create an empty primary on the next allocation
                // (see ShardRouting#allocatedPostIndexCreate)
                if (remainingInSyncAllocations.isEmpty() == false) {
                    if (indexMetadataBuilder == null) {
                        indexMetadataBuilder = IndexMetadata.builder(oldIndexMetadata);
                    }
                    // 覆盖之前的 in-sync容器
                    indexMetadataBuilder.putInSyncAllocationIds(shardNumber, remainingInSyncAllocations);
                }
                logger.warn("{} marking unavailable shards as stale: {}", shardEntry.getKey(), idsToRemove);
            }

            if (indexMetadataBuilder != null) {
                if (metadataBuilder == null) {
                    metadataBuilder = Metadata.builder(oldMetadata);
                }
                metadataBuilder.put(indexMetadataBuilder);
            }
        }

        if (metadataBuilder != null) {
            return ClusterState.builder(clusterState).metadata(metadataBuilder).build();
        } else {
            return clusterState;
        }
    }

    /**
     * Increases the primary term if {@link #increasePrimaryTerm} was called for this shard id.
     * 更新indexMetadata.primaryTerm  每次在 reroute中如果主分片发生了变化 就会增大term
     */
    private IndexMetadata.Builder updatePrimaryTerm(IndexMetadata oldIndexMetadata, IndexMetadata.Builder indexMetadataBuilder,
                                                    ShardId shardId, Updates updates) {
        if (updates.increaseTerm) {
            if (indexMetadataBuilder == null) {
                indexMetadataBuilder = IndexMetadata.builder(oldIndexMetadata);
            }
            indexMetadataBuilder.primaryTerm(shardId.id(), oldIndexMetadata.primaryTerm(shardId.id()) + 1);
        }
        return indexMetadataBuilder;
    }

    /**
     * Helper method that creates update entry for the given shard id if such an entry does not exist yet.
     */
    private Updates changes(ShardId shardId) {
        return shardChanges.computeIfAbsent(shardId, k -> new Updates());
    }

    /**
     * Remove allocation id of this shard from the set of in-sync shard copies
     * 记录一组之后会移除的分片
     */
    void removeAllocationId(ShardRouting shardRouting) {
        // 首先该分片已经完成了数据恢复才有可能进入 inSync 才有移除的必要
        if (shardRouting.active()) {
            changes(shardRouting.shardId()).removedAllocationIds.add(shardRouting.allocationId().getId());
        }
    }

    /**
     * Increase primary term for this shard id
     */
    private void increasePrimaryTerm(ShardId shardId) {
        changes(shardId).increaseTerm = true;
    }


    /**
     * 该对象用于描述分片的变化  一个RoutingAllocation对象 对应一次数据的变化(可能是多个请求)
     */
    private static class Updates {
        /**
         * 每当主分片分配到了一个新的节点时 需要更新任期
         */
        private boolean increaseTerm; // whether primary term should be increased
        /**
         * 当某些分片从init状态变成start状态后 就要开始跟随主分片并同步数据了  在索引操作阶段就会将数据同步发送到 tracked为true的分片上
         */
        private Set<String> addedAllocationIds = new HashSet<>(); // allocation ids that should be added to the in-sync set

        /**
         * 存储所有要移出 in-sync容器的分片对应的allocationId
         */
        private Set<String> removedAllocationIds = new HashSet<>(); // allocation ids that should be removed from the in-sync set
        /**
         * 某次 reroute过程中 某个主分片变成了init状态
         */
        private ShardRouting initializedPrimary = null; // primary that was initialized from unassigned

        /**
         * 在本次处理过程中 主分片处理失败 比如恢复数据失败 或者进行索引操作时失败
         */
        private ShardRouting firstFailedPrimary = null; // first active primary that was failed
    }
}
