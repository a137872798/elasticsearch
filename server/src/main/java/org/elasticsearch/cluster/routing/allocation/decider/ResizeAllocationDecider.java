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

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

/**
 * An allocation decider that ensures we allocate the shards of a target index for resize operations next to the source primaries
 * 分配决策会收到一个 RESIZE.INDEX 的影响
 */
public class ResizeAllocationDecider extends AllocationDecider {

    public static final String NAME = "resize";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return canAllocate(shardRouting, null, allocation);
    }

    /**
     * 是否允许将某个分片分配到某个node上
     * @param shardRouting
     * @param node  可以为null
     * @param allocation
     * @return
     */
    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        // 决策需要的信息都存储在 unassigned内
        final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
        // 当通过本地分片进行数据恢复时  TODO 怎么做??? 同一节点上不是不能存在2个shardId 相同的分片吗
        if (unassignedInfo != null && shardRouting.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            // we only make decisions here if we have an unassigned info and we have to recover from another index ie. split / shrink
            final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());

            // 获取NAME 为 index.resize.source.name 的索引
            Index resizeSourceIndex = indexMetadata.getResizeSourceIndex();
            assert resizeSourceIndex != null;
            // 如果没有获取到有关resize的index 无法进行处理 也就是默认存在该索引
            if (allocation.metadata().index(resizeSourceIndex) == null) {
                return allocation.decision(Decision.NO, NAME, "resize source index [%s] doesn't exists", resizeSourceIndex.toString());
            }

            // 当分片对应的索引设置的分片数量 小于 resize记录的最大值 返回YES （ALWAYS 就是 YES）
            IndexMetadata sourceIndexMetadata = allocation.metadata().getIndexSafe(resizeSourceIndex);
            if (indexMetadata.getNumberOfShards() < sourceIndexMetadata.getNumberOfShards()) {
                // this only handles splits and clone so far.
                return Decision.ALWAYS;
            }

            // 生成了 RESIZE index 下的某个分片
            ShardId shardId = indexMetadata.getNumberOfShards() == sourceIndexMetadata.getNumberOfShards() ?
                // 当分片数达到resize要求的值时   会返回 new ShardId(sourceIndexMetadata.getIndex(), shardId)
                IndexMetadata.selectCloneShard(shardRouting.id(), sourceIndexMetadata, indexMetadata.getNumberOfShards()) :
                // 当分片数量超过了resize要求的值时    返回 new ShardId(sourceIndexMetadata.getIndex(), shardId/一个超过1的值)
                IndexMetadata.selectSplitShard(shardRouting.id(), sourceIndexMetadata, indexMetadata.getNumberOfShards());

            // 可能之前在ES中会内置写入RESIZE 的分片
            ShardRouting sourceShardRouting = allocation.routingNodes().activePrimary(shardId);
            // 代表没有找到匹配的分片  就无法进行allocate
            if (sourceShardRouting == null) {
                return allocation.decision(Decision.NO, NAME, "source primary shard [%s] is not active", shardId);
            }
            if (node != null) { // we might get called from the 2 param canAllocate method..
                // 为什么一定要跟RESIZE索引在同一个node上
                if (sourceShardRouting.currentNodeId().equals(node.nodeId())) {
                    return allocation.decision(Decision.YES, NAME, "source primary is allocated on this node");
                } else {
                    return allocation.decision(Decision.NO, NAME, "source primary is allocated on another node");
                }
            } else {
                // 当没有要求 node时 总是返回true
                return allocation.decision(Decision.YES, NAME, "source primary is active");
            }
        }
        // 非LOCAL_SHARDS 类型就是直接返回 YES
        return super.canAllocate(shardRouting, node, allocation);
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard " + shardRouting;
        return canAllocate(shardRouting, node, allocation);
    }
}
