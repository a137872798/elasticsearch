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

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.util.set.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Replication group for a shard. Used by a primary shard to coordinate replication and recoveries.
 * 主分片会负责维护所有的副本此时的数据同步状态 每当主分片感知到集群变化时 就会更新该对象
 */
public class ReplicationGroup {

    /**
     * 该索引相关的路由表
     */
    private final IndexShardRoutingTable routingTable;

    /**
     * 已经变成start状态的副本会存储在该列表中 代表已经完成与主分片的同步
     */
    private final Set<String> inSyncAllocationIds;
    /**
     * 在往主分片写入数据时 会同时往该列表中所有副本写入数据
     */
    private final Set<String> trackedAllocationIds;
    private final long version;

    /**
     * unavailableInSyncShards + inSyncAllocationIds = allAllocationIds
     */
    private final Set<String> unavailableInSyncShards; // derived from the other fields

    /**
     * 需要继续同步数据的分片  因为在副本与主分片同步完数据后  主分片还会继续写入数据 副本需要继续同步数据
     */
    private final List<ShardRouting> replicationTargets; // derived from the other fields
    /**
     * unassigned 以及 未完成同步的分片会存储在这个容器中
     */
    private final List<ShardRouting> skippedShards; // derived from the other fields

    /**
     *
     * @param routingTable  记录某个索引下所有的分片 以及它们会被分配到哪里
     * @param inSyncAllocationIds    目前看来 sync 和 tracked的标识是一致的  sync代表此时已经与primary的数据完成同步 tracked代表会将写入到primary的操作同时写入到副本上
     * @param trackedAllocationIds
     * @param version 当前副本组的版本号 每次变化 版本号+1
     */
    public ReplicationGroup(IndexShardRoutingTable routingTable, Set<String> inSyncAllocationIds, Set<String> trackedAllocationIds,
                            long version) {
        this.routingTable = routingTable;
        this.inSyncAllocationIds = inSyncAllocationIds;
        this.trackedAllocationIds = trackedAllocationIds;
        this.version = version;

        // 这部分数据代表还未完成数据同步
        this.unavailableInSyncShards = Sets.difference(inSyncAllocationIds, routingTable.getAllAllocationIds());
        this.replicationTargets = new ArrayList<>();
        this.skippedShards = new ArrayList<>();
        // 遍历某个index下所有的分片
        for (final ShardRouting shard : routingTable) {
            if (shard.unassigned()) {
                assert shard.primary() == false : "primary shard should not be unassigned in a replication group: " + shard;
                skippedShards.add(shard);
            } else {
                // 根据不同的情况设置到不同的容器中

                // 代表需要继续同步最新写入的数据   这也暗示了处于relocating阶段的分片还是会继续同步primary的写入操作 因为只有当relocating结束时 才会从inSync中移除
                if (trackedAllocationIds.contains(shard.allocationId().getId())) {
                    replicationTargets.add(shard);
                } else {
                    // 从这个断言中可以看出  tracked 与 in-sync 应该是一样的
                    assert inSyncAllocationIds.contains(shard.allocationId().getId()) == false :
                        "in-sync shard copy but not tracked: " + shard;
                    skippedShards.add(shard);
                }
                // 在重定向尚未完成时 需要继续同步数据  并且此时 重定向后的target分片还处于init状态 会从leader处拉取数据
                if (shard.relocating()) {
                    ShardRouting relocationTarget = shard.getTargetRelocatingShard();
                    // 这步判断感觉有点多余 无非就是获取target分片并检测是否需要同步新写入leader的数据
                    if (trackedAllocationIds.contains(relocationTarget.allocationId().getId())) {
                        replicationTargets.add(relocationTarget);
                    } else {
                        skippedShards.add(relocationTarget);
                        assert inSyncAllocationIds.contains(relocationTarget.allocationId().getId()) == false :
                            "in-sync shard copy but not tracked: " + shard;
                    }
                }
            }
        }
    }

    public long getVersion() {
        return version;
    }

    public IndexShardRoutingTable getRoutingTable() {
        return routingTable;
    }

    public Set<String> getInSyncAllocationIds() {
        return inSyncAllocationIds;
    }

    public Set<String> getTrackedAllocationIds() {
        return trackedAllocationIds;
    }

    /**
     * Returns the set of shard allocation ids that are in the in-sync set but have no assigned routing entry
     */
    public Set<String> getUnavailableInSyncShards() {
        return unavailableInSyncShards;
    }

    /**
     * Returns the subset of shards in the routing table that should be replicated to. Includes relocation targets.
     */
    public List<ShardRouting> getReplicationTargets() {
        return replicationTargets;
    }

    /**
     * Returns the subset of shards in the routing table that are unassigned or initializing and not ready yet to receive operations
     * (i.e. engine not opened yet). Includes relocation targets.
     */
    public List<ShardRouting> getSkippedShards() {
        return skippedShards;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicationGroup that = (ReplicationGroup) o;

        if (!routingTable.equals(that.routingTable)) return false;
        if (!inSyncAllocationIds.equals(that.inSyncAllocationIds)) return false;
        return trackedAllocationIds.equals(that.trackedAllocationIds);
    }

    @Override
    public int hashCode() {
        int result = routingTable.hashCode();
        result = 31 * result + inSyncAllocationIds.hashCode();
        result = 31 * result + trackedAllocationIds.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ReplicationGroup{" +
            "routingTable=" + routingTable +
            ", inSyncAllocationIds=" + inSyncAllocationIds +
            ", trackedAllocationIds=" + trackedAllocationIds +
            '}';
    }

}
