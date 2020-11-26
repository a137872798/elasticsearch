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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

/**
 * This {@link org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider} prevents shards that
 * are currently been snapshotted to be moved to other nodes.
 * TODO MOVE 和 ALLOCATION 的区别是什么???
 */
public class SnapshotInProgressAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(SnapshotInProgressAllocationDecider.class);

    public static final String NAME = "snapshot_in_progress";

    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * re-balanced to the given allocation. The default is
     * {@link Decision#ALWAYS}.
     */
    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return canMove(shardRouting, allocation);
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * allocated on the given node. The default is {@link Decision#ALWAYS}.
     */
    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canMove(shardRouting, allocation);
    }

    /**
     * 检测是否可以移动
     *
     * @param shardRouting
     * @param allocation
     * @return
     */
    private Decision canMove(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            // Only primary shards are snapshotted

            // 除了快照外 还有一个 RestoreInProgress
            SnapshotsInProgress snapshotsInProgress = allocation.custom(SnapshotsInProgress.TYPE);

            // 在主分片中 必须要确保快照操作处于暂停状态 才可以move
            if (snapshotsInProgress == null || snapshotsInProgress.entries().isEmpty()) {
                // Snapshots are not running
                return allocation.decision(Decision.YES, NAME, "no snapshots are currently running");
            }

            // 通过index进行匹配  匹配失败的可以正常move
            for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {
                SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus = snapshot.shards().get(shardRouting.shardId());
                // node 匹配 且 快照处于未完成状态 返回 Throttle
                if (shardSnapshotStatus != null && !shardSnapshotStatus.state().completed() && shardSnapshotStatus.nodeId() != null &&
                    shardSnapshotStatus.nodeId().equals(shardRouting.currentNodeId())) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Preventing snapshotted shard [{}] from being moved away from node [{}]",
                            shardRouting.shardId(), shardSnapshotStatus.nodeId());
                    }
                    return allocation.decision(Decision.THROTTLE, NAME,
                        "waiting for snapshotting of shard [%s] to complete on this node [%s]",
                        shardRouting.shardId(), shardSnapshotStatus.nodeId());
                }
            }
        }
        // 副本总是可以move
        return allocation.decision(Decision.YES, NAME, "the shard is not being snapshotted");
    }

}
