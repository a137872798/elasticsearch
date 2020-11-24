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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

import java.util.ArrayList;
import java.util.List;

/**
 * An abstract class that implements basic functionality for allocating
 * shards to nodes based on shard copies that already exist in the cluster.
 *
 * Individual implementations of this class are responsible for providing
 * the logic to determine to which nodes (if any) those shards are allocated.
 * 基础的网关分片分配器
 */
public abstract class BaseGatewayShardAllocator {

    protected final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * Allocate an unassigned shard to nodes (if any) where valid copies of the shard already exist.
     * It is up to the individual implementations of {@link #makeAllocationDecision(ShardRouting, RoutingAllocation, Logger)}
     * to make decisions on assigning shards to nodes.
     * @param shardRouting the shard to allocate
     * @param allocation the allocation state container object
     * @param unassignedAllocationHandler handles the allocation of the current shard
     *
     *                                    将某个unassigned的主分片分配到一个合适的位置
     */
    public void allocateUnassigned(ShardRouting shardRouting, RoutingAllocation allocation,
                                   ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler) {
        // allocateUnassignedDecision 决定分配的结果
        final AllocateUnassignedDecision allocateUnassignedDecision = makeAllocationDecision(shardRouting, allocation, logger);

        // 如果 处理没有产生结果 直接返回
        if (allocateUnassignedDecision.isDecisionTaken() == false) {
            // no decision was taken by this allocator
            return;
        }

        // 代表本次找到了一个合适的node
        if (allocateUnassignedDecision.getAllocationDecision() == AllocationDecision.YES) {
            // TODO 这里获取到的node 应该是还未分配该shard的节点  那么已经分配过的节点又是在什么时候排除的呢 推测是利用 ignoreNode机制
            unassignedAllocationHandler.initialize(allocateUnassignedDecision.getTargetNode().getId(),
                allocateUnassignedDecision.getAllocationId(),
                shardRouting.primary() ? ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE :
                    allocation.clusterInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                // 这个对象会观测某个分片的状态变化 比如某个新的unassigned 转换成init状态
                allocation.changes());
        } else {
            // 无法分配或者以异步形式处理 则可以从迭代器中移除这个分片了  同时会更新 unassignedInfo信息 以及触发相关的钩子
            unassignedAllocationHandler.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
        }
    }

    /**
     * Make a decision on the allocation of an unassigned shard.  This method is used by
     * {@link #allocateUnassigned(ShardRouting, RoutingAllocation, ExistingShardsAllocator.UnassignedAllocationHandler)} to make decisions
     * about whether or not the shard can be allocated by this allocator and if so, to which node it will be allocated.
     *
     * @param unassignedShard  the unassigned shard to allocate
     * @param allocation       the current routing state
     * @param logger           the logger
     * @return an {@link AllocateUnassignedDecision} with the final decision of whether to allocate and details of the decision
     */
    public abstract AllocateUnassignedDecision makeAllocationDecision(ShardRouting unassignedShard,
                                                                      RoutingAllocation allocation,
                                                                      Logger logger);

    /**
     * Builds decisions for all nodes in the cluster, so that the explain API can provide information on
     * allocation decisions for each node, while still waiting to allocate the shard (e.g. due to fetching shard data).
     */
    protected static List<NodeAllocationResult> buildDecisionsForAllNodes(ShardRouting shard, RoutingAllocation allocation) {
        List<NodeAllocationResult> results = new ArrayList<>();
        for (RoutingNode node : allocation.routingNodes()) {
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            results.add(new NodeAllocationResult(node.node(), null, decision));
        }
        return results;
    }
}
