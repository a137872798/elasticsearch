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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

import java.util.Collection;
import java.util.Collections;

/**
 * A composite {@link AllocationDecider} combining the "decision" of multiple
 * {@link AllocationDecider} implementations into a single allocation decision.
 * 代表多个决策对象组合的结果  每个决策对象都可以根据某个分片/节点  以及此时集群下所有分片的分配情况 进行适当的调整
 */
public class AllocationDeciders extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(AllocationDeciders.class);

    private final Collection<AllocationDecider> allocations;

    public AllocationDeciders(Collection<AllocationDecider> allocations) {
        this.allocations = Collections.unmodifiableCollection(allocations);
    }

    /**
     * 某个分片是否支持rebalance
     * @param shardRouting
     * @param allocation
     * @return
     */
    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        // 如果处于debug模式 只要携带信息的都必须加入
        // 如果处于非debug模式  1检测到no直接返回 2检查到ALWAYS忽略(因为这个yes不包含详细信息)
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canRebalance(shardRouting, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                // 当不需要输出日志信息时 直接返回 No对象
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    // 需要输出日志的情况就会添加到ret中
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        // 返回多个决定的共同结果
        return ret;
    }

    /**
     * 检测某个分片是否支持分配到该node上
     * @param shardRouting
     * @param node
     * @param allocation
     * @return
     */
    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        // 检测是否在 ignore 容器中 如果存在代表无法分配
        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            return Decision.NO;
        }
        Decision.Multi ret = new Decision.Multi();
        // 通过 decider对象进行决策
        for (AllocationDecider allocationDecider : allocations) {
            // 在这里就有检测某个node是否已经存在 该shardId 分片的逻辑
            Decision decision = allocationDecider.canAllocate(shardRouting, node, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Can not allocate [{}] on node [{}] due to [{}]",
                        shardRouting, node.node(), allocationDecider.getClass().getSimpleName());
                }
                // short circuit only if debugging is not enabled
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    // 以下的几个方法逻辑都是类似的

    /**
     * 某个分片能否在某个节点上继续保留  当处理有关分片迁移的时候需要调用的方法
     * @param shardRouting
     * @param node
     * @param allocation
     * @return
     */
    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            if (logger.isTraceEnabled()) {
                logger.trace("Shard [{}] should be ignored for node [{}]", shardRouting, node.nodeId());
            }
            return Decision.NO;
        }
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canRemain(shardRouting, node, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Shard [{}] can not remain on node [{}] due to [{}]",
                        shardRouting, node.nodeId(), allocationDecider.getClass().getSimpleName());
                }
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    /**
     * 当前索引能否分配在该node上
     * @param indexMetadata
     * @param node
     * @param allocation
     * @return
     */
    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canAllocate(indexMetadata, node, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    /**
     * 该分片是否应该存在该索引相关的分片
     * @param indexMetadata
     * @param node
     * @param allocation
     * @return
     */
    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.shouldAutoExpandToNode(indexMetadata, node, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                // 不需要输出日志信息 那么直接将该失败结果返回
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    // 如果需要输出日志信息 就将所有决策结果都包含进去
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    /**
     * 检测是否允许为某个分片进行分配
     * @param shardRouting
     * @param allocation  包含索引的元数据 作为分配的参考
     * @return
     */
    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canAllocate(shardRouting, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                // 如果不需要打印详细信息 直接返回NO 就可以
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    // 将结果合并 并在之后打印详细信息
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    /**
     * 检测能否将某个分片设置到 某个节点
     * @param node
     * @param allocation
     * @return
     */
    @Override
    public Decision canAllocate(RoutingNode node, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canAllocate(node, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    /**
     * 检测此时是否可以进行重平衡
     * @param allocation
     * @return
     */
    @Override
    public Decision canRebalance(RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider allocationDecider : allocations) {
            Decision decision = allocationDecider.canRebalance(allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard routing " + shardRouting;

        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            return Decision.NO;
        }
        Decision.Multi ret = new Decision.Multi();
        for (AllocationDecider decider : allocations) {
            Decision decision = decider.canForceAllocatePrimary(shardRouting, node, allocation);
            // short track if a NO is returned.
            if (decision == Decision.NO) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Shard [{}] can not be forcefully allocated to node [{}] due to [{}].",
                        shardRouting.shardId(), node.nodeId(), decider.getClass().getSimpleName());
                }
                if (!allocation.debugDecision()) {
                    return decision;
                } else {
                    ret.add(decision);
                }
            } else {
                addDecision(ret, decision, allocation);
            }
        }
        return ret;
    }

    private void addDecision(Decision.Multi ret, Decision decision, RoutingAllocation allocation) {
        // We never add ALWAYS decisions and only add YES decisions when requested by debug mode (since Multi default is YES).
        // always就不用打印日志了   当日志被关闭时 如果结果是YES 还是需要打印日志 加入到ret后 会打印日志
        if (decision != Decision.ALWAYS
            && (allocation.getDebugMode() == RoutingAllocation.DebugMode.ON || decision.type() != Decision.Type.YES)) {
            ret.add(decision);
        }
    }
}
