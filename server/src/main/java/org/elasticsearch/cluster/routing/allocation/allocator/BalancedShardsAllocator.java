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

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.MoveDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.PriorityComparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;

/**
 * The {@link BalancedShardsAllocator} re-balances the nodes allocations
 * within an cluster based on a {@link WeightFunction}. The clusters balance is defined by four parameters which can be set
 * in the cluster update API that allows changes in real-time:
 * <ul><li><code>cluster.routing.allocation.balance.shard</code> - The <b>shard balance</b> defines the weight factor
 * for shards allocated on a {@link RoutingNode}</li>
 * <li><code>cluster.routing.allocation.balance.index</code> - The <b>index balance</b> defines a factor to the number
 * of {@link org.elasticsearch.cluster.routing.ShardRouting}s per index allocated on a specific node</li>
 * <li><code>cluster.routing.allocation.balance.threshold</code> - A <b>threshold</b> to set the minimal optimization
 * value of operations that should be performed</li>
 * </ul>
 * <p>
 * These parameters are combined in a {@link WeightFunction} that allows calculation of node weights which
 * are used to re-balance shards based on global as well as per-index factors.
 * 该对象允许将分片重新定位到不同的节点上
 */
public class BalancedShardsAllocator implements ShardsAllocator {

    private static final Logger logger = LogManager.getLogger(BalancedShardsAllocator.class);

    public static final Setting<Float> INDEX_BALANCE_FACTOR_SETTING =
        Setting.floatSetting("cluster.routing.allocation.balance.index", 0.55f, 0.0f, Property.Dynamic, Property.NodeScope);
    public static final Setting<Float> SHARD_BALANCE_FACTOR_SETTING =
        Setting.floatSetting("cluster.routing.allocation.balance.shard", 0.45f, 0.0f, Property.Dynamic, Property.NodeScope);
    public static final Setting<Float> THRESHOLD_SETTING =
        Setting.floatSetting("cluster.routing.allocation.balance.threshold", 1.0f, 0.0f,
            Property.Dynamic, Property.NodeScope);

    /**
     * 该函数可以基于 index/shard 因子计算权重值
     */
    private volatile WeightFunction weightFunction;

    /**
     * 获取阈值信息
     */
    private volatile float threshold;

    public BalancedShardsAllocator(Settings settings) {
        this(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }


    @Inject
    public BalancedShardsAllocator(Settings settings, ClusterSettings clusterSettings) {
        // 取出index 和 shard的平衡因子 初始化生成权重的对象
        setWeightFunction(INDEX_BALANCE_FACTOR_SETTING.get(settings), SHARD_BALANCE_FACTOR_SETTING.get(settings));
        setThreshold(THRESHOLD_SETTING.get(settings));

        // 注册2个会动态变化的配置
        clusterSettings.addSettingsUpdateConsumer(INDEX_BALANCE_FACTOR_SETTING, SHARD_BALANCE_FACTOR_SETTING, this::setWeightFunction);
        clusterSettings.addSettingsUpdateConsumer(THRESHOLD_SETTING, this::setThreshold);
    }

    private void setWeightFunction(float indexBalance, float shardBalanceFactor) {
        weightFunction = new WeightFunction(indexBalance, shardBalanceFactor);
    }

    private void setThreshold(float threshold) {
        this.threshold = threshold;
    }

    /**
     * 根据当前分片在集群中的分配状态进行重分配
     * @param allocation current node allocation
     */
    @Override
    public void allocate(RoutingAllocation allocation) {
        // 代表此时集群中所有分片都处于未分配状态
        if (allocation.routingNodes().size() == 0) {
            // 将 unassignedInfo 修改成decide_no 状态  TODO 为什么???
            failAllocationOfNewPrimaries(allocation);
            return;
        }
        final Balancer balancer = new Balancer(logger, allocation, weightFunction, threshold);
        balancer.allocateUnassigned();
        balancer.moveShards();
        balancer.balance();
    }

    @Override
    public ShardAllocationDecision decideShardAllocation(final ShardRouting shard, final RoutingAllocation allocation) {
        Balancer balancer = new Balancer(logger, allocation, weightFunction, threshold);
        AllocateUnassignedDecision allocateUnassignedDecision = AllocateUnassignedDecision.NOT_TAKEN;
        MoveDecision moveDecision = MoveDecision.NOT_TAKEN;
        if (shard.unassigned()) {
            allocateUnassignedDecision = balancer.decideAllocateUnassigned(shard, Sets.newHashSet());
        } else {
            moveDecision = balancer.decideMove(shard);
            if (moveDecision.isDecisionTaken() && moveDecision.canRemain()) {
                MoveDecision rebalanceDecision = balancer.decideRebalance(shard);
                moveDecision = rebalanceDecision.withRemainDecision(moveDecision.getCanRemainDecision());
            }
        }
        return new ShardAllocationDecision(allocateUnassignedDecision, moveDecision);
    }

    /**
     * 代表集群中都是为分配的分片
     * @param allocation
     */
    private void failAllocationOfNewPrimaries(RoutingAllocation allocation) {
        RoutingNodes routingNodes = allocation.routingNodes();
        assert routingNodes.size() == 0 : routingNodes;
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            final ShardRouting shardRouting = unassignedIterator.next();
            final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
            // 只会处理主分片 并且要求未分配的原因是还没有开始分配  而不是分配失败
            if (shardRouting.primary() && unassignedInfo.getLastAllocationStatus() == AllocationStatus.NO_ATTEMPT) {
                // 更新未分配信息 同时触发Observer   (这里将所有状态修改成 DECIDERS_NO)
                unassignedIterator.updateUnassigned(new UnassignedInfo(unassignedInfo.getReason(), unassignedInfo.getMessage(),
                        unassignedInfo.getFailure(), unassignedInfo.getNumFailedAllocations(), unassignedInfo.getUnassignedTimeInNanos(),
                        unassignedInfo.getUnassignedTimeInMillis(), unassignedInfo.isDelayed(), AllocationStatus.DECIDERS_NO,
                        unassignedInfo.getFailedNodeIds()),
                    shardRouting.recoverySource(), allocation.changes());
            }
        }
    }

    /**
     * Returns the currently configured delta threshold
     */
    public float getThreshold() {
        return threshold;
    }

    /**
     * Returns the index related weight factor.
     */
    public float getIndexBalance() {
        return weightFunction.indexBalance;
    }

    /**
     * Returns the shard related weight factor.
     */
    public float getShardBalance() {
        return weightFunction.shardBalance;
    }


    /**
     * This class is the primary weight function used to create balanced over nodes and shards in the cluster.
     * Currently this function has 3 properties:
     * <ul>
     * <li><code>index balance</code> - balance property over shards per index</li>
     * <li><code>shard balance</code> - balance property over shards per cluster</li>
     * </ul>
     * <p>
     * Each of these properties are expressed as factor such that the properties factor defines the relative
     * importance of the property for the weight function. For example if the weight function should calculate
     * the weights only based on a global (shard) balance the index balance can be set to {@code 0.0} and will
     * in turn have no effect on the distribution.
     * </p>
     * The weight per index is calculated based on the following formula:
     * <ul>
     * <li>
     * <code>weight<sub>index</sub>(node, index) = indexBalance * (node.numShards(index) - avgShardsPerNode(index))</code>
     * </li>
     * <li>
     * <code>weight<sub>node</sub>(node, index) = shardBalance * (node.numShards() - avgShardsPerNode)</code>
     * </li>
     * </ul>
     * <code>weight(node, index) = weight<sub>index</sub>(node, index) + weight<sub>node</sub>(node, index)</code>
     * 基于2个维度计算权重值 一个是某节点的分片数量与其他节点作比较
     * 一个是节点下某个索引的分片数量
     */
    private static class WeightFunction {

        private final float indexBalance;
        private final float shardBalance;
        private final float theta0;
        private final float theta1;

        /**
         * 通过2个平衡因子进行初始化
         * @param indexBalance
         * @param shardBalance
         */
        WeightFunction(float indexBalance, float shardBalance) {
            float sum = indexBalance + shardBalance;
            if (sum <= 0.0f) {
                throw new IllegalArgumentException("Balance factors must sum to a value > 0 but was: " + sum);
            }
            theta0 = shardBalance / sum;
            theta1 = indexBalance / sum;
            this.indexBalance = indexBalance;
            this.shardBalance = shardBalance;
        }

        /**
         * 该方法负责 计算某个节点上某个索引相关的数据已经占了多少权重
         * @param balancer
         * @param node
         * @param index
         * @return
         */
        float weight(Balancer balancer, ModelNode node, String index) {
            // 该节点相较于其他节点的分片数量
            final float weightShard = node.numShards() - balancer.avgShardsPerNode();
            final float weightIndex = node.numShards(index) - balancer.avgShardsPerNode(index);
            return theta0 * weightShard + theta1 * weightIndex;
        }
    }

    /**
     * A {@link Balancer}
     * 重分配对象
     */
    public static class Balancer {
        private final Logger logger;
        /**
         * 将当前集群中已经分配好的分片 包装成ModelNode
         */
        private final Map<String, ModelNode> nodes;
        private final RoutingAllocation allocation;
        /**
         * 将所有分片信息 按照node划分
         */
        private final RoutingNodes routingNodes;
        private final WeightFunction weight;

        private final float threshold;
        /**
         * 该对象内部包含了所有索引的元数据信息
         */
        private final Metadata metadata;
        /**
         * 平均每个节点上有多少分片
         */
        private final float avgShardsPerNode;

        /**
         * 该对象负责将node 排序
         */
        private final NodeSorter sorter;

        public Balancer(Logger logger, RoutingAllocation allocation, WeightFunction weight, float threshold) {
            this.logger = logger;
            this.allocation = allocation;
            this.weight = weight;
            this.threshold = threshold;
            this.routingNodes = allocation.routingNodes();
            this.metadata = allocation.metadata();
            avgShardsPerNode = ((float) metadata.getTotalNumberOfShards()) / routingNodes.size();
            nodes = Collections.unmodifiableMap(buildModelFromAssigned());
            sorter = newNodeSorter();
        }

        /**
         * Returns an array view on the nodes in the balancer. Nodes should not be removed from this list.
         */
        private ModelNode[] nodesArray() {
            return nodes.values().toArray(new ModelNode[nodes.size()]);
        }

        /**
         * Returns the average of shards per node for the given index
         */
        public float avgShardsPerNode(String index) {
            return ((float) metadata.index(index).getTotalNumberOfShards()) / nodes.size();
        }

        /**
         * Returns the global average of shards per node
         */
        public float avgShardsPerNode() {
            return avgShardsPerNode;
        }

        /**
         * Returns a new {@link NodeSorter} that sorts the nodes based on their
         * current weight with respect to the index passed to the sorter. The
         * returned sorter is not sorted. Use {@link NodeSorter#reset(String)}
         * to sort based on an index.
         */
        private NodeSorter newNodeSorter() {
            return new NodeSorter(nodesArray(), weight, this);
        }

        /**
         * The absolute value difference between two weights.
         */
        private static float absDelta(float lower, float higher) {
            assert higher >= lower : higher + " lt " + lower +" but was expected to be gte";
            return Math.abs(higher - lower);
        }

        /**
         * Returns {@code true} iff the weight delta between two nodes is under a defined threshold.
         * See {@link #THRESHOLD_SETTING} for defining the threshold.
         */
        private static boolean lessThan(float delta, float threshold) {
            /* deltas close to the threshold are "rounded" to the threshold manually
               to prevent floating point problems if the delta is very close to the
               threshold ie. 1.000000002 which can trigger unnecessary balance actions*/
            return delta <= (threshold + 0.001f);
        }

        /**
         * Balances the nodes on the cluster model according to the weight function.
         * The actual balancing is delegated to {@link #balanceByWeights()}
         */
        private void balance() {
            if (logger.isTraceEnabled()) {
                logger.trace("Start balancing cluster");
            }
            if (allocation.hasPendingAsyncFetch()) {
                /*
                 * see https://github.com/elastic/elasticsearch/issues/14387
                 * if we allow rebalance operations while we are still fetching shard store data
                 * we might end up with unnecessary rebalance operations which can be super confusion/frustrating
                 * since once the fetches come back we might just move all the shards back again.
                 * Therefore we only do a rebalance if we have fetched all information.
                 */
                logger.debug("skipping rebalance due to in-flight shard/store fetches");
                return;
            }
            if (allocation.deciders().canRebalance(allocation).type() != Type.YES) {
                logger.trace("skipping rebalance as it is disabled");
                return;
            }
            if (nodes.size() < 2) { /* skip if we only have one node */
                logger.trace("skipping rebalance as single node only");
                return;
            }
            balanceByWeights();
        }

        /**
         * Makes a decision about moving a single shard to a different node to form a more
         * optimally balanced cluster.  This method is invoked from the cluster allocation
         * explain API only.
         */
        private MoveDecision decideRebalance(final ShardRouting shard) {
            if (shard.started() == false) {
                // we can only rebalance started shards
                return MoveDecision.NOT_TAKEN;
            }

            Decision canRebalance = allocation.deciders().canRebalance(shard, allocation);

            sorter.reset(shard.getIndexName());
            ModelNode[] modelNodes = sorter.modelNodes;
            final String currentNodeId = shard.currentNodeId();
            // find currently assigned node
            ModelNode currentNode = null;
            for (ModelNode node : modelNodes) {
                if (node.getNodeId().equals(currentNodeId)) {
                    currentNode = node;
                    break;
                }
            }
            assert currentNode != null : "currently assigned node could not be found";

            // balance the shard, if a better node can be found
            final String idxName = shard.getIndexName();
            final float currentWeight = weight.weight(this, currentNode, idxName);
            final AllocationDeciders deciders = allocation.deciders();
            Type rebalanceDecisionType = Type.NO;
            ModelNode assignedNode = null;
            List<Tuple<ModelNode, Decision>> betterBalanceNodes = new ArrayList<>();
            List<Tuple<ModelNode, Decision>> sameBalanceNodes = new ArrayList<>();
            List<Tuple<ModelNode, Decision>> worseBalanceNodes = new ArrayList<>();
            for (ModelNode node : modelNodes) {
                if (node == currentNode) {
                    continue; // skip over node we're currently allocated to
                }
                final Decision canAllocate = deciders.canAllocate(shard, node.getRoutingNode(), allocation);
                // the current weight of the node in the cluster, as computed by the weight function;
                // this is a comparison of the number of shards on this node to the number of shards
                // that should be on each node on average (both taking the cluster as a whole into account
                // as well as shards per index)
                final float nodeWeight = weight.weight(this, node, idxName);
                // if the node we are examining has a worse (higher) weight than the node the shard is
                // assigned to, then there is no way moving the shard to the node with the worse weight
                // can make the balance of the cluster better, so we check for that here
                final boolean betterWeightThanCurrent = nodeWeight <= currentWeight;
                boolean rebalanceConditionsMet = false;
                if (betterWeightThanCurrent) {
                    // get the delta between the weights of the node we are checking and the node that holds the shard
                    float currentDelta = absDelta(nodeWeight, currentWeight);
                    // checks if the weight delta is above a certain threshold; if it is not above a certain threshold,
                    // then even though the node we are examining has a better weight and may make the cluster balance
                    // more even, it doesn't make sense to execute the heavyweight operation of relocating a shard unless
                    // the gains make it worth it, as defined by the threshold
                    boolean deltaAboveThreshold = lessThan(currentDelta, threshold) == false;
                    // calculate the delta of the weights of the two nodes if we were to add the shard to the
                    // node in question and move it away from the node that currently holds it.
                    boolean betterWeightWithShardAdded = nodeWeight + 1.0f < currentWeight;
                    rebalanceConditionsMet = deltaAboveThreshold && betterWeightWithShardAdded;
                    // if the simulated weight delta with the shard moved away is better than the weight delta
                    // with the shard remaining on the current node, and we are allowed to allocate to the
                    // node in question, then allow the rebalance
                    if (rebalanceConditionsMet && canAllocate.type().higherThan(rebalanceDecisionType)) {
                        // rebalance to the node, only will get overwritten if the decision here is to
                        // THROTTLE and we get a decision with YES on another node
                        rebalanceDecisionType = canAllocate.type();
                        assignedNode = node;
                    }
                }
                Tuple<ModelNode, Decision> nodeResult = Tuple.tuple(node, canAllocate);
                if (rebalanceConditionsMet) {
                    betterBalanceNodes.add(nodeResult);
                } else if (betterWeightThanCurrent) {
                    sameBalanceNodes.add(nodeResult);
                } else {
                    worseBalanceNodes.add(nodeResult);
                }
            }

            int weightRanking = 0;
            List<NodeAllocationResult> nodeDecisions = new ArrayList<>(modelNodes.length - 1);
            for (Tuple<ModelNode, Decision> result : betterBalanceNodes) {
                nodeDecisions.add(new NodeAllocationResult(
                    result.v1().routingNode.node(), AllocationDecision.fromDecisionType(result.v2().type()), result.v2(), ++weightRanking)
                );
            }
            int currentNodeWeightRanking = ++weightRanking;
            for (Tuple<ModelNode, Decision> result : sameBalanceNodes) {
                AllocationDecision nodeDecision = result.v2().type() == Type.NO ? AllocationDecision.NO : AllocationDecision.WORSE_BALANCE;
                nodeDecisions.add(new NodeAllocationResult(
                    result.v1().routingNode.node(), nodeDecision, result.v2(), currentNodeWeightRanking)
                );
            }
            for (Tuple<ModelNode, Decision> result : worseBalanceNodes) {
                AllocationDecision nodeDecision = result.v2().type() == Type.NO ? AllocationDecision.NO : AllocationDecision.WORSE_BALANCE;
                nodeDecisions.add(new NodeAllocationResult(
                    result.v1().routingNode.node(), nodeDecision, result.v2(), ++weightRanking)
                );
            }

            if (canRebalance.type() != Type.YES || allocation.hasPendingAsyncFetch()) {
                AllocationDecision allocationDecision = allocation.hasPendingAsyncFetch() ? AllocationDecision.AWAITING_INFO :
                                                            AllocationDecision.fromDecisionType(canRebalance.type());
                return MoveDecision.cannotRebalance(canRebalance, allocationDecision, currentNodeWeightRanking, nodeDecisions);
            } else {
                return MoveDecision.rebalance(canRebalance, AllocationDecision.fromDecisionType(rebalanceDecisionType),
                    assignedNode != null ? assignedNode.routingNode.node() : null, currentNodeWeightRanking, nodeDecisions);
            }
        }

        /**
         * Balances the nodes on the cluster model according to the weight
         * function. The configured threshold is the minimum delta between the
         * weight of the maximum node and the minimum node according to the
         * {@link WeightFunction}. This weight is calculated per index to
         * distribute shards evenly per index. The balancer tries to relocate
         * shards only if the delta exceeds the threshold. In the default case
         * the threshold is set to {@code 1.0} to enforce gaining relocation
         * only, or in other words relocations that move the weight delta closer
         * to {@code 0.0}
         */
        private void balanceByWeights() {
            final AllocationDeciders deciders = allocation.deciders();
            final ModelNode[] modelNodes = sorter.modelNodes;
            final float[] weights = sorter.weights;
            for (String index : buildWeightOrderedIndices()) {
                IndexMetadata indexMetadata = metadata.index(index);

                // find nodes that have a shard of this index or where shards of this index are allowed to be allocated to,
                // move these nodes to the front of modelNodes so that we can only balance based on these nodes
                int relevantNodes = 0;
                for (int i = 0; i < modelNodes.length; i++) {
                    ModelNode modelNode = modelNodes[i];
                    if (modelNode.getIndex(index) != null
                        || deciders.canAllocate(indexMetadata, modelNode.getRoutingNode(), allocation).type() != Type.NO) {
                        // swap nodes at position i and relevantNodes
                        modelNodes[i] = modelNodes[relevantNodes];
                        modelNodes[relevantNodes] = modelNode;
                        relevantNodes++;
                    }
                }

                if (relevantNodes < 2) {
                    continue;
                }

                sorter.reset(index, 0, relevantNodes);
                int lowIdx = 0;
                int highIdx = relevantNodes - 1;
                while (true) {
                    final ModelNode minNode = modelNodes[lowIdx];
                    final ModelNode maxNode = modelNodes[highIdx];
                    advance_range:
                    if (maxNode.numShards(index) > 0) {
                        final float delta = absDelta(weights[lowIdx], weights[highIdx]);
                        if (lessThan(delta, threshold)) {
                            if (lowIdx > 0 && highIdx-1 > 0 // is there a chance for a higher delta?
                                && (absDelta(weights[0], weights[highIdx-1]) > threshold) // check if we need to break at all
                                ) {
                                /* This is a special case if allocations from the "heaviest" to the "lighter" nodes is not possible
                                 * due to some allocation decider restrictions like zone awareness. if one zone has for instance
                                 * less nodes than another zone. so one zone is horribly overloaded from a balanced perspective but we
                                 * can't move to the "lighter" shards since otherwise the zone would go over capacity.
                                 *
                                 * This break jumps straight to the condition below were we start moving from the high index towards
                                 * the low index to shrink the window we are considering for balance from the other direction.
                                 * (check shrinking the window from MAX to MIN)
                                 * See #3580
                                 */
                                break advance_range;
                            }
                            if (logger.isTraceEnabled()) {
                                logger.trace("Stop balancing index [{}]  min_node [{}] weight: [{}]" +
                                        "  max_node [{}] weight: [{}]  delta: [{}]",
                                        index, maxNode.getNodeId(), weights[highIdx], minNode.getNodeId(), weights[lowIdx], delta);
                            }
                            break;
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("Balancing from node [{}] weight: [{}] to node [{}] weight: [{}]  delta: [{}]",
                                    maxNode.getNodeId(), weights[highIdx], minNode.getNodeId(), weights[lowIdx], delta);
                        }
                        if (delta <= 1.0f) {
                            /*
                             * prevent relocations that only swap the weights of the two nodes. a relocation must bring us closer to the
                             * balance if we only achieve the same delta the relocation is useless
                             *
                             * NB this comment above was preserved from an earlier version but doesn't obviously describe the code today. We
                             * already know that lessThan(delta, threshold) == false and threshold defaults to 1.0, so by default we never
                             * hit this case anyway.
                             */
                            logger.trace("Couldn't find shard to relocate from node [{}] to node [{}]",
                                maxNode.getNodeId(), minNode.getNodeId());
                        } else if (tryRelocateShard(minNode, maxNode, index)) {
                            /*
                             * TODO we could be a bit smarter here, we don't need to fully sort necessarily
                             * we could just find the place to insert linearly but the win might be minor
                             * compared to the added complexity
                             */
                            weights[lowIdx] = sorter.weight(modelNodes[lowIdx]);
                            weights[highIdx] = sorter.weight(modelNodes[highIdx]);
                            sorter.sort(0, relevantNodes);
                            lowIdx = 0;
                            highIdx = relevantNodes - 1;
                            continue;
                        }
                    }
                    if (lowIdx < highIdx - 1) {
                        /* Shrinking the window from MIN to MAX
                         * we can't move from any shard from the min node lets move on to the next node
                         * and see if the threshold still holds. We either don't have any shard of this
                         * index on this node of allocation deciders prevent any relocation.*/
                        lowIdx++;
                    } else if (lowIdx > 0) {
                        /* Shrinking the window from MAX to MIN
                         * now we go max to min since obviously we can't move anything to the max node
                         * lets pick the next highest */
                        lowIdx = 0;
                        highIdx--;
                    } else {
                        /* we are done here, we either can't relocate anymore or we are balanced */
                        break;
                    }
                }
            }
        }

        /**
         * This builds a initial index ordering where the indices are returned
         * in most unbalanced first. We need this in order to prevent over
         * allocations on added nodes from one index when the weight parameters
         * for global balance overrule the index balance at an intermediate
         * state. For example this can happen if we have 3 nodes and 3 indices
         * with 3 primary and 1 replica shards. At the first stage all three nodes hold
         * 2 shard for each index. Now we add another node and the first index
         * is balanced moving three shards from two of the nodes over to the new node since it
         * has no shards yet and global balance for the node is way below
         * average. To re-balance we need to move shards back eventually likely
         * to the nodes we relocated them from.
         */
        private String[] buildWeightOrderedIndices() {
            final String[] indices = allocation.routingTable().indicesRouting().keys().toArray(String.class);
            final float[] deltas = new float[indices.length];
            for (int i = 0; i < deltas.length; i++) {
                sorter.reset(indices[i]);
                deltas[i] = sorter.delta();
            }
            new IntroSorter() {

                float pivotWeight;

                @Override
                protected void swap(int i, int j) {
                    final String tmpIdx = indices[i];
                    indices[i] = indices[j];
                    indices[j] = tmpIdx;
                    final float tmpDelta = deltas[i];
                    deltas[i] = deltas[j];
                    deltas[j] = tmpDelta;
                }

                @Override
                protected int compare(int i, int j) {
                    return Float.compare(deltas[j], deltas[i]);
                }

                @Override
                protected void setPivot(int i) {
                    pivotWeight = deltas[i];
                }

                @Override
                protected int comparePivot(int j) {
                    return Float.compare(deltas[j], pivotWeight);
                }
            }.sort(0, deltas.length);

            return indices;
        }

        /**
         * Move started shards that can not be allocated to a node anymore
         *
         * For each shard to be moved this function executes a move operation
         * to the minimal eligible node with respect to the
         * weight function. If a shard is moved the shard will be set to
         * {@link ShardRoutingState#RELOCATING} and a shadow instance of this
         * shard is created with an incremented version in the state
         * {@link ShardRoutingState#INITIALIZING}.
         */
        public void moveShards() {
            // Iterate over the started shards interleaving between nodes, and check if they can remain. In the presence of throttling
            // shard movements, the goal of this iteration order is to achieve a fairer movement of shards from the nodes that are
            // offloading the shards.
            for (Iterator<ShardRouting> it = allocation.routingNodes().nodeInterleavedShardIterator(); it.hasNext(); ) {
                ShardRouting shardRouting = it.next();
                final MoveDecision moveDecision = decideMove(shardRouting);
                if (moveDecision.isDecisionTaken() && moveDecision.forceMove()) {
                    final ModelNode sourceNode = nodes.get(shardRouting.currentNodeId());
                    final ModelNode targetNode = nodes.get(moveDecision.getTargetNode().getId());
                    sourceNode.removeShard(shardRouting);
                    Tuple<ShardRouting, ShardRouting> relocatingShards = routingNodes.relocateShard(shardRouting, targetNode.getNodeId(),
                        allocation.clusterInfo().getShardSize(shardRouting,
                                                              ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE), allocation.changes());
                    targetNode.addShard(relocatingShards.v2());
                    if (logger.isTraceEnabled()) {
                        logger.trace("Moved shard [{}] to node [{}]", shardRouting, targetNode.getRoutingNode());
                    }
                } else if (moveDecision.isDecisionTaken() && moveDecision.canRemain() == false) {
                    logger.trace("[{}][{}] can't move", shardRouting.index(), shardRouting.id());
                }
            }
        }

        /**
         * Makes a decision on whether to move a started shard to another node.  The following rules apply
         * to the {@link MoveDecision} return object:
         *   1. If the shard is not started, no decision will be taken and {@link MoveDecision#isDecisionTaken()} will return false.
         *   2. If the shard is allowed to remain on its current node, no attempt will be made to move the shard and
         *      {@link MoveDecision#getCanRemainDecision} will have a decision type of YES.  All other fields in the object will be null.
         *   3. If the shard is not allowed to remain on its current node, then {@link MoveDecision#getAllocationDecision()} will be
         *      populated with the decision of moving to another node.  If {@link MoveDecision#forceMove()} ()} returns {@code true}, then
         *      {@link MoveDecision#getTargetNode} will return a non-null value, otherwise the assignedNodeId will be null.
         *   4. If the method is invoked in explain mode (e.g. from the cluster allocation explain APIs), then
         *      {@link MoveDecision#getNodeDecisions} will have a non-null value.
         */
        public MoveDecision decideMove(final ShardRouting shardRouting) {
            if (shardRouting.started() == false) {
                // we can only move started shards
                return MoveDecision.NOT_TAKEN;
            }

            final boolean explain = allocation.debugDecision();
            final ModelNode sourceNode = nodes.get(shardRouting.currentNodeId());
            assert sourceNode != null && sourceNode.containsShard(shardRouting);
            RoutingNode routingNode = sourceNode.getRoutingNode();
            Decision canRemain = allocation.deciders().canRemain(shardRouting, routingNode, allocation);
            if (canRemain.type() != Decision.Type.NO) {
                return MoveDecision.stay(canRemain);
            }

            sorter.reset(shardRouting.getIndexName());
            /*
             * the sorter holds the minimum weight node first for the shards index.
             * We now walk through the nodes until we find a node to allocate the shard.
             * This is not guaranteed to be balanced after this operation we still try best effort to
             * allocate on the minimal eligible node.
             */
            Type bestDecision = Type.NO;
            RoutingNode targetNode = null;
            final List<NodeAllocationResult> nodeExplanationMap = explain ? new ArrayList<>() : null;
            int weightRanking = 0;
            for (ModelNode currentNode : sorter.modelNodes) {
                if (currentNode != sourceNode) {
                    RoutingNode target = currentNode.getRoutingNode();
                    // don't use canRebalance as we want hard filtering rules to apply. See #17698
                    Decision allocationDecision = allocation.deciders().canAllocate(shardRouting, target, allocation);
                    if (explain) {
                        nodeExplanationMap.add(new NodeAllocationResult(
                            currentNode.getRoutingNode().node(), allocationDecision, ++weightRanking));
                    }
                    // TODO maybe we can respect throttling here too?
                    if (allocationDecision.type().higherThan(bestDecision)) {
                        bestDecision = allocationDecision.type();
                        if (bestDecision == Type.YES) {
                            targetNode = target;
                            if (explain == false) {
                                // we are not in explain mode and already have a YES decision on the best weighted node,
                                // no need to continue iterating
                                break;
                            }
                        }
                    }
                }
            }

            return MoveDecision.cannotRemain(canRemain, AllocationDecision.fromDecisionType(bestDecision),
                targetNode != null ? targetNode.node() : null, nodeExplanationMap);
        }

        /**
         * Builds the internal model from all shards in the given
         * {@link Iterable}. All shards in the {@link Iterable} must be assigned
         * to a node. This method will skip shards in the state
         * {@link ShardRoutingState#RELOCATING} since each relocating shard has
         * a shadow shard in the state {@link ShardRoutingState#INITIALIZING}
         * on the target node which we respect during the allocation / balancing
         * process. In short, this method recreates the status-quo in the cluster.
         * 根据当前所有node 的分片信息构建一个 ModelNode map  注意这里只处理已经分配好的
         */
        private Map<String, ModelNode> buildModelFromAssigned() {
            Map<String, ModelNode> nodes = new HashMap<>();
            for (RoutingNode rn : routingNodes) {
                ModelNode node = new ModelNode(rn);
                nodes.put(rn.nodeId(), node);
                // 将每个node下的分片填充到 modelNode中
                for (ShardRouting shard : rn) {
                    assert rn.nodeId().equals(shard.currentNodeId());
                    /* we skip relocating shards here since we expect an initializing shard with the same id coming in */
                    // 注意这里不会添加处于重分配状态的分片
                    if (shard.state() != RELOCATING) {
                        node.addShard(shard);
                        if (logger.isTraceEnabled()) {
                            logger.trace("Assigned shard [{}] to node [{}]", shard, node.getNodeId());
                        }
                    }
                }
            }
            return nodes;
        }

        /**
         * Allocates all given shards on the minimal eligible node for the shards index
         * with respect to the weight function. All given shards must be unassigned.
         * 为当前所有未分配的节点进行分配
         */
        private void allocateUnassigned() {
            RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
            assert !nodes.isEmpty();
            if (logger.isTraceEnabled()) {
                logger.trace("Start allocating unassigned shards");
            }
            if (unassigned.isEmpty()) {
                return;
            }

            /*
             * TODO: We could be smarter here and group the shards by index and then
             * use the sorter to save some iterations.
             * 这些 decision对象共同作用的结果将会决定某个分片应该被分配到哪个节点
             */
            final AllocationDeciders deciders = allocation.deciders();
            // 根据索引的优先级以及创建时间为索引排序
            final PriorityComparator secondaryComparator = PriorityComparator.getAllocationComparator(allocation);
            // 该对象可以为分片排序
            final Comparator<ShardRouting> comparator = (o1, o2) -> {
                // 主分片的优先级低
                if (o1.primary() ^ o2.primary()) {
                    return o1.primary() ? -1 : 1;
                }
                // 其次比较索引的大小
                final int indexCmp;
                if ((indexCmp = o1.getIndexName().compareTo(o2.getIndexName())) == 0) {
                    return o1.getId() - o2.getId();
                }
                // this comparator is more expensive than all the others up there
                // that's why it's added last even though it could be easier to read
                // if we'd apply it earlier. this comparator will only differentiate across
                // indices all shards of the same index is treated equally.
                final int secondary = secondaryComparator.compare(o1, o2);
                return secondary == 0 ? indexCmp : secondary;
            };
            /*
             * we use 2 arrays and move replicas to the second array once we allocated an identical
             * replica in the current iteration to make sure all indices get allocated in the same manner.
             * The arrays are sorted by primaries first and then by index and shard ID so a 2 indices with
             * 2 replica and 1 shard would look like:
             * [(0,P,IDX1), (0,P,IDX2), (0,R,IDX1), (0,R,IDX1), (0,R,IDX2), (0,R,IDX2)]
             * if we allocate for instance (0, R, IDX1) we move the second replica to the secondary array and proceed with
             * the next replica. If we could not find a node to allocate (0,R,IDX1) we move all it's replicas to ignoreUnassigned.
             */
            ShardRouting[] primary = unassigned.drain();
            ShardRouting[] secondary = new ShardRouting[primary.length];
            int secondaryLength = 0;
            int primaryLength = primary.length;
            // 将未分配的分片对象排序
            ArrayUtil.timSort(primary, comparator);
            final Set<ModelNode> throttledNodes = Collections.newSetFromMap(new IdentityHashMap<>());
            do {
                for (int i = 0; i < primaryLength; i++) {
                    ShardRouting shard = primary[i];
                    // 根据当前所有分片此时的分配状态决定新的分片应该分配在哪个node上 这里会利用decisions进行决定 同时从符合条件的多个node中根据权重值排序
                    AllocateUnassignedDecision allocationDecision = decideAllocateUnassigned(shard, throttledNodes);
                    // 获取本次指定的目标节点
                    final String assignedNodeId = allocationDecision.getTargetNode() != null ?
                                                      allocationDecision.getTargetNode().getId() : null;
                    final ModelNode minNode = assignedNodeId != null ? nodes.get(assignedNodeId) : null;

                    if (allocationDecision.getAllocationDecision() == AllocationDecision.YES) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Assigned shard [{}] to [{}]", shard, minNode.getNodeId());
                        }

                        final long shardSize = DiskThresholdDecider.getExpectedShardSize(shard,
                            ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                            allocation.clusterInfo(), allocation.metadata(), allocation.routingTable());
                        shard = routingNodes.initializeShard(shard, minNode.getNodeId(), null, shardSize, allocation.changes());
                        minNode.addShard(shard);
                        if (!shard.primary()) {
                            // copy over the same replica shards to the secondary array so they will get allocated
                            // in a subsequent iteration, allowing replicas of other shards to be allocated first
                            while(i < primaryLength-1 && comparator.compare(primary[i], primary[i+1]) == 0) {
                                secondary[secondaryLength++] = primary[++i];
                            }
                        }
                    } else {
                        // did *not* receive a YES decision
                        if (logger.isTraceEnabled()) {
                            logger.trace("No eligible node found to assign shard [{}] allocation_status [{}]", shard,
                                allocationDecision.getAllocationStatus());
                        }

                        if (minNode != null) {
                            // throttle decision scenario
                            assert allocationDecision.getAllocationStatus() == AllocationStatus.DECIDERS_THROTTLED;
                            final long shardSize = DiskThresholdDecider.getExpectedShardSize(shard,
                                ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                                allocation.clusterInfo(), allocation.metadata(), allocation.routingTable());
                            minNode.addShard(shard.initialize(minNode.getNodeId(), null, shardSize));
                            final RoutingNode node = minNode.getRoutingNode();
                            final Decision.Type nodeLevelDecision = deciders.canAllocate(node, allocation).type();
                            if (nodeLevelDecision != Type.YES) {
                                if (logger.isTraceEnabled()) {
                                    logger.trace("Can not allocate on node [{}] remove from round decision [{}]", node,
                                        allocationDecision.getAllocationStatus());
                                }
                                assert nodeLevelDecision == Type.NO;
                                throttledNodes.add(minNode);
                            }
                        } else {
                            if (logger.isTraceEnabled()) {
                                logger.trace("No Node found to assign shard [{}]", shard);
                            }
                        }

                        unassigned.ignoreShard(shard, allocationDecision.getAllocationStatus(), allocation.changes());
                        if (!shard.primary()) { // we could not allocate it and we are a replica - check if we can ignore the other replicas
                            while(i < primaryLength-1 && comparator.compare(primary[i], primary[i+1]) == 0) {
                                unassigned.ignoreShard(primary[++i], allocationDecision.getAllocationStatus(), allocation.changes());
                            }
                        }
                    }
                }
                primaryLength = secondaryLength;
                ShardRouting[] tmp = primary;
                primary = secondary;
                secondary = tmp;
                secondaryLength = 0;
            } while (primaryLength > 0);
            // clear everything we have either added it or moved to ignoreUnassigned
        }

        /**
         * Make a decision for allocating an unassigned shard.  This method returns a two values in a tuple: the
         * first value is the {@link Decision} taken to allocate the unassigned shard, the second value is the
         * {@link ModelNode} representing the node that the shard should be assigned to.  If the decision returned
         * is of type {@link Type#NO}, then the assigned node will be null.
         * @param shard 本次未分配的分片
         * @param throttledNodes  该容器一开始是空的  在某一个方法中循环调用 并不断填充
         * 决定某个未分配的分片最终会在哪里
         */
        private AllocateUnassignedDecision decideAllocateUnassigned(final ShardRouting shard, final Set<ModelNode> throttledNodes) {
            if (shard.assignedToNode()) {
                // we only make decisions for unassigned shards here
                return AllocateUnassignedDecision.NOT_TAKEN;
            }

            // 是否需要打印详细信息
            final boolean explain = allocation.debugDecision();
            // 首先判断能否为这个 分片分配
            Decision shardLevelDecision = allocation.deciders().canAllocate(shard, allocation);
            // 返回一个无法分配的结果
            if (shardLevelDecision.type() == Type.NO && explain == false) {
                // NO decision for allocating the shard, irrespective of any particular node, so exit early
                return AllocateUnassignedDecision.no(AllocationStatus.DECIDERS_NO, null);
            }

            /* find an node with minimal weight we can allocate on*/
            float minWeight = Float.POSITIVE_INFINITY;
            ModelNode minNode = null;
            Decision decision = null;
            // 当每个node 都至少分配过一个分片后 无法通过该方法继续分配
            if (throttledNodes.size() >= nodes.size() && explain == false) {
                // all nodes are throttled, so we know we won't be able to allocate this round,
                // so if we are not in explain mode, short circuit
                return AllocateUnassignedDecision.no(AllocationStatus.DECIDERS_NO, null);
            }
            /* Don't iterate over an identity hashset here the
             * iteration order is different for each run and makes testing hard */
            // 当需要详细信息时 使用map来填装结果
            Map<String, NodeAllocationResult> nodeExplanationMap = explain ? new HashMap<>() : null;
            // 代表某个分片尝试往相关节点分配时计算出的权重
            List<Tuple<String, Float>> nodeWeights = explain ? new ArrayList<>() : null;
            // 这里应该是寻找合适的节点
            for (ModelNode node : nodes.values()) {
                // 在这一轮中如果某个node已经使用过 或者这个node本身就包含了这个分片 那么直接忽略
                if ((throttledNodes.contains(node) || node.containsShard(shard)) && explain == false) {
                    // decision is NO without needing to check anything further, so short circuit
                    continue;
                }

                // weight of this index currently on the node
                // 计算这个节点的权重 权重将会决定某个分片最终分配到哪个节点    权重越小代表这个节点上有关这个索引的分片越少 或者是这个 节点在所有节点中分片少(共同作用的结果)
                float currentWeight = weight.weight(this, node, shard.getIndexName());
                // moving the shard would not improve the balance, and we are not in explain mode, so short circuit
                if (currentWeight > minWeight && explain == false) {
                    continue;
                }

                // 决定能否将分片分配到这个节点
                Decision currentDecision = allocation.deciders().canAllocate(shard, node.getRoutingNode(), allocation);
                // 生成描述信息
                if (explain) {
                    nodeExplanationMap.put(node.getNodeId(),
                        new NodeAllocationResult(node.getRoutingNode().node(), currentDecision, 0));
                    nodeWeights.add(Tuple.tuple(node.getNodeId(), currentWeight));
                }
                // 如果结果是 非No
                if (currentDecision.type() == Type.YES || currentDecision.type() == Type.THROTTLE) {
                    final boolean updateMinNode;
                    // 此时还没有更新最小权重 也就是本次的权重值与之前的最小权重刚好相同
                    if (currentWeight == minWeight) {
                        /*  we have an equal weight tie breaking:
                         *  1. if one decision is YES prefer it   优先选择YES
                         *  2. prefer the node that holds the primary for this index with the next id in the ring ie.
                         *  for the 3 shards 2 replica case we try to build up:
                         *    1 2 0
                         *    2 0 1
                         *    0 1 2
                         *  such that if we need to tie-break we try to prefer the node holding a shard with the minimal id greater
                         *  than the id of the shard we need to assign. This works find when new indices are created since
                         *  primaries are added first and we only add one shard set a time in this algorithm.
                         * 只有 YES 或者  THROTTLE 才有可能进入下面的逻辑
                         */
                        if (currentDecision.type() == decision.type()) {
                            final int repId = shard.id();
                            // 获取这个node下有关这个索引的最大的主分片
                            final int nodeHigh = node.highestPrimary(shard.index().getName());
                            // 获取上个node下该索引的最大分片
                            final int minNodeHigh = minNode.highestPrimary(shard.getIndexName());
                            updateMinNode = (
                            (
                                // 也就是本次的node上 最大分片id要小
                                ((nodeHigh > repId && minNodeHigh > repId) || (nodeHigh < repId && minNodeHigh < repId))
                                                  && (nodeHigh < minNodeHigh)
                            )
                                // 这2个条件不是冲突的么???
                                                 || (nodeHigh > repId && minNodeHigh < repId));
                        } else {
                            // 在权重值相同的时候如果type不同 优先选择 YES
                            updateMinNode = currentDecision.type() == Type.YES;
                        }
                    } else {
                        // 因为本次权重值更小 所以要更新更合适的node  首次触发肯定走这个逻辑
                        updateMinNode = true;
                    }
                    // 更新此时最合适的节点
                    if (updateMinNode) {
                        minNode = node;
                        minWeight = currentWeight;
                        decision = currentDecision;
                    }
                }
            }
            // 代表无法分配到任何节点上
            if (decision == null) {
                // decision was not set and a node was not assigned, so treat it as a NO decision
                decision = Decision.NO;
            }
            List<NodeAllocationResult> nodeDecisions = null;
            if (explain) {
                nodeDecisions = new ArrayList<>();
                // fill in the correct weight ranking, once we've been through all nodes
                nodeWeights.sort((nodeWeight1, nodeWeight2) -> Float.compare(nodeWeight1.v2(), nodeWeight2.v2()));
                int weightRanking = 0;
                // 根据每个node对应的权重信息生成描述结果
                for (Tuple<String, Float> nodeWeight : nodeWeights) {
                    NodeAllocationResult current = nodeExplanationMap.get(nodeWeight.v1());
                    nodeDecisions.add(new NodeAllocationResult(current.getNode(), current.getCanAllocateDecision(), ++weightRanking));
                }
            }
            return AllocateUnassignedDecision.fromDecision(
                decision,
                minNode != null ? minNode.routingNode.node() : null,
                nodeDecisions
            );
        }

        private static final Comparator<ShardRouting> BY_DESCENDING_SHARD_ID = Comparator.comparing(ShardRouting::shardId).reversed();

        /**
         * Tries to find a relocation from the max node to the minimal node for an arbitrary shard of the given index on the
         * balance model. Iff this method returns a <code>true</code> the relocation has already been executed on the
         * simulation model as well as on the cluster.
         */
        private boolean tryRelocateShard(ModelNode minNode, ModelNode maxNode, String idx) {
            final ModelIndex index = maxNode.getIndex(idx);
            if (index != null) {
                logger.trace("Try relocating shard of [{}] from [{}] to [{}]", idx, maxNode.getNodeId(), minNode.getNodeId());
                final Iterable<ShardRouting> shardRoutings = StreamSupport.stream(index.spliterator(), false)
                    .filter(ShardRouting::started) // cannot rebalance unassigned, initializing or relocating shards anyway
                    .filter(maxNode::containsShard)
                    .sorted(BY_DESCENDING_SHARD_ID) // check in descending order of shard id so that the decision is deterministic
                    ::iterator;

                final AllocationDeciders deciders = allocation.deciders();
                for (ShardRouting shard : shardRoutings) {
                    final Decision rebalanceDecision = deciders.canRebalance(shard, allocation);
                    if (rebalanceDecision.type() == Type.NO) {
                        continue;
                    }
                    final Decision allocationDecision = deciders.canAllocate(shard, minNode.getRoutingNode(), allocation);
                    if (allocationDecision.type() == Type.NO) {
                        continue;
                    }

                    final Decision decision = new Decision.Multi().add(allocationDecision).add(rebalanceDecision);

                    maxNode.removeShard(shard);
                    long shardSize = allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

                    if (decision.type() == Type.YES) {
                        /* only allocate on the cluster if we are not throttled */
                        logger.debug("Relocate [{}] from [{}] to [{}]", shard, maxNode.getNodeId(), minNode.getNodeId());
                        minNode.addShard(routingNodes.relocateShard(shard, minNode.getNodeId(), shardSize, allocation.changes()).v1());
                        return true;
                    } else {
                        /* allocate on the model even if throttled */
                        logger.debug("Simulate relocation of [{}] from [{}] to [{}]", shard, maxNode.getNodeId(), minNode.getNodeId());
                        assert decision.type() == Type.THROTTLE;
                        minNode.addShard(shard.relocate(minNode.getNodeId(), shardSize));
                        return false;
                    }
                }
            }
            logger.trace("No shards of [{}] can relocate from [{}] to [{}]", idx, maxNode.getNodeId(), minNode.getNodeId());
            return false;
        }

    }

    static class ModelNode implements Iterable<ModelIndex> {

        /**
         * 维护该node下所有分片对应的索引信息
         */
        private final Map<String, ModelIndex> indices = new HashMap<>();

        /**
         * 记录该node下总计存储了多少分片  而分片具体存储在 ModelIndex中
         */
        private int numShards = 0;
        private final RoutingNode routingNode;

        /**
         * 通过某个节点下所有的分片信息进行初始化  (包含不同shardId的数据)
         * @param routingNode
         */
        ModelNode(RoutingNode routingNode) {
            this.routingNode = routingNode;
        }

        public ModelIndex getIndex(String indexId) {
            return indices.get(indexId);
        }

        public String getNodeId() {
            return routingNode.nodeId();
        }

        public RoutingNode getRoutingNode() {
            return routingNode;
        }

        public int numShards() {
            return numShards;
        }

        public int numShards(String idx) {
            ModelIndex index = indices.get(idx);
            return index == null ? 0 : index.numShards();
        }

        public int highestPrimary(String index) {
            ModelIndex idx = indices.get(index);
            if (idx != null) {
                return idx.highestPrimary();
            }
            return -1;
        }

        /**
         * 为当前节点添加某个分片对象
         * @param shard
         */
        public void addShard(ShardRouting shard) {
            ModelIndex index = indices.get(shard.getIndexName());
            if (index == null) {
                index = new ModelIndex(shard.getIndexName());
                indices.put(index.getIndexId(), index);
            }
            // 将分片添加到索引中
            index.addShard(shard);
            numShards++;
        }

        public void removeShard(ShardRouting shard) {
            ModelIndex index = indices.get(shard.getIndexName());
            if (index != null) {
                index.removeShard(shard);
                if (index.numShards() == 0) {
                    indices.remove(shard.getIndexName());
                }
            }
            numShards--;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Node(").append(routingNode.nodeId()).append(")");
            return sb.toString();
        }

        @Override
        public Iterator<ModelIndex> iterator() {
            return indices.values().iterator();
        }

        public boolean containsShard(ShardRouting shard) {
            ModelIndex index = getIndex(shard.getIndexName());
            return index == null ? false : index.containsShard(shard);
        }

    }

    /**
     * 在 ModelNode中使用  对应一个索引
     */
    static final class ModelIndex implements Iterable<ShardRouting> {
        private final String id;
        /**
         * 属于该索引的某个分片
         */
        private final Set<ShardRouting> shards = new HashSet<>(4); // expect few shards of same index to be allocated on same node
        private int highestPrimary = -1;

        ModelIndex(String id) {
            this.id = id;
        }

        /**
         * 某个节点下的某个索引是否具有最高优先级  每次某个节点下的索引发生变化 优先级的值就重置为-1
         * @return
         */
        public int highestPrimary() {
            // 代表最近发生过变化 要重新计算这个值  这里返回的就是所有分片中 最大的主分片id
            if (highestPrimary == -1) {
                int maxId = -1;
                for (ShardRouting shard : shards) {
                    if (shard.primary()) {
                        maxId = Math.max(maxId, shard.id());
                    }
                }
                return highestPrimary = maxId;
            }
            return highestPrimary;
        }

        public String getIndexId() {
            return id;
        }

        public int numShards() {
            return shards.size();
        }

        @Override
        public Iterator<ShardRouting> iterator() {
            return shards.iterator();
        }

        public void removeShard(ShardRouting shard) {
            highestPrimary = -1;
            assert shards.contains(shard) : "Shard not allocated on current node: " + shard;
            shards.remove(shard);
        }

        /**
         * 在该索引下追加一个新的分片
         * @param shard
         */
        public void addShard(ShardRouting shard) {
            highestPrimary = -1;
            assert !shards.contains(shard) : "Shard already allocated on current node: " + shard;
            shards.add(shard);
        }

        public boolean containsShard(ShardRouting shard) {
            return shards.contains(shard);
        }
    }

    /**
     * 该对象基于某种规则对node 进行排序
     */
    static final class NodeSorter extends IntroSorter {

        /**
         * 内部包含该节点上所有的分片
         */
        final ModelNode[] modelNodes;
        /*
        * the nodes weights with respect to the current weight function / index
        * 每个节点有自己的权重值
        */
        final float[] weights;
        private final WeightFunction function;
        private String index;
        private final Balancer balancer;
        private float pivotWeight;

        NodeSorter(ModelNode[] modelNodes, WeightFunction function, Balancer balancer) {
            this.function = function;
            this.balancer = balancer;
            this.modelNodes = modelNodes;
            weights = new float[modelNodes.length];
        }

        /**
         * Resets the sorter, recalculates the weights per node and sorts the
         * nodes by weight, with minimal weight first.
         */
        public void reset(String index, int from, int to) {
            this.index = index;
            for (int i = from; i < to; i++) {
                weights[i] = weight(modelNodes[i]);
            }
            sort(from, to);
        }

        public void reset(String index) {
            reset(index, 0, modelNodes.length);
        }

        public float weight(ModelNode node) {
            return function.weight(balancer, node, index);
        }

        @Override
        protected void swap(int i, int j) {
            final ModelNode tmpNode = modelNodes[i];
            modelNodes[i] = modelNodes[j];
            modelNodes[j] = tmpNode;
            final float tmpWeight = weights[i];
            weights[i] = weights[j];
            weights[j] = tmpWeight;
        }

        @Override
        protected int compare(int i, int j) {
            return Float.compare(weights[i], weights[j]);
        }

        @Override
        protected void setPivot(int i) {
            pivotWeight = weights[i];
        }

        @Override
        protected int comparePivot(int j) {
            return Float.compare(pivotWeight, weights[j]);
        }

        public float delta() {
            return weights[weights.length - 1] - weights[0];
        }
    }
}
