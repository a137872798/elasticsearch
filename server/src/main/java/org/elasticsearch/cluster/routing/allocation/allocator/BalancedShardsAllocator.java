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
 * 该对象定义了如何使用 RoutingAllocation 确定分片的分配位置 以及分片是否需要移动
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

        // 追加2个处理配置变化时的消费者
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
     * 处理此时处于init状态的分片
     * @param allocation current node allocation
     */
    @Override
    public void allocate(RoutingAllocation allocation) {
        // 代表此时全都是未分配的shard   (即使是从unassigned转移到init状态 也会加入到routingNodes的相关容器)
        // TODO 先忽略这种情况
        if (allocation.routingNodes().size() == 0) {
            failAllocationOfNewPrimaries(allocation);
            return;
        }
        final Balancer balancer = new Balancer(logger, allocation, weightFunction, threshold);
        // 先分配所有unassigned shard   TODO 如果是通过allocationService调用该方法 那么之前就应该已经为unassigned分配完毕了 如果是异步处理 那么也从unassigned中移除了
        balancer.allocateUnassigned();
        // 找出此时所有需要移动的分片 将当前分片状态从start修改成 relocation 并且插入一个在target的init分片  检测是否需要移动 以及应该分配在哪个节点 都是通过一组deciders决定的
        balancer.moveShards();
        // 针对当前分片进行重分配  并没有发生数据的迁移  也只是将一些分片从start修改成relocation
        balancer.balance();
    }


    /**
     * 尝试将某个分片进行分配
     * @param shard
     * @param allocation
     * @return
     */
    @Override
    public ShardAllocationDecision decideShardAllocation(final ShardRouting shard, final RoutingAllocation allocation) {
        Balancer balancer = new Balancer(logger, allocation, weightFunction, threshold);
        AllocateUnassignedDecision allocateUnassignedDecision = AllocateUnassignedDecision.NOT_TAKEN;
        MoveDecision moveDecision = MoveDecision.NOT_TAKEN;
        if (shard.unassigned()) {
            allocateUnassignedDecision = balancer.decideAllocateUnassigned(shard, Sets.newHashSet());
        } else {
            // 如果该分片已经分配 那么尝试进行移动
            moveDecision = balancer.decideMove(shard);
            // 如果不需要 移动 那么尝试进行平衡
            if (moveDecision.isDecisionTaken() && moveDecision.canRemain()) {
                MoveDecision rebalanceDecision = balancer.decideRebalance(shard);
                moveDecision = rebalanceDecision.withRemainDecision(moveDecision.getCanRemainDecision());
            }
        }
        // 将决定对象合并后返回
        return new ShardAllocationDecision(allocateUnassignedDecision, moveDecision);
    }

    /**
     * 代表集群中都是未分配的分片
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
     * 该对象是用于计算权重值的
     * 权重值本身应该是越小越好  相当于负载越低
     * 同时当分片数在node上都一致时  index上的分片数量就变成了决定权重大小的关键变量
     */
    private static class WeightFunction {

        // 这2个因子是随便填的
        private final float indexBalance;
        private final float shardBalance;

        // 下面2个是它们的占比
        private final float theta0;
        private final float theta1;

        /**
         * @param indexBalance  默认 0.55
         * @param shardBalance  默认 0.45
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
         * 计算某个node上 index的权重值  方便平衡集群中每个节点下 某index的分片数量
         * @param balancer
         * @param node
         * @param index
         * @return
         */
        float weight(Balancer balancer, ModelNode node, String index) {
            // 节点上分片总数 与标准值的偏差量
            final float weightShard = node.numShards() - balancer.avgShardsPerNode();
            // 在某个索引级别下 分片数量与标准值的偏差值
            final float weightIndex = node.numShards(index) - balancer.avgShardsPerNode(index);
            return theta0 * weightShard + theta1 * weightIndex;
        }
    }

    /**
     * A {@link Balancer}
     * 平衡器对象 通过一系列的规则确定shard最适合被放在哪个node上
     */
    public static class Balancer {
        private final Logger logger;
        /**
         * key: nodeId
         * value: modelNode 对应一个routingNode下的数据 提供了需要的api接口
         * 内部不包含处于 relocate状态的shard
         */
        private final Map<String, ModelNode> nodes;
        private final RoutingAllocation allocation;
        /**
         * 将所有分片信息 按照node划分
         */
        private final RoutingNodes routingNodes;
        private final WeightFunction weight;

        /**
         * 在进行rebalance 之前需要确保  某个索引在节点上的最大权重值与最小权重值的差值 超过该值 否则无法进行rebalance
         */
        private final float threshold;
        /**
         * 该对象内部包含了所有索引的元数据信息
         */
        private final Metadata metadata;
        /**
         * 平均每个节点上分片的最佳数量  (不包含shardId相同的shard 因为 primary或者replica 是要分配在不同node上的)
         */
        private final float avgShardsPerNode;

        /**
         * 该对象负责将node 排序
         */
        private final NodeSorter sorter;

        /**
         * 初始化 balancer对象
         * @param logger
         * @param allocation
         * @param weight  这个对象是用来计算权重的
         * @param threshold
         */
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
         * 通过计算权重 将shard分散到node上   这里没有发生数据的迁移 只是将几个shard转换成了 relocation
         */
        private void balance() {
            if (logger.isTraceEnabled()) {
                logger.trace("Start balancing cluster");
            }
            // 正在异步拉取一些数据 此时先不进行处理 而在异步回调中还是会触发reroute 进而转发到 allocate上 最终还是会调用balance
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
            // 检测此时是否需要balance
            if (allocation.deciders().canRebalance(allocation).type() != Type.YES) {
                logger.trace("skipping rebalance as it is disabled");
                return;
            }
            // 单个节点就不需要balance了
            if (nodes.size() < 2) { /* skip if we only have one node */
                logger.trace("skipping rebalance as single node only");
                return;
            }
            // 基于权重值进行平衡  可以理解为将某个index下的所有分片在相关node上做平衡
            balanceByWeights();
        }

        /**
         * Makes a decision about moving a single shard to a different node to form a more
         * optimally balanced cluster.  This method is invoked from the cluster allocation
         * explain API only.
         * 尝试针对某个分片进行平衡
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
            // 计算该分片对应的索引在这个节点上的权重值
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

                // 计算这个索引在所有node上的权重值
                final float nodeWeight = weight.weight(this, node, idxName);
                // if the node we are examining has a worse (higher) weight than the node the shard is
                // assigned to, then there is no way moving the shard to the node with the worse weight
                // can make the balance of the cluster better, so we check for that here

                // 如果在该节点上的权重值 小于当前节点的权重值 那么认为这是一个更合适的节点
                final boolean betterWeightThanCurrent = nodeWeight <= currentWeight;
                boolean rebalanceConditionsMet = false;
                if (betterWeightThanCurrent) {
                    // get the delta between the weights of the node we are checking and the node that holds the shard

                    // 计算权重差值
                    float currentDelta = absDelta(nodeWeight, currentWeight);
                    // checks if the weight delta is above a certain threshold; if it is not above a certain threshold,
                    // then even though the node we are examining has a better weight and may make the cluster balance
                    // more even, it doesn't make sense to execute the heavyweight operation of relocating a shard unless
                    // the gains make it worth it, as defined by the threshold

                    // 判断权重差值是否与 阈值大
                    boolean deltaAboveThreshold = lessThan(currentDelta, threshold) == false;
                    // calculate the delta of the weights of the two nodes if we were to add the shard to the
                    // node in question and move it away from the node that currently holds it.

                    // 同时要确保权重差值 大于这个固定值
                    boolean betterWeightWithShardAdded = nodeWeight + 1.0f < currentWeight;

                    // 当2个判断大小的条件都满足时 该值才会为true
                    rebalanceConditionsMet = deltaAboveThreshold && betterWeightWithShardAdded;
                    // if the simulated weight delta with the shard moved away is better than the weight delta
                    // with the shard remaining on the current node, and we are allowed to allocate to the
                    // node in question, then allow the rebalance

                    // 这个在挑选最合适的目标节点
                    if (rebalanceConditionsMet && canAllocate.type().higherThan(rebalanceDecisionType)) {
                        // rebalance to the node, only will get overwritten if the decision here is to
                        // THROTTLE and we get a decision with YES on another node
                        rebalanceDecisionType = canAllocate.type();
                        assignedNode = node;
                    }
                }
                Tuple<ModelNode, Decision> nodeResult = Tuple.tuple(node, canAllocate);

                // 按照不同情况 将节点进行分组
                if (rebalanceConditionsMet) {
                    betterBalanceNodes.add(nodeResult);
                } else if (betterWeightThanCurrent) {
                    sameBalanceNodes.add(nodeResult);
                } else {
                    worseBalanceNodes.add(nodeResult);
                }
            }

            // 给结果何止不同的 rank 后填充到数组中
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

            // 如果 分配器认为此时不可以分配 或者此时集群中处于某种忙碌状态  返回一个等待结果
            if (canRebalance.type() != Type.YES || allocation.hasPendingAsyncFetch()) {
                AllocationDecision allocationDecision = allocation.hasPendingAsyncFetch() ? AllocationDecision.AWAITING_INFO :
                                                            AllocationDecision.fromDecisionType(canRebalance.type());
                return MoveDecision.cannotRebalance(canRebalance, allocationDecision, currentNodeWeightRanking, nodeDecisions);
            } else {
                // 返回当前结果
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
         * 以索引为单位 决定某个索引相关的所有节点下的分片应该怎样被重新分配
         */
        private void balanceByWeights() {
            // 获取所有决策对象
            final AllocationDeciders deciders = allocation.deciders();
            final ModelNode[] modelNodes = sorter.modelNodes;
            final float[] weights = sorter.weights;

            // 按照索引权重偏差大小排序  偏差大就代表该index下的分片在集群中分布不均匀 需要进行balance
            for (String index : buildWeightOrderedIndices()) {
                // 获取对应的索引元数据
                IndexMetadata indexMetadata = metadata.index(index);

                // find nodes that have a shard of this index or where shards of this index are allowed to be allocated to,
                // move these nodes to the front of modelNodes so that we can only balance based on these nodes
                int relevantNodes = 0;
                for (int i = 0; i < modelNodes.length; i++) {
                    ModelNode modelNode = modelNodes[i];

                    // 如果当前节点本身就包含了某个index的分片 那么必然可以参与该index之后的 balance
                    // 而通过deciders可以决定某个node之后是否可以参与某个index的balance  即使此时它可能没有该index的分片   这种比较合理
                    if (modelNode.getIndex(index) != null || deciders.canAllocate(indexMetadata, modelNode.getRoutingNode(), allocation).type() != Type.NO) {
                        // swap nodes at position i and relevantNodes
                        modelNodes[i] = modelNodes[relevantNodes];
                        modelNodes[relevantNodes] = modelNode;
                        relevantNodes++;
                    }
                }

                // 如果某index相关的分片仅能设置到一个node上 就不需要进行balance了
                if (relevantNodes < 2) {
                    continue;
                }

                // 几个有关的node 都被排在了一起 这时以node为单位再进行一次排序
                sorter.reset(index, 0, relevantNodes);
                int lowIdx = 0;
                int highIdx = relevantNodes - 1;

                while (true) {
                    // 分别获取当前index 在不同
                    final ModelNode minNode = modelNodes[lowIdx];
                    final ModelNode maxNode = modelNodes[highIdx];
                    advance_range:
                    if (maxNode.numShards(index) > 0) {
                        // 获取权重的差值
                        final float delta = absDelta(weights[lowIdx], weights[highIdx]);

                        // 此时node间的偏差值已经不足最小balance的阈值了 此时balance已经不划算了  不再进行
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
                            // 此时退出balance循环
                            break;
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("Balancing from node [{}] weight: [{}] to node [{}] weight: [{}]  delta: [{}]",
                                    maxNode.getNodeId(), weights[highIdx], minNode.getNodeId(), weights[lowIdx], delta);
                        }

                        // 这里偏差值有一个固定值 小于该值时 不经过continue
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

                            // 这里进行移动 源分片切换成relocation后会设置到 minNode(ModelNode) 中  如果deciders返回限制状态 返回false
                            // 一次只移动一个shard 然后会重新按照权重排序 这样分片就会尽可能的均匀了
                        } else if (tryRelocateShard(minNode, maxNode, index)) {
                            /*
                             * TODO we could be a bit smarter here, we don't need to fully sort necessarily
                             * we could just find the place to insert linearly but the win might be minor
                             * compared to the added complexity
                             * 重新计算权重值 因为一旦分配
                             */
                            weights[lowIdx] = sorter.weight(modelNodes[lowIdx]);
                            weights[highIdx] = sorter.weight(modelNodes[highIdx]);
                            // 将相关的node 按照权重重新排序
                            sorter.sort(0, relevantNodes);
                            lowIdx = 0;
                            highIdx = relevantNodes - 1;
                            continue;
                        }
                    }

                    // 2个游标会不断接近 这样获取到的在node级别的权重也会不断减小 当小于阈值时 不再进行balance
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
         * 按照索引的权重值进行排序
         */
        private String[] buildWeightOrderedIndices() {
            // 获取此时集群中所有的索引信息
            final String[] indices = allocation.routingTable().indicesRouting().keys().toArray(String.class);

            // 按照索引的排序顺序 将每个索引下 最大权重与最小权重的差值计算出来 并填充到数组中
            // 通过检查这些偏差值就可以知道某个索引的分片分配是不合理的  进而决定哪些index需要进行balance
            final float[] deltas = new float[indices.length];
            for (int i = 0; i < deltas.length; i++) {
                sorter.reset(indices[i]);
                deltas[i] = sorter.delta();
            }
            new IntroSorter() {

                float pivotWeight;

                /**
                 * 在swap函数中 同时交换索引和权重差值
                 * @param i
                 * @param j
                 */
                @Override
                protected void swap(int i, int j) {
                    final String tmpIdx = indices[i];
                    indices[i] = indices[j];
                    indices[j] = tmpIdx;
                    final float tmpDelta = deltas[i];
                    deltas[i] = deltas[j];
                    deltas[j] = tmpDelta;
                }

                /**
                 * 按权重差值大小进行排序
                 * @param i
                 * @param j
                 * @return
                 */
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
         * 尝试移动某些分片
         */
        public void moveShards() {
            // Iterate over the started shards interleaving between nodes, and check if they can remain. In the presence of throttling
            // shard movements, the goal of this iteration order is to achieve a fairer movement of shards from the nodes that are
            // offloading the shards.

            // 将所有节点下面的分片信息 平铺展示
            for (Iterator<ShardRouting> it = allocation.routingNodes().nodeInterleavedShardIterator(); it.hasNext(); ) {
                ShardRouting shardRouting = it.next();
                // 检测是否需要移动该分片
                final MoveDecision moveDecision = decideMove(shardRouting);

                // 代表需要进行移动
                if (moveDecision.isDecisionTaken() && moveDecision.forceMove()) {
                    final ModelNode sourceNode = nodes.get(shardRouting.currentNodeId());
                    final ModelNode targetNode = nodes.get(moveDecision.getTargetNode().getId());
                    // 在ModelNode这层将路由信息移除
                    sourceNode.removeShard(shardRouting);

                    // t1: 对应更改成relocation的原分片  t2: 对应在 target 生成的init分片
                    Tuple<ShardRouting, ShardRouting> relocatingShards = routingNodes.relocateShard(shardRouting, targetNode.getNodeId(),
                        allocation.clusterInfo().getShardSize(shardRouting,
                                                              ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE), allocation.changes());
                    // 将新产生的分片添加到 ModelNode中
                    targetNode.addShard(relocatingShards.v2());
                    if (logger.isTraceEnabled()) {
                        logger.trace("Moved shard [{}] to node [{}]", shardRouting, targetNode.getRoutingNode());
                    }

                    // 不需要移动该分片
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
         *      检测是否需要移动该分片    TODO 分片为什么要移动  比如在集群范围新增了一个node 它的负载很低 那么就有必要将一些分片转移上去 推测是处于这个理由
         */
        public MoveDecision decideMove(final ShardRouting shardRouting) {
            // 移动仅针对此时正常运行的分片  init/relocation 这种将会被忽略
            if (shardRouting.started() == false) {
                // we can only move started shards
                return MoveDecision.NOT_TAKEN;
            }

            // 先忽略DEBUG
            final boolean explain = allocation.debugDecision();
            // ModelNode以index为单位存储了shard信息
            final ModelNode sourceNode = nodes.get(shardRouting.currentNodeId());
            assert sourceNode != null && sourceNode.containsShard(shardRouting);

            RoutingNode routingNode = sourceNode.getRoutingNode();
            // 检测某个分片能否在某个节点上继续保留
            Decision canRemain = allocation.deciders().canRemain(shardRouting, routingNode, allocation);
            // 允许保留 也就是不需要移动
            if (canRemain.type() != Decision.Type.NO) {
                return MoveDecision.stay(canRemain);
            }

            // 将modelNode 按照此时的index计算权重并进行排序 这样就可以知道移动到哪个节点是最合适的
            sorter.reset(shardRouting.getIndexName());
            /*
             * the sorter holds the minimum weight node first for the shards index.
             * We now walk through the nodes until we find a node to allocate the shard.
             * This is not guaranteed to be balanced after this operation we still try best effort to
             * allocate on the minimal eligible node.
             * 下面寻找最合适的节点
             */
            Type bestDecision = Type.NO;
            RoutingNode targetNode = null;
            // TODO 先忽略 debug相关的
            final List<NodeAllocationResult> nodeExplanationMap = explain ? new ArrayList<>() : null;
            int weightRanking = 0;
            for (ModelNode currentNode : sorter.modelNodes) {
                // 跳过本节点的处理
                if (currentNode != sourceNode) {
                    RoutingNode target = currentNode.getRoutingNode();
                    // don't use canRebalance as we want hard filtering rules to apply. See #17698
                    // 检测能否分配到这个节点上
                    Decision allocationDecision = allocation.deciders().canAllocate(shardRouting, target, allocation);
                    // TODO
                    if (explain) {
                        nodeExplanationMap.add(new NodeAllocationResult(
                            currentNode.getRoutingNode().node(), allocationDecision, ++weightRanking));
                    }
                    // TODO maybe we can respect throttling here too?
                    if (allocationDecision.type().higherThan(bestDecision)) {
                        bestDecision = allocationDecision.type();
                        // 找到匹配的node后退出循环
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

            // 返回分片重定位后的目标节点
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
         * 将routingNodes 转换成 Map<String, ModelNode>
         *
         */
        private Map<String, ModelNode> buildModelFromAssigned() {
            Map<String, ModelNode> nodes = new HashMap<>();
            for (RoutingNode rn : routingNodes) {

                // 通过该node下面所有的shard 初始化modelNode对象
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

            // TODO 如果是通过 allocationService调用 本对象的allocate()方法 那么此时所有分片都应该已经分配完了
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

            // 内部存储了因为限流 无法加入到 RoutingNode的node   但是已经加入到ModelNode中  同时 deciders.canAllocate 返回非YES
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

                    // 代表分配成功 成功时allocationStatus 为null 但是在调用getAllocationDecision时 会将其转换成YES
                    if (allocationDecision.getAllocationDecision() == AllocationDecision.YES) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Assigned shard [{}] to [{}]", shard, minNode.getNodeId());
                        }

                        // 获取当前分片的大小  这个大小是从clusterInfo中获取的 应该是在哪里创建时就设置好的
                        final long shardSize = DiskThresholdDecider.getExpectedShardSize(shard,
                            ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                            allocation.clusterInfo(), allocation.metadata(), allocation.routingTable());
                        // 将分片状态修改成初始化并加入到 routingNodes中
                        // 同时触发监听器
                        // 当某个分片从未分配 变成初始化状态会分配一个随机的 allocationId
                        shard = routingNodes.initializeShard(shard, minNode.getNodeId(), null, shardSize, allocation.changes());

                        // 将分片添加到 ModelNode中
                        minNode.addShard(shard);
                        if (!shard.primary()) {
                            // copy over the same replica shards to the secondary array so they will get allocated
                            // in a subsequent iteration, allowing replicas of other shards to be allocated first
                            // 代表处理某个分片的相同副本
                            while(i < primaryLength-1 && comparator.compare(primary[i], primary[i+1]) == 0) {
                                secondary[secondaryLength++] = primary[++i];
                            }
                        }

                    // 某个分片分配失败了
                    } else {
                        // did *not* receive a YES decision
                        if (logger.isTraceEnabled()) {
                            logger.trace("No eligible node found to assign shard [{}] allocation_status [{}]", shard,
                                allocationDecision.getAllocationStatus());
                        }

                        // 代表此时最合适的节点处于 DECIDERS_THROTTLED 状态
                        if (minNode != null) {
                            // throttle decision scenario
                            assert allocationDecision.getAllocationStatus() == AllocationStatus.DECIDERS_THROTTLED;
                            // 获取该分片合适的大小
                            final long shardSize = DiskThresholdDecider.getExpectedShardSize(shard,
                                ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                                allocation.clusterInfo(), allocation.metadata(), allocation.routingTable());

                            minNode.addShard(shard.initialize(minNode.getNodeId(), null, shardSize));
                            final RoutingNode node = minNode.getRoutingNode();
                            // TODO 即使是YES也是以ignore的形式加入到 routingNode啊 有什么意义么
                            final Decision.Type nodeLevelDecision = deciders.canAllocate(node, allocation).type();
                            if (nodeLevelDecision != Type.YES) {
                                if (logger.isTraceEnabled()) {
                                    logger.trace("Can not allocate on node [{}] remove from round decision [{}]", node,
                                        allocationDecision.getAllocationStatus());
                                }
                                assert nodeLevelDecision == Type.NO;
                                // 此时无法加入时 将node设置到一个被限流的列表中
                                throttledNodes.add(minNode);
                            }
                        } else {
                            // 代表没有找到合适的节点
                            if (logger.isTraceEnabled()) {
                                logger.trace("No Node found to assign shard [{}]", shard);
                            }
                        }

                        // 将该分片以 ignore的形式加入到 RoutingNode中
                        unassigned.ignoreShard(shard, allocationDecision.getAllocationStatus(), allocation.changes());
                        if (!shard.primary()) { // we could not allocate it and we are a replica - check if we can ignore the other replicas
                            // 所有副本采用相同的处理策略
                            while(i < primaryLength-1 && comparator.compare(primary[i], primary[i+1]) == 0) {
                                unassigned.ignoreShard(primary[++i], allocationDecision.getAllocationStatus(), allocation.changes());
                            }
                        }
                    }
                }
                // 总共有多少次检测到成功的相同的副本
                primaryLength = secondaryLength;
                ShardRouting[] tmp = primary;
                // 将primary 替换成相同的副本对象
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
         * @param minNode 此时该索引对应的权重最小的node
         * @param maxNode 索引对应的权重最大的node
         *                某个index下面的部分分片 将会从maxNode转移到minNode
         */
        private boolean tryRelocateShard(ModelNode minNode, ModelNode maxNode, String idx) {

            // 此时该节点上必然有属于该index的分片 否则无法进行转移
            final ModelIndex index = maxNode.getIndex(idx);
            if (index != null) {
                logger.trace("Try relocating shard of [{}] from [{}] to [{}]", idx, maxNode.getNodeId(), minNode.getNodeId());

                // 获取此时所有start状态的分片 只有这些分片可以参与移动
                final Iterable<ShardRouting> shardRoutings = StreamSupport.stream(index.spliterator(), false)
                    .filter(ShardRouting::started) // cannot rebalance unassigned, initializing or relocating shards anyway
                    .filter(maxNode::containsShard)
                    .sorted(BY_DESCENDING_SHARD_ID) // check in descending order of shard id so that the decision is deterministic
                    ::iterator;

                final AllocationDeciders deciders = allocation.deciders();

                // 开始处理分片 当处理一定数量后应该就可以停止balance了 并且这个数值应该小于全部的分片数量
                for (ShardRouting shard : shardRoutings) {
                    // 首先通过deciders判断这个分片能否进行重分配  无法进行rebalance的跳过
                    final Decision rebalanceDecision = deciders.canRebalance(shard, allocation);
                    if (rebalanceDecision.type() == Type.NO) {
                        continue;
                    }
                    // 之后判断该分片能否分配到权重小的节点上  这里应该就要考虑到如果目标节点已经拥有相同shardId的分片就进行relocation
                    final Decision allocationDecision = deciders.canAllocate(shard, minNode.getRoutingNode(), allocation);
                    if (allocationDecision.type() == Type.NO) {
                        continue;
                    }

                    // 将结果整合
                    final Decision decision = new Decision.Multi().add(allocationDecision).add(rebalanceDecision);

                    // 这里代表已经允许移动分片了  所以从权重较大的节点中移除这个分片
                    maxNode.removeShard(shard);

                    // 获取这个分片预估的大小
                    long shardSize = allocation.clusterInfo().getShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);


                    // 代表允许进行rebalance
                    if (decision.type() == Type.YES) {
                        /* only allocate on the cluster if we are not throttled */
                        logger.debug("Relocate [{}] from [{}] to [{}]", shard, maxNode.getNodeId(), minNode.getNodeId());
                        // ModelNode 对于balance的分片的移动规则与 RoutingNode不同   RoutingNode 会同时存在一个sourceShard 和 一个targetShard
                        // 而ModelNode 则是一进一出 全局的shard数量不变
                        // TODO 这里为什么不是加入 v2啊
                        minNode.addShard(routingNodes.relocateShard(shard, minNode.getNodeId(), shardSize, allocation.changes()).v1());
                        return true;
                    } else {
                        /* allocate on the model even if throttled */
                        // 代表决策结果是此时处于限流阶段
                        logger.debug("Simulate relocation of [{}] from [{}] to [{}]", shard, maxNode.getNodeId(), minNode.getNodeId());
                        assert decision.type() == Type.THROTTLE;
                        // 被限制的不会直接加入到 routingNodes中  这个加入的规则跟move不一样   TODO move和rebalance是不一样的场景吗???  但是都是将start的分片修改成了relocation
                        minNode.addShard(shard.relocate(minNode.getNodeId(), shardSize));
                        return false;
                    }
                }
            }
            logger.trace("No shards of [{}] can relocate from [{}] to [{}]", idx, maxNode.getNodeId(), minNode.getNodeId());
            return false;
        }

    }

    /**
     * 负责提供访问 RoutingNode下所有分片的api
     * 分层是这样的  node -> index -> shard
     *         对应  modelNode -> modelIndex -> shardRouting
     */
    static class ModelNode implements Iterable<ModelIndex> {

        /**
         * 维护该node下所有分片对应的索引信息
         */
        private final Map<String, ModelIndex> indices = new HashMap<>();

        /**
         * 记录该node下总计存储了多少分片 为每个modelIndex下的分片总和
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
            // 通过shard.index 定位到具体的 modelIndex
            ModelIndex index = indices.get(shard.getIndexName());
            if (index == null) {
                index = new ModelIndex(shard.getIndexName());
                indices.put(index.getIndexId(), index);
            }
            // 将分片添加到索引中
            index.addShard(shard);
            numShards++;
        }

        /**
         * 将某个分片从该node中移除
         * @param shard
         */
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
     * 对应一个索引
     */
    static final class ModelIndex implements Iterable<ShardRouting> {

        /**
         * IndexId
         */
        private final String id;
        /**
         * 该所有的所有分片 其中可能有primary分片 也有replica 但是它们的shardId都是不同的 因为在一个node上不会出现多个shardId相同的分片
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
         * 以node为单位进行排序
         */
        final ModelNode[] modelNodes;
        /*
        * the nodes weights with respect to the current weight function / index
        * 每个节点有自己的权重值
        */
        final float[] weights;

        /**
         * 该函数负责计算每个节点的权重值
         */
        private final WeightFunction function;

        /**
         * 此时的排序顺序权重计算是基于哪个index
         */
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

        /**
         * 每当传入不同的index时 可以为这组node进行一次排序
         * @param index
         */
        public void reset(String index) {
            reset(index, 0, modelNodes.length);
        }

        /**
         * 根据对应的索引 node 计算其权重值
         * @param node
         * @return
         */
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

        /**
         * 计算最大/最小权重间的差值
         * @return
         */
        public float delta() {
            return weights[weights.length - 1] - weights[0];
        }
    }
}
