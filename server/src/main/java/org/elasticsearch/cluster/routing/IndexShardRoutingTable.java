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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.ResponseCollectorService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * {@link IndexShardRoutingTable} encapsulates all instances of a single shard.
 * Each Elasticsearch index consists of multiple shards, each shard encapsulates
 * a disjoint set of the index data and each shard has one or more instances
 * referred to as replicas of a shard. Given that, this class encapsulates all
 * replicas (instances) for a single index shard.
 *
 * IndexShardRoutingTable 与 RoutingNode 是以不同维度管理数据分片的
 * IndexShardRoutingTable 代表某个索引下某个分片以及相关副本的分配信息
 * RoutingNode            代表某个node下的所有分片
 */
public class IndexShardRoutingTable implements Iterable<ShardRouting> {

    /**
     * 分片路由打乱器
     */
    final ShardShuffler shuffler;

    /**
     * 下面所有的分片id 应该都是一致的
     */
    final ShardId shardId;

    /**
     * 每个shardId 对应的所有分片中 只有一个是主分片 其余都是副本
     */
    final ShardRouting primary;
    final List<ShardRouting> primaryAsList;

    /**
     * 在该容器中的所有分片都是在集群中存在副本的
     */
    final List<ShardRouting> replicas;
    final List<ShardRouting> shards;
    /**
     * 该列表中的分片都是处于 STARTED/RELOCATING
     */
    final List<ShardRouting> activeShards;

    /**
     * 存储所有 state 不是 UNASSIGNED 的shardRouting对象
     */
    final List<ShardRouting> assignedShards;

    /**
     * 存储了所有 ShardRouting 的AllocationId
     * AllocationId 应该是为该分片进行分配的分配器id
     */
    final Set<String> allAllocationIds;

    /**
     * 代表是否当前所有分片都处在 started状态
     */
    final boolean allShardsStarted;

    /**
     * The initializing list, including ones that are initializing on a target node because of relocation.
     * If we can come up with a better variable name, it would be nice..
     * 所有处于 INITIALIZING 的分片
     */
    final List<ShardRouting> allInitializingShards;


    /**
     * 每个  ShardRouting 代表一个最小粒度的数据分片  推测这些shard 都是针对同一个索引
     * @param shardId
     * @param shards
     */
    IndexShardRoutingTable(ShardId shardId, List<ShardRouting> shards) {
        this.shardId = shardId;

        // 生成打乱器对象
        this.shuffler = new RotationShardShuffler(Randomness.get().nextInt());
        this.shards = Collections.unmodifiableList(shards);

        ShardRouting primary = null;
        List<ShardRouting> replicas = new ArrayList<>();
        List<ShardRouting> activeShards = new ArrayList<>();
        List<ShardRouting> assignedShards = new ArrayList<>();
        List<ShardRouting> allInitializingShards = new ArrayList<>();
        Set<String> allAllocationIds = new HashSet<>();
        boolean allShardsStarted = true;
        // 开始遍历所有相关的分片对象
        for (ShardRouting shard : shards) {
            // 代表在集群中仅存在该单一分片
            if (shard.primary()) {
                primary = shard;
            } else {
                replicas.add(shard);
            }
            if (shard.active()) {
                activeShards.add(shard);
            }
            if (shard.initializing()) {
                allInitializingShards.add(shard);
            }
            if (shard.relocating()) {
                // create the target initializing shard routing on the node the shard is relocating to
                // 如果当前分片处于重分配状态 它必然有一个对应的 处于初始状态的分片对象
                allInitializingShards.add(shard.getTargetRelocatingShard());
                allAllocationIds.add(shard.getTargetRelocatingShard().allocationId().getId());

                assert shard.assignedToNode() : "relocating from unassigned " + shard;
                assert shard.getTargetRelocatingShard().assignedToNode() : "relocating to unassigned " + shard.getTargetRelocatingShard();
                assignedShards.add(shard.getTargetRelocatingShard());
            }
            // UNASSIGNED 是没有currentNodeId 的 也就是该数据分片还没有分配到集群中任何一个节点上
            if (shard.assignedToNode()) {
                assignedShards.add(shard);
                allAllocationIds.add(shard.allocationId().getId());
            }
            if (shard.state() != ShardRoutingState.STARTED) {
                allShardsStarted = false;
            }
        }
        this.allShardsStarted = allShardsStarted;
        this.primary = primary;
        if (primary != null) {
            this.primaryAsList = Collections.singletonList(primary);
        } else {
            this.primaryAsList = Collections.emptyList();
        }
        this.replicas = Collections.unmodifiableList(replicas);
        this.activeShards = Collections.unmodifiableList(activeShards);
        this.assignedShards = Collections.unmodifiableList(assignedShards);
        this.allInitializingShards = Collections.unmodifiableList(allInitializingShards);
        this.allAllocationIds = Collections.unmodifiableSet(allAllocationIds);
    }

    /**
     * Returns the shards id
     *
     * @return id of the shard
     */
    public ShardId shardId() {
        return shardId;
    }

    /**
     * Returns the shards id
     *
     * @return id of the shard
     */
    public ShardId getShardId() {
        return shardId();
    }

    /**
     * 这里遍历到的是所有的分片
     * @return
     */
    @Override
    public Iterator<ShardRouting> iterator() {
        return shards.iterator();
    }

    /**
     * Returns the number of this shards instances.
     */
    public int size() {
        return shards.size();
    }

    /**
     * Returns the number of this shards instances.
     */
    public int getSize() {
        return size();
    }

    /**
     * Returns a {@link List} of shards
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> shards() {
        return this.shards;
    }

    /**
     * Returns a {@link List} of shards
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> getShards() {
        return shards();
    }

    /**
     * Returns a {@link List} of active shards
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> activeShards() {
        return this.activeShards;
    }

    /**
     * Returns a {@link List} of all initializing shards, including target shards of relocations
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> getAllInitializingShards() {
        return this.allInitializingShards;
    }

    /**
     * Returns a {@link List} of active shards
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> getActiveShards() {
        return activeShards();
    }

    /**
     * Returns a {@link List} of assigned shards, including relocation targets
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> assignedShards() {
        return this.assignedShards;
    }

    /**
     * 在将所有分片打乱后 使用新顺序遍历分片
     * @return
     */
    public ShardIterator shardsRandomIt() {
        return new PlainShardIterator(shardId, shuffler.shuffle(shards));
    }

    public ShardIterator shardsIt() {
        return new PlainShardIterator(shardId, shards);
    }

    /**
     * 使用指定的随机值进行打乱
     * @param seed
     * @return
     */
    public ShardIterator shardsIt(int seed) {
        return new PlainShardIterator(shardId, shuffler.shuffle(shards, seed));
    }

    /**
     * Returns an iterator over active and initializing shards. Making sure though that
     * its random within the active shards, and initializing shards are the last to iterate through.
     */
    public ShardIterator activeInitializingShardsRandomIt() {
        return activeInitializingShardsIt(shuffler.nextSeed());
    }

    /**
     * Returns an iterator over active and initializing shards. Making sure though that
     * its random within the active shards, and initializing shards are the last to iterate through.
     * 返回活跃的 和初始状态的列表 同时将活跃的放在前面   并且活跃的打乱 初始化的不修改
     */
    public ShardIterator activeInitializingShardsIt(int seed) {
        if (allInitializingShards.isEmpty()) {
            // 没有处于初始状态的分片 仅返回所有活跃状态的分片
            return new PlainShardIterator(shardId, shuffler.shuffle(activeShards, seed));
        }
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        ordered.addAll(shuffler.shuffle(activeShards, seed));
        ordered.addAll(allInitializingShards);
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * Returns an iterator over active and initializing shards, ordered by the adaptive replica
     * selection formula. Making sure though that its random within the active shards of the same
     * (or missing) rank, and initializing shards are the last to iterate through.
     * @param collector 该对象负责统计一些数据 (responseTime serviceTime queueSize)
     *      将一些分片打乱后返回
     */
    public ShardIterator activeInitializingShardsRankedIt(@Nullable ResponseCollectorService collector,
                                                          @Nullable Map<String, Long> nodeSearchCounts) {
        final int seed = shuffler.nextSeed();
        // 没有处于init状态的分片 返回当前所有活跃分片
        if (allInitializingShards.isEmpty()) {
            return new PlainShardIterator(shardId,
                    rankShardsAndUpdateStats(shuffler.shuffle(activeShards, seed), collector, nodeSearchCounts));
        }

        // 分别将活跃分片 与初始状态分片打乱后设置到容器中
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        List<ShardRouting> rankedActiveShards =
                rankShardsAndUpdateStats(shuffler.shuffle(activeShards, seed), collector, nodeSearchCounts);
        ordered.addAll(rankedActiveShards);
        List<ShardRouting> rankedInitializingShards =
                rankShardsAndUpdateStats(allInitializingShards, collector, nodeSearchCounts);
        ordered.addAll(rankedInitializingShards);
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * 从分片路由中获取对应的节点信息
     * @param shards
     * @return
     */
    private static Set<String> getAllNodeIds(final List<ShardRouting> shards) {
        final Set<String> nodeIds = new HashSet<>();
        for (ShardRouting shard : shards) {
            nodeIds.add(shard.currentNodeId());
        }
        return nodeIds;
    }

    private static Map<String, Optional<ResponseCollectorService.ComputedNodeStats>>
        getNodeStats(final Set<String> nodeIds, final ResponseCollectorService collector) {

        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = new HashMap<>(nodeIds.size());
        for (String nodeId : nodeIds) {
            nodeStats.put(nodeId, collector.getNodeStatistics(nodeId));
        }
        return nodeStats;
    }

    /**
     * 实际上就是调用 stats的rank方法
     * @param nodeStats
     * @param nodeSearchCounts
     * @return
     */
    private static Map<String, Double> rankNodes(final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats,
                                                 final Map<String, Long> nodeSearchCounts) {
        final Map<String, Double> nodeRanks = new HashMap<>(nodeStats.size());
        for (Map.Entry<String, Optional<ResponseCollectorService.ComputedNodeStats>> entry : nodeStats.entrySet()) {
            Optional<ResponseCollectorService.ComputedNodeStats> maybeStats = entry.getValue();
            maybeStats.ifPresent(stats -> {
                final String nodeId = entry.getKey();
                // 这里默认值是 1 为啥???
                nodeRanks.put(nodeId, stats.rank(nodeSearchCounts.getOrDefault(nodeId, 1L)));
            });
        }
        return nodeRanks;
    }

    /**
     * Adjust the for all other nodes' collected stats. In the original ranking paper there is no need to adjust other nodes' stats because
     * Cassandra sends occasional requests to all copies of the data, so their stats will be updated during that broadcast phase. In
     * Elasticsearch, however, we do not have that sort of broadcast-to-all behavior. In order to prevent a node that gets a high score and
     * then never gets any more requests, we must ensure it eventually returns to a more normal score and can be a candidate for serving
     * requests.
     *
     * This adjustment takes the "winning" node's statistics and adds the average of those statistics with each non-winning node. Let's say
     * the winning node had a queue size of 10 and a non-winning node had a queue of 18. The average queue size is (10 + 18) / 2 = 14 so the
     * non-winning node will have statistics added for a queue size of 14. This is repeated for the response time and service times as well.
     * @param collector  采集数据的对象
     * @param nodeStats 该对象包含一个计算rank的api
     * @param minNodeId 当前rank最小的节点id
     * @param minStats 最小的节点对应的统计数据
     * 调整统计值
     */
    private static void adjustStats(final ResponseCollectorService collector,
                                    final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats,
                                    final String minNodeId,
                                    final ResponseCollectorService.ComputedNodeStats minStats) {
        if (minNodeId != null) {
            for (Map.Entry<String, Optional<ResponseCollectorService.ComputedNodeStats>> entry : nodeStats.entrySet()) {
                final String nodeId = entry.getKey();
                final Optional<ResponseCollectorService.ComputedNodeStats> maybeStats = entry.getValue();
                // 找到处理min 之外的其他节点
                if (nodeId.equals(minNodeId) == false && maybeStats.isPresent()) {
                    // 在跟min节点的数据综合后 生成一个新的统计对象 并填充到容器中
                    final ResponseCollectorService.ComputedNodeStats stats = maybeStats.get();
                    final int updatedQueue = (minStats.queueSize + stats.queueSize) / 2;
                    final long updatedResponse = (long) (minStats.responseTime + stats.responseTime) / 2;
                    final long updatedService = (long) (minStats.serviceTime + stats.serviceTime) / 2;
                    // 因为之前已经存在这些node的数据了 所以这里是做加权求平均数处理
                    collector.addNodeStatistics(nodeId, updatedQueue, updatedResponse, updatedService);
                }
            }
        }
    }

    /**
     * 按照一些统计项信息 对分片评级
     * @param shards  当前待处理的分片
     * @param collector   该对象具备收集节点 响应时间 服务时间等的能力
     * @param nodeSearchCounts
     * @return
     */
    private static List<ShardRouting> rankShardsAndUpdateStats(List<ShardRouting> shards, final ResponseCollectorService collector,
                                                               final Map<String, Long> nodeSearchCounts) {
        if (collector == null || nodeSearchCounts == null || shards.size() <= 1) {
            return shards;
        }

        // TODO 先忽略 默认情况下 collector/nodeSearchCounts 都为null
        // Retrieve which nodes we can potentially send the query to
        final Set<String> nodeIds = getAllNodeIds(shards);
        // 获取每个节点对应的统计数据  当对应数据不存在时  Optional为空
        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = getNodeStats(nodeIds, collector);

        // Retrieve all the nodes the shards exist on
        // 为每个 ComputedNodeStats 对象计算rank值
        final Map<String, Double> nodeRanks = rankNodes(nodeStats, nodeSearchCounts);

        // sort all shards based on the shard rank
        ArrayList<ShardRouting> sortedShards = new ArrayList<>(shards);
        // 使用rank值为每个对象进行排序
        Collections.sort(sortedShards, new NodeRankComparator(nodeRanks));

        // adjust the non-winner nodes' stats so they will get a chance to receive queries
        if (sortedShards.size() > 1) {
            // 获取rank分最小的shard 对象
            ShardRouting minShard = sortedShards.get(0);
            // If the winning shard is not started we are ranking initializing
            // shards, don't bother to do adjustments
            // 必须要确保最小的分片已经启动了  TODO 什么意思  为什么只处理最小rank的分片  以及为什么仅处理started状态的
            if (minShard.started()) {
                String minNodeId = minShard.currentNodeId();
                Optional<ResponseCollectorService.ComputedNodeStats> maybeMinStats = nodeStats.get(minNodeId);
                if (maybeMinStats.isPresent()) {
                    // 调整统计值
                    adjustStats(collector, nodeStats, minNodeId, maybeMinStats.get());
                    // Increase the number of searches for the "winning" node by one.
                    // Note that this doesn't actually affect the "real" counts, instead
                    // it only affects the captured node search counts, which is
                    // captured once for each query in TransportSearchAction
                    // 增加查询数
                    nodeSearchCounts.compute(minNodeId, (id, conns) -> conns == null ? 1 : conns + 1);
                }
            }
        }

        return sortedShards;
    }

    /**
     * 使用rank值比较大小
     */
    private static class NodeRankComparator implements Comparator<ShardRouting> {
        private final Map<String, Double> nodeRanks;

        NodeRankComparator(Map<String, Double> nodeRanks) {
            this.nodeRanks = nodeRanks;
        }

        @Override
        public int compare(ShardRouting s1, ShardRouting s2) {
            if (s1.currentNodeId().equals(s2.currentNodeId())) {
                // these shards on the same node
                return 0;
            }
            Double shard1rank = nodeRanks.get(s1.currentNodeId());
            Double shard2rank = nodeRanks.get(s2.currentNodeId());
            if (shard1rank != null) {
                if (shard2rank != null) {
                    return shard1rank.compareTo(shard2rank);
                } else {
                    // place non-nulls after null values
                    return 1;
                }
            } else {
                if (shard2rank != null) {
                    // place nulls before non-null values
                    return -1;
                } else {
                    // Both nodes do not have stats, they are equal
                    return 0;
                }
            }
        }
    }

    /**
     * Returns an iterator only on the primary shard.
     */
    public ShardIterator primaryShardIt() {
        return new PlainShardIterator(shardId, primaryAsList);
    }

    /**
     * 将该节点下所有active/init的数据分片返回
     * @param nodeId
     * @return
     */
    public ShardIterator onlyNodeActiveInitializingShardsIt(String nodeId) {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        int seed = shuffler.nextSeed();
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards, seed)) {
            if (nodeId.equals(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        for (ShardRouting shardRouting : shuffler.shuffle(allInitializingShards, seed)) {
            if (nodeId.equals(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardIterator onlyNodeSelectorActiveInitializingShardsIt(String nodeAttributes, DiscoveryNodes discoveryNodes) {
        return onlyNodeSelectorActiveInitializingShardsIt(new String[] {nodeAttributes}, discoveryNodes);
    }

    /**
     * Returns shards based on nodeAttributes given  such as node name , node attribute, node IP
     * Supports node specifications in cluster API
     * @param nodeAttributes 用于定位nodeId的查询条件
     */
    public ShardIterator onlyNodeSelectorActiveInitializingShardsIt(String[] nodeAttributes, DiscoveryNodes discoveryNodes) {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        Set<String> selectedNodes = Sets.newHashSet(discoveryNodes.resolveNodes(nodeAttributes));
        int seed = shuffler.nextSeed();
        // 每次返回的数据都是打乱顺序的  为啥
        // 这里将attr定位到的nodeId下 所有 active/init  的数据分片都设置到list中
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards, seed)) {
            if (selectedNodes.contains(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        for (ShardRouting shardRouting : shuffler.shuffle(allInitializingShards, seed)) {
            if (selectedNodes.contains(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        if (ordered.isEmpty()) {
            final String message = String.format(
                    Locale.ROOT,
                    "no data nodes with %s [%s] found for shard: %s",
                    nodeAttributes.length == 1 ? "criteria" : "criterion",
                    String.join(",", nodeAttributes),
                    shardId());
            throw new IllegalArgumentException(message);
        }
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * 优先将 nodeIds 的排在前面
     * @param nodeIds
     * @return
     */
    public ShardIterator preferNodeActiveInitializingShardsIt(Set<String> nodeIds) {
        ArrayList<ShardRouting> preferred = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        ArrayList<ShardRouting> notPreferred = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        // 先存储所有活跃的分片数据 并且 命中nodeIds的排在前面 之后插入所有init的节点

        // fill it in a randomized fashion
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards)) {
            if (nodeIds.contains(shardRouting.currentNodeId())) {
                preferred.add(shardRouting);
            } else {
                notPreferred.add(shardRouting);
            }
        }
        preferred.addAll(notPreferred);
        if (!allInitializingShards.isEmpty()) {
            preferred.addAll(allInitializingShards);
        }
        return new PlainShardIterator(shardId, preferred);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexShardRoutingTable that = (IndexShardRoutingTable) o;

        if (!shardId.equals(that.shardId)) return false;
        if (!shards.equals(that.shards)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = shardId.hashCode();
        result = 31 * result + shards.hashCode();
        return result;
    }

    /**
     * Returns <code>true</code> iff all shards in the routing table are started otherwise <code>false</code>
     * 检测是否所有的分片此时都处于started状态
     */
    public boolean allShardsStarted() {
        return allShardsStarted;
    }

    /**
     * 找到与分配id 匹配的分片对象
     * @param allocationId
     * @return
     */
    @Nullable
    public ShardRouting getByAllocationId(String allocationId) {
        for (ShardRouting shardRouting : assignedShards()) {
            if (shardRouting.allocationId().getId().equals(allocationId)) {
                return shardRouting;
            }
        }
        return null;
    }

    public Set<String> getAllAllocationIds() {
        return allAllocationIds;
    }

    /**
     * 描述一组特殊的属性
     */
    static class AttributesKey {

        final List<String> attributes;

        AttributesKey(List<String> attributes) {
            this.attributes = attributes;
        }

        @Override
        public int hashCode() {
            return attributes.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof AttributesKey && attributes.equals(((AttributesKey) obj).attributes);
        }
    }

    public ShardRouting primaryShard() {
        return primary;
    }

    public List<ShardRouting> replicaShards() {
        return this.replicas;
    }

    /**
     * 将副本中 符合状态的所有分片返回  replicas 存储的是除了 primary外的其他分片
     * @param states
     * @return
     */
    public List<ShardRouting> replicaShardsWithState(ShardRoutingState... states) {
        List<ShardRouting> shards = new ArrayList<>();
        for (ShardRouting shardEntry : replicas) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    shards.add(shardEntry);
                }
            }
        }
        return shards;
    }

    public List<ShardRouting> shardsWithState(ShardRoutingState state) {
        if (state == ShardRoutingState.INITIALIZING) {
            return allInitializingShards;
        }
        List<ShardRouting> shards = new ArrayList<>();
        for (ShardRouting shardEntry : this) {
            if (shardEntry.state() == state) {
                shards.add(shardEntry);
            }
        }
        return shards;
    }

    /**
     * 建造器模式
     */
    public static class Builder {

        /**
         * 某个 IndexShardRoutingTable下的 shardId 都是一样的 也就意味着 他们对应的index都是一样的
         */
        private ShardId shardId;
        private final List<ShardRouting> shards;

        public Builder(IndexShardRoutingTable indexShard) {
            this.shardId = indexShard.shardId;
            this.shards = new ArrayList<>(indexShard.shards);
        }

        public Builder(ShardId shardId) {
            this.shardId = shardId;
            this.shards = new ArrayList<>();
        }

        public Builder addShard(ShardRouting shardEntry) {
            shards.add(shardEntry);
            return this;
        }

        public Builder removeShard(ShardRouting shardEntry) {
            shards.remove(shardEntry);
            return this;
        }

        public IndexShardRoutingTable build() {
            // don't allow more than one shard copy with same id to be allocated to same node
            assert distinctNodes(shards) : "more than one shard with same id assigned to same node (shards: " + shards + ")";
            return new IndexShardRoutingTable(shardId, List.copyOf(shards));
        }

        static boolean distinctNodes(List<ShardRouting> shards) {
            Set<String> nodes = new HashSet<>();
            for (ShardRouting shard : shards) {
                if (shard.assignedToNode()) {
                    if (nodes.add(shard.currentNodeId()) == false) {
                        return false;
                    }
                    if (shard.relocating()) {
                        if (nodes.add(shard.relocatingNodeId()) == false) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        public static IndexShardRoutingTable readFrom(StreamInput in) throws IOException {
            Index index = new Index(in);
            return readFromThin(in, index);
        }

        public static IndexShardRoutingTable readFromThin(StreamInput in, Index index) throws IOException {
            int iShardId = in.readVInt();
            ShardId shardId = new ShardId(index, iShardId);
            Builder builder = new Builder(shardId);

            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                ShardRouting shard = new ShardRouting(shardId, in);
                builder.addShard(shard);
            }

            return builder.build();
        }

        public static void writeTo(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            indexShard.shardId().getIndex().writeTo(out);
            writeToThin(indexShard, out);
        }

        public static void writeToThin(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            out.writeVInt(indexShard.shardId.id());

            out.writeVInt(indexShard.shards.size());
            for (ShardRouting entry : indexShard) {
                entry.writeToThin(out);
            }
        }

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IndexShardRoutingTable(").append(shardId()).append("){");
        final int numShards = shards.size();
        for (int i = 0; i < numShards; i++) {
            sb.append(shards.get(i).shortSummary());
            if (i < numShards - 1) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
