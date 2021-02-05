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

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.Assertions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;

/**
 * {@link RoutingNodes} represents a copy the routing information contained in the {@link ClusterState cluster state}.
 * It can be either initialized as mutable or immutable (see {@link #RoutingNodes(ClusterState, boolean)}), allowing
 * or disallowing changes to its elements.
 *
 * The main methods used to update routing entries are:
 * <ul>
 * <li> {@link #initializeShard} initializes an unassigned shard.
 * <li> {@link #startShard} starts an initializing shard / completes relocation of a shard.
 * <li> {@link #relocateShard} starts relocation of a started shard.
 * <li> {@link #failShard} fails/cancels an assigned shard.
 * </ul>
 * 直接将所有分片副本的路由信息按照node进行划分 这样就可以快速找到某个分片的副本在哪个node上还没有分配
 */
public class RoutingNodes implements Iterable<RoutingNode> {

    private final Map<String, RoutingNode> nodesToShards = new HashMap<>();

    /**
     * 该对象管理了当前集群所有节点中未分配的分片
     */
    private final UnassignedShards unassignedShards = new UnassignedShards(this);

    /**
     * 所有已经分配好的node
     */
    private final Map<ShardId, List<ShardRouting>> assignedShards = new HashMap<>();

    /**
     * 如果当前对象是只读对象 不允许修改
     */
    private final boolean readOnly;

    /**
     * 记录此时有多少primary分片 处于init状态
     */
    private int inactivePrimaryCount = 0;

    /**
     * 包含primary and replicate在内 处于init状态的数量
     * 未分配的分片不算在内
     */
    private int inactiveShardCount = 0;

    /**
     * 对应此时正在进行relocate的分片
     */
    private int relocatingShards = 0;

    /**
     * key 对应node value对应每个node的属性 同时属性可以通过下标检索
     */
    private final Map<String, ObjectIntHashMap<String>> nodesPerAttributeNames = new HashMap<>();

    /**
     * 存储此时需要进行数据恢复的节点
     */
    private final Map<String, Recoveries> recoveriesPerNode = new HashMap<>();

    /**
     * 默认情况下该对象就是只读的 也就是无法修改 UnassignedShards 迭代器内部的元素
     * @param clusterState
     */
    public RoutingNodes(ClusterState clusterState) {
        this(clusterState, true);
    }

    /**
     * 通过之前以索引为单位维护的分片信息转换成以node为维度
     * @param clusterState  通过当前集群状态对象进行初始化
     * @param readOnly 是否只读 默认是false  如果是只读对象 是不允许修改内部数据的
     */
    public RoutingNodes(ClusterState clusterState, boolean readOnly) {
        this.readOnly = readOnly;

        // 该对象维护了集群下所有分片 以及分片副本的分配情况
        final RoutingTable routingTable = clusterState.routingTable();

        // key1: nodeId  key2: shardId  value: replicate
        Map<String, LinkedHashMap<ShardId, ShardRouting>> nodesToShards = new HashMap<>();
        // fill in the nodeToShards with the "live" nodes
        // 只需要遍历数据节点  因为其他节点是不支持存储数据的
        for (ObjectCursor<DiscoveryNode> cursor : clusterState.nodes().getDataNodes().values()) {
            nodesToShards.put(cursor.value.getId(), new LinkedHashMap<>()); // LinkedHashMap to preserve order
        }

        // fill in the inverse of node -> shards allocated
        // also fill replicaSet information
        // 当前集群下所有索引
        for (ObjectCursor<IndexRoutingTable> indexRoutingTable : routingTable.indicesRouting().values()) {
            // 遍历每个分片 再下一级则是 primary/replica
            for (IndexShardRoutingTable indexShard : indexRoutingTable.value) {
                assert indexShard.primary != null;

                for (ShardRouting shard : indexShard) {
                    // to get all the shards belonging to an index, including the replicas,
                    // we define a replica set and keep track of it. A replica set is identified
                    // by the ShardId, as this is common for primary and replicas.
                    // A replica Set might have one (and not more) replicas with the state of RELOCATING.
                    // 代表这个副本已经决定好分配的node了
                    if (shard.assignedToNode()) {
                        Map<ShardId, ShardRouting> entries = nodesToShards.computeIfAbsent(shard.currentNodeId(),
                            k -> new LinkedHashMap<>()); // LinkedHashMap to preserve order
                        ShardRouting previousValue = entries.put(shard.shardId(), shard);
                        // 同一个分片的多个副本不能分配到同一个节点上
                        if (previousValue != null) {
                            throw new IllegalArgumentException("Cannot have two different shards with same shard id on same node");
                        }
                        // 增加一个已分配的分片
                        assignedShardsAdd(shard);

                        // 代表该分片处于重分配中
                        if (shard.relocating()) {
                            relocatingShards++;
                            // LinkedHashMap to preserve order.
                            // Add the counterpart shard with relocatingNodeId reflecting the source from which
                            // it's relocating from.
                            // 目标节点信息也会存储到 nodesToShards中
                            entries = nodesToShards.computeIfAbsent(shard.relocatingNodeId(),
                                k -> new LinkedHashMap<>());
                            // 当分片处于重分配状态时 会在内部存储一个 targetRelocating  对应重分配后的位置   目标分片才是需要恢复数据的分片
                            ShardRouting targetShardRouting = shard.getTargetRelocatingShard();
                            // 参与数据恢复的分片有2种 一种是向外输出数据 一种是接收数据 对应主分片与副本
                            addInitialRecovery(targetShardRouting, indexShard.primary);
                            previousValue = entries.put(targetShardRouting.shardId(), targetShardRouting);
                            if (previousValue != null) {
                                throw new IllegalArgumentException("Cannot have two different shards with same shard id on same node");
                            }
                            // 重分配后的节点也加入到 assigned中
                            assignedShardsAdd(targetShardRouting);
                            // 分片刚创建的时候还没有被分配 在被rerouting服务分配后 分片才变成init状态 之后就是进行数据恢复
                        } else if (shard.initializing()) {
                            if (shard.primary()) {
                                inactivePrimaryCount++;
                            }
                            inactiveShardCount++;
                            // 初始状态的分片也是需要恢复数据的
                            addInitialRecovery(shard, indexShard.primary);
                        }
                    } else {
                        unassignedShards.add(shard);
                    }
                }
            }
        }
        // 此时分片已经按照node分组
        for (Map.Entry<String, LinkedHashMap<ShardId, ShardRouting>> entry : nodesToShards.entrySet()) {
            String nodeId = entry.getKey();
            // 将属于同一node 的所有分片对象合并成 RoutingNode
            this.nodesToShards.put(nodeId, new RoutingNode(nodeId, clusterState.nodes().get(nodeId), entry.getValue()));
        }
    }

    /**
     * 当某个分片从未分配状态变成init状态后 就代表该分片需要进行数据恢复了
     * 数据恢复将会在该分片被分配到的节点上执行 并且在分片数据恢复完成后 节点会通知leader 将分片转换成started状态 同时触发 removeRecovery 代表本分片完成了数据恢复
     * @param routing
     */
    private void addRecovery(ShardRouting routing) {
        updateRecoveryCounts(routing, true, findAssignedPrimaryIfPeerRecovery(routing));
    }

    /**
     * 表示此时有某个分片结束了recovery阶段
     * @param routing
     */
    private void removeRecovery(ShardRouting routing) {
        updateRecoveryCounts(routing, false, findAssignedPrimaryIfPeerRecovery(routing));
    }

    /**
     *
     * @param routing  从主分片拉取数据的分片
     * @param initialPrimaryShard  用于输出数据的主分片
     */
    private void addInitialRecovery(ShardRouting routing, ShardRouting initialPrimaryShard) {
        updateRecoveryCounts(routing, true, initialPrimaryShard);
    }

    /**
     * 更新当前需要恢复数据的分片信息
     * @param routing  需要恢复数据的分片
     * @param increment  新增需要恢复的副本/又完成了一个需要恢复的副本
     * @param primary   当routing是主分片时 primary与routing一样都是主分片
     */
    private void updateRecoveryCounts(final ShardRouting routing, final boolean increment, @Nullable final ShardRouting primary) {
        final int howMany = increment ? 1 : -1;
        assert routing.initializing() : "routing must be initializing: " + routing;
        // TODO: check primary == null || primary.active() after all tests properly add ReplicaAfterPrimaryActiveAllocationDecider
        assert primary == null || primary.assignedToNode() :
            "shard is initializing but its primary is not assigned to a node";

        // 记录某个节点上此时总计要恢复数据的分片数量  这里主分片也会记录
        Recoveries.getOrAdd(recoveriesPerNode, routing.currentNodeId()).addIncoming(howMany);

        // 普通副本恢复数据 使用的类型就是 PEER
        if (routing.recoverySource().getType() == RecoverySource.Type.PEER) {
            // add/remove corresponding outgoing recovery on node with primary shard
            if (primary == null) {
                throw new IllegalStateException("shard is peer recovering but primary is unassigned");
            }
            // 增加主分片向外输出的数据量
            Recoveries.getOrAdd(recoveriesPerNode, primary.currentNodeId()).addOutgoing(howMany);

            // TODO 等到了 increment == false的场景再看
            if (increment == false && routing.primary() && routing.relocatingNodeId() != null) {
                // primary is done relocating, move non-primary recoveries from old primary to new primary
                int numRecoveringReplicas = 0;
                // 找到所有需要从远端恢复的分片
                for (ShardRouting assigned : assignedShards(routing.shardId())) {
                    if (assigned.primary() == false && assigned.initializing() &&
                        assigned.recoverySource().getType() == RecoverySource.Type.PEER) {
                        numRecoveringReplicas++;
                    }
                }
                // 更新变动
                recoveriesPerNode.get(routing.relocatingNodeId()).addOutgoing(-numRecoveringReplicas);
                recoveriesPerNode.get(routing.currentNodeId()).addOutgoing(numRecoveringReplicas);
            }
        }
    }

    public int getIncomingRecoveries(String nodeId) {
        return recoveriesPerNode.getOrDefault(nodeId, Recoveries.EMPTY).getIncoming();
    }

    public int getOutgoingRecoveries(String nodeId) {
        return recoveriesPerNode.getOrDefault(nodeId, Recoveries.EMPTY).getOutgoing();
    }

    /**
     * 如果本分片是一个副本 那么找到用于恢复数据的主分片
     * @param routing
     * @return
     */
    @Nullable
    private ShardRouting findAssignedPrimaryIfPeerRecovery(ShardRouting routing) {
        ShardRouting primary = null;
        if (routing.recoverySource() != null && routing.recoverySource().getType() == RecoverySource.Type.PEER) {
            List<ShardRouting> shardRoutings = assignedShards.get(routing.shardId());
            if (shardRoutings != null) {
                for (ShardRouting shardRouting : shardRoutings) {
                    if (shardRouting.primary()) {
                        // 当此时主分片处于活跃状态 也就是已经完成了数据恢复 那么可以返回
                        // 如果此时主分片处于 relocate状态 新生成的分片还是主分片 同时原主分片还是处于active状态 直接从它那里拷贝数据
                        if (shardRouting.active()) {
                            return shardRouting;
                        } else if (primary == null) {
                            primary = shardRouting;
                        } else if (primary.relocatingNodeId() != null) {
                            primary = shardRouting;
                        }
                    }
                }
            }
        }
        return primary;
    }

    @Override
    public Iterator<RoutingNode> iterator() {
        return Collections.unmodifiableCollection(nodesToShards.values()).iterator();
    }

    public Iterator<RoutingNode> mutableIterator() {
        ensureMutable();
        return nodesToShards.values().iterator();
    }

    public UnassignedShards unassigned() {
        return this.unassignedShards;
    }

    public RoutingNode node(String nodeId) {
        return nodesToShards.get(nodeId);
    }

    public ObjectIntHashMap<String> nodesPerAttributesCounts(String attributeName) {
        ObjectIntHashMap<String> nodesPerAttributesCounts = nodesPerAttributeNames.get(attributeName);
        if (nodesPerAttributesCounts != null) {
            return nodesPerAttributesCounts;
        }
        nodesPerAttributesCounts = new ObjectIntHashMap<>();
        for (RoutingNode routingNode : this) {
            String attrValue = routingNode.node().getAttributes().get(attributeName);
            nodesPerAttributesCounts.addTo(attrValue, 1);
        }
        nodesPerAttributeNames.put(attributeName, nodesPerAttributesCounts);
        return nodesPerAttributesCounts;
    }

    /**
     * Returns <code>true</code> iff this {@link RoutingNodes} instance has any unassigned primaries even if the
     * primaries are marked as temporarily ignored.
     */
    public boolean hasUnassignedPrimaries() {
        return unassignedShards.getNumPrimaries() + unassignedShards.getNumIgnoredPrimaries() > 0;
    }

    /**
     * Returns <code>true</code> iff this {@link RoutingNodes} instance has any unassigned shards even if the
     * shards are marked as temporarily ignored.
     * @see UnassignedShards#isEmpty()
     * @see UnassignedShards#isIgnoredEmpty()
     */
    public boolean hasUnassignedShards() {
        return unassignedShards.isEmpty() == false || unassignedShards.isIgnoredEmpty() == false;
    }

    public boolean hasInactivePrimaries() {
        return inactivePrimaryCount > 0;
    }

    public boolean hasInactiveShards() {
        return inactiveShardCount > 0;
    }

    public int getRelocatingShardCount() {
        return relocatingShards;
    }

    /**
     * Returns all shards that are not in the state UNASSIGNED with the same shard
     * ID as the given shard.
     */
    public List<ShardRouting> assignedShards(ShardId shardId) {
        final List<ShardRouting> replicaSet = assignedShards.get(shardId);
        return replicaSet == null ? EMPTY : Collections.unmodifiableList(replicaSet);
    }

    @Nullable
    public ShardRouting getByAllocationId(ShardId shardId, String allocationId) {
        final List<ShardRouting> replicaSet = assignedShards.get(shardId);
        if (replicaSet == null) {
            return null;
        }
        for (ShardRouting shardRouting : replicaSet) {
            if (shardRouting.allocationId().getId().equals(allocationId)) {
                return shardRouting;
            }
        }
        return null;
    }

    /**
     * Returns the active primary shard for the given shard id or <code>null</code> if
     * no primary is found or the primary is not active.
     */
    public ShardRouting activePrimary(ShardId shardId) {
        for (ShardRouting shardRouting : assignedShards(shardId)) {
            if (shardRouting.primary() && shardRouting.active()) {
                return shardRouting;
            }
        }
        return null;
    }

    /**
     * Returns one active replica shard for the given shard id or <code>null</code> if
     * no active replica is found.
     *
     * Since replicas could possibly be on nodes with a older version of ES than
     * the primary is, this will return replicas on the highest version of ES.
     * 找到所有存活的副本中 版本最高的
     */
    public ShardRouting activeReplicaWithHighestVersion(ShardId shardId) {
        // It's possible for replicaNodeVersion to be null, when disassociating dead nodes
        // that have been removed, the shards are failed, and part of the shard failing
        // calls this method with an out-of-date RoutingNodes, where the version might not
        // be accessible. Therefore, we need to protect against the version being null
        // (meaning the node will be going away).
        return assignedShards(shardId).stream()
            // active 为true 就代表已经完成了 PeerRecovery
                .filter(shr -> !shr.primary() && shr.active())
                .filter(shr -> node(shr.currentNodeId()) != null)
            // 将版本号最高的副本返回
                .max(Comparator.comparing(shr -> node(shr.currentNodeId()).node(),
                                Comparator.nullsFirst(Comparator.comparing(DiscoveryNode::getVersion))))
                .orElse(null);
    }

    /**
     * Returns <code>true</code> iff all replicas are active for the given shard routing. Otherwise <code>false</code>
     * 检测该分片下的所有副本是否都处于激活状态
     */
    public boolean allReplicasActive(ShardId shardId, Metadata metadata) {
        // 获取该shardId 对应的所有已经分配完成的副本
        final List<ShardRouting> shards = assignedShards(shardId);
        // 因为shards 应该是 primary + replica 所以 当数量不足要求时返回false
        if (shards.isEmpty() || shards.size() < metadata.getIndexSafe(shardId.getIndex()).getNumberOfReplicas() + 1) {
            return false; // if we are empty nothing is active if we have less than total at least one is unassigned
        }
        for (ShardRouting shard : shards) {
            if (!shard.active()) {
                return false;
            }
        }
        return true;
    }

    public List<ShardRouting> shards(Predicate<ShardRouting> predicate) {
        List<ShardRouting> shards = new ArrayList<>();
        for (RoutingNode routingNode : this) {
            for (ShardRouting shardRouting : routingNode) {
                if (predicate.test(shardRouting)) {
                    shards.add(shardRouting);
                }
            }
        }
        return shards;
    }

    public List<ShardRouting> shardsWithState(ShardRoutingState... state) {
        // TODO these are used on tests only - move into utils class
        List<ShardRouting> shards = new ArrayList<>();
        for (RoutingNode routingNode : this) {
            shards.addAll(routingNode.shardsWithState(state));
        }
        for (ShardRoutingState s : state) {
            if (s == ShardRoutingState.UNASSIGNED) {
                unassigned().forEach(shards::add);
                break;
            }
        }
        return shards;
    }

    public List<ShardRouting> shardsWithState(String index, ShardRoutingState... state) {
        // TODO these are used on tests only - move into utils class
        List<ShardRouting> shards = new ArrayList<>();
        for (RoutingNode routingNode : this) {
            shards.addAll(routingNode.shardsWithState(index, state));
        }
        for (ShardRoutingState s : state) {
            if (s == ShardRoutingState.UNASSIGNED) {
                for (ShardRouting unassignedShard : unassignedShards) {
                    if (unassignedShard.index().getName().equals(index)) {
                        shards.add(unassignedShard);
                    }
                }
                break;
            }
        }
        return shards;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("routing_nodes:\n");
        for (RoutingNode routingNode : this) {
            sb.append(routingNode.prettyPrint());
        }
        sb.append("---- unassigned\n");
        for (ShardRouting shardEntry : unassignedShards) {
            sb.append("--------").append(shardEntry.shortSummary()).append('\n');
        }
        return sb.toString();
    }

    /**
     * Moves a shard from unassigned to initialize state
     *
     * @param existingAllocationId allocation id to use. If null, a fresh allocation id is generated.
     * @param expectedSize 该分片预期会使用的大小
     * @return                     the initialized shard
     * 将某个分片从 unassigned状态转换成init 状态
     */
    public ShardRouting initializeShard(ShardRouting unassignedShard, String nodeId, @Nullable String existingAllocationId,
                                        long expectedSize, RoutingChangesObserver routingChangesObserver) {
        ensureMutable();
        assert unassignedShard.unassigned() : "expected an unassigned shard " + unassignedShard;
        // 基于原本的shardRouting信息 加上本次传入的新信息 生成新的shardRouting对象
        ShardRouting initializedShard = unassignedShard.initialize(nodeId, existingAllocationId, expectedSize);
        // nodesToShards 本身就只存储分配好的分片  所以此时满足条件 设置到容器中
        node(nodeId).add(initializedShard);
        // 增加了一个非活跃的分片
        inactiveShardCount++;
        // 如果该分片刚好是主分片 那么增加一个非活跃的主分片
        if (initializedShard.primary()) {
            inactivePrimaryCount++;
        }
        // 增加一个需要恢复数据的分片 当某个分片被初始化后 内部还没有数据
        addRecovery(initializedShard);
        // 增加一个已经完成分配的分片
        assignedShardsAdd(initializedShard);
        // 触发观察者钩子
        routingChangesObserver.shardInitialized(unassignedShard, initializedShard);
        return initializedShard;
    }

    /**
     * Relocate a shard to another node, adding the target initializing
     * shard as well as assigning it.
     *
     * @param startedShard 原分片信息
     * @param nodeId  移动到哪个目标节点
     * @param expectedShardSize 推荐的分片数量
     * @return pair of source relocating and target initializing shards.
     * 将某个分片移动到目标分片    (原节点可能数据负载比较高 新节点可能是刚创建的  就会考虑进行数据迁移)
     */
    public Tuple<ShardRouting,ShardRouting> relocateShard(ShardRouting startedShard, String nodeId, long expectedShardSize,
                                                          RoutingChangesObserver changes) {
        ensureMutable();
        relocatingShards++;

        // 主分片重定向后生成的分片 还是主分片
        // 源分片的 currentNodeId, relocationId  与target的正好相反   这里target分片会使用一个新的 allocateId
        ShardRouting source = startedShard.relocate(nodeId, expectedShardSize);
        // 获取重定向后的targetNode对应的分片  重定向的分片recoverySource 总是PEER
        ShardRouting target = source.getTargetRelocatingShard();

        // 用source 替换 startedShard
        updateAssigned(startedShard, source);
        // 在目标节点上增加一个新的分片
        node(target.currentNodeId()).add(target);
        // 将新分片设置到 assignedShards 中
        assignedShardsAdd(target);
        // 因为 target处于 init状态 所以加入到待恢复的容器中
        addRecovery(target);
        // 触发相关钩子
        changes.relocationStarted(startedShard, target);
        return Tuple.tuple(source, target);
    }

    /**
     * Applies the relevant logic to start an initializing shard.
     *
     * Moves the initializing shard to started. If the shard is a relocation target, also removes the relocation source.
     *
     * If the started shard is a primary relocation target, this also reinitializes currently initializing replicas as their
     * recovery source changes
     *
     * @param initializingShard 本次需要修改成start状态的分片
     * @param routingChangesObserver 监听分片变化    是从 RoutingAllocation中抽取出来的 该对象内部内置了3个observer对象
     * @return the started shard
     */
    public ShardRouting startShard(Logger logger, ShardRouting initializingShard, RoutingChangesObserver routingChangesObserver) {
        ensureMutable();
        // 将init分片修改成start状态 同时更新了一些内部的属性
        ShardRouting startedShard = started(initializingShard);
        logger.trace("{} marked shard as started (routing: {})", initializingShard.shardId(), initializingShard);

        // 触发观察者钩子
        routingChangesObserver.shardStarted(initializingShard, startedShard);

        // 代表这是一个重定向产生的init分片 也是从primary拉取数据   可能是primary发生relocate 也可能是某个副本发生relocate
        if (initializingShard.relocatingNodeId() != null) {
            // relocation target has been started, remove relocation source
            RoutingNode relocationSourceNode = node(initializingShard.relocatingNodeId());
            ShardRouting relocationSourceShard = relocationSourceNode.getByShardId(initializingShard.shardId());
            assert relocationSourceShard.isRelocationSourceOf(initializingShard);
            assert relocationSourceShard.getTargetRelocatingShard() == initializingShard : "relocation target mismatch, expected: "
                + initializingShard + " but was: " + relocationSourceShard.getTargetRelocatingShard();

            // 这里移除了relocating的分片 那么在对应节点就会自然的移除indexShard
            remove(relocationSourceShard);
            // 之后会将该分片从inSync中移除
            routingChangesObserver.relocationCompleted(relocationSourceShard);

            // if this is a primary shard with ongoing replica recoveries, reinitialize them as their recovery source changed
            // 如果是主分片发生了relocate  这里会影响到所有副本的recovery  比如一开始在主分片a上进行recovery 同时a也在进行relocate target为b 当完成后 副本应该从b上获取数据
            if (startedShard.primary()) {
                // 找到某个shardId 对应的所有分片 包含所有副本
                List<ShardRouting> assignedShards = assignedShards(startedShard.shardId());
                // copy list to prevent ConcurrentModificationException
                for (ShardRouting routing : new ArrayList<>(assignedShards)) {
                    // 找到处于初始状态的所有副本 因为副本需要进行peerRecovery 所以需要追踪最新的primary分片
                    // 而处于 relocateSource 或者 start的分片则是记录在 inSync中 由replicateGroup进行管理
                    if (routing.initializing() && routing.primary() == false) {
                        // 如果是某次 relocating的目标分片
                        if (routing.isRelocationTarget()) {
                            // find the relocation source
                            // 找到source 分片
                            ShardRouting sourceShard = getByAllocationId(routing.shardId(), routing.allocationId().getRelocationId());
                            // cancel relocation and start relocation to same node again
                            // 将之前的source分片恢复成start状态
                            ShardRouting startedReplica = cancelRelocation(sourceShard);
                            // 将target分片移除
                            remove(routing);
                            routingChangesObserver.shardFailed(routing,
                                new UnassignedInfo(UnassignedInfo.Reason.REINITIALIZED, "primary changed"));
                            // 重新开启一个relocate任务  不过这时 peerSource 已经变成了最新的主分片位置了
                            relocateShard(startedReplica, sourceShard.relocatingNodeId(),
                                sourceShard.getExpectedShardSize(), routingChangesObserver);
                        } else {
                            // 普通的副本 重新生成分配id
                            ShardRouting reinitializedReplica = reinitReplica(routing);
                            routingChangesObserver.initializedReplicaReinitialized(routing, reinitializedReplica);
                        }
                    }
                }
            }
        }
        return startedShard;
    }

    /**
     * Applies the relevant logic to handle a cancelled or failed shard.
     *
     * Moves the shard to unassigned or completely removes the shard (if relocation target).
     *
     * - If shard is a primary, this also fails initializing replicas.
     * - If shard is an active primary, this also promotes an active replica to primary (if such a replica exists).
     * - If shard is a relocating primary, this also removes the primary relocation target shard.
     * - If shard is a relocating replica, this promotes the replica relocation target to a full initializing replica, removing the
     *   relocation source information. This is possible as peer recovery is always done from the primary.
     * - If shard is a (primary or replica) relocation target, this also clears the relocation information on the source shard.
     * @param failedShard     本次要被标记成失败的分片
     * @param unassignedInfo   描述分配的相关信息
     * @param indexMetadata   索引元数据
     * @param routingChangesObserver  记录了该分片在某次allocate过程中发生的变化
     *                                 某个分片在分配到某个node后 处理失败了  比如写入数据失败 又或者在恢复数据阶段失败
     */
    public void failShard(Logger logger, ShardRouting failedShard, UnassignedInfo unassignedInfo, IndexMetadata indexMetadata,
                          RoutingChangesObserver routingChangesObserver) {
        ensureMutable();
        assert failedShard.assignedToNode() : "only assigned shards can be failed";
        assert indexMetadata.getIndex().equals(failedShard.index()) :
            "shard failed for unknown index (shard entry: " + failedShard + ")";
        assert getByAllocationId(failedShard.shardId(), failedShard.allocationId().getId()) == failedShard :
            "shard routing to fail does not exist in routing table, expected: " + failedShard + " but was: " +
                getByAllocationId(failedShard.shardId(), failedShard.allocationId().getId());

        logger.debug("{} failing shard {} with unassigned info ({})", failedShard.shardId(), failedShard, unassignedInfo.shortSummary());

        // if this is a primary, fail initializing replicas first (otherwise we move RoutingNodes into an inconsistent state)
        if (failedShard.primary()) {
            List<ShardRouting> assignedShards = assignedShards(failedShard.shardId());
            if (assignedShards.isEmpty() == false) {
                // copy list to prevent ConcurrentModificationException
                // 找到所有处于init状态的副本  已经在写入数据 start/relocate 的副本不处理
                // 推测是将位置空出来 方便新的主分片分配
                for (ShardRouting routing : new ArrayList<>(assignedShards)) {
                    if (!routing.primary() && routing.initializing()) {
                        // re-resolve replica as earlier iteration could have changed source/target of replica relocation
                        // 定位到这个路由对象
                        ShardRouting replicaShard = getByAllocationId(routing.shardId(), routing.allocationId().getId());
                        assert replicaShard != null : "failed to re-resolve " + routing + " when failing replicas";

                        UnassignedInfo primaryFailedUnassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.PRIMARY_FAILED,
                            "primary failed while replica initializing", null, 0, unassignedInfo.getUnassignedTimeInNanos(),
                            unassignedInfo.getUnassignedTimeInMillis(), false, AllocationStatus.NO_ATTEMPT, Collections.emptySet());
                        // 将该分片无效化
                        failShard(logger, replicaShard, primaryFailedUnassignedInfo, indexMetadata, routingChangesObserver);
                    }
                }
            }
        }

        // 当前失败的分片有可能处于重分配状态
        if (failedShard.relocating()) {
            // find the shard that is initializing on the target node
            ShardRouting targetShard = getByAllocationId(failedShard.shardId(), failedShard.allocationId().getRelocationId());
            assert targetShard.isRelocationTargetOf(failedShard);
            // 如果失败的是主分片 重定向也无法正常执行了
            if (failedShard.primary()) {
                logger.trace("{} is removed due to the failure/cancellation of the source shard", targetShard);
                // cancel and remove target shard
                // 因为主分片分配失败了 所以本次 relocate分片也不需要
                remove(targetShard);
                routingChangesObserver.shardFailed(targetShard, unassignedInfo);
            } else {
                logger.trace("{}, relocation source failed / cancelled, mark as initializing without relocation source", targetShard);
                // promote to initializing shard without relocation source and ensure that removed relocation source
                // is not added back as unassigned shard
                // 代表是某个副本 由于relocate 产生的init分片  直接当作一个普通的 init分片就可以
                removeRelocationSource(targetShard);
                // 目前是 NOOP
                routingChangesObserver.relocationSourceRemoved(targetShard);
            }
        }

        // fail actual shard
        // 如果当前分片处于初始状态  那么就是在recovery阶段出现异常
        if (failedShard.initializing()) {

            // 该分片不是由于relocation 产生的
            if (failedShard.relocatingNodeId() == null) {
                if (failedShard.primary()) {
                    // promote active replica to primary if active replica exists (only the case for shadow replicas)
                    // 当主分片处于init状态失败时 尝试寻找其他处于活跃状态的副本 并进行升级
                    // 比如之前主分片是正常启动的 同时有一组replicate与它保持同步状态  都在inSync中 那么最好是选择某个副本进行升级
                    unassignPrimaryAndPromoteActiveReplicaIfExists(failedShard, unassignedInfo, routingChangesObserver);
                } else {
                    // initializing shard that is not relocation target, just move to unassigned
                    // 普通副本只是转换成 unassigned 等待重新分配
                    moveToUnassigned(failedShard, unassignedInfo);
                }
                // 针对重分配后的节点不可用的情况  这种处理比较简单 不需要转换成unassigned状态 只要继续使用原分片就可以（取消重分配）
            } else {
                // The shard is a target of a relocating shard. In that case we only need to remove the target shard and cancel the source
                // relocation. No shard is left unassigned
                logger.trace("{} is a relocation target, resolving source to cancel relocation ({})", failedShard,
                    unassignedInfo.shortSummary());
                // 找到源分片
                ShardRouting sourceShard = getByAllocationId(failedShard.shardId(),
                    failedShard.allocationId().getRelocationId());
                assert sourceShard.isRelocationSourceOf(failedShard);
                logger.trace("{}, resolved source to [{}]. canceling relocation ... ({})", failedShard.shardId(), sourceShard,
                    unassignedInfo.shortSummary());
                // 回复成普通分片
                cancelRelocation(sourceShard);
                // 将target分片移除
                remove(failedShard);
            }
        } else {
            // 分片属于 start/relocate
            assert failedShard.active();
            if (failedShard.primary()) {
                // promote active replica to primary if active replica exists
                // 重新分配原主分片 同时将某个副本升级成主分片  因为active副本已经在 inSync中 所以升级不会影响数据
                unassignPrimaryAndPromoteActiveReplicaIfExists(failedShard, unassignedInfo, routingChangesObserver);
            } else {
                // 重定向的副本直接移除就可以 target会继续从主分片拉取数据并最终加入inSync队列
                if (failedShard.relocating()) {
                    remove(failedShard);
                } else {
                    // 正常处于start状态的副本 需要变回unassigned 并等待重新分配
                    moveToUnassigned(failedShard, unassignedInfo);
                }
            }
        }
        routingChangesObserver.shardFailed(failedShard, unassignedInfo);
        assert node(failedShard.currentNodeId()).getByShardId(failedShard.shardId()) == null : "failedShard " + failedShard +
            " was matched but wasn't removed";
    }

    /**
     * 将某个副本提升成primary
     * @param failedShard  本次失败的某个主分片
     * @param unassignedInfo    相关的分配失败信息
     * @param routingChangesObserver
     */
    private void unassignPrimaryAndPromoteActiveReplicaIfExists(ShardRouting failedShard, UnassignedInfo unassignedInfo,
                                                                RoutingChangesObserver routingChangesObserver) {
        assert failedShard.primary();

        // 原本某个主分片处理失败 需要重新分配时 如果此时有活跃的副本 选择将主分片降级  副本升级 这样可以继续写入新数据 否则就要等待主分片分配完成后index才可用

        // 找到目前存活的最高版本的副本
        ShardRouting activeReplica = activeReplicaWithHighestVersion(failedShard.shardId());
        // 此时没有活跃的副本 代表集群之前都没有可用的分片 无法进行升级 只能尝试更换主分片的节点 如果一个副本都没有实际上主分片的失败就代表着数据的丢失
        // 而本身高可用框架就应该保证至少有某些副本与主分片的数据是同步的 这时候只要将副本升级 同时旧的primary降级就可以了 (还伴随着这个失败分片的重新分配)
        if (activeReplica == null) {
            // 将当前分片变成 unassigned  等待之后重新分配
            moveToUnassigned(failedShard, unassignedInfo);
        } else {
            // 代表集群之前 已经有部分副本与主分片保持同步 在inSync容器中 而此时主分片fail后 首先应该尝试将某个副本升级
            movePrimaryToUnassignedAndDemoteToReplica(failedShard, unassignedInfo);
            // 将副本升级成主分片
            promoteReplicaToPrimary(activeReplica, routingChangesObserver);
        }
    }

    /**
     * 将某个副本升级成 主分片
     * @param activeReplica
     * @param routingChangesObserver
     */
    private void promoteReplicaToPrimary(ShardRouting activeReplica, RoutingChangesObserver routingChangesObserver) {
        // if the activeReplica was relocating before this call to failShard, its relocation was cancelled earlier when we
        // failed initializing replica shards (and moved replica relocation source back to started)
        assert activeReplica.started() : "replica relocation should have been cancelled: " + activeReplica;
        promoteActiveReplicaShardToPrimary(activeReplica);
        // 触发观察者对象
        routingChangesObserver.replicaPromoted(activeReplica);
    }

    /**
     * Mark a shard as started and adjusts internal statistics.
     *
     * @return the started shard
     * 将某个分片标记成启动状态
     */
    private ShardRouting started(ShardRouting shard) {
        assert shard.initializing() : "expected an initializing shard " + shard;
        // 初始状态的shard 有2种 一种是真的还未启动的分片 一种是之前启动 但是进入了relocating状态 这时会在target节点上创建一个 init的分片 它的relocatingNodeId 指向source节点
        // 这里代表分片不是由于重分配生成的
        if (shard.relocatingNodeId() == null) {
            // if this is not a target shard for relocation, we need to update statistics
            // 因为某个分片的启动  inactive的数量减少了
            inactiveShardCount--;
            if (shard.primary()) {
                inactivePrimaryCount--;
            }
        }
        // 切换成start的分片必然完成了数据恢复   初始化该对象时使用的是之前的ClusterState 然后本次针对分片状态的修改会直接体现在ClusterState上
        removeRecovery(shard);
        ShardRouting startedShard = shard.moveToStarted();
        updateAssigned(shard, startedShard);
        return startedShard;
    }



    /**
     * Cancels a relocation of a shard that shard must relocating.
     *
     * @return the shard after cancelling relocation
     * 取消某个relocation的分片
     */
    private ShardRouting cancelRelocation(ShardRouting shard) {
        relocatingShards--;
        // 分片又变回了 start状态 并且更新assigned容器
        ShardRouting cancelledShard = shard.cancelRelocation();
        updateAssigned(shard, cancelledShard);
        return cancelledShard;
    }

    /**
     * moves the assigned replica shard to primary.
     *
     * @param replicaShard the replica shard to be promoted to primary
     * @return             the resulting primary shard
     * 将某个副本升级成主分片
     */
    private ShardRouting promoteActiveReplicaShardToPrimary(ShardRouting replicaShard) {
        assert replicaShard.active() : "non-active shard cannot be promoted to primary: " + replicaShard;
        assert replicaShard.primary() == false : "primary shard cannot be promoted to primary: " + replicaShard;
        // 将副本更新成主分片后 更新assigned
        ShardRouting primaryShard = replicaShard.moveActiveReplicaToPrimary();
        updateAssigned(replicaShard, primaryShard);
        return primaryShard;
    }

    private static final List<ShardRouting> EMPTY = Collections.emptyList();

    /**
     * Cancels the give shard from the Routing nodes internal statistics and cancels
     * the relocation if the shard is relocating.
     * 将某个分片从当前节点下移除
     */
    private void remove(ShardRouting shard) {
        assert shard.unassigned() == false : "only assigned shards can be removed here (" + shard + ")";
        node(shard.currentNodeId()).remove(shard);
        if (shard.initializing() && shard.relocatingNodeId() == null) {
            inactiveShardCount--;
            assert inactiveShardCount >= 0;
            if (shard.primary()) {
                inactivePrimaryCount--;
            }
            // 如果移除的是某个relocating分片
        } else if (shard.relocating()) {
            // 将分片修改成start后 覆盖旧分片
            shard = cancelRelocation(shard);
        }
        // 移除该分片
        assignedShardsRemove(shard);
        // 如果该分片处于 init状态 那么它之前一定被加入到了recovery容器中 现在不需要对它进行数据恢复了
        if (shard.initializing()) {
            removeRecovery(shard);
        }
    }

    /**
     * Removes relocation source of an initializing non-primary shard. This allows the replica shard to continue recovery from
     * the primary even though its non-primary relocation source has failed.
     * 将重分配的target分片 的源分片信息清除  作为一个普通的init
     */
    private ShardRouting removeRelocationSource(ShardRouting shard) {
        assert shard.isRelocationTarget() : "only relocation target shards can have their relocation source removed (" + shard + ")";
        ShardRouting relocationMarkerRemoved = shard.removeRelocationSource();
        updateAssigned(shard, relocationMarkerRemoved);
        inactiveShardCount++; // relocation targets are not counted as inactive shards whereas initializing shards are
        return relocationMarkerRemoved;
    }

    /**
     * 添加一个已分配的分片
     * @param shard
     */
    private void assignedShardsAdd(ShardRouting shard) {
        assert shard.unassigned() == false : "unassigned shard " + shard + " cannot be added to list of assigned shards";
        List<ShardRouting> shards = assignedShards.computeIfAbsent(shard.shardId(), k -> new ArrayList<>());
        assert assertInstanceNotInList(shard, shards) : "shard " + shard + " cannot appear twice in list of assigned shards";
        shards.add(shard);
    }

    private boolean assertInstanceNotInList(ShardRouting shard, List<ShardRouting> shards) {
        for (ShardRouting s : shards) {
            assert s != shard;
        }
        return true;
    }

    private void assignedShardsRemove(ShardRouting shard) {
        final List<ShardRouting> replicaSet = assignedShards.get(shard.shardId());
        if (replicaSet != null) {
            final Iterator<ShardRouting> iterator = replicaSet.iterator();
            while(iterator.hasNext()) {
                // yes we check identity here
                if (shard == iterator.next()) {
                    iterator.remove();
                    return;
                }
            }
        }
        assert false : "No shard found to remove";
    }

    /**
     * 当主分片完成relocate时 副本需要更新 peer的目标节点 好从最新的分片上拉取数据
     * @param shard  待更新的副本分片
     * @return
     */
    private ShardRouting reinitReplica(ShardRouting shard) {
        assert shard.primary() == false : "shard must be a replica: " + shard;
        assert shard.initializing() : "can only reinitialize an initializing replica: " + shard;
        assert shard.isRelocationTarget() == false : "replication target cannot be reinitialized: " + shard;
        // 主要就是重新生成了一个 分配id  在IndicesClusterStateService中 当发现某个分片的allocateId发生变化 会重启一次indexShard 这样就可以从最新的primary上拉取数据了
        ShardRouting reinitializedShard = shard.reinitializeReplicaShard();
        updateAssigned(shard, reinitializedShard);
        return reinitializedShard;
    }

    /**
     * 更新某个已经完成分配的分片
     * @param oldShard
     * @param newShard
     */
    private void updateAssigned(ShardRouting oldShard, ShardRouting newShard) {
        assert oldShard.shardId().equals(newShard.shardId()) :
            "can only update " + oldShard + " by shard with same shard id but was " + newShard;
        assert oldShard.unassigned() == false && newShard.unassigned() == false :
            "only assigned shards can be updated in list of assigned shards (prev: " + oldShard + ", new: " + newShard + ")";
        assert oldShard.currentNodeId().equals(newShard.currentNodeId()) : "shard to update " + oldShard +
            " can only update " + oldShard + " by shard assigned to same node but was " + newShard;
        // 将信息更新到 RoutingNode中
        node(oldShard.currentNodeId()).update(oldShard, newShard);

        // 替换assignedShards的分片
        List<ShardRouting> shardsWithMatchingShardId = assignedShards.computeIfAbsent(oldShard.shardId(), k -> new ArrayList<>());
        int previousShardIndex = shardsWithMatchingShardId.indexOf(oldShard);
        assert previousShardIndex >= 0 : "shard to update " + oldShard + " does not exist in list of assigned shards";
        shardsWithMatchingShardId.set(previousShardIndex, newShard);
    }

    /**
     * 将某个失败的分片 从assigned移动到 unassigned
     * @param shard
     * @param unassignedInfo
     * @return
     */
    private ShardRouting moveToUnassigned(ShardRouting shard, UnassignedInfo unassignedInfo) {
        assert shard.unassigned() == false : "only assigned shards can be moved to unassigned (" + shard + ")";
        remove(shard);
        ShardRouting unassigned = shard.moveToUnassigned(unassignedInfo);
        unassignedShards.add(unassigned);
        return unassigned;
    }

    /**
     * Moves assigned primary to unassigned and demotes it to a replica.
     * Used in conjunction with {@link #promoteActiveReplicaShardToPrimary} when an active replica is promoted to primary.
     * 将某个分配失败的主分片变成 unassigned
     */
    private ShardRouting movePrimaryToUnassignedAndDemoteToReplica(ShardRouting shard, UnassignedInfo unassignedInfo) {
        assert shard.unassigned() == false : "only assigned shards can be moved to unassigned (" + shard + ")";
        assert shard.primary() : "only primary can be demoted to replica (" + shard + ")";
        remove(shard);
        // 将主分片降级成了副本
        ShardRouting unassigned = shard.moveToUnassigned(unassignedInfo).moveUnassignedFromPrimary();
        unassignedShards.add(unassigned);
        return unassigned;
    }

    /**
     * Returns the number of routing nodes
     */
    public int size() {
        return nodesToShards.size();
    }


    /**
     * 该对象负责管理此时所有还未分配的 replicate primary
     */
    public static final class UnassignedShards implements Iterable<ShardRouting>  {

        /**
         * 本次选取的node范围
         */
        private final RoutingNodes nodes;
        /**
         * 未分配的分片列表
         */
        private final List<ShardRouting> unassigned;

        /**
         * 某些分片处于 unassigned 与 assigned 的中间状态  比如正在准备生成分配结果的数据  会暂存在该容器中
         */
        private final List<ShardRouting> ignored;

        /**
         * 这些未分配的分片中有多少分片被标记成 primary
         */
        private int primaries = 0;

        /**
         * 处于 unassigned 与 assigned 中间状态的分片是主分片
         */
        private int ignoredPrimaries = 0;

        public UnassignedShards(RoutingNodes nodes) {
            this.nodes = nodes;
            unassigned = new ArrayList<>();
            ignored = new ArrayList<>();
        }

        public void add(ShardRouting shardRouting) {
            if(shardRouting.primary()) {
                primaries++;
            }
            unassigned.add(shardRouting);
        }

        public void sort(Comparator<ShardRouting> comparator) {
            // 确保此对象是允许修改的
            nodes.ensureMutable();
            CollectionUtil.timSort(unassigned, comparator);
        }

        /**
         * Returns the size of the non-ignored unassigned shards
         */
        public int size() { return unassigned.size(); }

        /**
         * Returns the number of non-ignored unassigned primaries
         */
        public int getNumPrimaries() {
            return primaries;
        }

        /**
         * Returns the number of temporarily marked as ignored unassigned primaries
         */
        public int getNumIgnoredPrimaries() { return ignoredPrimaries; }

        @Override
        public UnassignedIterator iterator() {
            return new UnassignedIterator();
        }

        /**
         * The list of ignored unassigned shards (read only). The ignored unassigned shards
         * are not part of the formal unassigned list, but are kept around and used to build
         * back the list of unassigned shards as part of the routing table.
         */
        public List<ShardRouting> ignored() {
            return Collections.unmodifiableList(ignored);
        }

        /**
         * Marks a shard as temporarily ignored and adds it to the ignore unassigned list.
         * Should be used with caution, typically,
         * the correct usage is to removeAndIgnore from the iterator.
         * @see #ignored()
         * @see UnassignedIterator#removeAndIgnore(AllocationStatus, RoutingChangesObserver)
         * @see #isIgnoredEmpty()
         * 比如某个未分配的分片 此时正在获取该分片在所有node上的元数据信息  无法立即生成分配结果  为了避免重复处理 先设置成忽略
         */
        public void ignoreShard(ShardRouting shard, AllocationStatus allocationStatus, RoutingChangesObserver changes) {
            nodes.ensureMutable();
            if (shard.primary()) {
                ignoredPrimaries++;
                UnassignedInfo currInfo = shard.unassignedInfo();
                assert currInfo != null;

                // 更新当前 unassignedInfo的状态
                if (allocationStatus.equals(currInfo.getLastAllocationStatus()) == false) {
                    UnassignedInfo newInfo = new UnassignedInfo(currInfo.getReason(), currInfo.getMessage(), currInfo.getFailure(),
                                                                currInfo.getNumFailedAllocations(), currInfo.getUnassignedTimeInNanos(),
                                                                currInfo.getUnassignedTimeInMillis(), currInfo.isDelayed(),
                                                                allocationStatus, currInfo.getFailedNodeIds());
                    // 更新分片此时的 unassignedInfo
                    ShardRouting updatedShard = shard.updateUnassigned(newInfo, shard.recoverySource());
                    // 触发观察者钩子
                    changes.unassignedInfoUpdated(shard, newInfo);
                    shard = updatedShard;
                }
            }
            ignored.add(shard);
        }

        /**
         * 该对象维护了所有未分配的分配  同时对外暴露一些处理用的api (unassignedAllocationHandler)
         */
        public class UnassignedIterator implements Iterator<ShardRouting>, ExistingShardsAllocator.UnassignedAllocationHandler {

            private final ListIterator<ShardRouting> iterator;
            private ShardRouting current;

            public UnassignedIterator() {
                this.iterator = unassigned.listIterator();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public ShardRouting next() {
                return current = iterator.next();
            }

            /**
             * Initializes the current unassigned shard and moves it from the unassigned list.
             *
             * @param nodeId 本次要分配的目标节点
             * @param existingAllocationId allocation id to use. If null, a fresh allocation id is generated.  每个shard分配到某个node上 就会有一个对应的allocationId
             *                             某个unassigned的分片变成了init状态
             */
            @Override
            public ShardRouting initialize(String nodeId, @Nullable String existingAllocationId, long expectedShardSize,
                                           RoutingChangesObserver routingChangesObserver) {
                nodes.ensureMutable();
                // 从迭代器中移除  这个操作也会间接影响到 unassigned
                innerRemove();
                // 切换到init状态
                return nodes.initializeShard(current, nodeId, existingAllocationId, expectedShardSize, routingChangesObserver);
            }

            /**
             * Removes and ignores the unassigned shard (will be ignored for this run, but
             * will be added back to unassigned once the metadata is constructed again).
             * Typically this is used when an allocation decision prevents a shard from being allocated such
             * that subsequent consumers of this API won't try to allocate this shard again.
             *
             * @param attempt the result of the allocation attempt
             * @param changes 观测被移除的分片
             *                某个 unassigned分片已经被处理了  不需要继续维护
             */
            @Override
            public void removeAndIgnore(AllocationStatus attempt, RoutingChangesObserver changes) {
                nodes.ensureMutable();
                // 迭代器操作 这里会间接从unassigned中移除current
                innerRemove();
                ignoreShard(current, attempt, changes);
            }

            /**
             * 替换迭代器此时读取的对象
             * @param shardRouting
             */
            private void updateShardRouting(ShardRouting shardRouting) {
                current = shardRouting;
                iterator.set(shardRouting);
            }

            /**
             * updates the unassigned info and recovery source on the current unassigned shard
             *
             * @param  unassignedInfo the new unassigned info to use
             * @param  recoverySource the new recovery source to use
             * @return the shard with unassigned info updated
             * 更新当前节点的 unassigned信息
             */
            @Override
            public ShardRouting updateUnassigned(UnassignedInfo unassignedInfo, RecoverySource recoverySource,
                                                 RoutingChangesObserver changes) {
                nodes.ensureMutable();
                // 更新 unassignedInfo
                ShardRouting updatedShardRouting = current.updateUnassigned(unassignedInfo, recoverySource);
                changes.unassignedInfoUpdated(current, unassignedInfo);
                updateShardRouting(updatedShardRouting);
                return updatedShardRouting;
            }

            /**
             * Unsupported operation, just there for the interface. Use
             * {@link #removeAndIgnore(AllocationStatus, RoutingChangesObserver)} or
             * {@link #initialize(String, String, long, RoutingChangesObserver)}.
             */
            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported in unassigned iterator," +
                    " use removeAndIgnore or initialize");
            }

            private void innerRemove() {
                iterator.remove();
                if (current.primary()) {
                    primaries--;
                }
            }
        }

        /**
         * Returns <code>true</code> iff this collection contains one or more non-ignored unassigned shards.
         */
        public boolean isEmpty() {
            return unassigned.isEmpty();
        }

        /**
         * Returns <code>true</code> iff any unassigned shards are marked as temporarily ignored.
         * @see UnassignedShards#ignoreShard(ShardRouting, AllocationStatus, RoutingChangesObserver)
         * @see UnassignedIterator#removeAndIgnore(AllocationStatus, RoutingChangesObserver)
         */
        public boolean isIgnoredEmpty() {
            return ignored.isEmpty();
        }

        /**
         * 将顺序打乱
         */
        public void shuffle() {
            nodes.ensureMutable();
            Randomness.shuffle(unassigned);
        }

        /**
         * Drains all unassigned shards and returns it.
         * This method will not drain ignored shards.
         * 将内部所有未分配分片清空同时返回
         */
        public ShardRouting[] drain() {
            nodes.ensureMutable();
            ShardRouting[] mutableShardRoutings = unassigned.toArray(new ShardRouting[unassigned.size()]);
            unassigned.clear();
            primaries = 0;
            return mutableShardRoutings;
        }
    }


    /**
     * Calculates RoutingNodes statistics by iterating over all {@link ShardRouting}s
     * in the cluster to ensure the book-keeping is correct.
     * For performance reasons, this should only be called from asserts
     *
     * @return this method always returns <code>true</code> or throws an assertion error. If assertion are not enabled
     *         this method does nothing.
     */
    public static boolean assertShardStats(RoutingNodes routingNodes) {
        if (!Assertions.ENABLED) {
            return true;
        }
        int unassignedPrimaryCount = 0;
        int unassignedIgnoredPrimaryCount = 0;
        int inactivePrimaryCount = 0;
        int inactiveShardCount = 0;
        int relocating = 0;
        Map<Index, Integer> indicesAndShards = new HashMap<>();
        for (RoutingNode node : routingNodes) {
            for (ShardRouting shard : node) {
                if (shard.initializing() && shard.relocatingNodeId() == null) {
                    inactiveShardCount++;
                    if (shard.primary()) {
                        inactivePrimaryCount++;
                    }
                }
                if (shard.relocating()) {
                    relocating++;
                }
                Integer i = indicesAndShards.get(shard.index());
                if (i == null) {
                    i = shard.id();
                }
                indicesAndShards.put(shard.index(), Math.max(i, shard.id()));
            }
        }

        // Assert that the active shard routing are identical.
        Set<Map.Entry<Index, Integer>> entries = indicesAndShards.entrySet();

        final Map<ShardId, HashSet<ShardRouting>> shardsByShardId = new HashMap<>();
        for (final RoutingNode routingNode: routingNodes) {
            for (final ShardRouting shardRouting : routingNode) {
                final HashSet<ShardRouting> shards =
                        shardsByShardId.computeIfAbsent(new ShardId(shardRouting.index(), shardRouting.id()), k -> new HashSet<>());
                shards.add(shardRouting);
            }
        }

        for (final Map.Entry<Index, Integer> e : entries) {
            final Index index = e.getKey();
            for (int i = 0; i < e.getValue(); i++) {
                final ShardId shardId = new ShardId(index, i);
                final HashSet<ShardRouting> shards = shardsByShardId.get(shardId);
                final List<ShardRouting> mutableShardRoutings = routingNodes.assignedShards(shardId);
                assert (shards == null && mutableShardRoutings.size() == 0)
                        || (shards != null && shards.size() == mutableShardRoutings.size() && shards.containsAll(mutableShardRoutings));
            }
        }

        for (ShardRouting shard : routingNodes.unassigned()) {
            if (shard.primary()) {
                unassignedPrimaryCount++;
            }
        }

        for (ShardRouting shard : routingNodes.unassigned().ignored()) {
            if (shard.primary()) {
                unassignedIgnoredPrimaryCount++;
            }
        }

        for (Map.Entry<String, Recoveries> recoveries : routingNodes.recoveriesPerNode.entrySet()) {
            String node = recoveries.getKey();
            final Recoveries value = recoveries.getValue();
            int incoming = 0;
            int outgoing = 0;
            RoutingNode routingNode = routingNodes.nodesToShards.get(node);
            if (routingNode != null) { // node might have dropped out of the cluster
                for (ShardRouting routing : routingNode) {
                    if (routing.initializing()) {
                        incoming++;
                    }
                    if (routing.primary() && routing.isRelocationTarget() == false) {
                        for (ShardRouting assigned : routingNodes.assignedShards.get(routing.shardId())) {
                            if (assigned.initializing() && assigned.recoverySource().getType() == RecoverySource.Type.PEER) {
                                outgoing++;
                            }
                        }
                    }
                }
            }
            assert incoming == value.incoming : incoming + " != " + value.incoming + " node: " + routingNode;
            assert outgoing == value.outgoing : outgoing + " != " + value.outgoing + " node: " + routingNode;
        }


        assert unassignedPrimaryCount == routingNodes.unassignedShards.getNumPrimaries() :
                "Unassigned primaries is [" + unassignedPrimaryCount + "] but RoutingNodes returned unassigned primaries [" +
                    routingNodes.unassigned().getNumPrimaries() + "]";
        assert unassignedIgnoredPrimaryCount == routingNodes.unassignedShards.getNumIgnoredPrimaries() :
                "Unassigned ignored primaries is [" + unassignedIgnoredPrimaryCount +
                    "] but RoutingNodes returned unassigned ignored primaries [" + routingNodes.unassigned().getNumIgnoredPrimaries() + "]";
        assert inactivePrimaryCount == routingNodes.inactivePrimaryCount :
                "Inactive Primary count [" + inactivePrimaryCount + "] but RoutingNodes returned inactive primaries [" +
                    routingNodes.inactivePrimaryCount + "]";
        assert inactiveShardCount == routingNodes.inactiveShardCount :
                "Inactive Shard count [" + inactiveShardCount + "] but RoutingNodes returned inactive shards [" +
                    routingNodes.inactiveShardCount + "]";
        assert routingNodes.getRelocatingShardCount() == relocating : "Relocating shards mismatch [" +
            routingNodes.getRelocatingShardCount() + "] but expected [" + relocating + "]";

        return true;
    }

    private void ensureMutable() {
        if (readOnly) {
            throw new IllegalStateException("can't modify RoutingNodes - readonly");
        }
    }

    /**
     * Creates an iterator over shards interleaving between nodes: The iterator returns the first shard from
     * the first node, then the first shard of the second node, etc. until one shard from each node has been returned.
     * The iterator then resumes on the first node by returning the second shard and continues until all shards from
     * all the nodes have been returned.
     * 将所有已经分配完成的节点包装成迭代器
     */
    public Iterator<ShardRouting> nodeInterleavedShardIterator() {
        final Queue<Iterator<ShardRouting>> queue = new ArrayDeque<>();
        for (Map.Entry<String, RoutingNode> entry : nodesToShards.entrySet()) {
            queue.add(entry.getValue().copyShards().iterator());
        }
        return new Iterator<ShardRouting>() {
            public boolean hasNext() {
                while (!queue.isEmpty()) {
                    if (queue.peek().hasNext()) {
                        return true;
                    }
                    queue.poll();
                }
                return false;
            }

            public ShardRouting next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                Iterator<ShardRouting> iter = queue.poll();
                ShardRouting result = iter.next();
                queue.offer(iter);
                return result;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * 以node为界限  记录每个节点会有多少分片需要输入数据  有多少分片需要向外输出数据
     * 比如1主8副 那么主分片所在的节点 outgoing就是8 其他节点incoming 都是1
     */
    private static final class Recoveries {
        private static final Recoveries EMPTY = new Recoveries();
        /**
         * 需要恢复数据的分片数量
         */
        private int incoming = 0;
        /**
         * 此时往外输出数据
         */
        private int outgoing = 0;

        void addOutgoing(int howMany) {
            assert outgoing + howMany >= 0 : outgoing + howMany+ " must be >= 0";
            outgoing += howMany;
        }

        void addIncoming(int howMany) {
            assert incoming + howMany >= 0 : incoming + howMany+ " must be >= 0";
            incoming += howMany;
        }

        int getOutgoing() {
            return outgoing;
        }

        int getIncoming() {
            return incoming;
        }

        /**
         * 获取map中通过key 映射到的补偿对象 如果没有则追加映射关系
         * @param map
         * @param key
         * @return
         */
        public static Recoveries getOrAdd(Map<String, Recoveries> map, String key) {
            Recoveries recoveries = map.get(key);
            if (recoveries == null) {
                recoveries = new Recoveries();
                map.put(key, recoveries);
            }
            return recoveries;
        }
     }
}
