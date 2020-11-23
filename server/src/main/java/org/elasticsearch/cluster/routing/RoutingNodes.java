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

    /**
     * 这个容器中仅存储已经分配完毕的分片
     */
    private final Map<String, RoutingNode> nodesToShards = new HashMap<>();

    /**
     * 该对象管理了当前集群所有节点中未分配的分片
     */
    private final UnassignedShards unassignedShards = new UnassignedShards(this);

    /**
     * 记录所有已经找到归属节点的副本  key则是这些副本的分片id  (在ES的架构中最小的数据单位就是副本)
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
     */
    private int inactiveShardCount = 0;

    /**
     * 记录此时正在移动中的分片数量
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
            // 遍历每个分片 再下一级则是副本
            for (IndexShardRoutingTable indexShard : indexRoutingTable.value) {
                assert indexShard.primary != null;
                // 遍历每个副本
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

                        // 根据该分片处于初始状态 或者是重分配状态做不同处理
                        if (shard.relocating()) {
                            relocatingShards++;
                            // LinkedHashMap to preserve order.
                            // Add the counterpart shard with relocatingNodeId reflecting the source from which
                            // it's relocating from.
                            // 在relocate的节点上 又增加了一份关联关系  相当于此时可以认为该副本同时在2个节点上生效  应该是起到保护作用吧
                            entries = nodesToShards.computeIfAbsent(shard.relocatingNodeId(),
                                k -> new LinkedHashMap<>());
                            // 当分片处于重分配状态时 会在内部存储一个 targetRelocating  对应重分配后的位置
                            ShardRouting targetShardRouting = shard.getTargetRelocatingShard();
                            // 这里添加一个需要恢复数据的副本对象  因为副本是需要与主分片同步数据的    TODO 如果主分片挂了 副本会晋升吗???
                            addInitialRecovery(targetShardRouting, indexShard.primary);
                            previousValue = entries.put(targetShardRouting.shardId(), targetShardRouting);
                            if (previousValue != null) {
                                throw new IllegalArgumentException("Cannot have two different shards with same shard id on same node");
                            }
                            // 重分配后的节点也加入到 assigned中
                            assignedShardsAdd(targetShardRouting);
                        } else if (shard.initializing()) {
                            if (shard.primary()) {
                                inactivePrimaryCount++;
                            }
                            inactiveShardCount++;
                            // 初始状态的分片也是需要恢复数据的
                            addInitialRecovery(shard, indexShard.primary);
                        }
                        // TODO 处于started状态的副本不做处理
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

    private void addRecovery(ShardRouting routing) {
        updateRecoveryCounts(routing, true, findAssignedPrimaryIfPeerRecovery(routing));
    }

    private void removeRecovery(ShardRouting routing) {
        updateRecoveryCounts(routing, false, findAssignedPrimaryIfPeerRecovery(routing));
    }

    /**
     * 代表目标副本进入了数据恢复的初始阶段
     * @param routing  本次需要同步数据的副本
     * @param initialPrimaryShard  对应的主分片
     */
    private void addInitialRecovery(ShardRouting routing, ShardRouting initialPrimaryShard) {
        updateRecoveryCounts(routing, true, initialPrimaryShard);
    }

    /**
     * 更新需要恢复数据的副本数量
     * @param routing  待处理的分片
     * @param increment  新增需要恢复的副本/又完成了一个需要恢复的副本
     * @param primary   与 routing相同shardId的 某个私有分片 在IndexShardRoutingTable中可以看到 每个shardId 对应的所有分片中都有一个是私有分片
     */
    private void updateRecoveryCounts(final ShardRouting routing, final boolean increment, @Nullable final ShardRouting primary) {
        final int howMany = increment ? 1 : -1;
        assert routing.initializing() : "routing must be initializing: " + routing;
        // TODO: check primary == null || primary.active() after all tests properly add ReplicaAfterPrimaryActiveAllocationDecider
        assert primary == null || primary.assignedToNode() :
            "shard is initializing but its primary is not assigned to a node";

        // 代表某个节点对应的需要恢复数据的副本数发生了变化
        Recoveries.getOrAdd(recoveriesPerNode, routing.currentNodeId()).addIncoming(howMany);

        // 默认情况下 副本的恢复源就是 PEER  其余情况 比如primary就不需要处理了 并且应该是通过本地数据还原
        if (routing.recoverySource().getType() == RecoverySource.Type.PEER) {
            // add/remove corresponding outgoing recovery on node with primary shard
            if (primary == null) {
                throw new IllegalStateException("shard is peer recovering but primary is unassigned");
            }
            // 主分片对应数据输出 也就是outgoing
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

    @Nullable
    private ShardRouting findAssignedPrimaryIfPeerRecovery(ShardRouting routing) {
        ShardRouting primary = null;
        if (routing.recoverySource() != null && routing.recoverySource().getType() == RecoverySource.Type.PEER) {
            List<ShardRouting> shardRoutings = assignedShards.get(routing.shardId());
            if (shardRoutings != null) {
                for (ShardRouting shardRouting : shardRoutings) {
                    if (shardRouting.primary()) {
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
     * 找到某个分片id 对应的一个处于活跃状态的分片对象
     */
    public ShardRouting activeReplicaWithHighestVersion(ShardId shardId) {
        // It's possible for replicaNodeVersion to be null, when disassociating dead nodes
        // that have been removed, the shards are failed, and part of the shard failing
        // calls this method with an out-of-date RoutingNodes, where the version might not
        // be accessible. Therefore, we need to protect against the version being null
        // (meaning the node will be going away).
        return assignedShards(shardId).stream()
                .filter(shr -> !shr.primary() && shr.active())
                .filter(shr -> node(shr.currentNodeId()) != null)
                .max(Comparator.comparing(shr -> node(shr.currentNodeId()).node(),
                                Comparator.nullsFirst(Comparator.comparing(DiscoveryNode::getVersion))))
                .orElse(null);
    }

    /**
     * Returns <code>true</code> iff all replicas are active for the given shard routing. Otherwise <code>false</code>
     */
    public boolean allReplicasActive(ShardId shardId, Metadata metadata) {
        final List<ShardRouting> shards = assignedShards(shardId);
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
     * 将某个未分配的分片修改成初始化状态 同时设置到该对象中
     */
    public ShardRouting initializeShard(ShardRouting unassignedShard, String nodeId, @Nullable String existingAllocationId,
                                        long expectedSize, RoutingChangesObserver routingChangesObserver) {
        ensureMutable();
        assert unassignedShard.unassigned() : "expected an unassigned shard " + unassignedShard;
        ShardRouting initializedShard = unassignedShard.initialize(nodeId, existingAllocationId, expectedSize);
        node(nodeId).add(initializedShard);
        inactiveShardCount++;
        if (initializedShard.primary()) {
            inactivePrimaryCount++;
        }
        addRecovery(initializedShard);
        assignedShardsAdd(initializedShard);
        routingChangesObserver.shardInitialized(unassignedShard, initializedShard);
        return initializedShard;
    }

    /**
     * Relocate a shard to another node, adding the target initializing
     * shard as well as assigning it.
     *
     * @param nodeId  移动到哪个目标节点
     * @return pair of source relocating and target initializing shards.
     * 移动某个分片
     */
    public Tuple<ShardRouting,ShardRouting> relocateShard(ShardRouting startedShard, String nodeId, long expectedShardSize,
                                                          RoutingChangesObserver changes) {
        ensureMutable();
        relocatingShards++;

        // !!! 注意此时source 还没有被移除   source从started 转换成了 relocating状态  同时生成了一个target分片  它处于 init状态

        // 源分片的 currentNodeId, relocationId  与target的正好相反
        ShardRouting source = startedShard.relocate(nodeId, expectedShardSize);
        ShardRouting target = source.getTargetRelocatingShard();

        // 用source 替换 startedShard
        updateAssigned(startedShard, source);
        // 在目标节点上增加一个新的分片
        node(target.currentNodeId()).add(target);
        // 将新分片设置到 assignedShards 中
        assignedShardsAdd(target);
        // TODO
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
     * @return the started shard
     * 将某个处于初始状态的分片转换成启动
     */
    public ShardRouting startShard(Logger logger, ShardRouting initializingShard, RoutingChangesObserver routingChangesObserver) {
        ensureMutable();
        ShardRouting startedShard = started(initializingShard);
        logger.trace("{} marked shard as started (routing: {})", initializingShard.shardId(), initializingShard);

        // 触发观察者钩子
        routingChangesObserver.shardStarted(initializingShard, startedShard);


        // 如果这个分片是由重分配创建的shard 那么它的 relocating 指向的就是source Node
        if (initializingShard.relocatingNodeId() != null) {
            // relocation target has been started, remove relocation source
            RoutingNode relocationSourceNode = node(initializingShard.relocatingNodeId());
            ShardRouting relocationSourceShard = relocationSourceNode.getByShardId(initializingShard.shardId());
            assert relocationSourceShard.isRelocationSourceOf(initializingShard);
            assert relocationSourceShard.getTargetRelocatingShard() == initializingShard : "relocation target mismatch, expected: "
                + initializingShard + " but was: " + relocationSourceShard.getTargetRelocatingShard();

            // 这里移除了relocating的分片 同时修改成start后 又重新加入到了node中
            remove(relocationSourceShard);
            routingChangesObserver.relocationCompleted(relocationSourceShard);

            // if this is a primary shard with ongoing replica recoveries, reinitialize them as their recovery source changed
            // 如果是主分片从 init变成start 要重启所有 副本
            if (startedShard.primary()) {
                // 找到某个shardId 对应的所有分片 包含所有副本
                List<ShardRouting> assignedShards = assignedShards(startedShard.shardId());
                // copy list to prevent ConcurrentModificationException
                for (ShardRouting routing : new ArrayList<>(assignedShards)) {
                    // 找到处于初始状态的所有副本
                    if (routing.initializing() && routing.primary() == false) {
                        // 如果是某次 relocating的目标分片
                        if (routing.isRelocationTarget()) {
                            // find the relocation source
                            // 找到source 分片
                            ShardRouting sourceShard = getByAllocationId(routing.shardId(), routing.allocationId().getRelocationId());
                            // cancel relocation and start relocation to same node again
                            // 以下2步将  某个调用relocation的副本分片还原
                            // 将之前打算重分配的分片 还原成start状态
                            ShardRouting startedReplica = cancelRelocation(sourceShard);
                            // 将这个打算重分配的分片移除
                            remove(routing);
                            routingChangesObserver.shardFailed(routing,
                                new UnassignedInfo(UnassignedInfo.Reason.REINITIALIZED, "primary changed"));
                            // 重新为某个分片重分配
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
     * @param failedShard 分配失败的分片
     * @param unassignedInfo 分配失败的信息
     * @param indexMetadata 本次分配失败的分片对应的索引描述信息
     * 标记某个分片处理失败  TODO 还需要梳理
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
        // 如果分配失败的是一个主分片   那么所有副本要跟着修改
        if (failedShard.primary()) {
            // 先找到这个分片id 下所有已经分配的 分片副本
            List<ShardRouting> assignedShards = assignedShards(failedShard.shardId());
            if (assignedShards.isEmpty() == false) {
                // copy list to prevent ConcurrentModificationException
                for (ShardRouting routing : new ArrayList<>(assignedShards)) {
                    // TODO 为什么只处理初始状态的分片呢 如果副本已经在使用阶段会怎么样
                    if (!routing.primary() && routing.initializing()) {
                        // re-resolve replica as earlier iteration could have changed source/target of replica relocation
                        ShardRouting replicaShard = getByAllocationId(routing.shardId(), routing.allocationId().getId());
                        assert replicaShard != null : "failed to re-resolve " + routing + " when failing replicas";
                        // 因为主分片处理失败 所以这些副本都要更新成失败
                        UnassignedInfo primaryFailedUnassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.PRIMARY_FAILED,
                            "primary failed while replica initializing", null, 0, unassignedInfo.getUnassignedTimeInNanos(),
                            unassignedInfo.getUnassignedTimeInMillis(), false, AllocationStatus.NO_ATTEMPT, Collections.emptySet());
                        failShard(logger, replicaShard, primaryFailedUnassignedInfo, indexMetadata, routingChangesObserver);
                    }
                }
            }
        }

        // 如果这个失败的副本正好处在 重分配的情况
        if (failedShard.relocating()) {
            // find the shard that is initializing on the target node
            // 找到即将分配到target节点的分片对象
            ShardRouting targetShard = getByAllocationId(failedShard.shardId(), failedShard.allocationId().getRelocationId());
            assert targetShard.isRelocationTargetOf(failedShard);
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
                // 将这个分片变成了一个普通的分片  这样不就相当于直接完成relocate了么
                removeRelocationSource(targetShard);
                routingChangesObserver.relocationSourceRemoved(targetShard);
            }
        }

        // fail actual shard
        // 如果这个分片处于初始化状态
        if (failedShard.initializing()) {
            if (failedShard.relocatingNodeId() == null) {
                if (failedShard.primary()) {
                    // promote active replica to primary if active replica exists (only the case for shadow replicas)
                    // 主分片降级成副本  副本升级成主分片
                    unassignPrimaryAndPromoteActiveReplicaIfExists(failedShard, unassignedInfo, routingChangesObserver);
                } else {
                    // initializing shard that is not relocation target, just move to unassigned
                    // 如果只是普通的副本 直接转移到 unassigned
                    moveToUnassigned(failedShard, unassignedInfo);
                }
            // 代表这是一个relocate 中的 target分片
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
            // 如果这个失败的分片此时处于活跃状态
            assert failedShard.active();
            if (failedShard.primary()) {
                // promote active replica to primary if active replica exists
                // 主分片降级成副本  副本升级成主分片
                unassignPrimaryAndPromoteActiveReplicaIfExists(failedShard, unassignedInfo, routingChangesObserver);
            } else {
                // 重分配就变成start 否则就移除
                if (failedShard.relocating()) {
                    remove(failedShard);
                } else {
                    moveToUnassigned(failedShard, unassignedInfo);
                }
            }
        }
        routingChangesObserver.shardFailed(failedShard, unassignedInfo);
        assert node(failedShard.currentNodeId()).getByShardId(failedShard.shardId()) == null : "failedShard " + failedShard +
            " was matched but wasn't removed";
    }

    /**
     * 某个处于init状态 且分配失败的主分片
     * @param failedShard
     * @param unassignedInfo
     * @param routingChangesObserver
     */
    private void unassignPrimaryAndPromoteActiveReplicaIfExists(ShardRouting failedShard, UnassignedInfo unassignedInfo,
                                                                RoutingChangesObserver routingChangesObserver) {
        assert failedShard.primary();
        // 找到某个活跃的副本对象
        ShardRouting activeReplica = activeReplicaWithHighestVersion(failedShard.shardId());
        // 如果这个分配失败的主分片此时没有任何活跃的副本
        if (activeReplica == null) {
            // 将当前分片变成 unassigned
            moveToUnassigned(failedShard, unassignedInfo);
        } else {
            // 如果存在副本  将主分片降级成副本分片
            movePrimaryToUnassignedAndDemoteToReplica(failedShard, unassignedInfo);
            // 将副本升级成主分片
            promoteReplicaToPrimary(activeReplica, routingChangesObserver);
        }
    }

    private void promoteReplicaToPrimary(ShardRouting activeReplica, RoutingChangesObserver routingChangesObserver) {
        // if the activeReplica was relocating before this call to failShard, its relocation was cancelled earlier when we
        // failed initializing replica shards (and moved replica relocation source back to started)
        assert activeReplica.started() : "replica relocation should have been cancelled: " + activeReplica;
        promoteActiveReplicaShardToPrimary(activeReplica);
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
        if (shard.relocatingNodeId() == null) {
            // if this is not a target shard for relocation, we need to update statistics
            inactiveShardCount--;
            if (shard.primary()) {
                inactivePrimaryCount--;
            }
        }
        // TODO
        removeRecovery(shard);
        ShardRouting startedShard = shard.moveToStarted();
        updateAssigned(shard, startedShard);
        return startedShard;
    }



    /**
     * Cancels a relocation of a shard that shard must relocating.
     *
     * @return the shard after cancelling relocation
     * 某个正在重分配的分片 在target节点上的分片转换成start状态时 之前的relocating分片就要移除 就会触发该方法
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
     * 卧槽 主分片应该才是最新的吧
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
            shard = cancelRelocation(shard);
        }
        assignedShardsRemove(shard);
        if (shard.initializing()) {
            // TODO
            removeRecovery(shard);
        }
    }

    /**
     * Removes relocation source of an initializing non-primary shard. This allows the replica shard to continue recovery from
     * the primary even though its non-primary relocation source has failed.
     * 只是把它作为一个普通的分片
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
     * 某个副本原本处于 init状态 当他的主分片由init变成start时 需要更新 副本分片
     * @param shard  待更新的副本分片
     * @return
     */
    private ShardRouting reinitReplica(ShardRouting shard) {
        assert shard.primary() == false : "shard must be a replica: " + shard;
        assert shard.initializing() : "can only reinitialize an initializing replica: " + shard;
        assert shard.isRelocationTarget() == false : "replication target cannot be reinitialized: " + shard;
        // 主要就是重新生成了一个 分配id
        ShardRouting reinitializedShard = shard.reinitializeReplicaShard();
        updateAssigned(shard, reinitializedShard);
        return reinitializedShard;
    }

    /**
     * 更新旧分片
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
        node(oldShard.currentNodeId()).update(oldShard, newShard);

        // 更新assignedShards 的分片内容
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
         * 某些分片将不会参与分配 此时会存储到这个list中
         */
        private final List<ShardRouting> ignored;

        /**
         * 其中有多少分片被标记成 primary
         */
        private int primaries = 0;

        /**
         * 每当有某个标记成 primary的分片被 ignore时 增加该值
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
         * 某个分片将不会被处理
         */
        public void ignoreShard(ShardRouting shard, AllocationStatus allocationStatus, RoutingChangesObserver changes) {
            nodes.ensureMutable();
            if (shard.primary()) {
                ignoredPrimaries++;
                UnassignedInfo currInfo = shard.unassignedInfo();
                assert currInfo != null;
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
         * 该对象负责遍历此时所有未分配的副本
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
             * @param existingAllocationId allocation id to use. If null, a fresh allocation id is generated.
             *                             将当前未分配的节点从迭代器移除 并修改成初始化状态
             */
            @Override
            public ShardRouting initialize(String nodeId, @Nullable String existingAllocationId, long expectedShardSize,
                                           RoutingChangesObserver routingChangesObserver) {
                nodes.ensureMutable();
                innerRemove();
                return nodes.initializeShard(current, nodeId, existingAllocationId, expectedShardSize, routingChangesObserver);
            }

            /**
             * Removes and ignores the unassigned shard (will be ignored for this run, but
             * will be added back to unassigned once the metadata is constructed again).
             * Typically this is used when an allocation decision prevents a shard from being allocated such
             * that subsequent consumers of this API won't try to allocate this shard again.
             *
             * @param attempt the result of the allocation attempt
             * @param changes 分片变化钩子
             *                忽略某个分片 同时从迭代器移除
             */
            @Override
            public void removeAndIgnore(AllocationStatus attempt, RoutingChangesObserver changes) {
                nodes.ensureMutable();
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
             * 更新当前节点的 未分配信息
             */
            @Override
            public ShardRouting updateUnassigned(UnassignedInfo unassignedInfo, RecoverySource recoverySource,
                                                 RoutingChangesObserver changes) {
                nodes.ensureMutable();
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
     * 返回一个迭代所有节点分片的迭代器
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
     * 记录此时所有需要恢复数据的分片
     * 以node作为划分界限
     */
    private static final class Recoveries {
        private static final Recoveries EMPTY = new Recoveries();
        /**
         * 需要接收数据的分片 比如 replicate
         */
        private int incoming = 0;
        /**
         * 此时往外输出数据的分片 比如 primary
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
