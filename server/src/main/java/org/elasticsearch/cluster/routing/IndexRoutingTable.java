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

import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.LocalShardsRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * The {@link IndexRoutingTable} represents routing information for a single
 * index. The routing table maintains a list of all shards in the index. A
 * single shard in this context has one more instances namely exactly one
 * {@link ShardRouting#primary() primary} and 1 or more replicas. In other
 * words, each instance of a shard is considered a replica while only one
 * replica per shard is a {@code primary} replica. The {@code primary} replica
 * can be seen as the "leader" of the shard acting as the primary entry point
 * for operations on a specific shard.
 * <p>
 * Note: The term replica is not directly
 * reflected in the routing table or in related classes, replicas are
 * represented as {@link ShardRouting}.
 * </p>
 * 每个 IndexShardRoutingTable 对象存储的是所有shardId 一致的分片 他们属于同一个index
 * 该对象也是以index为单位进行划分的 内部包含多个 shardId 对应的分片数据
 */
public class IndexRoutingTable extends AbstractDiffable<IndexRoutingTable> implements Iterable<IndexShardRoutingTable> {

    /**
     * 每个索引在 es 中有一个 name 和 uuid 属性
     */
    private final Index index;
    /**
     * 分片打乱器
     */
    private final ShardShuffler shuffler;

    // note, we assume that when the index routing is created, ShardRoutings are created for all possible number of
    // shards with state set to UNASSIGNED
    // 可以简单理解为一个map 同时key是 Integer类型
    private final ImmutableOpenIntMap<IndexShardRoutingTable> shards;

    /**
     * 遍历不同 shardId 下所有的 active 分片
     */
    private final List<ShardRouting> allActiveShards;

    /**
     *
     * @param index  该对象管理的是哪个index下所有的分片数据
     * @param shards   每个对象的shardId不同
     */
    IndexRoutingTable(Index index, ImmutableOpenIntMap<IndexShardRoutingTable> shards) {
        this.index = index;
        this.shuffler = new RotationShardShuffler(Randomness.get().nextInt());
        this.shards = shards;
        List<ShardRouting> allActiveShards = new ArrayList<>();
        for (IntObjectCursor<IndexShardRoutingTable> cursor : shards) {
            for (ShardRouting shardRouting : cursor.value) {
                if (shardRouting.active()) {
                    allActiveShards.add(shardRouting);
                }
            }
        }
        this.allActiveShards = Collections.unmodifiableList(allActiveShards);
    }

    /**
     * Return the index id
     *
     * @return id of the index
     */
    public Index getIndex() {
        return index;
    }

    /**
     * 校验元数据是否正确
     * @param metadata
     * @return
     */
    boolean validate(Metadata metadata) {
        // check index exists
        // 确保当前元数据中确实存在 index
        if (!metadata.hasIndex(index.getName())) {
            throw new IllegalStateException(index + " exists in routing does not exists in metadata");
        }
        // 确保2个index的uuid一致
        IndexMetadata indexMetadata = metadata.index(index.getName());
        if (indexMetadata.getIndexUUID().equals(index.getUUID()) == false) {
            throw new IllegalStateException(index.getName() + " exists in routing does not exists in metadata with the same uuid");
        }

        // check the number of shards
        // 确保分片数量一致  这里的分片数量是指 shardId不同的分片组的数量  每个shardId 对应一个 IndexShardingRoutingTable
        if (indexMetadata.getNumberOfShards() != shards().size()) {
            Set<Integer> expected = new HashSet<>();
            for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                expected.add(i);
            }
            for (IndexShardRoutingTable indexShardRoutingTable : this) {
                expected.remove(indexShardRoutingTable.shardId().id());
            }
            throw new IllegalStateException("Wrong number of shards in routing table, missing: " + expected);
        }

        // check the replicas
        // 每个shardId 下关联的分片数也要求一致  这个数量是一开始就设定好的么
        for (IndexShardRoutingTable indexShardRoutingTable : this) {
            int routingNumberOfReplicas = indexShardRoutingTable.size() - 1;
            if (routingNumberOfReplicas != indexMetadata.getNumberOfReplicas()) {
                throw new IllegalStateException("Shard [" + indexShardRoutingTable.shardId().id() +
                                 "] routing table has wrong number of replicas, expected [" + indexMetadata.getNumberOfReplicas() +
                                 "], got [" + routingNumberOfReplicas + "]");
            }
            // 确保最小单位的shardRouting 的index都是一样的
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                if (!shardRouting.index().equals(index)) {
                    throw new IllegalStateException("shard routing has an index [" + shardRouting.index() + "] that is different " +
                                                    "from the routing table");
                }
                // 获取分片对应的 分配id
                final Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(shardRouting.id());
                // 如果此时分片处于活跃阶段  该id必须存在于 inSyncAllocationIds 中
                if (shardRouting.active() &&
                    inSyncAllocationIds.contains(shardRouting.allocationId().getId()) == false) {
                    throw new IllegalStateException("active shard routing " + shardRouting + " has no corresponding entry in the in-sync " +
                        "allocation set " + inSyncAllocationIds);
                }

                // 针对私有 分片 或者初始状态的分片 他们恢复数据的 source类型 指定是磁盘时 需要做校验
                // TODO 下面的校验还没有理解
                if (shardRouting.primary() && shardRouting.initializing() &&
                    shardRouting.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE) {
                    if (inSyncAllocationIds.contains(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID)) {
                        if (inSyncAllocationIds.size() != 1) {
                            throw new IllegalStateException("a primary shard routing " + shardRouting
                                + " is a primary that is recovering from a stale primary has unexpected allocation ids in in-sync " +
                                "allocation set " + inSyncAllocationIds);
                        }
                    } else if (inSyncAllocationIds.contains(shardRouting.allocationId().getId()) == false) {
                        throw new IllegalStateException("a primary shard routing " + shardRouting
                            + " is a primary that is recovering from a known allocation id but has no corresponding entry in the in-sync " +
                            "allocation set " + inSyncAllocationIds);
                    }
                }
            }
        }
        return true;
    }

    @Override
    public Iterator<IndexShardRoutingTable> iterator() {
        return shards.valuesIt();
    }

    /**
     * Calculates the number of nodes that hold one or more shards of this index
     * {@link IndexRoutingTable} excluding the nodes with the node ids give as
     * the <code>excludedNodes</code> parameter.
     *
     * @param excludedNodes id of nodes that will be excluded  这些node对应的数据需要被排除
     * @return number of distinct nodes this index has at least one shard allocated on
     * 找到所有已经分配好的分片
     */
    public int numberOfNodesShardsAreAllocatedOn(String... excludedNodes) {
        Set<String> nodes = new HashSet<>();
        for (IndexShardRoutingTable shardRoutingTable : this) {
            for (ShardRouting shardRouting : shardRoutingTable) {
                if (shardRouting.assignedToNode()) {
                    String currentNodeId = shardRouting.currentNodeId();
                    boolean excluded = false;
                    if (excludedNodes != null) {
                        for (String excludedNode : excludedNodes) {
                            if (currentNodeId.equals(excludedNode)) {
                                excluded = true;
                                break;
                            }
                        }
                    }
                    if (!excluded) {
                        nodes.add(currentNodeId);
                    }
                }
            }
        }
        return nodes.size();
    }

    public ImmutableOpenIntMap<IndexShardRoutingTable> shards() {
        return shards;
    }

    public ImmutableOpenIntMap<IndexShardRoutingTable> getShards() {
        return shards();
    }

    /**
     * 通过指定 shardId 获取对应的 IndexShardRoutingTable
     * @param shardId
     * @return
     */
    public IndexShardRoutingTable shard(int shardId) {
        return shards.get(shardId);
    }

    /**
     * Returns <code>true</code> if all shards are primary and active. Otherwise <code>false</code>.
     * 代表所有私有的分片刚好都是活跃分片
     */
    public boolean allPrimaryShardsActive() {
        return primaryShardsActive() == shards().size();
    }

    /**
     * Calculates the number of primary shards in active state in routing table
     *
     * @return number of active primary shards
     */
    public int primaryShardsActive() {
        int counter = 0;
        for (IndexShardRoutingTable shardRoutingTable : this) {
            if (shardRoutingTable.primaryShard().active()) {
                counter++;
            }
        }
        return counter;
    }

    /**
     * Returns <code>true</code> if all primary shards are in
     * {@link ShardRoutingState#UNASSIGNED} state. Otherwise <code>false</code>.
     */
    public boolean allPrimaryShardsUnassigned() {
        return primaryShardsUnassigned() == shards.size();
    }

    /**
     * Calculates the number of primary shards in the routing table the are in
     * {@link ShardRoutingState#UNASSIGNED} state.
     * 返回私有 shard中 未分配的数量
     */
    public int primaryShardsUnassigned() {
        int counter = 0;
        for (IndexShardRoutingTable shardRoutingTable : this) {
            if (shardRoutingTable.primaryShard().unassigned()) {
                counter++;
            }
        }
        return counter;
    }

    /**
     * Returns a {@link List} of shards that match one of the states listed in {@link ShardRoutingState states}
     *
     * @param state {@link ShardRoutingState} to retrieve 0  描述分片的状态
     * @return a {@link List} of shards that match one of the given {@link ShardRoutingState states}
     *
     * 将所有与传入的state匹配的分片返回
     */
    public List<ShardRouting> shardsWithState(ShardRoutingState state) {
        List<ShardRouting> shards = new ArrayList<>();
        for (IndexShardRoutingTable shardRoutingTable : this) {
            shards.addAll(shardRoutingTable.shardsWithState(state));
        }
        return shards;
    }

    /**
     * Returns an unordered iterator over all active shards (including replicas).
     */
    public ShardsIterator randomAllActiveShardsIt() {
        return new PlainShardsIterator(shuffler.shuffle(allActiveShards));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexRoutingTable that = (IndexRoutingTable) o;

        if (!index.equals(that.index)) return false;
        if (!shards.equals(that.shards)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + shards.hashCode();
        return result;
    }

    public static IndexRoutingTable readFrom(StreamInput in) throws IOException {
        Index index = new Index(in);
        Builder builder = new Builder(index);

        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.addIndexShard(IndexShardRoutingTable.Builder.readFromThin(in, index));
        }

        return builder.build();
    }

    /**
     * 将对象包装成 Diff对象  内部有一个apply方法 可以获取解析后的对象  用户需要在 readDiffFrom() 的第一个参数中定义读取的逻辑
     * @param in
     * @return
     * @throws IOException
     */
    public static Diff<IndexRoutingTable> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(IndexRoutingTable::readFrom, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        index.writeTo(out);
        out.writeVInt(shards.size());
        for (IndexShardRoutingTable indexShard : this) {
            IndexShardRoutingTable.Builder.writeToThin(indexShard, out);
        }
    }

    public static Builder builder(Index index) {
        return new Builder(index);
    }


    public static class Builder {

        private final Index index;
        private final ImmutableOpenIntMap.Builder<IndexShardRoutingTable> shards = ImmutableOpenIntMap.builder();

        public Builder(Index index) {
            this.index = index;
        }

        /**
         * Initializes a new empty index, as if it was created from an API.
         * 因为该索引才完成创建 所以处于未分配状态
         */
        public Builder initializeAsNew(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        }

        /**
         * Initializes an existing index.
         * 代表在集群恢复状态下生成的 索引路由对象 此时会携带未分配信息 且原因是CLUSTER_RECOVERED
         */
        public Builder initializeAsRecovery(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null));
        }

        /**
         * Initializes a new index caused by dangling index imported.
         * 处于未分配状态的原因是 处于摇摆状态???
         */
        public Builder initializeAsFromDangling(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.DANGLING_INDEX_IMPORTED, null));
        }

        /**
         * Initializes a new empty index, as as a result of opening a closed index.
         * 重新开启关闭的索引
         */
        public Builder initializeAsFromCloseToOpen(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, null));
        }

        /**
         * Initializes a new empty index, as as a result of closing an opened index.
         * 此时关闭了某个索引
         */
        public Builder initializeAsFromOpenToClose(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CLOSED, null));
        }

        /**
         * Initializes a new empty index, to be restored from a snapshot
         * 生成有关某个index 的路由信息 并标明数据恢复方式
         * @param ignoreShards 本次是基于快照恢复的 但是该快照可能有某些分片的数据生成失败了
         */
        public Builder initializeAsNewRestore(IndexMetadata indexMetadata, SnapshotRecoverySource recoverySource, IntSet ignoreShards) {
            // 这里创建了一个未分配的新分片  并标明是从 restore中生成的
            final UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.NEW_INDEX_RESTORED,
                "restore_source[" + recoverySource.snapshot().getRepository() + "/" +
                 recoverySource.snapshot().getSnapshotId().getName() + "]");
            return initializeAsRestore(indexMetadata, recoverySource, ignoreShards, true, unassignedInfo);
        }

        /**
         * Initializes an existing index, to be restored from a snapshot
         * @param indexMetadata 使用这个新的索引元数据去更新之前的元数据
         */
        public Builder initializeAsRestore(IndexMetadata indexMetadata, SnapshotRecoverySource recoverySource) {
            final UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.EXISTING_INDEX_RESTORED,
                 "restore_source[" + recoverySource.snapshot().getRepository() + "/" +
                 recoverySource.snapshot().getSnapshotId().getName() + "]");
            return initializeAsRestore(indexMetadata, recoverySource, null, false, unassignedInfo);
        }

        /**
         * Initializes an index, to be restored from snapshot
         * @param asNew  本次索引是新生成的 还是原本就存在的
         * 代表在通过快照restore时 产生了一个新的索引
         */
        private Builder initializeAsRestore(IndexMetadata indexMetadata, SnapshotRecoverySource recoverySource, IntSet ignoreShards,
                                            boolean asNew, UnassignedInfo unassignedInfo) {
            assert indexMetadata.getIndex().equals(index);
            // 代表已经执行过初始化了
            if (!shards.isEmpty()) {
                throw new IllegalStateException("trying to initialize an index with fresh shards, but already has shards created");
            }

            // 插入 shard级别的数据
            for (int shardNumber = 0; shardNumber < indexMetadata.getNumberOfShards(); shardNumber++) {
                ShardId shardId = new ShardId(index, shardNumber);
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
                for (int i = 0; i <= indexMetadata.getNumberOfReplicas(); i++) {
                    boolean primary = i == 0;
                    // 代表本次如果是新建的索引   但是这个分片可能无法从快照中恢复数据 比如创建快照时失败了 那么就要指定别的 RecoverySource
                    if (asNew && ignoreShards.contains(shardNumber)) {
                        // This shards wasn't completely snapshotted - restore it as new shard
                        indexShardRoutingBuilder.addShard(ShardRouting.newUnassigned(shardId, primary,
                            // 如果当前遍历到的是主分片 那么恢复源为空   副本分片指定从其他节点恢复数据    TODO 主分片都无法正常工作了 副本怎么恢复数据还有意义吗
                            primary ? EmptyStoreRecoverySource.INSTANCE : PeerRecoverySource.INSTANCE, unassignedInfo));
                    } else {
                        // 主分片使用 快照进行恢复  其他分片采用PeerRecoverySource.INSTANCE
                        indexShardRoutingBuilder.addShard(ShardRouting.newUnassigned(shardId, primary,
                            primary ? recoverySource : PeerRecoverySource.INSTANCE, unassignedInfo));
                    }
                }
                shards.put(shardNumber, indexShardRoutingBuilder.build());
            }
            return this;
        }

        /**
         * Initializes a new empty index, with an option to control if its from an API or not.
         * @param indexMetadata 描述索引的元数据
         * @param unassignedInfo 代表分片未分配的原因
         * 初始化一个空的索引对象
         */
        private Builder initializeEmpty(IndexMetadata indexMetadata, UnassignedInfo unassignedInfo) {
            assert indexMetadata.getIndex().equals(index);
            // 在调用该方法前 确保shards为空
            if (!shards.isEmpty()) {
                throw new IllegalStateException("trying to initialize an index with fresh shards, but already has shards created");
            }
            for (int shardNumber = 0; shardNumber < indexMetadata.getNumberOfShards(); shardNumber++) {
                // 为每个 IndexShardRoutingTable 生成对应的shardId
                ShardId shardId = new ShardId(index, shardNumber);
                final RecoverySource primaryRecoverySource;
                // 如果分片id 已经有某些 AllocationIds了   这里私有分片的恢复源就设定为 从磁盘中恢复
                if (indexMetadata.inSyncAllocationIds(shardNumber).isEmpty() == false) {
                    // we have previous valid copies for this shard. use them for recovery
                    primaryRecoverySource = ExistingStoreRecoverySource.INSTANCE;
                // TODO ResizeSourceIndex 是什么???
                } else if (indexMetadata.getResizeSourceIndex() != null) {
                    // this is a new index but the initial shards should merged from another index
                    primaryRecoverySource = LocalShardsRecoverySource.INSTANCE;
                } else {
                    // a freshly created index with no restriction
                    primaryRecoverySource = EmptyStoreRecoverySource.INSTANCE;
                }
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
                for (int i = 0; i <= indexMetadata.getNumberOfReplicas(); i++) {
                    // 第一个分片总是作为 私有分片  啥意思   可以看到其他分片都是使用 PeerRecoverySource 代表从远端进行数据恢复
                    boolean primary = i == 0;
                    indexShardRoutingBuilder.addShard(ShardRouting.newUnassigned(shardId, primary,
                        primary ? primaryRecoverySource : PeerRecoverySource.INSTANCE, unassignedInfo));
                }
                shards.put(shardNumber, indexShardRoutingBuilder.build());
            }
            return this;
        }

        /**
         * 增加一个副本 就代表着为该索引下所有的分片都增加一个副本
         * @return
         */
        public Builder addReplica() {
            for (IntCursor cursor : shards.keys()) {
                int shardNumber = cursor.value;
                ShardId shardId = new ShardId(index, shardNumber);
                // version 0, will get updated when reroute will happen
                // 指明了创建的分片对象的 未分配原因为 REPLICA_ADDED     注意这里的恢复源选择的是从远端节点  副本肯定是从primary中恢复数据的
                ShardRouting shard = ShardRouting.newUnassigned(shardId, false, PeerRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.REPLICA_ADDED, null));

                // 在额外增加一个副本后覆盖之前的 IndexShardRoutingTable信息
                shards.put(shardNumber,
                        new IndexShardRoutingTable.Builder(shards.get(shard.id())).addShard(shard).build()
                );
            }
            return this;
        }

        /**
         * 将分片下多个副本中的其中一个移除
         * @return
         */
        public Builder removeReplica() {
            for (IntCursor cursor : shards.keys()) {
                int shardId = cursor.value;
                // 对应某个分片下所有的副本分配情况
                IndexShardRoutingTable indexShard = shards.get(shardId);
                // 当此时已经没有副本了 就无法移除 (不能通过这种方式移除primary)
                if (indexShard.replicaShards().isEmpty()) {
                    // nothing to do here!
                    return this;
                }
                // re-add all the current ones
                // 先将此时所有的副本设置进去
                IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(indexShard.shardId());
                for (ShardRouting shardRouting : indexShard) {
                    builder.addShard(shardRouting);
                }
                // first check if there is one that is not assigned to a node, and remove it
                boolean removed = false;
                for (ShardRouting shardRouting : indexShard) {
                    // 首先主分片必然是无法被移除的  其次优先找到此时node未明确的副本
                    if (!shardRouting.primary() && !shardRouting.assignedToNode()) {
                        builder.removeShard(shardRouting);
                        removed = true;
                        break;
                    }
                }
                if (!removed) {
                    for (ShardRouting shardRouting : indexShard) {
                        // 此时只能选择一个已经分配好的副本进行移除
                        if (!shardRouting.primary()) {
                            builder.removeShard(shardRouting);
                            break;
                        }
                    }
                }
                shards.put(shardId, builder.build());
            }
            return this;
        }

        public Builder addIndexShard(IndexShardRoutingTable indexShard) {
            shards.put(indexShard.shardId().id(), indexShard);
            return this;
        }

        /**
         * Adds a new shard routing (makes a copy of it), with reference data used from the index shard routing table
         * if it needs to be created.
         * 将某个分片加入到容器中
         */
        public Builder addShard(ShardRouting shard) {
            IndexShardRoutingTable indexShard = shards.get(shard.id());
            if (indexShard == null) {
                indexShard = new IndexShardRoutingTable.Builder(shard.shardId()).addShard(shard).build();
            } else {
                indexShard = new IndexShardRoutingTable.Builder(indexShard).addShard(shard).build();
            }
            shards.put(indexShard.shardId().id(), indexShard);
            return this;
        }

        public IndexRoutingTable build() {
            return new IndexRoutingTable(index, shards.build());
        }
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("-- index [" + index + "]\n");

        List<IndexShardRoutingTable> ordered = new ArrayList<>();
        for (IndexShardRoutingTable indexShard : this) {
            ordered.add(indexShard);
        }

        CollectionUtil.timSort(ordered, (o1, o2) -> {
            int v = o1.shardId().getIndex().getName().compareTo(
                    o2.shardId().getIndex().getName());
            if (v == 0) {
                v = Integer.compare(o1.shardId().id(),
                                    o2.shardId().id());
            }
            return v;
        });

        for (IndexShardRoutingTable indexShard : ordered) {
            sb.append("----shard_id [").append(indexShard.shardId().getIndex().getName())
                .append("][").append(indexShard.shardId().id()).append("]\n");
            for (ShardRouting shard : indexShard) {
                sb.append("--------").append(shard.shortSummary()).append("\n");
            }
        }
        return sb.toString();
    }


}
