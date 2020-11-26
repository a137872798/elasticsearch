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

import org.elasticsearch.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * {@link ShardRouting} immutably encapsulates information about shard
 * indexRoutings like id, state, version, etc.
 * 代表某个可路由的分片 它可能此时正分配在某个node上 也可能在某个时刻会触发relocating 重分配到其他node上
 */
public final class ShardRouting implements Writeable, ToXContentObject {

    /**
     * Used if shard size is not available
     */
    public static final long UNAVAILABLE_EXPECTED_SHARD_SIZE = -1;

    /**
     * 包含了分片id(int) 以及对应的索引信息 (该分片是针对哪个索引而言的)
     */
    private final ShardId shardId;

    /**
     * 该分片当前所在的节点
     */
    private final String currentNodeId;
    /**
     * 该分片正在转移到哪个node
     */
    private final String relocatingNodeId;

    /**
     * 是否是主分片 其余分片都是副本  那么恢复数据时 也应该以主分片为基准
     */
    private final boolean primary;
    /**
     * 描述分片的状态
     */
    private final ShardRoutingState state;
    /**
     * 指定了如何恢复数据分片
     */
    private final RecoverySource recoverySource;

    /**
     * 描述未分配原因 和 当前分配状态的对象
     * 比如这个shard可能分配失败 那么失败信息就会被记录在info内 之后decider就可以根据这个info做一些处理
     */
    private final UnassignedInfo unassignedInfo;

    /**
     * 该分片由哪个分配器调度
     */
    private final AllocationId allocationId;

    /**
     * 单实例列表
     */
    private final transient List<ShardRouting> asList;
    /**
     * 预期的分片大小
     */
    private final long expectedShardSize;

    /**
     * 如果此时创建的shardRouting 处于 relocation 那么会生成该对象  挂载在targetNode上 并且标识状态为INIT
     */
    @Nullable
    private final ShardRouting targetRelocatingShard;

    /**
     * A constructor to internally create shard routing instances, note, the internal flag should only be set to true
     * by either this class or tests. Visible for testing.
     */
    ShardRouting(ShardId shardId, String currentNodeId,
                 String relocatingNodeId, boolean primary, ShardRoutingState state, RecoverySource recoverySource,
                 UnassignedInfo unassignedInfo, AllocationId allocationId, long expectedShardSize) {
        this.shardId = shardId;
        this.currentNodeId = currentNodeId;
        this.relocatingNodeId = relocatingNodeId;
        this.primary = primary;
        this.state = state;
        this.recoverySource = recoverySource;
        this.unassignedInfo = unassignedInfo;
        this.allocationId = allocationId;
        this.expectedShardSize = expectedShardSize;
        // 初始化relocation对应的分片
        this.targetRelocatingShard = initializeTargetRelocatingShard();
        this.asList = Collections.singletonList(this);
        assert expectedShardSize == UNAVAILABLE_EXPECTED_SHARD_SIZE || state == ShardRoutingState.INITIALIZING ||
            state == ShardRoutingState.RELOCATING : expectedShardSize + " state: " + state;
        assert expectedShardSize >= 0 || state != ShardRoutingState.INITIALIZING || state != ShardRoutingState.RELOCATING :
            expectedShardSize + " state: " + state;
        assert !(state == ShardRoutingState.UNASSIGNED && unassignedInfo == null) : "unassigned shard must be created with meta";
        assert (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) == (recoverySource != null) :
            "recovery source only available on unassigned or initializing shard but was " + state;
        assert recoverySource == null || recoverySource == PeerRecoverySource.INSTANCE || primary :
            "replica shards always recover from primary";
        assert (currentNodeId == null) == (state == ShardRoutingState.UNASSIGNED)  :
            "unassigned shard must not be assigned to a node " + this;
    }

    /**
     * 生成重定位后的分片信息
     * @return
     */
    @Nullable
    private ShardRouting initializeTargetRelocatingShard() {
        // 首先确保当前分片状态处在 重定位中
        if (state == ShardRoutingState.RELOCATING) {
            // 重定位后的节点 relocatingNodeId currentNodeId 发生了交换
            return new ShardRouting(shardId, relocatingNodeId, currentNodeId, primary, ShardRoutingState.INITIALIZING,
                PeerRecoverySource.INSTANCE, unassignedInfo, AllocationId.newTargetRelocation(allocationId), expectedShardSize);
        } else {
            return null;
        }
    }

    /**
     * Creates a new unassigned shard.
     * 生成一个未分配的分片数据   因为此时还没有指派给某个分配器  所以 allocationId为null
     */
    public static ShardRouting newUnassigned(ShardId shardId, boolean primary, RecoverySource recoverySource,
                                             UnassignedInfo unassignedInfo) {
        return new ShardRouting(shardId, null, null, primary, ShardRoutingState.UNASSIGNED,
                                recoverySource, unassignedInfo, null, UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    /**
     * 获取该分片对应的索引信息
     * @return
     */
    public Index index() {
        return shardId.getIndex();
    }

    /**
     * The index name.
     */
    public String getIndexName() {
        return shardId.getIndexName();
    }

    /**
     * The shard id.
     */
    public int id() {
        return shardId.id();
    }

    /**
     * The shard id.
     */
    public int getId() {
        return id();
    }


    /**
     * The shard is unassigned (not allocated to any node).
     */
    public boolean unassigned() {
        return state == ShardRoutingState.UNASSIGNED;
    }

    /**
     * The shard is initializing (usually recovering either from peer shard
     * or from gateway).
     */
    public boolean initializing() {
        return state == ShardRoutingState.INITIALIZING;
    }

    /**
     * Returns <code>true</code> iff the this shard is currently
     * {@link ShardRoutingState#STARTED started} or
     * {@link ShardRoutingState#RELOCATING relocating} to another node.
     * Otherwise <code>false</code>
     * 在 init之上的状态都代表活跃状态
     */
    public boolean active() {
        return started() || relocating();
    }

    /**
     * The shard is in started mode.
     */
    public boolean started() {
        return state == ShardRoutingState.STARTED;
    }

    /**
     * Returns <code>true</code> iff the this shard is currently relocating to
     * another node. Otherwise <code>false</code>
     *
     * @see ShardRoutingState#RELOCATING
     */
    public boolean relocating() {
        return state == ShardRoutingState.RELOCATING;
    }

    /**
     * Returns <code>true</code> iff this shard is assigned to a node ie. not
     * {@link ShardRoutingState#UNASSIGNED unassigned}. Otherwise <code>false</code>
     */
    public boolean assignedToNode() {
        return currentNodeId != null;
    }

    /**
     * The current node id the shard is allocated on.
     */
    public String currentNodeId() {
        return this.currentNodeId;
    }

    /**
     * The relocating node id the shard is either relocating to or relocating from.
     */
    public String relocatingNodeId() {
        return this.relocatingNodeId;
    }

    /**
     * Returns a shard routing representing the target shard.
     * The target shard routing will be the INITIALIZING state and have relocatingNodeId set to the
     * source node.
     */
    public ShardRouting getTargetRelocatingShard() {
        assert relocating();
        return targetRelocatingShard;
    }

    /**
     * Additional metadata on why the shard is/was unassigned. The metadata is kept around
     * until the shard moves to STARTED.
     */
    @Nullable
    public UnassignedInfo unassignedInfo() {
        return unassignedInfo;
    }

    /**
     * An id that uniquely identifies an allocation.
     */
    @Nullable
    public AllocationId allocationId() {
        return this.allocationId;
    }

    /**
     * Returns <code>true</code> iff this shard is a primary.
     */
    public boolean primary() {
        return this.primary;
    }

    /**
     * The shard state.
     */
    public ShardRoutingState state() {
        return this.state;
    }

    /**
     * The shard id.
     */
    public ShardId shardId() {
        return shardId;
    }

    /**
     * A shard iterator with just this shard in it.
     * 返回分片列表信息  在初始节点 asList中仅保存当前对象
     * 该对象后面增加的 都是同一个 shardId的分片
     */
    public ShardIterator shardsIt() {
        return new PlainShardIterator(shardId, asList);
    }

    /**
     * 从数据流中读取属性并进行初始化
     * @param shardId
     * @param in
     * @throws IOException
     */
    public ShardRouting(ShardId shardId, StreamInput in) throws IOException {
        this.shardId = shardId;
        currentNodeId = in.readOptionalString();
        relocatingNodeId = in.readOptionalString();
        primary = in.readBoolean();
        state = ShardRoutingState.fromValue(in.readByte());
        if (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) {
            recoverySource = RecoverySource.readFrom(in);
        } else {
            recoverySource = null;
        }
        unassignedInfo = in.readOptionalWriteable(UnassignedInfo::new);
        allocationId = in.readOptionalWriteable(AllocationId::new);
        final long shardSize;
        if (state == ShardRoutingState.RELOCATING || state == ShardRoutingState.INITIALIZING) {
            shardSize = in.readLong();
        } else {
            shardSize = UNAVAILABLE_EXPECTED_SHARD_SIZE;
        }
        expectedShardSize = shardSize;
        asList = Collections.singletonList(this);
        targetRelocatingShard = initializeTargetRelocatingShard();
    }

    public ShardRouting(StreamInput in) throws IOException {
        this(new ShardId(in), in);
    }

    /**
     * Writes shard information to {@link StreamOutput} without writing index name and shard id
     *
     * @param out {@link StreamOutput} to write shard information to
     * @throws IOException if something happens during write
     */
    public void writeToThin(StreamOutput out) throws IOException {
        out.writeOptionalString(currentNodeId);
        out.writeOptionalString(relocatingNodeId);
        out.writeBoolean(primary);
        out.writeByte(state.value());
        if (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) {
            recoverySource.writeTo(out);
        }
        out.writeOptionalWriteable(unassignedInfo);
        out.writeOptionalWriteable(allocationId);
        if (state == ShardRoutingState.RELOCATING || state == ShardRoutingState.INITIALIZING) {
            out.writeLong(expectedShardSize);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        writeToThin(out);
    }

    // -- 以下操作都是将原有对象的某些属性做更新后 生成新的分片对象  -- //

    public ShardRouting updateUnassigned(UnassignedInfo unassignedInfo, RecoverySource recoverySource) {
        assert this.unassignedInfo != null : "can only update unassign info if they are already set";
        assert this.unassignedInfo.isDelayed() || (unassignedInfo.isDelayed() == false) : "cannot transition from non-delayed to delayed";
        return new ShardRouting(shardId, currentNodeId, relocatingNodeId, primary, state, recoverySource,
            unassignedInfo, allocationId, expectedShardSize);
    }

    /**
     * Moves the shard to unassigned state.
     * 将当前分片更新成  未分配状态
     */
    public ShardRouting moveToUnassigned(UnassignedInfo unassignedInfo) {
        assert state != ShardRoutingState.UNASSIGNED : this;
        final RecoverySource recoverySource;
        if (active()) {
            if (primary()) {
                recoverySource = ExistingStoreRecoverySource.INSTANCE;
            } else {
                recoverySource = PeerRecoverySource.INSTANCE;
            }
        } else {
            recoverySource = recoverySource();
        }
        return new ShardRouting(shardId, null, null, primary, ShardRoutingState.UNASSIGNED, recoverySource,
            unassignedInfo, null, UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    /**
     * Initializes an unassigned shard on a node.
     *
     * @param existingAllocationId allocation id to use. If null, a fresh allocation id is generated.
     *                             将当前分片更新成初始状态
     */
    public ShardRouting initialize(String nodeId, @Nullable String existingAllocationId, long expectedShardSize) {
        assert state == ShardRoutingState.UNASSIGNED : this;
        assert relocatingNodeId == null : this;

        // 每当初始化状态时 就会生成一个新的allocationId
        // AllocationId 内部总计包含2个属性 一个是 relocateId 还有个是 allocationId
        final AllocationId allocationId;
        if (existingAllocationId == null) {
            allocationId = AllocationId.newInitializing();
        } else {
            allocationId = AllocationId.newInitializing(existingAllocationId);
        }
        return new ShardRouting(shardId, nodeId, null, primary, ShardRoutingState.INITIALIZING, recoverySource,
            unassignedInfo, allocationId, expectedShardSize);
    }

    /**
     * Relocate the shard to another node.
     *
     * @param relocatingNodeId id of the node to relocate the shard  本分片本次要前往的目标节点
     *                         生成一个处于重定向的node
     */
    public ShardRouting relocate(String relocatingNodeId, long expectedShardSize) {
        assert state == ShardRoutingState.STARTED : "current shard has to be started in order to be relocated " + this;
        return new ShardRouting(shardId, currentNodeId, relocatingNodeId, primary, ShardRoutingState.RELOCATING, recoverySource,
            null, AllocationId.newRelocation(allocationId), expectedShardSize);
    }

    /**
     * Cancel relocation of a shard. The shards state must be set
     * to <code>RELOCATING</code>.
     * 代表从relocating的状态结束   TODO 这里又转换回了 started状态  难道 relocating指的是副本拷贝么
     */
    public ShardRouting cancelRelocation() {
        assert state == ShardRoutingState.RELOCATING : this;
        assert assignedToNode() : this;
        assert relocatingNodeId != null : this;
        return new ShardRouting(shardId, currentNodeId, null, primary, ShardRoutingState.STARTED, recoverySource,
            null, AllocationId.cancelRelocation(allocationId), UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    /**
     * Removes relocation source of a non-primary shard. The shard state must be <code>INITIALIZING</code>.
     * This allows the non-primary shard to continue recovery from the primary even though its non-primary
     * relocation source has failed.
     */
    public ShardRouting removeRelocationSource() {
        assert primary == false : this;
        assert state == ShardRoutingState.INITIALIZING : this;
        assert assignedToNode() : this;
        assert relocatingNodeId != null : this;
        return new ShardRouting(shardId, currentNodeId, null, primary, state, recoverySource, unassignedInfo,
            AllocationId.finishRelocation(allocationId), expectedShardSize);
    }

    /**
     * Reinitializes a replica shard, giving it a fresh allocation id
     * 将当前已经处于init状态的分片更新 这里会使用一个新的分配id
     */
    public ShardRouting reinitializeReplicaShard() {
        assert state == ShardRoutingState.INITIALIZING : this;
        assert primary == false : this;
        assert isRelocationTarget() == false : this;
        return new ShardRouting(shardId, currentNodeId, null, primary, ShardRoutingState.INITIALIZING,
            recoverySource, unassignedInfo, AllocationId.newInitializing(), expectedShardSize);
    }

    /**
     * Set the shards state to <code>STARTED</code>. The shards state must be
     * <code>INITIALIZING</code> or <code>RELOCATING</code>. Any relocation will be
     * canceled.
     */
    public ShardRouting moveToStarted() {
        assert state == ShardRoutingState.INITIALIZING : "expected an initializing shard " + this;
        AllocationId allocationId = this.allocationId;
        if (allocationId.getRelocationId() != null) {
            // relocation target
            allocationId = AllocationId.finishRelocation(allocationId);
        }
        return new ShardRouting(shardId, currentNodeId, null, primary, ShardRoutingState.STARTED, null, null, allocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    /**
     * Make the active shard primary unless it's not primary
     *
     * @throws IllegalShardRoutingStateException if shard is already a primary
     * 将某个副本对象升级成主分片
     */
    public ShardRouting moveActiveReplicaToPrimary() {
        assert active(): "expected an active shard " + this;
        if (primary) {
            throw new IllegalShardRoutingStateException(this, "Already primary, can't move to primary");
        }
        return new ShardRouting(shardId, currentNodeId, relocatingNodeId, true, state, recoverySource, unassignedInfo, allocationId,
            expectedShardSize);
    }

    /**
     * Set the unassigned primary shard to non-primary
     *
     * @throws IllegalShardRoutingStateException if shard is already a replica
     */
    public ShardRouting moveUnassignedFromPrimary() {
        assert state == ShardRoutingState.UNASSIGNED : "expected an unassigned shard " + this;
        if (!primary) {
            throw new IllegalShardRoutingStateException(this, "Not primary, can't move to replica");
        }
        return new ShardRouting(shardId, currentNodeId, relocatingNodeId, false, state, PeerRecoverySource.INSTANCE, unassignedInfo,
            allocationId, expectedShardSize);
    }

    /**
     * returns true if this routing has the same allocation ID as another.
     * <p>
     * Note: if both shard routing has a null as their {@link #allocationId()}, this method returns false as the routing describe
     * no allocation at all..
     **/
    public boolean isSameAllocation(ShardRouting other) {
        boolean b = this.allocationId != null && other.allocationId != null && this.allocationId.getId().equals(other.allocationId.getId());
        assert b == false || this.currentNodeId.equals(other.currentNodeId) :
            "ShardRoutings have the same allocation id but not the same node. This [" + this + "], other [" + other + "]";
        return b;
    }

    /**
     * Returns <code>true</code> if this shard is a relocation target for another shard
     * (i.e., was created with {@link #initializeTargetRelocatingShard()}
     */
    public boolean isRelocationTarget() {
        return state == ShardRoutingState.INITIALIZING && relocatingNodeId != null;
    }

    /** returns true if the routing is the relocation target of the given routing */
    public boolean isRelocationTargetOf(ShardRouting other) {
        boolean b = this.allocationId != null && other.allocationId != null && this.state == ShardRoutingState.INITIALIZING &&
            this.allocationId.getId().equals(other.allocationId.getRelocationId());

        assert b == false || other.state == ShardRoutingState.RELOCATING :
            "ShardRouting is a relocation target but the source shard state isn't relocating. This [" + this + "], other [" + other + "]";

        assert b == false || other.allocationId.getId().equals(this.allocationId.getRelocationId()) :
            "ShardRouting is a relocation target but the source id isn't equal to source's allocationId.getRelocationId." +
            " This [" + this + "], other [" + other + "]";

        assert b == false || other.currentNodeId().equals(this.relocatingNodeId) :
            "ShardRouting is a relocation target but source current node id isn't equal to target relocating node." +
            " This [" + this + "], other [" + other + "]";

        assert b == false || this.currentNodeId().equals(other.relocatingNodeId) :
            "ShardRouting is a relocation target but current node id isn't equal to source relocating node." +
                " This [" + this + "], other [" + other + "]";

        assert b == false || this.shardId.equals(other.shardId) :
            "ShardRouting is a relocation target but both indexRoutings are not of the same shard id." +
                " This [" + this + "], other [" + other + "]";

        assert b == false || this.primary == other.primary :
            "ShardRouting is a relocation target but primary flag is different." +
                " This [" + this + "], target [" + other + "]";

        return b;
    }

    /** returns true if the routing is the relocation source for the given routing */
    public boolean isRelocationSourceOf(ShardRouting other) {
        boolean b = this.allocationId != null && other.allocationId != null && other.state == ShardRoutingState.INITIALIZING &&
            other.allocationId.getId().equals(this.allocationId.getRelocationId());

        assert b == false || this.state == ShardRoutingState.RELOCATING :
            "ShardRouting is a relocation source but shard state isn't relocating. This [" + this + "], other [" + other + "]";


        assert b == false || this.allocationId.getId().equals(other.allocationId.getRelocationId()) :
            "ShardRouting is a relocation source but the allocation id isn't equal to other.allocationId.getRelocationId." +
                " This [" + this + "], other [" + other + "]";

        assert b == false || this.currentNodeId().equals(other.relocatingNodeId) :
            "ShardRouting is a relocation source but current node isn't equal to other's relocating node." +
                " This [" + this + "], other [" + other + "]";

        assert b == false || other.currentNodeId().equals(this.relocatingNodeId) :
            "ShardRouting is a relocation source but relocating node isn't equal to other's current node." +
                " This [" + this + "], other [" + other + "]";

        assert b == false || this.shardId.equals(other.shardId) :
            "ShardRouting is a relocation source but both indexRoutings are not of the same shard." +
                " This [" + this + "], target [" + other + "]";

        assert b == false || this.primary == other.primary :
            "ShardRouting is a relocation source but primary flag is different. This [" + this + "], target [" + other + "]";

        return b;
    }

    /** returns true if the current routing is identical to the other routing in all but meta fields, i.e., unassigned info */
    public boolean equalsIgnoringMetadata(ShardRouting other) {
        if (primary != other.primary) {
            return false;
        }
        if (shardId != null ? !shardId.equals(other.shardId) : other.shardId != null) {
            return false;
        }
        if (currentNodeId != null ? !currentNodeId.equals(other.currentNodeId) : other.currentNodeId != null) {
            return false;
        }
        if (relocatingNodeId != null ? !relocatingNodeId.equals(other.relocatingNodeId) : other.relocatingNodeId != null) {
            return false;
        }
        if (allocationId != null ? !allocationId.equals(other.allocationId) : other.allocationId != null) {
            return false;
        }
        if (state != other.state) {
            return false;
        }
        if (recoverySource != null ? !recoverySource.equals(other.recoverySource) : other.recoverySource != null) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof ShardRouting)) {
            return false;
        }
        ShardRouting that = (ShardRouting) o;
        if (unassignedInfo != null ? !unassignedInfo.equals(that.unassignedInfo) : that.unassignedInfo != null) {
            return false;
        }
        return equalsIgnoringMetadata(that);
    }

    /**
     * Cache hash code in same same way as {@link String#hashCode()}) using racy single-check idiom
     * as it is mainly used in single-threaded code ({@link BalancedShardsAllocator}).
     */
    private int hashCode; // default to 0

    @Override
    public int hashCode() {
        int h = hashCode;
        if (h == 0) {
            h = shardId.hashCode();
            h = 31 * h + (currentNodeId != null ? currentNodeId.hashCode() : 0);
            h = 31 * h + (relocatingNodeId != null ? relocatingNodeId.hashCode() : 0);
            h = 31 * h + (primary ? 1 : 0);
            h = 31 * h + (state != null ? state.hashCode() : 0);
            h = 31 * h + (recoverySource != null ? recoverySource.hashCode() : 0);
            h = 31 * h + (allocationId != null ? allocationId.hashCode() : 0);
            h = 31 * h + (unassignedInfo != null ? unassignedInfo.hashCode() : 0);
            hashCode = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return shortSummary();
    }

    /**
     * A short description of the shard.
     */
    public String shortSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(shardId.getIndexName()).append(']').append('[').append(shardId.getId()).append(']');
        sb.append(", node[").append(currentNodeId).append("], ");
        if (relocatingNodeId != null) {
            sb.append("relocating [").append(relocatingNodeId).append("], ");
        }
        if (primary) {
            sb.append("[P]");
        } else {
            sb.append("[R]");
        }
        if (recoverySource != null) {
            sb.append(", recovery_source[").append(recoverySource).append("]");
        }
        sb.append(", s[").append(state).append("]");
        if (allocationId != null) {
            sb.append(", a").append(allocationId);
        }
        if (this.unassignedInfo != null) {
            sb.append(", ").append(unassignedInfo.toString());
        }
        if (expectedShardSize != UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            sb.append(", expected_shard_size[").append(expectedShardSize).append("]");
        }
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("state", state())
            .field("primary", primary())
            .field("node", currentNodeId())
            .field("relocating_node", relocatingNodeId())
            .field("shard", id())
            .field("index", getIndexName());
        if (expectedShardSize != UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            builder.field("expected_shard_size_in_bytes", expectedShardSize);
        }
        if (recoverySource != null) {
            builder.field("recovery_source", recoverySource);
        }
        if (allocationId != null) {
            builder.field("allocation_id");
            allocationId.toXContent(builder, params);
        }
        if (unassignedInfo != null) {
            unassignedInfo.toXContent(builder, params);
        }
        return builder.endObject();
    }

    /**
     * Returns the expected shard size for {@link ShardRoutingState#RELOCATING} and {@link ShardRoutingState#INITIALIZING}
     * shards. If it's size is not available {@value #UNAVAILABLE_EXPECTED_SHARD_SIZE} will be returned.
     */
    public long getExpectedShardSize() {
        return expectedShardSize;
    }

    /**
     * Returns recovery source for the given shard. Replica shards always recover from the primary {@link PeerRecoverySource}.
     *
     * @return recovery source or null if shard is {@link #active()}
     */
    @Nullable
    public RecoverySource recoverySource() {
        return recoverySource;
    }
}
