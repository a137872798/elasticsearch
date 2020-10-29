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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents the recovery source of a shard. Available recovery types are:
 *
 * - {@link EmptyStoreRecoverySource} recovery from an empty store
 * - {@link ExistingStoreRecoverySource} recovery from an existing store
 * - {@link PeerRecoverySource} recovery from a primary on another node
 * - {@link SnapshotRecoverySource} recovery from a snapshot
 * - {@link LocalShardsRecoverySource} recovery from other shards of another index on the same node
 * 代表分片数据从哪里恢复
 */
public abstract class RecoverySource implements Writeable, ToXContentObject {

    /**
     * 在父类只定义了最基础的 type信息  其他信息需要子类进行覆盖
     * @param builder
     * @param params
     * @return
     * @throws IOException
     */
    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("type", getType());
        addAdditionalFields(builder, params);
        return builder.endObject();
    }

    /**
     * to be overridden by subclasses
、     */
    public void addAdditionalFields(XContentBuilder builder, ToXContent.Params params) throws IOException {

    }

    /**
     * 从某个输入流中还原 RecoverySource 对象
     * @param in
     * @return
     * @throws IOException
     */
    public static RecoverySource readFrom(StreamInput in) throws IOException {
        // 第一个数据决定了 source类型
        Type type = Type.values()[in.readByte()];
        switch (type) {
            case EMPTY_STORE: return EmptyStoreRecoverySource.INSTANCE;
            case EXISTING_STORE: return ExistingStoreRecoverySource.read(in);
            case PEER: return PeerRecoverySource.INSTANCE;
            case SNAPSHOT: return new SnapshotRecoverySource(in);
            case LOCAL_SHARDS: return LocalShardsRecoverySource.INSTANCE;
            default: throw new IllegalArgumentException("unknown recovery type: " + type.name());
        }
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) getType().ordinal());
        writeAdditionalFields(out);
    }

    /**
     * to be overridden by subclasses
     * 由子类进行覆盖 写入额外的field信息
     */
    protected void writeAdditionalFields(StreamOutput out) throws IOException {

    }

    public enum Type {
        EMPTY_STORE,
        EXISTING_STORE,
        PEER,
        SNAPSHOT,
        LOCAL_SHARDS
    }

    public abstract Type getType();

    public boolean shouldBootstrapNewHistoryUUID() {
        return false;
    }

    public boolean expectEmptyRetentionLeases() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RecoverySource that = (RecoverySource) o;

        return getType() == that.getType();
    }

    @Override
    public int hashCode() {
        return getType().hashCode();
    }

    /**
     * Recovery from a fresh copy
     * 空对象没有额外的field信息写入
     */
    public static final class EmptyStoreRecoverySource extends RecoverySource {
        public static final EmptyStoreRecoverySource INSTANCE = new EmptyStoreRecoverySource();

        @Override
        public Type getType() {
            return Type.EMPTY_STORE;
        }

        @Override
        public String toString() {
            return "new shard recovery";
        }
    }

    /**
     * Recovery from an existing on-disk store
     * 代表从磁盘恢复数据
     */
    public static final class ExistingStoreRecoverySource extends RecoverySource {
        /**
         * Special allocation id that shard has during initialization on allocate_stale_primary
         */
        public static final String FORCED_ALLOCATION_ID = "_forced_allocation_";

        /**
         * 单例模式
         */
        public static final ExistingStoreRecoverySource INSTANCE = new ExistingStoreRecoverySource(false);
        public static final ExistingStoreRecoverySource FORCE_STALE_PRIMARY_INSTANCE = new ExistingStoreRecoverySource(true);

        private final boolean bootstrapNewHistoryUUID;

        private ExistingStoreRecoverySource(boolean bootstrapNewHistoryUUID) {
            this.bootstrapNewHistoryUUID = bootstrapNewHistoryUUID;
        }

        private static ExistingStoreRecoverySource read(StreamInput in) throws IOException {
            return in.readBoolean() ? FORCE_STALE_PRIMARY_INSTANCE : INSTANCE;
        }

        /**
         * 额外写入一个  bootstrap_new_history_uuid
         * @param builder
         * @param params
         * @throws IOException
         */
        @Override
        public void addAdditionalFields(XContentBuilder builder, Params params) throws IOException {
            builder.field("bootstrap_new_history_uuid", bootstrapNewHistoryUUID);
        }

        @Override
        protected void writeAdditionalFields(StreamOutput out) throws IOException {
            out.writeBoolean(bootstrapNewHistoryUUID);
        }

        @Override
        public boolean shouldBootstrapNewHistoryUUID() {
            return bootstrapNewHistoryUUID;
        }

        @Override
        public Type getType() {
            return Type.EXISTING_STORE;
        }

        @Override
        public String toString() {
            return "existing store recovery; bootstrap_history_uuid=" + bootstrapNewHistoryUUID;
        }

        @Override
        public boolean expectEmptyRetentionLeases() {
            return bootstrapNewHistoryUUID;
        }
    }

    /**
     * recovery from other shards on same node (shrink index action)
     * 应该是这样  当前机器起了2个节点  他们同时存在某个分片  当其中一个分片数据丢失时 通过另一个分片数据进行恢复
     */
    public static class LocalShardsRecoverySource extends RecoverySource {

        public static final LocalShardsRecoverySource INSTANCE = new LocalShardsRecoverySource();

        private LocalShardsRecoverySource() {
        }

        @Override
        public Type getType() {
            return Type.LOCAL_SHARDS;
        }

        @Override
        public String toString() {
            return "local shards recovery";
        }

    }

    /**
     * recovery from a snapshot
     * 从快照中恢复数据  看来他跟redis这种类似  也是定期生成快照的
     */
    public static class SnapshotRecoverySource extends RecoverySource {

        /**
         * 每次申请恢复都会生成一个唯一id
         */
        private final String restoreUUID;
        /**
         * 快照对象主要就是存在一个仓库属性 以及一个 snapshotId 属性
         */
        private final Snapshot snapshot;
        private final IndexId index;
        private final Version version;

        public SnapshotRecoverySource(String restoreUUID, Snapshot snapshot, Version version, IndexId indexId) {
            this.restoreUUID = restoreUUID;
            this.snapshot = Objects.requireNonNull(snapshot);
            this.version = Objects.requireNonNull(version);
            this.index = Objects.requireNonNull(indexId);
        }

        SnapshotRecoverySource(StreamInput in) throws IOException {
            restoreUUID = in.readString();
            snapshot = new Snapshot(in);
            version = Version.readVersion(in);
            if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
                index = new IndexId(in);
            } else {
                index = new IndexId(in.readString(), IndexMetadata.INDEX_UUID_NA_VALUE);
            }
        }

        public String restoreUUID() {
            return restoreUUID;
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        /**
         * Gets the {@link IndexId} of the recovery source. May contain {@link IndexMetadata#INDEX_UUID_NA_VALUE} as the index uuid if it
         * was created by an older version master in a mixed version cluster.
         *
         * @return IndexId
         */
        public IndexId index() {
            return index;
        }

        public Version version() {
            return version;
        }

        @Override
        protected void writeAdditionalFields(StreamOutput out) throws IOException {
            out.writeString(restoreUUID);
            snapshot.writeTo(out);
            Version.writeVersion(version, out);
            if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
                index.writeTo(out);
            } else {
                out.writeString(index.getName());
            }
        }

        @Override
        public Type getType() {
            return Type.SNAPSHOT;
        }

        @Override
        public void addAdditionalFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.field("repository", snapshot.getRepository())
                .field("snapshot", snapshot.getSnapshotId().getName())
                .field("version", version.toString())
                .field("index", index.getName())
                .field("restoreUUID", restoreUUID);
        }

        @Override
        public String toString() {
            return "snapshot recovery [" + restoreUUID + "] from " + snapshot;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SnapshotRecoverySource that = (SnapshotRecoverySource) o;
            return restoreUUID.equals(that.restoreUUID) && snapshot.equals(that.snapshot)
                && index.equals(that.index) && version.equals(that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(restoreUUID, snapshot, index, version);
        }

    }

    /**
     * peer recovery from a primary shard
     * 代表从其他节点的分片上恢复数据
     */
    public static class PeerRecoverySource extends RecoverySource {

        public static final PeerRecoverySource INSTANCE = new PeerRecoverySource();

        private PeerRecoverySource() {
        }

        @Override
        public Type getType() {
            return Type.PEER;
        }

        @Override
        public String toString() {
            return "peer recovery";
        }

        @Override
        public boolean expectEmptyRetentionLeases() {
            return false;
        }
    }
}
