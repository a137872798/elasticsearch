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

package org.elasticsearch.index.seqno;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.gateway.WriteStateException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.SafeCommitInfo;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class is responsible for tracking the replication group with its progress and safety markers (local and global checkpoints).
 *
 * The global checkpoint is the highest sequence number for which all lower (or equal) sequence number have been processed
 * on all shards that are currently active. Since shards count as "active" when the master starts
 * them, and before this primary shard has been notified of this fact, we also include shards that have completed recovery. These shards
 * have received all old operations via the recovery mechanism and are kept up to date by the various replications actions. The set of
 * shards that are taken into account for the global checkpoint calculation are called the "in-sync shards".
 * <p>
 * The global checkpoint is maintained by the primary shard and is replicated to all the replicas (via {@link GlobalCheckpointSyncAction}).
 * 监控所有副本的对象
 */
public class ReplicationTracker extends AbstractIndexShardComponent implements LongSupplier {

    /**
     * The allocation ID for the shard to which this tracker is a component of.
     * 与 allocation 一一对应 并且一个allocation只能处理一个shard
     */
    final String shardAllocationId;

    /**
     * The global checkpoint tracker can operate in two modes:
     * - primary: this shard is in charge of collecting local checkpoint information from all shard copies and computing the global
     *            checkpoint based on the local checkpoints of all in-sync shard copies.
     * - replica: this shard receives global checkpoint information from the primary (see
     *   {@link #updateGlobalCheckpointOnReplica(long, String)}).
     *
     * When a shard is initialized (be it a primary or replica), it initially operates in replica mode. The global checkpoint tracker is
     * then switched to primary mode in the following three scenarios:
     *
     * - An initializing primary shard that is not a relocation target is moved to primary mode (using {@link #activatePrimaryMode}) once
     *   the shard becomes active.
     * - An active replica shard is moved to primary mode (using {@link #activatePrimaryMode}) once it is promoted to primary.
     * - A primary relocation target is moved to primary mode (using {@link #activateWithPrimaryContext}) during the primary relocation
     *   handoff. If the target shard is successfully initialized in primary mode, the source shard of a primary relocation is then moved
     *   to replica mode (using {@link #completeRelocationHandoff}), as the relocation target will be in charge of the global checkpoint
     *   computation from that point on.
     *   首先本分片是主分片  并且从init状态变成started状态后 该标识为true
     */
    volatile boolean primaryMode;

    /**
     * The current operation primary term. Management of this value is done through {@link IndexShard} and must only be done when safe. See
     * {@link #setOperationPrimaryTerm(long)}.
     */
    private volatile long operationPrimaryTerm;

    /**
     * Boolean flag that indicates if a relocation handoff is in progress. A handoff is started by calling
     * {@link #startRelocationHandoff(String)} and is finished by either calling {@link #completeRelocationHandoff} or
     * {@link #abortRelocationHandoff}, depending on whether the handoff was successful or not. During the handoff, which has as main
     * objective to transfer the internal state of the global checkpoint tracker from the relocation source to the target, the list of
     * in-sync shard copies cannot grow, otherwise the relocation target might miss this information and increase the global checkpoint
     * to eagerly. As consequence, some of the methods in this class are not allowed to be called while a handoff is in progress,
     * in particular {@link #markAllocationIdAsInSync}.
     *
     * A notable exception to this is the method {@link #updateFromMaster}, which is still allowed to be called during a relocation handoff.
     * The reason for this is that the handoff might fail and can be aborted (using {@link #abortRelocationHandoff}), in which case
     * it is important that the global checkpoint tracker does not miss any state updates that might happened during the handoff attempt.
     * This means, however, that the global checkpoint can still advance after the primary relocation handoff has been initiated, but only
     * because the master could have failed some of the in-sync shard copies and marked them as stale. That is ok though, as this
     * information is conveyed through cluster state updates, and the new primary relocation target will also eventually learn about those.
     * 代表开始进行重定向的处理了
     */
    boolean handoffInProgress;

    /**
     * Boolean flag that indicates whether a relocation handoff completed (see {@link #completeRelocationHandoff}).
     * 代表当前分片的重定向完成
     */
    volatile boolean relocated;

    /**
     * The global checkpoint tracker relies on the property that cluster state updates are applied in-order. After transferring a primary
     * context from the primary relocation source to the target and initializing the target, it is possible for the target to apply a
     * cluster state that is older than the one upon which the primary context was based. If we allowed this old cluster state
     * to influence the list of in-sync shard copies here, this could possibly remove such an in-sync copy from the internal structures
     * until the newer cluster state were to be applied, which would unsafely advance the global checkpoint. This field thus captures
     * the version of the last applied cluster state to ensure in-order updates.
     * 记录当前集群中的版本信息  当相关信息发生变化时 会更新版本号
     */
    long appliedClusterStateVersion;

    /**
     * 有关某个索引下所有分片的路由信息     同一分片应该是共用一个shardId
     */
    IndexShardRoutingTable routingTable;

    /**
     * Local checkpoint information for all shard copies that are tracked. Has an entry for all shard copies that are either initializing
     * and / or in-sync, possibly also containing information about unassigned in-sync shard copies. The information that is tracked for
     * each shard copy is explained in the docs for the {@link CheckpointState} class.
     * key: allocationId
     * value: 描述该分片(primary or replica) 的检查点信息
     */
    final Map<String, CheckpointState> checkpoints;

    /**
     * The current in-memory global checkpoint. In primary mode, this is a cached version of the checkpoint computed from the local
     * checkpoints. In replica mode, this is the in-memory global checkpoint that's communicated by the primary.
     * 每个副本都有自己持久化的进度 全局检查点就是这么多副本中最小的persistedCheckpoint
     */
    volatile long globalCheckpoint;

    /**
     * A callback invoked when the in-memory global checkpoint is updated. For primary mode this occurs if the computed global checkpoint
     * advances on the basis of state changes tracked here. For non-primary mode this occurs if the local knowledge of the global checkpoint
     * advances due to an update from the primary.
     */
    private final LongConsumer onGlobalCheckpointUpdated;

    /**
     * A supplier of the current time. This supplier is used to add a timestamp to retention leases, and to determine retention lease
     * expiration.
     */
    private final LongSupplier currentTimeMillisSupplier;

    /**
     * A callback when a new retention lease is created or an existing retention lease is removed. In practice, this callback invokes the
     * retention lease sync action, to sync retention leases to replicas.
     */
    private final BiConsumer<RetentionLeases, ActionListener<ReplicationResponse>> onSyncRetentionLeases;

    /**
     * This set contains allocation IDs for which there is a thread actively waiting for the local checkpoint to advance to at least the
     * current global checkpoint.
     * 只维护 还未同步完数据的分片
     */
    final Set<String> pendingInSync;

    /**
     * Cached value for the last replication group that was computed
     */
    volatile ReplicationGroup replicationGroup;

    /**
     * The current retention leases.
     * 本对象被初始化时 续约信息为空
     */
    private RetentionLeases retentionLeases = RetentionLeases.EMPTY;

    // -------  最近一次持久化的续约信息的 version/ primaryTerm  --------- //

    /**
     * The primary term of the most-recently persisted retention leases. This is used to check if we need to persist the current retention
     * leases.
     */
    private long persistedRetentionLeasesPrimaryTerm;

    /**
     * The version of the most-recently persisted retention leases. This is used to check if we need to persist the current retention
     * leases.
     */
    private long persistedRetentionLeasesVersion;

    // ------------------------------------------------------------------ //

    /**
     * Whether there should be a peer recovery retention lease (PRRL) for every tracked shard copy. Always true on indices created from
     * {@link Version#V_7_4_0} onwards, because these versions create PRRLs properly. May be false on indices created in an earlier version
     * if we recently did a rolling upgrade and {@link ReplicationTracker#createMissingPeerRecoveryRetentionLeases(ActionListener)} has not
     * yet completed. Is only permitted to change from false to true; can be removed once support for pre-PRRL indices is no longer needed.
     */
    private boolean hasAllPeerRecoveryRetentionLeases;

    /**
     * Supplies information about the current safe commit which may be used to expire peer-recovery retention leases.
     */
    private final Supplier<SafeCommitInfo> safeCommitInfoSupplier;

    /**
     * Threshold for expiring peer-recovery retention leases and falling back to file-based recovery. See
     * {@link IndexSettings#FILE_BASED_RECOVERY_THRESHOLD_SETTING}.
     */
    private final double fileBasedRecoveryThreshold;

    private final Consumer<ReplicationGroup> onReplicationGroupUpdated;

    /**
     * Get all retention leases tracked on this shard.
     *
     * @return the retention leases
     */
    public RetentionLeases getRetentionLeases() {
        return getRetentionLeases(false).v2();
    }

    /**
     * If the expire leases parameter is false, gets all retention leases tracked on this shard and otherwise first calculates
     * expiration of existing retention leases, and then gets all non-expired retention leases tracked on this shard. Note that only the
     * primary shard calculates which leases are expired, and if any have expired, syncs the retention leases to any replicas. If the
     * expire leases parameter is true, this replication tracker must be in primary mode.
     *
     * @param expireLeases 是否包含过期信息
     * @return a tuple indicating whether or not any retention leases were expired, and the non-expired retention leases
     * 获取续约信息
     */
    public synchronized Tuple<Boolean, RetentionLeases> getRetentionLeases(final boolean expireLeases) {
        // 不包含过期信息的情况 直接返回当前续约信息
        if (expireLeases == false) {
            return Tuple.tuple(false, retentionLeases);
        }
        assert primaryMode;
        // the primary calculates the non-expired retention leases and syncs them to replicas
        final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();
        final long retentionLeaseMillis = indexSettings.getRetentionLeaseMillis();
        // 一组续约id
        final Set<String> leaseIdsForCurrentPeers
            = routingTable.assignedShards().stream().map(ReplicationTracker::getPeerRecoveryRetentionLeaseId).collect(Collectors.toSet());
        final boolean allShardsStarted = routingTable.allShardsStarted();
        // TODO
        final long minimumReasonableRetainedSeqNo = allShardsStarted ? 0L : getMinimumReasonableRetainedSeqNo();
        final Map<Boolean, List<RetentionLease>> partitionByExpiration = retentionLeases
                .leases()
                .stream()
                .collect(Collectors.groupingBy(lease -> {
                    if (lease.source().equals(PEER_RECOVERY_RETENTION_LEASE_SOURCE)) {
                        if (leaseIdsForCurrentPeers.contains(lease.id())) {
                            return false;
                        }
                        if (allShardsStarted) {
                            logger.trace("expiring unused [{}]", lease);
                            return true;
                        }
                        if (lease.retainingSequenceNumber() < minimumReasonableRetainedSeqNo) {
                            logger.trace("expiring unreasonable [{}] retaining history before [{}]", lease, minimumReasonableRetainedSeqNo);
                            return true;
                        }
                    }
                    return currentTimeMillis - lease.timestamp() > retentionLeaseMillis;
                }));
        final Collection<RetentionLease> expiredLeases = partitionByExpiration.get(true);
        if (expiredLeases == null) {
            // early out as no retention leases have expired
            logger.debug("no retention leases are expired from current retention leases [{}]", retentionLeases);
            return Tuple.tuple(false, retentionLeases);
        }
        final Collection<RetentionLease> nonExpiredLeases =
                partitionByExpiration.get(false) != null ? partitionByExpiration.get(false) : Collections.emptyList();
        logger.debug("expiring retention leases [{}] from current retention leases [{}]", expiredLeases, retentionLeases);
        retentionLeases = new RetentionLeases(operationPrimaryTerm, retentionLeases.version() + 1, nonExpiredLeases);
        return Tuple.tuple(true, retentionLeases);
    }

    private long getMinimumReasonableRetainedSeqNo() {
        final SafeCommitInfo safeCommitInfo = safeCommitInfoSupplier.get();
        return safeCommitInfo.localCheckpoint + 1 - Math.round(Math.ceil(safeCommitInfo.docCount * fileBasedRecoveryThreshold));
        // NB safeCommitInfo.docCount is a very low-level count of the docs in the index, and in particular if this shard contains nested
        // docs then safeCommitInfo.docCount counts every child doc separately from the parent doc. However every part of a nested document
        // has the same seqno, so we may be overestimating the cost of a file-based recovery when compared to an ops-based recovery and
        // therefore preferring ops-based recoveries inappropriately in this case. Correctly accounting for nested docs seems difficult to
        // do cheaply, and the circumstances in which this matters should be relatively rare, so we use this naive calculation regardless.
        // TODO improve this measure for when nested docs are in use
    }

    /**
     * Adds a new retention lease.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number
     * @param source                  the source of the retention lease
     * @param listener                the callback when the retention lease is successfully added and synced to replicas
     * @return the new retention lease
     * @throws RetentionLeaseAlreadyExistsException if the specified retention lease already exists
     * 插入一个新的续约对象
     */
    public RetentionLease addRetentionLease(
            final String id,
            final long retainingSequenceNumber,
            final String source,
            final ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(listener);
        final RetentionLease retentionLease;
        final RetentionLeases currentRetentionLeases;
        synchronized (this) {
            retentionLease = innerAddRetentionLease(id, retainingSequenceNumber, source);
            currentRetentionLeases = retentionLeases;
        }

        // 每当续约信息发生变化 需要同步到该分片的其他副本
        onSyncRetentionLeases.accept(currentRetentionLeases, listener);
        return retentionLease;
    }

    /**
     * Atomically clones an existing retention lease to a new ID.
     *
     * @param sourceLeaseId the identifier of the source retention lease         通过主分片节点生成的续约id
     * @param targetLeaseId the identifier of the retention lease to create      通过目标节点生成的续约id
     * @param listener      the callback when the retention lease is successfully added and synced to replicas
     * @return the new retention lease
     * @throws RetentionLeaseNotFoundException      if the specified source retention lease does not exist
     * @throws RetentionLeaseAlreadyExistsException if the specified target retention lease already exists
     * 生成一个续约对象的副本
     */
    RetentionLease cloneRetentionLease(String sourceLeaseId, String targetLeaseId, ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(listener);
        final RetentionLease retentionLease;
        final RetentionLeases currentRetentionLeases;
        synchronized (this) {
            // 只有主分片才应该调用该方法
            assert primaryMode;
            // 获取主分片的续约信息
            if (getRetentionLeases().contains(sourceLeaseId) == false) {
                throw new RetentionLeaseNotFoundException(sourceLeaseId);
            }
            final RetentionLease sourceLease = getRetentionLeases().get(sourceLeaseId);
            // 记得 不在in-sync的分片是不会自动创建续约对象的  而当它们向primary发起 recovery请求时 在完成了 索引文件数据的发送后 会生成续约对象
            retentionLease = innerAddRetentionLease(targetLeaseId, sourceLease.retainingSequenceNumber(), sourceLease.source());
            currentRetentionLeases = retentionLeases;
        }

        // Syncing here may not be strictly necessary, because this new lease isn't retaining any extra history that wasn't previously
        // retained by the source lease; however we prefer to sync anyway since we expect to do so whenever creating a new lease.
        // 此时还是要将续约信息同步到其他节点上
        onSyncRetentionLeases.accept(currentRetentionLeases, listener);
        return retentionLease;
    }

    /**
     * Adds a new retention lease, but does not synchronise it with the rest of the replication group.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number   对应一个seqNo
     * @param source                  the source of the retention lease   描述信息
     * @return the new retention lease
     * @throws RetentionLeaseAlreadyExistsException if the specified retention lease already exists
     * 插入一个新的续约对象
     */
    private RetentionLease innerAddRetentionLease(String id, long retainingSequenceNumber, String source) {
        assert Thread.holdsLock(this);
        assert primaryMode : id + "/" + retainingSequenceNumber + "/" + source;
        if (retentionLeases.contains(id)) {
            throw new RetentionLeaseAlreadyExistsException(id);
        }

        // 生成一个续约信息 插入到之前的续约队列中
        final RetentionLease retentionLease
            = new RetentionLease(id, retainingSequenceNumber, currentTimeMillisSupplier.getAsLong(), source);
        logger.debug("adding new retention lease [{}] to current retention leases [{}]", retentionLease, retentionLeases);
        retentionLeases = new RetentionLeases(
                operationPrimaryTerm,
                // 因为这里version +1   代表续约信息发生更新了  就可以正常进行 persistRetentionLeases
                retentionLeases.version() + 1,
                Stream.concat(retentionLeases.leases().stream(), Stream.of(retentionLease)).collect(Collectors.toList()));
        return retentionLease;
    }

    /**
     * Renews an existing retention lease.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number
     * @param source                  the source of the retention lease
     * @return the renewed retention lease
     * @throws RetentionLeaseNotFoundException              if the specified retention lease does not exist
     * @throws RetentionLeaseInvalidRetainingSeqNoException if the new retaining sequence number is lower than
     *                                                      the retaining sequence number of the current retention lease.
     *                                                      更新某个续约对象的数据
     */
    public synchronized RetentionLease renewRetentionLease(final String id, final long retainingSequenceNumber, final String source) {
        assert primaryMode;
        final RetentionLease existingRetentionLease = retentionLeases.get(id);
        if (existingRetentionLease == null) {
            throw new RetentionLeaseNotFoundException(id);
        }
        if (retainingSequenceNumber < existingRetentionLease.retainingSequenceNumber()) {
            assert PEER_RECOVERY_RETENTION_LEASE_SOURCE.equals(source) == false :
                "renewing peer recovery retention lease [" + existingRetentionLease + "]" +
                    " with a lower retaining sequence number [" + retainingSequenceNumber + "]";
            throw new RetentionLeaseInvalidRetainingSeqNoException(id, source, retainingSequenceNumber, existingRetentionLease);
        }
        final RetentionLease retentionLease =
            new RetentionLease(id, retainingSequenceNumber, currentTimeMillisSupplier.getAsLong(), source);
        retentionLeases = new RetentionLeases(
                operationPrimaryTerm,
                retentionLeases.version() + 1,
                Stream.concat(
                        retentionLeases.leases().stream().filter(lease -> lease.id().equals(id) == false),
                        Stream.of(retentionLease))
                        .collect(Collectors.toList()));
        return retentionLease;
    }

    /**
     * Removes an existing retention lease.
     *
     * @param id       the identifier of the retention lease
     * @param listener the callback when the retention lease is successfully removed and synced to replicas
     *                 移除某个续约对象  这里也会涉及到在shardId 对应的所有分片同步续约信息
     */
    public void removeRetentionLease(final String id, final ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(listener);
        final RetentionLeases currentRetentionLeases;
        synchronized (this) {
            assert primaryMode;
            if (retentionLeases.contains(id) == false) {
                throw new RetentionLeaseNotFoundException(id);
            }
            logger.debug("removing retention lease [{}] from current retention leases [{}]", id, retentionLeases);
            retentionLeases = new RetentionLeases(
                    operationPrimaryTerm,
                    retentionLeases.version() + 1,
                    retentionLeases.leases().stream().filter(lease -> lease.id().equals(id) == false).collect(Collectors.toList()));
            currentRetentionLeases = retentionLeases;
        }
        // 数据同步完成时会触发监听器
        onSyncRetentionLeases.accept(currentRetentionLeases, listener);
    }

    /**
     * Updates retention leases on a replica.
     *
     * @param retentionLeases the retention leases
     *                        在副本上更新续约信息   一般就是主分片先修改续约信息 在发起请求同步到所有副本上
     */
    public synchronized void updateRetentionLeasesOnReplica(final RetentionLeases retentionLeases) {
        assert primaryMode == false;
        if (retentionLeases.supersedes(this.retentionLeases)) {
            this.retentionLeases = retentionLeases;
        }
    }

    /**
     * Loads the latest retention leases from their dedicated state file.
     *
     * @param path the path to the directory containing the state file  相关文件的存储路径
     * @return the retention leases
     * @throws IOException if an I/O exception occurs reading the retention leases
     * 加载之前写入到磁盘的续约信息
     */
    public RetentionLeases loadRetentionLeases(final Path path) throws IOException {
        final RetentionLeases retentionLeases;
        synchronized (retentionLeasePersistenceLock) {
            retentionLeases = RetentionLeases.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, path);
        }

        // TODO after backporting we expect this never to happen in 8.x, so adjust this to throw an exception instead.
        assert Version.CURRENT.major <= 8 : "throw an exception instead of returning EMPTY on null";
        if (retentionLeases == null) {
            return RetentionLeases.EMPTY;
        }
        return retentionLeases;
    }

    private final Object retentionLeasePersistenceLock = new Object();

    /**
     * Persists the current retention leases to their dedicated state file. If this version of the retention leases are already persisted
     * then persistence is skipped.
     *
     * @param path the path to the directory containing the state file
     * @throws WriteStateException if an exception occurs writing the state file
     * 每次更新续约信息后 会发起一个同步续约信息的请求 首先就交由primary处理  就是指持久化续约信息
     */
    public void persistRetentionLeases(final Path path) throws WriteStateException {
        synchronized (retentionLeasePersistenceLock) {
            final RetentionLeases currentRetentionLeases;
            synchronized (this) {
                // 确保此时续约对象的相关信息比之前持久化的要新
                // 在初始化阶段 persistedRetentionLeasesPrimaryTerm/persistedRetentionLeasesVersion 都是0
                // 但是 retentionLeases.Empty 的term为1  也是就初始化该对象后可以直接调用该方法
                if (retentionLeases.supersedes(persistedRetentionLeasesPrimaryTerm, persistedRetentionLeasesVersion) == false) {
                    logger.trace("skipping persisting retention leases [{}], already persisted", retentionLeases);
                    return;
                }
                currentRetentionLeases = retentionLeases;
            }
            logger.trace("persisting retention leases [{}]", currentRetentionLeases);
            // 将续约信息写入到分片目录下
            RetentionLeases.FORMAT.writeAndCleanup(currentRetentionLeases, path);
            // 因为持久化成功  更新persistedXXX的值
            persistedRetentionLeasesPrimaryTerm = currentRetentionLeases.primaryTerm();
            persistedRetentionLeasesVersion = currentRetentionLeases.version();
        }
    }

    public boolean assertRetentionLeasesPersisted(final Path path) throws IOException {
        assert RetentionLeases.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, path) != null;
        return true;
    }


    /**
     * Retention leases for peer recovery have source {@link ReplicationTracker#PEER_RECOVERY_RETENTION_LEASE_SOURCE}, a lease ID
     * containing the persistent node ID calculated by {@link ReplicationTracker#getPeerRecoveryRetentionLeaseId}, and retain operations
     * with sequence numbers strictly greater than the given global checkpoint.
     */
    public RetentionLease addPeerRecoveryRetentionLease(String nodeId, long globalCheckpoint,
                                                        ActionListener<ReplicationResponse> listener) {
        return addRetentionLease(getPeerRecoveryRetentionLeaseId(nodeId), globalCheckpoint + 1,
            PEER_RECOVERY_RETENTION_LEASE_SOURCE, listener);
    }

    /**
     * 根据相关信息生成续约对象
     * @param nodeId
     * @param listener
     * @return
     */
    public RetentionLease cloneLocalPeerRecoveryRetentionLease(String nodeId, ActionListener<ReplicationResponse> listener) {
        return cloneRetentionLease(
            // 生成主分片相关的续约id
            getPeerRecoveryRetentionLeaseId(routingTable.primaryShard()),
            // 目标节点对应的续约id
            getPeerRecoveryRetentionLeaseId(nodeId), listener);
    }

    /**
     * 移除某个之前存在的续约信息
     * @param nodeId
     * @param listener
     */
    public void removePeerRecoveryRetentionLease(String nodeId, ActionListener<ReplicationResponse> listener) {
        removeRetentionLease(getPeerRecoveryRetentionLeaseId(nodeId), listener);
    }

    /**
     * Source for peer recovery retention leases; see {@link ReplicationTracker#addPeerRecoveryRetentionLease}.
     */
    public static final String PEER_RECOVERY_RETENTION_LEASE_SOURCE = "peer recovery";

    /**
     * Id for a peer recovery retention lease for the given node. See {@link ReplicationTracker#addPeerRecoveryRetentionLease}.
     */
    public static String getPeerRecoveryRetentionLeaseId(String nodeId) {
        return "peer_recovery/" + nodeId;
    }

    /**
     * Id for a peer recovery retention lease for the given {@link ShardRouting}.
     * See {@link ReplicationTracker#addPeerRecoveryRetentionLease}.
     * 通过某个分片所在的节点获取续约信息
     */
    public static String getPeerRecoveryRetentionLeaseId(ShardRouting shardRouting) {
        return getPeerRecoveryRetentionLeaseId(shardRouting.currentNodeId());
    }

    /**
     * Returns a list of peer recovery retention leases installed in this replication group
     */
    public List<RetentionLease> getPeerRecoveryRetentionLeases() {
        return getRetentionLeases().leases().stream()
            .filter(lease -> PEER_RECOVERY_RETENTION_LEASE_SOURCE.equals(lease.source()))
            .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Advance the peer-recovery retention leases for all assigned shard copies to discard history below the corresponding global
     * checkpoint, and renew any leases that are approaching expiry.
     * 更新续约对象
     */
    public synchronized void renewPeerRecoveryRetentionLeases() {
        assert primaryMode;
        assert invariant();

        /*
         * Peer-recovery retention leases never expire while the associated shard is assigned, but we must still renew them occasionally in
         * case the associated shard is temporarily unassigned. However we must not renew them too often, since each renewal must be
         * persisted and the resulting IO can be expensive on nodes with large numbers of shards (see #42299). We choose to renew them after
         * half the expiry time, so that by default the cluster has at least 6 hours to recover before these leases start to expire.
         * 代表每隔多久要更新一次吧
         */
        final long renewalTimeMillis = currentTimeMillisSupplier.getAsLong() - indexSettings.getRetentionLeaseMillis() / 2;

        /*
         * If any of the peer-recovery retention leases need renewal, it's a good opportunity to renew them all.
         * 检测是否需要更新
         */
        final boolean renewalNeeded = StreamSupport.stream(routingTable.spliterator(), false).filter(ShardRouting::assignedToNode)
            .anyMatch(shardRouting -> {
                // 获取某个allocation 对应的续约对象
                final RetentionLease retentionLease = retentionLeases.get(getPeerRecoveryRetentionLeaseId(shardRouting));
                if (retentionLease == null) {
                    /*
                     * If this shard copy is tracked then we got here here via a rolling upgrade from an older version that doesn't
                     * create peer recovery retention leases for every shard copy.
                     */
                    assert checkpoints.get(shardRouting.allocationId().getId()).tracked == false
                        || hasAllPeerRecoveryRetentionLeases == false;
                    return false;
                }
                // 续约对象的时间戳 小于需要续约的时间  或者号码小于global
                return retentionLease.timestamp() <= renewalTimeMillis
                    || retentionLease.retainingSequenceNumber() <= checkpoints.get(shardRouting.allocationId().getId()).globalCheckpoint;
            });

        if (renewalNeeded) {
            for (ShardRouting shardRouting : routingTable) {
                if (shardRouting.assignedToNode()) {
                    final RetentionLease retentionLease = retentionLeases.get(getPeerRecoveryRetentionLeaseId(shardRouting));
                    if (retentionLease != null) {
                        final CheckpointState checkpointState = checkpoints.get(shardRouting.allocationId().getId());
                        final long newRetainedSequenceNumber = Math.max(0L, checkpointState.globalCheckpoint + 1L);
                        if (retentionLease.retainingSequenceNumber() <= newRetainedSequenceNumber) {
                            renewRetentionLease(getPeerRecoveryRetentionLeaseId(shardRouting), newRetainedSequenceNumber,
                                PEER_RECOVERY_RETENTION_LEASE_SOURCE);
                        } else {
                            // the retention lease is tied to the node, not the shard copy, so it's possible a copy was removed and now
                            // we are in the process of recovering it again, or maybe we were just promoted and have not yet received the
                            // global checkpoints from our peers.
                            assert checkpointState.globalCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO :
                                "cannot renew " + retentionLease + " according to " + checkpointState + " for " + shardRouting;
                        }
                    }
                }
            }
        }

        assert invariant();
    }

    /**
     * 描述某个分片当前检查点信息
     */
    public static class CheckpointState implements Writeable {

        /**
         * the last local checkpoint information that we have for this shard. All operations up to this point are properly fsynced to disk.
         * 该分片此时本地检查点
         */
        long localCheckpoint;

        /**
         * the last global checkpoint information that we have for this shard. This is the global checkpoint that's fsynced to disk on the
         * respective shard, and all operations up to this point are properly fsynced to disk as well.
         * 该分片此时的全局检查点
         */
        long globalCheckpoint;
        /**
         * whether this shard is treated as in-sync and thus contributes to the global checkpoint calculation
         * 代表某个副本此时已经持久化的全局检查点 与主分片的全局检查点一致
         */
        boolean inSync;

        /**
         * whether this shard is tracked in the replication group, i.e., should receive document updates from the primary.
         */
        boolean tracked;

        /**
         *
         * @param localCheckpoint
         * @param globalCheckpoint
         * @param inSync  是否已经与主分片完成同步
         * @param tracked 在进行索引操作时  tracked为true的副本也会执行索引操作
         */
        public CheckpointState(long localCheckpoint, long globalCheckpoint, boolean inSync, boolean tracked) {
            this.localCheckpoint = localCheckpoint;
            this.globalCheckpoint = globalCheckpoint;
            this.inSync = inSync;
            this.tracked = tracked;
        }

        public CheckpointState(StreamInput in) throws IOException {
            this.localCheckpoint = in.readZLong();
            this.globalCheckpoint = in.readZLong();
            this.inSync = in.readBoolean();
            this.tracked = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(localCheckpoint);
            out.writeZLong(globalCheckpoint);
            out.writeBoolean(inSync);
            out.writeBoolean(tracked);
        }

        /**
         * Returns a full copy of this object
         */
        public CheckpointState copy() {
            return new CheckpointState(localCheckpoint, globalCheckpoint, inSync, tracked);
        }

        public long getLocalCheckpoint() {
            return localCheckpoint;
        }

        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public String toString() {
            return "LocalCheckpointState{" +
                "localCheckpoint=" + localCheckpoint +
                ", globalCheckpoint=" + globalCheckpoint +
                ", inSync=" + inSync +
                ", tracked=" + tracked +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CheckpointState that = (CheckpointState) o;

            if (localCheckpoint != that.localCheckpoint) return false;
            if (globalCheckpoint != that.globalCheckpoint) return false;
            if (inSync != that.inSync) return false;
            return tracked == that.tracked;
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(localCheckpoint);
            result = 31 * result + Long.hashCode(globalCheckpoint);
            result = 31 * result + Boolean.hashCode(inSync);
            result = 31 * result + Boolean.hashCode(tracked);
            return result;
        }
    }

    /**
     * Get the local knowledge of the persisted global checkpoints for all in-sync allocation IDs.
     *
     * @return a map from allocation ID to the local knowledge of the persisted global checkpoint for that allocation ID
     * inSync 为true  代表该分片的persistedCheckpoint 已经超过了 全局检查点
     */
    public synchronized ObjectLongMap<String> getInSyncGlobalCheckpoints() {
        assert primaryMode;
        assert handoffInProgress == false;
        final ObjectLongMap<String> globalCheckpoints = new ObjectLongHashMap<>(checkpoints.size()); // upper bound on the size
        checkpoints
                .entrySet()
                .stream()
                .filter(e -> e.getValue().inSync)
                .forEach(e -> globalCheckpoints.put(e.getKey(), e.getValue().globalCheckpoint));
        return globalCheckpoints;
    }

    /**
     * Returns whether the replication tracker is in primary mode, i.e., whether the current shard is acting as primary from the point of
     * view of replication.
     */
    public boolean isPrimaryMode() {
        return primaryMode;
    }

    /**
     * Returns the current operation primary term.
     *
     * @return the primary term
     * 获取主分片当前的任期
     */
    public long getOperationPrimaryTerm() {
        return operationPrimaryTerm;
    }

    /**
     * Sets the current operation primary term. This method should be invoked only when no other operations are possible on the shard. That
     * is, either from the constructor of {@link IndexShard} or while holding all permits on the {@link IndexShard} instance.
     *
     * @param operationPrimaryTerm the new operation primary term
     */
    public void setOperationPrimaryTerm(final long operationPrimaryTerm) {
        this.operationPrimaryTerm = operationPrimaryTerm;
    }

    /**
     * Returns whether the replication tracker has relocated away to another shard copy.
     */
    public boolean isRelocated() {
        return relocated;
    }

    /**
     * Class invariant that should hold before and after every invocation of public methods on this class. As Java lacks implication
     * as a logical operator, many of the invariants are written under the form (!A || B), they should be read as (A implies B) however.
     */
    private boolean invariant() {
        // local checkpoints only set during primary mode
        assert primaryMode || checkpoints.values().stream().allMatch(lcps -> lcps.localCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO);

        // global checkpoints only set during primary mode
        assert primaryMode || checkpoints.values().stream().allMatch(cps -> cps.globalCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO);

        // relocation handoff can only occur in primary mode
        assert !handoffInProgress || primaryMode;

        // a relocated copy is not in primary mode
        assert !relocated || !primaryMode;

        // the current shard is marked as in-sync when the global checkpoint tracker operates in primary mode
        assert !primaryMode || checkpoints.get(shardAllocationId).inSync;

        // the routing table and replication group is set when the global checkpoint tracker operates in primary mode
        assert !primaryMode || (routingTable != null && replicationGroup != null) :
            "primary mode but routing table is " + routingTable + " and replication group is " + replicationGroup;

        // when in primary mode, the current allocation ID is the allocation ID of the primary or the relocation allocation ID
        assert !primaryMode
                || (routingTable.primaryShard().allocationId().getId().equals(shardAllocationId)
                || routingTable.primaryShard().allocationId().getRelocationId().equals(shardAllocationId));

        // during relocation handoff there are no entries blocking global checkpoint advancement
        assert !handoffInProgress || pendingInSync.isEmpty() :
            "entries blocking global checkpoint advancement during relocation handoff: " + pendingInSync;

        // entries blocking global checkpoint advancement can only exist in primary mode and when not having a relocation handoff
        assert pendingInSync.isEmpty() || (primaryMode && !handoffInProgress);

        // the computed global checkpoint is always up-to-date
        assert !primaryMode
                || globalCheckpoint == computeGlobalCheckpoint(pendingInSync, checkpoints.values(), globalCheckpoint)
                : "global checkpoint is not up-to-date, expected: " +
                computeGlobalCheckpoint(pendingInSync, checkpoints.values(), globalCheckpoint) + " but was: " + globalCheckpoint;

        // when in primary mode, the global checkpoint is at most the minimum local checkpoint on all in-sync shard copies
        assert !primaryMode
                || globalCheckpoint <= inSyncCheckpointStates(checkpoints, CheckpointState::getLocalCheckpoint, LongStream::min)
                : "global checkpoint [" + globalCheckpoint + "] "
                + "for primary mode allocation ID [" + shardAllocationId + "] "
                + "more than in-sync local checkpoints [" + checkpoints + "]";

        // we have a routing table iff we have a replication group
        assert (routingTable == null) == (replicationGroup == null) :
            "routing table is " + routingTable + " but replication group is " + replicationGroup;

        assert replicationGroup == null || replicationGroup.equals(calculateReplicationGroup()) :
            "cached replication group out of sync: expected: " + calculateReplicationGroup() + " but was: " + replicationGroup;

        // all assigned shards from the routing table are tracked
        assert routingTable == null || checkpoints.keySet().containsAll(routingTable.getAllAllocationIds()) :
            "local checkpoints " + checkpoints + " not in-sync with routing table " + routingTable;

        for (Map.Entry<String, CheckpointState> entry : checkpoints.entrySet()) {
            // blocking global checkpoint advancement only happens for shards that are not in-sync
            assert !pendingInSync.contains(entry.getKey()) || !entry.getValue().inSync :
                "shard copy " + entry.getKey() + " blocks global checkpoint advancement but is in-sync";
            // in-sync shard copies are tracked
            assert !entry.getValue().inSync || entry.getValue().tracked :
                "shard copy " + entry.getKey() + " is in-sync but not tracked";
        }

        // all pending in sync shards are tracked
        for (String aId : pendingInSync) {
            assert checkpoints.get(aId) != null : "aId [" + aId + "] is pending in sync but isn't tracked";
        }

        if (primaryMode && indexSettings.isSoftDeleteEnabled() && hasAllPeerRecoveryRetentionLeases) {
            // all tracked shard copies have a corresponding peer-recovery retention lease
            for (final ShardRouting shardRouting : routingTable.assignedShards()) {
                if (checkpoints.get(shardRouting.allocationId().getId()).tracked) {
                    assert retentionLeases.contains(getPeerRecoveryRetentionLeaseId(shardRouting))
                        : "no retention lease for tracked shard [" + shardRouting + "] in " + retentionLeases;
                    assert PEER_RECOVERY_RETENTION_LEASE_SOURCE.equals(
                        retentionLeases.get(getPeerRecoveryRetentionLeaseId(shardRouting)).source())
                        : "incorrect source [" + retentionLeases.get(getPeerRecoveryRetentionLeaseId(shardRouting)).source()
                        + "] for [" + shardRouting + "] in " + retentionLeases;
                }
            }
        }

        return true;
    }

    private static long inSyncCheckpointStates(
            final Map<String, CheckpointState> checkpoints,
            ToLongFunction<CheckpointState> function,
            Function<LongStream, OptionalLong> reducer) {
        final OptionalLong value =
                reducer.apply(
                        checkpoints
                                .values()
                                .stream()
                                .filter(cps -> cps.inSync)
                                .mapToLong(function)
                                .filter(v -> v != SequenceNumbers.UNASSIGNED_SEQ_NO));
        return value.isPresent() ? value.getAsLong() : SequenceNumbers.UNASSIGNED_SEQ_NO;
    }

    /**
     * 初始化副本追踪对象
     * @param shardId  本次针对的分片
     * @param allocationId  分配该分片的allocation的id
     * @param indexSettings
     * @param operationPrimaryTerm
     * @param globalCheckpoint    当创建IndexShard时 传入的 globalCheckpoint为 -2
     * @param onGlobalCheckpointUpdated
     * @param currentTimeMillisSupplier
     * @param onSyncRetentionLeases
     * @param safeCommitInfoSupplier
     */
    public ReplicationTracker(
        final ShardId shardId,
        final String allocationId,
        final IndexSettings indexSettings,
        final long operationPrimaryTerm,
        final long globalCheckpoint,
        final LongConsumer onGlobalCheckpointUpdated,
        final LongSupplier currentTimeMillisSupplier,
        final BiConsumer<RetentionLeases, ActionListener<ReplicationResponse>> onSyncRetentionLeases,
        final Supplier<SafeCommitInfo> safeCommitInfoSupplier) {
        this(shardId, allocationId, indexSettings, operationPrimaryTerm, globalCheckpoint, onGlobalCheckpointUpdated,
            currentTimeMillisSupplier, onSyncRetentionLeases, safeCommitInfoSupplier, x -> {});
    }

    /**
     * Initialize the global checkpoint service. The specified global checkpoint should be set to the last known global checkpoint, or
     * {@link SequenceNumbers#UNASSIGNED_SEQ_NO}.
     *
     * @param shardId                   the shard ID
     * @param allocationId              the allocation ID
     * @param indexSettings             the index settings
     * @param operationPrimaryTerm      the current primary term
     * @param globalCheckpoint          the last known global checkpoint for this shard, or {@link SequenceNumbers#UNASSIGNED_SEQ_NO}
     * @param onSyncRetentionLeases     a callback when a new retention lease is created or an existing retention lease expires
     * @param onReplicationGroupUpdated a callback when the replica group changes
     */
    public ReplicationTracker(
            final ShardId shardId,
            final String allocationId,
            final IndexSettings indexSettings,
            final long operationPrimaryTerm,
            final long globalCheckpoint,
            final LongConsumer onGlobalCheckpointUpdated,  // 当更新全局检查点时  通过该函数来处理
            final LongSupplier currentTimeMillisSupplier,
            final BiConsumer<RetentionLeases, ActionListener<ReplicationResponse>> onSyncRetentionLeases,  // 当进行续约时 会触发该函数
            final Supplier<SafeCommitInfo> safeCommitInfoSupplier,   // 获取安全提交点信息
            final Consumer<ReplicationGroup> onReplicationGroupUpdated  // 当复制组发生变化时 触发该函数
    ) {
        super(shardId, indexSettings);
        assert globalCheckpoint >= SequenceNumbers.UNASSIGNED_SEQ_NO : "illegal initial global checkpoint: " + globalCheckpoint;
        this.shardAllocationId = allocationId;
        this.primaryMode = false;
        this.operationPrimaryTerm = operationPrimaryTerm;
        this.handoffInProgress = false;
        this.appliedClusterStateVersion = -1L;
        this.globalCheckpoint = globalCheckpoint;
        this.checkpoints = new HashMap<>(1 + indexSettings.getNumberOfReplicas());
        this.onGlobalCheckpointUpdated = Objects.requireNonNull(onGlobalCheckpointUpdated);
        this.currentTimeMillisSupplier = Objects.requireNonNull(currentTimeMillisSupplier);
        this.onSyncRetentionLeases = Objects.requireNonNull(onSyncRetentionLeases);
        this.pendingInSync = new HashSet<>();
        this.routingTable = null;
        this.replicationGroup = null;

        // 排除非兼容性的部分 该值默认为true
        this.hasAllPeerRecoveryRetentionLeases = indexSettings.getIndexVersionCreated().onOrAfter(Version.V_7_6_0)
            || (indexSettings.isSoftDeleteEnabled() &&
                indexSettings.getIndexVersionCreated().onOrAfter(Version.V_7_4_0) &&
                indexSettings.getIndexMetadata().getState() == IndexMetadata.State.OPEN);

        this.fileBasedRecoveryThreshold = IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.get(indexSettings.getSettings());
        this.safeCommitInfoSupplier = safeCommitInfoSupplier;
        this.onReplicationGroupUpdated = onReplicationGroupUpdated;
        assert Version.V_EMPTY.equals(indexSettings.getIndexVersionCreated()) == false;
        assert invariant();
    }

    /**
     * Returns the current replication group for the shard.
     *
     * @return the replication group
     */
    public ReplicationGroup getReplicationGroup() {
        assert primaryMode;
        return replicationGroup;
    }

    /**
     * 根据当前该分片的所有副本信息 更新replicationGroup
     */
    private void updateReplicationGroupAndNotify() {
        assert Thread.holdsLock(this);
        ReplicationGroup newReplicationGroup = calculateReplicationGroup();
        replicationGroup = newReplicationGroup;
        onReplicationGroupUpdated.accept(newReplicationGroup);
    }

    /**
     * 根据当前所有 allocation的同步状态生成 replicationGroup对象
     * 如果是更新操作 每次version+1
     * @return
     */
    private ReplicationGroup calculateReplicationGroup() {
        long newVersion;
        if (replicationGroup == null) {
            newVersion = 0;
        } else {
            newVersion = replicationGroup.getVersion() + 1;
        }
        return new ReplicationGroup(routingTable,
            checkpoints.entrySet().stream().filter(e -> e.getValue().inSync).map(Map.Entry::getKey).collect(Collectors.toSet()),
            checkpoints.entrySet().stream().filter(e -> e.getValue().tracked).map(Map.Entry::getKey).collect(Collectors.toSet()),
            newVersion);
    }

    /**
     * Returns the in-memory global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public long getGlobalCheckpoint() {
        return globalCheckpoint;
    }

    /**
     * 通过该对象可以获取到全局检查点
     * @return
     */
    @Override
    public long getAsLong() {
        return globalCheckpoint;
    }

    /**
     * Updates the global checkpoint on a replica shard after it has been updated by the primary.
     *
     * @param newGlobalCheckpoint the new global checkpoint
     * @param reason              the reason the global checkpoint was updated
     *                            在数据恢复阶段 会从索引文件的translogUUID 中找到索引文件 并尝试加载全局检查点
     */
    public synchronized void updateGlobalCheckpointOnReplica(final long newGlobalCheckpoint, final String reason) {
        assert invariant();
        assert primaryMode == false;
        /*
         * The global checkpoint here is a local knowledge which is updated under the mandate of the primary. It can happen that the primary
         * information is lagging compared to a replica (e.g., if a replica is promoted to primary but has stale info relative to other
         * replica shards). In these cases, the local knowledge of the global checkpoint could be higher than the sync from the lagging
         * primary.
         * 在本对象被初始化时  使用的全局检查点默认为-2 (UNASSIGNED_SEQ_NO)
         */
        final long previousGlobalCheckpoint = globalCheckpoint;
        if (newGlobalCheckpoint > previousGlobalCheckpoint) {
            globalCheckpoint = newGlobalCheckpoint;
            logger.trace("updated global checkpoint from [{}] to [{}] due to [{}]", previousGlobalCheckpoint, globalCheckpoint, reason);
            onGlobalCheckpointUpdated.accept(globalCheckpoint);
        }
        assert invariant();
    }

    /**
     * Update the local knowledge of the persisted global checkpoint for the specified allocation ID.
     *
     * @param allocationId     the allocation ID to update the global checkpoint for
     * @param globalCheckpoint the global checkpoint
     *                         更新某个分片的全局检查点
     *                         副本的全局检查点变化 并不会影响到主分片的
     *
     */
    public synchronized void updateGlobalCheckpointForShard(final String allocationId, final long globalCheckpoint) {
        assert primaryMode;
        assert handoffInProgress == false;
        assert invariant();
        final CheckpointState cps = checkpoints.get(allocationId);
        assert !this.shardAllocationId.equals(allocationId) || cps != null;
        if (cps != null && globalCheckpoint > cps.globalCheckpoint) {
            final long previousGlobalCheckpoint = cps.globalCheckpoint;
            cps.globalCheckpoint = globalCheckpoint;
            logger.trace("updated local knowledge for [{}] on the primary of the global checkpoint from [{}] to [{}]",
                allocationId, previousGlobalCheckpoint, globalCheckpoint);
        }
        assert invariant();
    }

    /**
     * Initializes the global checkpoint tracker in primary mode (see {@link #primaryMode}. Called on primary activation or promotion.
     * 当本节点对应的shard是主分片 并且从init状态变成 started状态时  触发该函数
     */
    public synchronized void activatePrimaryMode(final long localCheckpoint) {
        assert invariant();
        assert primaryMode == false;
        assert checkpoints.get(shardAllocationId) != null && checkpoints.get(shardAllocationId).inSync &&
            checkpoints.get(shardAllocationId).localCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO :
            "expected " + shardAllocationId + " to have initialized entry in " + checkpoints + " when activating primary";
        assert localCheckpoint >= SequenceNumbers.NO_OPS_PERFORMED;
        primaryMode = true;
        // 更新主分片对应的checkpoint 本对象会记录该分片所有replica,primary的检查点
        updateLocalCheckpoint(shardAllocationId, checkpoints.get(shardAllocationId), localCheckpoint);

        // 根据此时checkpoints 中所有分片的localCheckpoint 生成一个全局检查点
        // 首次触发时其他副本还没有上报自己的localCheckpoint 应该就是 -2  那么此时全局检查点还没有变化 还是-2
        updateGlobalCheckpointOnPrimary();

        // 只插入一个主分片的续约信息
        addPeerRecoveryRetentionLeaseForSolePrimary();
        assert invariant();
    }

    /**
     * Creates a peer recovery retention lease for this shard, if one does not already exist and this shard is the sole shard copy in the
     * replication group. If one does not already exist and yet there are other shard copies in this group then we must have just done
     * a rolling upgrade from a version before {@link Version#V_7_4_0}, in which case the missing leases should be created asynchronously
     * by the caller using {@link ReplicationTracker#createMissingPeerRecoveryRetentionLeases(ActionListener)}.
     * 只处理主分片的续约信息
     */
    private void addPeerRecoveryRetentionLeaseForSolePrimary() {
        assert primaryMode;
        assert Thread.holdsLock(this);

        // 从某个分片的路由表中获取此时的主分片
        final ShardRouting primaryShard = routingTable.primaryShard();
        final String leaseId = getPeerRecoveryRetentionLeaseId(primaryShard);

        // 当续约信息不存在时 进行创建
        if (retentionLeases.get(leaseId) == null) {

            // 代表这个时候完成数据同步的只有主分片
            if (replicationGroup.getReplicationTargets().equals(Collections.singletonList(primaryShard))) {
                assert primaryShard.allocationId().getId().equals(shardAllocationId)
                    : routingTable.assignedShards() + " vs " + shardAllocationId;
                // Safe to call innerAddRetentionLease() without a subsequent sync since there are no other members of this replication
                // group.
                logger.trace("addPeerRecoveryRetentionLeaseForSolePrimary: adding lease [{}]", leaseId);
                innerAddRetentionLease(leaseId, Math.max(0L, checkpoints.get(shardAllocationId).globalCheckpoint + 1),
                    PEER_RECOVERY_RETENTION_LEASE_SOURCE);
                // 代表所有分片的续约信息都已经插入完成
                hasAllPeerRecoveryRetentionLeases = true;
            } else {
                /*
                 * We got here here via a rolling upgrade from an older version that doesn't create peer recovery retention
                 * leases for every shard copy, but in this case we do not expect any leases to exist.
                 */
                assert hasAllPeerRecoveryRetentionLeases == false : routingTable + " vs " + retentionLeases;
                logger.debug("{} becoming primary of {} with missing lease: {}", primaryShard, routingTable, retentionLeases);
            }
            // 检测是否所有分片都已经有对应的续约信息了 有的话修改标识
        } else if (hasAllPeerRecoveryRetentionLeases == false && routingTable.assignedShards().stream().allMatch(shardRouting ->
            retentionLeases.contains(getPeerRecoveryRetentionLeaseId(shardRouting))
                || checkpoints.get(shardRouting.allocationId().getId()).tracked == false)) {
            // Although this index is old enough not to have all the expected peer recovery retention leases, in fact it does, so we
            // don't need to do any more work.
            hasAllPeerRecoveryRetentionLeases = true;
        }
    }

    /**
     * Notifies the tracker of the current allocation IDs in the cluster state.
     *
     * @param applyingClusterStateVersion the cluster state version being applied when updating the allocation IDs from the master
     *                                    此时集群的版本号
     * @param inSyncAllocationIds         the allocation IDs of the currently in-sync shard copies
     *                                    此时认为与主分片完成数据同步的分片 比如刚从recovery中结束的副本
     * @param routingTable                the shard routing table   这时该shard下primary+replica 实际上主分片需要知道此时有多少副本
     *                                    因为每次想要增加globalCheckpoint 应该要获得所有副本的支持
     * 代表接收到leader节点发来的更新分片信息的请求
     */
    public synchronized void updateFromMaster(final long applyingClusterStateVersion, final Set<String> inSyncAllocationIds,
                                              final IndexShardRoutingTable routingTable) {
        assert invariant();

        // 每次集群发生了一次变化 就需要增加版本号  这里是做去重操作
        if (applyingClusterStateVersion > appliedClusterStateVersion) {
            // check that the master does not fabricate new in-sync entries out of thin air once we are in primary mode
            assert !primaryMode || inSyncAllocationIds.stream().allMatch(
                inSyncId -> checkpoints.containsKey(inSyncId) && checkpoints.get(inSyncId).inSync) :
                "update from master in primary mode contains in-sync ids " + inSyncAllocationIds +
                    " that have no matching entries in " + checkpoints;

            // remove entries which don't exist on master
            // 找到所有处于 init阶段的shard对应的allocationId 并生成集合
            Set<String> initializingAllocationIds = routingTable.getAllInitializingShards().stream()
                .map(ShardRouting::allocationId).map(AllocationId::getId).collect(Collectors.toSet());
            // 对于主分片来说需要维护的副本只有2种
            // 第一种 还处于init状态 该副本需要从主分片获取数据  并且在这个时候 推测 globalCheckpoint 是无法变动的 必须要等待最后的副本同步完数据
            // 第二种 就是已经完成recovery 并且转换成start的分片
            boolean removedEntries = checkpoints.keySet().removeIf(
                aid -> !inSyncAllocationIds.contains(aid) && !initializingAllocationIds.contains(aid));

            // 当本分片已经启动完成后 之后感知到的副本都会进入这个分支
            if (primaryMode) {
                // add new initializingIds that are missing locally. These are fresh shard copies - and not in-sync
                // 当又增加了新的副本后 也需要被主分片管理
                for (String initializingId : initializingAllocationIds) {
                    if (checkpoints.containsKey(initializingId) == false) {
                        // 这里断言揭示了 新的init分片必然不在inSync队列中
                        final boolean inSync = inSyncAllocationIds.contains(initializingId);
                        assert inSync == false : "update from master in primary mode has " + initializingId +
                            " as in-sync but it does not exist locally";
                        final long localCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
                        final long globalCheckpoint = localCheckpoint;
                        checkpoints.put(initializingId, new CheckpointState(localCheckpoint, globalCheckpoint, inSync, inSync));
                    }
                }

                // 不需要维护的分片也要从 pendingInSync中移除
                if (removedEntries) {
                    pendingInSync.removeIf(aId -> checkpoints.containsKey(aId) == false);
                }

            } else {
                // 可以看到只要主分片感知到该shardId 增加了新的副本 就会设置到checkpoint中进行维护 并且此时不知道副本的检查点情况 所以都是-2
                // 在副本还未同步完数据前 tracked为false 代表往主分片写入的操作不会复制到该副本上 并且也应该如此 否则数据还未完全恢复就插入新数据会产生混乱
                for (String initializingId : initializingAllocationIds) {
                    final long localCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
                    final long globalCheckpoint = localCheckpoint;
                    checkpoints.put(initializingId, new CheckpointState(localCheckpoint, globalCheckpoint, false, false));
                }

                // 在执行写入操作后 其他副本会上报 localCheckpoint/globalCheckpoint 所以一开始不需要设置 且容易不准确
                // 因为这些分片已经完成数据同步了 所以设置insync为true 且 tracked为true  代表往主分片的写入也要复制到这些副本上
                for (String inSyncId : inSyncAllocationIds) {
                    final long localCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
                    final long globalCheckpoint = localCheckpoint;
                    checkpoints.put(inSyncId, new CheckpointState(localCheckpoint, globalCheckpoint, true, true));
                }
            }
            appliedClusterStateVersion = applyingClusterStateVersion;
            this.routingTable = routingTable;

            // 根据当前最新的 checkpoints 信息  更新 replicationGroup对象
            updateReplicationGroupAndNotify();

            // 当本主分片已经启动 并接收副本分片的更新信息时  某些副本可能不再需要同步数据了 就从相关容器中移除
            if (primaryMode && removedEntries) {
                // 由于某些分片的移除 全局检查点就可以更新
                updateGlobalCheckpointOnPrimary();
                // notify any waiter for local checkpoint advancement to recheck that their shard is still being tracked.
                // 唤醒阻塞在 pending-in-sync的线程
                notifyAllWaiters();
            }
        }
        assert invariant();
    }

    /**
     * Called when the recovery process for a shard has opened the engine on the target shard. Ensures that the right data structures
     * have been set up locally to track local checkpoint information for the shard and that the shard is added to the replication group.
     *
     * @param allocationId  the allocation ID of the shard for which recovery was initiated
     *                      某个分片可以开始接收写入到primary的op了
     */
    public synchronized void initiateTracking(final String allocationId) {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress == false;
        CheckpointState cps = checkpoints.get(allocationId);
        if (cps == null) {
            // can happen if replica was removed from cluster but recovery process is unaware of it yet
            throw new IllegalStateException("no local checkpoint tracking information available");
        }
        cps.tracked = true;
        // 当标识修改为true后 会加入到replicationGroup中
        updateReplicationGroupAndNotify();
        assert invariant();
    }

    /**
     * Marks the shard with the provided allocation ID as in-sync with the primary shard. This method will block until the local checkpoint
     * on the specified shard advances above the current global checkpoint.
     *
     * @param allocationId    the allocation ID of the shard to mark as in-sync
     * @param localCheckpoint the current local checkpoint on the shard
     *                        每当primary往副本传输数据并执行恢复操作后 会触发一次该方法 这里会直到
     */
    public synchronized void markAllocationIdAsInSync(final String allocationId, final long localCheckpoint) throws InterruptedException {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress == false;
        CheckpointState cps = checkpoints.get(allocationId);
        if (cps == null) {
            // can happen if replica was removed from cluster but recovery process is unaware of it yet
            throw new IllegalStateException("no local checkpoint tracking information available for " + allocationId);
        }
        assert localCheckpoint >= SequenceNumbers.NO_OPS_PERFORMED :
            "expected known local checkpoint for " + allocationId + " but was " + localCheckpoint;
        assert pendingInSync.contains(allocationId) == false : "shard copy " + allocationId + " is already marked as pending in-sync";
        assert cps.tracked : "shard copy " + allocationId + " cannot be marked as in-sync as it's not tracked";

        // 在主分片上更新有关该副本分片的localCheckpoint
        updateLocalCheckpoint(allocationId, cps, localCheckpoint);
        // if it was already in-sync (because of a previously failed recovery attempt), global checkpoint must have been
        // stuck from advancing
        assert !cps.inSync || (cps.localCheckpoint >= getGlobalCheckpoint()) :
            "shard copy " + allocationId + " that's already in-sync should have a local checkpoint " + cps.localCheckpoint +
                " that's above the global checkpoint " + getGlobalCheckpoint();


        // 检测该分片是否超过了全局检查点  未超过加入到 pendingInSync中  并阻塞当前线程 直到完成同步
        if (cps.localCheckpoint < getGlobalCheckpoint()) {
            pendingInSync.add(allocationId);
            try {
                while (true) {
                    if (pendingInSync.contains(allocationId)) {
                        // 阻塞当前线程直到同步完成
                        waitForLocalCheckpointToAdvance();
                    } else {
                        break;
                    }
                }
            } finally {
                pendingInSync.remove(allocationId);
            }
        } else {
            // 更新replicationGroup信息
            cps.inSync = true;
            updateReplicationGroupAndNotify();
            logger.trace("marked [{}] as in-sync", allocationId);
            updateGlobalCheckpointOnPrimary();
        }

        assert invariant();
    }

    /**
     * 更新某个分片此时的本地检查点
     * @param allocationId
     * @param cps
     * @param localCheckpoint
     * @return
     */
    private boolean updateLocalCheckpoint(String allocationId, CheckpointState cps, long localCheckpoint) {
        // a local checkpoint for a shard copy should be a valid sequence number
        assert localCheckpoint >= SequenceNumbers.NO_OPS_PERFORMED :
            "invalid local checkpoint [" + localCheckpoint + "] for shard copy [" + allocationId + "]";
        if (localCheckpoint > cps.localCheckpoint) {
            logger.trace("updated local checkpoint of [{}] from [{}] to [{}]", allocationId, cps.localCheckpoint, localCheckpoint);
            cps.localCheckpoint = localCheckpoint;
            return true;
        } else {
            logger.trace("skipped updating local checkpoint of [{}] from [{}] to [{}], current checkpoint is higher", allocationId,
                cps.localCheckpoint, localCheckpoint);
            return false;
        }
    }

    /**
     * Notifies the service to update the local checkpoint for the shard with the provided allocation ID. If the checkpoint is lower than
     * the currently known one, this is a no-op. If the allocation ID is not tracked, it is ignored.
     *
     * @param allocationId    the allocation ID of the shard to update the local checkpoint for
     * @param localCheckpoint the local checkpoint for the shard
     *                        更新某个分片的本地检查点 已持久化 (persistedCheckpoint)
     */
    public synchronized void updateLocalCheckpoint(final String allocationId, final long localCheckpoint) {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress == false;
        // 这里维护了主分片管理的所有副本  只有已经完成数据恢复阶段并进入 in-sync 的副本才会添加到 checkpoints
        CheckpointState cps = checkpoints.get(allocationId);
        if (cps == null) {
            // can happen if replica was removed from cluster but replication process is unaware of it yet
            return;
        }
        // 更新之前的 state对象
        boolean increasedLocalCheckpoint = updateLocalCheckpoint(allocationId, cps, localCheckpoint);

        // 代表该分片在主分片上认为还处于未完成数据同步的状态
        boolean pending = pendingInSync.contains(allocationId);
        // 当副本最新的 persistedCheckpoint 追赶上主分片的globalCheckpoint时  认为已经完成了同步阶段  进入到 in-sync 队列中
        if (pending && cps.localCheckpoint >= getGlobalCheckpoint()) {
            pendingInSync.remove(allocationId);
            pending = false;
            cps.inSync = true;
            // 因为cps发生了变化 所以要更新 replicationGroup
            updateReplicationGroupAndNotify();
            logger.trace("marked [{}] as in-sync", allocationId);
            // 唤醒所有阻塞线程  可能有些操作针对的副本分片 还没有进入到 in-sync 队列 操作就会被阻塞
            notifyAllWaiters();
        }
        // 如果该副本的检查点是全局最小的 本次更新 就有可能会增大全局检查点
        if (increasedLocalCheckpoint && pending == false) {
            updateGlobalCheckpointOnPrimary();
        }
        assert invariant();
    }

    /**
     * Computes the global checkpoint based on the given local checkpoints. In case where there are entries preventing the
     * computation to happen (for example due to blocking), it returns the fallback value.
     * @param pendingInSync 容器内的分片还没有同步好global之前的数据
     * @param localCheckpoints   从CS中获取到的其他分片信息 需要通过该对象维护它们此时的检查点
     * @param fallback  之前的全局检查点
     * 通过一组相关信息  计算此时最新的全局检查点
     */
    private static long computeGlobalCheckpoint(final Set<String> pendingInSync, final Collection<CheckpointState> localCheckpoints,
                                                final long fallback) {
        long minLocalCheckpoint = Long.MAX_VALUE;

        // 只要有副本还处于向主分片同步数据的状态 那么全局检查点就不应该更新
        if (pendingInSync.isEmpty() == false) {
            return fallback;
        }

        // 代表所有分片都已经同步到全局检查点的位置
        for (final CheckpointState cps : localCheckpoints) {
            // 只要分片完成了同步 该标识为true
            if (cps.inSync) {
                // 代表此时副本还未上报自己的本地检查点 不修改此时的全局检查点
                if (cps.localCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    // unassigned in-sync replica
                    return fallback;
                } else {
                    // 所有副本分片最小的本地检查点会作为新的全局检查点
                    minLocalCheckpoint = Math.min(cps.localCheckpoint, minLocalCheckpoint);
                }
            }
        }
        assert minLocalCheckpoint != Long.MAX_VALUE;
        return minLocalCheckpoint;
    }

    /**
     * Scans through the currently known local checkpoint and updates the global checkpoint accordingly.
     * 每个副本分片在处理完索引请求的同时 都会携带 persistedCheckpoint
     */
    private synchronized void updateGlobalCheckpointOnPrimary() {
        assert primaryMode;

        // pendingInsync 代表的是还未进入in-sync的副本 也就是数据还没同步到全局检查点的副本
        final long computedGlobalCheckpoint = computeGlobalCheckpoint(pendingInSync, checkpoints.values(), getGlobalCheckpoint());
        assert computedGlobalCheckpoint >= globalCheckpoint : "new global checkpoint [" + computedGlobalCheckpoint +
            "] is lower than previous one [" + globalCheckpoint + "]";

        // 最小分片持久化的checkpoint增加了 主分片记录的全局检查点也要变化
        if (globalCheckpoint != computedGlobalCheckpoint) {
            globalCheckpoint = computedGlobalCheckpoint;
            logger.trace("updated global checkpoint to [{}]", computedGlobalCheckpoint);
            onGlobalCheckpointUpdated.accept(computedGlobalCheckpoint);
        }
    }

    /**
     * Initiates a relocation handoff and returns the corresponding primary context.
     * @param targetAllocationId  当进行重分配时 目的地所在的allocationId
     */
    public synchronized PrimaryContext startRelocationHandoff(String targetAllocationId) {
        assert invariant();
        // 此时分片必须是主分片才可以调用该方法
        assert primaryMode;
        assert handoffInProgress == false;
        assert pendingInSync.isEmpty() : "relocation handoff started while there are still shard copies pending in-sync: " + pendingInSync;
        // 首先要求这个allocation 必须被checkpoints 维护
        if (checkpoints.containsKey(targetAllocationId) == false) {
            // can happen if the relocation target was removed from cluster but the recovery process isn't aware of that.
            throw new IllegalStateException("relocation target [" + targetAllocationId + "] is no longer part of the replication group");
        }
        handoffInProgress = true;
        // copy clusterStateVersion and checkpoints and return
        // all the entries from checkpoints that are inSync: the reason we don't need to care about initializing non-insync entries
        // is that they will have to undergo a recovery attempt on the relocation target, and will hence be supplied by the cluster state
        // update on the relocation target once relocation completes). We could alternatively also copy the map as-is (it’s safe), and it
        // would be cleaned up on the target by cluster state updates.
        Map<String, CheckpointState> localCheckpointsCopy = new HashMap<>();
        // 将此时维护的所有allocation的检查点信息转存到容器中
        for (Map.Entry<String, CheckpointState> entry : checkpoints.entrySet()) {
            localCheckpointsCopy.put(entry.getKey(), entry.getValue().copy());
        }
        assert invariant();
        // 将当前信息包装成一个上下文对象
        return new PrimaryContext(appliedClusterStateVersion, localCheckpointsCopy, routingTable);
    }

    /**
     * Fails a relocation handoff attempt.
     * 由于在处理过程中产生了异常 导致重分配中断
     * 可以看到这里只是修改了handoff标识 而没有修改其他标识
     */
    public synchronized void abortRelocationHandoff() {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress;
        handoffInProgress = false;
        assert invariant();
    }

    /**
     * Marks a relocation handoff attempt as successful. Moves the tracker into replica mode.
     * 当状态从重定向中结束时触发
     */
    public synchronized void completeRelocationHandoff() {
        assert invariant();
        assert primaryMode;
        assert handoffInProgress;
        assert relocated == false;
        // 当重定向结束后该标识被修改成false   应该是这样 能够发起重定向必然是primary分片 而当重定向结束后 当前就不是主分片了
        primaryMode = false;
        // 代表处理中的标记  因为此时处理完了 所以修改成false
        handoffInProgress = false;
        // 重定向完成后 修改为true
        relocated = true;
        // forget all checkpoint information
        // 当该分片完成重定向后 要将之前的检查点信息都丢弃 因为此时分片检查点由新的node决定
        checkpoints.forEach((key, cps) -> {
            cps.localCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
            cps.globalCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
        });
        assert invariant();
    }

    /**
     * Activates the global checkpoint tracker in primary mode (see {@link #primaryMode}. Called on primary relocation target during
     * primary relocation handoff.
     *
     * @param primaryContext the primary context used to initialize the state
     *                       使用一个主分片上下文进行激活
     */
    public synchronized void activateWithPrimaryContext(PrimaryContext primaryContext) {
        assert invariant();
        assert primaryMode == false;
        final Runnable runAfter = getMasterUpdateOperationFromCurrentState();
        primaryMode = true;
        // capture current state to possibly replay missed cluster state update
        appliedClusterStateVersion = primaryContext.clusterStateVersion();
        checkpoints.clear();
        for (Map.Entry<String, CheckpointState> entry : primaryContext.checkpoints.entrySet()) {
            checkpoints.put(entry.getKey(), entry.getValue().copy());
        }
        routingTable = primaryContext.getRoutingTable();
        updateReplicationGroupAndNotify();
        updateGlobalCheckpointOnPrimary();
        // reapply missed cluster state update
        // note that if there was no cluster state update between start of the engine of this shard and the call to
        // initializeWithPrimaryContext, we might still have missed a cluster state update. This is best effort.
        runAfter.run();
        addPeerRecoveryRetentionLeaseForSolePrimary();
        assert invariant();
    }

    /**
     * 代表所有tracker为true的分片 都已经生成了自己的续约信息
     */
    private synchronized void setHasAllPeerRecoveryRetentionLeases() {
        hasAllPeerRecoveryRetentionLeases = true;
        assert invariant();
    }

    public synchronized boolean hasAllPeerRecoveryRetentionLeases() {
        return hasAllPeerRecoveryRetentionLeases;
    }

    /**
     * Create any required peer-recovery retention leases that do not currently exist because we just did a rolling upgrade from a version
     * prior to {@link Version#V_7_4_0} that does not create peer-recovery retention leases.
     * 确保所有分片都有续约对象 没有则创建
     */
    public synchronized void createMissingPeerRecoveryRetentionLeases(ActionListener<Void> listener) {
        if (hasAllPeerRecoveryRetentionLeases == false) {
            final List<ShardRouting> shardRoutings = routingTable.assignedShards();
            final GroupedActionListener<ReplicationResponse> groupedActionListener = new GroupedActionListener<>(ActionListener.wrap(vs -> {
                setHasAllPeerRecoveryRetentionLeases();
                listener.onResponse(null);
            }, listener::onFailure), shardRoutings.size());
            for (ShardRouting shardRouting : shardRoutings) {
                // 代表这个分片的续约信息已经存在
                if (retentionLeases.contains(getPeerRecoveryRetentionLeaseId(shardRouting))) {
                    groupedActionListener.onResponse(null);
                } else {
                    // 当续约信息不存在时 进行手动插入
                    final CheckpointState checkpointState = checkpoints.get(shardRouting.allocationId().getId());
                    // 代表该分片还没有完成数据同步 不需要生成续约信息
                    if (checkpointState.tracked == false) {
                        groupedActionListener.onResponse(null);
                    } else {
                        logger.trace("createMissingPeerRecoveryRetentionLeases: adding missing lease for {}", shardRouting);
                        try {
                            // 每次续约信息发生变化 需要同步到集群其他节点
                            addPeerRecoveryRetentionLease(shardRouting.currentNodeId(),
                                Math.max(SequenceNumbers.NO_OPS_PERFORMED, checkpointState.globalCheckpoint), groupedActionListener);
                        } catch (Exception e) {
                            groupedActionListener.onFailure(e);
                        }
                    }
                }
            }
        } else {
            logger.trace("createMissingPeerRecoveryRetentionLeases: nothing to do");
            listener.onResponse(null);
        }
    }

    private Runnable getMasterUpdateOperationFromCurrentState() {
        assert primaryMode == false;
        final long lastAppliedClusterStateVersion = appliedClusterStateVersion;
        final Set<String> inSyncAllocationIds = new HashSet<>();
        checkpoints.entrySet().forEach(entry -> {
            if (entry.getValue().inSync) {
                inSyncAllocationIds.add(entry.getKey());
            }
        });
        final IndexShardRoutingTable lastAppliedRoutingTable = routingTable;
        return () -> updateFromMaster(lastAppliedClusterStateVersion, inSyncAllocationIds, lastAppliedRoutingTable);
    }

    /**
     * Whether the are shards blocking global checkpoint advancement.
     */
    public synchronized boolean pendingInSync() {
        assert primaryMode;
        return pendingInSync.isEmpty() == false;
    }

    /**
     * Returns the local checkpoint information tracked for a specific shard. Used by tests.
     */
    public synchronized CheckpointState getTrackedLocalCheckpointForShard(String allocationId) {
        assert primaryMode;
        return checkpoints.get(allocationId);
    }

    /**
     * Notify all threads waiting on the monitor on this tracker. These threads should be waiting for the local checkpoint on a specific
     * allocation ID to catch up to the global checkpoint.
     */
    @SuppressForbidden(reason = "Object#notifyAll waiters for local checkpoint advancement")
    private synchronized void notifyAllWaiters() {
        this.notifyAll();
    }

    /**
     * Wait for the local checkpoint to advance to the global checkpoint.
     *
     * @throws InterruptedException if this thread was interrupted before of during waiting
     */
    @SuppressForbidden(reason = "Object#wait for local checkpoint advancement")
    private synchronized void waitForLocalCheckpointToAdvance() throws InterruptedException {
        this.wait();
    }

    /**
     * Represents the sequence number component of the primary context. This is the knowledge on the primary of the in-sync and initializing
     * shards and their local checkpoints.
     * 在副本数据拷贝过程中  代表当前是主分片吗
     */
    public static class PrimaryContext implements Writeable {

        private final long clusterStateVersion;
        private final Map<String, CheckpointState> checkpoints;
        private final IndexShardRoutingTable routingTable;

        public PrimaryContext(long clusterStateVersion, Map<String, CheckpointState> checkpoints,
                              IndexShardRoutingTable routingTable) {
            this.clusterStateVersion = clusterStateVersion;
            this.checkpoints = checkpoints;
            this.routingTable = routingTable;
        }

        public PrimaryContext(StreamInput in) throws IOException {
            clusterStateVersion = in.readVLong();
            checkpoints = in.readMap(StreamInput::readString, CheckpointState::new);
            routingTable = IndexShardRoutingTable.Builder.readFrom(in);
        }

        public long clusterStateVersion() {
            return clusterStateVersion;
        }

        public Map<String, CheckpointState> getCheckpointStates() {
            return checkpoints;
        }

        public IndexShardRoutingTable getRoutingTable() {
            return routingTable;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(clusterStateVersion);
            out.writeMap(checkpoints, (streamOutput, s) -> out.writeString(s), (streamOutput, cps) -> cps.writeTo(out));
            IndexShardRoutingTable.Builder.writeTo(routingTable, out);
        }

        @Override
        public String toString() {
            return "PrimaryContext{" +
                    "clusterStateVersion=" + clusterStateVersion +
                    ", checkpoints=" + checkpoints +
                    ", routingTable=" + routingTable +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PrimaryContext that = (PrimaryContext) o;

            if (clusterStateVersion != that.clusterStateVersion) return false;
            if (routingTable.equals(that.routingTable)) return false;
            return routingTable.equals(that.routingTable);
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(clusterStateVersion);
            result = 31 * result + checkpoints.hashCode();
            result = 31 * result + routingTable.hashCode();
            return result;
        }
    }
}
