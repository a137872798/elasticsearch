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

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotRecoveringException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a recovery where the current node is the target node of the recovery. To track recoveries in a central place, instances of
 * this class are created through {@link RecoveriesCollection}.
 * 每个shard 从primary拉取数据 并进行恢复的任务会被封装成一个 recoveryTarget对象
 */
public class RecoveryTarget extends AbstractRefCounted implements RecoveryTargetHandler {

    private final Logger logger;

    private static final AtomicLong idGenerator = new AtomicLong();

    private static final String RECOVERY_PREFIX = "recovery.";

    /**
     * 数据的恢复是以分片为单位的
     */
    private final ShardId shardId;
    /**
     * 每次发起的恢复操作应该都有一个id
     */
    private final long recoveryId;

    /**
     * shardId 对应的分片对象
     */
    private final IndexShard indexShard;
    /**
     * 代表会从这个节点上获取信息
     */
    private final DiscoveryNode sourceNode;
    /**
     * 该对象管理对多个文件的写入操作
     */
    private final MultiFileWriter multiFileWriter;
    private final Store store;

    /**
     * 基于钩子实现监听器
     */
    private final PeerRecoveryTargetService.RecoveryListener listener;

    /**
     * 代表从目标节点恢复数据的这个流程是否完成
     */
    private final AtomicBoolean finished = new AtomicBoolean();

    /**
     * 管理一组线程对象 通过一个关闭操作可以同时关闭管理的所有线程
     */
    private final CancellableThreads cancellableThreads;

    // last time this status was accessed
    private volatile long lastAccessTime = System.nanoTime();

    // latch that can be used to blockingly wait for RecoveryTarget to be closed
    // 提供阻塞等待close完成的能力
    private final CountDownLatch closedLatch = new CountDownLatch(1);

    /**
     * Creates a new recovery target object that represents a recovery to the provided shard.
     *
     * @param indexShard                        local shard where we want to recover to
     * @param sourceNode                        source node of the recovery where we recover from
     * @param listener                          called when recovery is completed/failed
     */
    public RecoveryTarget(IndexShard indexShard, DiscoveryNode sourceNode, PeerRecoveryTargetService.RecoveryListener listener) {
        super("recovery_status");
        this.cancellableThreads = new CancellableThreads();
        // 每个node上 recovery确保唯一性
        this.recoveryId = idGenerator.incrementAndGet();
        this.listener = listener;
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.sourceNode = sourceNode;
        this.shardId = indexShard.shardId();
        // 生成特殊的临时文件前缀
        final String tempFilePrefix = RECOVERY_PREFIX + UUIDs.randomBase64UUID() + ".";
        // indexShard.store() 对应分片数据所在的目录
        this.multiFileWriter = new MultiFileWriter(indexShard.store(), indexShard.recoveryState().getIndex(), tempFilePrefix, logger,
            this::ensureRefCount);
        this.store = indexShard.store();
        // make sure the store is not released until we are done.
        // 因为此时准备往store中写入文件 所以增加引用计数 避免dir被意外删除
        store.incRef();
        indexShard.recoveryStats().incCurrentAsTarget();
    }

    /**
     * Returns a fresh recovery target to retry recovery from the same source node onto the same shard and using the same listener.
     *
     * @return a copy of this recovery target
     */
    public RecoveryTarget retryCopy() {
        return new RecoveryTarget(indexShard, sourceNode, listener);
    }

    public long recoveryId() {
        return recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public IndexShard indexShard() {
        ensureRefCount();
        return indexShard;
    }

    public DiscoveryNode sourceNode() {
        return this.sourceNode;
    }

    public RecoveryState state() {
        return indexShard.recoveryState();
    }

    public CancellableThreads cancellableThreads() {
        return cancellableThreads;
    }

    /** return the last time this RecoveryStatus was used (based on System.nanoTime() */
    public long lastAccessTime() {
        return lastAccessTime;
    }

    /** sets the lasAccessTime flag to now */
    public void setLastAccessTime() {
        lastAccessTime = System.nanoTime();
    }

    public Store store() {
        ensureRefCount();
        return store;
    }

    /**
     * Closes the current recovery target and waits up to a certain timeout for resources to be freed.
     * Returns true if resetting the recovery was successful, false if the recovery target is already cancelled / failed or marked as done.
     * 关闭当前的恢复操作
     */
    boolean resetRecovery(CancellableThreads newTargetCancellableThreads) throws IOException {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("reset of recovery with shard {} and id [{}]", shardId, recoveryId);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now.
                decRef();
            }
            try {
                // 阻塞新的线程 直到本对象被关闭
                newTargetCancellableThreads.execute(closedLatch::await);
            } catch (CancellableThreads.ExecutionCancelledException e) {
                logger.trace("new recovery target cancelled for shard {} while waiting on old recovery target with id [{}] to close",
                    shardId, recoveryId);
                return false;
            }
            // 进入到这里时 代表 multiFileWriter已经被关闭了
            RecoveryState.Stage stage = indexShard.recoveryState().getStage();
            // 当本次recovery 已经完成了 那么就无法重新执行一次恢复了
            if (indexShard.recoveryState().getPrimary() && (stage == RecoveryState.Stage.FINALIZE || stage == RecoveryState.Stage.DONE)) {
                // once primary relocation has moved past the finalization step, the relocation source can put the target into primary mode
                // and start indexing as primary into the target shard (see TransportReplicationAction). Resetting the target shard in this
                // state could mean that indexing is halted until the recovery retry attempt is completed and could also destroy existing
                // documents indexed and acknowledged before the reset.
                assert stage != RecoveryState.Stage.DONE : "recovery should not have completed when it's being reset";
                throw new IllegalStateException("cannot reset recovery as previous attempt made it past finalization step");
            }
            // 这里只是将 recoveryState 重置成init
            indexShard.performRecoveryRestart();
            return true;
        }
        return false;
    }

    /**
     * cancel the recovery. calling this method will clean temporary files and release the store
     * unless this object is in use (in which case it will be cleaned once all ongoing users call
     * {@link #decRef()}
     * <p>
     * if {@link #cancellableThreads()} was used, the threads will be interrupted.
     * 关闭此时正在运行的所有线程 推测每个线程会拉取某个文件的数据 但是多线程应该是无法提高网络IO的性能的 等下网络IO  跟磁盘IO是不同的 不需要旋转磁头可能能提高性能???
     */
    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("recovery canceled (reason: [{}])", reason);
                cancellableThreads.cancel(reason);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
        }
    }

    /**
     * fail the recovery and call listener
     *
     * @param e                exception that encapsulating the failure
     * @param sendShardFailure indicates whether to notify the master of the shard failure
     *                         本次恢复任务因为失败而中止
     */
    public void fail(RecoveryFailedException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                notifyListener(e, sendShardFailure);
            } finally {
                try {
                    // 关闭恢复用线程  会打断所有阻塞中线程
                    cancellableThreads.cancel("failed recovery [" + ExceptionsHelper.stackTrace(e) + "]");
                } finally {
                    // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                    decRef();
                }
            }
        }
    }

    public void notifyListener(RecoveryFailedException e, boolean sendShardFailure) {
        listener.onRecoveryFailure(state(), e, sendShardFailure);
    }

    /**
     * mark the current recovery as done
     * */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            assert multiFileWriter.tempFileNames.isEmpty() : "not all temporary files are renamed";
            try {
                // this might still throw an exception ie. if the shard is CLOSED due to some other event.
                // it's safer to decrement the reference in a try finally here.
                // 当恢复流程执行完毕后 将数据持久化同时修改recoveryState的状态
                indexShard.postRecovery("peer recovery done");
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
            listener.onRecoveryDone(state());
        }
    }

    /**
     * 当本对象被关闭时 关闭闭锁
     */
    @Override
    protected void closeInternal() {
        try {
            multiFileWriter.close();
        } finally {
            // free store. increment happens in constructor
            store.decRef();
            indexShard.recoveryStats().decCurrentAsTarget();
            closedLatch.countDown();
        }
    }

    @Override
    public String toString() {
        return shardId + " [" + recoveryId + "]";
    }

    private void ensureRefCount() {
        if (refCount() <= 0) {
            throw new ElasticsearchException("RecoveryStatus is used but it's refcount is 0. Probably a mismatch between incRef/decRef " +
                    "calls");
        }
    }

    /*** Implementation of {@link RecoveryTargetHandler } */

    /**
     * 启动engine 并准备接收从 primaryShard发送的操作日志
     * @param totalTranslogOps  total translog operations expected to be sent
     * @param listener
     */
    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            // 标记本次恢复的操作数量
            state().getTranslog().totalOperations(totalTranslogOps);
            // 打开engine对象 但是跳过数据恢复阶段
            indexShard().openEngineAndSkipTranslogRecovery();
            return null;
        });
    }

    /**
     * 代表整个恢复操作已经完成了  可以清理一些无效的事务文件了
     * @param globalCheckpoint the global checkpoint on the recovery source   此时从远端节点收到的globalCheckpoint
     * @param trimAboveSeqNo   The recovery target should erase its existing translog above this sequence number
     *                         from the previous primary terms.      本次恢复流程对应的 startingSeqNo-1   不过在前面拉取最新的快照数据时(索引文件)  已经清空了事务日志啊
     * @param listener         the listener which will be notified when this method is completed
     */
    @Override
    public void finalizeRecovery(final long globalCheckpoint, final long trimAboveSeqNo, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            // 通过主分片传递过来的最新全局检查点 更新本地分片
            indexShard.updateGlobalCheckpointOnReplica(globalCheckpoint, "finalizing recovery");
            // Persist the global checkpoint.
            // 对事务日志进行刷盘
            indexShard.sync();
            // 将续约信息写入到一个 _state 文件中     TODO 续约信息在整个恢复过程中好像没有被使用到
            indexShard.persistRetentionLeases();
            if (trimAboveSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                // We should erase all translog operations above trimAboveSeqNo as we have received either the same or a newer copy
                // from the recovery source in phase2. Rolling a new translog generation is not strictly required here for we won't
                // trim the current generation. It's merely to satisfy the assumption that the current generation does not have any
                // operation that would be trimmed (see TranslogWriter#assertNoSeqAbove). This assumption does not hold for peer
                // recovery because we could have received operations above startingSeqNo from the previous primary terms.
                // 滚动事务文件会将 gen+1 并作为新的事务文件名  生成文件
                indexShard.rollTranslogGeneration();
                // the flush or translog generation threshold can be reached after we roll a new translog
                indexShard.afterWriteOperation();
                // 将指定seq之前的数据全部清除  实际上只是更新了 checkpoint.trimmedAboveSeqNo
                indexShard.trimOperationOfPreviousPrimaryTerms(trimAboveSeqNo);
            }
            // 如果还有事务日志没有刷盘 进行持久化
            if (hasUncommittedOperations()) {
                indexShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            }
            // 当恢复操作完成时 修改recoveryState的状态
            indexShard.finalizeRecovery();
            return null;
        });
    }

    /**
     * 检查是否有未提交的数据
     * @return
     * @throws IOException
     */
    private boolean hasUncommittedOperations() throws IOException {
        long localCheckpointOfCommit = Long.parseLong(indexShard.commitStats().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
        // 检测在指定seq之上有多少operation
        try (Translog.Snapshot snapshot =
                 indexShard.newChangesSnapshot("peer-recovery", localCheckpointOfCommit + 1, Long.MAX_VALUE, false)) {
            return snapshot.totalOperations() > 0;
        }
    }

    @Override
    public void handoffPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) {
        indexShard.activateWithPrimaryContext(primaryContext);
    }

    /**
     * @param operations                          operations to index
     * @param totalTranslogOps                    current number of total operations expected to be indexed
     * @param maxSeenAutoIdTimestampOnPrimary     the maximum auto_id_timestamp of all append-only requests processed by the primary shard
     * @param maxSeqNoOfDeletesOrUpdatesOnPrimary
     * @param retentionLeases                     the retention leases on the primary
     * @param mappingVersionOnPrimary             the mapping version which is at least as up to date as the mapping version that the
     *                                            primary used to index translog {@code operations} in this request.
     *                                            If the mapping version on the replica is not older this version, we should not retry on
     *                                            {@link org.elasticsearch.index.mapper.MapperException}; otherwise we should wait for a
     *                                            new mapping then retry.
     * @param listener                            a listener which will be notified with the local checkpoint on the target
     *                                            从primary接收safeCommit后的事务操作日志  用于恢复数据 同时也会记录在本地事务日志中
     */
    @Override
    public void indexTranslogOperations(
            // 以下参数都是从 primary.indexShard携带过来的 //
            final List<Translog.Operation> operations,
            final int totalTranslogOps,
            final long maxSeenAutoIdTimestampOnPrimary,
            final long maxSeqNoOfDeletesOrUpdatesOnPrimary,
            final RetentionLeases retentionLeases,
            // ------------------------------------------- //
            final long mappingVersionOnPrimary,
            final ActionListener<Long> listener) {
        ActionListener.completeWith(listener, () -> {

            // 这个对象是统计本次恢复的事务日志信息的
            final RecoveryState.Translog translog = state().getTranslog();
            // 记录本次要恢复多少个operation
            translog.totalOperations(totalTranslogOps);
            assert indexShard().recoveryState() == state();
            // 确保此时处于recovery状态
            if (indexShard().state() != IndexShardState.RECOVERING) {
                throw new IndexShardNotRecoveringException(shardId, indexShard().state());
            }
            // 从主分片上携带过来的信息设置到了 副本上

            /*
             * The maxSeenAutoIdTimestampOnPrimary received from the primary is at least the highest auto_id_timestamp from any operation
             * will be replayed. Bootstrapping this timestamp here will disable the optimization for original append-only requests
             * (source of these operations) replicated via replication. Without this step, we may have duplicate documents if we
             * replay these operations first (without timestamp), then optimize append-only requests (with timestamp).
             */
            indexShard().updateMaxUnsafeAutoIdTimestamp(maxSeenAutoIdTimestampOnPrimary);
            /*
             * Bootstrap the max_seq_no_of_updates from the primary to make sure that the max_seq_no_of_updates on this replica when
             * replaying any of these operations will be at least the max_seq_no_of_updates on the primary when that op was executed on.
             */
            indexShard().advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfDeletesOrUpdatesOnPrimary);
            /*
             * We have to update the retention leases before we start applying translog operations to ensure we are retaining according to
             * the policy.
             * 副本上更新续约信息 是不需要同步到集群其他分片的
             */
            indexShard().updateRetentionLeasesOnReplica(retentionLeases);

            for (Translog.Operation operation : operations) {
                // 将operation 重新写入到 lucene中   注意使用的origin是PEER_RECOVERY
                Engine.Result result = indexShard().applyTranslogOperation(operation, Engine.Operation.Origin.PEER_RECOVERY);
                if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                    throw new MapperException("mapping updates are not allowed [" + operation + "]");
                }
                if (result.getFailure() != null) {
                    if (Assertions.ENABLED && result.getFailure() instanceof MapperException == false) {
                        throw new AssertionError("unexpected failure while replicating translog entry", result.getFailure());
                    }
                    ExceptionsHelper.reThrowIfNotNull(result.getFailure());
                }
            }
            // update stats only after all operations completed (to ensure that mapping updates don't mess with stats)
            // 记录当前总计恢复了多少operation
            translog.incrementRecoveredOperations(operations.size());
            // 写入完成后将事务日志刷盘   在写入过程中可能会被动的触发lucene的刷盘 不过事务日志中还记录着 globalCheckpoint 而最接近这个检查点的commit信息才是可靠的
            // 在这种场景下 lucene刷盘只是为了缓解内存压力
            indexShard().sync();
            // roll over / flush / trim if needed   检测是否需要开启定时任务 进行事务日志的滚动  以及lucene数据的刷盘
            indexShard().afterWriteOperation();
            return indexShard().getLocalCheckpoint();
        });
    }

    /**
     * 主分片收到副本的恢复请求时 要先确定本次从哪里开始恢复数据   主分片会先将safeCommit的文件传过来
     * @param phase1FileNames
     * @param phase1FileSizes
     * @param phase1ExistingFileNames
     * @param phase1ExistingFileSizes
     * @param totalTranslogOps
     * @param listener
     */
    @Override
    public void receiveFileInfo(List<String> phase1FileNames,
                                List<Long> phase1FileSizes,
                                List<String> phase1ExistingFileNames,
                                List<Long> phase1ExistingFileSizes,
                                int totalTranslogOps,
                                ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            // 在 IndexShardState中还有个recoveryState 这里重置它的状态
            indexShard.resetRecoveryStage();
            // 切换成index状态
            indexShard.prepareForIndexRecovery();
            final RecoveryState.Index index = state().getIndex();
            // 将文件的描述信息写入到index中    已经存在的文件 直接复用就可以了 所以 reused为true
            for (int i = 0; i < phase1ExistingFileNames.size(); i++) {
                index.addFileDetail(phase1ExistingFileNames.get(i), phase1ExistingFileSizes.get(i), true);
            }
            for (int i = 0; i < phase1FileNames.size(); i++) {
                index.addFileDetail(phase1FileNames.get(i), phase1FileSizes.get(i), false);
            }
            state().getTranslog().totalOperations(totalTranslogOps);
            state().getTranslog().totalOperationsOnStart(totalTranslogOps);
            return null;
        });
    }

    /**
     * 清除全局检查点之前的文件
     * @param totalTranslogOps an update number of translog operations that will be replayed later on
     * @param globalCheckpoint the global checkpoint on the primary
     * @param sourceMetadata   meta data of the source store
     * @param listener
     */
    @Override
    public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetadata,
                           ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            state().getTranslog().totalOperations(totalTranslogOps);
            // first, we go and move files that were created with the recovery id suffix to
            // the actual names, its ok if we have a corrupted index here, since we have replicas
            // to recover from in case of a full cluster shutdown just when this code executes...
            // 之前在 FILE_CHUNK 阶段 某次恢复需要的所有索引文件都以文件片段的方式发送到primary 并在replica 完成拼接了
            // 不过那时是以临时文件命名的 现在修改成索引文件名
            multiFileWriter.renameAllTempFiles();
            final Store store = store();
            store.incRef();
            try {
                // 除了本次还原的索引文件外 其余文件都要清除  包括事务文件
                // TODO 如果这个时候 本节点变成leader节点数据是否会丢失  应该是不会的  这时lucene中存储的应该是最接近globalCheckpoint的数据  也可以理解为这份数据就是所有副本认可的数据
                store.cleanupAndVerify("recovery CleanFilesRequestHandler", sourceMetadata);

                // 这里要重新创建事务日志文件
                final String translogUUID = Translog.createEmptyTranslog(
                    indexShard.shardPath().resolveTranslog(), globalCheckpoint, shardId, indexShard.getPendingPrimaryTerm());
                // 更新lucene.userData 中的translogUUID
                store.associateIndexWithNewTranslog(translogUUID);

                // TODO 主分片会将续约信息同步到所有副本上  这种情况应该不存在
                if (indexShard.getRetentionLeases().leases().isEmpty()) {
                    // if empty, may be a fresh IndexShard, so write an empty leases file to disk
                    indexShard.persistRetentionLeases();
                    assert indexShard.loadRetentionLeases().leases().isEmpty();
                } else {
                    assert indexShard.assertRetentionLeasesPersisted();
                }
                // TODO 校验先忽略
                indexShard.maybeCheckIndex();
                // 标记成事务日志恢复阶段  下一步 primary就会传输事务日志
                state().setStage(RecoveryState.Stage.TRANSLOG);
            } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException ex) {
                // this is a fatal exception at this stage.
                // this means we transferred files from the remote that have not be checksummed and they are
                // broken. We have to clean up this shard entirely, remove all files and bubble it up to the
                // source shard since this index might be broken there as well? The Source can handle this and checks
                // its content on disk if possible.
                try {
                    try {
                        store.removeCorruptionMarker();
                    } finally {
                        Lucene.cleanLuceneIndex(store.directory()); // clean up and delete all files
                    }
                } catch (Exception e) {
                    logger.debug("Failed to clean lucene index", e);
                    ex.addSuppressed(e);
                }
                RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
                fail(rfe, true);
                throw rfe;
            } catch (Exception ex) {
                RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
                fail(rfe, true);
                throw rfe;
            } finally {
                store.decRef();
            }
            return null;
        });
    }

    /**
     * primaryShard 所在的节点会通过  MultiFileTransfer 将本次要恢复的文件数据通过网络发送到replicaShard 并在本地生成临时文件 写入数据
     * @param fileMetadata
     * @param position
     * @param content
     * @param lastChunk
     * @param totalTranslogOps
     * @param listener
     */
    @Override
    public void writeFileChunk(StoreFileMetadata fileMetadata, long position, BytesReference content,
                               boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener) {
        try {
            state().getTranslog().totalOperations(totalTranslogOps);
            multiFileWriter.writeFileChunk(fileMetadata, position, content, lastChunk);
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /** Get a temporary name for the provided file name. */
    public String getTempNameForFile(String origFile) {
        return multiFileWriter.getTempNameForFile(origFile);
    }

    Path translogLocation() {
        return indexShard().shardPath().resolveTranslog();
    }
}
