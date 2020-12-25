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

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * This package private utility class encapsulates the logic to recover an index shard from either an existing index on
 * disk or from a snapshot in a repository.
 * 代表通过store对某个分片的数据进行恢复
 */
final class StoreRecovery {

    private final Logger logger;
    private final ShardId shardId;

    StoreRecovery(ShardId shardId, Logger logger) {
        this.logger = logger;
        this.shardId = shardId;
    }

    /**
     * Recovers a shard from it's local file system store. This method required pre-knowledge about if the shard should
     * exist on disk ie. has been previously allocated or if the shard is a brand new allocation without pre-existing index
     * files / transaction logs. This
     * @param indexShard the index shard instance to recovery the shard into   本次要恢复的是哪个shard的数据
     * @param listener resolves to <code>true</code> if the shard has been recovered successfully, <code>false</code> if the recovery
     *                 has been ignored due to a concurrent modification of if the clusters state has changed due to async updates.
     *                 监听recovery的结果
     * @see Store
     * 从 store中恢复数据
     */
    void recoverFromStore(final IndexShard indexShard, ActionListener<Boolean> listener) {
        // 只有主分片的数据是可靠的 所以只有主分片才允许从本地事务日志恢复数据
        if (canRecover(indexShard)) {
            RecoverySource.Type recoveryType = indexShard.recoveryState().getRecoverySource().getType();
            // type必须要匹配
            assert recoveryType == RecoverySource.Type.EMPTY_STORE || recoveryType == RecoverySource.Type.EXISTING_STORE :
                "expected store recovery type but was: " + recoveryType;
            // 先执行第二个参数  获取结果后触发第一个参数
            ActionListener.completeWith(recoveryListener(indexShard, listener), () -> {
                logger.debug("starting recovery from store ...");
                internalRecoverFromStore(indexShard);
                return true;
            });
        } else {
            listener.onResponse(false);
        }
    }

    /**
     *
     * @param mappingUpdateConsumer  处理mapping元数据的对象
     * @param indexShard  恢复的数据会填充到这个分片上
     * @param shards    数据从这些分片中获取
     * @param listener
     */
    void recoverFromLocalShards(Consumer<MappingMetadata> mappingUpdateConsumer, final IndexShard indexShard,
                                   final List<LocalShardSnapshot> shards, ActionListener<Boolean> listener) {
        if (canRecover(indexShard)) {
            // 标明数据恢复源是什么  主要是为了检测
            RecoverySource.Type recoveryType = indexShard.recoveryState().getRecoverySource().getType();
            assert recoveryType == RecoverySource.Type.LOCAL_SHARDS: "expected local shards recovery type: " + recoveryType;
            if (shards.isEmpty()) {
                throw new IllegalArgumentException("shards must not be empty");
            }
            // 这里有限制 shards列表内应该只有一个元素
            Set<Index> indices = shards.stream().map((s) -> s.getIndex()).collect(Collectors.toSet());
            if (indices.size() > 1) {
                throw new IllegalArgumentException("can't add shards from more than one index");
            }
            IndexMetadata sourceMetadata = shards.get(0).getIndexMetadata();
            if (sourceMetadata.mapping() != null) {
                mappingUpdateConsumer.accept(sourceMetadata.mapping());
            }
            // TODO 也就是根据shard进行恢复时 才会用到mapping吗  先不细究
            indexShard.mapperService().merge(sourceMetadata, MapperService.MergeReason.MAPPING_RECOVERY);
            // now that the mapping is merged we can validate the index sort configuration.
            Sort indexSort = indexShard.getIndexSort();
            final boolean hasNested = indexShard.mapperService().hasNested();
            final boolean isSplit = sourceMetadata.getNumberOfShards() < indexShard.indexSettings().getNumberOfShards();
            ActionListener.completeWith(recoveryListener(indexShard, listener), () -> {
                logger.debug("starting recovery from local shards {}", shards);
                try {
                    final Directory directory = indexShard.store().directory(); // don't close this directory!!
                    final Directory[] sources = shards.stream().map(LocalShardSnapshot::getSnapshotDirectory).toArray(Directory[]::new);
                    final long maxSeqNo = shards.stream().mapToLong(LocalShardSnapshot::maxSeqNo).max().getAsLong();
                    final long maxUnsafeAutoIdTimestamp =
                        shards.stream().mapToLong(LocalShardSnapshot::maxUnsafeAutoIdTimestamp).max().getAsLong();
                    addIndices(indexShard.recoveryState().getIndex(), directory, indexSort, sources, maxSeqNo, maxUnsafeAutoIdTimestamp,
                        indexShard.indexSettings().getIndexMetadata(), indexShard.shardId().id(), isSplit, hasNested);
                    internalRecoverFromStore(indexShard);
                    // just trigger a merge to do housekeeping on the
                    // copied segments - we will also see them in stats etc.
                    indexShard.getEngine().forceMerge(false, -1, false,
                        false, false, UUIDs.randomBase64UUID());
                    return true;
                } catch (IOException ex) {
                    throw new IndexShardRecoveryException(indexShard.shardId(), "failed to recover from local shards", ex);
                }
            });
        } else {
            listener.onResponse(false);
        }
    }

    void addIndices(final RecoveryState.Index indexRecoveryStats, final Directory target, final Sort indexSort, final Directory[] sources,
            final long maxSeqNo, final long maxUnsafeAutoIdTimestamp, IndexMetadata indexMetadata, int shardId, boolean split,
            boolean hasNested) throws IOException {

        assert sources.length > 0;
        final int luceneIndexCreatedVersionMajor = Lucene.readSegmentInfos(sources[0]).getIndexCreatedVersionMajor();

        final Directory hardLinkOrCopyTarget = new org.apache.lucene.store.HardlinkCopyDirectoryWrapper(target);

        IndexWriterConfig iwc = new IndexWriterConfig(null)
            .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setCommitOnClose(false)
            // we don't want merges to happen here - we call maybe merge on the engine
            // later once we stared it up otherwise we would need to wait for it here
            // we also don't specify a codec here and merges should use the engines for this index
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            .setIndexCreatedVersionMajor(luceneIndexCreatedVersionMajor);
        if (indexSort != null) {
            iwc.setIndexSort(indexSort);
        }

        try (IndexWriter writer = new IndexWriter(new StatsDirectoryWrapper(hardLinkOrCopyTarget, indexRecoveryStats), iwc)) {
            writer.addIndexes(sources);
            if (split) {
                writer.deleteDocuments(new ShardSplittingQuery(indexMetadata, shardId, hasNested));
            }
            /*
             * We set the maximum sequence number and the local checkpoint on the target to the maximum of the maximum sequence numbers on
             * the source shards. This ensures that history after this maximum sequence number can advance and we have correct
             * document-level semantics.
             */
            writer.setLiveCommitData(() -> {
                final HashMap<String, String> liveCommitData = new HashMap<>(3);
                liveCommitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
                liveCommitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(maxSeqNo));
                liveCommitData.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp));
                return liveCommitData.entrySet().iterator();
            });
            writer.commit();
        }
    }

    /**
     * Directory wrapper that records copy process for recovery statistics
     */
    static final class StatsDirectoryWrapper extends FilterDirectory {
        private final RecoveryState.Index index;

        StatsDirectoryWrapper(Directory in, RecoveryState.Index indexRecoveryStats) {
            super(in);
            this.index = indexRecoveryStats;
        }

        @Override
        public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
            final long l = from.fileLength(src);
            final AtomicBoolean copies = new AtomicBoolean(false);
            // here we wrap the index input form the source directory to report progress of file copy for the recovery stats.
            // we increment the num bytes recovered in the readBytes method below, if users pull statistics they can see immediately
            // how much has been recovered.
            in.copyFrom(new FilterDirectory(from) {
                @Override
                public IndexInput openInput(String name, IOContext context) throws IOException {
                    index.addFileDetail(dest, l, false);
                    copies.set(true);
                    final IndexInput input = in.openInput(name, context);
                    return new IndexInput("StatsDirectoryWrapper(" + input.toString() + ")") {
                        @Override
                        public void close() throws IOException {
                            input.close();
                        }

                        @Override
                        public long getFilePointer() {
                            throw new UnsupportedOperationException("only straight copies are supported");
                        }

                        @Override
                        public void seek(long pos) throws IOException {
                            throw new UnsupportedOperationException("seeks are not supported");
                        }

                        @Override
                        public long length() {
                            return input.length();
                        }

                        @Override
                        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
                            throw new UnsupportedOperationException("slices are not supported");
                        }

                        @Override
                        public byte readByte() throws IOException {
                           throw new UnsupportedOperationException("use a buffer if you wanna perform well");
                        }

                        @Override
                        public void readBytes(byte[] b, int offset, int len) throws IOException {
                            // we rely on the fact that copyFrom uses a buffer
                            input.readBytes(b, offset, len);
                            index.addRecoveredBytesToFile(dest, len);
                        }
                    };
                }
            }, src, dest, context);
            if (copies.get() == false) {
                index.addFileDetail(dest, l, true); // hardlinked - we treat it as reused since the file was already somewhat there
            } else {
                assert index.getFileDetails(dest) != null : "File [" + dest + "] has no file details";
                assert index.getFileDetails(dest).recovered() == l : index.getFileDetails(dest).toString();
            }
        }
    }

    /**
     * Recovers an index from a given {@link Repository}. This method restores a
     * previously created index snapshot into an existing initializing shard.
     * @param indexShard the index shard instance to recovery the snapshot from
     * @param repository the repository holding the physical files the shard should be recovered from
     * @param listener resolves to <code>true</code> if the shard has been recovered successfully, <code>false</code> if the recovery
     *                 has been ignored due to a concurrent modification of if the clusters state has changed due to async updates.
     *                 从仓库中恢复数据
     */
    void recoverFromRepository(final IndexShard indexShard, Repository repository, ActionListener<Boolean> listener) {
        try {
            if (canRecover(indexShard)) {
                RecoverySource.Type recoveryType = indexShard.recoveryState().getRecoverySource().getType();
                // 要求恢复类型为快照
                assert recoveryType == RecoverySource.Type.SNAPSHOT : "expected snapshot recovery type: " + recoveryType;
                SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) indexShard.recoveryState().getRecoverySource();
                restore(indexShard, repository, recoverySource, recoveryListener(indexShard, listener));
            } else {
                listener.onResponse(false);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * 先检测是否有必要恢复该indexShard的数据
     * @param indexShard
     * @return
     */
    private boolean canRecover(IndexShard indexShard) {
        // 已经被关闭了就不需要恢复了
        if (indexShard.state() == IndexShardState.CLOSED) {
            // got closed on us, just ignore this recovery
            return false;
        }
        // 只有主分片才有恢复的必要  副本应该是从主分片获取数据的
        if (indexShard.routingEntry().primary() == false) {
            throw new IndexShardRecoveryException(shardId, "Trying to recover when the shard is in backup state", null);
        }
        return true;
    }

    private ActionListener<Boolean> recoveryListener(IndexShard indexShard, ActionListener<Boolean> listener) {
        return ActionListener.wrap(res -> {
            if (res) {
                // Check that the gateway didn't leave the shard in init or recovering stage. it is up to the gateway
                // to call post recovery.
                final IndexShardState shardState = indexShard.state();
                final RecoveryState recoveryState = indexShard.recoveryState();
                assert shardState != IndexShardState.CREATED && shardState != IndexShardState.RECOVERING :
                    "recovery process of " + shardId + " didn't get to post_recovery. shardState [" + shardState + "]";

                if (logger.isTraceEnabled()) {
                    RecoveryState.Index index = recoveryState.getIndex();
                    StringBuilder sb = new StringBuilder();
                    sb.append("    index    : files           [").append(index.totalFileCount()).append("] with total_size [")
                        .append(new ByteSizeValue(index.totalBytes())).append("], took[")
                        .append(TimeValue.timeValueMillis(index.time())).append("]\n");
                    sb.append("             : recovered_files [").append(index.recoveredFileCount()).append("] with total_size [")
                        .append(new ByteSizeValue(index.recoveredBytes())).append("]\n");
                    sb.append("             : reusing_files   [").append(index.reusedFileCount()).append("] with total_size [")
                        .append(new ByteSizeValue(index.reusedBytes())).append("]\n");
                    sb.append("    verify_index    : took [")
                        .append(TimeValue.timeValueMillis(recoveryState.getVerifyIndex().time())).append("], check_index [")
                        .append(timeValueMillis(recoveryState.getVerifyIndex().checkIndexTime())).append("]\n");
                    sb.append("    translog : number_of_operations [").append(recoveryState.getTranslog().recoveredOperations())
                        .append("], took [").append(TimeValue.timeValueMillis(recoveryState.getTranslog().time())).append("]");
                    logger.trace("recovery completed from [shard_store], took [{}]\n{}",
                        timeValueMillis(recoveryState.getTimer().time()), sb);
                } else if (logger.isDebugEnabled()) {
                    logger.debug("recovery completed from [shard_store], took [{}]", timeValueMillis(recoveryState.getTimer().time()));
                }
            }
            listener.onResponse(res);
        }, ex -> {
            if (ex instanceof IndexShardRecoveryException) {
                if (indexShard.state() == IndexShardState.CLOSED) {
                    // got closed on us, just ignore this recovery
                    listener.onResponse(false);
                    return;
                }
                if ((ex.getCause() instanceof IndexShardClosedException) || (ex.getCause() instanceof IndexShardNotStartedException)) {
                    // got closed on us, just ignore this recovery
                    listener.onResponse(false);
                    return;
                }
                listener.onFailure(ex);
            } else if (ex instanceof IndexShardClosedException || ex instanceof IndexShardNotStartedException) {
                listener.onResponse(false);
            } else {
                if (indexShard.state() == IndexShardState.CLOSED) {
                    // got closed on us, just ignore this recovery
                    listener.onResponse(false);
                } else {
                    listener.onFailure(new IndexShardRecoveryException(shardId, "failed recovery", ex));
                }
            }
        });
    }

    /**
     * Recovers the state of the shard from the store.
     * 从store中恢复数据
     */
    private void internalRecoverFromStore(IndexShard indexShard) throws IndexShardRecoveryException {
        // 执行恢复前置钩子 当前默认为空
        indexShard.preRecovery();
        // 获取描述此时恢复状态的对象   对于分片来说只有一个 恢复中的状态 而更加详细的状态就需要通过 recoveryState来描述
        final RecoveryState recoveryState = indexShard.recoveryState();
        // EmptyStore 代表希望从空的仓库恢复数据
        final boolean indexShouldExists = recoveryState.getRecoverySource().getType() != RecoverySource.Type.EMPTY_STORE;
        // 将recoveryState 修改成 INDEX
        indexShard.prepareForIndexRecovery();
        SegmentInfos si = null;
        // 每个分片在创建时会占用一个目录 并将它包装成store
        final Store store = indexShard.store();
        store.incRef();
        try {
            try {
                // 检查dir下是否已经出现了记录异常信息的文件 如果有的话 代表之前出现了某种错误 就无法正常进行数据恢复了
                store.failIfCorrupted();
                try {
                    // 获取该目录下最后一个segment信息   就是获取最大的segment_N 文件 并解析成 segmentInfos对象
                    // 每当执行一次刷盘操作 应该都会生成一个segment_N 文件
                    si = store.readLastCommittedSegmentsInfo();
                } catch (Exception e) {
                    String files = "_unknown_";
                    try {
                        files = Arrays.toString(store.directory().listAll());
                    } catch (Exception inner) {
                        files += " (failure=" + ExceptionsHelper.stackTrace(inner) + ")";
                    }
                    if (indexShouldExists) {
                        throw new IndexShardRecoveryException(shardId,
                            "shard allocated for local recovery (post api), should exist, but doesn't, current files: " + files, e);
                    }
                }
                // 如果存在数据 但是指明了 RecoverySource 应该为Empty 那么清理之前的数据
                if (si != null && indexShouldExists == false) {
                    // it exists on the directory, but shouldn't exist on the FS, its a leftover (possibly dangling)
                    // its a "new index create" API, we have to do something, so better to clean it than use same data
                    logger.trace("cleaning existing shard, shouldn't exists");
                    Lucene.cleanLuceneIndex(store.directory());
                    si = null;
                }
            } catch (Exception e) {
                throw new IndexShardRecoveryException(shardId, "failed to fetch index version after copying it over", e);
            }
            // 代表尝试通过 本地分片进行恢复
            if (recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
                assert indexShouldExists;
                // 清空事务日志目录下所有的文件  并生成一个新的检查点文件/事务文件   同时将historyUUID/transportUUID 追加到segment_N.userData中
                bootstrap(indexShard, store);
                // 将续约信息写入到文件中
                writeEmptyRetentionLeasesFile(indexShard);
                // 其余不为 Empty_Store的类型   这种情况 代表之前事务日志中有数据 并且肯定已经存储到userData中了
            } else if (indexShouldExists) {
                // 当使用 EXISTING_STORE 作为恢复源时  要求必须更新historyUUID    与上面Local_Shard的区别是  不需要清除事务日志文件以及创建空文件
                if (recoveryState.getRecoverySource().shouldBootstrapNewHistoryUUID()) {
                    store.bootstrapNewHistory();
                    // 这里也要将续约信息写入到文件中
                    writeEmptyRetentionLeasesFile(indexShard);
                }
                // since we recover from local, just fill the files and size
                try {
                    // 获取recoveryState内部的临时index对象 并将相关信息填充进去
                    final RecoveryState.Index index = recoveryState.getIndex();
                    // 当存在 segment信息时 将相关文件都设置到 RecoveryState.Index 内
                    // 如果之前没有commit 则不会填充文件
                    if (si != null) {
                        addRecoveredFileDetails(si, store, index);
                    }
                } catch (IOException e) {
                    logger.debug("failed to list file details", e);
                }
            } else {
                // 代表是 Empty_Store 类型
                // 生成新的segmentWriter对象  这里localCheckpoint/seq 都是默认值(-1)
                store.createEmpty(indexShard.indexSettings().getIndexVersionCreated().luceneVersion);
                // 创建空的事务日志文件 以及checkPoint文件
                final String translogUUID = Translog.createEmptyTranslog(
                    indexShard.shardPath().resolveTranslog(), SequenceNumbers.NO_OPS_PERFORMED, shardId,
                    indexShard.getPendingPrimaryTerm());
                // 将最新的事务id设置到 segment_N 文件中
                store.associateIndexWithNewTranslog(translogUUID);
                // 将一个空的续约信息持久化
                writeEmptyRetentionLeasesFile(indexShard);
            }
            // 开启引擎对象 并从事务日志中恢复数据
            indexShard.openEngineAndRecoverFromTranslog();
            // seq出现不连续的情况  使用NOOP进行填充
            indexShard.getEngine().fillSeqNoGaps(indexShard.getPendingPrimaryTerm());
            // 整个恢复流程结束
            indexShard.finalizeRecovery();
            // 触发后置钩子
            indexShard.postRecovery("post recovery from shard_store");
        } catch (EngineException | IOException e) {
            throw new IndexShardRecoveryException(shardId, "failed to recover from gateway", e);
        } finally {
            store.decRef();
        }
    }

    /**
     * 将空的续约信息写入到文件中
     * @param indexShard
     * @throws IOException
     */
    private static void writeEmptyRetentionLeasesFile(IndexShard indexShard) throws IOException {
        assert indexShard.getRetentionLeases().leases().isEmpty() : indexShard.getRetentionLeases(); // not loaded yet
        // 将此时 ReplicationTracker内部的续约信息持久化
        indexShard.persistRetentionLeases();
        assert indexShard.loadRetentionLeases().leases().isEmpty();
    }

    /**
     * 添加描述恢复文件详情
     * @param si   记录某次commit时 所有segment的信息  对应segment_N文件
     * @param store
     * @param index
     * @throws IOException
     */
    private void addRecoveredFileDetails(SegmentInfos si, Store store, RecoveryState.Index index) throws IOException {
        final Directory directory = store.directory();
        // 获取本次lucene涉及到的所有文件
        for (String name : Lucene.files(si)) {
            long length = directory.fileLength(name);
            // 将每个文件的长度信息设置进去
            index.addFileDetail(name, length, true);
        }
    }

    /**
     * Restores shard from {@link SnapshotRecoverySource} associated with this shard in routing table
     * 从 repository中恢复数据
     */
    private void restore(IndexShard indexShard, Repository repository, SnapshotRecoverySource restoreSource,
                         ActionListener<Boolean> listener) {
        logger.debug("restoring from {} ...", indexShard.recoveryState().getRecoverySource());
        indexShard.preRecovery();
        final RecoveryState.Translog translogState = indexShard.recoveryState().getTranslog();
        if (restoreSource == null) {
            listener.onFailure(new IndexShardRestoreFailedException(shardId, "empty restore source"));
            return;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] restoring shard [{}]", restoreSource.snapshot(), shardId);
        }
        final ActionListener<Void> restoreListener = ActionListener.wrap(
            v -> {
                // 这里恢复的一套流程和 fromStore是一样的
                final Store store = indexShard.store();
                bootstrap(indexShard, store);
                assert indexShard.shardRouting.primary() : "only primary shards can recover from store";
                writeEmptyRetentionLeasesFile(indexShard);
                indexShard.openEngineAndRecoverFromTranslog();
                indexShard.getEngine().fillSeqNoGaps(indexShard.getPendingPrimaryTerm());
                indexShard.finalizeRecovery();
                indexShard.postRecovery("restore done");
                listener.onResponse(true);
            }, e -> listener.onFailure(new IndexShardRestoreFailedException(shardId, "restore failed", e))
        );
        try {
            translogState.totalOperations(0);
            translogState.totalOperationsOnStart(0);
            indexShard.prepareForIndexRecovery();
            final ShardId snapshotShardId;
            final IndexId indexId = restoreSource.index();
            if (shardId.getIndexName().equals(indexId.getName())) {
                snapshotShardId = shardId;
            } else {
                snapshotShardId = new ShardId(indexId.getName(), IndexMetadata.INDEX_UUID_NA_VALUE, shardId.id());
            }
            final StepListener<IndexId> indexIdListener = new StepListener<>();
            // If the index UUID was not found in the recovery source we will have to load RepositoryData and resolve it by index name
            // TODO
            if (indexId.getId().equals(IndexMetadata.INDEX_UUID_NA_VALUE)) {
                // BwC path, running against an old version master that did not add the IndexId to the recovery source
                repository.getRepositoryData(ActionListener.map(
                    indexIdListener, repositoryData -> repositoryData.resolveIndexId(indexId.getName())));
            } else {
                indexIdListener.onResponse(indexId);
            }
            assert indexShard.getEngineOrNull() == null;
            // 使用 index触发监听器
            indexIdListener.whenComplete(idx -> repository.restoreShard(indexShard.store(), restoreSource.snapshot().getSnapshotId(),
                idx, snapshotShardId, indexShard.recoveryState(), restoreListener), restoreListener::onFailure);
        } catch (Exception e) {
            restoreListener.onFailure(e);
        }
    }

    /**
     * 进行引导
     * @param indexShard
     * @param store
     * @throws IOException
     */
    private void bootstrap(final IndexShard indexShard, final Store store) throws IOException {
        // 更新最后一个segment_N 文件中的 userData.historyUUID
        store.bootstrapNewHistory();
        // 获取最新的segment信息
        final SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
        // 获取之前写入的本地检查点
        final long localCheckpoint = Long.parseLong(segmentInfos.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
        // 清空之前所有的事务信息 并生成一个空事务文件
        final String translogUUID = Translog.createEmptyTranslog(
            indexShard.shardPath().resolveTranslog(), localCheckpoint, shardId, indexShard.getPendingPrimaryTerm());
        // 在写入事务文件后会返回一个事务相关的uuid  将其更新到segment_N中
        store.associateIndexWithNewTranslog(translogUUID);
    }
}
