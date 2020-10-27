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
package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * This context will execute a file restore of the lucene files. It is primarily designed to be used to
 * restore from some form of a snapshot. It will setup a new store, identify files that need to be copied
 * for the source, and perform the copies. Implementers must implement the functionality of opening the
 * underlying file streams for snapshotted lucene file.
 * 进行文件恢复时的上下文对象
 */
public abstract class FileRestoreContext {

    protected static final Logger logger = LogManager.getLogger(FileRestoreContext.class);

    protected final String repositoryName;
    protected final RecoveryState recoveryState;
    protected final SnapshotId snapshotId;
    protected final ShardId shardId;

    /**
     * Constructs new restore context
     *
     * @param shardId       shard id to restore into  本次还原数据时对应的存储层实现
     * @param snapshotId    snapshot id   从哪个快照中还原
     * @param recoveryState recovery state to report progress   描述此时还原的进度等信息
     */
    protected FileRestoreContext(String repositoryName, ShardId shardId, SnapshotId snapshotId, RecoveryState recoveryState) {
        this.repositoryName = repositoryName;
        this.recoveryState = recoveryState;
        this.snapshotId = snapshotId;
        this.shardId = shardId;
    }

    /**
     * Performs restore operation
     * 开始还原数据
     */
    public void restore(SnapshotFiles snapshotFiles, Store store, ActionListener<Void> listener) {
        store.incRef();
        try {
            logger.debug("[{}] [{}] restoring to [{}] ...", snapshotId, repositoryName, shardId);
            Store.MetadataSnapshot recoveryTargetMetadata;
            try {
                // this will throw an IOException if the store has no segments infos file. The
                // store can still have existing files but they will be deleted just before being
                // restored.
                // 从store对应的目录下解析元数据信息
                recoveryTargetMetadata = store.getMetadata(null, true);
            } catch (org.apache.lucene.index.IndexNotFoundException e) {
                // happens when restore to an empty shard, not a big deal
                logger.trace("[{}] [{}] restoring from to an empty shard", shardId, snapshotId);
                recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
            } catch (IOException e) {
                logger.warn(new ParameterizedMessage("[{}] [{}] Can't read metadata from store, will not reuse local files during restore",
                    shardId, snapshotId), e);
                recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
            }

            // 本次实际恢复的所有文件
            final List<BlobStoreIndexShardSnapshot.FileInfo> filesToRecover = new ArrayList<>();
            final Map<String, StoreFileMetadata> snapshotMetadata = new HashMap<>();
            final Map<String, BlobStoreIndexShardSnapshot.FileInfo> fileInfos = new HashMap<>();

            // 将本次快照相关的文件信息存储到map中
            for (final BlobStoreIndexShardSnapshot.FileInfo fileInfo : snapshotFiles.indexFiles()) {
                snapshotMetadata.put(fileInfo.metadata().name(), fileInfo.metadata());
                fileInfos.put(fileInfo.metadata().name(), fileInfo);
            }

            final Store.MetadataSnapshot sourceMetadata = new Store.MetadataSnapshot(unmodifiableMap(snapshotMetadata), emptyMap(), 0);

            // 因为快照文件可以理解为普通文件的副本  在这些快照文件中也应该会存在 segment_N 的副本文件
            final StoreFileMetadata restoredSegmentsFile = sourceMetadata.getSegmentsFile();
            if (restoredSegmentsFile == null) {
                throw new IndexShardRestoreFailedException(shardId, "Snapshot has no segments file");
            }

            // 生成diff对象
            final Store.RecoveryDiff diff = sourceMetadata.recoveryDiff(recoveryTargetMetadata);
            // 一致的文件应该是不需要恢复的吧  restore代表还原 那么也就是可以回退到之前的状态了???
            for (StoreFileMetadata md : diff.identical) {
                BlobStoreIndexShardSnapshot.FileInfo fileInfo = fileInfos.get(md.name());
                // 因为这些文件没有发生变化  直接插入到recoveryState中 同时传入的 reused 为true
                recoveryState.getIndex().addFileDetail(fileInfo.physicalName(), fileInfo.length(), true);
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] [{}] not_recovering file [{}] from [{}], exists in local store and is same", shardId, snapshotId,
                        fileInfo.physicalName(), fileInfo.name());
                }
            }

            // 每个恢复的结果都会存储到 recoveryState中
            // concat() 找到 diff中所有 diff或者miss 的对象 也就代表这些文件发生了变化 需要改变
            for (StoreFileMetadata md : concat(diff)) {
                BlobStoreIndexShardSnapshot.FileInfo fileInfo = fileInfos.get(md.name());
                filesToRecover.add(fileInfo);
                // 这些文件还没有完成恢复 所以 reused为false
                recoveryState.getIndex().addFileDetail(fileInfo.physicalName(), fileInfo.length(), false);
                if (logger.isTraceEnabled()) {
                    logger.trace("[{}] [{}] recovering [{}] from [{}]", shardId, snapshotId,
                        fileInfo.physicalName(), fileInfo.name());
                }
            }

            // 代表本次不需要从快照中恢复数据 快照文件与当前正在使用的文件一致
            if (filesToRecover.isEmpty()) {
                logger.trace("[{}] [{}] no files to recover, all exist within the local store", shardId, snapshotId);
            }

            try {
                // list of all existing store files
                final List<String> deleteIfExistFiles = Arrays.asList(store.directory().listAll());

                for (final BlobStoreIndexShardSnapshot.FileInfo fileToRecover : filesToRecover) {
                    // if a file with a same physical name already exist in the store we need to delete it
                    // before restoring it from the snapshot. We could be lenient and try to reuse the existing
                    // store files (and compare their names/length/checksum again with the snapshot files) but to
                    // avoid extra complexity we simply delete them and restore them again like StoreRecovery
                    // does with dangling indices. Any existing store file that is not restored from the snapshot
                    // will be clean up by RecoveryTarget.cleanFiles().
                    final String physicalName = fileToRecover.physicalName();
                    if (deleteIfExistFiles.contains(physicalName)) {
                        logger.trace("[{}] [{}] deleting pre-existing file [{}]", shardId, snapshotId, physicalName);
                        // 对应diff 的场景 要删除旧文件
                        store.directory().deleteFile(physicalName);
                    }
                }

                // filesToRecover 内部的文件都是需要恢复的
                restoreFiles(filesToRecover, store, ActionListener.wrap(
                    v -> {
                        store.incRef();
                        try {
                            afterRestore(snapshotFiles, store, restoredSegmentsFile);
                            listener.onResponse(null);
                        } finally {
                            store.decRef();
                        }
                    }, listener::onFailure));
            } catch (IOException ex) {
                throw new IndexShardRestoreFailedException(shardId, "Failed to recover index", ex);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            store.decRef();
        }
    }

    /**
     * 当restore完成时 会触发后置函数
     * @param snapshotFiles
     * @param store
     * @param restoredSegmentsFile
     */
    private void afterRestore(SnapshotFiles snapshotFiles, Store store, StoreFileMetadata restoredSegmentsFile) {
        // read the snapshot data persisted
        try {
            Lucene.pruneUnreferencedFiles(restoredSegmentsFile.name(), store.directory());
        } catch (IOException e) {
            throw new IndexShardRestoreFailedException(shardId, "Failed to fetch index version after copying it over", e);
        }

        /// now, go over and clean files that are in the store, but were not in the snapshot
        try {
            for (String storeFile : store.directory().listAll()) {
                // 什么叫物理文件???
                if (Store.isAutogenerated(storeFile) || snapshotFiles.containPhysicalIndexFile(storeFile)) {
                    continue; //skip write.lock, checksum files and files that exist in the snapshot
                }
                try {
                    // 删除restore文件 以及当前快照文件  TODO 在这个时候删除 如果之后又重启了怎么办 快照数据会丢失么
                    store.deleteQuiet("restore", storeFile);
                    // 这里为什么又删除一次
                    store.directory().deleteFile(storeFile);
                } catch (IOException e) {
                    logger.warn("[{}] [{}] failed to delete file [{}] during snapshot cleanup", shardId, snapshotId, storeFile);
                }
            }
        } catch (IOException e) {
            logger.warn("[{}] [{}] failed to list directory - some of files might not be deleted", shardId, snapshotId);
        }
    }

    /**
     * Restores given list of {@link BlobStoreIndexShardSnapshot.FileInfo} to the given {@link Store}.
     *
     * @param filesToRecover List of files to restore
     * @param store          Store to restore into
     */
    protected abstract void restoreFiles(List<BlobStoreIndexShardSnapshot.FileInfo> filesToRecover, Store store,
                                         ActionListener<Void> listener);

    @SuppressWarnings("unchecked")
    private static Iterable<StoreFileMetadata> concat(Store.RecoveryDiff diff) {
        return Iterables.concat(diff.different, diff.missing);
    }
}
