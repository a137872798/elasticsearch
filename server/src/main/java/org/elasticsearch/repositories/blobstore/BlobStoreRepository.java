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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.RateLimitingInputStream;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryOperation;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo.canonicalName;

/**
 * BlobStore - based implementation of Snapshot Repository
 * <p>
 * This repository works with any {@link BlobStore} implementation. The blobStore could be (and preferred) lazy initialized in
 * {@link #createBlobStore()}.
 * </p>
 * For in depth documentation on how exactly implementations of this class interact with the snapshot functionality please refer to the
 * documentation of the package {@link org.elasticsearch.repositories.blobstore}.
 * 代表大块的数据存储
 */
public abstract class BlobStoreRepository extends AbstractLifecycleComponent implements Repository {
    private static final Logger logger = LogManager.getLogger(BlobStoreRepository.class);

    /**
     * 每个存储对象都会关联一个元数据
     */
    protected volatile RepositoryMetadata metadata;

    /**
     * 通过线程池对象 将任务合理拆解成并行执行
     */
    protected final ThreadPool threadPool;

    private static final int BUFFER_SIZE = 4096;

    public static final String SNAPSHOT_PREFIX = "snap-";

    public static final String SNAPSHOT_CODEC = "snapshot";

    public static final String INDEX_FILE_PREFIX = "index-";

    public static final String INDEX_LATEST_BLOB = "index.latest";

    private static final String TESTS_FILE = "tests-";

    public static final String METADATA_PREFIX = "meta-";

    public static final String METADATA_NAME_FORMAT = METADATA_PREFIX + "%s.dat";

    private static final String METADATA_CODEC = "metadata";

    private static final String INDEX_METADATA_CODEC = "index-metadata";

    public static final String SNAPSHOT_NAME_FORMAT = SNAPSHOT_PREFIX + "%s.dat";

    private static final String SNAPSHOT_INDEX_PREFIX = "index-";

    private static final String SNAPSHOT_INDEX_NAME_FORMAT = SNAPSHOT_INDEX_PREFIX + "%s";

    private static final String SNAPSHOT_INDEX_CODEC = "snapshots";

    private static final String UPLOADED_DATA_BLOB_PREFIX = "__";

    /**
     * Prefix used for the identifiers of data blobs that were not actually written to the repository physically because their contents are
     * already stored in the metadata referencing them, i.e. in {@link BlobStoreIndexShardSnapshot} and
     * {@link BlobStoreIndexShardSnapshots}. This is the case for files for which {@link StoreFileMetadata#hashEqualsContents()} is
     * {@code true}.
     */
    private static final String VIRTUAL_DATA_BLOB_PREFIX = "v__";

    /**
     * When set to true metadata files are stored in compressed format. This setting doesn’t affect index
     * files that are already compressed by default. Changing the setting does not invalidate existing files since reads
     * do not observe the setting, instead they examine the file to see if it is compressed or not.
     */
    public static final Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", true, Setting.Property.NodeScope);

    /**
     * Setting to disable caching of the latest repository data.
     */
    public static final Setting<Boolean> CACHE_REPOSITORY_DATA =
        Setting.boolSetting("cache_repository_data", true, Setting.Property.Deprecated);

    private final boolean compress;

    /**
     * 是否缓存存储层的数据
     */
    private final boolean cacheRepositoryData;

    /**
     * 果然是lucene的限流器  会根据限流值 缓慢的将数据写入到FS系统中
     */
    private final RateLimiter snapshotRateLimiter;

    private final RateLimiter restoreRateLimiter;

    /**
     * 这些计数器对象是用来标明 在每个纳秒内执行了多少次写入???
     */
    private final CounterMetric snapshotRateLimitingTimeInNanos = new CounterMetric();

    private final CounterMetric restoreRateLimitingTimeInNanos = new CounterMetric();

    private final ChecksumBlobStoreFormat<Metadata> globalMetadataFormat;

    private final ChecksumBlobStoreFormat<IndexMetadata> indexMetadataFormat;

    /**
     * 格式化对象定义了如何从数据流还原成pojo类
     */
    protected final ChecksumBlobStoreFormat<SnapshotInfo> snapshotFormat;

    private final boolean readOnly;

    private final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshot> indexShardSnapshotFormat;

    private final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshots> indexShardSnapshotsFormat;

    private final Object lock = new Object();

    private final SetOnce<BlobContainer> blobContainer = new SetOnce<>();

    /**
     * 一个存储服务只对应一个仓库  仓库下还有各种子级容器
     */
    private final SetOnce<BlobStore> blobStore = new SetOnce<>();

    private final BlobPath basePath;

    private final ClusterService clusterService;

    /**
     * Flag that is set to {@code true} if this instance is started with {@link #metadata} that has a higher value for
     * {@link RepositoryMetadata#pendingGeneration()} than for {@link RepositoryMetadata#generation()} indicating a full cluster restart
     * potentially accounting for the the last {@code index-N} write in the cluster state.
     * Note: While it is true that this value could also be set to {@code true} for an instance on a node that is just joining the cluster
     * during a new {@code index-N} write, this does not present a problem. The node will still load the correct {@link RepositoryData} in
     * all cases and simply do a redundant listing of the repository contents if it tries to load {@link RepositoryData} and falls back
     * to {@link #latestIndexBlobId()} to validate the value of {@link RepositoryMetadata#generation()}.
     * 更新repositoryData 分为2步 第一步更新pendingGen 第二步 更新gen  在执行完第一步后会存在leader宕机 并切换的可能
     */
    private boolean uncleanStart;

    /**
     * This flag indicates that the repository can not exclusively rely on the value stored in {@link #latestKnownRepoGen} to determine the
     * latest repository generation but must inspect its physical contents as well via {@link #latestIndexBlobId()}.
     * This flag is set in the following situations:
     * <ul>
     *     <li>All repositories that are read-only, i.e. for which {@link #isReadOnly()} returns {@code true} because there are no
     *     guarantees that another cluster is not writing to the repository at the same time</li>
     *     <li>The value of {@link RepositoryMetadata#generation()} for this repository is {@link RepositoryData#UNKNOWN_REPO_GEN}
     *     indicating that no consistent repository generation is tracked in the cluster state yet.</li>
     *     <li>The {@link #uncleanStart} flag is set to {@code true}</li>
     * </ul>
     */
    private volatile boolean bestEffortConsistency;

    /**
     * Constructs new BlobStoreRepository
     * @param metadata   The metadata for this repository including name and settings  描述该存储层的元数据信息
     * @param clusterService ClusterService
     * @param basePath 描述数据的存储路径
     */
    protected BlobStoreRepository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry namedXContentRegistry,
        final ClusterService clusterService,
        final BlobPath basePath) {
        this.metadata = metadata;
        this.threadPool = clusterService.getClusterApplierService().threadPool();
        this.clusterService = clusterService;
        // 是否采用压缩方式存储数据流
        this.compress = COMPRESS_SETTING.get(metadata.settings());
        // 针对生成快照 以及恢复操作 生成2个限流器
        snapshotRateLimiter = getRateLimiter(metadata.settings(), "max_snapshot_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));
        restoreRateLimiter = getRateLimiter(metadata.settings(), "max_restore_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));

        // 内部的数据是否只读
        readOnly = metadata.settings().getAsBoolean("readonly", false);
        // 是否缓存存储层的数据
        cacheRepositoryData = CACHE_REPOSITORY_DATA.get(metadata.settings());
        this.basePath = basePath;

        // 这些对象负责将数据流格式化  XXX_NAME_FORMAT 代表着 blobName 也是格式化的 通过snapshot.uuid 替换关键字后 可以作为blobName
        // uuid 在每次生成的快照动作中是唯一的  每次快照会创建一些数据体 每个数据体的文件名 都有一个模板
        indexShardSnapshotFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_CODEC, SNAPSHOT_NAME_FORMAT,
            BlobStoreIndexShardSnapshot::fromXContent, namedXContentRegistry, compress);
        indexShardSnapshotsFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_INDEX_CODEC, SNAPSHOT_INDEX_NAME_FORMAT,
            BlobStoreIndexShardSnapshots::fromXContent, namedXContentRegistry, compress);
        globalMetadataFormat = new ChecksumBlobStoreFormat<>(METADATA_CODEC, METADATA_NAME_FORMAT,
            Metadata::fromXContent, namedXContentRegistry, compress);
        indexMetadataFormat = new ChecksumBlobStoreFormat<>(INDEX_METADATA_CODEC, METADATA_NAME_FORMAT,
            IndexMetadata::fromXContent, namedXContentRegistry, compress);
        snapshotFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_CODEC, SNAPSHOT_NAME_FORMAT,
            SnapshotInfo::fromXContentInternal, namedXContentRegistry, compress);
    }

    /**
     * 当存储层组件初始化时 触发该方法
     */
    @Override
    protected void doStart() {

        // 代表上一次更新repositoryData的任务被中断
        uncleanStart = metadata.pendingGeneration() > RepositoryData.EMPTY_REPO_GEN &&
            metadata.generation() != metadata.pendingGeneration();

        // 校验chunk大小是否合法
        ByteSizeValue chunkSize = chunkSize();
        if (chunkSize != null && chunkSize.getBytes() <= 0) {
            throw new IllegalArgumentException("the chunk size cannot be negative: [" + chunkSize + "]");
        }
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
        BlobStore store;
        // to close blobStore if blobStore initialization is started during close
        synchronized (lock) {
            store = blobStore.get();
        }
        if (store != null) {
            try {
                store.close();
            } catch (Exception t) {
                logger.warn("cannot close blob store", t);
            }
        }
    }

    /**
     * @param createUpdateTask function to supply cluster state update task
     * @param source           the source of the cluster state update task
     * @param onFailure        error handler invoked on failure to get a consistent view of the current {@link RepositoryData}
     */
    @Override
    public void executeConsistentStateUpdate(Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask, String source,
                                             Consumer<Exception> onFailure) {
        threadPool.generic().execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                // 获取当前存储层实现对应的元数据  一个存储系统可能同时使用了多个存储层
                final RepositoryMetadata repositoryMetadataStart = metadata;
                // 从缓存中 获取最新的index-n 中获取数据并触发监听器
                getRepositoryData(ActionListener.wrap(repositoryData -> {
                    // 根据此时最新的 存储数据 生成一个更新clusterState的任务 并通过集群服务发布
                    final ClusterStateUpdateTask updateTask = createUpdateTask.apply(repositoryData);

                    clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask(updateTask.priority()) {

                        /**
                         * 是否真正执行了任务
                         */
                        private boolean executedTask = false;

                        /**
                         * 如何将之前的clusteState 更新
                         * @param currentState
                         * @return
                         * @throws Exception
                         */
                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            // Comparing the full metadata here on purpose instead of simply comparing the safe generation.
                            // If the safe generation has changed, then we have to reload repository data and start over.
                            // If the pending generation has changed we are in the midst of a write operation and might pick up the
                            // updated repository data and state on the retry. We don't want to wait for the write to finish though
                            // because it could fail for any number of reasons so we just retry instead of waiting on the cluster state
                            // to change in any form.

                            // 代表仓库元数据状态在前后没有被修改 如果被修改了 那么实际上repository对象会重启 本次任务就不会作用到新的仓库实例上
                            if (repositoryMetadataStart.equals(getRepoMetadata(currentState))) {
                                executedTask = true;
                                return updateTask.execute(currentState);
                            }
                            return currentState;
                        }

                        @Override
                        public void onFailure(String source, Exception e) {

                            if (executedTask) {
                                updateTask.onFailure(source, e);
                            } else {
                                onFailure.accept(e);
                            }
                        }

                        /**
                         * 当任务成功执行时触发
                         * @param source
                         * @param oldState
                         * @param newState
                         */
                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            if (executedTask) {
                                updateTask.clusterStateProcessed(source, oldState, newState);
                            } else {
                                // 因为repository实例发生了更新 所以重新触发一次任务
                                executeConsistentStateUpdate(createUpdateTask, source, onFailure);
                            }
                        }

                        @Override
                        public TimeValue timeout() {
                            return updateTask.timeout();
                        }
                    });
                }, onFailure));
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.accept(e);
            }
        });
    }

    // Inspects all cluster state elements that contain a hint about what the current repository generation is and updates
    // #latestKnownRepoGen if a newer than currently known generation is found
    // 当集群状态发生变化时 会触发该函数
    @Override
    public void updateState(ClusterState state) {
        // 从当前clusterState中 获取有关存储的元数据信息
        metadata = getRepoMetadata(state);
        // 代表上次的repositoryData存储工作未完成
        uncleanStart = uncleanStart && metadata.generation() != metadata.pendingGeneration();

        // 在上次数据未正常写入的情况下无法确定当前最新的数据  所以总是尽最大努力实现一致性 就体现在每次从数据源获取数据 而不是走缓存层
        bestEffortConsistency = uncleanStart || isReadOnly() || metadata.generation() == RepositoryData.UNKNOWN_REPO_GEN;
        // isReadOnly 默认为false
        if (isReadOnly()) {
            // No need to waste cycles, no operations can run against a read-only repository
            return;
        }

        // 对应的快照任务在创建时 就会获取那时候leader下repositoryData的gen 这里就是进行回查
        if (bestEffortConsistency) {

            // 每次仓库的变化 应该都会更新gen

            long bestGenerationFromCS = RepositoryData.EMPTY_REPO_GEN;
            // 该对象描述了 所有正在执行的快照任务
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            if (snapshotsInProgress != null) {
                // 从所有正在执行的快照任务中 挑选最新的任务对应的gen
                bestGenerationFromCS = bestGeneration(snapshotsInProgress.entries());
            }

            // 当快照任务不存在时  尝试从删除快照的任务中查询 gen
            final SnapshotDeletionsInProgress deletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            // Don't use generation from the delete task if we already found a generation for an in progress snapshot.
            // In this case, the generation points at the generation the repo will be in after the snapshot finishes so it may not yet
            // exist
            if (bestGenerationFromCS == RepositoryData.EMPTY_REPO_GEN && deletionsInProgress != null) {
                bestGenerationFromCS = bestGeneration(deletionsInProgress.getEntries());
            }

            // 快照不应该长期存储在仓库中 当已经没有使用价值时 可以通过 repositoryCleanup 任务 将旧数据清理
            final RepositoryCleanupInProgress cleanupInProgress = state.custom(RepositoryCleanupInProgress.TYPE);
            if (bestGenerationFromCS == RepositoryData.EMPTY_REPO_GEN && cleanupInProgress != null) {
                bestGenerationFromCS = bestGeneration(cleanupInProgress.entries());
            }

            final long finalBestGen = Math.max(bestGenerationFromCS, metadata.generation());
            // 更新当前感知到的最大的gen
            latestKnownRepoGen.updateAndGet(known -> Math.max(known, finalBestGen));
            // 这里只是更新 lastestKnownGen  并没有做实际的写入操作
        } else {
            // 非强一致性场景下 直接使用metadata的数据就可以
            final long previousBest = latestKnownRepoGen.getAndSet(metadata.generation());
            if (previousBest != metadata.generation()) {
                assert metadata.generation() == RepositoryData.CORRUPTED_REPO_GEN || previousBest < metadata.generation() :
                    "Illegal move from repository generation [" + previousBest + "] to generation [" + metadata.generation() + "]";
                logger.debug("Updated repository generation from [{}] to [{}]", previousBest, metadata.generation());
            }
        }
    }

    /**
     * 从一组正在执行的 operation中找到最合适的gen
     * RepositoryOperation 抽象出了一种针对仓库的操作 比如生成快照
     * @param operations
     * @return
     */
    private long bestGeneration(Collection<? extends RepositoryOperation> operations) {
        // 获取当前存储实例的名称
        final String repoName = metadata.name();
        assert operations.size() <= 1 : "Assumed one or no operations but received " + operations;
        return operations.stream().filter(e -> e.repository().equals(repoName)).mapToLong(RepositoryOperation::repositoryStateId)
            .max().orElse(RepositoryData.EMPTY_REPO_GEN);
    }

    public ThreadPool threadPool() {
        return threadPool;
    }

    // package private, only use for testing
    BlobContainer getBlobContainer() {
        return blobContainer.get();
    }

    // for test purposes only
    protected BlobStore getBlobStore() {
        return blobStore.get();
    }

    /**
     * maintains single lazy instance of {@link BlobContainer}
     * 什么对象需要惰性生成呢  一般是大对象
     */
    protected BlobContainer blobContainer() {
        assertSnapshotOrGenericThread();

        BlobContainer blobContainer = this.blobContainer.get();
        if (blobContainer == null) {
           synchronized (lock) {

               blobContainer = this.blobContainer.get();
               if (blobContainer == null) {
                   // 通过basePath + store 构构建基础的container 并且在该对象上可以通过追加路径生成子级container
                   // container 本身是一个树结构
                   blobContainer = blobStore().blobContainer(basePath());
                   this.blobContainer.set(blobContainer);
               }
           }
        }

        return blobContainer;
    }

    /**
     * Maintains single lazy instance of {@link BlobStore}.
     * Public for testing.
     * store对象也是惰性加载的
     */
    public BlobStore blobStore() {
        assertSnapshotOrGenericThread();

        BlobStore store = blobStore.get();
        if (store == null) {
            synchronized (lock) {
                store = blobStore.get();
                if (store == null) {
                    if (lifecycle.started() == false) {
                        throw new RepositoryException(metadata.name(), "repository is not in started state");
                    }
                    try {
                        store = createBlobStore();
                    } catch (RepositoryException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RepositoryException(metadata.name(), "cannot create blob store" , e);
                    }
                    blobStore.set(store);
                }
            }
        }
        return store;
    }

    /**
     * Creates new BlobStore to read and write data.
     * 基于metadata 创建store对象   因为store涉及到具体的实现类  所以对子类开放钩子
     */
    protected abstract BlobStore createBlobStore() throws Exception;

    /**
     * Returns base path of the repository
     * Public for testing.
     */
    public BlobPath basePath() {
        return basePath;
    }

    /**
     * Returns true if metadata and snapshot files should be compressed
     *
     * @return true if compression is needed
     */
    protected final boolean isCompress() {
        return compress;
    }

    /**
     * Returns data file chunk size.
     * <p>
     * This method should return null if no chunking is needed.
     *
     * @return chunk size
     */
    protected ByteSizeValue chunkSize() {
        return null;
    }

    /**
     * 获取存储相关的元数据
     * @return
     */
    @Override
    public RepositoryMetadata getMetadata() {
        return metadata;
    }

    /**
     * 默认实现 FsStore中 没有覆盖该方法 也就是返回空数据
     * @return
     */
    @Override
    public RepositoryStats stats() {
        final BlobStore store = blobStore.get();
        if (store == null) {
            return RepositoryStats.EMPTY_STATS;
        }
        return new RepositoryStats(store.stats());
    }

    /**
     * 通过发起命令 可以删除仓库中存储的快照数据
     *
     * @param snapshotIds           snapshot ids
     * @param repositoryStateId     the unique id identifying the state of the repository when the snapshot deletion began
     * @param repositoryMetaVersion version of the updated repository metadata to write
     * @param listener              completion listener
     */
    @Override
    public void deleteSnapshots(Collection<SnapshotId> snapshotIds, long repositoryStateId, Version repositoryMetaVersion,
                                ActionListener<Void> listener) {
        // 在只读环境下不允许删除
        if (isReadOnly()) {
            listener.onFailure(new RepositoryException(metadata.name(), "cannot delete snapshot from a readonly repository"));
        } else {
            try {
                final Map<String, BlobMetadata> rootBlobs = blobContainer().listBlobs();

                // 这个 safeXXX方法就是多了一道检测 确保此时最新的repository.gen 应该与传入的参数一致
                final RepositoryData repositoryData = safeRepositoryData(repositoryStateId, rootBlobs);
                // Cache the indices that were found before writing out the new index-N blob so that a stuck master will never
                // delete an index that was created by another master node after writing this index-N blob.

                // 每次生成快照时  IndexMetadata 会存储在 /indices/{indexName} 目录下
                // 这里是获取所有index的indexMetadata信息
                final Map<String, BlobContainer> foundIndices = blobStore().blobContainer(indicesPath()).children();
                // 删除快照信息 以及删除残留文件
                doDeleteShardSnapshots(snapshotIds, repositoryStateId, foundIndices, rootBlobs, repositoryData,
                    SnapshotsService.useShardGenerations(repositoryMetaVersion), listener);
            } catch (Exception ex) {
                listener.onFailure(new RepositoryException(metadata.name(), "failed to delete snapshots " + snapshotIds, ex));
            }
        }
    }

    /**
     * Loads {@link RepositoryData} ensuring that it is consistent with the given {@code rootBlobs} as well of the assumed generation.
     *
     * @param repositoryStateId Expected repository generation   在发起删除任务时 获取到的仓库的gen
     * @param rootBlobs         Blobs at the repository root  仓库root目录下 每个文件的元数据   在仓库的root目录下 实际上就是一组 index-?/index.latest 文件
     * @return RepositoryData
     */
    private RepositoryData safeRepositoryData(long repositoryStateId, Map<String, BlobMetadata> rootBlobs) throws IOException {
        final long generation = latestGeneration(rootBlobs.keySet());
        final long genToLoad;
        final Tuple<Long, BytesReference> cached;
        if (bestEffortConsistency) {
            // updateAndGet 返回的是新的数值
            genToLoad = latestKnownRepoGen.updateAndGet(known -> Math.max(known, repositoryStateId));
            cached = null;
        } else {
            // 非强一致性场景 就是获取之前存储的数据
            genToLoad = latestKnownRepoGen.get();
            cached = latestKnownRepositoryData.get();
        }
        if (genToLoad > generation) {
            // It's always a possibility to not see the latest index-N in the listing here on an eventually consistent blob store, just
            // debug log it. Any blobs leaked as a result of an inconsistent listing here will be cleaned up in a subsequent cleanup or
            // snapshot delete run anyway.
            logger.debug("Determined repository's generation from its contents to [" + generation + "] but " +
                "current generation is at least [" + genToLoad + "]");
        }
        // 代表在执行删除快照任务前后 又产生了新的快照 这种情况应该是不会发生的  并且这种情况下的处理措施是通知用户 而不是自动切换到最新的index-?进行删除
        if (genToLoad != repositoryStateId) {
            throw new RepositoryException(metadata.name(), "concurrent modification of the index-N file, expected current generation [" +
                repositoryStateId + "], actual current generation [" + genToLoad + "]");
        }
        // 如果此时缓存的数据是准确的  直接返回缓存的数据
        if (cached != null && cached.v1() == genToLoad) {
            return repositoryDataFromCachedEntry(cached);
        }
        // 加载指定gen对应 index-? 文件 并将内部的数据 通过反序列化工具还原成 RepositoryData
        return getRepositoryData(genToLoad);
    }

    /**
     * After updating the {@link RepositoryData} each of the shards directories is individually first moved to the next shard generation
     * and then has all now unreferenced blobs in it deleted.
     *
     * @param snapshotIds       SnapshotIds to delete
     * @param repositoryStateId Expected repository state id
     * @param foundIndices      All indices folders found in the repository before executing any writes to the repository during this
     *                          delete operation
     * @param rootBlobs         All blobs found at the root of the repository before executing any writes to the repository during this
     *                          delete operation
     * @param repositoryData    RepositoryData found the in the repository before executing this delete
     * @param listener          Listener to invoke once finished
     *                          删除快照
     */
    private void doDeleteShardSnapshots(Collection<SnapshotId> snapshotIds,  // 快照id
                                        long repositoryStateId,   // 发起删除操作时的仓库id
                                        Map<String, BlobContainer> foundIndices,  // repository/indices 目录下所有的indexMetadata
                                        Map<String, BlobMetadata> rootBlobs, // repository root目录
                                        RepositoryData repositoryData, boolean writeShardGens,
                                        ActionListener<Void> listener) {

        // 是否写入了 分片的gen信息 当前版本都是写入了的 需要一起删除
        if (writeShardGens) {
            // First write the new shard state metadata (with the removed snapshot) and compute deletion targets
            final StepListener<Collection<ShardSnapshotMetaDeleteResult>> writeShardMetaDataAndComputeDeletesStep = new StepListener<>();

            /**
             * 这里就是执行所有shard级别的快照删除任务
             * 流程为
             * 1.先通过本次要删除的 snapshotIds 换算成参与删除的所有indexId
             * 2.在生成快照数据时 每次都会将当前indexMetadata的数据记录下来 通过snapshotId+indexId 可以知道本次参与快照任务的有多少shard
             * 3.repositoryData 内记录了本次存储分片级快照数据的目录名shardGen 通过上一步得到的shardId 可以反查最新的shardGen
             * 4.通过shardGen 查询本地的快照数据 将本次要删除的快照数据剔除后 剩余部分存储在一个newGen文件夹下
             */
            writeUpdatedShardMetaDataAndComputeDeletes(snapshotIds, repositoryData, true, writeShardMetaDataAndComputeDeletesStep);
            // Once we have put the new shard-level metadata into place, we can update the repository metadata as follows:
            // 1. Remove the snapshots from the list of existing snapshots
            // 2. Update the index shard generations of all updated shard folders
            //
            // Note: If we fail updating any of the individual shard paths, none of them are changed since the newly created
            //       index-${gen_uuid} will not be referenced by the existing RepositoryData and new RepositoryData is only
            //       written if all shard paths have been successfully updated.
            final StepListener<RepositoryData> writeUpdatedRepoDataStep = new StepListener<>();

            // 此时所有有关的index下shard的删除结果已经生成   需要做的就是更新repositoryData 并发布到集群中
            writeShardMetaDataAndComputeDeletesStep.whenComplete(deleteResults -> {
                final ShardGenerations.Builder builder = ShardGenerations.builder();
                for (ShardSnapshotMetaDeleteResult newGen : deleteResults) {
                    builder.put(newGen.indexId, newGen.shardId, newGen.newGeneration);
                }

                // 分片的gen信息是通过 repositoryData获取的 所以这里进行更新
                final RepositoryData updatedRepoData = repositoryData.removeSnapshots(snapshotIds, builder.build());

                // 将最新的repositoryData 发布到集群中
                writeIndexGen(updatedRepoData, repositoryStateId, true, Function.identity(),
                    ActionListener.wrap(v -> writeUpdatedRepoDataStep.onResponse(updatedRepoData), listener::onFailure));
            }, listener::onFailure);


            // Once we have updated the repository, run the clean-ups
            // 当更新后的 repositoryData 已经发布到集群后
            writeUpdatedRepoDataStep.whenComplete(updatedRepoData -> {
                // Run unreferenced blobs cleanup in parallel to shard-level snapshot deletion
                final ActionListener<Void> afterCleanupsListener =
                    new GroupedActionListener<>(ActionListener.wrap(() -> listener.onResponse(null)), 2);

                // 这里是清理残留的无效数据  不细看了
                asyncCleanupUnlinkedRootAndIndicesBlobs(foundIndices, rootBlobs, updatedRepoData, afterCleanupsListener);
                asyncCleanupUnlinkedShardLevelBlobs(snapshotIds, writeShardMetaDataAndComputeDeletesStep.result(), afterCleanupsListener);
            }, listener::onFailure);
            // TODO 兼容旧版本的忽略
        } else {
            // Write the new repository data first (with the removed snapshot), using no shard generations
            final RepositoryData updatedRepoData = repositoryData.removeSnapshots(snapshotIds, ShardGenerations.EMPTY);
            writeIndexGen(updatedRepoData, repositoryStateId, false, Function.identity(), ActionListener.wrap(v -> {
                // Run unreferenced blobs cleanup in parallel to shard-level snapshot deletion
                final ActionListener<Void> afterCleanupsListener =
                    new GroupedActionListener<>(ActionListener.wrap(() -> listener.onResponse(null)), 2);
                asyncCleanupUnlinkedRootAndIndicesBlobs(foundIndices, rootBlobs, updatedRepoData, afterCleanupsListener);
                final StepListener<Collection<ShardSnapshotMetaDeleteResult>> writeMetaAndComputeDeletesStep = new StepListener<>();
                writeUpdatedShardMetaDataAndComputeDeletes(snapshotIds, repositoryData, false, writeMetaAndComputeDeletesStep);
                writeMetaAndComputeDeletesStep.whenComplete(deleteResults ->
                        asyncCleanupUnlinkedShardLevelBlobs(snapshotIds, deleteResults, afterCleanupsListener),
                    afterCleanupsListener::onFailure);
            }, listener::onFailure));
        }
    }

    /**
     * 异步清理此时已经不再被使用的所有blob
     * @param foundIndices  索引目录下对应的所有数据体
     * @param rootBlobs   根目录下对应的所有数据体
     * @param updatedRepoData   此时最新的repositoryData
     * @param listener
     */
    private void asyncCleanupUnlinkedRootAndIndicesBlobs(Map<String, BlobContainer> foundIndices, Map<String, BlobMetadata> rootBlobs,
                                                         RepositoryData updatedRepoData, ActionListener<Void> listener) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(
            listener,
            // 清理过期的blob 对象
            l -> cleanupStaleBlobs(foundIndices, rootBlobs, updatedRepoData, ActionListener.map(l, ignored -> null))));
    }

    /**
     * 删除旧的 shardGen 文件
     * @param snapshotIds
     * @param deleteResults
     * @param listener
     */
    private void asyncCleanupUnlinkedShardLevelBlobs(Collection<SnapshotId> snapshotIds,
                                                     Collection<ShardSnapshotMetaDeleteResult> deleteResults,
                                                     ActionListener<Void> listener) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(
            listener,
            l -> {
                try {
                    blobContainer().deleteBlobsIgnoringIfNotExists(resolveFilesToDelete(snapshotIds, deleteResults));
                    l.onResponse(null);
                } catch (Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage("{} Failed to delete some blobs during snapshot delete", snapshotIds),
                        e);
                    throw e;
                }
            }));
    }

    /**
     * updates the shard state metadata for shards of a snapshot that is to be deleted. Also computes the files to be cleaned up.
     * @param snapshotIds
     * @param oldRepositoryData
     * @param useUUIDs
     * @param onAllShardsCompleted
     */
    private void writeUpdatedShardMetaDataAndComputeDeletes(Collection<SnapshotId> snapshotIds,  // 本次要删除的所有快照对应的id
                                                            RepositoryData oldRepositoryData,   // 当前仓库数据 对应最新的index-?
            boolean useUUIDs, ActionListener<Collection<ShardSnapshotMetaDeleteResult>> onAllShardsCompleted) {

        // 获取执行快照任务相关的线程池
        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        // 这次要删除的所有快照 与哪些index有关
        final List<IndexId> indices = oldRepositoryData.indicesToUpdateAfterRemovingSnapshot(snapshotIds);

        // 没有需要删除的数据 直接触发监听器
        if (indices.isEmpty()) {
            onAllShardsCompleted.onResponse(Collections.emptyList());
            return;
        }

        // Listener that flattens out the delete results for each index
        // 当所有分片完成删除任务后触发该监听器
        final ActionListener<Collection<ShardSnapshotMetaDeleteResult>> deleteIndexMetadataListener = new GroupedActionListener<>(
            ActionListener.map(onAllShardsCompleted,
                // GroupedActionListener 在触发时传入的是一个list 通过map函数平铺后 触发onAllShardsCompleted
                res -> res.stream().flatMap(Collection::stream).collect(Collectors.toList())), indices.size());

        // 将每个索引下相关的快照数据删除
        for (IndexId indexId : indices) {
            // 代表需要保留的快照
            final Set<SnapshotId> survivingSnapshots = oldRepositoryData.getSnapshots(indexId).stream()
                .filter(id -> snapshotIds.contains(id) == false).collect(Collectors.toSet());
            final StepListener<Collection<Integer>> shardCountListener = new StepListener<>();

            final ActionListener<Integer> allShardCountsListener = new GroupedActionListener<>(shardCountListener, snapshotIds.size());

            // 这里是获取每次快照对应的index下的分片数量
            for (SnapshotId snapshotId : snapshotIds) {
                executor.execute(ActionRunnable.supply(allShardCountsListener, () -> {
                    try {
                        // 获取某个索引在某个快照对应的时刻的分片数量是多少
                        return getSnapshotIndexMetadata(snapshotId, indexId).getNumberOfShards();
                    } catch (Exception ex) {
                        logger.warn(() -> new ParameterizedMessage(
                                "[{}] [{}] failed to read metadata for index", snapshotId, indexId.getName()), ex);
                        // Just invoke the listener without any shard generations to count it down, this index will be cleaned up
                        // by the stale data cleanup in the end.
                        // TODO: Getting here means repository corruption. We should find a way of dealing with this instead of just
                        //       ignoring it and letting the cleanup deal with it.
                        // 如果某个索引下的某个快照 没有数据时 设置null
                        return null;
                    }
                }));
            }

            shardCountListener.whenComplete(counts -> {
                // 该索引下删除的每个快照的分片数量
                final int shardCount = counts.stream().mapToInt(i -> i).max().orElse(0);
                if (shardCount == 0) {
                    // 虽然该index 参与了某次快照任务  但是此时index下没有分片 所以不需要进行删除
                    deleteIndexMetadataListener.onResponse(null);
                    return;
                }
                // Listener for collecting the results of removing the snapshot from each shard's metadata in the current index

                // 每当所有分片完成删除任务 通知到上层监听index级别任务的监听器 当所有index级别的任务都完成后 代表整个快照任务结束
                final ActionListener<ShardSnapshotMetaDeleteResult> allShardsListener =
                        new GroupedActionListener<>(deleteIndexMetadataListener, shardCount);
                // 开始挨个删除shard级别的文件
                for (int shardId = 0; shardId < shardCount; shardId++) {
                    final int finalShardId = shardId;
                    executor.execute(new AbstractRunnable() {
                        @Override
                        protected void doRun() throws Exception {

                            // 定位到存储shard级别的容器   下面应该是以 shardGen为单位存储快照数据的 每次分片快照任务对应唯一一个shardGen
                            final BlobContainer shardContainer = shardContainer(indexId, finalShardId);

                            // 获取所有分片快照目录
                            final Set<String> blobs = shardContainer.listBlobs().keySet();
                            final BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots;
                            final String newGen;
                            // 默认为true
                            if (useUUIDs) {
                                newGen = UUIDs.randomBase64UUID();
                                // 这里获取最近一次的分片快照任务的数据   每当新执行一次快照任务时 该目录下存储的都是全量数据 (即包含之前的快照数据)
                                blobStoreIndexShardSnapshots = buildBlobStoreIndexShardSnapshots(blobs, shardContainer,
                                        // 这里获取的是最近一次的分片快照任务对应的gen
                                        oldRepositoryData.shardGenerations().getShardGen(indexId, finalShardId)).v1();
                            } else {
                                // TODO 先忽略 感觉是兼容旧逻辑的
                                Tuple<BlobStoreIndexShardSnapshots, Long> tuple = buildBlobStoreIndexShardSnapshots(blobs, shardContainer);
                                newGen = Long.toString(tuple.v2() + 1);
                                blobStoreIndexShardSnapshots = tuple.v1();
                            }

                            // 每当某个分片的删除任务完成后 触发一次监听器 直到所有分片都完成删除工作
                            allShardsListener.onResponse(deleteFromShardSnapshotMeta(survivingSnapshots, indexId, finalShardId,
                                    snapshotIds, shardContainer, blobs, blobStoreIndexShardSnapshots, newGen));
                        }

                        @Override
                        public void onFailure(Exception ex) {
                            logger.warn(
                                () -> new ParameterizedMessage("{} failed to delete shard data for shard [{}][{}]",
                                    snapshotIds, indexId.getName(), finalShardId), ex);
                            // Just passing null here to count down the listener instead of failing it, the stale data left behind
                            // here will be retried in the next delete or repository cleanup
                            allShardsListener.onResponse(null);
                        }
                    });
                }
            }, deleteIndexMetadataListener::onFailure);
        }
    }

    private List<String> resolveFilesToDelete(Collection<SnapshotId> snapshotIds,
                                              Collection<ShardSnapshotMetaDeleteResult> deleteResults) {
        final String basePath = basePath().buildAsString();
        final int basePathLen = basePath.length();
        return Stream.concat(
            deleteResults.stream().flatMap(shardResult -> {
                final String shardPath =
                    shardContainer(shardResult.indexId, shardResult.shardId).path().buildAsString();
                return shardResult.blobsToDelete.stream().map(blob -> shardPath + blob);
            }),
            deleteResults.stream().map(shardResult -> shardResult.indexId).distinct().flatMap(indexId -> {
                final String indexContainerPath = indexContainer(indexId).path().buildAsString();
                return snapshotIds.stream().map(snapshotId -> indexContainerPath + globalMetadataFormat.blobName(snapshotId.getUUID()));
            })
        ).map(absolutePath -> {
            assert absolutePath.startsWith(basePath);
            return absolutePath.substring(basePathLen);
        }).collect(Collectors.toList());
    }

    /**
     * Cleans up stale blobs directly under the repository root as well as all indices paths that aren't referenced by any existing
     * snapshots. This method is only to be called directly after a new {@link RepositoryData} was written to the repository and with
     * parameters {@code foundIndices}, {@code rootBlobs}
     *
     * @param foundIndices all indices blob containers found in the repository before {@code newRepoData} was written    此时能找到的所有索引
     * @param rootBlobs    all blobs found directly under the repository root   此时能找到的所有数据块
     * @param newRepoData  new repository data that was just written      此时已经是最新的记录了
     * @param listener     listener to invoke with the combined {@link DeleteResult} of all blobs removed in this operation
     *
     *                      清理掉不再需要维护的数据
     */
    private void cleanupStaleBlobs(Map<String, BlobContainer> foundIndices, Map<String, BlobMetadata> rootBlobs,
                                   RepositoryData newRepoData, ActionListener<DeleteResult> listener) {

        // 当相关数据都被清除时 触发该监听器
        final GroupedActionListener<DeleteResult> groupedListener = new GroupedActionListener<>(ActionListener.wrap(deleteResults -> {
            DeleteResult deleteResult = DeleteResult.ZERO;
            for (DeleteResult result : deleteResults) {
                deleteResult = deleteResult.add(result);
            }
            listener.onResponse(deleteResult);
        }, listener::onFailure), 2);

        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        executor.execute(ActionRunnable.supply(groupedListener, () -> {
            // 通过staleRootBlobs方法   找到失效的blob
            // 通过cleanupStaleRootFiles 删除失效的blob
            List<String> deletedBlobs = cleanupStaleRootFiles(staleRootBlobs(newRepoData, rootBlobs.keySet()));
            return new DeleteResult(deletedBlobs.size(), deletedBlobs.stream().mapToLong(name -> rootBlobs.get(name).length()).sum());
        }));

        final Set<String> survivingIndexIds = newRepoData.getIndices().values().stream().map(IndexId::getId).collect(Collectors.toSet());
        executor.execute(ActionRunnable.supply(groupedListener, () -> cleanupStaleIndices(foundIndices, survivingIndexIds)));
    }

    /**
     * Runs cleanup actions on the repository. Increments the repository state id by one before executing any modifications on the
     * repository.
     * TODO: Add shard level cleanups
     * <ul>
     *     <li>Deleting stale indices {@link #cleanupStaleIndices}</li>
     *     <li>Deleting unreferenced root level blobs {@link #cleanupStaleRootFiles}</li>
     * </ul>
     * @param repositoryStateId     Current repository state id
     * @param repositoryMetaVersion version of the updated repository metadata to write
     * @param listener              Listener to complete when done
     *                              对应 RepositoryCleanupInProgress
     */
    public void cleanup(long repositoryStateId,  // 在执行清理任务前获取到的最新的repositoryData.gen  主要是中途repositoryData 是否发生变化
                        Version repositoryMetaVersion, ActionListener<RepositoryCleanupResult> listener) {
        try {
            if (isReadOnly()) {
                throw new RepositoryException(metadata.name(), "cannot run cleanup on readonly repository");
            }

            // 获取root目录下每个文件的描述信息 实际上就是 index-? 文件
            Map<String, BlobMetadata> rootBlobs = blobContainer().listBlobs();

            // 读取最新的仓库数据
            final RepositoryData repositoryData = safeRepositoryData(repositoryStateId, rootBlobs);

            // 这里存储了每个index 的 indexMetadata 数据
            final Map<String, BlobContainer> foundIndices = blobStore().blobContainer(indicesPath()).children();

            // 往期所有快照相关的索引
            final Set<String> survivingIndexIds =
                repositoryData.getIndices().values().stream().map(IndexId::getId).collect(Collectors.toSet());

            // 找到 repositoryData.gen 之前的所有快照文件 这些文件可以删除了  因为最新的repositoryData中记录了最新的shardGen 而对应的目录下存放了以往所有的快照数据
            final List<String> staleRootBlobs = staleRootBlobs(repositoryData, rootBlobs.keySet());

            // 索引文件都是必要的 且没有找到过期的文件 那么本次不需要删除任何文件
            if (survivingIndexIds.equals(foundIndices.keySet()) && staleRootBlobs.isEmpty()) {
                // Nothing to clean up we return
                listener.onResponse(new RepositoryCleanupResult(DeleteResult.ZERO));
            } else {
                // write new index-N blob to ensure concurrent operations will fail
                // 根据此时的repositoryData 配合最新的snapshot 生成一个新的index-n 文件  同时删除旧的index-n文件
                writeIndexGen(repositoryData, repositoryStateId, SnapshotsService.useShardGenerations(repositoryMetaVersion),
                        // cleanupStaleBlobs 这里才开始清理过期数据
                        Function.identity(), ActionListener.wrap(v -> cleanupStaleBlobs(foundIndices, rootBlobs, repositoryData,
                        ActionListener.map(listener, RepositoryCleanupResult::new)), listener::onFailure));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Finds all blobs directly under the repository root path that are not referenced by the current RepositoryData
     * 返回当前数据块对应的文件之外的其他文件  这些文件都属于不被使用的文件
     * @param repositoryData  某一数据块反序列化后生成的存储数据对象
     * @param rootBlobNames  本次考虑范围内的所有数据块描述对象
     * @return
     */
    private List<String> staleRootBlobs(RepositoryData repositoryData, Set<String> rootBlobNames) {
        // 此时需要被保留的所有快照id
        final Set<String> allSnapshotIds =
            repositoryData.getSnapshotIds().stream().map(SnapshotId::getUUID).collect(Collectors.toSet());
        return rootBlobNames.stream().filter(
            blob -> {
                // 如果是临时文件 需要被删除
                if (FsBlobContainer.isTempBlobName(blob)) {
                    return true;
                }
                // TODO
                if (blob.endsWith(".dat")) {
                    final String foundUUID;
                    if (blob.startsWith(SNAPSHOT_PREFIX)) {
                        foundUUID = blob.substring(SNAPSHOT_PREFIX.length(), blob.length() - ".dat".length());
                        assert snapshotFormat.blobName(foundUUID).equals(blob);
                    } else if (blob.startsWith(METADATA_PREFIX)) {
                        foundUUID = blob.substring(METADATA_PREFIX.length(), blob.length() - ".dat".length());
                        assert globalMetadataFormat.blobName(foundUUID).equals(blob);
                    } else {
                        // 当 .dat 文件既不是快照文件也不是元数据文件  不进行处理  应该是避免误删文件
                        return false;
                    }

                    // 快照文件或者元数据文件对应的uuid 已经不包含在最新的repositoryData内   那么可以删除这个文件
                    return allSnapshotIds.contains(foundUUID) == false;
                    // 旧的index-? 文件都可以被删除
                } else if (blob.startsWith(INDEX_FILE_PREFIX)) {
                    // TODO: Include the current generation here once we remove keeping index-(N-1) around from #writeIndexGen
                    return repositoryData.getGenId() > Long.parseLong(blob.substring(INDEX_FILE_PREFIX.length()));
                }

                // 避免误删文件 其余文件不做处理
                return false;
            }
        ).collect(Collectors.toList());
    }

    /**
     * 删除列表对应的blob
     * @param blobsToDelete
     * @return
     */
    private List<String> cleanupStaleRootFiles(List<String> blobsToDelete) {
        if (blobsToDelete.isEmpty()) {
            return blobsToDelete;
        }
        try {
            logger.info("[{}] Found stale root level blobs {}. Cleaning them up", metadata.name(), blobsToDelete);
            // 直接使用container 对外开放的api执行删除操作
            blobContainer().deleteBlobsIgnoringIfNotExists(blobsToDelete);
            return blobsToDelete;
        } catch (IOException e) {
            logger.warn(() -> new ParameterizedMessage(
                "[{}] The following blobs are no longer part of any snapshot [{}] but failed to remove them",
                metadata.name(), blobsToDelete), e);
        } catch (Exception e) {
            // TODO: We shouldn't be blanket catching and suppressing all exceptions here and instead handle them safely upstream.
            //       Currently this catch exists as a stop gap solution to tackle unexpected runtime exceptions from implementations
            //       bubbling up and breaking the snapshot functionality.
            assert false : e;
            logger.warn(new ParameterizedMessage("[{}] Exception during cleanup of root level blobs", metadata.name()), e);
        }
        return Collections.emptyList();
    }

    /**
     *
     * @param foundIndices
     * @param survivingIndexIds
     * @return
     */
    private DeleteResult cleanupStaleIndices(Map<String, BlobContainer> foundIndices, Set<String> survivingIndexIds) {
        DeleteResult deleteResult = DeleteResult.ZERO;
        try {
            for (Map.Entry<String, BlobContainer> indexEntry : foundIndices.entrySet()) {
                final String indexSnId = indexEntry.getKey();
                try {
                    if (survivingIndexIds.contains(indexSnId) == false) {
                        logger.debug("[{}] Found stale index [{}]. Cleaning it up", metadata.name(), indexSnId);
                        deleteResult = deleteResult.add(indexEntry.getValue().delete());
                        logger.debug("[{}] Cleaned up stale index [{}]", metadata.name(), indexSnId);
                    }
                } catch (IOException e) {
                    logger.warn(() -> new ParameterizedMessage(
                        "[{}] index {} is no longer part of any snapshots in the repository, " +
                            "but failed to clean up their index folders", metadata.name(), indexSnId), e);
                }
            }
        } catch (Exception e) {
            // TODO: We shouldn't be blanket catching and suppressing all exceptions here and instead handle them safely upstream.
            //       Currently this catch exists as a stop gap solution to tackle unexpected runtime exceptions from implementations
            //       bubbling up and breaking the snapshot functionality.
            assert false : e;
            logger.warn(new ParameterizedMessage("[{}] Exception during cleanup of stale indices", metadata.name()), e);
        }
        return deleteResult;
    }

    /**
     * @param snapshotId            snapshot id
     * @param shardGenerations      updated shard generations
     * @param startTime             start time of the snapshot
     * @param failure               global failure reason or null
     * @param totalShards           total number of shards
     * @param shardFailures         list of shard failures
     * @param repositoryStateId     the unique id identifying the state of the repository when the snapshot began
     * @param includeGlobalState    include cluster global state
     * @param clusterMetadata       cluster metadata
     * @param userMetadata          user metadata
     * @param repositoryMetaVersion version of the updated repository metadata to write
     * @param stateTransformer      a function that filters the last cluster state update that the snapshot finalization will execute and
     *                              is used to remove any state tracked for the in-progress snapshot from the cluster state
     * @param listener              listener to be invoked with the new {@link RepositoryData} and the snapshot's {@link SnapshotInfo}
     *                              completion of the snapshot
     *                              当某次快照任务结束后触发
     */
    @Override
    public void finalizeSnapshot(final SnapshotId snapshotId,   // 本次要关闭的快照
                                 final ShardGenerations shardGenerations,  // 描述本次快照任务涉及的所有索引 以及下面每个分片的gen信息
                                 final long startTime,    // 开始生成快照的时间
                                 final String failure,
                                 final int totalShards,  // 本次总计涉及到多少分片
                                 final List<SnapshotShardFailure> shardFailures,   // 本次写入中 总计有多少分片失败了
                                 final long repositoryStateId,   // 在创建快照任务时 仓库的gen
                                 final boolean includeGlobalState,
                                 final Metadata clusterMetadata,
                                 final Map<String, Object> userMetadata,
                                 Version repositoryMetaVersion,  // 描述使用的lucene版本
                                 Function<ClusterState, ClusterState> stateTransformer,   // 该函数就是将快照任务从当前clusterState中移除 代表本次快照完成
                                 final ActionListener<Tuple<RepositoryData, SnapshotInfo>> listener) {
        assert repositoryStateId > RepositoryData.UNKNOWN_REPO_GEN :
            "Must finalize based on a valid repository generation but received [" + repositoryStateId + "]";

        // 本次生成快照相关的所有索引id
        final Collection<IndexId> indices = shardGenerations.indices();
        // Once we are done writing the updated index-N blob we remove the now unreferenced index-${uuid} blobs in each shard
        // directory if all nodes are at least at version SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION
        // If there are older version nodes in the cluster, we don't need to run this cleanup as it will have already happened
        // when writing the index-${N} to each shard directory.
        // 在7.6版本之后会将 shard.gen 写入到存储层中
        final boolean writeShardGens = SnapshotsService.useShardGenerations(repositoryMetaVersion);

        final Consumer<Exception> onUpdateFailure =
            e -> listener.onFailure(new SnapshotException(metadata.name(), snapshotId, "failed to update snapshot in repository", e));

        // 该监听器要监听到3个任务完成后 才能触发最终的逻辑
        final ActionListener<SnapshotInfo> allMetaListener = new GroupedActionListener<>(
            ActionListener.wrap(snapshotInfos -> {
                assert snapshotInfos.size() == 1 : "Should have only received a single SnapshotInfo but received " + snapshotInfos;
                final SnapshotInfo snapshotInfo = snapshotInfos.iterator().next();

                // 开始处理这个生成的快照描述信息
                getRepositoryData(ActionListener.wrap(existingRepositoryData -> {

                    // 先获取之前的仓库描述信息 追加一个快照信息  shardGen 代表本次快照任务中细化到每个shard级别的快照文件元数据名称
                    final RepositoryData updatedRepositoryData =
                        existingRepositoryData.addSnapshot(snapshotId, snapshotInfo.state(), Version.CURRENT, shardGenerations);

                    // 在这里将最新的repositoryData 写入到 index-? 文件中 以及清理旧的 index-?文件 以及更新clusterState 并发布到集群中
                    writeIndexGen(updatedRepositoryData, repositoryStateId, writeShardGens, stateTransformer,
                            ActionListener.wrap(writtenRepoData -> {
                                // 清除旧的 shardGen 对应的信息   实际上就是上一次该shard产生的快照文件
                                if (writeShardGens) {
                                    cleanupOldShardGens(existingRepositoryData, updatedRepositoryData);
                                }
                                listener.onResponse(new Tuple<>(writtenRepoData, snapshotInfo));
                            }, onUpdateFailure));
                }, onUpdateFailure));
            }, onUpdateFailure), 2 + indices.size());

        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);

        // We ignore all FileAlreadyExistsException when writing metadata since otherwise a master failover while in this method will
        // mean that no snap-${uuid}.dat blob is ever written for this snapshot. This is safe because any updated version of the
        // index or global metadata will be compatible with the segments written in this snapshot as well.
        // Failing on an already existing index-${repoGeneration} below ensures that the index.latest blob is not updated in a way
        // that decrements the generation it points at


        // 分别将此时的 Metadata/indexMetadata/snapshotInfo信息写入到仓库中

        // Write Global Metadata
        // 将生成该快照时的集群元数据信息写入到 仓库中
        executor.execute(ActionRunnable.run(allMetaListener,
            () -> globalMetadataFormat.write(clusterMetadata, blobContainer(), snapshotId.getUUID(), false)));

        // write the index metadata for each index in the snapshot
        // 将索引级别的元数据信息写入到仓库中
        for (IndexId index : indices) {
            executor.execute(ActionRunnable.run(allMetaListener, () ->
                indexMetadataFormat.write(clusterMetadata.index(index.getName()), indexContainer(index), snapshotId.getUUID(), false)));
        }

        // 将本次快照任务的信息写入到仓库后 触发监听器
        executor.execute(ActionRunnable.supply(allMetaListener, () -> {
            final SnapshotInfo snapshotInfo = new SnapshotInfo(snapshotId,
                indices.stream().map(IndexId::getName).collect(Collectors.toList()),
                startTime, failure, threadPool.absoluteTimeInMillis(), totalShards, shardFailures,
                includeGlobalState, userMetadata);
            snapshotFormat.write(snapshotInfo, blobContainer(), snapshotId.getUUID(), false);
            return snapshotInfo;
        }));
    }

    // Delete all old shard gen blobs that aren't referenced any longer as a result from moving to updated repository data

    /**
     * 通过对比新旧2个repositoryData 删除旧的shardGen信息
     * @param existingRepositoryData
     * @param updatedRepositoryData
     */
    private void cleanupOldShardGens(RepositoryData existingRepositoryData, RepositoryData updatedRepositoryData) {
        final List<String> toDelete = new ArrayList<>();
        final int prefixPathLen = basePath().buildAsString().length();
        updatedRepositoryData.shardGenerations().obsoleteShardGenerations(existingRepositoryData.shardGenerations()).forEach(
            (indexId, gens) -> gens.forEach((shardId, oldGen) -> toDelete.add(
                shardContainer(indexId, shardId).path().buildAsString().substring(prefixPathLen) + INDEX_FILE_PREFIX + oldGen)));
        try {
            blobContainer().deleteBlobsIgnoringIfNotExists(toDelete);
        } catch (Exception e) {
            logger.warn("Failed to clean up old shard generation blobs", e);
        }
    }

    // 应该是这样 每个快照对应一种数据  快照本身则代表满足条件时存储的文件 比如说各种元数据可能会采用快照的方式  描述快照本身可能也需要一个文件

    /**
     * 通过传入快照id 找到快照对象
     * @param snapshotId  snapshot id
     * @return
     */
    @Override
    public SnapshotInfo getSnapshotInfo(final SnapshotId snapshotId) {
        try {
            // format对象已经反序列化的规则
            return snapshotFormat.read(blobContainer(), snapshotId.getUUID());
        } catch (NoSuchFileException ex) {
            // 代表快照路径错误  是一个固定格式的文件名 拼接上快照id
            throw new SnapshotMissingException(metadata.name(), snapshotId, ex);
        } catch (IOException | NotXContentException ex) {
            // 解析失败时抛出异常   反序列化的逻辑就不细看了 反正依赖于第三方框架
            throw new SnapshotException(metadata.name(), snapshotId, "failed to get snapshots", ex);
        }
    }

    /**
     * 获取某个快照对应的 元数据信息   因为某些快照在执行完成时 会记录此时的集群元数据
     * @param snapshotId the snapshot id to load the global metadata from
     * @return
     */
    @Override
    public Metadata getSnapshotGlobalMetadata(final SnapshotId snapshotId) {
        try {
            return globalMetadataFormat.read(blobContainer(), snapshotId.getUUID());
        } catch (NoSuchFileException ex) {
            throw new SnapshotMissingException(metadata.name(), snapshotId, ex);
        } catch (IOException ex) {
            throw new SnapshotException(metadata.name(), snapshotId, "failed to read global metadata", ex);
        }
    }

    /**
     * 获取生成某个快照时 索引的元数据信息
     * @param snapshotId the snapshot id to load the index metadata from
     * @param index      the {@link IndexId} to load the metadata from
     * @return
     * @throws IOException
     */
    @Override
    public IndexMetadata getSnapshotIndexMetadata(final SnapshotId snapshotId, final IndexId index) throws IOException {
        try {
            return indexMetadataFormat.read(indexContainer(index), snapshotId.getUUID());
        } catch (NoSuchFileException e) {
            throw new SnapshotMissingException(metadata.name(), snapshotId, e);
        }
    }

    private BlobPath indicesPath() {
        return basePath().add("indices");
    }

    private BlobContainer indexContainer(IndexId indexId) {
        return blobStore().blobContainer(indicesPath().add(indexId.getId()));
    }

    private BlobContainer shardContainer(IndexId indexId, ShardId shardId) {
        return shardContainer(indexId, shardId.getId());
    }

    public BlobContainer shardContainer(IndexId indexId, int shardId) {
        return blobStore().blobContainer(indicesPath().add(indexId.getId()).add(Integer.toString(shardId)));
    }

    /**
     * Configures RateLimiter based on repository and global settings
     *
     * @param repositorySettings repository settings
     * @param setting            setting to use to configure rate limiter
     * @param defaultRate        default limiting rate
     * @return rate limiter or null of no throttling is needed
     */
    private RateLimiter getRateLimiter(Settings repositorySettings, String setting, ByteSizeValue defaultRate) {
        ByteSizeValue maxSnapshotBytesPerSec = repositorySettings.getAsBytesSize(setting, defaultRate);
        if (maxSnapshotBytesPerSec.getBytes() <= 0) {
            return null;
        } else {
            return new RateLimiter.SimpleRateLimiter(maxSnapshotBytesPerSec.getMbFrac());
        }
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        return snapshotRateLimitingTimeInNanos.count();
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return restoreRateLimitingTimeInNanos.count();
    }

    protected void assertSnapshotOrGenericThread() {
        assert Thread.currentThread().getName().contains('[' + ThreadPool.Names.SNAPSHOT + ']')
            || Thread.currentThread().getName().contains('[' + ThreadPool.Names.GENERIC + ']') :
            "Expected current thread [" + Thread.currentThread() + "] to be the snapshot or generic thread.";
    }

    /**
     * 发起一个验证请求 实际上就是随便生成一个字符串并尝试是否能够存储
     * @return
     */
    @Override
    public String startVerification() {
        try {
            if (isReadOnly()) {
                // It's readonly - so there is not much we can do here to verify it apart from reading the blob store metadata
                latestIndexBlobId();
                return "read-only";
            } else {
                String seed = UUIDs.randomBase64UUID();
                byte[] testBytes = Strings.toUTF8Bytes(seed);
                BlobContainer testContainer = blobStore().blobContainer(basePath().add(testBlobPrefix(seed)));
                BytesArray bytes = new BytesArray(testBytes);
                try (InputStream stream = bytes.streamInput()) {
                    testContainer.writeBlobAtomic("master.dat", stream, bytes.length(), true);
                }
                return seed;
            }
        } catch (IOException exp) {
            throw new RepositoryVerificationException(metadata.name(), "path " + basePath() + " is not accessible on master node", exp);
        }
    }

    @Override
    public void endVerification(String seed) {
        if (isReadOnly() == false) {
            try {
                final String testPrefix = testBlobPrefix(seed);
                blobStore().blobContainer(basePath().add(testPrefix)).delete();
            } catch (IOException exp) {
                throw new RepositoryVerificationException(metadata.name(), "cannot delete test data at " + basePath(), exp);
            }
        }
    }

    // Tracks the latest known repository generation in a best-effort way to detect inconsistent listing of root level index-N blobs
    // and concurrent modifications.
    // 当前感知到的最大的gen可能是从repositoryMetadata中直接获取  也可能是从当前正在执行的快照/删除快照/清理仓库 的任务中获取的
    private final AtomicLong latestKnownRepoGen = new AtomicLong(RepositoryData.UNKNOWN_REPO_GEN);

    // Best effort cache of the latest known repository data and its generation, cached serialized as compressed json
    // 最新一次写入数据对应的缓存
    private final AtomicReference<Tuple<Long, BytesReference>> latestKnownRepositoryData = new AtomicReference<>();


    /**
     * 获取此时最新的数据并触发监听器
     * @param listener
     */
    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        // latestKnownRepoGen 代表目前已知的最新的gen 每次生成快照时 应该都会更新该值 当某次失败时 应该就会将该标识设置成一个异常 (CORRUPTED_REPO_GEN)
        if (latestKnownRepoGen.get() == RepositoryData.CORRUPTED_REPO_GEN) {
            listener.onFailure(corruptedStateException(null));
            return;
        }
        // Retry loading RepositoryData in a loop in case we run into concurrent modifications of the repository.
        // Keep track of the most recent generation we failed to load so we can break out of the loop if we fail to load the same
        // generation repeatedly.
        long lastFailedGeneration = RepositoryData.UNKNOWN_REPO_GEN;
        while (true) {
            final long genToLoad;
            // 尽最大的可能实现一致性  写入repositoryData 分为 更新pending -> 写入数据 -> 更新gen  当pending与gen不一致时就代表即将要写入数据了 此时尝试获取repositoryData 都是从磁盘中读取
            if (bestEffortConsistency) {
                // We're only using #latestKnownRepoGen as a hint in this mode and listing repo contents as a secondary way of trying
                // to find a higher generation
                final long generation;
                try {
                    // 获取此时最新的  index gen
                    generation = latestIndexBlobId();
                } catch (IOException ioe) {
                    listener.onFailure(
                        new RepositoryException(metadata.name(), "Could not determine repository generation from root blobs", ioe));
                    return;
                }
                // 返回此时最新的gen
                genToLoad = latestKnownRepoGen.updateAndGet(known -> Math.max(known, generation));
                if (genToLoad > generation) {
                    logger.info("Determined repository generation [" + generation
                        + "] from repository contents but correct generation must be at least [" + genToLoad + "]");
                }
            } else {
                // We only rely on the generation tracked in #latestKnownRepoGen which is exclusively updated from the cluster state
                // 不会主动去获取最新的gen 并更新 而是使用之前缓存的gen
                genToLoad = latestKnownRepoGen.get();
            }
            try {
                // 最近一次数据的缓存 但是每次数据写入存储层后 与更新缓存这个时间差内 还是可能读取到旧数据 在要求强一致性的场景下 每次都是从存储层中获取数据
                final Tuple<Long, BytesReference> cached = latestKnownRepositoryData.get();
                final RepositoryData loaded;
                // Caching is not used with #bestEffortConsistency see docs on #cacheRepositoryData for details
                // 没有要求强一致性的场景下 存在缓存 且 gen一致  直接返回缓存的数据就可以
                // 也就是 同一gen的数据可能后面还会有修改
                if (bestEffortConsistency == false && cached != null && cached.v1() == genToLoad) {
                    loaded = repositoryDataFromCachedEntry(cached);
                } else {
                    // 使用最新的gen 加载数据 并反序列化
                    loaded = getRepositoryData(genToLoad);
                    // 顺便更新缓存
                    cacheRepositoryData(loaded);
                }
                // 当成功读取到数据后 触发监听器
                listener.onResponse(loaded);
                return;
            } catch (RepositoryException e) {
                // If the generation to load changed concurrently and we didn't just try loading the same generation before we retry
                if (genToLoad != latestKnownRepoGen.get() && genToLoad != lastFailedGeneration) {
                    lastFailedGeneration = genToLoad;
                    logger.warn("Failed to load repository data generation [" + genToLoad +
                        "] because a concurrent operation moved the current generation to [" + latestKnownRepoGen.get() + "]", e);
                    continue;
                }
                // 代表某个异常只该文件不存在 且允许使用缓存 那么直接将latestKnownRepoGen 打成异常的
                if (bestEffortConsistency == false && ExceptionsHelper.unwrap(e, NoSuchFileException.class) != null) {
                    // We did not find the expected index-N even though the cluster state continues to point at the missing value
                    // of N so we mark this repository as corrupted.
                    markRepoCorrupted(genToLoad, e,
                        ActionListener.wrap(v -> listener.onFailure(corruptedStateException(e)), listener::onFailure));
                } else {
                    listener.onFailure(e);
                }
                return;
            } catch (Exception e) {
                listener.onFailure(new RepositoryException(metadata.name(), "Unexpected exception when loading repository data", e));
                return;
            }
        }
    }

    /**
     * Puts the given {@link RepositoryData} into the cache if it is of a newer generation and only if the repository is not using
     * {@link #bestEffortConsistency}. When using {@link #bestEffortConsistency} the repository is using listing to find the latest
     * {@code index-N} blob and there are no hard guarantees that a given repository generation won't be reused since an external
     * modification can lead to moving from a higher {@code N} to a lower {@code N} value which mean we can't safely assume that a given
     * generation will always contain the same {@link RepositoryData}.
     *
     * @param updated RepositoryData to cache if newer than the cache contents
     *                将最新的存储数据 写入到缓存中
     */
    private void cacheRepositoryData(RepositoryData updated) {
        // 首先允许使用缓存 且在读取时没有要求强一致性 如果要求了强一致性 那么即使写入了缓存 也不会被访问到
        if (cacheRepositoryData && bestEffortConsistency == false) {
            final BytesReference serialized;
            BytesStreamOutput out = new BytesStreamOutput();
            try {
                // 包装成压缩输出流
                try (StreamOutput tmp = CompressorFactory.COMPRESSOR.streamOutput(out);
                     XContentBuilder builder = XContentFactory.jsonBuilder(tmp)) {
                    // 将存储数据压缩后写入到 output中
                    updated.snapshotsToXContent(builder, true);
                }

                serialized = out.bytes();
                final int len = serialized.length();
                // 当数据过大时 不采用缓存策略
                if (len > ByteSizeUnit.KB.toBytes(500)) {
                    logger.debug("Not caching repository data of size [{}] for repository [{}] because it is larger than 500KB in" +
                        " serialized size", len, metadata.name());
                    if (len > ByteSizeUnit.MB.toBytes(5)) {
                        logger.warn("Your repository metadata blob for repository [{}] is larger than 5MB. Consider moving to a fresh" +
                            " repository for new snapshots or deleting unneeded snapshots from your repository to ensure stable" +
                            " repository behavior going forward.", metadata.name());
                    }
                    // Set empty repository data to not waste heap for an outdated cached value
                    latestKnownRepositoryData.set(null);
                    return;
                }
            } catch (IOException e) {
                assert false : new AssertionError("Impossible, no IO happens here", e);
                logger.warn("Failed to serialize repository data", e);
                return;
            }
            // 更新缓存数据
            latestKnownRepositoryData.updateAndGet(known -> {
                if (known != null && known.v1() > updated.getGenId()) {
                    return known;
                }
                return new Tuple<>(updated.getGenId(), serialized);
            });
        }
    }

    /**
     * 从缓存中读取数据 并通过 反序列化框架 将数据还原成 RepositoryData
     * @param cacheEntry
     * @return
     * @throws IOException
     */
    private RepositoryData repositoryDataFromCachedEntry(Tuple<Long, BytesReference> cacheEntry) throws IOException {
        return RepositoryData.snapshotsFromXContent(
            XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                CompressorFactory.COMPRESSOR.streamInput(cacheEntry.v2().streamInput())), cacheEntry.v1());
    }

    private RepositoryException corruptedStateException(@Nullable Exception cause) {
        return new RepositoryException(metadata.name(),
            "Could not read repository data because the contents of the repository do not match its " +
                "expected state. This is likely the result of either concurrently modifying the contents of the " +
                "repository by a process other than this cluster or an issue with the repository's underlying" +
                "storage. The repository has been disabled to prevent corrupting its contents. To re-enable it " +
                "and continue using it please remove the repository from the cluster and add it again to make " +
                "the cluster recover the known state of the repository from its physical contents.", cause);
    }

    /**
     * Marks the repository as corrupted. This puts the repository in a state where its tracked value for
     * {@link RepositoryMetadata#pendingGeneration()} is unchanged while its value for {@link RepositoryMetadata#generation()} is set to
     * {@link RepositoryData#CORRUPTED_REPO_GEN}. In this state, the repository can not be used any longer and must be removed and
     * recreated after the problem that lead to it being marked as corrupted has been fixed.
     *
     * @param corruptedGeneration generation that failed to load because the index file was not found but that should have loaded     当尝试读取index-gen 文件时 发现文件不存在 导致抛出异常
     * @param originalException   exception that lead to the failing to load the {@code index-N} blob     读取数据时出现的异常
     * @param listener            listener to invoke once done
     */
    private void markRepoCorrupted(long corruptedGeneration, Exception originalException, ActionListener<Void> listener) {
        assert corruptedGeneration != RepositoryData.UNKNOWN_REPO_GEN;
        assert bestEffortConsistency == false;
        // 会触发publish 那么只有leader节点才能执行这个任务了
        clusterService.submitStateUpdateTask("mark repository corrupted [" + metadata.name() + "][" + corruptedGeneration + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    // 获取在元数据中 针对存储层创建的自定义元数据   这里获取到的是一个总的存储层元数据
                    final RepositoriesMetadata state = currentState.metadata().custom(RepositoriesMetadata.TYPE);
                    // 获取当前存储层对应的元数据对象
                    final RepositoryMetadata repoState = state.repository(metadata.name());
                    // 此时元数据也定位到这个gen 才有设置的意义 如果corruptedGeneration本身就是一个有问题的值 那么出现异常就是存储层自身的问题了 也就没有标记的必要
                    if (repoState.generation() != corruptedGeneration) {
                        throw new IllegalStateException("Tried to mark repo generation [" + corruptedGeneration
                            + "] as corrupted but its state concurrently changed to [" + repoState + "]");
                    }
                    return ClusterState.builder(currentState).metadata(Metadata.builder(currentState.metadata()).putCustom(
                        RepositoriesMetadata.TYPE, state.withUpdatedGeneration(
                            metadata.name(), RepositoryData.CORRUPTED_REPO_GEN, repoState.pendingGeneration())).build()).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(new RepositoryException(metadata.name(), "Failed marking repository state as corrupted",
                        ExceptionsHelper.useOrSuppress(e, originalException)));
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(null);
                }
            });
    }

    /**
     * 根据传入的index 从存储层读取数据
     * @param indexGen
     * @return
     */
    private RepositoryData getRepositoryData(long indexGen) {
        // 当仓库首次启动时  不存在index-gen文件  此时gen为-1 并且查询的repositoryData为empty
        if (indexGen == RepositoryData.EMPTY_REPO_GEN) {
            return RepositoryData.EMPTY;
        }
        try {
            // 通过gen 找到 index-gen 文件 并将内部的数据解析成 RepositoryData 对象
            final String snapshotsIndexBlobName = INDEX_FILE_PREFIX + Long.toString(indexGen);

            // EMPTY is safe here because RepositoryData#fromXContent calls namedObject
            try (InputStream blob = blobContainer().readBlob(snapshotsIndexBlobName);
                 XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                     LoggingDeprecationHandler.INSTANCE, blob)) {
                return RepositoryData.snapshotsFromXContent(parser, indexGen);
            }
        } catch (IOException ioe) {
            if (bestEffortConsistency) {
                // If we fail to load the generation we tracked in latestKnownRepoGen we reset it.
                // This is done as a fail-safe in case a user manually deletes the contents of the repository in which case subsequent
                // operations must start from the EMPTY_REPO_GEN again
                if (latestKnownRepoGen.compareAndSet(indexGen, RepositoryData.EMPTY_REPO_GEN)) {
                    logger.warn("Resetting repository generation tracker because we failed to read generation [" + indexGen + "]", ioe);
                }
            }
            throw new RepositoryException(metadata.name(), "could not read repository data from index blob", ioe);
        }
    }

    private static String testBlobPrefix(String seed) {
        return TESTS_FILE + seed;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Writing a new index generation is a three step process.
     * First, the {@link RepositoryMetadata} entry for this repository is set into a pending state by incrementing its
     * pending generation {@code P} while its safe generation {@code N} remains unchanged.
     * Second, the updated {@link RepositoryData} is written to generation {@code P + 1}.
     * Lastly, the {@link RepositoryMetadata} entry for this repository is updated to the new generation {@code P + 1} and thus
     * pending and safe generation are set to the same value marking the end of the update of the repository data.
     *
     * @param repositoryData RepositoryData to write
     * @param expectedGen    expected repository generation at the start of the operation
     * @param writeShardGens whether to write {@link ShardGenerations} to the new {@link RepositoryData} blob
     * @param stateFilter    filter for the last cluster state update executed by this method
     * @param listener       completion listener
     *                       当快照任务结束后 相关信息会追加到repositoryData中 之后要发布到集群中
     */
    protected void writeIndexGen(RepositoryData repositoryData,
                                 long expectedGen,   // 在创建快照任务时的仓库id
                                 boolean writeShardGens,  // 是否要在repositoryData中写入 分片的gen信息
                                 Function<ClusterState, ClusterState> stateFilter, ActionListener<RepositoryData> listener) {
        assert isReadOnly() == false; // can not write to a read only repository

        // 每当完成一次快照任务后 都应该更新repositoryData 这里发现快照执行前后仓库id已经变化了 此时仓库数据已经过时 就没有必要处理了
        // 实际上在执行快照任务时 即使获取到的repositoryData过时也不会影响本次操作 无非是无法找到可复用的索引文件 而降级为为所有索引文件创建快照
        // 如果更换了leader节点 其他节点上的repositoryData.gen 与之前是不一致的 repositoryData无法得到更新
        final long currentGen = repositoryData.getGenId();
        if (currentGen != expectedGen) {
            // the index file was updated by a concurrent operation, so we were operating on stale
            // repository data
            listener.onFailure(new RepositoryException(metadata.name(),
                "concurrent modification of the index-N file, expected current generation [" + expectedGen +
                    "], actual current generation [" + currentGen + "]"));
            return;
        }

        // Step 1: Set repository generation state to the next possible pending generation
        // 第一步先更新pendingGen 并发布到集群中
        final StepListener<Long> setPendingStep = new StepListener<>();

        // 第一步 更新集群中其他节点的 gen
        clusterService.submitStateUpdateTask("set pending repository generation [" + metadata.name() + "][" + expectedGen + "]",
            new ClusterStateUpdateTask() {

                private long newGen;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    // 在 clusterState中是可以存储仓库元数据的
                    final RepositoryMetadata meta = getRepoMetadata(currentState);
                    final String repoName = metadata.name();

                    // 集群元数据中记录了 repositoryData.gen
                    final long genInState = meta.generation();
                    // 如果要求强一致性场景 或者不知道当前的gen
                    final boolean uninitializedMeta = meta.generation() == RepositoryData.UNKNOWN_REPO_GEN || bestEffortConsistency;
                    if (uninitializedMeta == false && meta.pendingGeneration() != genInState) {
                        logger.info("Trying to write new repository data over unfinished write, repo [{}] is at " +
                            "safe generation [{}] and pending generation [{}]", meta.name(), genInState, meta.pendingGeneration());
                    }
                    assert expectedGen == RepositoryData.EMPTY_REPO_GEN || uninitializedMeta
                        || expectedGen == meta.generation() :
                        "Expected non-empty generation [" + expectedGen + "] does not match generation tracked in [" + meta + "]";
                    // If we run into the empty repo generation for the expected gen, the repo is assumed to have been cleared of
                    // all contents by an external process so we reset the safe generation to the empty generation.
                    // expectedGen 是在开始执行快照任务时 从仓库目录中加载出来的
                    // 而genInState 是从元数据中获取的 可能存在滞后性
                    final long safeGeneration = expectedGen == RepositoryData.EMPTY_REPO_GEN ? RepositoryData.EMPTY_REPO_GEN
                        : (uninitializedMeta ? expectedGen : genInState);
                    // Regardless of whether or not the safe generation has been reset, the pending generation always increments so that
                    // even if a repository has been manually cleared of all contents we will never reuse the same repository generation.
                    // This is motivated by the consistency behavior the S3 based blob repository implementation has to support which does
                    // not offer any consistency guarantees when it comes to overwriting the same blob name with different content.


                    // 第一步先是将 repositoryData的pendingGen+1 并发布到集群中
                    final long nextPendingGen = metadata.pendingGeneration() + 1;
                    newGen = uninitializedMeta ? Math.max(expectedGen + 1, nextPendingGen) : nextPendingGen;
                    assert newGen > latestKnownRepoGen.get() : "Attempted new generation [" + newGen +
                        "] must be larger than latest known generation [" + latestKnownRepoGen.get() + "]";

                    // TODO 数据仅存储在当前节点 那么一旦leader发生改变 数据没有同步过来 如何正常进行快照呢???
                    return ClusterState.builder(currentState).metadata(Metadata.builder(currentState.getMetadata())
                        .putCustom(RepositoriesMetadata.TYPE,
                            currentState.metadata().<RepositoriesMetadata>custom(RepositoriesMetadata.TYPE).withUpdatedGeneration(
                                repoName, safeGeneration, newGen)).build()).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(
                        new RepositoryException(metadata.name(), "Failed to execute cluster state update [" + source + "]", e));
                }

                /**
                 * 当通知任务完成时 使用最新的gen 通知监听器
                 * @param source
                 * @param oldState
                 * @param newState
                 */
                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    setPendingStep.onResponse(newGen);
                }
            });

        final StepListener<RepositoryData> filterRepositoryDataStep = new StepListener<>();

        // Step 2: Write new index-N blob to repository and update index.latest
        // 这里的newGen 代表本次更新repositoryData后预期的gen  在第一步中只是将它设置成pendingGen
        // 这一步是为未设置版本号的分片 设置版本信息
        setPendingStep.whenComplete(newGen -> threadPool().executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(listener, l -> {
            // BwC logic: Load snapshot version information if any snapshot is missing a version in RepositoryData so that the new
            // RepositoryData contains a version for every snapshot

            // 此时找到所有没有版本号的 快照id
            final List<SnapshotId> snapshotIdsWithoutVersion = repositoryData.getSnapshotIds().stream().filter(
                snapshotId -> repositoryData.getVersion(snapshotId) == null).collect(Collectors.toList());

            // 代表部分快照数据在repositoryData中没有找到匹配的版本号
            if (snapshotIdsWithoutVersion.isEmpty() == false) {
                final Map<SnapshotId, Version> updatedVersionMap = new ConcurrentHashMap<>();
                final GroupedActionListener<Void> loadAllVersionsListener = new GroupedActionListener<>(
                    ActionListener.runAfter(
                        new ActionListener<>() {
                            @Override
                            public void onResponse(Collection<Void> voids) {
                                logger.info("Successfully loaded all snapshot's version information for {} from snapshot metadata",
                                    AllocationService.firstListElementsToCommaDelimitedString(
                                        snapshotIdsWithoutVersion, SnapshotId::toString, logger.isDebugEnabled()));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn("Failure when trying to load missing version information from snapshot metadata", e);
                            }
                        },
                        // 当所有分片的版本号都设置完毕后 触发监听器
                        () -> filterRepositoryDataStep.onResponse(repositoryData.withVersions(updatedVersionMap))),
                    snapshotIdsWithoutVersion.size());

                // snapshotInfo 在写入时 默认就是使用本节点的版本号
                for (SnapshotId snapshotId : snapshotIdsWithoutVersion) {
                    threadPool().executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.run(loadAllVersionsListener,
                        () -> updatedVersionMap.put(snapshotId, getSnapshotInfo(snapshotId).version())));
                }
            } else {
                // 如果所有快照都已经包含了版本号 直接触发监听器
                filterRepositoryDataStep.onResponse(repositoryData);
            }
        })), listener::onFailure);


        // 当每个分片的版本号都设置好后 执行下一步
        filterRepositoryDataStep.whenComplete(filteredRepositoryData -> {

            // 本次更新后 repositoryData的gen
            final long newGen = setPendingStep.result();
            if (latestKnownRepoGen.get() >= newGen) {
                throw new IllegalArgumentException(
                    "Tried writing generation [" + newGen + "] but repository is at least at generation [" + latestKnownRepoGen.get()
                        + "] already");
            }
            // write the index file
            // 在仓库目录下生成 index-? 文件
            final String indexBlob = INDEX_FILE_PREFIX + Long.toString(newGen);
            logger.debug("Repository [{}] writing new index generational blob [{}]", metadata.name(), indexBlob);

            // 将此时快照数据序列化后写入到文件中
            writeAtomic(indexBlob, BytesReference.bytes(filteredRepositoryData.snapshotsToXContent(XContentFactory.jsonBuilder(), writeShardGens)), true);
            // write the current generation to the index-latest file
            final BytesReference genBytes;
            try (BytesStreamOutput bStream = new BytesStreamOutput()) {
                // 将newGen 转换成bytes 类型
                bStream.writeLong(newGen);
                genBytes = bStream.bytes();
            }
            logger.debug("Repository [{}] updating index.latest with generation [{}]", metadata.name(), newGen);

            // 可以看到在这层目录下 除了index-? 文件外 还存储了一个index.latest 文件 描述下面这些index-? 中最新的索引文件
            writeAtomic(INDEX_LATEST_BLOB, genBytes, false);

            // Step 3: Update CS to reflect new repository generation.
            // 因为此时最新的repositoryData 已经写入到仓库了 可以更新gen了
            clusterService.submitStateUpdateTask("set safe repository generation [" + metadata.name() + "][" + newGen + "]",
                new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        final RepositoryMetadata meta = getRepoMetadata(currentState);
                        // 这里确保gen按照预料的情况变化
                        if (meta.generation() != expectedGen) {
                            throw new IllegalStateException("Tried to update repo generation to [" + newGen
                                + "] but saw unexpected generation in state [" + meta + "]");
                        }
                        if (meta.pendingGeneration() != newGen) {
                            throw new IllegalStateException(
                                "Tried to update from unexpected pending repo generation [" + meta.pendingGeneration() +
                                    "] after write to generation [" + newGen + "]");
                        }
                        // 发布最新的gen 这里还将之前的快照任务 从clusterState中移除了
                        return stateFilter.apply(ClusterState.builder(currentState).metadata(Metadata.builder(currentState.getMetadata())
                            .putCustom(RepositoriesMetadata.TYPE,
                                currentState.metadata().<RepositoriesMetadata>custom(RepositoriesMetadata.TYPE).withUpdatedGeneration(
                                    metadata.name(), newGen, newGen))).build());
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(
                            new RepositoryException(metadata.name(), "Failed to execute cluster state update [" + source + "]", e));
                    }

                    /**
                     * 此时更新 repositoryData 的事件已经通知到集群中其他节点了
                     * @param source
                     * @param oldState
                     * @param newState
                     */
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

                        // 当执行了一次快照任务后 repositoryData的gen 已经发生了变化 这里写入到缓存中
                        final RepositoryData writtenRepositoryData = filteredRepositoryData.withGenId(newGen);
                        cacheRepositoryData(writtenRepositoryData);

                        // 因为每次index-? 文件记录的都是全量数据 所以可以删除旧的文件了
                        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.supply(listener, () -> {
                            // Delete all now outdated index files up to 1000 blobs back from the new generation.
                            // If there are more than 1000 dangling index-N cleanup functionality on repo delete will take care of them.
                            // Deleting one older than the current expectedGen is done for BwC reasons as older versions used to keep
                            // two index-N blobs around.
                            // 之前的 index-? 文件都可以删除了
                            final List<String> oldIndexN = LongStream.range(
                                Math.max(Math.max(expectedGen - 1, 0), newGen - 1000), newGen)
                                .mapToObj(gen -> INDEX_FILE_PREFIX + gen)
                                .collect(Collectors.toList());
                            try {
                                blobContainer().deleteBlobsIgnoringIfNotExists(oldIndexN);
                            } catch (IOException e) {
                                logger.warn(() -> new ParameterizedMessage("Failed to clean up old index blobs {}", oldIndexN), e);
                            }
                            return writtenRepositoryData;
                        }));
                    }
                });
        }, listener::onFailure);
    }

    /**
     * 从集群状态中 获取有关该存储层此时的元数据信息   ClusterState中包含了一个集群需要的各种信息
     * 一些插件或者其他自定义的数据都是通过 创建对应的自定义元数据并插入到 custom容器中的
     * @param state
     * @return
     */
    private RepositoryMetadata getRepoMetadata(ClusterState state) {
        final RepositoryMetadata repositoryMetadata =
            state.getMetadata().<RepositoriesMetadata>custom(RepositoriesMetadata.TYPE).repository(metadata.name());
        assert repositoryMetadata != null;
        return repositoryMetadata;
    }

    /**
     * Get the latest snapshot index blob id.  Snapshot index blobs are named index-N, where N is
     * the next version number from when the index blob was written.  Each individual index-N blob is
     * only written once and never overwritten.  The highest numbered index-N blob is the latest one
     * that contains the current snapshots in the repository.
     *
     * Package private for testing
     * 尝试找到索引文件最大的gen
     */
    long latestIndexBlobId() throws IOException {
        try {
            // First, try listing all index-N blobs (there should only be two index-N blobs at any given
            // time in a repository if cleanup is happening properly) and pick the index-N blob with the
            // highest N value - this will be the latest index blob for the repository.  Note, we do this
            // instead of directly reading the index.latest blob to get the current index-N blob because
            // index.latest is not written atomically and is not immutable - on every index-N change,
            // we first delete the old index.latest and then write the new one.  If the repository is not
            // read-only, it is possible that we try deleting the index.latest blob while it is being read
            // by some other operation (such as the get snapshots operation).  In some file systems, it is
            // illegal to delete a file while it is being read elsewhere (e.g. Windows).  For read-only
            // repositories, we read for index.latest, both because listing blob prefixes is often unsupported
            // and because the index.latest blob will never be deleted and re-written.

            // 获取根目录下 所有 index-? 文件最大的gen
            return listBlobsToGetLatestIndexId();
        } catch (UnsupportedOperationException e) {
            // If its a read-only repository, listing blobs by prefix may not be supported (e.g. a URL repository),
            // in this case, try reading the latest index generation from the index.latest blob
            try {
                // 有关索引的gen 也会存储一份快照 如果当前存储层不支持写入操作 就读取这个快照下的gen
                return readSnapshotIndexLatestBlob();
            } catch (NoSuchFileException nsfe) {
                return RepositoryData.EMPTY_REPO_GEN;
            }
        }
    }

    // package private for testing
    // 当生成快照时 对应的此时最新的 index gen  每次写入gen都会增大 而快照并不是每次写入都会触发的  这样就有了2种精度的 gen 一种是实时的 一种是可能落后 但是相对接受的
    // 并且在只读存储层中 快照最新的gen 就是最新的索引gen
    long readSnapshotIndexLatestBlob() throws IOException {
        return Numbers.bytesToLong(Streams.readFully(blobContainer().readBlob(INDEX_LATEST_BLOB)).toBytesRef());
    }

    /**
     * 找到存储层根目录下 所有 index- 开头的文件 并找到最后一个文件对应的后缀数字,也就是gen
     * @return
     * @throws IOException
     */
    private long listBlobsToGetLatestIndexId() throws IOException {
        return latestGeneration(blobContainer().listBlobsByPrefix(INDEX_FILE_PREFIX).keySet());
    }

    /**
     * 找到多个index-? 最大的gen
     * @param rootBlobs
     * @return
     */
    private long latestGeneration(Collection<String> rootBlobs) {
        // 如果传入的是一个空列表时 默认返回-1
        long latest = RepositoryData.EMPTY_REPO_GEN;
        for (String blobName : rootBlobs) {
            // 跳过非目标文件
            if (blobName.startsWith(INDEX_FILE_PREFIX) == false) {
                continue;
            }
            try {
                final long curr = Long.parseLong(blobName.substring(INDEX_FILE_PREFIX.length()));
                latest = Math.max(latest, curr);
            } catch (NumberFormatException nfe) {
                // the index- blob wasn't of the format index-N where N is a number,
                // no idea what this blob is but it doesn't belong in the repository!
                logger.warn("[{}] Unknown blob in the repository: {}", metadata.name(), blobName);
            }
        }
        return latest;
    }

    private void writeAtomic(final String blobName, final BytesReference bytesRef, boolean failIfAlreadyExists) throws IOException {
        try (InputStream stream = bytesRef.streamInput()) {
            blobContainer().writeBlobAtomic(blobName, stream, bytesRef.length(), failIfAlreadyExists);
        }
    }

    /**
     * @param store                 store to be snapshotted
     * @param mapperService         the shards mapper service
     * @param snapshotId            snapshot id
     * @param indexId               id for the index being snapshotted
     * @param snapshotIndexCommit   commit point
     * @param shardStateIdentifier  a unique identifier of the state of the shard that is stored with the shard's snapshot and used
     *                              to detect if the shard has changed between snapshots. If {@code null} is passed as the identifier
     *                              snapshotting will be done by inspecting the physical files referenced by {@code snapshotIndexCommit}
     * @param snapshotStatus        snapshot status
     * @param repositoryMetaVersion version of the updated repository metadata to write
     * @param userMetadata          user metadata of the snapshot found in {@link SnapshotsInProgress.Entry#userMetadata()}
     * @param listener              listener invoked on completion
     *
     *                              基于某个indexCommit 生成快照数据
     */
    @Override
    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                              IndexCommit snapshotIndexCommit, String shardStateIdentifier, IndexShardSnapshotStatus snapshotStatus,
                              Version repositoryMetaVersion, Map<String, Object> userMetadata, ActionListener<String> listener) {
        // 每个store 对应存储某个shard数据的目录
        final ShardId shardId = store.shardId();
        final long startTime = threadPool.absoluteTimeInMillis();
        try {
            // 对应shard的 gen
            final String generation = snapshotStatus.generation();
            logger.debug("[{}] [{}] snapshot to [{}] [{}] ...", shardId, snapshotId, metadata.name(), generation);
            // 定位存储快照文件的目录
            final BlobContainer shardContainer = shardContainer(indexId, shardId);
            final Set<String> blobs;
            // TODO 忽略兼容性代码
            if (generation == null) {
                try {
                    blobs = shardContainer.listBlobsByPrefix(INDEX_FILE_PREFIX).keySet();
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "failed to list blobs", e);
                }
            } else {
                // 看来以分片为目录  每个shardGen 存储了分片的快照数据   这是获取上次的数据
                // 因为在创建快照任务时 会采用上次repositoryData.shardGen 如果shardGen不存在 就会使用一个_new_
                // 而在repository外层的目录下  有一个index-? 文件 用于描述仓库的状态
                blobs = Collections.singleton(INDEX_FILE_PREFIX + generation);
            }

            // 开始恢复上次的快照数据
            Tuple<BlobStoreIndexShardSnapshots, String> tuple = buildBlobStoreIndexShardSnapshots(blobs, shardContainer, generation);
            BlobStoreIndexShardSnapshots snapshots = tuple.v1();
            String fileListGeneration = tuple.v2();

            // 本次要执行的快照任务 不能与之前快照任务同名 可能会混淆吧
            if (snapshots.snapshots().stream().anyMatch(sf -> sf.snapshot().equals(snapshotId.getName()))) {
                throw new IndexShardSnapshotFailedException(shardId,
                    "Duplicate snapshot name [" + snapshotId.getName() + "] detected, aborting");
            }
            // First inspect all known SegmentInfos instances to see if we already have an equivalent commit in the repository
            // 尝试通过 shardStateIdentifier 寻找有没有完全匹配的快照  如果存在那么本次不需要执行快照工作了 代表没有数据发生改变
            final List<BlobStoreIndexShardSnapshot.FileInfo> filesFromSegmentInfos = Optional.ofNullable(shardStateIdentifier).map(id -> {
                // 一个 shardGen 对应多个 SnapshotFiles 而每个 SnapshotFiles 又对应了一组快照文件  shardGen在什么时候发生改变 ???
                for (SnapshotFiles snapshotFileSet : snapshots.snapshots()) {
                    if (id.equals(snapshotFileSet.shardStateIdentifier())) {
                        return snapshotFileSet.indexFiles();
                    }
                }
                return null;
            }).orElse(null);

            final List<BlobStoreIndexShardSnapshot.FileInfo> indexCommitPointFiles;
            // 分别记录本次新增的快照文件数量 以及大小    和 总的快照文件数量，大小
            int indexIncrementalFileCount = 0;
            int indexTotalNumberOfFiles = 0;
            long indexIncrementalSize = 0;
            long indexTotalFileSize = 0;

            // needsWrite == true 的文件存储在阻塞队列中
            final BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> filesToSnapshot = new LinkedBlockingQueue<>();
            // If we did not find a set of files that is equal to the current commit we determine the files to upload by comparing files
            // in the commit with files already in the repository
            // 无法找到精确匹配的快照文件
            // 这里对本次commit的文件 与之前生成的快照文件做比较
            if (filesFromSegmentInfos == null) {
                indexCommitPointFiles = new ArrayList<>();
                // 增加引用计数 避免被意外关闭
                store.incRef();
                final Collection<String> fileNames;
                final Store.MetadataSnapshot metadataFromStore;
                try {
                    // TODO apparently we don't use the MetadataSnapshot#.recoveryDiff(...) here but we should
                    try {
                        logger.trace(
                            "[{}] [{}] Loading store metadata using index commit [{}]", shardId, snapshotId, snapshotIndexCommit);
                        // 从存储分片数据的目录下找到本次提交的所有数据
                        metadataFromStore = store.getMetadata(snapshotIndexCommit);
                        fileNames = snapshotIndexCommit.getFileNames();
                    } catch (IOException e) {
                        // 当索引出现异常时会存储一个异常文件 一旦检测到异常文件 就抛出异常
                        throw new IndexShardSnapshotFailedException(shardId, "Failed to get store file metadata", e);
                    }
                } finally {
                    // 读取完相关数据后就可以允许关闭store了
                    store.decRef();
                }
                // 遍历本次提交的所有文件
                for (String fileName : fileNames) {
                    // 因为快照任务 可能会被修改成终止 所以将检测时机 尽可能往后延
                    if (snapshotStatus.isAborted()) {
                        logger.debug("[{}] [{}] Aborted on the file [{}], exiting", shardId, snapshotId, fileName);
                        throw new IndexShardSnapshotFailedException(shardId, "Aborted");
                    }

                    logger.trace("[{}] [{}] Processing [{}]", shardId, snapshotId, fileName);
                    // 获取该文件的元数据 也就是大小 校验和啥的
                    final StoreFileMetadata md = metadataFromStore.get(fileName);
                    BlobStoreIndexShardSnapshot.FileInfo existingFileInfo = null;
                    // 在仓库中找到完全相同的快照文件
                    List<BlobStoreIndexShardSnapshot.FileInfo> filesInfo = snapshots.findPhysicalIndexFiles(fileName);
                    if (filesInfo != null) {
                        for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : filesInfo) {
                            if (fileInfo.isSame(md)) {
                                // a commit point file with the same name, size and checksum was already copied to repository
                                // we will reuse it for this snapshot
                                existingFileInfo = fileInfo;
                                break;
                            }
                        }
                    }

                    // We can skip writing blobs where the metadata hash is equal to the blob's contents because we store the hash/contents
                    // directly in the shard level metadata in this case
                    // 如果本次刷盘生成的文件可以在之前的快照文件中找到大小/校验和等完全匹配的文件  代表本次不需要再生成快照文件了
                    final boolean needsWrite = md.hashEqualsContents() == false;

                    // 这里累加文件总长度
                    indexTotalFileSize += md.length();
                    indexTotalNumberOfFiles++;

                    // 代表本次数据发生了变化 需要重新生成快照文件
                    if (existingFileInfo == null) {
                        // 新增了多少文件
                        indexIncrementalFileCount++;
                        indexIncrementalSize += md.length();
                        // create a new FileInfo
                        // 进入到这里代表需要创建一个新的快照文件
                        BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo =
                            new BlobStoreIndexShardSnapshot.FileInfo(
                                (needsWrite ? UPLOADED_DATA_BLOB_PREFIX : VIRTUAL_DATA_BLOB_PREFIX) + UUIDs.randomBase64UUID(),
                                md, chunkSize());
                        // 每个索引文件会对应一个快照文件
                        indexCommitPointFiles.add(snapshotFileInfo);
                        if (needsWrite) {
                            filesToSnapshot.add(snapshotFileInfo);
                        }
                        assert needsWrite || assertFileContentsMatchHash(snapshotFileInfo, store);
                    } else {
                        // 每个索引文件会对应一个快照文件
                        indexCommitPointFiles.add(existingFileInfo);
                    }
                }
            } else {
                 // shardStateIdentifier 匹配的情况 代表所有文件没有发生变化
                indexCommitPointFiles = filesFromSegmentInfos;
            }

            // 开始执行快照任务
            snapshotStatus.moveToStarted(startTime, indexIncrementalFileCount,
                indexTotalNumberOfFiles, indexIncrementalSize, indexTotalFileSize);

            final StepListener<Collection<Void>> allFilesUploadedListener = new StepListener<>();

            // 当生成快照的逻辑处理完后 触发相关逻辑  v 可以忽略 因为触发监听器时 对应的类型是Void
            allFilesUploadedListener.whenComplete(v -> {
                // 此时快照任务已经完成  并返回本次快照任务的元数据
                final IndexShardSnapshotStatus.Copy lastSnapshotStatus =
                    snapshotStatus.moveToFinalize(snapshotIndexCommit.getGeneration());

                // now create and write the commit point
                // 该对象用于描述 一次快照任务生成的所有快照文件的描述信息
                final BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot(snapshotId.getName(),
                    lastSnapshotStatus.getIndexVersion(),
                    indexCommitPointFiles,
                    lastSnapshotStatus.getStartTime(),
                    threadPool.absoluteTimeInMillis() - lastSnapshotStatus.getStartTime(),
                    lastSnapshotStatus.getIncrementalFileCount(),
                    lastSnapshotStatus.getIncrementalSize()
                );

                logger.trace("[{}] [{}] writing shard snapshot file", shardId, snapshotId);
                try {
                    // 这个跟 indexShardSnapshotsFormat 是不一样的 这里的数据描述所有快照
                    // snapshot 仅描述单次快照
                    indexShardSnapshotFormat.write(snapshot, shardContainer, snapshotId.getUUID(), false);
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "Failed to write commit point", e);
                }
                // build a new BlobStoreIndexShardSnapshot, that includes this one and all the saved ones
                // 以新的gen 生成一个新的 BlobStoreIndexShardSnapshot
                List<SnapshotFiles> newSnapshotsList = new ArrayList<>();
                newSnapshotsList.add(new SnapshotFiles(snapshot.snapshot(), snapshot.indexFiles(), shardStateIdentifier));
                for (SnapshotFiles point : snapshots) {
                    newSnapshotsList.add(point);
                }
                final List<String> blobsToDelete;
                final String indexGeneration;
                // 看来每次执行快照任务 shardGen 都会更新   在一开始创建快照任务时 会先获取repositoryData中最新的shardGen信息
                // 本来一次完整的流程 在快照结束后 会更新repositoryData的  但是中途某一环出现了问题就会导致加载旧的shardGen
                // 就加载不到旧的快照文件 会导致重新生成一份快照文件 不过也是小概率事件 并且最新的shardGen在结束后 也会写入到repositoryData 也就是一次失败的快照不会影响下一次任务
                // 在全局中一次只能执行一个快照任务  并且快照任务本身与删除快照任务/清理仓库任务是互斥的
                final boolean writeShardGens = SnapshotsService.useShardGenerations(repositoryMetaVersion);
                if (writeShardGens) {
                    indexGeneration = UUIDs.randomBase64UUID();
                    blobsToDelete = Collections.emptyList();
                } else {
                    // TODO 忽略兼容性代码
                    indexGeneration = Long.toString(Long.parseLong(fileListGeneration) + 1);
                    // Delete all previous index-N blobs
                    blobsToDelete = blobs.stream().filter(blob -> blob.startsWith(SNAPSHOT_INDEX_PREFIX)).collect(Collectors.toList());
                    assert blobsToDelete.stream().mapToLong(b -> Long.parseLong(b.replaceFirst(SNAPSHOT_INDEX_PREFIX, "")))
                        .max().orElse(-1L) < Long.parseLong(indexGeneration)
                        : "Tried to delete an index-N blob newer than the current generation [" + indexGeneration
                        + "] when deleting index-N blobs " + blobsToDelete;
                }
                try {
                    // 将此时最新的 BlobStoreIndexShardSnapshots 写入到container
                    writeShardIndexBlob(shardContainer, indexGeneration, new BlobStoreIndexShardSnapshots(newSnapshotsList));
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId,
                        "Failed to finalize snapshot creation [" + snapshotId + "] with shard index ["
                            + indexShardSnapshotsFormat.blobName(indexGeneration) + "]", e);
                }
                // TODO 忽略兼容代码
                if (writeShardGens == false) {
                    try {
                        shardContainer.deleteBlobsIgnoringIfNotExists(blobsToDelete);
                    } catch (IOException e) {
                        logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to delete old index-N blobs during finalization",
                            snapshotId, shardId), e);
                    }
                }
                // 代表快照的同步工作也已经完成
                snapshotStatus.moveToDone(threadPool.absoluteTimeInMillis(), indexGeneration);
                listener.onResponse(indexGeneration);
            }, listener::onFailure);

            // 当数据没有变化时 不需要执行快照任务 直接触发监听器
            if (indexIncrementalFileCount == 0) {
                allFilesUploadedListener.onResponse(Collections.emptyList());
                return;
            }
            final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
            // Start as many workers as fit into the snapshot pool at once at the most
            // 针对每个索引文件 采用并行的方式生成快照
            final int workers = Math.min(threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(), indexIncrementalFileCount);
            // 装饰了一层监听器
            final ActionListener<Void> filesListener = fileQueueListener(filesToSnapshot, workers, allFilesUploadedListener);
            for (int i = 0; i < workers; ++i) {
                executor.execute(ActionRunnable.run(filesListener,
                    // 运行下面的逻辑 并在完成后触发监听器
                    () -> {
                    BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo = filesToSnapshot.poll(0L, TimeUnit.MILLISECONDS);
                    if (snapshotFileInfo != null) {
                        store.incRef();
                        try {
                            do {
                                // 实际上就是生成了一份索引文件的副本 并存储在 repository对应的分片目录下
                                snapshotFile(snapshotFileInfo, indexId, shardId, snapshotId, snapshotStatus, store);
                                snapshotFileInfo = filesToSnapshot.poll(0L, TimeUnit.MILLISECONDS);
                            } while (snapshotFileInfo != null);
                        } finally {
                            store.decRef();
                        }
                    }
                }));
            }
        } catch (Exception e) {
            // TODO
            //  如果gen 没有找到对应的文件 也是以失败方式触发监听器 但是shard本身是可以relocation的啊 比如a 分片此时在a节点上生成快照 同时生成了一个shardGen
            //  之后快照任务结束 写入到repositoryData中 然后下次执行快照任务 重新取出 shardGen后 发现节点发生移动了 那么在b节点上 使用shardGen 当然找不到数据
            listener.onFailure(e);
        }
    }

    private static boolean assertFileContentsMatchHash(BlobStoreIndexShardSnapshot.FileInfo fileInfo, Store store) {
        try (IndexInput indexInput = store.openVerifyingInput(fileInfo.physicalName(), IOContext.READONCE, fileInfo.metadata())) {
            final byte[] tmp = new byte[Math.toIntExact(fileInfo.metadata().length())];
            indexInput.readBytes(tmp, 0, tmp.length);
            assert fileInfo.metadata().hash().bytesEquals(new BytesRef(tmp));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return true;
    }

    /**
     * 从快照中还原 数据分片
     * @param store           the store to restore the index into
     * @param snapshotId      snapshot id
     * @param indexId         id of the index in the repository from which the restore is occurring
     * @param snapshotShardId shard id (in the snapshot)
     * @param recoveryState   recovery state   在该对象内部还包含了Index  每次恢复是以index为单位的么
     *
     * @param listener        listener to invoke once done
     */
    @Override
    public void restoreShard(Store store, SnapshotId snapshotId, IndexId indexId, ShardId snapshotShardId,
                             RecoveryState recoveryState, ActionListener<Void> listener) {
        final ShardId shardId = store.shardId();
        // 生成一个桥接的监听器
        final ActionListener<Void> restoreListener = ActionListener.delegateResponse(listener,
            (l, e) -> l.onFailure(new IndexShardRestoreFailedException(shardId, "failed to restore snapshot [" + snapshotId + "]", e)));
        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);

        // 先通过索引分片id  定位到container
        final BlobContainer container = shardContainer(indexId, snapshotShardId);
        executor.execute(ActionRunnable.wrap(restoreListener, l -> {
            // 在指定快照id 后获取描述本次快照的对象 比如本次快照涉及到多少文件
            final BlobStoreIndexShardSnapshot snapshot = loadShardSnapshot(container, snapshotId);
            // 将涉及到的所有文件包装成 SnapshotFiles 对象
            final SnapshotFiles snapshotFiles = new SnapshotFiles(snapshot.snapshot(), snapshot.indexFiles(), null);
            new FileRestoreContext(metadata.name(), shardId, snapshotId, recoveryState) {

                /**
                 * 具体的恢复逻辑由子类定义
                 * @param filesToRecover List of files to restore  本次恢复需要使用到的所有快照文件
                 * @param store          Store to restore into
                 * @param listener
                 */
                @Override
                protected void restoreFiles(List<BlobStoreIndexShardSnapshot.FileInfo> filesToRecover, Store store,
                                            ActionListener<Void> listener) {
                    if (filesToRecover.isEmpty()) {
                        listener.onResponse(null);
                    } else {
                        // Start as many workers as fit into the snapshot pool at once at the most
                        final int workers =
                            Math.min(threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(), snapshotFiles.indexFiles().size());
                        final BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> files = new LinkedBlockingQueue<>(filesToRecover);
                        final ActionListener<Void> allFilesListener =
                            fileQueueListener(files, workers, ActionListener.map(listener, v -> null));
                        // restore the files from the snapshot to the Lucene store
                        for (int i = 0; i < workers; ++i) {
                            executor.execute(ActionRunnable.run(allFilesListener, () -> {
                                store.incRef();
                                try {
                                    BlobStoreIndexShardSnapshot.FileInfo fileToRecover;
                                    while ((fileToRecover = files.poll(0L, TimeUnit.MILLISECONDS)) != null) {
                                        restoreFile(fileToRecover, store);
                                    }
                                } finally {
                                    store.decRef();
                                }
                            }));
                        }
                    }
                }

                /**
                 * 实际的恢复操作
                 * @param fileInfo
                 * @param store
                 * @throws IOException
                 */
                private void restoreFile(BlobStoreIndexShardSnapshot.FileInfo fileInfo, Store store) throws IOException {
                    boolean success = false;
                    try (IndexOutput indexOutput =
                             store.createVerifyingOutput(fileInfo.physicalName(), fileInfo.metadata(), IOContext.DEFAULT)) {

                        // 如果当前文件是虚拟文件
                        if (fileInfo.name().startsWith(VIRTUAL_DATA_BLOB_PREFIX)) {
                            // 获取文件的hash信息 并写入到目标文件中
                            final BytesRef hash = fileInfo.metadata().hash();
                            indexOutput.writeBytes(hash.bytes, hash.offset, hash.length);
                            recoveryState.getIndex().addRecoveredBytesToFile(fileInfo.physicalName(), hash.length);
                        } else {
                            // 如果内部包含有效数据 通过rateLimit加工输入流  并进行写入
                            try (InputStream stream = maybeRateLimit(new SlicedInputStream(fileInfo.numberOfParts()) {
                                @Override
                                protected InputStream openSlice(long slice) throws IOException {
                                    return container.readBlob(fileInfo.partName(slice));
                                }
                            }, restoreRateLimiter, restoreRateLimitingTimeInNanos)) {
                                final byte[] buffer = new byte[BUFFER_SIZE];
                                int length;
                                // 将数据转移到 buffer中 再写入到 output中   buffer使用固定大小 避免在一次传输中占据太多内存量
                                while ((length = stream.read(buffer)) > 0) {
                                    indexOutput.writeBytes(buffer, 0, length);
                                    recoveryState.getIndex().addRecoveredBytesToFile(fileInfo.physicalName(), length);
                                }
                            }
                        }
                        Store.verify(indexOutput);
                        indexOutput.close();
                        // 将页缓存中的数据置换到磁盘中
                        store.directory().sync(Collections.singleton(fileInfo.physicalName()));
                        success = true;
                    } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                        try {
                            store.markStoreCorrupted(ex);
                        } catch (IOException e) {
                            logger.warn("store cannot be marked as corrupted", e);
                        }
                        throw ex;
                    } finally {
                        if (success == false) {
                            store.deleteQuiet(fileInfo.physicalName());
                        }
                    }
                }
            }.restore(snapshotFiles, store, l);
        }));
    }

    private static ActionListener<Void> fileQueueListener(BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> files, int workers,
                                                          ActionListener<Collection<Void>> listener) {
        return ActionListener.delegateResponse(new GroupedActionListener<>(listener, workers), (l, e) -> {
            files.clear(); // Stop uploading the remaining files if we run into any exception
            l.onFailure(e);
        });
    }

    /**
     * 如果传入了限流器的话  使用限流器包装输入流
     * @param stream
     * @param rateLimiter
     * @param metric  每次触发监听器都会累加这个计数器
     * @return
     */
    private static InputStream maybeRateLimit(InputStream stream, @Nullable RateLimiter rateLimiter, CounterMetric metric) {
        return rateLimiter == null ? stream : new RateLimitingInputStream(stream, rateLimiter, metric::inc);
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        BlobStoreIndexShardSnapshot snapshot = loadShardSnapshot(shardContainer(indexId, shardId), snapshotId);
        return IndexShardSnapshotStatus.newDone(snapshot.startTime(), snapshot.time(),
            snapshot.incrementalFileCount(), snapshot.totalFileCount(),
            snapshot.incrementalSize(), snapshot.totalSize(), null); // Not adding a real generation here as it doesn't matter to callers
    }

    /**
     * 就是检测能否正常写入blob 失败的情况下抛出异常
     * @param seed
     * @param localNode         the local node information, for inclusion in verification errors
     */
    @Override
    public void verify(String seed, DiscoveryNode localNode) {
        assertSnapshotOrGenericThread();
        if (isReadOnly()) {
            try {
                latestIndexBlobId();
            } catch (IOException e) {
                throw new RepositoryVerificationException(metadata.name(), "path " + basePath() +
                    " is not accessible on node " + localNode, e);
            }
        } else {
            BlobContainer testBlobContainer = blobStore().blobContainer(basePath().add(testBlobPrefix(seed)));
            try {
                BytesArray bytes = new BytesArray(seed);
                try (InputStream stream = bytes.streamInput()) {
                    testBlobContainer.writeBlob("data-" + localNode.getId() + ".dat", stream, bytes.length(), true);
                }
            } catch (IOException exp) {
                throw new RepositoryVerificationException(metadata.name(), "store location [" + blobStore() +
                    "] is not accessible on the node [" + localNode + "]", exp);
            }
            try (InputStream masterDat = testBlobContainer.readBlob("master.dat")) {
                final String seedRead = Streams.readFully(masterDat).utf8ToString();
                if (seedRead.equals(seed) == false) {
                    throw new RepositoryVerificationException(metadata.name(), "Seed read from master.dat was [" + seedRead +
                        "] but expected seed [" + seed + "]");
                }
            } catch (NoSuchFileException e) {
                throw new RepositoryVerificationException(metadata.name(), "a file written by master to the store [" + blobStore() +
                    "] cannot be accessed on the node [" + localNode + "]. " +
                    "This might indicate that the store [" + blobStore() + "] is not shared between this node and the master node or " +
                    "that permissions on the store don't allow reading files written by the master node", e);
            } catch (IOException e) {
                throw new RepositoryVerificationException(metadata.name(), "Failed to verify repository", e);
            }
        }
    }

    @Override
    public String toString() {
        return "BlobStoreRepository[" +
            "[" + metadata.name() +
            "], [" + blobStore.get() + ']' +
            ']';
    }

    /**
     * Delete snapshot from shard level metadata.
     * @param survivingSnapshots
     * @param indexId
     * @param snapshotShardId
     * @param blobs
     * @param snapshotIds
     * @param indexGeneration
     * 更新分片级别的快照数据 主要就是只保留 surviveSnapshot 并作为一份新的快照数据存储到新的 shardGen目录下
     */
    private ShardSnapshotMetaDeleteResult deleteFromShardSnapshotMeta(Set<SnapshotId> survivingSnapshots, // 在一次删除快照任务中 需要保留的快照
                                                                      IndexId indexId,  // 本次在处理的indexId 一次快照任务会对应的多个index
                                                                      int snapshotShardId, // 每次删除的最细粒度是对应到 indexId/shardId
                                                                      Collection<SnapshotId> snapshotIds, // 某次快照删除任务对应的所有快照id
                                                                      BlobContainer shardContainer, // index.shardId 级别的容器 下面存储了一堆以shardGen为名字的文件夹
                                                                                                    // 内部就是每次针对该分片的快照数据
                                                                      Set<String> blobs,  // 对应各种shardGen目录
                                                                      BlobStoreIndexShardSnapshots snapshots, // 最新的快照数据 记录了往期快照数据
                                                                      String indexGeneration  // 这里没有立即删除快照文件中的数据 而是新生成一个文件 同时该文件内部剔除了本次要删除的快照数据
    ) {
        // Build a list of snapshots that should be preserved
        List<SnapshotFiles> newSnapshotsList = new ArrayList<>();
        // 将这组快照id 转换成快照名
        final Set<String> survivingSnapshotNames = survivingSnapshots.stream().map(SnapshotId::getName).collect(Collectors.toSet());
        // 为需要保留的文件单独生成一份新的 快照数据   每个snapshotFiles 就对应了某次快照任务下所有的文件
        for (SnapshotFiles point : snapshots) {
            if (survivingSnapshotNames.contains(point.snapshot())) {
                newSnapshotsList.add(point);
            }
        }
        try {
            if (newSnapshotsList.isEmpty()) {
                // 代表所有blobs都需要删除
                return new ShardSnapshotMetaDeleteResult(indexId, snapshotShardId, ShardGenerations.DELETED_SHARD_GEN, blobs);
            } else {

                final BlobStoreIndexShardSnapshots updatedSnapshots = new BlobStoreIndexShardSnapshots(newSnapshotsList);
                // 写入一个最新的文件
                writeShardIndexBlob(shardContainer, indexGeneration, updatedSnapshots);
                final Set<String> survivingSnapshotUUIDs = survivingSnapshots.stream().map(SnapshotId::getUUID).collect(Collectors.toSet());

                return new ShardSnapshotMetaDeleteResult(indexId, snapshotShardId, indexGeneration,
                    // 从剩下的文件中找到允许被删除的文件
                    unusedBlobs(blobs, survivingSnapshotUUIDs, updatedSnapshots));
            }
        } catch (IOException e) {
            throw new RepositoryException(metadata.name(), "Failed to finalize snapshot deletion " + snapshotIds +
                    " with shard index [" + indexShardSnapshotsFormat.blobName(indexGeneration) + "]", e);
        }
    }

    /**
     * 将最新的快照数据写入到目标container中
     * @param shardContainer
     * @param indexGeneration
     * @param updatedSnapshots
     * @throws IOException
     */
    private void writeShardIndexBlob(BlobContainer shardContainer, String indexGeneration,
                                     BlobStoreIndexShardSnapshots updatedSnapshots) throws IOException {
        assert ShardGenerations.NEW_SHARD_GEN.equals(indexGeneration) == false;
        assert ShardGenerations.DELETED_SHARD_GEN.equals(indexGeneration) == false;
        indexShardSnapshotsFormat.writeAtomic(updatedSnapshots, shardContainer, indexGeneration);
    }

    /**
     * Unused blobs are all previous index-, data- and meta-blobs and that are not referenced by the new index- as well as all
     * temporary blobs
     * @param blobs
     * @param survivingSnapshotUUIDs
     * @param updatedSnapshots
     * @return
     * 在执行删除快照的任务后  会将此时存活的快照数据保存在一个新的 gen对应的目录下 这里是要检测哪些数据可以被删除
     */
    private static List<String> unusedBlobs(Set<String> blobs, Set<String> survivingSnapshotUUIDs,
                                            BlobStoreIndexShardSnapshots updatedSnapshots) {
        return blobs.stream().filter(blob ->
            // 之前所有的 index-? 文件都可以被丢弃了
            blob.startsWith(SNAPSHOT_INDEX_PREFIX)
            // TODO 以下3种情况 先忽略
            || (blob.startsWith(SNAPSHOT_PREFIX) && blob.endsWith(".dat") && survivingSnapshotUUIDs.contains(blob.substring(SNAPSHOT_PREFIX.length(), blob.length() - ".dat".length())) == false)
            || (blob.startsWith(UPLOADED_DATA_BLOB_PREFIX) && updatedSnapshots.findNameFile(canonicalName(blob)) == null)
            || FsBlobContainer.isTempBlobName(blob)).collect(Collectors.toList());
    }

    /**
     * Loads information about shard snapshot
     */
    public BlobStoreIndexShardSnapshot loadShardSnapshot(BlobContainer shardContainer, SnapshotId snapshotId) {
        try {
            return indexShardSnapshotFormat.read(shardContainer, snapshotId.getUUID());
        } catch (NoSuchFileException ex) {
            throw new SnapshotMissingException(metadata.name(), snapshotId, ex);
        } catch (IOException ex) {
            throw new SnapshotException(metadata.name(), snapshotId,
                "failed to read shard snapshot file for [" + shardContainer.path() + ']', ex);
        }
    }

    /**
     * Loads all available snapshots in the repository using the given {@code generation} or falling back to trying to determine it from
     * the given list of blobs in the shard container.
     *
     * @param blobs      list of blobs in repository
     * @param shardContainer
     * @param generation shard generation or {@code null} in case there was no shard generation tracked in the {@link RepositoryData} for
     *                   this shard because its snapshot was created in a version older than
     *                   {@link SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION}.
     * @return tuple of BlobStoreIndexShardSnapshots and the last snapshot index generation
     */
    private Tuple<BlobStoreIndexShardSnapshots, String> buildBlobStoreIndexShardSnapshots(Set<String> blobs,    // 对应上次快照数据的目录
                                                                                          BlobContainer shardContainer,   // 存储分片快照数据的目录
                                                                                          @Nullable String generation  // 上次shard快照数据的gen
    ) throws IOException {
        if (generation != null) {
            // 代表本次快照对应的索引是刚创建的  之前还没有该分片的数据
            if (generation.equals(ShardGenerations.NEW_SHARD_GEN)) {
                return new Tuple<>(BlobStoreIndexShardSnapshots.EMPTY, ShardGenerations.NEW_SHARD_GEN);
            }
            // 读取之前的数据  indexShardSnapshotsFormat 定义了数据反序列化方式
            return new Tuple<>(indexShardSnapshotsFormat.read(shardContainer, generation), generation);
        }
        // TODO 忽略兼容逻辑
        final Tuple<BlobStoreIndexShardSnapshots, Long> legacyIndex = buildBlobStoreIndexShardSnapshots(blobs, shardContainer);
        return new Tuple<>(legacyIndex.v1(), String.valueOf(legacyIndex.v2()));
    }

    /**
     * Loads all available snapshots in the repository
     *
     * @param blobs list of blobs in repository
     * @return tuple of BlobStoreIndexShardSnapshots and the last snapshot index generation
     */
    private Tuple<BlobStoreIndexShardSnapshots, Long> buildBlobStoreIndexShardSnapshots(Set<String> blobs, BlobContainer shardContainer)
            throws IOException {
        long latest = latestGeneration(blobs);
        if (latest >= 0) {
            final BlobStoreIndexShardSnapshots shardSnapshots = indexShardSnapshotsFormat.read(shardContainer, Long.toString(latest));
            return new Tuple<>(shardSnapshots, latest);
        } else if (blobs.stream().anyMatch(b -> b.startsWith(SNAPSHOT_PREFIX) || b.startsWith(INDEX_FILE_PREFIX)
                                                                              || b.startsWith(UPLOADED_DATA_BLOB_PREFIX))) {
            throw new IllegalStateException(
                "Could not find a readable index-N file in a non-empty shard snapshot directory [" + shardContainer.path() + "]");
        }
        return new Tuple<>(BlobStoreIndexShardSnapshots.EMPTY, latest);
    }

    /**
     * Snapshot individual file
     * @param fileInfo file to be snapshotted
     *
     *                 针对此时的索引文件 生成一份快照文件
     *                 实际上就像是拷贝一份文件副本
     */
    private void snapshotFile(BlobStoreIndexShardSnapshot.FileInfo fileInfo, IndexId indexId, ShardId shardId, SnapshotId snapshotId,
                              IndexShardSnapshotStatus snapshotStatus, Store store) throws IOException {
        // 定位该分片在仓库中的位置
        final BlobContainer shardContainer = shardContainer(indexId, shardId);
        // 获取本次快照文件对应的名字
        final String file = fileInfo.physicalName();
        try (IndexInput indexInput = store.openVerifyingInput(file, IOContext.READONCE, fileInfo.metadata())) {

            // 在生成快照时 将文件按照以几个数据块的形式进行写入
            for (int i = 0; i < fileInfo.numberOfParts(); i++) {
                // 每个部分的长度
                final long partBytes = fileInfo.partBytes(i);

                // Make reads abortable by mutating the snapshotStatus object
                // 在读取快照数据时 使用到了限流器   同时每个inputStream 只读取一个片段
                final InputStream inputStream = new FilterInputStream(maybeRateLimit(
                    new InputStreamIndexInput(indexInput, partBytes), snapshotRateLimiter, snapshotRateLimitingTimeInNanos)) {
                    @Override
                    public int read() throws IOException {
                        checkAborted();
                        return super.read();
                    }

                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        checkAborted();
                        return super.read(b, off, len);
                    }

                    private void checkAborted() {
                        if (snapshotStatus.isAborted()) {
                            logger.debug("[{}] [{}] Aborted on the file [{}], exiting", shardId,
                                snapshotId, fileInfo.physicalName());
                            throw new IndexShardSnapshotFailedException(shardId, "Aborted");
                        }
                    }
                };
                // 索引文件被拆分后 每个文件的命名变成了 xxx.part1/xxx.part2 这种格式
                shardContainer.writeBlob(fileInfo.partName(i), inputStream, partBytes, true);
            }
            // 校验checksum
            Store.verify(indexInput);
            // 增加一个已经处理完的快照对象
            snapshotStatus.addProcessedFile(fileInfo.length());
        } catch (Exception t) {
            failStoreIfCorrupted(store, t);
            snapshotStatus.addProcessedFile(0);
            throw t;
        }
    }

    private static void failStoreIfCorrupted(Store store, Exception e) {
        if (Lucene.isCorruptionException(e)) {
            try {
                store.markStoreCorrupted((IOException) e);
            } catch (IOException inner) {
                inner.addSuppressed(e);
                logger.warn("store cannot be marked as corrupted", inner);
            }
        }
    }

    /**
     * The result of removing a snapshot from a shard folder in the repository.
     * 描述一次删除快照任务的结果
     * 删除任务会被细化到 shard级别
     */
    private static final class ShardSnapshotMetaDeleteResult {

        // Index that the snapshot was removed from  本次子任务针对的是哪个index
        private final IndexId indexId;

        // Shard id that the snapshot was removed from  本次子任务针对的是哪个shard
        private final int shardId;

        // Id of the new index-${uuid} blob that does not include the snapshot any more
        // 此次更新后保留的快照数据存储的shardGen  如果本次所有快照文件都被删除 该值为 "_deleted"
        private final String newGeneration;

        // Blob names in the shard directory that have become unreferenced in the new shard generation
        // 本次要删除的一组文件的名字
        private final Collection<String> blobsToDelete;

        /**
         *
         * @param indexId  本次删除涉及到了这个index
         * @param shardId  涉及到了这个shard
         * @param newGeneration  当该分片下所有的快照数据都被删除时  也就是此时已经没有数据了   gen = "_delete"
         * @param blobsToDelete  本次被删除的所有快照对应的所有文件  一次快照可能会关联很多文件
         */
        ShardSnapshotMetaDeleteResult(IndexId indexId, int shardId, String newGeneration, Collection<String> blobsToDelete) {
            this.indexId = indexId;
            this.shardId = shardId;
            this.newGeneration = newGeneration;
            this.blobsToDelete = blobsToDelete;
        }
    }
}
