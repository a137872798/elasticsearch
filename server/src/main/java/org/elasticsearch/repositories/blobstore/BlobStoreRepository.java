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
     * 启动时 发现上次的写入是否被中断
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
     * @param metadata   The metadata for this repository including name and settings  在初始化存储对象时还会传入相关的元数据
     * @param clusterService ClusterService
     * @param basePath 定义store的基础目录
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
        // 每秒仅允许写入 40MB    文件系统的限流有什么意义么
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
        // 推测每次写入前 先设置pendingGen  当写入完成时 将 gen 同于到 pendingGen中
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
     * 使用指定的函数将此时最新的存储数据转换成 clusterState更新任务 并通过masterService发布到集群中其他节点  本方法应该是只能由leader调用
     * @param createUpdateTask function to supply cluster state update task  该函数定义了如何将存储的数据 生成最新clusterState任务
     * @param source           the source of the cluster state update task
     * @param onFailure        error handler invoked on failure to get a consistent view of the current {@link RepositoryData}
     *                         当还没有执行update任务前出现的异常 触发该函数
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

                    // 该任务执行后 如果clusterState 发生了变化 会通过coordinator 发布到此时集群能观测 到的所有节点  并且只有写入的可选举节点超过半数时才算真正写入
                    clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask(updateTask.priority()) {

                        /**
                         * 是否已经执行了任务 在之后完updateTask后 还是有可能失败的
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
                            // 适配器模式 只有确保能进行修正 才会使用通过映射函数生成的 updateTask
                            // 这里就是做一层校验 确保当前clusterState中 使用该存储层的name 可以找到metadata
                            if (repositoryMetadataStart.equals(getRepoMetadata(currentState))) {
                                executedTask = true;
                                return updateTask.execute(currentState);
                            }
                            return currentState;
                        }

                        @Override
                        public void onFailure(String source, Exception e) {

                            // 如果已经开始执行任务了 那么触发updateTask的失败异常
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
                                // TODO 正常情况应该是不会走这里的 在还没有理解上下文的情况下 还是先不看了
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
    // 将最新state的数据存储起来
    @Override
    public void updateState(ClusterState state) {
        // 从当前集群状态中获取该存储实现相关的元数据  该元数据是以custom 的形式存储在clusterState中的
        metadata = getRepoMetadata(state);
        // 代表上次的存储工作未完成
        uncleanStart = uncleanStart && metadata.generation() != metadata.pendingGeneration();
        // 在上次的存储工作没有正常完成的情况下 尽可能从  Store中获取最准确的数据
        // metadata.generation() == RepositoryData.UNKNOWN_REPO_GEN 应该是代表在这种场景下没法使用缓存吧
        bestEffortConsistency = uncleanStart || isReadOnly() || metadata.generation() == RepositoryData.UNKNOWN_REPO_GEN;
        // 只读场景下 不应该执行写入操作
        if (isReadOnly()) {
            // No need to waste cycles, no operations can run against a read-only repository
            return;
        }

        // 在追求强一致的情况下 会获取各个正在执行中的快照相关的任务 并尝试获取最新的gen
        if (bestEffortConsistency) {

            // 以下几个 progress对象都实现了相同的api 也就是获取 repositoryStateId 的 该返回值会作为bestGen使用  挨个往下会覆盖之前获取到的gen
            // TODO 待理解

            long bestGenerationFromCS = RepositoryData.EMPTY_REPO_GEN;
            // 这个快照状态应该也是按照不同的存储层实现划分的
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            if (snapshotsInProgress != null) {
                bestGenerationFromCS = bestGeneration(snapshotsInProgress.entries());
            }

            // 快照删除的进度
            final SnapshotDeletionsInProgress deletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            // Don't use generation from the delete task if we already found a generation for an in progress snapshot.
            // In this case, the generation points at the generation the repo will be in after the snapshot finishes so it may not yet
            // exist
            if (bestGenerationFromCS == RepositoryData.EMPTY_REPO_GEN && deletionsInProgress != null) {
                bestGenerationFromCS = bestGeneration(deletionsInProgress.getEntries());
            }
            final RepositoryCleanupInProgress cleanupInProgress = state.custom(RepositoryCleanupInProgress.TYPE);
            if (bestGenerationFromCS == RepositoryData.EMPTY_REPO_GEN && cleanupInProgress != null) {
                bestGenerationFromCS = bestGeneration(cleanupInProgress.entries());
            }
            final long finalBestGen = Math.max(bestGenerationFromCS, metadata.generation());
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
     * 寻找一个最合适的gen
     * @param operations
     * @return
     */
    private long bestGeneration(Collection<? extends RepositoryOperation> operations) {
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
     * 根据传入的 snapshotId  删除命中的快照
     * 目前只有S3实现有覆盖该方法 所以先忽略
     * @param snapshotIds           snapshot ids
     * @param repositoryStateId     the unique id identifying the state of the repository when the snapshot deletion began   在执行删除任务时 预期的gen
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
                // 获取当前容器下所有的blob对象
                final Map<String, BlobMetadata> rootBlobs = blobContainer().listBlobs();
                // 以repositoryStateId 作为gen 获取存储的数据流 并通过反序列化工具还原成bean对象   safe的意思是如果此时获取的gen与预期的传入不同则会抛出异常
                final RepositoryData repositoryData = safeRepositoryData(repositoryStateId, rootBlobs);
                // Cache the indices that were found before writing out the new index-N blob so that a stuck master will never
                // delete an index that was created by another master node after writing this index-N blob.

                // 获取 /indices 下所有的文件夹
                final Map<String, BlobContainer> foundIndices = blobStore().blobContainer(indicesPath()).children();
                // 按照要求删除数据
                doDeleteShardSnapshots(snapshotIds, repositoryStateId, foundIndices, rootBlobs, repositoryData,
                    // 根据当前存储层的版本号 检测是否使用了分片gen 并作为参数传入到 doDelete方法中
                    SnapshotsService.useShardGenerations(repositoryMetaVersion), listener);
            } catch (Exception ex) {
                listener.onFailure(new RepositoryException(metadata.name(), "failed to delete snapshots " + snapshotIds, ex));
            }
        }
    }

    /**
     * Loads {@link RepositoryData} ensuring that it is consistent with the given {@code rootBlobs} as well of the assumed generation.
     *
     * @param repositoryStateId Expected repository generation
     * @param rootBlobs         Blobs at the repository root  这些数据体的命名格式就是 index-gen
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
        // 当此时的gen 与预期不符时 抛出异常
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
     *                          delete operation    本次范围内所有可删除的container (对于FsStoreRepository来说实际上就是文件夹)
     * @param rootBlobs         All blobs found at the root of the repository before executing any writes to the repository during this
     *                          delete operation
     * @param repositoryData    RepositoryData found the in the repository before executing this delete      通过 repositoryStateId 找到的数据流还原出的对象
     * @param listener          Listener to invoke once finished  这个是用户定义的处理逻辑
     *                          执行删除快照的操作
     */
    private void doDeleteShardSnapshots(Collection<SnapshotId> snapshotIds, long repositoryStateId, Map<String, BlobContainer> foundIndices,
                                        Map<String, BlobMetadata> rootBlobs, RepositoryData repositoryData, boolean writeShardGens,
                                        ActionListener<Void> listener) {

        // 如果连同分片一起存储了 目前都是该情况
        if (writeShardGens) {
            // First write the new shard state metadata (with the removed snapshot) and compute deletion targets
            final StepListener<Collection<ShardSnapshotMetaDeleteResult>> writeShardMetaDataAndComputeDeletesStep = new StepListener<>();

            // 将需要保留的快照数据单独生成文件 并使用描述本次删除结果的deleteResult 触发监听器
            writeUpdatedShardMetaDataAndComputeDeletes(snapshotIds, repositoryData, true, writeShardMetaDataAndComputeDeletesStep);
            // Once we have put the new shard-level metadata into place, we can update the repository metadata as follows:
            // 1. Remove the snapshots from the list of existing snapshots
            // 2. Update the index shard generations of all updated shard folders
            //
            // Note: If we fail updating any of the individual shard paths, none of them are changed since the newly created
            //       index-${gen_uuid} will not be referenced by the existing RepositoryData and new RepositoryData is only
            //       written if all shard paths have been successfully updated.
            final StepListener<RepositoryData> writeUpdatedRepoDataStep = new StepListener<>();

            // 当 writeUpdatedShardMetaDataAndComputeDeletes 完成后触发监听器
            writeShardMetaDataAndComputeDeletesStep.whenComplete(deleteResults -> {
                final ShardGenerations.Builder builder = ShardGenerations.builder();
                for (ShardSnapshotMetaDeleteResult newGen : deleteResults) {
                    // 把每个新快照对应的gen indexId shardId 组合并存储起来
                    builder.put(newGen.indexId, newGen.shardId, newGen.newGeneration);
                }
                // 将本次的变化同步到 RepositoryData中
                final RepositoryData updatedRepoData = repositoryData.removeSnapshots(snapshotIds, builder.build());

                // 将最新的gen 写入到store 以及发布到集群中  当成功时还会删除一些 index-n 文件
                // 这里传入的 clusterState-> newClusterState 是 Function.identity()
                writeIndexGen(updatedRepoData, repositoryStateId, true, Function.identity(),
                    ActionListener.wrap(v -> writeUpdatedRepoDataStep.onResponse(updatedRepoData), listener::onFailure));
            }, listener::onFailure);
            // Once we have updated the repository, run the clean-ups
            // 当上面的发布新gen 以及删除操作完成时 执行最后一步 异步清理数据
            writeUpdatedRepoDataStep.whenComplete(updatedRepoData -> {
                // Run unreferenced blobs cleanup in parallel to shard-level snapshot deletion
                final ActionListener<Void> afterCleanupsListener =
                    new GroupedActionListener<>(ActionListener.wrap(() -> listener.onResponse(null)), 2);
                // 总计2个清理任务
                asyncCleanupUnlinkedRootAndIndicesBlobs(foundIndices, rootBlobs, updatedRepoData, afterCleanupsListener);
                asyncCleanupUnlinkedShardLevelBlobs(snapshotIds, writeShardMetaDataAndComputeDeletesStep.result(), afterCleanupsListener);
            }, listener::onFailure);
            // 兼容旧版本的忽略
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
     * @param foundIndices
     * @param rootBlobs
     * @param updatedRepoData
     * @param listener
     */
    private void asyncCleanupUnlinkedRootAndIndicesBlobs(Map<String, BlobContainer> foundIndices, Map<String, BlobMetadata> rootBlobs,
                                                         RepositoryData updatedRepoData, ActionListener<Void> listener) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(
            listener,
            // 清理过期的blob 对象
            l -> cleanupStaleBlobs(foundIndices, rootBlobs, updatedRepoData, ActionListener.map(l, ignored -> null))));
    }

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
     * @param snapshotIds  对应需要删除的快照id
     * @param oldRepositoryData 当前 repositoryData 在处理后应该会发生变化
     * @param useUUIDs  在7.6版本后该值为true
     * @param onAllShardsCompleted
     */
    private void writeUpdatedShardMetaDataAndComputeDeletes(Collection<SnapshotId> snapshotIds, RepositoryData oldRepositoryData,
            boolean useUUIDs, ActionListener<Collection<ShardSnapshotMetaDeleteResult>> onAllShardsCompleted) {

        // 获取执行快照任务相关的线程池
        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        // 找到命中快照id的所有索引id
        final List<IndexId> indices = oldRepositoryData.indicesToUpdateAfterRemovingSnapshot(snapshotIds);

        // 没有命中的索引 可以直接触发监听器了
        if (indices.isEmpty()) {
            onAllShardsCompleted.onResponse(Collections.emptyList());
            return;
        }

        // Listener that flattens out the delete results for each index
        // 也是做了一层适配 当每个索引都被处理完后 触发监听器
        final ActionListener<Collection<ShardSnapshotMetaDeleteResult>> deleteIndexMetadataListener = new GroupedActionListener<>(
            ActionListener.map(onAllShardsCompleted,
                // GroupedActionListener 在触发时传入的是一个list 通过map函数平铺后 触发onAllShardsCompleted
                res -> res.stream().flatMap(Collection::stream).collect(Collectors.toList())), indices.size());

        for (IndexId indexId : indices) {
            // 未被命中的 snapshotId 就是需要保留的id
            final Set<SnapshotId> survivingSnapshots = oldRepositoryData.getSnapshots(indexId).stream()
                .filter(id -> snapshotIds.contains(id) == false).collect(Collectors.toSet());
            final StepListener<Collection<Integer>> shardCountListener = new StepListener<>();
            final ActionListener<Integer> allShardCountsListener = new GroupedActionListener<>(shardCountListener, snapshotIds.size());
            for (SnapshotId snapshotId : snapshotIds) {
                // 将一个个异步任务丢进去 每个任务会生成一个id  全部执行完后会触发  shardCountListener
                executor.execute(ActionRunnable.supply(allShardCountsListener, () -> {
                    try {
                        // 找到该快照下该索引包含的所有分片
                        return getSnapshotIndexMetadata(snapshotId, indexId).getNumberOfShards();
                    } catch (Exception ex) {
                        logger.warn(() -> new ParameterizedMessage(
                                "[{}] [{}] failed to read metadata for index", snapshotId, indexId.getName()), ex);
                        // Just invoke the listener without any shard generations to count it down, this index will be cleaned up
                        // by the stale data cleanup in the end.
                        // TODO: Getting here means repository corruption. We should find a way of dealing with this instead of just
                        //       ignoring it and letting the cleanup deal with it.
                        return null;
                    }
                }));
            }
            // 制定完成任务后的逻辑
            shardCountListener.whenComplete(counts -> {
                // 找到最大的分片数 主要是检测最大分片数是否为0
                final int shardCount = counts.stream().mapToInt(i -> i).max().orElse(0);
                if (shardCount == 0) {
                    // 该快照下该索引并没有实际的分片 直接触发监听器
                    deleteIndexMetadataListener.onResponse(null);
                    return;
                }
                // Listener for collecting the results of removing the snapshot from each shard's metadata in the current index
                final ActionListener<ShardSnapshotMetaDeleteResult> allShardsListener =
                        new GroupedActionListener<>(deleteIndexMetadataListener, shardCount);
                for (int shardId = 0; shardId < shardCount; shardId++) {
                    final int finalShardId = shardId;
                    executor.execute(new AbstractRunnable() {
                        @Override
                        protected void doRun() throws Exception {
                            // 这里是怎么反向查找的还没理解 主要看数据是如何存入的
                            final BlobContainer shardContainer = shardContainer(indexId, finalShardId);
                            final Set<String> blobs = shardContainer.listBlobs().keySet();
                            final BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots;
                            final String newGen;
                            if (useUUIDs) {
                                newGen = UUIDs.randomBase64UUID();
                                // 获取此时的gen 并定位到的数据体的文件流 并反序列化成对象  应该是这样 即使是同一个索引 同一个分片 该目录下可能有一组数据 他们对应的gen不同
                                blobStoreIndexShardSnapshots = buildBlobStoreIndexShardSnapshots(blobs, shardContainer,
                                        oldRepositoryData.shardGenerations().getShardGen(indexId, finalShardId)).v1();
                            } else {
                                // TODO 先忽略 感觉是兼容旧逻辑的
                                Tuple<BlobStoreIndexShardSnapshots, Long> tuple = buildBlobStoreIndexShardSnapshots(blobs, shardContainer);
                                newGen = Long.toString(tuple.v2() + 1);
                                blobStoreIndexShardSnapshots = tuple.v1();
                            }
                            // newGen 代表针对新快照数据的gen   这个套路跟lucene有点类似
                            // 当需要删除命中某些Query的Doc时 不会在原来的segment上直接修改 而是将删除目标doc后剩余的doc生成一个新的segment

                            // 每个shardId + indexId 总能找到一个 snapshots 对象 通过找到符合条件的删除数 生成新的快照后 生成deleteResult 触发监听器
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
     * @param rootBlobs    all blobs found directly under the repository root
     * @param newRepoData  new repository data that was just written
     * @param listener     listener to invoke with the combined {@link DeleteResult} of all blobs removed in this operation
     *
     *                     清理此时已经认定为失效的 blob 对象
     */
    private void cleanupStaleBlobs(Map<String, BlobContainer> foundIndices, Map<String, BlobMetadata> rootBlobs,
                                   RepositoryData newRepoData, ActionListener<DeleteResult> listener) {
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
            List<String> deletedBlobs = cleanupStaleRootFiles(staleRootBlobs(newRepoData, rootBlobs.keySet()));
            return new DeleteResult(deletedBlobs.size(), deletedBlobs.stream().mapToLong(name -> rootBlobs.get(name).length()).sum());
        }));

        final Set<String> survivingIndexIds = newRepoData.getIndices().values().stream().map(IndexId::getId).collect(Collectors.toSet());
        // 清除掉过期索引
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
     */
    public void cleanup(long repositoryStateId, Version repositoryMetaVersion, ActionListener<RepositoryCleanupResult> listener) {
        try {
            if (isReadOnly()) {
                throw new RepositoryException(metadata.name(), "cannot run cleanup on readonly repository");
            }
            Map<String, BlobMetadata> rootBlobs = blobContainer().listBlobs();
            final RepositoryData repositoryData = safeRepositoryData(repositoryStateId, rootBlobs);
            final Map<String, BlobContainer> foundIndices = blobStore().blobContainer(indicesPath()).children();
            final Set<String> survivingIndexIds =
                repositoryData.getIndices().values().stream().map(IndexId::getId).collect(Collectors.toSet());
            final List<String> staleRootBlobs = staleRootBlobs(repositoryData, rootBlobs.keySet());
            if (survivingIndexIds.equals(foundIndices.keySet()) && staleRootBlobs.isEmpty()) {
                // Nothing to clean up we return
                listener.onResponse(new RepositoryCleanupResult(DeleteResult.ZERO));
            } else {
                // write new index-N blob to ensure concurrent operations will fail
                writeIndexGen(repositoryData, repositoryStateId, SnapshotsService.useShardGenerations(repositoryMetaVersion),
                        Function.identity(), ActionListener.wrap(v -> cleanupStaleBlobs(foundIndices, rootBlobs, repositoryData,
                        ActionListener.map(listener, RepositoryCleanupResult::new)), listener::onFailure));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    // Finds all blobs directly under the repository root path that are not referenced by the current RepositoryData
    // 以root目录为起始位置 找到所有未引用的blob
    private List<String> staleRootBlobs(RepositoryData repositoryData, Set<String> rootBlobNames) {
        // 找到此时还有效的快照
        final Set<String> allSnapshotIds =
            repositoryData.getSnapshotIds().stream().map(SnapshotId::getUUID).collect(Collectors.toSet());
        return rootBlobNames.stream().filter(
            blob -> {
                // 如果时临时文件 需要被删除
                if (FsBlobContainer.isTempBlobName(blob)) {
                    return true;
                }
                if (blob.endsWith(".dat")) {
                    final String foundUUID;
                    // 如果此时尾缀的 uuid 已经与当前blob不一致  就代表该blob 已经过时了
                    if (blob.startsWith(SNAPSHOT_PREFIX)) {
                        foundUUID = blob.substring(SNAPSHOT_PREFIX.length(), blob.length() - ".dat".length());
                        assert snapshotFormat.blobName(foundUUID).equals(blob);
                    } else if (blob.startsWith(METADATA_PREFIX)) {
                        foundUUID = blob.substring(METADATA_PREFIX.length(), blob.length() - ".dat".length());
                        assert globalMetadataFormat.blobName(foundUUID).equals(blob);
                    } else {
                        return false;
                    }
                    return allSnapshotIds.contains(foundUUID) == false;
                    // 如果是 index-n 文件   gen匹配不上则进行删除
                } else if (blob.startsWith(INDEX_FILE_PREFIX)) {
                    // TODO: Include the current generation here once we remove keeping index-(N-1) around from #writeIndexGen
                    return repositoryData.getGenId() > Long.parseLong(blob.substring(INDEX_FILE_PREFIX.length()));
                }
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
     * 结束某个快照进程
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
     */
    @Override
    public void finalizeSnapshot(final SnapshotId snapshotId,   // 本次要关闭的快照
                                 final ShardGenerations shardGenerations,  // 每个索引下每个分片对应的 gen
                                 final long startTime,    // 开始生成快照的时间
                                 final String failure,
                                 final int totalShards,  // 本次总计涉及到多少分片
                                 final List<SnapshotShardFailure> shardFailures,   // 本次写入中 总计有多少分片失败了
                                 final long repositoryStateId,
                                 final boolean includeGlobalState,
                                 final Metadata clusterMetadata,
                                 final Map<String, Object> userMetadata,
                                 Version repositoryMetaVersion,  // 通过该版本号检测是否要将 分片的gen写入到存储层
                                 Function<ClusterState, ClusterState> stateTransformer,
                                 final ActionListener<Tuple<RepositoryData, SnapshotInfo>> listener) {
        assert repositoryStateId > RepositoryData.UNKNOWN_REPO_GEN :
            "Must finalize based on a valid repository generation but received [" + repositoryStateId + "]";

        // 本次生成快照相关的所有索引id
        final Collection<IndexId> indices = shardGenerations.indices();
        // Once we are done writing the updated index-N blob we remove the now unreferenced index-${uuid} blobs in each shard
        // directory if all nodes are at least at version SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION
        // If there are older version nodes in the cluster, we don't need to run this cleanup as it will have already happened
        // when writing the index-${N} to each shard directory.
        // 在7.6版本之后会将 shardgen 写入到存储层中
        final boolean writeShardGens = SnapshotsService.useShardGenerations(repositoryMetaVersion);
        final Consumer<Exception> onUpdateFailure =
            e -> listener.onFailure(new SnapshotException(metadata.name(), snapshotId, "failed to update snapshot in repository", e));
        final ActionListener<SnapshotInfo> allMetaListener = new GroupedActionListener<>(
            ActionListener.wrap(snapshotInfos -> {
                assert snapshotInfos.size() == 1 : "Should have only received a single SnapshotInfo but received " + snapshotInfos;
                final SnapshotInfo snapshotInfo = snapshotInfos.iterator().next();
                getRepositoryData(ActionListener.wrap(existingRepositoryData -> {
                    final RepositoryData updatedRepositoryData =
                        existingRepositoryData.addSnapshot(snapshotId, snapshotInfo.state(), Version.CURRENT, shardGenerations);
                    writeIndexGen(updatedRepositoryData, repositoryStateId, writeShardGens, stateTransformer,
                            ActionListener.wrap(writtenRepoData -> {
                                if (writeShardGens) {
                                    cleanupOldShardGens(existingRepositoryData, updatedRepositoryData);
                                }
                                listener.onResponse(new Tuple<>(writtenRepoData, snapshotInfo));
                            }, onUpdateFailure));
                }, onUpdateFailure));
            }, onUpdateFailure), 2 + indices.size());  // 这里分配的监听器数量是 索引数量 + 2
        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);

        // We ignore all FileAlreadyExistsException when writing metadata since otherwise a master failover while in this method will
        // mean that no snap-${uuid}.dat blob is ever written for this snapshot. This is safe because any updated version of the
        // index or global metadata will be compatible with the segments written in this snapshot as well.
        // Failing on an already existing index-${repoGeneration} below ensures that the index.latest blob is not updated in a way
        // that decrements the generation it points at

        // Write Global Metadata
        executor.execute(ActionRunnable.run(allMetaListener,
            () -> globalMetadataFormat.write(clusterMetadata, blobContainer(), snapshotId.getUUID(), false)));

        // write the index metadata for each index in the snapshot
        for (IndexId index : indices) {
            executor.execute(ActionRunnable.run(allMetaListener, () ->
                indexMetadataFormat.write(clusterMetadata.index(index.getName()), indexContainer(index), snapshotId.getUUID(), false)));
        }

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
     * 实际上通过模板定义文件名 和 解析规则  只要传入对应的快照id就可以
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
     * 上面的快照都是针对所有 index 的  也就是全局范围
     * 而在获取某个索引相关的快照数据前   先通过indexId 定位到子级目录
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
            // 尽最大的可能实现一致性
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
                if (bestEffortConsistency == false && cached != null && cached.v1() == genToLoad) {
                    loaded = repositoryDataFromCachedEntry(cached);
                } else {
                    // 使用最新的gen 加载数据 并反序列化
                    loaded = getRepositoryData(genToLoad);
                    // 将最新的数据设置到缓存中
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
        if (indexGen == RepositoryData.EMPTY_REPO_GEN) {
            return RepositoryData.EMPTY;
        }
        try {
            // 还原 index-n 文件
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
     * @param repositoryData RepositoryData to write  本次要写入的数据
     * @param expectedGen    expected repository generation at the start of the operation     本次预期的gen
     * @param writeShardGens whether to write {@link ShardGenerations} to the new {@link RepositoryData} blob   是否要写入分片的gen 新版本为true 7.6之前不用
     * @param stateFilter    filter for the last cluster state update executed by this method   更新集群状态
     * @param listener       completion listener   当任务完成时触发监听器
     */
    protected void writeIndexGen(RepositoryData repositoryData, long expectedGen, boolean writeShardGens,
                                 Function<ClusterState, ClusterState> stateFilter, ActionListener<RepositoryData> listener) {
        assert isReadOnly() == false; // can not write to a read only repository
        final long currentGen = repositoryData.getGenId();
        // 本次处理的gen 与预期值不同 以失败形式触发
        if (currentGen != expectedGen) {
            // the index file was updated by a concurrent operation, so we were operating on stale
            // repository data
            listener.onFailure(new RepositoryException(metadata.name(),
                "concurrent modification of the index-N file, expected current generation [" + expectedGen +
                    "], actual current generation [" + currentGen + "]"));
            return;
        }

        // Step 1: Set repository generation state to the next possible pending generation
        final StepListener<Long> setPendingStep = new StepListener<>();
        // 提交一个更新集群的任务 该任务必须在leader节点触发 同时必须写入至少 1/2 参选节点才算成功  这里只是更新pendingGen 代表即将开始写入任务
        clusterService.submitStateUpdateTask("set pending repository generation [" + metadata.name() + "][" + expectedGen + "]",
            new ClusterStateUpdateTask() {

                private long newGen;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    // 从clusterState 中获取存储层元数据
                    final RepositoryMetadata meta = getRepoMetadata(currentState);
                    final String repoName = metadata.name();
                    final long genInState = meta.generation();
                    // 如果要求强一致性场景 或者不知道当前的gen    那么会使用genInState
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
                    // 非强一致性场景使用  期望的gen  否则使用从meta中读取的最新gen
                    final long safeGeneration = expectedGen == RepositoryData.EMPTY_REPO_GEN ? RepositoryData.EMPTY_REPO_GEN
                        : (uninitializedMeta ? expectedGen : genInState);
                    // Regardless of whether or not the safe generation has been reset, the pending generation always increments so that
                    // even if a repository has been manually cleared of all contents we will never reuse the same repository generation.
                    // This is motivated by the consistency behavior the S3 based blob repository implementation has to support which does
                    // not offer any consistency guarantees when it comes to overwriting the same blob name with different content.
                    // pending 代表此时开始执行写入操作了 当写入完成时会将gen与 pendingGen同步
                    final long nextPendingGen = metadata.pendingGeneration() + 1;
                    newGen = uninitializedMeta ? Math.max(expectedGen + 1, nextPendingGen) : nextPendingGen;
                    assert newGen > latestKnownRepoGen.get() : "Attempted new generation [" + newGen +
                        "] must be larger than latest known generation [" + latestKnownRepoGen.get() + "]";

                    // 更新clusterState的 gen后 发布到集群中
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
        // 当上面的发布任务完成后会触发监听器
        setPendingStep.whenComplete(newGen -> threadPool().executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(listener, l -> {
            // BwC logic: Load snapshot version information if any snapshot is missing a version in RepositoryData so that the new
            // RepositoryData contains a version for every snapshot
            // 找到此时还没有版本号的 快照id   TODO 什么场景下会出现这种情况
            final List<SnapshotId> snapshotIdsWithoutVersion = repositoryData.getSnapshotIds().stream().filter(
                snapshotId -> repositoryData.getVersion(snapshotId) == null).collect(Collectors.toList());
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
                        // 当执行完监听器的 onRes/onFail 后会触发该方法
                        () -> filterRepositoryDataStep.onResponse(repositoryData.withVersions(updatedVersionMap))),
                    snapshotIdsWithoutVersion.size());
                // 找到每个快照并将版本号 设置进去 当设置完成时触发监听器
                for (SnapshotId snapshotId : snapshotIdsWithoutVersion) {
                    threadPool().executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.run(loadAllVersionsListener,
                        // 当执行完这串逻辑后会触发回调
                        // 每个快照以及对应快照信息的版本号被填充到map 中
                        () -> updatedVersionMap.put(snapshotId, getSnapshotInfo(snapshotId).version())));
                }
            } else {
                // 如果所有快照都已经包含了版本号 直接触发监听器
                filterRepositoryDataStep.onResponse(repositoryData);
            }
        })), listener::onFailure);

        // 再将所有快照的版本号都填充完毕后  结果被写入到监听器中 进而触发该方法
        filterRepositoryDataStep.whenComplete(filteredRepositoryData -> {
            // 获取此时最新的gen
            final long newGen = setPendingStep.result();
            if (latestKnownRepoGen.get() >= newGen) {
                throw new IllegalArgumentException(
                    "Tried writing generation [" + newGen + "] but repository is at least at generation [" + latestKnownRepoGen.get()
                        + "] already");
            }
            // write the index file
            // 生成index-n 文件
            final String indexBlob = INDEX_FILE_PREFIX + Long.toString(newGen);
            logger.debug("Repository [{}] writing new index generational blob [{}]", metadata.name(), indexBlob);

            // 将此时快照数据序列化后写入到文件中
            writeAtomic(indexBlob, BytesReference.bytes(filteredRepositoryData.snapshotsToXContent(XContentFactory.jsonBuilder(), writeShardGens)), true);
            // write the current generation to the index-latest file
            final BytesReference genBytes;
            try (BytesStreamOutput bStream = new BytesStreamOutput()) {
                // writeLong 实际上在底层会拆解成多个byte写入
                bStream.writeLong(newGen);
                genBytes = bStream.bytes();
            }
            logger.debug("Repository [{}] updating index.latest with generation [{}]", metadata.name(), newGen);

            // 就一个gen 也单独写入到文件中么  并且进行覆盖   那么直接读取这个文件就可以知道此时最新的 gen了
            writeAtomic(INDEX_LATEST_BLOB, genBytes, false);

            // Step 3: Update CS to reflect new repository generation.
            // 将最新的gen设置到clusterState中 并通过集群服务发布到其他节点   上面是更新pendingGen 这里是更新gen
            clusterService.submitStateUpdateTask("set safe repository generation [" + metadata.name() + "][" + newGen + "]",
                new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        final RepositoryMetadata meta = getRepoMetadata(currentState);
                        if (meta.generation() != expectedGen) {
                            throw new IllegalStateException("Tried to update repo generation to [" + newGen
                                + "] but saw unexpected generation in state [" + meta + "]");
                        }
                        if (meta.pendingGeneration() != newGen) {
                            throw new IllegalStateException(
                                "Tried to update from unexpected pending repo generation [" + meta.pendingGeneration() +
                                    "] after write to generation [" + newGen + "]");
                        }
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
                     * 处理成功时触发该方法
                     * @param source
                     * @param oldState
                     * @param newState
                     */
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        // 将gen 同步到 repositoryData上
                        final RepositoryData writtenRepositoryData = filteredRepositoryData.withGenId(newGen);
                        // 将数据写入到缓存中
                        cacheRepositoryData(writtenRepositoryData);
                        // 删除前1000个blob index-n  TODO 什么时候能删除 当最新的gen被发布到集群时 ， 这意味着什么 之前的数据如果还有对象在访问会怎么样
                        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.supply(listener, () -> {
                            // Delete all now outdated index files up to 1000 blobs back from the new generation.
                            // If there are more than 1000 dangling index-N cleanup functionality on repo delete will take care of them.
                            // Deleting one older than the current expectedGen is done for BwC reasons as older versions used to keep
                            // two index-N blobs around.
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
     * 从所有index-？ 文件下找到最大的gen
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

    @Override
    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                              IndexCommit snapshotIndexCommit, String shardStateIdentifier, IndexShardSnapshotStatus snapshotStatus,
                              Version repositoryMetaVersion, Map<String, Object> userMetadata, ActionListener<String> listener) {
        final ShardId shardId = store.shardId();
        final long startTime = threadPool.absoluteTimeInMillis();
        try {
            final String generation = snapshotStatus.generation();
            logger.debug("[{}] [{}] snapshot to [{}] [{}] ...", shardId, snapshotId, metadata.name(), generation);
            final BlobContainer shardContainer = shardContainer(indexId, shardId);
            final Set<String> blobs;
            if (generation == null) {
                try {
                    blobs = shardContainer.listBlobsByPrefix(INDEX_FILE_PREFIX).keySet();
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "failed to list blobs", e);
                }
            } else {
                blobs = Collections.singleton(INDEX_FILE_PREFIX + generation);
            }

            Tuple<BlobStoreIndexShardSnapshots, String> tuple = buildBlobStoreIndexShardSnapshots(blobs, shardContainer, generation);
            BlobStoreIndexShardSnapshots snapshots = tuple.v1();
            String fileListGeneration = tuple.v2();

            if (snapshots.snapshots().stream().anyMatch(sf -> sf.snapshot().equals(snapshotId.getName()))) {
                throw new IndexShardSnapshotFailedException(shardId,
                    "Duplicate snapshot name [" + snapshotId.getName() + "] detected, aborting");
            }
            // First inspect all known SegmentInfos instances to see if we already have an equivalent commit in the repository
            final List<BlobStoreIndexShardSnapshot.FileInfo> filesFromSegmentInfos = Optional.ofNullable(shardStateIdentifier).map(id -> {
                for (SnapshotFiles snapshotFileSet : snapshots.snapshots()) {
                    if (id.equals(snapshotFileSet.shardStateIdentifier())) {
                        return snapshotFileSet.indexFiles();
                    }
                }
                return null;
            }).orElse(null);

            final List<BlobStoreIndexShardSnapshot.FileInfo> indexCommitPointFiles;
            int indexIncrementalFileCount = 0;
            int indexTotalNumberOfFiles = 0;
            long indexIncrementalSize = 0;
            long indexTotalFileSize = 0;
            final BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> filesToSnapshot = new LinkedBlockingQueue<>();
            // If we did not find a set of files that is equal to the current commit we determine the files to upload by comparing files
            // in the commit with files already in the repository
            if (filesFromSegmentInfos == null) {
                indexCommitPointFiles = new ArrayList<>();
                store.incRef();
                final Collection<String> fileNames;
                final Store.MetadataSnapshot metadataFromStore;
                try {
                    // TODO apparently we don't use the MetadataSnapshot#.recoveryDiff(...) here but we should
                    try {
                        logger.trace(
                            "[{}] [{}] Loading store metadata using index commit [{}]", shardId, snapshotId, snapshotIndexCommit);
                        metadataFromStore = store.getMetadata(snapshotIndexCommit);
                        fileNames = snapshotIndexCommit.getFileNames();
                    } catch (IOException e) {
                        throw new IndexShardSnapshotFailedException(shardId, "Failed to get store file metadata", e);
                    }
                } finally {
                    store.decRef();
                }
                for (String fileName : fileNames) {
                    if (snapshotStatus.isAborted()) {
                        logger.debug("[{}] [{}] Aborted on the file [{}], exiting", shardId, snapshotId, fileName);
                        throw new IndexShardSnapshotFailedException(shardId, "Aborted");
                    }

                    logger.trace("[{}] [{}] Processing [{}]", shardId, snapshotId, fileName);
                    final StoreFileMetadata md = metadataFromStore.get(fileName);
                    BlobStoreIndexShardSnapshot.FileInfo existingFileInfo = null;
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
                    final boolean needsWrite = md.hashEqualsContents() == false;
                    indexTotalFileSize += md.length();
                    indexTotalNumberOfFiles++;

                    if (existingFileInfo == null) {
                        indexIncrementalFileCount++;
                        indexIncrementalSize += md.length();
                        // create a new FileInfo
                        BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo =
                            new BlobStoreIndexShardSnapshot.FileInfo(
                                (needsWrite ? UPLOADED_DATA_BLOB_PREFIX : VIRTUAL_DATA_BLOB_PREFIX) + UUIDs.randomBase64UUID(),
                                md, chunkSize());
                        indexCommitPointFiles.add(snapshotFileInfo);
                        if (needsWrite) {
                            filesToSnapshot.add(snapshotFileInfo);
                        }
                        assert needsWrite || assertFileContentsMatchHash(snapshotFileInfo, store);
                    } else {
                        indexCommitPointFiles.add(existingFileInfo);
                    }
                }
            } else {
                indexCommitPointFiles = filesFromSegmentInfos;
            }

            snapshotStatus.moveToStarted(startTime, indexIncrementalFileCount,
                indexTotalNumberOfFiles, indexIncrementalSize, indexTotalFileSize);

            final StepListener<Collection<Void>> allFilesUploadedListener = new StepListener<>();
            allFilesUploadedListener.whenComplete(v -> {
                final IndexShardSnapshotStatus.Copy lastSnapshotStatus =
                    snapshotStatus.moveToFinalize(snapshotIndexCommit.getGeneration());

                // now create and write the commit point
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
                    indexShardSnapshotFormat.write(snapshot, shardContainer, snapshotId.getUUID(), false);
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "Failed to write commit point", e);
                }
                // build a new BlobStoreIndexShardSnapshot, that includes this one and all the saved ones
                List<SnapshotFiles> newSnapshotsList = new ArrayList<>();
                newSnapshotsList.add(new SnapshotFiles(snapshot.snapshot(), snapshot.indexFiles(), shardStateIdentifier));
                for (SnapshotFiles point : snapshots) {
                    newSnapshotsList.add(point);
                }
                final List<String> blobsToDelete;
                final String indexGeneration;
                final boolean writeShardGens = SnapshotsService.useShardGenerations(repositoryMetaVersion);
                if (writeShardGens) {
                    indexGeneration = UUIDs.randomBase64UUID();
                    blobsToDelete = Collections.emptyList();
                } else {
                    indexGeneration = Long.toString(Long.parseLong(fileListGeneration) + 1);
                    // Delete all previous index-N blobs
                    blobsToDelete = blobs.stream().filter(blob -> blob.startsWith(SNAPSHOT_INDEX_PREFIX)).collect(Collectors.toList());
                    assert blobsToDelete.stream().mapToLong(b -> Long.parseLong(b.replaceFirst(SNAPSHOT_INDEX_PREFIX, "")))
                        .max().orElse(-1L) < Long.parseLong(indexGeneration)
                        : "Tried to delete an index-N blob newer than the current generation [" + indexGeneration
                        + "] when deleting index-N blobs " + blobsToDelete;
                }
                try {
                    writeShardIndexBlob(shardContainer, indexGeneration, new BlobStoreIndexShardSnapshots(newSnapshotsList));
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId,
                        "Failed to finalize snapshot creation [" + snapshotId + "] with shard index ["
                            + indexShardSnapshotsFormat.blobName(indexGeneration) + "]", e);
                }
                if (writeShardGens == false) {
                    try {
                        shardContainer.deleteBlobsIgnoringIfNotExists(blobsToDelete);
                    } catch (IOException e) {
                        logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to delete old index-N blobs during finalization",
                            snapshotId, shardId), e);
                    }
                }
                snapshotStatus.moveToDone(threadPool.absoluteTimeInMillis(), indexGeneration);
                listener.onResponse(indexGeneration);
            }, listener::onFailure);
            if (indexIncrementalFileCount == 0) {
                allFilesUploadedListener.onResponse(Collections.emptyList());
                return;
            }
            final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
            // Start as many workers as fit into the snapshot pool at once at the most
            final int workers = Math.min(threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(), indexIncrementalFileCount);
            final ActionListener<Void> filesListener = fileQueueListener(filesToSnapshot, workers, allFilesUploadedListener);
            for (int i = 0; i < workers; ++i) {
                executor.execute(ActionRunnable.run(filesListener, () -> {
                    BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo = filesToSnapshot.poll(0L, TimeUnit.MILLISECONDS);
                    if (snapshotFileInfo != null) {
                        store.incRef();
                        try {
                            do {
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

    @Override
    public void restoreShard(Store store, SnapshotId snapshotId, IndexId indexId, ShardId snapshotShardId,
                             RecoveryState recoveryState, ActionListener<Void> listener) {
        final ShardId shardId = store.shardId();
        final ActionListener<Void> restoreListener = ActionListener.delegateResponse(listener,
            (l, e) -> l.onFailure(new IndexShardRestoreFailedException(shardId, "failed to restore snapshot [" + snapshotId + "]", e)));
        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        final BlobContainer container = shardContainer(indexId, snapshotShardId);
        executor.execute(ActionRunnable.wrap(restoreListener, l -> {
            final BlobStoreIndexShardSnapshot snapshot = loadShardSnapshot(container, snapshotId);
            final SnapshotFiles snapshotFiles = new SnapshotFiles(snapshot.snapshot(), snapshot.indexFiles(), null);
            new FileRestoreContext(metadata.name(), shardId, snapshotId, recoveryState) {
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

                private void restoreFile(BlobStoreIndexShardSnapshot.FileInfo fileInfo, Store store) throws IOException {
                    boolean success = false;
                    try (IndexOutput indexOutput =
                             store.createVerifyingOutput(fileInfo.physicalName(), fileInfo.metadata(), IOContext.DEFAULT)) {
                        if (fileInfo.name().startsWith(VIRTUAL_DATA_BLOB_PREFIX)) {
                            final BytesRef hash = fileInfo.metadata().hash();
                            indexOutput.writeBytes(hash.bytes, hash.offset, hash.length);
                            recoveryState.getIndex().addRecoveredBytesToFile(fileInfo.physicalName(), hash.length);
                        } else {
                            try (InputStream stream = maybeRateLimit(new SlicedInputStream(fileInfo.numberOfParts()) {
                                @Override
                                protected InputStream openSlice(long slice) throws IOException {
                                    return container.readBlob(fileInfo.partName(slice));
                                }
                            }, restoreRateLimiter, restoreRateLimitingTimeInNanos)) {
                                final byte[] buffer = new byte[BUFFER_SIZE];
                                int length;
                                while ((length = stream.read(buffer)) > 0) {
                                    indexOutput.writeBytes(buffer, 0, length);
                                    recoveryState.getIndex().addRecoveredBytesToFile(fileInfo.physicalName(), length);
                                }
                            }
                        }
                        Store.verify(indexOutput);
                        indexOutput.close();
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
     * @param snapshotShardId 需要保存的快照
     * @param blobs 本次删除操作所有可选择的blobs (实际要删除哪些需要看传入的参数)
     * @param snapshotIds 本次要删除的所有快照
     * @param indexGeneration 最新的快照文件对应的gen
     * 删除快照数据
     * 通过indexId 和 shardId 可以定位到一组快照 在删除过程中避免删除非目标数据
     */
    private ShardSnapshotMetaDeleteResult deleteFromShardSnapshotMeta(Set<SnapshotId> survivingSnapshots, IndexId indexId,
                                                                      int snapshotShardId, Collection<SnapshotId> snapshotIds,
                                                                      BlobContainer shardContainer, Set<String> blobs,
                                                                      BlobStoreIndexShardSnapshots snapshots,
                                                                      String indexGeneration) {
        // Build a list of snapshots that should be preserved
        // 存储会被保留的快照
        List<SnapshotFiles> newSnapshotsList = new ArrayList<>();
        // 将这组快照id 转换成快照名
        final Set<String> survivingSnapshotNames = survivingSnapshots.stream().map(SnapshotId::getName).collect(Collectors.toSet());
        // 遍历所有快照信息
        for (SnapshotFiles point : snapshots) {
            // 名字匹配的快照文件都设置到容器中  这组是需要保存的
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
                // 将最新的快照文件 以gen生成文件夹名称 并存储到container下
                writeShardIndexBlob(shardContainer, indexGeneration, updatedSnapshots);
                final Set<String> survivingSnapshotUUIDs = survivingSnapshots.stream().map(SnapshotId::getUUID).collect(Collectors.toSet());
                // 可以看到没有做实际的删除操作  可能是考虑到其他线程可能还在访问旧文件 或者其他原因
                // 但是通过观测deleteResult 可以知道有哪些快照是当前存活的 且gen是多少
                return new ShardSnapshotMetaDeleteResult(indexId, snapshotShardId, indexGeneration,
                    // TODO 现在还不能理解是如何选出这组 blob的
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

    // Unused blobs are all previous index-, data- and meta-blobs and that are not referenced by the new index- as well as all
    // temporary blobs
    // 找到此时未使用的所有blob
    private static List<String> unusedBlobs(Set<String> blobs, Set<String> survivingSnapshotUUIDs,
                                            BlobStoreIndexShardSnapshots updatedSnapshots) {
        return blobs.stream().filter(blob ->
            // 如果是 index- 文件
            blob.startsWith(SNAPSHOT_INDEX_PREFIX)
            // 或者是 snap- 文件 同时不存在于 surviving中
            || (blob.startsWith(SNAPSHOT_PREFIX) && blob.endsWith(".dat") && survivingSnapshotUUIDs.contains(blob.substring(SNAPSHOT_PREFIX.length(), blob.length() - ".dat".length())) == false)
            // TODO 啥意思 ???
            || (blob.startsWith(UPLOADED_DATA_BLOB_PREFIX) && updatedSnapshots.findNameFile(canonicalName(blob)) == null)
            // TODO
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
     * @param blobs      list of blobs in repository  某个分片下所有数据体对应的name
     * @param shardContainer   存储该分片的容器 在fs中就是文件夹
     * @param generation shard generation or {@code null} in case there was no shard generation tracked in the {@link RepositoryData} for
     *                   this shard because its snapshot was created in a version older than
     *                   {@link SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION}.    该分片对应的gen
     * @return tuple of BlobStoreIndexShardSnapshots and the last snapshot index generation
     * 调用该方法的前提是为分片设置了 gen  也就是7.6后的版本
     */
    private Tuple<BlobStoreIndexShardSnapshots, String> buildBlobStoreIndexShardSnapshots(Set<String> blobs,
                                                                                          BlobContainer shardContainer,
                                                                                          @Nullable String generation) throws IOException {
        if (generation != null) {
            // 应该是代表还没有写入任何数据
            if (generation.equals(ShardGenerations.NEW_SHARD_GEN)) {
                return new Tuple<>(BlobStoreIndexShardSnapshots.EMPTY, ShardGenerations.NEW_SHARD_GEN);
            }
            return new Tuple<>(indexShardSnapshotsFormat.read(shardContainer, generation), generation);
        }
        // 忽略兼容逻辑
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
     */
    private void snapshotFile(BlobStoreIndexShardSnapshot.FileInfo fileInfo, IndexId indexId, ShardId shardId, SnapshotId snapshotId,
                              IndexShardSnapshotStatus snapshotStatus, Store store) throws IOException {
        final BlobContainer shardContainer = shardContainer(indexId, shardId);
        final String file = fileInfo.physicalName();
        try (IndexInput indexInput = store.openVerifyingInput(file, IOContext.READONCE, fileInfo.metadata())) {
            for (int i = 0; i < fileInfo.numberOfParts(); i++) {
                final long partBytes = fileInfo.partBytes(i);

                // Make reads abortable by mutating the snapshotStatus object
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
                shardContainer.writeBlob(fileInfo.partName(i), inputStream, partBytes, true);
            }
            Store.verify(indexInput);
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
     * 某次操作中可能会删除一组快照  这里是针对最细粒度进行删除
     */
    private static final class ShardSnapshotMetaDeleteResult {

        // Index that the snapshot was removed from  针对的索引id
        private final IndexId indexId;

        // Shard id that the snapshot was removed from  针对哪个分片  一个index下会产生n个分片
        private final int shardId;

        // Id of the new index-${uuid} blob that does not include the snapshot any more
        private final String newGeneration;

        // Blob names in the shard directory that have become unreferenced in the new shard generation
        // 哪些数据块会被删除
        private final Collection<String> blobsToDelete;

        ShardSnapshotMetaDeleteResult(IndexId indexId, int shardId, String newGeneration, Collection<String> blobsToDelete) {
            this.indexId = indexId;
            this.shardId = shardId;
            this.newGeneration = newGeneration;
            this.blobsToDelete = blobsToDelete;
        }
    }
}
