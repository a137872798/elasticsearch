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

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader.FSTLoadMode;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.ShuffleForcedMergePolicy;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.Assertions;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.LoggerInfoStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.fieldvisitor.IdOnlyFieldVisitor;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ElasticsearchMergePolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 内部引擎对象   默认生成的就是该对象
 */
public class InternalEngine extends Engine {

    /**
     * When we last pruned expired tombstones from versionMap.deletes:
     * 代表最近一次从墓碑数据中移除某些version信息的时间戳
     * 该对象在初始化时 会通过当前时间设置该值
     */
    private volatile long lastDeleteVersionPruneTimeMSec;

    /**
     * 这个对象是负责写入事务日志的
     */
    private final Translog translog;

    /**
     * 与 lucene的ConcurrentMergeScheduler 最大的区别就是 开放了2个merge相关的钩子 以及 不使用lucene本身的 针对merge的限流机制
     */
    private final ElasticsearchConcurrentMergeScheduler mergeScheduler;

    /**
     * 通过该对象实现写入功能
     */
    private final IndexWriter indexWriter;

    /**
     * 这里在检测刷新时 强制调用了internalReaderManager的block方法 确保一定会获取到最新数据
     */
    private final ExternalReaderManager externalReaderManager;
    /**
     * 从命名上可以看出 2个reader管理器 分别对应外部请求和内部请求
     */
    private final ElasticsearchReaderManager internalReaderManager;

    private final Lock flushLock = new ReentrantLock();
    private final ReentrantLock optimizeLock = new ReentrantLock();

    // A uid (in the form of BytesRef) to the version map
    // we use the hashed variant since we iterate over it and check removal and additions on existing keys
    // 该对象可以通过byte 查询版本号信息    byte[] 实际上就是某个id
    private final LiveVersionMap versionMap = new LiveVersionMap();

    /**
     * 存储了当前所有的段信息
     */
    private volatile SegmentInfos lastCommittedSegmentInfos;

    /**
     * 可开关的阀门对象  在激活后会设置内部的锁 通过调用api可以抢占锁
     */
    private final IndexThrottle throttle;

    /**
     * 记录已经处理过的 seqNo 以及 persisted
     */
    private final LocalCheckpointTracker localCheckpointTracker;

    /**
     * 结合的一个删除策略
     */
    private final CombinedDeletionPolicy combinedDeletionPolicy;

    // How many callers are currently requesting index throttling.  Currently there are only two situations where we do this: when merges
    // are falling behind and when writing indexing buffer to disk is too slow.  When this is 0, there is no throttling, else we throttling
    // incoming indexing ops to a single thread:
    private final AtomicInteger throttleRequestCount = new AtomicInteger();

    /**
     * 在初始化时 会将该标识修改成true  代表此时还未从事务文件中恢复数据
     * 在这个阶段应该是拒绝处理其他请求的   TODO 那么是怎么做处理的
     */
    private final AtomicBoolean pendingTranslogRecovery = new AtomicBoolean(false);
    private final AtomicLong maxUnsafeAutoIdTimestamp = new AtomicLong(-1);

    /**
     * 此时所观测到的最大的时间戳
     */
    private final AtomicLong maxSeenAutoIdTimestamp = new AtomicLong(-1);
    // max_seq_no_of_updates_or_deletes tracks the max seq_no of update or delete operations that have been processed in this engine.
    // An index request is considered as an update if it overwrites existing documents with the same docId in the Lucene index.
    // The value of this marker never goes backwards, and is tracked/updated differently on primary and replica.
    private final AtomicLong maxSeqNoOfUpdatesOrDeletes;
    private final CounterMetric numVersionLookups = new CounterMetric();
    private final CounterMetric numIndexVersionsLookups = new CounterMetric();
    // Lucene operations since this engine was opened - not include operations from existing segments.
    private final CounterMetric numDocDeletes = new CounterMetric();
    private final CounterMetric numDocAppends = new CounterMetric();
    private final CounterMetric numDocUpdates = new CounterMetric();
    /**
     * 获取 _soft_delete 对应的docValue  这个值被保存到lucene中了
     */
    private final NumericDocValuesField softDeletesField = Lucene.newSoftDeletesField();
    private final SoftDeletesPolicy softDeletesPolicy;

    /**
     * 监听器刷新事件  配合 LocalCheckpointTracker使用
     */
    private final LastRefreshedCheckpointListener lastRefreshedCheckpointListener;

    private final CompletionStatsCache completionStatsCache;

    /**
     * 开启事务日志最新写入位置的追踪
     */
    private final AtomicBoolean trackTranslogLocation = new AtomicBoolean(false);
    private final KeyedLock<Long> noOpKeyedLock = new KeyedLock<>();

    /**
     * 代表此前执行了一次大规模的merge操作  某些地方会检测该标识 并执行刷盘
     */
    private final AtomicBoolean shouldPeriodicallyFlushAfterBigMerge = new AtomicBoolean(false);

    @Nullable
    private final String historyUUID;

    /**
     * UUID value that is updated every time the engine is force merged.
     */
    @Nullable
    private volatile String forceMergeUUID;

    /**
     * 通过引擎的相关配置进行初始化
     * @param engineConfig
     */
    public InternalEngine(EngineConfig engineConfig) {
        this(engineConfig, LocalCheckpointTracker::new);
    }

    /**
     *
     * @param engineConfig  配置对象
     * @param localCheckpointTrackerSupplier  会记录检查点的对象
     */
    InternalEngine(
            final EngineConfig engineConfig,
            final BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) {
        super(engineConfig);
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy();
        // 该引擎此时引用了这个目录 避免被意外关闭
        store.incRef();
        IndexWriter writer = null;
        Translog translog = null;
        ExternalReaderManager externalReaderManager = null;
        ElasticsearchReaderManager internalReaderManager = null;
        EngineMergeScheduler scheduler = null;
        boolean success = false;
        try {
            this.lastDeleteVersionPruneTimeMSec = engineConfig.getThreadPool().relativeTimeInMillis();

            // 拓展了 lucene内置的并发merge对象
            mergeScheduler = scheduler = new EngineMergeScheduler(engineConfig.getShardId(), engineConfig.getIndexSettings());
            throttle = new IndexThrottle();
            try {
                // 某些commit信息可能没有同步到其他节点 需要丢弃这些数据  旧的commit数据还会保留
                store.trimUnsafeCommits(config().getTranslogConfig().getTranslogPath());
                // engineConfig.getGlobalCheckpointSupplier() 实际上就是借助replicationTracker 获取最新的全局检查点
                translog = openTranslog(engineConfig, translogDeletionPolicy, engineConfig.getGlobalCheckpointSupplier(),
                    // 每当在事务日志文件中有某个operation的数据刷盘成功时就会触发该方法
                    seqNo -> {
                        final LocalCheckpointTracker tracker = getLocalCheckpointTracker();
                        assert tracker != null || getTranslog().isOpen() == false;
                        if (tracker != null) {
                            tracker.markSeqNoAsPersisted(seqNo);
                        }
                    });
                assert translog.getGeneration() != null;
                this.translog = translog;
                // 初始化软删除策略  ES 有一个_soft_delete field 被设置成软删除字段
                this.softDeletesPolicy = newSoftDeletesPolicy();
                // 这里也只是做一些赋值操作
                this.combinedDeletionPolicy =
                    new CombinedDeletionPolicy(logger, translogDeletionPolicy, softDeletesPolicy, translog::getLastSyncedGlobalCheckpoint);

                // 创建本地检查点追踪对象
                this.localCheckpointTracker = createLocalCheckpointTracker(localCheckpointTrackerSupplier);

                // 生成 IndexWriter 用于写入数据 其中mergePolicy 包装了好多层
                writer = createWriter();
                // 在indexWriter初始化时 会加载dir下最新的segment_N文件  这里是从userData中获取一个auto时间戳信息
                bootstrapAppendOnlyInfoFromWriter(writer);

                // 获取segments.userData
                final Map<String, String> commitData = commitDataAsMap(writer);
                // 获取userData中的historyUUID
                historyUUID = loadHistoryUUID(commitData);
                // 获取 force_merge_uuid属性
                forceMergeUUID = commitData.get(FORCE_MERGE_UUID_KEY);
                indexWriter = writer;
            } catch (IOException | TranslogCorruptedException e) {
                throw new EngineCreationFailureException(shardId, "failed to create engine", e);
            } catch (AssertionError e) {
                // IndexWriter throws AssertionError on init, if asserts are enabled, if any files don't exist, but tests that
                // randomly throw FNFE/NSFE can also hit this:
                if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                    throw new EngineCreationFailureException(shardId, "failed to create engine", e);
                } else {
                    throw e;
                }
            }
            // 创建资源管理器 在合适的时机会检测能否刷新 并返回最新的数据   总共存在2个readerManager对象 一个为internal一个为external
            // RefreshWarmerListener 负责对数据进行预热    每当外部readerManager对象触发refresh时 就会针对最新的数据进行预热
            externalReaderManager = createReaderManager(new RefreshWarmerListener(logger, isClosed, engineConfig));
            internalReaderManager = externalReaderManager.internalReaderManager;
            this.internalReaderManager = internalReaderManager;
            this.externalReaderManager = externalReaderManager;
            // 这里把记录版本号的容器作为监听器设置到 internalReaderManager上了
            internalReaderManager.addListener(versionMap);
            assert pendingTranslogRecovery.get() == false : "translog recovery can't be pending before we set it";
            // don't allow commits until we are done with recovering
            // 标记此时还未从事务日志中恢复数据
            pendingTranslogRecovery.set(true);
            // 从配置中获取相应的监听器对象并进行设置  对应 RefreshListeners,RefreshPendingLocationListener
            for (ReferenceManager.RefreshListener listener: engineConfig.getExternalRefreshListener()) {
                this.externalReaderManager.addListener(listener);
            }
            // 对应 RefreshMetricUpdater
            for (ReferenceManager.RefreshListener listener: engineConfig.getInternalRefreshListener()) {
                this.internalReaderManager.addListener(listener);
            }

            // 生成一个会记录最后的刷新检查点的监听器对象 并设置到readerManager中
            // localCheckpointTracker内部的processedCheckpoint初始值是从segment_N.userData 的_local_check_point上获取的
            this.lastRefreshedCheckpointListener = new LastRefreshedCheckpointListener(localCheckpointTracker.getProcessedCheckpoint());
            this.internalReaderManager.addListener(lastRefreshedCheckpointListener);

            // 获取 userData中的 maxSeq 或者事务日志中的checkPoint.maxSeq
            maxSeqNoOfUpdatesOrDeletes = new AtomicLong(SequenceNumbers.max(localCheckpointTracker.getMaxSeqNo(), translog.getMaxSeqNo()));
            // 在初始阶段 localCheckpointTracker.getPersistedCheckpoint() 就是userData._local_check_point
            // TODO 什么时候会出现这种情况
            if (localCheckpointTracker.getPersistedCheckpoint() < localCheckpointTracker.getMaxSeqNo()) {
                // 通过 InternalReaderManager 生成searcher   在InternalReaderManager中包含一个IndexReader  searcher就是包装了IndexReader
                try (Searcher searcher =
                         acquireSearcher("restore_version_map_and_checkpoint_tracker", SearcherScope.INTERNAL)) {
                    // 从之前通过lucene存储的doc中还原出 version checkpoint信息
                    restoreVersionMapAndCheckpointTracker(Lucene.wrapAllDocsLive(searcher.getDirectoryReader()));
                } catch (IOException e) {
                    throw new EngineCreationFailureException(config().getShardId(),
                        "failed to restore version map and local checkpoint tracker", e);
                }
            }
            // 统计相关的先忽略
            completionStatsCache = new CompletionStatsCache(() -> acquireSearcher("completion_stats"));
            this.externalReaderManager.addListener(completionStatsCache);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(writer, translog, internalReaderManager, externalReaderManager, scheduler);
                if (isClosed.get() == false) {
                    // failure we need to dec the store reference
                    store.decRef();
                }
            }
        }
        logger.trace("created new InternalEngine");
    }

    /**
     * 创建追踪本地检查点信息的对象
     * @param localCheckpointTrackerSupplier
     * @return
     * @throws IOException
     */
    private LocalCheckpointTracker createLocalCheckpointTracker(
        BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) throws IOException {
        final long maxSeqNo;
        final long localCheckpoint;
        // 从 userData中解析 localCheckpoint/maxSeq
        final SequenceNumbers.CommitInfo seqNoStats =
            SequenceNumbers.loadSeqNoInfoFromLuceneCommit(store.readLastCommittedSegmentsInfo().userData.entrySet());
        maxSeqNo = seqNoStats.maxSeqNo;
        localCheckpoint = seqNoStats.localCheckpoint;
        logger.trace("recovered maximum sequence number [{}] and local checkpoint [{}]", maxSeqNo, localCheckpoint);
        // 对应LocalCheckpointTracker::new
        return localCheckpointTrackerSupplier.apply(maxSeqNo, localCheckpoint);
    }

    /**
     * 生成软删除策略对象
     * @return
     * @throws IOException
     */
    private SoftDeletesPolicy newSoftDeletesPolicy() throws IOException {
        final Map<String, String> commitUserData = store.readLastCommittedSegmentsInfo().userData;
        final long lastMinRetainedSeqNo;
        // 如果存在保留的最小seqNo  解析出来
        if (commitUserData.containsKey(Engine.MIN_RETAINED_SEQNO)) {
            lastMinRetainedSeqNo = Long.parseLong(commitUserData.get(Engine.MIN_RETAINED_SEQNO));
        } else {
            lastMinRetainedSeqNo = Long.parseLong(commitUserData.get(SequenceNumbers.MAX_SEQ_NO)) + 1;
        }
        return new SoftDeletesPolicy(
                translog::getLastSyncedGlobalCheckpoint,
                lastMinRetainedSeqNo,
                engineConfig.getIndexSettings().getSoftDeleteRetentionOperations(),
                // 续约信息也是从 ReplicationTracker中获取的
                engineConfig.retentionLeasesSupplier());
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return completionStatsCache.get(fieldNamePatterns);
    }

    /**
     * This reference manager delegates all it's refresh calls to another (internal) ReaderManager
     * The main purpose for this is that if we have external refreshes happening we don't issue extra
     * refreshes to clear version map memory etc. this can cause excessive segment creation if heavy indexing
     * is happening and the refresh interval is low (ie. 1 sec)
     *
     * This also prevents segment starvation where an internal reader holds on to old segments literally forever
     * since no indexing is happening and refreshes are only happening to the external reader manager, while with
     * this specialized implementation an external refresh will immediately be reflected on the internal reader
     * and old segments can be released in the same way previous version did this (as a side-effect of _refresh)
     * 资源管理器就是对某个资源进行管理 可以在合适的时机进行刷新
     */
    @SuppressForbidden(reason = "reference counting is required here")
    private static final class ExternalReaderManager extends ReferenceManager<ElasticsearchDirectoryReader> {
        private final BiConsumer<ElasticsearchDirectoryReader, ElasticsearchDirectoryReader> refreshListener;

        /**
         * 该资源管理器 会自动检测最新的segment_N  并更新reader对象
         */
        private final ElasticsearchReaderManager internalReaderManager;

        /**
         * 代表还没有执行过预热工作
         * 预热指的就是将数据先加载到内存中
         * 首次通过refresh 获取到reader对象后该标识就为true
         */
        private boolean isWarmedUp; //guarded by refreshLock

        /**
         * 类似与代理模式
         * @param internalReaderManager
         * @param refreshListener
         * @throws IOException
         */
        ExternalReaderManager(ElasticsearchReaderManager internalReaderManager,
                              BiConsumer<ElasticsearchDirectoryReader, ElasticsearchDirectoryReader> refreshListener) throws IOException {
            this.refreshListener = refreshListener;
            this.internalReaderManager = internalReaderManager;
            this.current = internalReaderManager.acquire(); // steal the reference without warming up
        }

        /**
         * externalReaderManager 与 internalReaderManager的区别就是 外部对象会维护缓存  比如此时由于lucene内存占用过大的场景 会触发强制刷盘(通过refresh) 但是不会刷新对外的缓存
         * @param referenceToRefresh
         * @return
         * @throws IOException
         */
        @Override
        protected ElasticsearchDirectoryReader refreshIfNeeded(ElasticsearchDirectoryReader referenceToRefresh) throws IOException {
            // we simply run a blocking refresh on the internal reference manager and then steal it's reader
            // it's a save operation since we acquire the reader which incs it's reference but then down the road
            // steal it by calling incRef on the "stolen" reader
            // 每次必须抢占到锁 并触发refresh方法 这样就可以确保读取到的是最新的reader了
            internalReaderManager.maybeRefreshBlocking();
            final ElasticsearchDirectoryReader newReader = internalReaderManager.acquire();
            // 代表数据还没有加载到内存 或者reader发生了变化 那么就需要重新加载数据
            if (isWarmedUp == false || newReader != referenceToRefresh) {
                boolean success = false;
                try {
                    // isWarmedUp == false 代表之前没有reader 所以传入null   这里会对数据进行预热 主要就是将数据读取到缓存中
                    refreshListener.accept(newReader, isWarmedUp ? referenceToRefresh : null);
                    isWarmedUp = true;
                    success = true;
                } finally {
                    if (success == false) {
                        internalReaderManager.release(newReader);
                    }
                }
            }
            // nothing has changed - both ref managers share the same instance so we can use reference equality
            if (referenceToRefresh == newReader) {
                internalReaderManager.release(newReader);
                return null;
            } else {
                return newReader; // steal the reference
            }
        }

        @Override
        protected boolean tryIncRef(ElasticsearchDirectoryReader reference) {
            return reference.tryIncRef();
        }

        @Override
        protected int getRefCount(ElasticsearchDirectoryReader reference) {
            return reference.getRefCount();
        }

        @Override
        protected void decRef(ElasticsearchDirectoryReader reference) throws IOException {
            reference.decRef();
        }
    }

    @Override
    final boolean assertSearcherIsWarmedUp(String source, SearcherScope scope) {
        if (scope == SearcherScope.EXTERNAL) {
            switch (source) {
                // we can access segment_stats while a shard is still in the recovering state.
                case "segments":
                case "segments_stats":
                    break;
                default:
                    assert externalReaderManager.isWarmedUp : "searcher was not warmed up yet for source[" + source + "]";
            }
        }
        return true;
    }

    /**
     * 从事务日志中恢复历史数据
     * @param translogRecoveryRunner  是一个函数对象  通过engint 和 Translog.Snapshot 进行数据还原
     * @return
     * @throws IOException
     */
    @Override
    public int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            // 获取到此时已经处理完毕的数据对应的检查点    推测是某个处理完毕的operation对应的seq
            final long localCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
            // 获取检查点之后的所有数据快照
            try (Translog.Snapshot snapshot = getTranslog().newSnapshot(localCheckpoint + 1, Long.MAX_VALUE)) {
                return translogRecoveryRunner.run(this, snapshot);
            }
        }
    }

    /**
     * 填满 seq 之间的缺口
     * @param primaryTerm the shards primary term this engine was created for   在leader节点上生成该shard路由信息的主分片任期 应该是每一次分配的结果对应一个term
     * @return
     * @throws IOException
     */
    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            ensureOpen();
            // 获取当前最新的检查点以及序列  每当将一个 operation写入到lucene时 就会增加maxSeq 以及processedCheckpoint
            final long localCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
            final long maxSeqNo = localCheckpointTracker.getMaxSeqNo();
            int numNoOpsAdded = 0;
            // 这里是将 检查点与最大序列号之间的空缺补上
            for (
                    long seqNo = localCheckpoint + 1;
                    seqNo <= maxSeqNo;
                    seqNo = localCheckpointTracker.getProcessedCheckpoint() + 1 /* leap-frog the local checkpoint */) {
                // 这里往lucene中写入 noop  注意这里发起操作的 origin是 PRIMARY
                innerNoOp(new NoOp(seqNo, primaryTerm, Operation.Origin.PRIMARY, System.nanoTime(), "filling gaps"));
                numNoOpsAdded++;
                assert seqNo <= localCheckpointTracker.getProcessedCheckpoint() :
                    "local checkpoint did not advance; was [" + seqNo + "], now [" + localCheckpointTracker.getProcessedCheckpoint() + "]";

            }
            // 因为本次操作的origin是primary 数据会写入到事务文件中 并且还未持久化 所以要先进行刷盘
            syncTranslog(); // to persist noops associated with the advancement of the local checkpoint
            assert localCheckpointTracker.getPersistedCheckpoint() == maxSeqNo
                : "persisted local checkpoint did not advance to max seq no; is [" + localCheckpointTracker.getPersistedCheckpoint() +
                "], max seq no [" + maxSeqNo + "]";
            return numNoOpsAdded;
        }
    }

    /**
     * 从最新的segment_N文件中获取userData信息
     * @param writer
     */
    private void bootstrapAppendOnlyInfoFromWriter(IndexWriter writer) {
        for (Map.Entry<String, String> entry : writer.getLiveCommitData()) {
            // 如果存在 max_unsafe_auto_id_timestamp 获取时间戳信息
            if (MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID.equals(entry.getKey())) {
                assert maxUnsafeAutoIdTimestamp.get() == -1 :
                    "max unsafe timestamp was assigned already [" + maxUnsafeAutoIdTimestamp.get() + "]";
                updateAutoIdTimestamp(Long.parseLong(entry.getValue()), true);
            }
        }
    }

    /**
     * 从事务日志中恢复数据
     * @param translogRecoveryRunner the translog recovery runner    这里定义了恢复数据的逻辑
     * @param recoverUpToSeqNo       the upper bound, inclusive, of sequence number to be recovered   最多加载到seq为多少的数据后 就不再恢复了
     * @return
     * @throws IOException
     */
    @Override
    public InternalEngine recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo) throws IOException {
        flushLock.lock();
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            // 在初始化时 该标识会被修改成true 当该标识被修改成false时 代表已经恢复了数据  就不需要重复恢复数据了
            if (pendingTranslogRecovery.get() == false) {
                throw new IllegalStateException("Engine has already been recovered");
            }
            try {
                recoverFromTranslogInternal(translogRecoveryRunner, recoverUpToSeqNo);
            } catch (Exception e) {
                try {
                    pendingTranslogRecovery.set(true); // just play safe and never allow commits on this see #ensureCanFlush
                    failEngine("failed to recover from translog", e);
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                }
                throw e;
            }
        } finally {
            flushLock.unlock();
        }
        return this;
    }

    /**
     * 开启引擎对象 但是不需要从事务日志中恢复数据
     */
    @Override
    public void skipTranslogRecovery() {
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        // 直接标记成已经完成恢复
        pendingTranslogRecovery.set(false); // we are good - now we can commit
    }

    /**
     * 从事务日志中恢复数据
     * @param translogRecoveryRunner  该对象定义了如何通过engine 和从translog中获取的快照对象进行数据恢复
     *                                返回的结果就是 总计恢复了多少operation
     * @param recoverUpToSeqNo   代表最多加载到seq为多少的记录  默认情况下是不做限制的  Long.MAX_VALUE
     * @throws IOException
     */
    private void recoverFromTranslogInternal(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo) throws IOException {

        // 记录总计恢复了多少operation
        final int opsRecovered;
        // 获取 localCheckpointTracker 记录的  processedCheckpoint
        // 这之前的数据 已经存在于lucene的索引文件中了 所以不需要恢复
        final long localCheckpoint = getProcessedLocalCheckpoint();

        // 当前检查点小于 seq上限才有恢复的必要
        if (localCheckpoint < recoverUpToSeqNo) {
            // 将中间这部分数据生成快照
            try (Translog.Snapshot snapshot = translog.newSnapshot(localCheckpoint + 1, recoverUpToSeqNo)) {
                opsRecovered = translogRecoveryRunner.run(this, snapshot);
            } catch (Exception e) {
                throw new EngineException(shardId, "failed to recover from translog", e);
            }
        } else {
            // 代表没有operaton需要恢复
            opsRecovered = 0;
        }
        // flush if we recovered something or if we have references to older translogs
        // note: if opsRecovered == 0 and we have older translogs it means they are corrupted or 0 length.
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        // 代表数据的恢复已完成
        pendingTranslogRecovery.set(false); // we are good - now we can commit

        // 代表有数据从事务日志重做到了 lucene中
        if (opsRecovered > 0) {
            logger.trace("flushing post recovery from translog: ops recovered [{}], current translog generation [{}]",
                opsRecovered, translog.currentFileGeneration());

            // 之前在恢复过程中 数据只是写入到内存中  在这里针对之前写入lucene的数据进行刷盘
            // translog 在这之前应该是已经完成持久化了
            commitIndexWriter(indexWriter, translog);
            // 因为上面执行了刷盘操作 这里就要刷新segmentInfos
            refreshLastCommittedSegmentInfos();
            // 这里主要是刷新 reader对象 并且会触发一组refreshListener
            refresh("translog_recovery");
        }

        // 在refresh中 存储在内存中的结构化数据 已经刷盘到lucene的索引文件中了 之前seq对应的事务日志也就可以删除了
        translog.trimUnreferencedReaders();
    }

    /**
     * 创建事务日志对象 每当执行一个操作就进行记录
     * @param engineConfig
     * @param translogDeletionPolicy
     * @param globalCheckpointSupplier
     * @param persistedSequenceNumberConsumer  通过localCheckpointTracker 纪录此时持久化的seqNo
     * @return
     * @throws IOException
     */
    private Translog openTranslog(EngineConfig engineConfig, TranslogDeletionPolicy translogDeletionPolicy,
                                  LongSupplier globalCheckpointSupplier, LongConsumer persistedSequenceNumberConsumer) throws IOException {

        // 在初始化 indexShard时 就已经生成了 translogConfig对象了
        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        // 用户信息中有携带 translogUUID
        final Map<String, String> userData = store.readLastCommittedSegmentsInfo().getUserData();
        final String translogUUID = Objects.requireNonNull(userData.get(Translog.TRANSLOG_UUID_KEY));
        // We expect that this shard already exists, so it must already have an existing translog else something is badly wrong!
        // 在创建过程中会根据此时存在的所有事务文件生成reader对象 以及生成一个向最新事务日志文件(gen+1)写入数据的writer对象
        return new Translog(translogConfig, translogUUID, translogDeletionPolicy, globalCheckpointSupplier,
            engineConfig.getPrimaryTermSupplier(), persistedSequenceNumberConsumer);
    }

    // Package private for testing purposes only
    Translog getTranslog() {
        ensureOpen();
        return translog;
    }

    // Package private for testing purposes only
    // TODO 什么时候会将 commit数据填充到 snapshottedCommit中???
    boolean hasSnapshottedCommits() {
        return combinedDeletionPolicy.hasSnapshottedCommits();
    }

    /**
     * 只要 translogWriter内部偏移量发生了变化 就代表需要刷盘 而每次记录operation时 就会增加偏移量
     * @return
     */
    @Override
    public boolean isTranslogSyncNeeded() {
        return getTranslog().syncNeeded();
    }

    /**
     * 传入一组位置信息 并检测对应的operation是否已经刷盘成功了
     * @param locations
     * @return
     * @throws IOException
     */
    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
        // 返回true代表触发了刷盘  false代表location对应的operation已经刷盘成功了 不需要处理
        final boolean synced = translog.ensureSynced(locations);
        if (synced) {
            // 每次刷盘成功都要检测是否有可以删除的文件
            revisitIndexDeletionPolicyOnTranslogSynced();
        }
        return synced;
    }

    /**
     * 将事务文件刷盘
     * @throws IOException
     */
    @Override
    public void syncTranslog() throws IOException {
        translog.sync();
        // 每当刷盘完成时 都检测一下是否有不再被使用的 segment了 有的话进行移除
        revisitIndexDeletionPolicyOnTranslogSynced();
    }

    @Override
    public TranslogStats getTranslogStats() {
        return getTranslog().stats();
    }

    /**
     * 获取此时事务日志最后一条记录的位置信息
     */
    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return getTranslog().getLastWriteLocation();
    }

    /**
     * 在事务文件刷盘后检测是否有需要删除的文件
     * @throws IOException
     */
    private void revisitIndexDeletionPolicyOnTranslogSynced() throws IOException {
        if (combinedDeletionPolicy.hasUnreferencedCommits()) {
            // 触发删除策略的 onCommit 方法后  会有一部分文件被设置到删除队列中 之后通过 IndexFileDeleter删除文件
            // ES 使用的删除策略是  CombinedDeletionPolicy
            // 根据globalCheckpoint 选择不再维护的commit 并删除相关文件
            indexWriter.deleteUnusedFiles();
        }
        // 事务文件也包含了 checkpoint的信息 将小于 safeCommit的数据文件删除掉
        translog.trimUnreferencedReaders();
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    /** returns the force merge uuid for the engine */
    @Nullable
    public String getForceMergeUUID() {
        return forceMergeUUID;
    }

    /** Returns how many bytes we are currently moving from indexing buffer to segments on disk */
    @Override
    public long getWritingBytes() {
        return indexWriter.getFlushingBytes() + versionMap.getRefreshingBytes();
    }

    /**
     * Reads the current stored history ID from the IW commit data.
     */
    private String loadHistoryUUID(Map<String, String> commitData) {
        final String uuid = commitData.get(HISTORY_UUID_KEY);
        if (uuid == null) {
            throw new IllegalStateException("commit doesn't contain history uuid");
        }
        return uuid;
    }

    /**
     * 生成一个资源管理对象
     * @param externalRefreshListener 该对象一旦感知到reader对象就会进行预热工作
     * @return
     * @throws EngineException
     */
    private ExternalReaderManager createReaderManager(RefreshWarmerListener externalRefreshListener) throws EngineException {
        boolean success = false;
        ElasticsearchReaderManager internalReaderManager = null;
        try {
            try {
                // 该reader内部包含多个子reader 每个reader对应一个segment
                final ElasticsearchDirectoryReader directoryReader =
                    ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter), shardId);

                // listener 监听到新的reader时 会检测缓存键 并与熔断器产生联动
                internalReaderManager = new ElasticsearchReaderManager(directoryReader,
                       new RamAccountingRefreshListener(engineConfig.getCircuitBreakerService()));

                // 获取segment_N数据信息
                lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                // 内部对象和 外部对象维护的监听器不同
                ExternalReaderManager externalReaderManager = new ExternalReaderManager(internalReaderManager, externalRefreshListener);
                success = true;
                return externalReaderManager;
            } catch (IOException e) {
                maybeFailEngine("start", e);
                try {
                    indexWriter.rollback();
                } catch (IOException inner) { // iw is closed below
                    e.addSuppressed(inner);
                }
                throw new EngineCreationFailureException(shardId, "failed to open reader on writer", e);
            }
        } finally {
            if (success == false) { // release everything we created on a failure
                IOUtils.closeWhileHandlingException(internalReaderManager, indexWriter);
            }
        }
    }

    /**
     * 执行get请求 并返回查询结果
     * @param get  在get请求中还可以严格要求版本
     * @param searcherFactory   该对象可以根据要求 生成searcher
     * @return
     * @throws EngineException
     */
    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Engine.Searcher> searcherFactory) throws EngineException {
        assert Objects.equals(get.uid().field(), IdFieldMapper.NAME) : get.uid().field();
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            SearcherScope scope;
            if (get.realtime()) {
                VersionValue versionValue = null;
                // 可以看到先是通过 get内部的数据流 也就是id信息  获取对应的verion 信息   上面只是版本的校验 还没有到读取数据的时候
                try (Releasable ignore = versionMap.acquireLock(get.uid().bytes())) {
                    // we need to lock here to access the version map to do this truly in RT
                    // 获取某个byte 对应的version信息 包含seqNo 和 term
                    versionValue = getVersionFromMap(get.uid().bytes());
                }
                if (versionValue != null) {
                    // 如果version信息不存在 就是代表数据已经被删除
                    if (versionValue.isDelete()) {
                        return GetResult.NOT_EXISTS;
                    }
                    // 根据内部/外部发起的get请求 versionType是不一样的 会通过它来检验是否发生了版本冲突  冲突时抛出异常
                    if (get.versionType().isVersionConflictForReads(versionValue.version, get.version())) {
                        throw new VersionConflictEngineException(shardId, get.id(),
                            get.versionType().explainConflictForReads(versionValue.version, get.version()));
                    }
                    // getIfSeqNo 代表对 seqNo 做了限制 当不匹配时 抛出异常
                    if (get.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && (
                        get.getIfSeqNo() != versionValue.seqNo || get.getIfPrimaryTerm() != versionValue.term
                        )) {
                        throw new VersionConflictEngineException(shardId, get.id(),
                            get.getIfSeqNo(), get.getIfPrimaryTerm(), versionValue.seqNo, versionValue.term);
                    }
                    // 是否要求从事务日志中读取数据
                    if (get.isReadFromTranslog()) {
                        // this is only used for updates - API _GET calls will always read form a reader for consistency
                        // the update call doesn't need the consistency since it's source only + _parent but parent can go away in 7.0
                        if (versionValue.getLocation() != null) {
                            try {
                                // 从事务日志文件中还原数据
                                Translog.Operation operation = translog.readOperation(versionValue.getLocation());
                                if (operation != null) {
                                    // in the case of a already pruned translog generation we might get null here - yet very unlikely
                                    // 看来能被get查询到的一定是一个index
                                    final Translog.Index index = (Translog.Index) operation;
                                    // 这是把一个 Operation 包装成了reader
                                    TranslogLeafReader reader = new TranslogLeafReader(index);
                                    // 直接把reader设置到 IndexSearcher中
                                    return new GetResult(new Engine.Searcher("realtime_get", reader,
                                        IndexSearcher.getDefaultSimilarity(), null, IndexSearcher.getDefaultQueryCachingPolicy(), reader),
                                        new VersionsAndSeqNoResolver.DocIdAndVersion(0, index.version(), index.seqNo(), index.primaryTerm(),
                                            reader, 0), true);
                                }
                            } catch (IOException e) {
                                maybeFailEngine("realtime_get", e); // lets check if the translog has failed with a tragic event
                                throw new EngineException(shardId, "failed to read operation from translog", e);
                            }
                        } else {
                            trackTranslogLocation.set(true);
                        }
                    }
                    assert versionValue.seqNo >= 0 : versionValue;
                    // 在实时查询 且通过searcher的情况 需要为reader做刷新工作
                    // realtime 就是确保每次读取到的都是最新的数据 也就是刚刚刷新完的 因为在lucene中写入数据 并不会立即生成segment 一般要通过commit方法才能固化最新的segment
                    // 而为了能感知到上一时刻写入的数据 就需要触发refresh
                    refreshIfNeeded("realtime_get", versionValue.seqNo);
                }
                scope = SearcherScope.INTERNAL;
            } else {
                // 对应非实时查询
                // we expose what has been externally expose in a point in time snapshot via an explicit refresh
                scope = SearcherScope.EXTERNAL;
            }

            // no version, get the version from the index, we know that we refresh on flush
            return getFromSearcher(get, searcherFactory, scope);
        }
    }

    /**
     * the status of the current doc version in lucene, compared to the version in an incoming
     * operation
     */
    enum OpVsLuceneDocStatus {
        /**
         * the op is more recent than the one that last modified the doc found in lucene
         * 代表本次更新
         */
        OP_NEWER,
        /**
         * the op is older or the same as the one that last modified the doc found in lucene
         * 2个op相同
         */
        OP_STALE_OR_EQUAL,
        /** no doc was found in lucene */
        LUCENE_DOC_NOT_FOUND
    }

    /**
     * 检测  前3个参数与第4个参数的新旧关系
     * @param id
     * @param seqNo
     * @param primaryTerm
     * @param versionValue
     * @return
     */
    private static OpVsLuceneDocStatus compareOpToVersionMapOnSeqNo(String id, long seqNo, long primaryTerm, VersionValue versionValue) {
        Objects.requireNonNull(versionValue);
        if (seqNo > versionValue.seqNo) {
            return OpVsLuceneDocStatus.OP_NEWER;
        } else if (seqNo == versionValue.seqNo) {
            assert versionValue.term == primaryTerm : "primary term not matched; id=" + id + " seq_no=" + seqNo
                + " op_term=" + primaryTerm + " existing_term=" + versionValue.term;
            return OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
        } else {
            return OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
        }
    }

    /**
     *
     * @param op  本次发起的operation
     * @return
     * @throws IOException
     */
    private OpVsLuceneDocStatus compareOpToLuceneDocBasedOnSeqNo(final Operation op) throws IOException {
        assert op.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "resolving ops based on seq# but no seqNo is found";
        final OpVsLuceneDocStatus status;

        // 在safe模式下 获取该id对应的版本号信息
        VersionValue versionValue = getVersionFromMap(op.uid().bytes());
        assert incrementVersionLookup();
        // TODO 先不考虑 存在的情况 因为refresh过程中会清空map的数据
        if (versionValue != null) {
            // 将本次的operation 信息 与查询出来的versionMap中的信息做比较
            status = compareOpToVersionMapOnSeqNo(op.id(), op.seqNo(), op.primaryTerm(), versionValue);
        } else {
            // load from index
            assert incrementIndexVersionLookup();
            // 通过 internalReaderManager 获取searcher对象
            try (Searcher searcher = acquireSearcher("load_seq_no", SearcherScope.INTERNAL)) {
                // 查询该id对应的docId 以及seqNo
                final DocIdAndSeqNo docAndSeqNo = VersionsAndSeqNoResolver.loadDocIdAndSeqNo(searcher.getIndexReader(), op.uid());
                if (docAndSeqNo == null) {
                    // 代表本次op数据之前没有写入到lucene中
                    status = OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND;
                    // 代表同一id 但是本次的数据更新
                } else if (op.seqNo() > docAndSeqNo.seqNo) {
                    status = OpVsLuceneDocStatus.OP_NEWER;
                    // 2次数据的序列号完全一致
                } else if (op.seqNo() == docAndSeqNo.seqNo) {
                    assert localCheckpointTracker.hasProcessed(op.seqNo()) :
                        "local checkpoint tracker is not updated seq_no=" + op.seqNo() + " id=" + op.id();
                    status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
                } else {
                    status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
                }
            }
        }
        return status;
    }

    /**
     * resolves the current version of the document, returning null if not found
     * 解析当前operation的版本  当没有找到时返回null
     */
    private VersionValue resolveDocVersion(final Operation op, boolean loadSeqNo) throws IOException {
        assert incrementVersionLookup(); // used for asserting in tests
        VersionValue versionValue = getVersionFromMap(op.uid().bytes());
        // 当没有从 versionMap中直接找到 只能选择通过searcher查找
        if (versionValue == null) {
            assert incrementIndexVersionLookup(); // used for asserting in tests
            final VersionsAndSeqNoResolver.DocIdAndVersion docIdAndVersion;
            try (Searcher searcher = acquireSearcher("load_version", SearcherScope.INTERNAL)) {
                 docIdAndVersion = VersionsAndSeqNoResolver.loadDocIdAndVersion(searcher.getIndexReader(), op.uid(), loadSeqNo);
            }
            if (docIdAndVersion != null) {
                versionValue = new IndexVersionValue(null, docIdAndVersion.version, docIdAndVersion.seqNo, docIdAndVersion.primaryTerm);
            }
        // 当version存在的情况下 发现被标记成 delete了 返回null
        } else if (engineConfig.isEnableGcDeletes() && versionValue.isDelete() &&
            (engineConfig.getThreadPool().relativeTimeInMillis() - ((DeleteVersionValue)versionValue).time) > getGcDeletesInMillis()) {
            versionValue = null;
        }
        return versionValue;
    }

    /**
     * 使用 operation.id 去versionMap中查询版本号信息
     * @param id
     * @return
     */
    private VersionValue getVersionFromMap(BytesRef id) {
        // 只要 current  old 中有一个是unsafe的  就认为versionMap是unsafe的
        if (versionMap.isUnsafe()) {
            synchronized (versionMap) {
                // we are switching from an unsafe map to a safe map. This might happen concurrently
                // but we only need to do this once since the last operation per ID is to add to the version
                // map so once we pass this point we can safely lookup from the version map.
                if (versionMap.isUnsafe()) {
                    // 刷新操作会触发监听器 进而使得versionMap的old 和current被替换
                    refresh("unsafe_version_map", SearcherScope.INTERNAL, true);
                }
                versionMap.enforceSafeAccess();
            }
        }
        // 本身获取版本号的操作好像和 是否处于safe状态没有直接联系啊
        return versionMap.getUnderLock(id);
    }

    /**
     * 检测 addDoc的操作能否优化
     * @param index
     * @return
     */
    private boolean canOptimizeAddDocument(Index index) {
        // 首先要求index设置了自动生成id的时间戳
        if (index.getAutoGeneratedIdTimestamp() != IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP) {
            assert index.getAutoGeneratedIdTimestamp() >= 0 : "autoGeneratedIdTimestamp must be positive but was: "
                + index.getAutoGeneratedIdTimestamp();
            switch (index.origin()) {
                // 由主分片发起的 index操作
                case PRIMARY:
                    assert assertPrimaryCanOptimizeAddDocument(index);
                    return true;
                case PEER_RECOVERY:
                case REPLICA:
                    assert index.version() == 1 && index.versionType() == null
                        : "version: " + index.version() + " type: " + index.versionType();
                    return true;
                    // 当从事务日志中重做数据时 返回true
                case LOCAL_TRANSLOG_RECOVERY:
                case LOCAL_RESET:
                    assert index.isRetry();
                    return true; // allow to optimize in order to update the max safe time stamp
                default:
                    throw new IllegalArgumentException("unknown origin " + index.origin());
            }
        }
        return false;
    }

    protected boolean assertPrimaryCanOptimizeAddDocument(final Index index) {
        assert (index.version() == Versions.MATCH_DELETED || index.version() == Versions.MATCH_ANY) &&
            index.versionType() == VersionType.INTERNAL
            : "version: " + index.version() + " type: " + index.versionType();
        return true;
    }

    private boolean assertIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        if (origin == Operation.Origin.PRIMARY) {
            assert assertPrimaryIncomingSequenceNumber(origin, seqNo);
        } else {
            // sequence number should be set when operation origin is not primary
            assert seqNo >= 0 : "recovery or replica ops should have an assigned seq no.; origin: " + origin;
        }
        return true;
    }

    protected boolean assertPrimaryIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        // sequence number should not be set when operation origin is primary
        assert seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO
                : "primary operations must never have an assigned sequence number but was [" + seqNo + "]";
        return true;
    }

    /**
     * 为某个操作生成一个 seq
     * @param operation
     * @return
     */
    protected long generateSeqNoForOperationOnPrimary(final Operation operation) {
        assert operation.origin() == Operation.Origin.PRIMARY;
        assert operation.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO :
            "ops should not have an assigned seq no. but was: " + operation.seqNo();
        // 默认情况下就是获取 localCheckpoint.nextSeq
        return doGenerateSeqNoForOperation(operation);
    }

    protected void advanceMaxSeqNoOfUpdatesOrDeletesOnPrimary(long seqNo) {
        advanceMaxSeqNoOfUpdatesOrDeletes(seqNo);
    }

    /**
     * Generate the sequence number for the specified operation.
     *
     * @param operation the operation
     * @return the sequence number
     */
    long doGenerateSeqNoForOperation(final Operation operation) {
        return localCheckpointTracker.generateSeqNo();
    }

    /**
     * 将index内部的数据写入到lucene中
     * @param index operation to perform
     * @return
     * @throws IOException
     */
    @Override
    public IndexResult index(Index index) throws IOException {
        assert Objects.equals(index.uid().field(), IdFieldMapper.NAME) : index.uid().field();
        // 恢复操作是不会被限制的
        final boolean doThrottle = index.origin().isRecovery() == false;
        try (ReleasableLock releasableLock = readLock.acquire()) {
            ensureOpen();
            assert assertIncomingSequenceNumber(index.origin(), index.seqNo());
            // 以id为单位进行定制锁
            try (Releasable ignored = versionMap.acquireLock(index.uid().bytes());
                Releasable indexThrottle = doThrottle ? () -> {} : throttle.acquireThrottle()) {
                // startTime 对应index对象生成的时间
                lastWriteNanos = index.startTime();
                /* A NOTE ABOUT APPEND ONLY OPTIMIZATIONS:
                 * if we have an autoGeneratedID that comes into the engine we can potentially optimize
                 * and just use addDocument instead of updateDocument and skip the entire version and index lookupVersion across the board.
                 * Yet, we have to deal with multiple document delivery, for this we use a property of the document that is added
                 * to detect if it has potentially been added before. We use the documents timestamp for this since it's something
                 * that:
                 *  - doesn't change per document
                 *  - is preserved in the transaction log
                 *  - and is assigned before we start to index / replicate
                 * NOTE: it's not important for this timestamp to be consistent across nodes etc. it's just a number that is in the common
                 * case increasing and can be used in the failure case when we retry and resent documents to establish a happens before
                 * relationship. For instance:
                 *  - doc A has autoGeneratedIdTimestamp = 10, isRetry = false
                 *  - doc B has autoGeneratedIdTimestamp = 9, isRetry = false
                 *
                 *  while both docs are in in flight, we disconnect on one node, reconnect and send doc A again
                 *  - now doc A' has autoGeneratedIdTimestamp = 10, isRetry = true
                 *
                 *  if A' arrives on the shard first we update maxUnsafeAutoIdTimestamp to 10 and use update document. All subsequent
                 *  documents that arrive (A and B) will also use updateDocument since their timestamps are less than
                 *  maxUnsafeAutoIdTimestamp. While this is not strictly needed for doc B it is just much simpler to implement since it
                 *  will just de-optimize some doc in the worst case.
                 *
                 *  if A arrives on the shard first we use addDocument since maxUnsafeAutoIdTimestamp is < 10. A` will then just be skipped
                 *  or calls updateDocument.
                 * 根据index的信息  生成策略对象
                 */
                final IndexingStrategy plan = indexingStrategyForOperation(index);

                final IndexResult indexResult;
                // TODO 在恢复过程中可能生成的几个策略都不会包含这个异常
                if (plan.earlyResultOnPreFlightError.isPresent()) {
                    indexResult = plan.earlyResultOnPreFlightError.get();
                    assert indexResult.getResultType() == Result.Type.FAILURE : indexResult.getResultType();
                } else {
                    // generate or register sequence number
                    // TODO 代表在主分片执行写入操作 先忽略
                    if (index.origin() == Operation.Origin.PRIMARY) {

                        // index在传入时设置的是 ifSeq 并且校验使用的也是 ifSeq 而在写入时需要分配一个新的seq
                        // 副本的话 应该是有带seq的
                        // generateSeqNoForOperationOnPrimary 会生成一个新的seq
                        index = new Index(index.uid(), index.parsedDoc(), generateSeqNoForOperationOnPrimary(index), index.primaryTerm(),
                            index.version(), index.versionType(), index.origin(), index.startTime(), index.getAutoGeneratedIdTimestamp(),
                            index.isRetry(), index.getIfSeqNo(), index.getIfPrimaryTerm());

                        // indexIntoLucene 代表是否要写入到lucene中
                        // TODO useLuceneUpdateDocument 这个是代表要覆盖的意思吗
                        final boolean toAppend = plan.indexIntoLucene && plan.useLuceneUpdateDocument == false;
                        if (toAppend == false) {
                            advanceMaxSeqNoOfUpdatesOrDeletesOnPrimary(index.seqNo());
                        }
                    } else {
                        // 通过localCheckpointTracker 记录此时最新的seqNo
                        markSeqNoAsSeen(index.seqNo());
                    }

                    assert index.seqNo() >= 0 : "ops should have an assigned seq no.; origin: " + index.origin();

                    // 代表需要将数据写入到lucene中
                    if (plan.indexIntoLucene || plan.addStaleOpToLucene) {
                        indexResult = indexIntoLucene(index, plan);
                    } else {
                        // 不需要写入的话 直接生成结果(SUCCESS)
                        indexResult = new IndexResult(
                            plan.versionForIndexing, index.primaryTerm(), index.seqNo(), plan.currentNotFoundOrDeleted);
                    }
                }
                // TODO 先忽略 非事务日志发起的情况
                if (index.origin().isFromTranslog() == false) {
                    final Translog.Location location;
                    if (indexResult.getResultType() == Result.Type.SUCCESS) {
                        location = translog.add(new Translog.Index(index, indexResult));
                        // 操作失败时 记录一个NOOP 因为即使是失败的记录 也有seqNo
                    } else if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        // if we have document failure, record it as a no-op in the translog and Lucene with the generated seq_no
                        final NoOp noOp = new NoOp(indexResult.getSeqNo(), index.primaryTerm(), index.origin(),
                            index.startTime(), indexResult.getFailure().toString());
                        location = innerNoOp(noOp).getTranslogLocation();
                    } else {
                        location = null;
                    }
                    // 如果本次不是由事务日志发起的操作 那么还需要将操作写回到事务日志中 同时在indexResult中记录位置信息
                    indexResult.setTranslogLocation(location);
                }

                // 这时已经完成了 operation在lucene的写入以及 事务日志的写入 注意都还没刷盘 (lucene还没提交)
                // 如果频繁的调用lucene.commit 会影响效率
                if (plan.indexIntoLucene && indexResult.getResultType() == Result.Type.SUCCESS) {
                    final Translog.Location translogLocation = trackTranslogLocation.get() ? indexResult.getTranslogLocation() : null;
                    // 将该id对应的最新信息写入到 versionMap中 不过每次refresh  versionMap的数据都会被清除 那么意义在哪里
                    versionMap.maybePutIndexUnderLock(index.uid().bytes(),
                        new IndexVersionValue(translogLocation, plan.versionForIndexing, index.seqNo(), index.primaryTerm()));
                }
                // 更新 processedCheckpoint 代表该seq已经写入到lucene中了
                localCheckpointTracker.markSeqNoAsProcessed(indexResult.getSeqNo());
                // 比如从事务文件中恢复数据 就不会有位置信息
                if (indexResult.getTranslogLocation() == null) {
                    // the op is coming from the translog (and is hence persisted already) or it does not have a sequence number
                    assert index.origin().isFromTranslog() || indexResult.getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO;
                    // 在写入事务文件时应该已经修改过 seqNoPersisted了  这里如果是从事务文件中恢复的 那么也要进行同步
                    localCheckpointTracker.markSeqNoAsPersisted(indexResult.getSeqNo());
                }
                // 设置完成时间
                indexResult.setTook(System.nanoTime() - index.startTime());
                indexResult.freeze();
                return indexResult;
            }
        } catch (RuntimeException | IOException e) {
            try {
                if (e instanceof AlreadyClosedException == false && treatDocumentFailureAsTragicError(index)) {
                    failEngine("index id[" + index.id() + "] origin[" + index.origin() + "] seq#[" + index.seqNo() + "]", e);
                } else {
                    maybeFailEngine("index id[" + index.id() + "] origin[" + index.origin() + "] seq#[" + index.seqNo() + "]", e);
                }
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
    }

    /**
     * 当本对象作为副本 或者 从事务日志中恢复数据时走该分支
     * 生成一个index写入策略
     * @param index
     * @return
     * @throws IOException
     */
    protected final IndexingStrategy planIndexingAsNonPrimary(Index index) throws IOException {
        assert assertNonPrimaryOrigin(index);
        // needs to maintain the auto_id timestamp in case this replica becomes primary
        // 当设置了 自动id时间戳  就允许进行优化
        if (canOptimizeAddDocument(index)) {
            // 更新 2个maxTimestamp
            mayHaveBeenIndexedBefore(index);
        }
        final IndexingStrategy plan;
        // unlike the primary, replicas don't really care to about creation status of documents
        // this allows to ignore the case where a document was found in the live version maps in
        // a delete state and return false for the created flag in favor of code simplicity
        // 获取此时最大的seq  在启动阶段会从lucene.userData中获取maxSeqNo
        final long maxSeqNoOfUpdatesOrDeletes = getMaxSeqNoOfUpdatesOrDeletes();
        // 如果对应seq 小于 localCheckpoint.processed  代表之前已经处理过
        if (hasBeenProcessedBefore(index)) {
            // the operation seq# was processed and thus the same operation was already put into lucene
            // this can happen during recovery where older operations are sent from the translog that are already
            // part of the lucene commit (either from a peer recovery or a local translog)
            // or due to concurrent indexing & recovery. For the former it is important to skip lucene as the operation in
            // question may have been deleted in an out of order op that is not replayed.
            // See testRecoverFromStoreWithOutOfOrderDelete for an example of local recovery
            // See testRecoveryWithOutOfOrderDelete for an example of peer recovery
            // 这里返回的策略就是 会处理数据 但是跳过lucene的写入过程
            plan = IndexingStrategy.processButSkipLucene(false, index.version());

            // 这种情况是可能出现的 maxSeqNoOfUpdatesOrDeletes 对应lucene.userData记录的maxSeq
            // 而在初始化过程中会尽可能的读取此时lucene的doc数据 每个doc都有自己的seq  就有可能超过userData中记录的seq
        } else if (maxSeqNoOfUpdatesOrDeletes <= localCheckpointTracker.getProcessedCheckpoint()) {
            // see Engine#getMaxSeqNoOfUpdatesOrDeletes for the explanation of the optimization using sequence numbers
            assert maxSeqNoOfUpdatesOrDeletes < index.seqNo() : index.seqNo() + ">=" + maxSeqNoOfUpdatesOrDeletes;
            plan = IndexingStrategy.optimizedAppendOnly(index.version());
        } else {
            // 此时将记录 id版本号的map 标记为在安全状态下使用
            versionMap.enforceSafeAccess();
            // 通过index.id 去lucene中寻找匹配的数据记录 获取docId 以及 seqNo   与index内的数据进行比较
            final OpVsLuceneDocStatus opVsLucene = compareOpToLuceneDocBasedOnSeqNo(index);
            // 代表本次操作可能是一次无意义的操作 比如相同数据已经存在于lucene中了
            if (opVsLucene == OpVsLuceneDocStatus.OP_STALE_OR_EQUAL) {
                plan = IndexingStrategy.processAsStaleOp(index.version());
            } else {
                // 当currentNotFoundOrDeleted 为false时  实际上只有 OP_NEWER这种情况 也就是要更新某个id/seq对应的doc的数据
                plan = IndexingStrategy.processNormally(opVsLucene == OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND, index.version());
            }
        }
        return plan;
    }

    /**
     * index的写入可以使用多种策略
     * @param index
     * @return IndexingStrategy 用于决定本次index操作 会是更新数据 或者插入数据等
     * @throws IOException
     */
    protected IndexingStrategy indexingStrategyForOperation(final Index index) throws IOException {
        // TODO 先不看这个  先考虑从本地事务文件恢复数据怎么做
        if (index.origin() == Operation.Origin.PRIMARY) {
            return planIndexingAsPrimary(index);
        } else {
            // non-primary mode (i.e., replica or recovery)
            // 代表本对象是副本 或者是重做数据
            return planIndexingAsNonPrimary(index);
        }
    }

    /**
     * 当index.origin 是PRIMARY 时 使用这个策略
     * @param index
     * @return
     * @throws IOException
     */
    private IndexingStrategy planIndexingAsPrimary(Index index) throws IOException {
        assert index.origin() == Operation.Origin.PRIMARY : "planing as primary but origin isn't. got " + index.origin();
        final IndexingStrategy plan;
        // resolve an external operation into an internal one which is safe to replay
        // 默认情况下总是可以优化 index
        final boolean canOptimizeAddDocument = canOptimizeAddDocument(index);
        // 可优化且之前没有写入过该index信息
        if (canOptimizeAddDocument && mayHaveBeenIndexedBefore(index) == false) {
            plan = IndexingStrategy.optimizedAppendOnly(1L);
        } else {
            // 代表本次不可优化
            // 标志成需要安全访问
            versionMap.enforceSafeAccess();
            // resolves incoming version
            // 默认情况下版本应该是null  但是如果之前已经写如果 那么应该会将版本信息存储到 versionMap中
            final VersionValue versionValue =
                resolveDocVersion(index, index.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO);
            final long currentVersion;
            final boolean currentNotFoundOrDeleted;
            if (versionValue == null) {
                currentVersion = Versions.NOT_FOUND;
                currentNotFoundOrDeleted = true;
            } else {
                currentVersion = versionValue.version;
                // 找到的情况下查看是否已经被标记成删除了
                currentNotFoundOrDeleted = versionValue.isDelete();
            }
            // 代表需要seq 已经被删除了或者没找到
            if (index.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && currentNotFoundOrDeleted) {
                final VersionConflictEngineException e = new VersionConflictEngineException(shardId, index.id(),
                    index.getIfSeqNo(), index.getIfPrimaryTerm(), SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
                plan = IndexingStrategy.skipDueToVersionConflict(e, true, currentVersion);
                // 代表虽然存在 版本号 但是不匹配
            } else if (index.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && (
                versionValue.seqNo != index.getIfSeqNo() || versionValue.term != index.getIfPrimaryTerm()
            )) {
                final VersionConflictEngineException e = new VersionConflictEngineException(shardId, index.id(),
                    index.getIfSeqNo(), index.getIfPrimaryTerm(), versionValue.seqNo, versionValue.term);
                // 这会产生一个 内部已经设置有包含 Exception 的 IndexResult对象
                plan = IndexingStrategy.skipDueToVersionConflict(e, currentNotFoundOrDeleted, currentVersion);
                // index 的versionType 定义了一套版本校验规则 这里要进行校验
            } else if (index.versionType().isVersionConflictForWrites(
                currentVersion, index.version(), currentNotFoundOrDeleted)) {
                final VersionConflictEngineException e =
                        new VersionConflictEngineException(shardId, index, currentVersion, currentNotFoundOrDeleted);
                plan = IndexingStrategy.skipDueToVersionConflict(e, currentNotFoundOrDeleted, currentVersion);
            } else {
                plan = IndexingStrategy.processNormally(currentNotFoundOrDeleted,
                    canOptimizeAddDocument ? 1L : index.versionType().updateVersion(currentVersion, index.version())
                );
            }
        }
        return plan;
    }

    /**
     * 将index数据写入到 lucene中
     * @param index
     * @param plan
     * @return
     * @throws IOException
     */
    private IndexResult indexIntoLucene(Index index, IndexingStrategy plan)
        throws IOException {
        assert index.seqNo() >= 0 : "ops should have an assigned seq no.; origin: " + index.origin();
        assert plan.versionForIndexing >= 0 : "version must be set. got " + plan.versionForIndexing;
        assert plan.indexIntoLucene || plan.addStaleOpToLucene;
        /* Update the document's sequence number and primary term; the sequence number here is derived here from either the sequence
         * number service if this is on the primary, or the existing document's sequence number if this is on the replica. The
         * primary term here has already been set, see IndexShard#prepareIndex where the Engine$Index operation is created.
         * 这里就是填充数据
         */
        index.parsedDoc().updateSeqID(index.seqNo(), index.primaryTerm());
        index.parsedDoc().version().setLongValue(plan.versionForIndexing);
        try {
            // 根据不同情况选择写入 过期doc 更新doc 新增doc
            // addStaleOpToLucene  代表本次要写入的 op 对应的seq,id 已经有过相同的数据存在于lucene中了
            if (plan.addStaleOpToLucene) {
                addStaleDocs(index.docs(), indexWriter);
                // 代表本次是一次更新请求 这种情况是本次写入的id 在lucene中已经存在 但是seq更新
            } else if (plan.useLuceneUpdateDocument) {
                assert assertMaxSeqNoOfUpdatesIsAdvanced(index.uid(), index.seqNo(), true, true);
                updateDocs(index.uid(), index.docs(), indexWriter);
            } else {
                // 这里就是正常的写入操作
                // document does not exists, we can optimize for create, but double check if assertions are running
                assert assertDocDoesNotExist(index, canOptimizeAddDocument(index) == false);
                addDocs(index.docs(), indexWriter);
            }
            return new IndexResult(plan.versionForIndexing, index.primaryTerm(), index.seqNo(), plan.currentNotFoundOrDeleted);
        } catch (Exception ex) {
            if (ex instanceof AlreadyClosedException == false &&
                indexWriter.getTragicException() == null && treatDocumentFailureAsTragicError(index) == false) {
                /* There is no tragic event recorded so this must be a document failure.
                 *
                 * The handling inside IW doesn't guarantee that an tragic / aborting exception
                 * will be used as THE tragicEventException since if there are multiple exceptions causing an abort in IW
                 * only one wins. Yet, only the one that wins will also close the IW and in turn fail the engine such that
                 * we can potentially handle the exception before the engine is failed.
                 * Bottom line is that we can only rely on the fact that if it's a document failure then
                 * `indexWriter.getTragicException()` will be null otherwise we have to rethrow and treat it as fatal or rather
                 * non-document failure
                 *
                 * we return a `MATCH_ANY` version to indicate no document was index. The value is
                 * not used anyway
                 */
                return new IndexResult(ex, Versions.MATCH_ANY, index.primaryTerm(), index.seqNo());
            } else {
                throw ex;
            }
        }
    }

    /**
     * Whether we should treat any document failure as tragic error.
     * If we hit any failure while processing an indexing on a replica, we should treat that error as tragic and fail the engine.
     * However, we prefer to fail a request individually (instead of a shard) if we hit a document failure on the primary.
     * 某些失败是可以恢复的
     */
    private boolean treatDocumentFailureAsTragicError(Index index) {
        // TODO: can we enable this check for all origins except primary on the leader?
        return index.origin() == Operation.Origin.REPLICA
            || index.origin() == Operation.Origin.PEER_RECOVERY
            || index.origin() == Operation.Origin.LOCAL_RESET;
    }

    /**
     * returns true if the indexing operation may have already be processed by this engine.
     * Note that it is OK to rarely return true even if this is not the case. However a `false`
     * return value must always be correct.
     * 检测某个index 是否之前已经写入了
     */
    private boolean mayHaveBeenIndexedBefore(Index index) {
        assert canOptimizeAddDocument(index);
        final boolean mayHaveBeenIndexBefore;
        // 如果本次操作是重试操作  针对从事务日志中恢复数据 就属于retry操作
        if (index.isRetry()) {
            // 可能该index已经写入到lucene中了
            mayHaveBeenIndexBefore = true;
            // 就是更新时间戳的
            updateAutoIdTimestamp(index.getAutoGeneratedIdTimestamp(), true);
            assert maxUnsafeAutoIdTimestamp.get() >= index.getAutoGeneratedIdTimestamp();
        } else {
            // in this case we force
            // 如果此时的时间戳 已经超过了index的时间戳 就认为之前已经写入过了
            mayHaveBeenIndexBefore = maxUnsafeAutoIdTimestamp.get() >= index.getAutoGeneratedIdTimestamp();
            updateAutoIdTimestamp(index.getAutoGeneratedIdTimestamp(), false);
        }
        return mayHaveBeenIndexBefore;
    }

    /**
     * 直接写入doc
     * @param docs
     * @param indexWriter
     * @throws IOException
     */
    private void addDocs(final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs);
        } else {
            indexWriter.addDocument(docs.get(0));
        }
        numDocAppends.inc(docs.size());
    }

    /**
     * 将一个过期的doc 写入到lucene中
     * @param docs
     * @param indexWriter
     * @throws IOException
     */
    private void addStaleDocs(final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        // 为每个doc增加了一个  软删除的field
        for (ParseContext.Document doc : docs) {
            doc.add(softDeletesField); // soft-deleted every document before adding to Lucene
        }
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs);
        } else {
            indexWriter.addDocument(docs.get(0));
        }
    }

    /**
     * 在写入index时选择的策略
     */
    protected static final class IndexingStrategy {
        /**
         * 代表对应的数据是否未找到 或者被删除
         */
        final boolean currentNotFoundOrDeleted;
        final boolean useLuceneUpdateDocument;
        /**
         * 在使用优化措施时 该值为1
         */
        final long versionForIndexing;
        /**
         * 本次数据是否会写入到 lucene中
         */
        final boolean indexIntoLucene;
        /**
         * 是否插入一个过期的operation到lucene中
         */
        final boolean addStaleOpToLucene;
        final Optional<IndexResult> earlyResultOnPreFlightError;

        /**
         * 这里只是最简单的赋值操作
         * @param currentNotFoundOrDeleted
         * @param useLuceneUpdateDocument
         * @param indexIntoLucene
         * @param addStaleOpToLucene
         * @param versionForIndexing
         * @param earlyResultOnPreFlightError
         */
        private IndexingStrategy(boolean currentNotFoundOrDeleted, boolean useLuceneUpdateDocument,
                                 boolean indexIntoLucene, boolean addStaleOpToLucene,
                                 long versionForIndexing, IndexResult earlyResultOnPreFlightError) {
            assert useLuceneUpdateDocument == false || indexIntoLucene :
                "use lucene update is set to true, but we're not indexing into lucene";
            assert (indexIntoLucene && earlyResultOnPreFlightError != null) == false :
                "can only index into lucene or have a preflight result but not both." +
                    "indexIntoLucene: " + indexIntoLucene
                    + "  earlyResultOnPreFlightError:" + earlyResultOnPreFlightError;
            this.currentNotFoundOrDeleted = currentNotFoundOrDeleted;
            this.useLuceneUpdateDocument = useLuceneUpdateDocument;
            this.versionForIndexing = versionForIndexing;
            this.indexIntoLucene = indexIntoLucene;
            this.addStaleOpToLucene = addStaleOpToLucene;
            this.earlyResultOnPreFlightError =
                earlyResultOnPreFlightError == null ? Optional.empty() :
                    Optional.of(earlyResultOnPreFlightError);
        }

        /**
         * 针对仅追加的情况进行优化
         * @param versionForIndexing
         * @return
         */
        static IndexingStrategy optimizedAppendOnly(long versionForIndexing) {
            return new IndexingStrategy(true, false, true, false, versionForIndexing, null);
        }

        /**
         * 将版本标记成 -1 同时设置了一个失败的IndexResult 设置到IndexingStrategy中
         * @param e
         * @param currentNotFoundOrDeleted
         * @param currentVersion
         * @return
         */
        public static IndexingStrategy skipDueToVersionConflict(
                VersionConflictEngineException e, boolean currentNotFoundOrDeleted, long currentVersion) {
            final IndexResult result = new IndexResult(e, currentVersion);
            return new IndexingStrategy(
                    currentNotFoundOrDeleted, false, false, false,
                Versions.NOT_FOUND, result);
        }

        /**
         * 正常处理 生成 IndexingStrategy
         * @param currentNotFoundOrDeleted
         * @param versionForIndexing
         * @return
         */
        static IndexingStrategy processNormally(boolean currentNotFoundOrDeleted,
                                                long versionForIndexing) {
            return new IndexingStrategy(currentNotFoundOrDeleted, currentNotFoundOrDeleted == false,
                true, false, versionForIndexing, null);
        }

        /**
         * 代表会处理 Operation  但是会跳过有关lucene的写入过程
         * @param currentNotFoundOrDeleted
         * @param versionForIndexing
         * @return
         */
        public static IndexingStrategy processButSkipLucene(boolean currentNotFoundOrDeleted, long versionForIndexing) {
            return new IndexingStrategy(currentNotFoundOrDeleted, false, false,
                false, versionForIndexing, null);
        }

        /**
         * 处理过期的 op
         * @param versionForIndexing
         * @return
         */
        static IndexingStrategy processAsStaleOp(long versionForIndexing) {
            return new IndexingStrategy(false, false, false, true, versionForIndexing, null);
        }
    }

    /**
     * Asserts that the doc in the index operation really doesn't exist
     */
    private boolean assertDocDoesNotExist(final Index index, final boolean allowDeleted) throws IOException {
        // NOTE this uses direct access to the version map since we are in the assertion code where we maintain a secondary
        // map in the version map such that we don't need to refresh if we are unsafe;
        final VersionValue versionValue = versionMap.getVersionForAssert(index.uid().bytes());
        if (versionValue != null) {
            if (versionValue.isDelete() == false || allowDeleted == false) {
                throw new AssertionError("doc [" + index.id() + "] exists in version map (version " +
                    versionValue + ")");
            }
        } else {
            try (Searcher searcher = acquireSearcher("assert doc doesn't exist", SearcherScope.INTERNAL)) {
                final long docsWithId = searcher.count(new TermQuery(index.uid()));
                if (docsWithId > 0) {
                    throw new AssertionError("doc [" + index.id() + "] exists [" + docsWithId +
                        "] times in index");
                }
            }
        }
        return true;
    }

    /**
     * 更新doc的数据
     * @param uid
     * @param docs
     * @param indexWriter
     * @throws IOException
     */
    private void updateDocs(final Term uid, final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        // TODO 什么是软更新 以及为什么要传入软删除字段
        if (docs.size() > 1) {
            indexWriter.softUpdateDocuments(uid, docs, softDeletesField);
        } else {
            indexWriter.softUpdateDocument(uid, docs.get(0), softDeletesField);
        }
        numDocUpdates.inc(docs.size());
    }

    /**
     * 执行一次删除操作
     * @param delete operation to perform
     * @return
     * @throws IOException
     */
    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        versionMap.enforceSafeAccess();
        assert Objects.equals(delete.uid().field(), IdFieldMapper.NAME) : delete.uid().field();
        assert assertIncomingSequenceNumber(delete.origin(), delete.seqNo());
        final DeleteResult deleteResult;
        // NOTE: we don't throttle this when merges fall behind because delete-by-id does not create new segments:
        try (ReleasableLock ignored = readLock.acquire(); Releasable ignored2 = versionMap.acquireLock(delete.uid().bytes())) {
            ensureOpen();
            // 更新最后的写入时间戳 跟处理 Index时一样
            lastWriteNanos = delete.startTime();
            // 通过版本号校验后得到一个描述处理方式的 strategy
            final DeletionStrategy plan = deletionStrategyForOperation(delete);

            // 如果已经产生了异常 那么结果也提前设置好了 是一个失败结果
            if (plan.earlyResultOnPreflightError.isPresent()) {
                deleteResult = plan.earlyResultOnPreflightError.get();
            } else {
                // generate or register sequence number
                // TODO 先忽略 主分片的写入逻辑
                if (delete.origin() == Operation.Origin.PRIMARY) {
                    // 为delete生成seqNo
                    delete = new Delete(delete.id(), delete.uid(), generateSeqNoForOperationOnPrimary(delete),
                        delete.primaryTerm(), delete.version(), delete.versionType(), delete.origin(), delete.startTime(),
                        delete.getIfSeqNo(), delete.getIfPrimaryTerm());

                    advanceMaxSeqNoOfUpdatesOrDeletesOnPrimary(delete.seqNo());
                } else {
                    // 更新此时观测到的最大的seq
                    markSeqNoAsSeen(delete.seqNo());
                }

                assert delete.seqNo() >= 0 : "ops should have an assigned seq no.; origin: " + delete.origin();

                // 代表需要从lucene中删除该数据  当发现是stale操作 也需要重复执行(主要是为了重做事务日志)
                if (plan.deleteFromLucene || plan.addStaleOpToLucene) {
                    deleteResult = deleteInLucene(delete, plan);
                } else {
                    // 代表不需要写入到 lucene中 直接生成结果
                    deleteResult = new DeleteResult(
                        plan.versionOfDeletion, delete.primaryTerm(), delete.seqNo(), plan.currentlyDeleted == false);
                }
                // 代表本次操作作用到了lucene上 且不是之前已经存在于lucene的数据
                if (plan.deleteFromLucene) {
                    numDocDeletes.inc();
                    versionMap.putDeleteUnderLock(delete.uid().bytes(),
                        new DeleteVersionValue(plan.versionOfDeletion, delete.seqNo(), delete.primaryTerm(),
                            engineConfig.getThreadPool().relativeTimeInMillis()));
                }
            }
            // 如果本次操作不是从事务日志中获取的数据 就不需要记录到事务日志中
            if (delete.origin().isFromTranslog() == false && deleteResult.getResultType() == Result.Type.SUCCESS) {
                final Translog.Location location = translog.add(new Translog.Delete(delete, deleteResult));
                deleteResult.setTranslogLocation(location);
            }

            // 此时该seq对应的操作 同时记录到lucene和事务日志中 才增加 processedCheckpoint
            localCheckpointTracker.markSeqNoAsProcessed(deleteResult.getSeqNo());

            // 代表本次操作是从事务日志中重做的  这里增加的是 persistedCheckpoint
            // (因为从事务日志中恢复 反过来说记录一开始就存在于事务日志中 已经完成了持久化)
            if (deleteResult.getTranslogLocation() == null) {
                // the op is coming from the translog (and is hence persisted already) or does not have a sequence number (version conflict)
                assert delete.origin().isFromTranslog() || deleteResult.getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO;
                localCheckpointTracker.markSeqNoAsPersisted(deleteResult.getSeqNo());
            }
            deleteResult.setTook(System.nanoTime() - delete.startTime());
            deleteResult.freeze();
        } catch (RuntimeException | IOException e) {
            try {
                maybeFailEngine("delete", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
        // 处理delete操作会将数据存储到墓碑中 而每当间隔超过一定时间 会将太旧的墓碑数据清理掉
        // 这个数据存储在 versionMap的意义是什么
        maybePruneDeletes();
        return deleteResult;
    }

    /**
     * 通过本次传入的delete信息 生成一个删除策略
     * @param delete
     * @return
     * @throws IOException
     */
    protected DeletionStrategy deletionStrategyForOperation(final Delete delete) throws IOException {
        // TODO 先忽略主分片的删除操作
        if (delete.origin() == Operation.Origin.PRIMARY) {
            return planDeletionAsPrimary(delete);
        } else {
            // non-primary mode (i.e., replica or recovery)
            return planDeletionAsNonPrimary(delete);
        }
    }

    /**
     * 检测delete对应的id是否存在 或者是否已经处理过 生成不同的处理策略
     * @param delete
     * @return
     * @throws IOException
     */
    protected final DeletionStrategy planDeletionAsNonPrimary(Delete delete) throws IOException {
        assert assertNonPrimaryOrigin(delete);
        final DeletionStrategy plan;
        // 检测本次删除操作是否在之前已经执行过   就是比较 operation.seqNo 与 localCheckpointTracker.processedCheckpoint
        if (hasBeenProcessedBefore(delete)) {
            // the operation seq# was processed thus this operation was already put into lucene
            // this can happen during recovery where older operations are sent from the translog that are already
            // part of the lucene commit (either from a peer recovery or a local translog)
            // or due to concurrent indexing & recovery. For the former it is important to skip lucene as the operation in
            // question may have been deleted in an out of order op that is not replayed.
            // See testRecoverFromStoreWithOutOfOrderDelete for an example of local recovery
            // See testRecoveryWithOutOfOrderDelete for an example of peer recovery
            plan = DeletionStrategy.processButSkipLucene(false, delete.version());
        } else {
            // 实际上delete 也就是针对之前写入的index 这里就是匹配 seq/id 等信息
            // 推测是这样 如果某个op对应的seq已经更新到 processedCheckpoint上了 就代表这个操作已经同步到事务日志中了 就完全不需要处理了
            // 下面的情况 当发现seq重复 却没有写入到事务文件中 会再一次作用到lucene上 TODO 是为了重做事务日志吗  如果本身不需要记录事务操作又要怎么处理???
            final OpVsLuceneDocStatus opVsLucene = compareOpToLuceneDocBasedOnSeqNo(delete);
            // 如果查询到记录 应该就代表这条记录没有被删除
            if (opVsLucene == OpVsLuceneDocStatus.OP_STALE_OR_EQUAL) {
                plan = DeletionStrategy.processAsStaleOp(delete.version());
            } else {
                // 两种情况  一种是本次的seq比查询出来的记录更大 还有一种就是没有查询到记录
                plan = DeletionStrategy.processNormally(opVsLucene == OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND, delete.version());
            }
        }
        return plan;
    }

    protected boolean assertNonPrimaryOrigin(final Operation operation) {
        assert operation.origin() != Operation.Origin.PRIMARY : "planing as primary but got " + operation.origin();
        return true;
    }

    /**
     * 在主分片下处理 delete请求
     * @param delete
     * @return
     * @throws IOException
     */
    private DeletionStrategy planDeletionAsPrimary(Delete delete) throws IOException {
        assert delete.origin() == Operation.Origin.PRIMARY : "planing as primary but got " + delete.origin();
        // resolve operation from external to internal
        // 找到当前delete.id 对应的版本
        final VersionValue versionValue = resolveDocVersion(delete, delete.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO);
        assert incrementVersionLookup();
        final long currentVersion;
        final boolean currentlyDeleted;
        // 版本不存在 代表已经被删除了
        if (versionValue == null) {
            currentVersion = Versions.NOT_FOUND;
            currentlyDeleted = true;
        } else {
            // 设置当前版本号 以及是否已经被删除
            currentVersion = versionValue.version;
            currentlyDeleted = versionValue.isDelete();
        }
        // 下面的处理和 IndexStrategy是一样的

        final DeletionStrategy plan;
        // 当前已经被删除 无法校验版本号 在strategy中生成异常结果
        if (delete.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && currentlyDeleted) {
            final VersionConflictEngineException e = new VersionConflictEngineException(shardId, delete.id(),
                delete.getIfSeqNo(), delete.getIfPrimaryTerm(), SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
            plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, true);
            // 当版本号不匹配时抛出异常
        } else if (delete.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO && (
            versionValue.seqNo != delete.getIfSeqNo() || versionValue.term != delete.getIfPrimaryTerm()
        )) {
            final VersionConflictEngineException e = new VersionConflictEngineException(shardId, delete.id(),
                delete.getIfSeqNo(), delete.getIfPrimaryTerm(), versionValue.seqNo, versionValue.term);
            plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, currentlyDeleted);
        } else if (delete.versionType().isVersionConflictForWrites(currentVersion, delete.version(), currentlyDeleted)) {
            final VersionConflictEngineException e = new VersionConflictEngineException(shardId, delete, currentVersion, currentlyDeleted);
            plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, currentlyDeleted);
        } else {
            plan = DeletionStrategy.processNormally(currentlyDeleted, delete.versionType().updateVersion(currentVersion, delete.version()));
        }
        return plan;
    }

    /**
     * 将删除操作作用到lucene上
     * @param delete
     * @param plan
     * @return
     * @throws IOException
     */
    private DeleteResult deleteInLucene(Delete delete, DeletionStrategy plan) throws IOException {
        assert assertMaxSeqNoOfUpdatesIsAdvanced(delete.uid(), delete.seqNo(), false, false);
        try {
            // 在执行删除任务前 会生成一个对应删除操作的doc
            final ParsedDocument tombstone = engineConfig.getTombstoneDocSupplier().newDeleteTombstoneDoc(delete.id());
            assert tombstone.docs().size() == 1 : "Tombstone doc should have single doc [" + tombstone + "]";
            // 将Op内的信息设置到doc上
            tombstone.updateSeqID(delete.seqNo(), delete.primaryTerm());
            tombstone.version().setLongValue(plan.versionOfDeletion);
            final ParseContext.Document doc = tombstone.docs().get(0);
            assert doc.getField(SeqNoFieldMapper.TOMBSTONE_NAME) != null :
                "Delete tombstone document but _tombstone field is not set [" + doc + " ]";

            // 这些doc都追加上了 软删除字段 TODO 为啥
            doc.add(softDeletesField);
            // addStaleOpToLucene  代表需要再次写入 还不知道是为啥
            if (plan.addStaleOpToLucene || plan.currentlyDeleted) {
                indexWriter.addDocument(doc);
            } else {
                // 代表本次处理的数据 seq更大 执行更新操作
                indexWriter.softUpdateDocument(delete.uid(), doc, softDeletesField);
            }
            return new DeleteResult(
                plan.versionOfDeletion, delete.primaryTerm(), delete.seqNo(), plan.currentlyDeleted == false);
        } catch (final Exception ex) {
            /*
             * Document level failures when deleting are unexpected, we likely hit something fatal such as the Lucene index being corrupt,
             * or the Lucene document limit. We have already issued a sequence number here so this is fatal, fail the engine.
             */
            if (ex instanceof AlreadyClosedException == false && indexWriter.getTragicException() == null) {
                final String reason = String.format(
                    Locale.ROOT,
                    "delete id[%s] origin [%s] seq#[%d] failed at the document level",
                    delete.id(),
                    delete.origin(),
                    delete.seqNo());
                failEngine(reason, ex);
            }
            throw ex;
        }
    }

    /**
     * 基本跟 IndexStrategy一个套路
     */
    protected static final class DeletionStrategy {
        // of a rare double delete  代表本次删除操作是否需要作用到lucene上  比如该delete操作之前已经执行过了 那么就不该重复执行
        final boolean deleteFromLucene;
        final boolean addStaleOpToLucene;
        /**
         * 代表此时在lucene中无法找到相关数据
         */
        final boolean currentlyDeleted;
        final long versionOfDeletion;
        final Optional<DeleteResult> earlyResultOnPreflightError;

        private DeletionStrategy(boolean deleteFromLucene, boolean addStaleOpToLucene, boolean currentlyDeleted,
                                 long versionOfDeletion, DeleteResult earlyResultOnPreflightError) {
            assert (deleteFromLucene && earlyResultOnPreflightError != null) == false :
                "can only delete from lucene or have a preflight result but not both." +
                    "deleteFromLucene: " + deleteFromLucene
                    + "  earlyResultOnPreFlightError:" + earlyResultOnPreflightError;
            this.deleteFromLucene = deleteFromLucene;
            this.addStaleOpToLucene = addStaleOpToLucene;
            this.currentlyDeleted = currentlyDeleted;
            this.versionOfDeletion = versionOfDeletion;
            this.earlyResultOnPreflightError = earlyResultOnPreflightError == null ?
                Optional.empty() : Optional.of(earlyResultOnPreflightError);
        }

        public static DeletionStrategy skipDueToVersionConflict(
                VersionConflictEngineException e, long currentVersion, boolean currentlyDeleted) {
            final DeleteResult deleteResult = new DeleteResult(e, currentVersion, SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                SequenceNumbers.UNASSIGNED_SEQ_NO, currentlyDeleted == false);
            return new DeletionStrategy(false, false, currentlyDeleted, Versions.NOT_FOUND, deleteResult);
        }

        /**
         *
         * @param currentlyDeleted  true代表delete对应的doc信息在lucene中无法被找到
         * @param versionOfDeletion
         * @return
         */
        static DeletionStrategy processNormally(boolean currentlyDeleted, long versionOfDeletion) {
            return new DeletionStrategy(true, false, currentlyDeleted, versionOfDeletion, null);
        }

        /**
         * 代表本次 Op的seq 已经记录到事务日志中了 所以不需要重做到lucene上
         * @param currentlyDeleted
         * @param versionOfDeletion
         * @return
         */
        public static DeletionStrategy processButSkipLucene(boolean currentlyDeleted, long versionOfDeletion) {
            return new DeletionStrategy(false, false, currentlyDeleted, versionOfDeletion, null);
        }

        /**
         * delete对应的doc信息还存在于lucene中
         * @param versionOfDeletion
         * @return
         */
        static DeletionStrategy processAsStaleOp(long versionOfDeletion) {
            return new DeletionStrategy(false, true, false, versionOfDeletion, null);
        }
    }

    /**
     * 每当发生delete操作时 在VersionMap中会记录信息 (存储在 tombstoneMaps中)
     * 这里尝试清除数据
     */
    @Override
    public void maybePruneDeletes() {
        // It's expensive to prune because we walk the deletes map acquiring dirtyLock for each uid so we only do it
        // every 1/4 of gcDeletesInMillis:
        // 首先要确保开启了这个配置 其次2次删除应当有一个时间间隔
        if (engineConfig.isEnableGcDeletes() &&
                engineConfig.getThreadPool().relativeTimeInMillis() - lastDeleteVersionPruneTimeMSec > getGcDeletesInMillis() * 0.25) {
            pruneDeletedTombstones();
        }
    }

    /**
     * 处理noop
     * @param noOp
     * @return
     * @throws IOException
     */
    @Override
    public NoOpResult noOp(final NoOp noOp) throws IOException {
        final NoOpResult noOpResult;
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            noOpResult = innerNoOp(noOp);
        } catch (final Exception e) {
            try {
                maybeFailEngine("noop", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
        return noOpResult;
    }

    /**
     * 当需要处理一个 NOOP操作时 写入到lucene/事务日志中
     * @param noOp
     * @return
     * @throws IOException
     */
    private NoOpResult innerNoOp(final NoOp noOp) throws IOException {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread();
        assert noOp.seqNo() > SequenceNumbers.NO_OPS_PERFORMED;
        final long seqNo = noOp.seqNo();

        // 跟index/delete不同 是将seq作为锁的key    index/delete 都是将versionMap作为锁
        try (Releasable ignored = noOpKeyedLock.acquire(seqNo)) {
            final NoOpResult noOpResult;
            // 默认就是返回 EMPTY  先简单理解
            final Optional<Exception> preFlightError = preFlightCheckForNoOp(noOp);
            // 忽略
            if (preFlightError.isPresent()) {
                noOpResult = new NoOpResult(SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                    SequenceNumbers.UNASSIGNED_SEQ_NO, preFlightError.get());
            } else {

                // 每当要处理某个op时 就会尝试用它的seq 去更新localCheckpointTracker的nextSeq
                markSeqNoAsSeen(noOp.seqNo());

                // 如果该operate对应的seq 已经被处理了就可以跳过   这里没有生成 处理策略那一步操作
                if (hasBeenProcessedBefore(noOp) == false) {
                    try {

                        // 生成一个 noop对应的 doc对象
                        final ParsedDocument tombstone = engineConfig.getTombstoneDocSupplier().newNoopTombstoneDoc(noOp.reason());
                        tombstone.updateSeqID(noOp.seqNo(), noOp.primaryTerm());
                        // A noop tombstone does not require a _version but it's added to have a fully dense docvalues for the version
                        // field. 1L is selected to optimize the compression because it might probably be the most common value in
                        // version field.
                        // noop操作的 version总是1
                        tombstone.version().setLongValue(1L);
                        assert tombstone.docs().size() == 1 : "Tombstone should have a single doc [" + tombstone + "]";

                        final ParseContext.Document doc = tombstone.docs().get(0);
                        assert doc.getField(SeqNoFieldMapper.TOMBSTONE_NAME) != null
                            : "Noop tombstone document but _tombstone field is not set [" + doc + " ]";
                        // 从目前的逻辑来看  index/delete/noop 3种操作都会写入软删除的field
                        doc.add(softDeletesField);
                        // 将新的doc 写入到lucene中
                        indexWriter.addDocument(doc);
                    } catch (final Exception ex) {
                        /*
                         * Document level failures when adding a no-op are unexpected, we likely hit something fatal such as the Lucene
                         * index being corrupt, or the Lucene document limit. We have already issued a sequence number here so this is
                         * fatal, fail the engine.
                         */
                        if (ex instanceof AlreadyClosedException == false && indexWriter.getTragicException() == null) {
                            failEngine("no-op origin[" + noOp.origin() + "] seq#[" + noOp.seqNo() + "] failed at document level", ex);
                        }
                        throw ex;
                    }
                }
                noOpResult = new NoOpResult(noOp.primaryTerm(), noOp.seqNo());
                // 不是通过事务日志还原的操作 比如fillGap操作就是发起 Origin.Primary 的操作
                // 如果是从事务日志还原的操作 自然就不用写回到事务日志中了
                if (noOp.origin().isFromTranslog() == false && noOpResult.getResultType() == Result.Type.SUCCESS) {
                    final Translog.Location location = translog.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
                    // location 是该operation对应的描述数据在事务日志中的偏移量信息
                    noOpResult.setTranslogLocation(location);
                }
            }
            // 当某个操作被写入到事务文件后  processedCheckpoint 就会同步到对应的seq
            localCheckpointTracker.markSeqNoAsProcessed(noOpResult.getSeqNo());
            // 代表本次操作就是通过事务文件中已经存在的数据执行的   这时更新 persistedCheckpoint
            if (noOpResult.getTranslogLocation() == null) {
                // the op is coming from the translog (and is hence persisted already) or it does not have a sequence number
                assert noOp.origin().isFromTranslog() || noOpResult.getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO;
                localCheckpointTracker.markSeqNoAsPersisted(noOpResult.getSeqNo());
            }
            noOpResult.setTook(System.nanoTime() - noOp.startTime());
            noOpResult.freeze();
            return noOpResult;
        }
    }

    /**
     * Executes a pre-flight check for a given NoOp.
     * If this method returns a non-empty result, the engine won't process this NoOp and returns a failure.
     */
    protected Optional<Exception> preFlightCheckForNoOp(final NoOp noOp) throws IOException {
        return Optional.empty();
    }

    @Override
    public void refresh(String source) throws EngineException {
        refresh(source, SearcherScope.EXTERNAL, true);
    }

    /**
     * 尝试进行刷新 如果抢占锁失败就不刷新了
     * @param source
     * @return
     * @throws EngineException
     */
    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        return refresh(source, SearcherScope.EXTERNAL, false);
    }

    /**
     * 发起刷新请求   当发现lucene内部有未持久化的数据 会间接触发lucene的刷盘
     * @param source
     * @param scope  由内部发起还是外部发起  不同的scope会使用不同的readerManager
     * @param block  是否需要阻塞
     * @return
     * @throws EngineException
     */
    final boolean refresh(String source, SearcherScope scope, boolean block) throws EngineException {
        // both refresh types will result in an internal refresh but only the external will also
        // pass the new reader reference to the external reader manager.
        // 获取当前已经处理好的检查点
        final long localCheckpointBeforeRefresh = localCheckpointTracker.getProcessedCheckpoint();
        boolean refreshed;
        try {
            // refresh does not need to hold readLock as ReferenceManager can handle correctly if the engine is closed in mid-way.
            if (store.tryIncRef()) {
                // increment the ref just to ensure nobody closes the store during a refresh
                try {
                    // even though we maintain 2 managers we really do the heavy-lifting only once.
                    // the second refresh will only do the extra work we have to do for warming caches etc.
                    // 获取匹配的 readerManager对象
                    ReferenceManager<ElasticsearchDirectoryReader> referenceManager = getReferenceManager(scope);
                    // it is intentional that we never refresh both internal / external together
                    // block 尽可能获取最新的数据  即使阻塞一段时间
                    if (block) {
                        referenceManager.maybeRefreshBlocking();
                        refreshed = true;
                    } else {
                        // 代表获取最新数据 如果此时竞争激烈 则放弃
                        refreshed = referenceManager.maybeRefresh();
                    }
                } finally {
                    store.decRef();
                }
                // lastRefreshedCheckpointListener 本身也被注册在 referenceManager中 但是如果没有刷新成功是不会触发updateRefreshedCheckpoint的
                // 在这里手动触发
                if (refreshed) {
                    lastRefreshedCheckpointListener.updateRefreshedCheckpoint(localCheckpointBeforeRefresh);
                }
            } else {
                refreshed = false;
            }
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("refresh failed source[" + source + "]", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, e);
        }
        assert refreshed == false || lastRefreshedCheckpoint() >= localCheckpointBeforeRefresh : "refresh checkpoint was not advanced; " +
            "local_checkpoint=" + localCheckpointBeforeRefresh + " refresh_checkpoint=" + lastRefreshedCheckpoint();
        // TODO: maybe we should just put a scheduled job in threadPool?
        // We check for pruning in each delete request, but we also prune here e.g. in case a delete burst comes in and then no more deletes
        // for a long time:
        // 清除 VersionMap 中的墓碑信息
        maybePruneDeletes();
        // 更新merge对象内部的属性
        mergeScheduler.refreshConfig();
        return refreshed;
    }


    /**
     * 每当执行一个 operation时 会将数据存储在lucene管理的内存中  当超过一定值时 就会触发刷盘
     * refresh 实际上会间接触发 lucene.flush
     * @throws EngineException
     */
    @Override
    public void writeIndexingBuffer() throws EngineException {
        refresh("write indexing buffer", SearcherScope.INTERNAL, false);
    }

    /**
     * 刷盘是异步的 或者通过req触发
     * 检测是否需要定时触发刷盘
     * @return
     */
    @Override
    public boolean shouldPeriodicallyFlush() {
        ensureOpen();
        // 代表当发生了大块的数据合并后 需要做刷盘操作
        if (shouldPeriodicallyFlushAfterBigMerge.get()) {
            return true;
        }
        // 上次刷盘对应的检查点  检查从哪个文件开始刷盘 就是只要检查点要高于该值
        final long localCheckpointOfLastCommit =
            Long.parseLong(lastCommittedSegmentInfos.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
        // 检查点对应的事务文件的gen  每当开启一个新的事务文件 gen+1
        final long translogGenerationOfLastCommit =
            translog.getMinGenerationForSeqNo(localCheckpointOfLastCommit + 1).translogFileGeneration;
        // 达到该值时才推荐刷盘 是为了避免由于刷盘过于频繁导致太高的性能开销
        final long flushThreshold = config().getIndexSettings().getFlushThresholdSize().getBytes();
        if (translog.sizeInBytesByMinGen(translogGenerationOfLastCommit) < flushThreshold) {
            return false;
        }
        /*
         * We flush to reduce the size of uncommitted translog but strictly speaking the uncommitted size won't always be
         * below the flush-threshold after a flush. To avoid getting into an endless loop of flushing, we only enable the
         * periodically flush condition if this condition is disabled after a flush. The condition will change if the new
         * commit points to the later generation the last commit's(eg. gen-of-last-commit < gen-of-new-commit)[1].
         *
         * When the local checkpoint equals to max_seqno, and translog-gen of the last commit equals to translog-gen of
         * the new commit, we know that the last generation must contain operations because its size is above the flush
         * threshold and the flush-threshold is guaranteed to be higher than an empty translog by the setting validation.
         * This guarantees that the new commit will point to the newly rolled generation. In fact, this scenario only
         * happens when the generation-threshold is close to or above the flush-threshold; otherwise we have rolled
         * generations as the generation-threshold was reached, then the first condition (eg. [1]) is already satisfied.
         *
         * This method is to maintain translog only, thus IndexWriter#hasUncommittedChanges condition is not considered.
         * 过了上面有关偏移量的限制
         * 判断当前处理到的seqNo 是否比之前的gen 大
         */
        final long translogGenerationOfNewCommit =
            translog.getMinGenerationForSeqNo(localCheckpointTracker.getProcessedCheckpoint() + 1).translogFileGeneration;
        return translogGenerationOfLastCommit < translogGenerationOfNewCommit
            // 这个条件应该总是满足的吧
            || localCheckpointTracker.getProcessedCheckpoint() == localCheckpointTracker.getMaxSeqNo();
    }

    /**
     * 将当前内存中的数据写入到磁盘
     * @param force         if <code>true</code> a lucene commit is executed even if no changes need to be committed.
     * @param waitIfOngoing if <code>true</code> this call will block until all currently running flushes have finished.
     * @throws EngineException
     */
    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        ensureOpen();
        // 如果强制刷盘 那么需要等待的时候 就必须等待
        if (force && waitIfOngoing == false) {
            assert false : "wait_if_ongoing must be true for a force flush: force=" + force + " wait_if_ongoing=" + waitIfOngoing;
            throw new IllegalArgumentException(
                "wait_if_ongoing must be true for a force flush: force=" + force + " wait_if_ongoing=" + waitIfOngoing);
        }
        /*
         * Unfortunately the lock order is important here. We have to acquire the readlock first otherwise
         * if we are flushing at the end of the recovery while holding the write lock we can deadlock if:
         *  Thread 1: flushes via API and gets the flush lock but blocks on the readlock since Thread 2 has the writeLock
         *  Thread 2: flushes at the end of the recovery holding the writeLock and blocks on the flushLock owned by Thread 1
         */
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (flushLock.tryLock() == false) {
                // if we can't get the lock right away we block if needed otherwise barf
                // 对应非强制场景 抢占锁失败 直接返回
                if (waitIfOngoing == false) {
                    return;
                }
                logger.trace("waiting for in-flight flush to finish");
                flushLock.lock();
                logger.trace("acquired flush lock after blocking");
            } else {
                logger.trace("acquired flush lock immediately");
            }
            try {
                // Only flush if (1) Lucene has uncommitted docs, or (2) forced by caller, or (3) the
                // newly created commit points to a different translog generation (can free translog)
                // 代表lucene中存在未持久化的数据
                boolean hasUncommittedChanges = indexWriter.hasUncommittedChanges();
                // 判断是否满足刷盘条件
                boolean shouldPeriodicallyFlush = shouldPeriodicallyFlush();
                if (hasUncommittedChanges || force || shouldPeriodicallyFlush) {
                    ensureCanFlush();
                    try {
                        // 每当触发一次提交就要先滚动到下一个事务文件  在滚动前也会触发事务日志的刷盘
                        translog.rollGeneration();
                        logger.trace("starting commit for flush; commitTranslog=true");
                        // 将此时的一些版本号 seq信息作为userData 传入到IndexWriter中 并执行commit
                        commitIndexWriter(indexWriter, translog);
                        logger.trace("finished commit for flush");

                        // a temporary debugging to investigate test failure - issue#32827. Remove when the issue is resolved
                        logger.debug("new commit on flush, hasUncommittedChanges:{}, force:{}, shouldPeriodicallyFlush:{}",
                            hasUncommittedChanges, force, shouldPeriodicallyFlush);

                        // we need to refresh in order to clear older version values
                        // 更新reader对象
                        refresh("version_table_flush", SearcherScope.INTERNAL, true);
                        translog.trimUnreferencedReaders();
                    } catch (AlreadyClosedException e) {
                        failOnTragicEvent(e);
                        throw e;
                    } catch (Exception e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                    // 更新最新的 segmentInfos信息
                    refreshLastCommittedSegmentInfos();

                }
            } catch (FlushFailedEngineException ex) {
                maybeFailEngine("flush", ex);
                throw ex;
            } finally {
                flushLock.unlock();
            }
        }
        // We don't have to do this here; we do it defensively to make sure that even if wall clock time is misbehaving
        // (e.g., moves backwards) we will at least still sometimes prune deleted tombstones:
        // 检测是否有需要移除的 墓碑信息
        if (engineConfig.isEnableGcDeletes()) {
            pruneDeletedTombstones();
        }
    }

    /**
     * 重新读取最大的 segment_N 对应的段文件
     */
    private void refreshLastCommittedSegmentInfos() {
    /*
     * we have to inc-ref the store here since if the engine is closed by a tragic event
     * we don't acquire the write lock and wait until we have exclusive access. This might also
     * dec the store reference which can essentially close the store and unless we can inc the reference
     * we can't use it.
     */
        store.incRef();
        try {
            // reread the last committed segment infos
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        } catch (Exception e) {
            if (isClosed.get() == false) {
                try {
                    logger.warn("failed to read latest segment infos on flush", e);
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                }
                if (Lucene.isCorruptionException(e)) {
                    throw new FlushFailedEngineException(shardId, e);
                }
            }
        } finally {
            store.decRef();
        }
    }

    /**
     * 切换到下一个事务文件 并尝试丢弃旧的文件
     * @throws EngineException
     */
    @Override
    public void rollTranslogGeneration() throws EngineException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            translog.rollGeneration();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to roll translog", e);
        }
    }

    /**
     * 该方法会在定时器内触发
     * @throws EngineException
     */
    @Override
    public void trimUnreferencedTranslogFiles() throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to trim translog", e);
        }
    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return getTranslog().shouldRollGeneration();
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            translog.trimOperations(belowTerm, aboveSeqNo);
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog operations trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to trim translog operations", e);
        }
    }

    /**
     * 删除墓碑数据   当处理delete请求时会生成墓碑数据
     */
    private void pruneDeletedTombstones() {
        /*
         * We need to deploy two different trimming strategies for GC deletes on primary and replicas. Delete operations on primary
         * are remembered for at least one GC delete cycle and trimmed periodically. This is, at the moment, the best we can do on
         * primary for user facing APIs but this arbitrary time limit is problematic for replicas. On replicas however we should
         * trim only deletes whose seqno at most the local checkpoint. This requirement is explained as follows.
         *
         * Suppose o1 and o2 are two operations on the same document with seq#(o1) < seq#(o2), and o2 arrives before o1 on the replica.
         * o2 is processed normally since it arrives first; when o1 arrives it should be discarded:
         * - If seq#(o1) <= LCP, then it will be not be added to Lucene, as it was already previously added.
         * - If seq#(o1)  > LCP, then it depends on the nature of o2:
         *   *) If o2 is a delete then its seq# is recorded in the VersionMap, since seq#(o2) > seq#(o1) > LCP,
         *      so a lookup can find it and determine that o1 is stale.
         *   *) If o2 is an indexing then its seq# is either in Lucene (if refreshed) or the VersionMap (if not refreshed yet),
         *      so a real-time lookup can find it and determine that o1 is stale.
         *
         * Here we prefer to deploy a single trimming strategy, which satisfies two constraints, on both primary and replicas because:
         * - It's simpler - no need to distinguish if an engine is running at primary mode or replica mode or being promoted.
         * - If a replica subsequently is promoted, user experience is maintained as that replica remembers deletes for the last GC cycle.
         *
         * However, the version map may consume less memory if we deploy two different trimming strategies for primary and replicas.
         */
        final long timeMSec = engineConfig.getThreadPool().relativeTimeInMillis();
        // 只能删除该时间戳之前的数据
        final long maxTimestampToPrune = timeMSec - engineConfig.getIndexSettings().getGcDeletesInMillis();
        // 从墓碑中移除 过期的版本数据
        versionMap.pruneTombstones(maxTimestampToPrune, localCheckpointTracker.getProcessedCheckpoint());
        lastDeleteVersionPruneTimeMSec = timeMSec;
    }

    // testing
    void clearDeletedTombstones() {
        versionMap.pruneTombstones(Long.MAX_VALUE, localCheckpointTracker.getMaxSeqNo());
    }

    // for testing
    final Map<BytesRef, VersionValue> getVersionMap() {
        return Stream.concat(versionMap.getAllCurrent().entrySet().stream(), versionMap.getAllTombstones().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * 强制触发merge逻辑
     * @param flush
     * @param maxNumSegments
     * @param onlyExpungeDeletes
     * @param upgrade
     * @param upgradeOnlyAncientSegments
     * @param forceMergeUUID
     * @throws EngineException
     * @throws IOException
     */
    @Override
    public void forceMerge(final boolean flush, int maxNumSegments, boolean onlyExpungeDeletes,
                           final boolean upgrade, final boolean upgradeOnlyAncientSegments,
                           final String forceMergeUUID) throws EngineException, IOException {
        if (onlyExpungeDeletes && maxNumSegments >= 0) {
            throw new IllegalArgumentException("only_expunge_deletes and max_num_segments are mutually exclusive");
        }
        /*
         * We do NOT acquire the readlock here since we are waiting on the merges to finish
         * that's fine since the IW.rollback should stop all the threads and trigger an IOException
         * causing us to fail the forceMerge
         *
         * The way we implement upgrades is a bit hackish in the sense that we set an instance
         * variable and that this setting will thus apply to the next forced merge that will be run.
         * This is ok because (1) this is the only place we call forceMerge, (2) we have a single
         * thread for optimize, and the 'optimizeLock' guarding this code, and (3) ConcurrentMergeScheduler
         * syncs calls to findForcedMerges.
         */
        assert indexWriter.getConfig().getMergePolicy() instanceof ElasticsearchMergePolicy : "MergePolicy is " +
            indexWriter.getConfig().getMergePolicy().getClass().getName();
        // 获取使用的merge策略
        ElasticsearchMergePolicy mp = (ElasticsearchMergePolicy) indexWriter.getConfig().getMergePolicy();
        optimizeLock.lock();
        try {
            ensureOpen();
            if (upgrade) {
                logger.info("starting segment upgrade upgradeOnlyAncientSegments={}", upgradeOnlyAncientSegments);
                mp.setUpgradeInProgress(true, upgradeOnlyAncientSegments);
            }
            store.incRef(); // increment the ref just to ensure nobody closes the store while we optimize
            try {
                if (onlyExpungeDeletes) {
                    assert upgrade == false;
                    indexWriter.forceMergeDeletes(true /* blocks and waits for merges*/);
                } else if (maxNumSegments <= 0) {
                    assert upgrade == false;
                    indexWriter.maybeMerge();
                } else {
                    indexWriter.forceMerge(maxNumSegments, true /* blocks and waits for merges*/);
                    this.forceMergeUUID = forceMergeUUID;
                }
                if (flush) {
                    flush(false, true);
                }
                if (upgrade) {
                    logger.info("finished segment upgrade");
                }
            } finally {
                store.decRef();
            }
        } catch (AlreadyClosedException ex) {
            /* in this case we first check if the engine is still open. If so this exception is just fine
             * and expected. We don't hold any locks while we block on forceMerge otherwise it would block
             * closing the engine as well. If we are not closed we pass it on to failOnTragicEvent which ensures
             * we are handling a tragic even exception here */
            ensureOpen(ex);
            failOnTragicEvent(ex);
            throw ex;
        } catch (Exception e) {
            try {
                maybeFailEngine("force merge", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        } finally {
            try {
                // reset it just to make sure we reset it in a case of an error
                mp.setUpgradeInProgress(false, false);
            } finally {
                optimizeLock.unlock();
            }
        }
    }

    /**
     * 获取此时lucene中最新一次提交的相关信息
     * @param flushFirst indicates whether the engine should flush before returning the snapshot
     * @return
     * @throws EngineException
     */
    @Override
    public IndexCommitRef acquireLastIndexCommit(final boolean flushFirst) throws EngineException {
        // we have to flush outside of the readlock otherwise we might have a problem upgrading
        // the to a write lock when we fail the engine in this operation
        if (flushFirst) {
            logger.trace("start flush for snapshot");
            flush(false, true);
            logger.trace("finish flush for snapshot");
        }
        // 这里要获取的是最后一个commit
        final IndexCommit lastCommit = combinedDeletionPolicy.acquireIndexCommit(false);
        // 当indexCommit信息使用完毕后 会从快照容器中移除
        return new Engine.IndexCommitRef(lastCommit, () -> releaseIndexCommit(lastCommit));
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        final IndexCommit safeCommit = combinedDeletionPolicy.acquireIndexCommit(true);
        return new Engine.IndexCommitRef(safeCommit, () -> releaseIndexCommit(safeCommit));
    }

    /**
     * 代表某个快照不再使用 就是从内部快照容器中移除
     * @param snapshot
     * @throws IOException
     */
    private void releaseIndexCommit(IndexCommit snapshot) throws IOException {
        // Revisit the deletion policy if we can clean up the snapshotting commit.
        if (combinedDeletionPolicy.releaseCommit(snapshot)) {
            ensureOpen();
            // Here we don't have to trim translog because snapshotting an index commit
            // does not lock translog or prevents unreferenced files from trimming.
            // 因为之前可能由于快照持有了某些commit无法被释放  现在就可以进行释放了
            indexWriter.deleteUnusedFiles();
        }
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return combinedDeletionPolicy.getSafeCommitInfo();
    }

    private boolean failOnTragicEvent(AlreadyClosedException ex) {
        final boolean engineFailed;
        // if we are already closed due to some tragic exception
        // we need to fail the engine. it might have already been failed before
        // but we are double-checking it's failed and closed
        if (indexWriter.isOpen() == false && indexWriter.getTragicException() != null) {
            final Exception tragicException;
            if (indexWriter.getTragicException() instanceof Exception) {
                tragicException = (Exception) indexWriter.getTragicException();
            } else {
                tragicException = new RuntimeException(indexWriter.getTragicException());
            }
            failEngine("already closed by tragic event on the index writer", tragicException);
            engineFailed = true;
        } else if (translog.isOpen() == false && translog.getTragicException() != null) {
            failEngine("already closed by tragic event on the translog", translog.getTragicException());
            engineFailed = true;
        } else if (failedEngine.get() == null && isClosed.get() == false) { // we are closed but the engine is not failed yet?
            // this smells like a bug - we only expect ACE if we are in a fatal case ie. either translog or IW is closed by
            // a tragic event or has closed itself. if that is not the case we are in a buggy state and raise an assertion error
            throw new AssertionError("Unexpected AlreadyClosedException", ex);
        } else {
            engineFailed = false;
        }
        return engineFailed;
    }

    /**
     * 检测 engine 是否应该被关闭
     * @param source
     * @param e
     * @return
     */
    @Override
    protected boolean maybeFailEngine(String source, Exception e) {
        boolean shouldFail = super.maybeFailEngine(source, e);
        if (shouldFail) {
            return true;
        }
        // Check for AlreadyClosedException -- ACE is a very special
        // exception that should only be thrown in a tragic event. we pass on the checks to failOnTragicEvent which will
        // throw and AssertionError if the tragic event condition is not met.
        if (e instanceof AlreadyClosedException) {
            return failOnTragicEvent((AlreadyClosedException)e);
        } else if (e != null &&
                ((indexWriter.isOpen() == false && indexWriter.getTragicException() == e)
                        || (translog.isOpen() == false && translog.getTragicException() == e))) {
            // this spot on - we are handling the tragic event exception here so we have to fail the engine
            // right away
            failEngine(source, e);
            return true;
        }
        return false;
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    protected final void writerSegmentStats(SegmentsStats stats) {
        stats.addVersionMapMemoryInBytes(versionMap.ramBytesUsed());
        stats.addIndexWriterMemoryInBytes(indexWriter.ramBytesUsed());
        stats.updateMaxUnsafeAutoIdTimestamp(maxUnsafeAutoIdTimestamp.get());
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        // We don't guard w/ readLock here, so we could throw AlreadyClosedException
        return indexWriter.ramBytesUsed() + versionMap.ramBytesUsedForRefresh();
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        try (ReleasableLock lock = readLock.acquire()) {
            Segment[] segmentsArr = getSegmentInfo(lastCommittedSegmentInfos, verbose);

            // fill in the merges flag
            Set<OnGoingMerge> onGoingMerges = mergeScheduler.onGoingMerges();
            for (OnGoingMerge onGoingMerge : onGoingMerges) {
                for (SegmentCommitInfo segmentInfoPerCommit : onGoingMerge.getMergedSegments()) {
                    for (Segment segment : segmentsArr) {
                        if (segment.getName().equals(segmentInfoPerCommit.info.name)) {
                            segment.mergeId = onGoingMerge.getId();
                            break;
                        }
                    }
                }
            }
            return Arrays.asList(segmentsArr);
        }
    }

    /**
     * Closes the engine without acquiring the write lock. This should only be
     * called while the write lock is hold or in a disaster condition ie. if the engine
     * is failed.
     */
    @Override
    protected final void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread() :
                "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                this.versionMap.clear();
                if (internalReaderManager != null) {
                    internalReaderManager.removeListener(versionMap);
                }
                try {
                    IOUtils.close(externalReaderManager, internalReaderManager);
                } catch (Exception e) {
                    logger.warn("Failed to close ReaderManager", e);
                }
                try {
                    IOUtils.close(translog);
                } catch (Exception e) {
                    logger.warn("Failed to close translog", e);
                }
                // no need to commit in this case!, we snapshot before we close the shard, so translog and all sync'ed
                logger.trace("rollback indexWriter");
                try {
                    indexWriter.rollback();
                } catch (AlreadyClosedException ex) {
                    failOnTragicEvent(ex);
                    throw ex;
                }
                logger.trace("rollback indexWriter done");
            } catch (Exception e) {
                logger.warn("failed to rollback writer on close", e);
            } finally {
                try {
                    store.decRef();
                    logger.debug("engine closed [{}]", reason);
                } finally {
                    closedLatch.countDown();
                }
            }
        }
    }

    @Override
    protected final ReferenceManager<ElasticsearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        switch (scope) {
            case INTERNAL:
                return internalReaderManager;
            case EXTERNAL:
                return externalReaderManager;
            default:
                throw new IllegalStateException("unknown scope: " + scope);
        }
    }

    /**
     * 初始化indexWriter
     * @return
     * @throws IOException
     */
    private IndexWriter createWriter() throws IOException {
        try {
            final IndexWriterConfig iwc = getIndexWriterConfig();
            // 使用该配置项生成 IndexWriter
            return createWriter(store.directory(), iwc);
        } catch (LockObtainFailedException ex) {
            logger.warn("could not lock IndexWriter", ex);
            throw ex;
        }
    }

    // pkg-private for testing
    IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
        if (Assertions.ENABLED) {
            return new AssertingIndexWriter(directory, iwc);
        } else {
            return new IndexWriter(directory, iwc);
        }
    }

    /**
     * 获取 DirectoryReader的相关配置
     * @param directory
     * @param indexSettings
     * @return
     */
    static Map<String, String> getReaderAttributes(Directory directory, IndexSettings indexSettings) {
        Directory unwrap = FilterDirectory.unwrap(directory);
        // 使用基于heap的目录
        boolean defaultOffHeap = FsDirectoryFactory.isHybridFs(unwrap) || unwrap instanceof MMapDirectory;
        Map<String, String> attributes = new HashMap<>();
        // 是否要将FST 装载到内存中 默认是true吧
        attributes.put(BlockTreeTermsReader.FST_MODE_KEY, defaultOffHeap ? FSTLoadMode.OFF_HEAP.name() : FSTLoadMode.ON_HEAP.name());
        if (IndexSettings.ON_HEAP_ID_TERMS_INDEX.exists(indexSettings.getSettings())) {
            final boolean idOffHeap = IndexSettings.ON_HEAP_ID_TERMS_INDEX.get(indexSettings.getSettings()) == false;
            attributes.put(BlockTreeTermsReader.FST_MODE_KEY + "." + IdFieldMapper.NAME,
                    idOffHeap ? FSTLoadMode.OFF_HEAP.name() : FSTLoadMode.ON_HEAP.name());
        }
        return Collections.unmodifiableMap(attributes);
    }

    /**
     * 获取相关配置
     * @return
     */
    private IndexWriterConfig getIndexWriterConfig() {
        // 一开始的分词器是通过插件系统加载的  也有默认的分词器 这里就是利用分词器来配置config对象
        final IndexWriterConfig iwc = new IndexWriterConfig(engineConfig.getAnalyzer());
        // 在close时 不需要自动提交  因为要考虑到在集群间的同步
        iwc.setCommitOnClose(false); // we by default don't commit on close
        // 采用 append模式
        iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        // 设置reader相关的参数
        iwc.setReaderAttributes(getReaderAttributes(store.directory(), engineConfig.getIndexSettings()));
        // 指定删除策略  原本默认的删除策略是KeepOnlyLastCommitDeletionPolicy
        iwc.setIndexDeletionPolicy(combinedDeletionPolicy);
        // with tests.verbose, lucene sets this up: plumb to align with filesystem stream
        boolean verbose = false;
        try {
            verbose = Boolean.parseBoolean(System.getProperty("tests.verbose"));
        } catch (Exception ignore) {
        }
        iwc.setInfoStream(verbose ? InfoStream.getDefault() : new LoggerInfoStream(logger));
        // 默认merge策略是 ConcurrentMergeScheduler    这里设置ES封装的merge策略(做了增强)
        iwc.setMergeScheduler(mergeScheduler);
        // Give us the opportunity to upgrade old segments while performing
        // background merges
        // 这个merge策略也是 ES增强过的  对应EsTieredMergePolicy
        MergePolicy mergePolicy = config().getMergePolicy();
        // always configure soft-deletes field so an engine with soft-deletes disabled can open a Lucene index with soft-deletes.
        // 指定软删除字段为 "_soft_deletes"
        iwc.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        // RecoverySourcePruneMergePolicy 负责处理 recovery_source 相关的
        // TODO 这个merge策略先不看 等回顾lucene的merge流程后重看
        mergePolicy = new RecoverySourcePruneMergePolicy(SourceFieldMapper.RECOVERY_SOURCE_NAME, softDeletesPolicy::getRetentionQuery,
            // 这个是保留软删除字段的merge策略 也就是 在merge过程中软删除字段不会被丢弃
            new SoftDeletesRetentionMergePolicy(Lucene.SOFT_DELETES_FIELD, softDeletesPolicy::getRetentionQuery,
                // 最基础的merge策略先是被这个对象包装 当发现field为_id时需要做特殊处理
                new PrunePostingsMergePolicy(mergePolicy, IdFieldMapper.NAME)));

        // 是否打乱merge
        boolean shuffleForcedMerge = Booleans.parseBoolean(System.getProperty("es.shuffle_forced_merge", Boolean.TRUE.toString()));
        if (shuffleForcedMerge) {
            // We wrap the merge policy for all indices even though it is mostly useful for time-based indices
            // but there should be no overhead for other type of indices so it's simpler than adding a setting
            // to enable it.
            // 如果要打乱的话 还要再包装一层
            mergePolicy = new ShuffleForcedMergePolicy(mergePolicy);
        }
        // ElasticsearchMergePolicy 主要是解决兼容性问题的
        iwc.setMergePolicy(new ElasticsearchMergePolicy(mergePolicy));
        // 打分策略先不管了
        iwc.setSimilarity(engineConfig.getSimilarity());
        iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().getMbFrac());
        iwc.setCodec(engineConfig.getCodec());
        // TODO 复合文件先忽略
        iwc.setUseCompoundFile(true); // always use compound on flush - reduces # of file-handles on refresh
        // 设置排序对象后 doc将会按照排序规则进行存储
        if (config().getIndexSort() != null) {
            iwc.setIndexSort(config().getIndexSort());
        }
        return iwc;
    }

    /**
     * A listener that warms the segments if needed when acquiring a new reader
     * 当获取到一个最新的reader对象时 立即进行预热
     * 该对象是针对 externalReaderManager的监听器
     */
    static final class RefreshWarmerListener implements BiConsumer<ElasticsearchDirectoryReader, ElasticsearchDirectoryReader> {
        private final Engine.Warmer warmer;
        private final Logger logger;
        private final AtomicBoolean isEngineClosed;

        RefreshWarmerListener(Logger logger, AtomicBoolean isEngineClosed, EngineConfig engineConfig) {
            warmer = engineConfig.getWarmer();
            this.logger = logger;
            this.isEngineClosed = isEngineClosed;
        }

        @Override
        public void accept(ElasticsearchDirectoryReader reader, ElasticsearchDirectoryReader previousReader) {
            if (warmer != null) {
                try {
                    warmer.warm(reader);
                } catch (Exception e) {
                    if (isEngineClosed.get() == false) {
                        logger.warn("failed to prepare/warm", e);
                    }
                }
            }
        }
    }

    /**
     * 开启阀门
     */
    @Override
    public void activateThrottling() {
        int count = throttleRequestCount.incrementAndGet();
        assert count >= 1 : "invalid post-increment throttleRequestCount=" + count;
        if (count == 1) {
            throttle.activate();
        }
    }

    @Override
    public void deactivateThrottling() {
        int count = throttleRequestCount.decrementAndGet();
        assert count >= 0 : "invalid post-decrement throttleRequestCount=" + count;
        if (count == 0) {
            throttle.deactivate();
        }
    }

    @Override
    public boolean isThrottled() {
        return throttle.isThrottled();
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return throttle.getThrottleTimeInMillis();
    }

    long getGcDeletesInMillis() {
        return engineConfig.getIndexSettings().getGcDeletesInMillis();
    }

    LiveIndexWriterConfig getCurrentIndexWriterConfig() {
        return indexWriter.getConfig();
    }

    /**
     * 用于并发执行merge任务的对象
     */
    private final class EngineMergeScheduler extends ElasticsearchConcurrentMergeScheduler {
        /**
         * 此时有多少正在merge的任务
         */
        private final AtomicInteger numMergesInFlight = new AtomicInteger(0);
        /**
         * 此时是否处于限流状态  ES 关闭了lucene自带的merge限流器 自己定义了一套限流规则
         */
        private final AtomicBoolean isThrottling = new AtomicBoolean();

        EngineMergeScheduler(ShardId shardId, IndexSettings indexSettings) {
            super(shardId, indexSettings);
        }

        @Override
        public synchronized void beforeMerge(OnGoingMerge merge) {
            // 最多允许多少merge同时进行
            int maxNumMerges = mergeScheduler.getMaxMergeCount();
            if (numMergesInFlight.incrementAndGet() > maxNumMerges) {
                if (isThrottling.getAndSet(true) == false) {
                    logger.info("now throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    // 超出限制 开启阀门
                    activateThrottling();
                }
            }
        }

        /**
         * merge 后执行
         * @param merge
         */
        @Override
        public synchronized void afterMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMergeCount();
            // 一旦少于最大merge线程 马上停止限流
            if (numMergesInFlight.decrementAndGet() < maxNumMerges) {
                if (isThrottling.getAndSet(false)) {
                    logger.info("stop throttling indexing: numMergesInFlight={}, maxNumMerges={}",
                        numMergesInFlight, maxNumMerges);
                    deactivateThrottling();
                }
            }
            if (indexWriter.hasPendingMerges() == false &&
                    System.nanoTime() - lastWriteNanos >= engineConfig.getFlushMergesAfter().nanos()) {
                // NEVER do this on a merge thread since we acquire some locks blocking here and if we concurrently rollback the writer
                // we deadlock on engine#close for instance.
                engineConfig.getThreadPool().executor(ThreadPool.Names.FLUSH).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        if (isClosed.get() == false) {
                            logger.warn("failed to flush after merge has finished");
                        }
                    }

                    @Override
                    protected void doRun() {
                        // if we have no pending merges and we are supposed to flush once merges have finished to
                        // free up transient disk usage of the (presumably biggish) segments that were just merged
                        flush();
                    }
                });
            } else if (merge.getTotalBytesSize() >= engineConfig.getIndexSettings().getFlushAfterMergeThresholdSize().getBytes()) {
                // we hit a significant merge which would allow us to free up memory if we'd commit it hence on the next change
                // we should execute a flush on the next operation if that's a flush after inactive or indexing a document.
                // we could fork a thread and do it right away but we try to minimize forking and piggyback on outside events.
                shouldPeriodicallyFlushAfterBigMerge.set(true);
            }
        }

        @Override
        protected void handleMergeException(final Directory dir, final Throwable exc) {
            engineConfig.getThreadPool().generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("merge failure action rejected", e);
                }

                @Override
                protected void doRun() throws Exception {
                    /*
                     * We do this on another thread rather than the merge thread that we are initially called on so that we have complete
                     * confidence that the call stack does not contain catch statements that would cause the error that might be thrown
                     * here from being caught and never reaching the uncaught exception handler.
                     */
                    failEngine("merge failed", new MergePolicy.MergeException(exc, dir));
                }
            });
        }
    }

    /**
     * Commits the specified index writer.
     *
     * @param writer   the index writer to commit
     * @param translog the translog
     *                 针对lucene内的数据进行持久化
     */
    protected void commitIndexWriter(final IndexWriter writer, final Translog translog) throws IOException {
        ensureCanFlush();
        try {
            // 提交点就对应此时写入到内存中最新的operation对应的seq
            // 可以看到这里没有做并发处理 也就是获取到的seq 可能不是最新的 不过也只是有部分最新数据没有做持久化 不影响
            final long localCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
            // 在对lucene数据进行持久化前 会将此时一些信息记录到userData中
            writer.setLiveCommitData(() -> {
                /*
                 * The user data captured above (e.g. local checkpoint) contains data that must be evaluated *before* Lucene flushes
                 * segments, including the local checkpoint amongst other values. The maximum sequence number is different, we never want
                 * the maximum sequence number to be less than the last sequence number to go into a Lucene commit, otherwise we run the
                 * risk of re-using a sequence number for two different documents when restoring from this commit point and subsequently
                 * writing new documents to the index. Since we only know which Lucene documents made it into the final commit after the
                 * {@link IndexWriter#commit()} call flushes all documents, we defer computation of the maximum sequence number to the time
                 * of invocation of the commit data iterator (which occurs after all documents have been flushed to Lucene).
                 */
                final Map<String, String> commitData = new HashMap<>(7);

                // 设置当前刷盘对应的事务日志的id
                commitData.put(Translog.TRANSLOG_UUID_KEY, translog.getTranslogUUID());
                // 对应本次commit的数据中 最新的operation的seq
                commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCheckpoint));
                // 此时观测到的最大的seq 因为触发该方法时 可能又有新的operation需要处理了 而它们会更新这个maxSeq  TODO 但是这个值存储起来的意义是什么 ???
                commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(localCheckpointTracker.getMaxSeqNo()));

                // 某些index操作可能会记录一个 maxUnsafeAutoIdTimestamp  那么在处理时可能就会更新到userData中
                commitData.put(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp.get()));

                // 存储此时的historyUUID TODO 这个值的作用是什么???
                commitData.put(HISTORY_UUID_KEY, historyUUID);
                // force_merge 也是原封不动的写回到userData中 这个值的作用是???
                final String currentForceMergeUUID = forceMergeUUID;
                if (currentForceMergeUUID != null) {
                    commitData.put(FORCE_MERGE_UUID_KEY, currentForceMergeUUID);
                }

                // 获取此时需要保留的最小的seq 并存储到userData中
                commitData.put(Engine.MIN_RETAINED_SEQNO, Long.toString(softDeletesPolicy.getMinRetainedSeqNo()));
                logger.trace("committing writer with commit data [{}]", commitData);
                return commitData.entrySet().iterator();
            });
            // 因为此时即将进行刷盘 为了避免重复刷盘就将该标识设置为false
            shouldPeriodicallyFlushAfterBigMerge.set(false);
            writer.commit();
        } catch (final Exception ex) {
            try {
                failEngine("lucene commit failed", ex);
            } catch (final Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        } catch (final AssertionError e) {
            /*
             * If assertions are enabled, IndexWriter throws AssertionError on commit if any files don't exist, but tests that randomly
             * throw FileNotFoundException or NoSuchFileException can also hit this.
             */
            if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                final EngineException engineException = new EngineException(shardId, "failed to commit engine", e);
                try {
                    failEngine("lucene commit failed", engineException);
                } catch (final Exception inner) {
                    engineException.addSuppressed(inner);
                }
                throw engineException;
            } else {
                throw e;
            }
        }
    }

    final void ensureCanFlush() {
        // translog recovery happens after the engine is fully constructed.
        // If we are in this stage we have to prevent flushes from this
        // engine otherwise we might loose documents if the flush succeeds
        // and the translog recovery fails when we "commit" the translog on flush.
        if (pendingTranslogRecovery.get()) {
            throw new IllegalStateException(shardId.toString() + " flushes are disabled - pending translog recovery");
        }
    }

    /**
     * 接收到某些配置发生变化的通知 更新内部组件
     */
    @Override
    public void onSettingsChanged() {
        mergeScheduler.refreshConfig();
        // config().isEnableGcDeletes() or config.getGcDeletesInMillis() may have changed:
        // 可能有关GCDeletes的配置项会发生变化
        maybePruneDeletes();
        // 保留数量发生了变化
        softDeletesPolicy.setRetentionOperations(config().getIndexSettings().getSoftDeleteRetentionOperations());
    }

    public MergeStats getMergeStats() {
        return mergeScheduler.stats();
    }

    LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return getTranslog().getLastSyncedGlobalCheckpoint();
    }

    public long getProcessedLocalCheckpoint() {
        return localCheckpointTracker.getProcessedCheckpoint();
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return localCheckpointTracker.getPersistedCheckpoint();
    }

    /**
     * Marks the given seq_no as seen and advances the max_seq_no of this engine to at least that value.
     */
    protected final void markSeqNoAsSeen(long seqNo) {
        localCheckpointTracker.advanceMaxSeqNo(seqNo);
    }

    /**
     * Checks if the given operation has been processed in this engine or not.
     * @return true if the given operation was processed; otherwise false.
     * 检测某个operation是否已经被处理
     */
    protected final boolean hasBeenProcessedBefore(Operation op) {
        if (Assertions.ENABLED) {
            assert op.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "operation is not assigned seq_no";
            if (op.operationType() == Operation.TYPE.NO_OP) {
                assert noOpKeyedLock.isHeldByCurrentThread(op.seqNo());
            } else {
                assert versionMap.assertKeyedLockHeldByCurrentThread(op.uid().bytes());
            }
        }
        return localCheckpointTracker.hasProcessed(op.seqNo());
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return localCheckpointTracker.getStats(globalCheckpoint);
    }

    /**
     * Returns the number of times a version was looked up either from the index.
     * Note this is only available if assertions are enabled
     */
    long getNumIndexVersionsLookups() { // for testing
        return numIndexVersionsLookups.count();
    }

    /**
     * Returns the number of times a version was looked up either from memory or from the index.
     * Note this is only available if assertions are enabled
     */
    long getNumVersionLookups() { // for testing
        return numVersionLookups.count();
    }

    private boolean incrementVersionLookup() { // only used by asserts
        numVersionLookups.inc();
        return true;
    }

    private boolean incrementIndexVersionLookup() {
        numIndexVersionsLookups.inc();
        return true;
    }

    boolean isSafeAccessRequired() {
        return versionMap.isSafeAccessRequired();
    }

    /**
     * Returns the number of documents have been deleted since this engine was opened.
     * This count does not include the deletions from the existing segments before opening engine.
     */
    long getNumDocDeletes() {
        return numDocDeletes.count();
    }

    /**
     * Returns the number of documents have been appended since this engine was opened.
     * This count does not include the appends from the existing segments before opening engine.
     */
    long getNumDocAppends() {
        return numDocAppends.count();
    }

    /**
     * Returns the number of documents have been updated since this engine was opened.
     * This count does not include the updates from the existing segments before opening engine.
     */
    long getNumDocUpdates() {
        return numDocUpdates.count();
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(String source, MapperService mapperService,
                                                long fromSeqNo, long toSeqNo, boolean requiredFullRange) throws IOException {
        ensureOpen();
        refreshIfNeeded(source, toSeqNo);
        Searcher searcher = acquireSearcher(source, SearcherScope.INTERNAL);
        try {
            LuceneChangesSnapshot snapshot = new LuceneChangesSnapshot(
                searcher, mapperService, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE, fromSeqNo, toSeqNo, requiredFullRange);
            searcher = null;
            return snapshot;
        } catch (Exception e) {
            try {
                maybeFailEngine("acquire changes snapshot", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        } finally {
            IOUtils.close(searcher);
        }
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return getMinRetainedSeqNo() <= startingSeqNo;
    }

    /**
     * Returns the minimum seqno that is retained in the Lucene index.
     * Operations whose seq# are at least this value should exist in the Lucene index.
     */
    public final long getMinRetainedSeqNo() {
        return softDeletesPolicy.getMinRetainedSeqNo();
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return softDeletesPolicy.acquireRetentionLock();
    }

    /**
     * Gets the commit data from {@link IndexWriter} as a map.
     */
    private static Map<String, String> commitDataAsMap(final IndexWriter indexWriter) {
        final Map<String, String> commitData = new HashMap<>(8);
        for (Map.Entry<String, String> entry : indexWriter.getLiveCommitData()) {
            commitData.put(entry.getKey(), entry.getValue());
        }
        return commitData;
    }

    private static class AssertingIndexWriter extends IndexWriter {
        AssertingIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
            super(d, conf);
        }

        @Override
        public long updateDocument(Term term, Iterable<? extends IndexableField> doc) {
            throw new AssertionError("must not hard update document");
        }

        @Override
        public long updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs) {
            throw new AssertionError("must not hard update documents");
        }

        @Override
        public long deleteDocuments(Term... terms) {
            throw new AssertionError("must not hard delete documents");
        }

        @Override
        public long tryDeleteDocument(IndexReader readerIn, int docID) {
            throw new AssertionError("tryDeleteDocument is not supported. See Lucene#DirectoryReaderWithAllLiveDocs");
        }
    }

    /**
     * Returned the last local checkpoint value has been refreshed internally.
     */
    final long lastRefreshedCheckpoint() {
        return lastRefreshedCheckpointListener.refreshedCheckpoint.get();
    }


    private final Object refreshIfNeededMutex = new Object();

    /**
     * Refresh this engine **internally** iff the requesting seq_no is greater than the last refreshed checkpoint.
     * 尝试刷新内部的数据
     */
    protected final void refreshIfNeeded(String source, long requestingSeqNo) {
        // lastRefreshedCheckpoint 对应上一次刷新时得到的检查点 如果本次传入的seq比上次检查点大 才有刷新的必要
        if (lastRefreshedCheckpoint() < requestingSeqNo) {
            synchronized (refreshIfNeededMutex) {
                if (lastRefreshedCheckpoint() < requestingSeqNo) {
                    refresh(source, SearcherScope.INTERNAL, true);
                }
            }
        }
    }

    /**
     * 该对象会设置在 internalReaderManager上 监听资源的刷新
     */
    private final class LastRefreshedCheckpointListener implements ReferenceManager.RefreshListener {

        /**
         * 代表最近一次刷新时写入的checkpoint 对应的 processedCheckpoint
         */
        final AtomicLong refreshedCheckpoint;

        private long pendingCheckpoint;

        LastRefreshedCheckpointListener(long initialLocalCheckpoint) {
            this.refreshedCheckpoint = new AtomicLong(initialLocalCheckpoint);
        }


        /**
         * 每次在刷新前都会重新读取  localCheckpointTracker 已处理的检查点
         */
        @Override
        public void beforeRefresh() {
            // all changes until this point should be visible after refresh
            pendingCheckpoint = localCheckpointTracker.getProcessedCheckpoint();
        }

        /**
         * 当刷新流程结束时触发该方法
         * @param didRefresh  是否真的刷新了数据
         */
        @Override
        public void afterRefresh(boolean didRefresh) {
            if (didRefresh) {
                updateRefreshedCheckpoint(pendingCheckpoint);
            }
        }

        /**
         * 当成功刷新时触发该方法
         * @param checkpoint
         */
        void updateRefreshedCheckpoint(long checkpoint) {
            refreshedCheckpoint.updateAndGet(curr -> Math.max(curr, checkpoint));
            assert refreshedCheckpoint.get() >= checkpoint : refreshedCheckpoint.get() + " < " + checkpoint;
        }
    }

    @Override
    public final long getMaxSeenAutoIdTimestamp() {
        return maxSeenAutoIdTimestamp.get();
    }

    @Override
    public final void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {
        updateAutoIdTimestamp(newTimestamp, true);
    }

    /**
     * 更新自动id时间戳
     * @param newTimestamp
     * @param unsafe
     */
    private void updateAutoIdTimestamp(long newTimestamp, boolean unsafe) {
        assert newTimestamp >= -1 : "invalid timestamp [" + newTimestamp + "]";
        maxSeenAutoIdTimestamp.updateAndGet(curr -> Math.max(curr, newTimestamp));
        if (unsafe) {
            maxUnsafeAutoIdTimestamp.updateAndGet(curr -> Math.max(curr, newTimestamp));
        }
        assert maxUnsafeAutoIdTimestamp.get() <= maxSeenAutoIdTimestamp.get();
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return maxSeqNoOfUpdatesOrDeletes.get();
    }

    /**
     * 更新maxSeqNoOfUpdatesOnPrimary
     * @param maxSeqNoOfUpdatesOnPrimary
     */
    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        if (maxSeqNoOfUpdatesOnPrimary == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            assert false : "max_seq_no_of_updates on primary is unassigned";
            throw new IllegalArgumentException("max_seq_no_of_updates on primary is unassigned");
        }
        this.maxSeqNoOfUpdatesOrDeletes.updateAndGet(curr -> Math.max(curr, maxSeqNoOfUpdatesOnPrimary));
    }

    private boolean assertMaxSeqNoOfUpdatesIsAdvanced(Term id, long seqNo, boolean allowDeleted, boolean relaxIfGapInSeqNo) {
        final long maxSeqNoOfUpdates = getMaxSeqNoOfUpdatesOrDeletes();
        // We treat a delete on the tombstones on replicas as a regular document, then use updateDocument (not addDocument).
        if (allowDeleted) {
            final VersionValue versionValue = versionMap.getVersionForAssert(id.bytes());
            if (versionValue != null && versionValue.isDelete()) {
                return true;
            }
        }
        // Operations can be processed on a replica in a different order than on the primary. If the order on the primary is index-1,
        // delete-2, index-3, and the order on a replica is index-1, index-3, delete-2, then the msu of index-3 on the replica is 2
        // even though it is an update (overwrites index-1). We should relax this assertion if there is a pending gap in the seq_no.
        if (relaxIfGapInSeqNo && localCheckpointTracker.getProcessedCheckpoint() < maxSeqNoOfUpdates) {
            return true;
        }
        assert seqNo <= maxSeqNoOfUpdates : "id=" + id + " seq_no=" + seqNo + " msu=" + maxSeqNoOfUpdates;
        return true;
    }

    /**
     * Restores the live version map and local checkpoint of this engine using documents (including soft-deleted)
     * after the local checkpoint in the safe commit. This step ensures the live version map and checkpoint tracker
     * are in sync with the Lucene commit.
     * 通过reader读取doc  并从中还原 version/checkpoint 信息
     */
    private void restoreVersionMapAndCheckpointTracker(DirectoryReader directoryReader) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(directoryReader);
        searcher.setQueryCache(null);
        // BooleanQuery 是多个查询条件累加的结果  这里是获取 本次已经持久化后的checkpoint对应的doc
        final Query query = new BooleanQuery.Builder()
            .add(LongPoint.newRangeQuery(
                    SeqNoFieldMapper.NAME, getPersistedLocalCheckpoint() + 1, Long.MAX_VALUE), BooleanClause.Occur.MUST)
            // doc 必须要包含 _primary_term field
            .add(Queries.newNonNestedFilter(), BooleanClause.Occur.MUST) // exclude non-root nested documents
            .build();
        // 此时weight中已经包含了查询结果了
        final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        // 遍历所有segmentReader
        for (LeafReaderContext leaf : directoryReader.leaves()) {
            // 对应每个segment 查询到的结果
            final Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            // 将 segment下  某些特殊的field相关的所有docValue 汇聚到一个对象中
            final CombinedDocValues dv = new CombinedDocValues(leaf.reader());
            // 该对象专门负责从doc中找到 _id 的field 并将结果存起来
            final IdOnlyFieldVisitor idFieldVisitor = new IdOnlyFieldVisitor();
            final DocIdSetIterator iterator = scorer.iterator();
            int docId;
            // 只要能读取到doc 就不断遍历
            while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                // 获取这个doc 下 primary field 对应的值
                final long primaryTerm = dv.docPrimaryTerm(docId);
                final long seqNo = dv.docSeqNo(docId);

                // 之前是用 userData.maxSeq 来还原 processedSeq/persistedSeq的
                // 现在通过 segmentInfos打开索引文件 并读取内部的docValue 获取记录的最新的seq 更新localCheckpointTracker内的数据
                // 当seq 超过了checkPoint时 会将2者同步
                localCheckpointTracker.markSeqNoAsProcessed(seqNo);
                localCheckpointTracker.markSeqNoAsPersisted(seqNo);
                idFieldVisitor.reset();

                // 处理每个doc时  通过visitor获取id信息
                leaf.reader().document(docId, idFieldVisitor);
                if (idFieldVisitor.getId() == null) {
                    assert dv.isTombstone(docId);
                    continue;
                }
                final BytesRef uid = new Term(IdFieldMapper.NAME, Uid.encodeId(idFieldVisitor.getId())).bytes();
                // 在KeyedLock中 为每个id 维护一个锁对象 在获取的同时会对锁进行lock操作   也就是以id为单位进行操作是线程安全的
                // 在使用完后会自动释放锁
                try (Releasable ignored = versionMap.acquireLock(uid)) {
                    // 从容器中找到该id的版本信息
                    final VersionValue curr = versionMap.getUnderLock(uid);

                    // 当前版本为空 或者本次从 versionValue中获取的seq 比doc解析出来的小    一开始 versionMap中还没有存储任何数据
                    if (curr == null ||
                        compareOpToVersionMapOnSeqNo(idFieldVisitor.getId(), seqNo, primaryTerm, curr) == OpVsLuceneDocStatus.OP_NEWER) {
                        // 如果当前doc携带了 tombstone field 先忽略这种情况
                        if (dv.isTombstone(docId)) {
                            // use 0L for the start time so we can prune this delete tombstone quickly
                            // when the local checkpoint advances (i.e., after a recovery completed).
                            // 传入0的话 就是都可以裁剪
                            final long startTime = 0L;
                            // putDeleteUnderLock 将数据存储到 tombstone 中 并从 maps移除
                            versionMap.putDeleteUnderLock(uid, new DeleteVersionValue(dv.docVersion(docId), seqNo, primaryTerm, startTime));
                        } else {
                            // 为 id 插入一个version信息   内部包含了 doc下 _version 字段的值   还有 _seq_no 的值
                            versionMap.putIndexUnderLock(uid, new IndexVersionValue(null, dv.docVersion(docId), seqNo, primaryTerm));
                        }
                    }
                }
            }
        }
        // remove live entries in the version map
        // 在通过之前segment_N 文件关联的所有索引文件恢复了 seq 以及生成id对应的版本信息后 进行刷新工作
        refresh("restore_version_map_and_checkpoint_tracker", SearcherScope.INTERNAL, true);
    }

}
