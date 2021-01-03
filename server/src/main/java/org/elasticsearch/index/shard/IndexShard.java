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

import static org.elasticsearch.index.seqno.RetentionLeaseActions.RETAIN_ALL;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import com.carrotsearch.hppc.ObjectLongMap;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.support.replication.PendingReplicationActions;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AsyncIOProcessor;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.gateway.WriteStateException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.bulk.stats.BulkOperationListener;
import org.elasticsearch.index.bulk.stats.BulkStats;
import org.elasticsearch.index.bulk.stats.ShardBulkStats;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.ShardBitsetFilterCache;
import org.elasticsearch.index.cache.request.ShardRequestCache;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.GetResult;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.engine.RefreshFailedEngineException;
import org.elasticsearch.index.engine.SafeCommitInfo;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.ShardFieldData;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.Store.MetadataSnapshot;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.ShardIndexWarmerService;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 索引分片对象
 */
public class IndexShard extends AbstractIndexShardComponent implements IndicesClusterStateService.Shard {

    private final ThreadPool threadPool;
    /**
     * 映射服务 内部就是各种mapper对象 现在还不知道怎么用
     */
    private final MapperService mapperService;
    /**
     * 索引相关的缓存
     */
    private final IndexCache indexCache;
    /**
     * 存储模块
     */
    private final Store store;

    /**
     * 就是一个统计对象 实现了indexListener 会在操作index的各个时期统计相关信息
     */
    private final InternalIndexingStats internalIndexingStats;
    /**
     * 实现search钩子 用于统计数据
     */
    private final ShardSearchStats searchStats = new ShardSearchStats();

    /**
     * 可以获取到分片的服务
     */
    private final ShardGetService getService;
    /**
     * 统计有关预热的数据 本身与预热没有关系
     */
    private final ShardIndexWarmerService shardWarmerService;

    /**
     * 也是一个计数器 与缓存没有直接关系
     */
    private final ShardRequestCache requestCacheStats;
    /**
     * 这也是统计对象
     */
    private final ShardFieldData shardFieldData;

    /**
     * 也是统计对象
     */
    private final ShardBitsetFilterCache shardBitsetFilterCache;
    private final Object mutex = new Object();

    /**
     * 是否在启动时要对分片做检查
     */
    private final String checkIndexOnStartup;
    /**
     * 内部存储了ES加载的 所有codec 以及ES内置的codec 它基于lucene的默认编解码器做了加工
     * 当检测到fieldType 为某种completionFieldType时 就会返回一种特殊的format
     */
    private final CodecService codecService;
    /**
     * 暖机的意思就是提前将数据从文件读取到缓存中
     */
    private final Engine.Warmer warmer;
    /**
     * TODO
     */
    private final SimilarityService similarityService;
    /**
     * 存储了需要生成translog相关文件时的参数 比如shardId 以及dir
     */
    private final TranslogConfig translogConfig;
    /**
     * 监听索引的变化
     */
    private final IndexEventListener indexEventListener;
    /**
     * 该对象主要是鉴别某个Query是否支持使用缓存  以及当使用缓存时触发的钩子
     */
    private final QueryCachingPolicy cachingPolicy;
    /**
     * 查询出来的结果可以根据该对象进行排序
     * 查询出来的一组doc 内部都会存在各种field  sort就是定义如何利用这些field.value 对doc进行排序
     */
    private final Supplier<Sort> indexSortSupplier;
    // Package visible for testing
    /**
     * 该对象可以根据不同的name 获取不同的熔断器对象  在name级别 也就是使用级别解耦
     */
    final CircuitBreakerService circuitBreakerService;

    /**
     * 查询前后会触发相关钩子
     */
    private final SearchOperationListener searchOperationListener;

    /**
     * 监听bulk操作 并统计相关数据
     */
    private final ShardBulkStats bulkOperationListener;

    /**
     * 可以往内部追加globalCheckpoint 监听器 同时设置一个超时时间
     * 如果在规定的时间内得到了比listener知晓的更大的checkpoint 就可以触发监听器
     */
    private final GlobalCheckpointListeners globalCheckpointListeners;
    /**
     * 管理 副本组中 每个allocationId 对应路由任务
     */
    private final PendingReplicationActions pendingReplicationActions;
    /**
     * 记录续约信息
     */
    private final ReplicationTracker replicationTracker;

    /**
     * 描述该分片的路由信息
     */
    protected volatile ShardRouting shardRouting;

    /**
     * 描述该分片此时的状态
     * 根据状态判断此时是否允许 read/write 操作
     */
    protected volatile IndexShardState state;
    // ensure happens-before relation between addRefreshListener() and postRecovery()
    private final Object postRecoveryMutex = new Object();
    private volatile long pendingPrimaryTerm; // see JavaDocs for getPendingPrimaryTerm
    private final Object engineMutex = new Object(); // lock ordering: engineMutex -> mutex

    /**
     * 当前引用的引擎对象
     */
    private final AtomicReference<Engine> currentEngineReference = new AtomicReference<>();
    /**
     * 该对象根据相关配置生成引擎对象
     */
    final EngineFactory engineFactory;

    private final IndexingOperationListener indexingOperationListeners;
    private final Runnable globalCheckpointSyncer;

    Runnable getGlobalCheckpointSyncer() {
        return globalCheckpointSyncer;
    }

    /**
     * 该对象负责同步 某个shardId 对应的所有分片的续约信息
     */
    private final RetentionLeaseSyncer retentionLeaseSyncer;

    /**
     * 恢复数据时有一连串状态  代表此时所处的状态
     */
    @Nullable
    private RecoveryState recoveryState;

    private final RecoveryStats recoveryStats = new RecoveryStats();
    private final MeanMetric refreshMetric = new MeanMetric();
    private final MeanMetric externalRefreshMetric = new MeanMetric();
    private final MeanMetric flushMetric = new MeanMetric();
    private final CounterMetric periodicFlushMetric = new CounterMetric();

    /**
     * 当 engine执行出现异常时 触发该监听器
     */
    private final ShardEventListener shardEventListener = new ShardEventListener();

    /**
     * 表示当前分片的路径
     */
    private final ShardPath path;

    /**
     * 通过这个对象 提供了一个阻塞任务的功能
     */
    private final IndexShardOperationPermits indexShardOperationPermits;

    /**
     * 只有当分片启动完成时 以及恢复数据后 才处于可读状态
     * 在分片初始化阶段  由于本地数据还未恢复 不支持查询
     */
    private static final EnumSet<IndexShardState> readAllowedStates = EnumSet.of(IndexShardState.STARTED, IndexShardState.POST_RECOVERY);
    // for primaries, we only allow to write when actually started (so the cluster has decided we started)
    // in case we have a relocation of a primary, we also allow to write after phase 2 completed, where the shard may be
    // in state RECOVERING or POST_RECOVERY.
    // for replicas, replication is also allowed while recovering, since we index also during recovery to replicas and rely on
    // version checks to make sure its consistent a relocated shard can also be target of a replication if the relocation target has not
    // been marked as active yet and is syncing it's changes back to the relocation source
    /**
     * 在恢复状态下也是处于可写状态
     * 恢复状态下写入的是之前的数据 不是新数据
     */
    private static final EnumSet<IndexShardState> writeAllowedStates = EnumSet.of(IndexShardState.RECOVERING,
        IndexShardState.POST_RECOVERY, IndexShardState.STARTED);

    /**
     * 包装reader对象
     */
    private final CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper;

    /**
     * True if this shard is still indexing (recently) and false if we've been idle for long enough (as periodically checked by {@link
     * IndexingMemoryController}).
     * 当前分片是否处于激活状态  当发起一次index/delete/noop操作时 就会设置为true
     * 当分片首次被创建 并且启动engine后 也会设置为true
     */
    private final AtomicBoolean active = new AtomicBoolean();
    /**
     * Allows for the registration of listeners that are called when a change becomes visible for search.
     */
    private final RefreshListeners refreshListeners;

    /**
     * 最近一次获取searcher的时间 当该对象被初始化时 使用当前时间作为该时间戳
     * 每次重新获取searcher 就会刷新这个值
     */
    private final AtomicLong lastSearcherAccess = new AtomicLong();

    /**
     * location 是定义translog 中某个operation的位置的
     */
    private final AtomicReference<Translog.Location> pendingRefreshLocation = new AtomicReference<>();

    private final RefreshPendingLocationListener refreshPendingLocationListener;
    private volatile boolean useRetentionLeasesInPeerRecovery;

    /**
     * @param shardRouting            描述这个分片会被分配到哪个节点上
     *                                这个属性是谁创建的 基于什么原则进行分配
     * @param indexSettings
     * @param path                    存储shard信息的路径
     * @param store                   好像就是读取有关 segment_N 文件的 还不清楚咋用
     * @param indexSortSupplier
     * @param indexCache              内部维护有关索引的所有缓存 根据维度 又分为 queryCache BitsetFilterCache
     * @param mapperService           映射服务 还没搞明白怎么用的
     * @param similarityService       打分服务先不看
     * @param engineFactory           通过engine实现与lucene组件的对接
     * @param indexEventListener      监听索引各个事件的监听器
     * @param indexReaderWrapper
     * @param threadPool
     * @param bigArrays               该对象利用内存池 减少GC开销 内部可以分配各种数组
     * @param warmer                  暖机对象的意义就是提前将数据从磁盘加入到缓存中
     * @param searchOperationListener 监听查询并执行相关逻辑
     * @param listeners
     * @param globalCheckpointSyncer
     * @param retentionLeaseSyncer
     * @param circuitBreakerService
     * @throws IOException
     */
    public IndexShard(
        final ShardRouting shardRouting,
        final IndexSettings indexSettings,
        final ShardPath path,
        final Store store,
        final Supplier<Sort> indexSortSupplier,
        final IndexCache indexCache,
        final MapperService mapperService,
        final SimilarityService similarityService,
        final @Nullable EngineFactory engineFactory,
        final IndexEventListener indexEventListener,
        final CheckedFunction<DirectoryReader, DirectoryReader, IOException> indexReaderWrapper,
        final ThreadPool threadPool,
        final BigArrays bigArrays,
        final Engine.Warmer warmer,
        final List<SearchOperationListener> searchOperationListener,
        final List<IndexingOperationListener> listeners,
        final Runnable globalCheckpointSyncer,
        final RetentionLeaseSyncer retentionLeaseSyncer,
        final CircuitBreakerService circuitBreakerService) throws IOException {
        // 因为IndexShard 继承自 IndexShardComponent 所以将相关信息注册上去
        super(shardRouting.shardId(), indexSettings);
        assert shardRouting.initializing();
        this.shardRouting = shardRouting;
        final Settings settings = indexSettings.getSettings();
        // 通过不同的 name 可以获取到不同的Codec对象
        this.codecService = new CodecService(mapperService, logger);
        this.warmer = warmer;
        this.similarityService = similarityService;
        Objects.requireNonNull(store, "Store must be provided to the index shard");
        this.engineFactory = Objects.requireNonNull(engineFactory);
        this.store = store;
        // 通过该对象可以为查询结果排序
        this.indexSortSupplier = indexSortSupplier;
        this.indexEventListener = indexEventListener;
        this.threadPool = threadPool;
        // 该对象可以检测 一组location是否已经刷盘
        this.translogSyncProcessor = createTranslogSyncProcessor(logger, threadPool.getThreadContext(), this::getEngine);
        this.mapperService = mapperService;
        this.indexCache = indexCache;

        // 在原有的监听器基础上追加一个记录统计信息的监听器
        this.internalIndexingStats = new InternalIndexingStats();
        final List<IndexingOperationListener> listenersList = new ArrayList<>(listeners);
        listenersList.add(internalIndexingStats);

        // 将这组监听器合并成一个
        this.indexingOperationListeners = new IndexingOperationListener.CompositeListener(listenersList, logger);
        this.bulkOperationListener = new ShardBulkStats();

        // 为全局检查点对象 以及续约对象赋值
        this.globalCheckpointSyncer = globalCheckpointSyncer;
        this.retentionLeaseSyncer = Objects.requireNonNull(retentionLeaseSyncer);

        // 有关查询操作的监听器
        final List<SearchOperationListener> searchListenersList = new ArrayList<>(searchOperationListener);
        searchListenersList.add(searchStats);
        this.searchOperationListener = new SearchOperationListener.CompositeListener(searchListenersList, logger);

        // 通过该对象查询写入到lucene中的数据
        this.getService = new ShardGetService(indexSettings, this, mapperService);
        // 4个数据统计对象
        this.shardWarmerService = new ShardIndexWarmerService(shardId, indexSettings);
        this.requestCacheStats = new ShardRequestCache();
        this.shardFieldData = new ShardFieldData();
        this.shardBitsetFilterCache = new ShardBitsetFilterCache(shardId, indexSettings);


        // 此时分片处于被创建的状态
        state = IndexShardState.CREATED;
        this.path = path;
        this.circuitBreakerService = circuitBreakerService;
        /* create engine config */
        logger.debug("state: [CREATED]");

        // 是否要在启动shard时进行检查 默认是false
        this.checkIndexOnStartup = indexSettings.getValue(IndexSettings.INDEX_CHECK_ON_STARTUP);
        this.translogConfig = new TranslogConfig(shardId, shardPath().resolveTranslog(), indexSettings, bigArrays);
        final String aId = shardRouting.allocationId().getId();

        // 从索引元数据中获取该分片对应的 term
        final long primaryTerm = indexSettings.getIndexMetadata().primaryTerm(shardId.id());
        this.pendingPrimaryTerm = primaryTerm;
        // 该对象内部整合了一组全局检查点监听器
        this.globalCheckpointListeners =
            new GlobalCheckpointListeners(shardId, threadPool.scheduler(), logger);
        // 存储正在执行复制的action
        this.pendingReplicationActions = new PendingReplicationActions(shardId, threadPool);

        // 该对象记录了 集群中副本分片的同步状态  无论主副本shard都会携带该对象
        this.replicationTracker = new ReplicationTracker(
            shardId,
            aId,
            indexSettings,
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            globalCheckpointListeners::globalCheckpointUpdated,
            threadPool::absoluteTimeInMillis,
            // 处理续约信息的函数对象
            (retentionLeases, listener) -> retentionLeaseSyncer.sync(shardId, aId, getPendingPrimaryTerm(), retentionLeases, listener),
            this::getSafeCommitInfo,
            pendingReplicationActions);

        // the query cache is a node-level thing, however we want the most popular filters
        // to be computed on a per-shard basis
        // 默认false
        if (IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.get(settings)) {
            cachingPolicy = new QueryCachingPolicy() {
                @Override
                public void onUse(Query query) {

                }

                @Override
                public boolean shouldCache(Query query) {
                    return true;
                }
            };
        } else {
            // 使用根据频率来决定是否使用缓存的策略  实际上也是lru算法
            cachingPolicy = new UsageTrackingQueryCachingPolicy();
        }
        // 针对shard的某些操作也许可以并行 也许只能串行 通过该对象内部的permits实现隔离
        indexShardOperationPermits = new IndexShardOperationPermits(shardId, threadPool);
        // 默认为空
        readerWrapper = indexReaderWrapper;
        refreshListeners = buildRefreshListeners();
        lastSearcherAccess.set(threadPool.relativeTimeInMillis());
        // 在初始化完成后 将此时分片的元数据信息写入到相关目录中  写入的元数据是 ShardStateMetadata
        persistMetadata(path, indexSettings, shardRouting, null, logger);
        // 默认为true
        this.useRetentionLeasesInPeerRecovery = replicationTracker.hasAllPeerRecoveryRetentionLeases();
        // 监听最新写入的operation的location 当超过了pending时 将pending置空
        this.refreshPendingLocationListener = new RefreshPendingLocationListener();
    }

    public ThreadPool getThreadPool() {
        return this.threadPool;
    }

    public Store store() {
        return this.store;
    }

    /**
     * Return the sort order of this index, or null if the index has no sort.
     */
    public Sort getIndexSort() {
        return indexSortSupplier.get();
    }

    public ShardGetService getService() {
        return this.getService;
    }

    public ShardBitsetFilterCache shardBitsetFilterCache() {
        return shardBitsetFilterCache;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public SearchOperationListener getSearchOperationListener() {
        return this.searchOperationListener;
    }

    public BulkOperationListener getBulkOperationListener() {
        return this.bulkOperationListener;
    }

    public ShardIndexWarmerService warmerService() {
        return this.shardWarmerService;
    }

    public ShardRequestCache requestCache() {
        return this.requestCacheStats;
    }

    public ShardFieldData fieldData() {
        return this.shardFieldData;
    }

    /**
     * USE THIS METHOD WITH CARE!
     * Returns the primary term the index shard is supposed to be on. In case of primary promotion or when a replica learns about
     * a new term due to a new primary, the term that's exposed here will not be the term that the shard internally uses to assign
     * to operations. The shard will auto-correct its internal operation term, but this might take time.
     * See {@link org.elasticsearch.cluster.metadata.IndexMetadata#primaryTerm(int)}
     */
    public long getPendingPrimaryTerm() {
        return this.pendingPrimaryTerm;
    }

    /**
     * Returns the primary term that is currently being used to assign to operations
     * 当前发起操作的主分片任期是多少
     */
    public long getOperationPrimaryTerm() {
        return replicationTracker.getOperationPrimaryTerm();
    }

    /**
     * Returns the latest cluster routing entry received with this shard.
     */
    @Override
    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    public QueryCachingPolicy getQueryCachingPolicy() {
        return cachingPolicy;
    }


    /**
     *
     * @param newRouting
     * @param newPrimaryTerm              当前主分片的 term 每个在集群中更替 主分片在集群中的位置 就会增加term
     * @param primaryReplicaSyncer        the primary-replica resync action to trigger when a term is increased on a primary
     * @param applyingClusterStateVersion the cluster state version being applied when updating the allocation IDs from the master
     * @param inSyncAllocationIds         the allocation ids of the currently in-sync shard copies    已经完成同步的所有分片
     * @param routingTable                the shard routing table     某个shard在整个集群中最新的分布情况
     * @throws IOException
     * 比如本节点上报修改状态的请求被leader通过  在发布到集群通知本节点 就要做一些更新操作
     */
    @Override
    public void updateShardState(final ShardRouting newRouting,
                                 final long newPrimaryTerm,
                                 final BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer,
                                 final long applyingClusterStateVersion,
                                 final Set<String> inSyncAllocationIds,
                                 final IndexShardRoutingTable routingTable) throws IOException {
        final ShardRouting currentRouting;
        synchronized (mutex) {
            currentRouting = this.shardRouting;
            assert currentRouting != null;

            // 处理的shard不匹配时 抛出异常
            if (!newRouting.shardId().equals(shardId())) {
                throw new IllegalArgumentException("Trying to set a routing entry with shardId " +
                    newRouting.shardId() + " on a shard with shardId " + shardId());
            }
            // 代表虽然是同一个分片 分配到同一个节点 但是发生在2次分配动作中   allocationId是为了解决ABA的问题
            if (newRouting.isSameAllocation(currentRouting) == false) {
                throw new IllegalArgumentException("Trying to set a routing entry with a different allocation. Current " +
                    currentRouting + ", new " + newRouting);
            }
            // 不允许直接更新分片角色  应该要先经历移除的操作 再重新添加
            if (currentRouting.primary() && newRouting.primary() == false) {
                throw new IllegalArgumentException("illegal state: trying to move shard from primary mode to replica mode. Current "
                    + currentRouting + ", new " + newRouting);
            }

            // 当前如果是主分片 要更新副本组信息 这涉及到了 副本从主分片拉取数据进行recovery的过程
            if (newRouting.primary()) {
                replicationTracker.updateFromMaster(applyingClusterStateVersion, inSyncAllocationIds, routingTable);
            }

            // 更新本地分片状态
            if (state == IndexShardState.POST_RECOVERY && newRouting.active()) {
                assert currentRouting.active() == false : "we are in POST_RECOVERY, but our shard routing is active " + currentRouting;
                assert currentRouting.isRelocationTarget() == false || currentRouting.primary() == false ||
                    replicationTracker.isPrimaryMode() :
                    "a primary relocation is completed by the master, but primary mode is not active " + currentRouting;

                changeState(IndexShardState.STARTED, "global state is [" + newRouting.state() + "]");
                // TODO
            } else if (currentRouting.primary() && currentRouting.relocating() && replicationTracker.isRelocated() &&
                (newRouting.relocating() == false || newRouting.equalsIgnoringMetadata(currentRouting) == false)) {
                // if the shard is not in primary mode anymore (after primary relocation) we have to fail when any changes in shard
                // routing occur (e.g. due to recovery failure / cancellation). The reason is that at the moment we cannot safely
                // reactivate primary mode without risking two active primaries.
                throw new IndexShardRelocatedException(shardId(), "Shard is marked as relocated, cannot safely move to state " +
                    newRouting.state());
            }
            assert newRouting.active() == false || state == IndexShardState.STARTED || state == IndexShardState.CLOSED :
                "routing is active, but local shard state isn't. routing: " + newRouting + ", local state: " + state;

            // 更新最新的路由信息 并进行持久化
            persistMetadata(path, indexSettings, newRouting, currentRouting, logger);
            final CountDownLatch shardStateUpdated = new CountDownLatch(1);

            // 主分片时才需要继续处理
            if (newRouting.primary()) {
                // 主分片路由相关的信息没有变化 只是state变化
                if (newPrimaryTerm == pendingPrimaryTerm) {
                    // 主分片被激活了
                    if (currentRouting.initializing() && currentRouting.isRelocationTarget() == false && newRouting.active()) {
                        // the master started a recovering primary, activate primary mode.
                        // 激活主分片模式 这样其他分片才可以从主分片恢复数据
                        replicationTracker.activatePrimaryMode(getLocalCheckpoint());
                        ensurePeerRecoveryRetentionLeasesExist();
                    }
                } else {
                    // TODO
                    assert currentRouting.primary() == false : "term is only increased as part of primary promotion";
                    /* Note that due to cluster state batching an initializing primary shard term can failed and re-assigned
                     * in one state causing it's term to be incremented. Note that if both current shard state and new
                     * shard state are initializing, we could replace the current shard and reinitialize it. It is however
                     * possible that this shard is being started. This can happen if:
                     * 1) Shard is post recovery and sends shard started to the master
                     * 2) Node gets disconnected and rejoins
                     * 3) Master assigns the shard back to the node
                     * 4) Master processes the shard started and starts the shard
                     * 5) The node process the cluster state where the shard is both started and primary term is incremented.
                     *
                     * We could fail the shard in that case, but this will cause it to be removed from the insync allocations list
                     * potentially preventing re-allocation.
                     */
                    assert newRouting.initializing() == false :
                        "a started primary shard should never update its term; "
                            + "shard " + newRouting + ", "
                            + "current term [" + pendingPrimaryTerm + "], "
                            + "new term [" + newPrimaryTerm + "]";
                    assert newPrimaryTerm > pendingPrimaryTerm :
                        "primary terms can only go up; current term [" + pendingPrimaryTerm + "], new term [" + newPrimaryTerm + "]";
                    /*
                     * Before this call returns, we are guaranteed that all future operations are delayed and so this happens before we
                     * increment the primary term. The latch is needed to ensure that we do not unblock operations before the primary
                     * term is incremented.
                     */
                    // to prevent primary relocation handoff while resync is not completed
                    boolean resyncStarted = primaryReplicaResyncInProgress.compareAndSet(false, true);
                    if (resyncStarted == false) {
                        throw new IllegalStateException("cannot start resync while it's already in progress");
                    }
                    bumpPrimaryTerm(newPrimaryTerm,
                        () -> {
                            shardStateUpdated.await();
                            assert pendingPrimaryTerm == newPrimaryTerm :
                                "shard term changed on primary. expected [" + newPrimaryTerm + "] but was [" + pendingPrimaryTerm + "]" +
                                    ", current routing: " + currentRouting + ", new routing: " + newRouting;
                            assert getOperationPrimaryTerm() == newPrimaryTerm;
                            try {
                                replicationTracker.activatePrimaryMode(getLocalCheckpoint());
                                ensurePeerRecoveryRetentionLeasesExist();
                                /*
                                 * If this shard was serving as a replica shard when another shard was promoted to primary then
                                 * its Lucene index was reset during the primary term transition. In particular, the Lucene index
                                 * on this shard was reset to the global checkpoint and the operations above the local checkpoint
                                 * were reverted. If the other shard that was promoted to primary subsequently fails before the
                                 * primary/replica re-sync completes successfully and we are now being promoted, we have to restore
                                 * the reverted operations on this shard by replaying the translog to avoid losing acknowledged writes.
                                 */
                                final Engine engine = getEngine();
                                engine.restoreLocalHistoryFromTranslog((resettingEngine, snapshot) ->
                                    runTranslogRecovery(resettingEngine, snapshot, Engine.Operation.Origin.LOCAL_RESET, () -> {
                                    }));
                                /* Rolling the translog generation is not strictly needed here (as we will never have collisions between
                                 * sequence numbers in a translog generation in a new primary as it takes the last known sequence number
                                 * as a starting point), but it simplifies reasoning about the relationship between primary terms and
                                 * translog generations.
                                 */
                                engine.rollTranslogGeneration();
                                engine.fillSeqNoGaps(newPrimaryTerm);
                                replicationTracker.updateLocalCheckpoint(currentRouting.allocationId().getId(),
                                    getLocalCheckpoint());
                                primaryReplicaSyncer.accept(this, new ActionListener<ResyncTask>() {
                                    @Override
                                    public void onResponse(ResyncTask resyncTask) {
                                        logger.info("primary-replica resync completed with {} operations",
                                            resyncTask.getResyncedOperations());
                                        boolean resyncCompleted =
                                            primaryReplicaResyncInProgress.compareAndSet(true, false);
                                        assert resyncCompleted : "primary-replica resync finished but was not started";
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        boolean resyncCompleted =
                                            primaryReplicaResyncInProgress.compareAndSet(true, false);
                                        assert resyncCompleted : "primary-replica resync finished but was not started";
                                        if (state == IndexShardState.CLOSED) {
                                            // ignore, shutting down
                                        } else {
                                            failShard("exception during primary-replica resync", e);
                                        }
                                    }
                                });
                            } catch (final AlreadyClosedException e) {
                                // okay, the index was deleted
                            }
                        }, null);
                }
            }
            // set this last, once we finished updating all internal state.
            this.shardRouting = newRouting;

            assert this.shardRouting.primary() == false ||
                this.shardRouting.started() == false || // note that we use started and not active to avoid relocating shards
                this.indexShardOperationPermits.isBlocked() || // if permits are blocked, we are still transitioning
                this.replicationTracker.isPrimaryMode()
                : "a started primary with non-pending operation term must be in primary mode " + this.shardRouting;
            shardStateUpdated.countDown();
        }

        // 目前这2个钩子都是空实现
        if (currentRouting.active() == false && newRouting.active()) {
            indexEventListener.afterIndexShardStarted(this);
        }
        if (newRouting.equals(currentRouting) == false) {
            indexEventListener.shardRoutingChanged(this, currentRouting, newRouting);
        }

        // TODO
        if (indexSettings.isSoftDeleteEnabled() && useRetentionLeasesInPeerRecovery == false) {
            final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases();
            final Set<ShardRouting> shardRoutings = new HashSet<>(routingTable.getShards());
            shardRoutings.addAll(routingTable.assignedShards()); // include relocation targets
            if (shardRoutings.stream().allMatch(
                shr -> shr.assignedToNode() && retentionLeases.contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(shr)))) {
                useRetentionLeasesInPeerRecovery = true;
            }
        }
    }

    /**
     * Marks the shard as recovering based on a recovery state, fails with exception is recovering is not allowed to be set.
     *
     * @param recoveryState 将当前分片状态修改成恢复中
     */
    public IndexShardState markAsRecovering(String reason, RecoveryState recoveryState) throws IndexShardStartedException,
        IndexShardRelocatedException, IndexShardRecoveringException, IndexShardClosedException {
        synchronized (mutex) {
            // 只有在 CREATED 模式下才可以进行state的变换
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            if (state == IndexShardState.RECOVERING) {
                throw new IndexShardRecoveringException(shardId);
            }
            if (state == IndexShardState.POST_RECOVERY) {
                throw new IndexShardRecoveringException(shardId);
            }
            this.recoveryState = recoveryState;
            return changeState(IndexShardState.RECOVERING, reason);
        }
    }

    /**
     * 是否在进行 副本与主节点之间的同步
     */
    private final AtomicBoolean primaryReplicaResyncInProgress = new AtomicBoolean();

    /**
     * Completes the relocation. Operations are blocked and current operations are drained before changing state to relocated. The provided
     * {@link Runnable} is executed after all operations are successfully blocked.
     *
     * @param consumer           a {@link Runnable} that is executed after operations are blocked   整个relocate的流程都被
     * @param targetAllocationId 重定向选中的分配者id
     *                           进行重定位
     * @throws IllegalIndexShardStateException if the shard is not relocating due to concurrent cancellation
     * @throws IllegalStateException           if the relocation target is no longer part of the replication group
     * @throws InterruptedException            if blocking operations is interrupted
     */
    public void relocated(final String targetAllocationId, final Consumer<ReplicationTracker.PrimaryContext> consumer)
        throws IllegalIndexShardStateException, IllegalStateException, InterruptedException {
        assert shardRouting.primary() : "only primaries can be marked as relocated: " + shardRouting;
        // 执行强制刷新动作  会调用 refresh()
        // 对应 DirectoryReader的更新动作 就是检测IndexWriter中是否有未处理的变化 之后commit这些变化 并更新此时可见的所有reader
        try (Releasable forceRefreshes = refreshListeners.forceRefreshes()) {
            // 在重定位的过程中 需要进入一个阻塞状态 之后通过indexShardOperationPermits发起的所有操作都会被阻塞
            indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, () -> {
                // 因为刷新已经完成了 修改refreshListeners内部的刷新计数器
                forceRefreshes.close();
                // no shard operation permits are being held here, move state from started to relocated
                assert indexShardOperationPermits.getActiveOperationsCount() == OPERATIONS_BLOCKED :
                    "in-flight operations in progress while moving shard state to relocated";
                /*
                 * We should not invoke the runnable under the mutex as the expected implementation is to handoff the primary context via a
                 * network operation. Doing this under the mutex can implicitly block the cluster state update thread on network operations.
                 * 确保此时能进行重定向
                 */
                verifyRelocatingState();
                // 切换成重定向专用的上下文  上下文中包含了当前的 routingTable   以及此时每个allocationId 对应的检查点状态(CheckpointState)
                // 检查点状态中包含了 localCheckpoint globalCheckpoint 等信息
                final ReplicationTracker.PrimaryContext primaryContext = replicationTracker.startRelocationHandoff(targetAllocationId);
                try {
                    // 在这里已经完成了整个重定向了
                    consumer.accept(primaryContext);
                    synchronized (mutex) {
                        // 确保结束时还处于重定向状态中 避免函数做恶意修改
                        verifyRelocatingState();
                        // 代表处理结束
                        replicationTracker.completeRelocationHandoff(); // make changes to primaryMode and relocated flag only under mutex
                    }
                } catch (final Exception e) {
                    try {
                        replicationTracker.abortRelocationHandoff();
                    } catch (final Exception inner) {
                        e.addSuppressed(inner);
                    }
                    throw e;
                }
            });
        } catch (TimeoutException e) {
            logger.warn("timed out waiting for relocation hand-off to complete");
            // This is really bad as ongoing replication operations are preventing this shard from completing relocation hand-off.
            // Fail primary relocation source and target shards.
            failShard("timed out waiting for relocation hand-off to complete", null);
            throw new IndexShardClosedException(shardId(), "timed out waiting for relocation hand-off to complete");
        }
    }

    /**
     * 校验重定向状态
     */
    private void verifyRelocatingState() {
        if (state != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(shardId, state);
        }
        /*
         * If the master cancelled recovery, the target will be removed and the recovery will be cancelled. However, it is still possible
         * that we concurrently end up here and therefore have to protect that we do not mark the shard as relocated when its shard routing
         * says otherwise.
         * TODO 必须要确保分片处于重定向状态  什么时候变化的 又是因为什么原因变化的
         */
        if (shardRouting.relocating() == false) {
            throw new IllegalIndexShardStateException(shardId, IndexShardState.STARTED,
                ": shard is no longer relocating " + shardRouting);
        }

        // resync 与 relocation 不能同时进行
        if (primaryReplicaResyncInProgress.get()) {
            throw new IllegalIndexShardStateException(shardId, IndexShardState.STARTED,
                ": primary relocation is forbidden while primary-replica resync is in progress " + shardRouting);
        }
    }

    @Override
    public IndexShardState state() {
        return state;
    }

    /**
     * Changes the state of the current shard
     *
     * @param newState the new shard state
     * @param reason   the reason for the state change
     * @return the previous shard state
     * 更改当前shard的状态
     */
    private IndexShardState changeState(IndexShardState newState, String reason) {
        assert Thread.holdsLock(mutex);
        logger.debug("state: [{}]->[{}], reason [{}]", state, newState, reason);
        IndexShardState previousState = state;
        state = newState;
        // 因为分片的状态发生了变化 所以触发监听器  目前都是空钩子
        this.indexEventListener.indexShardStateChanged(this, previousState, newState, reason);
        return previousState;
    }

    /**
     * 在主分片上执行某种操作
     *
     * @param version
     * @param versionType            这个内外版本是什么意思???
     * @param sourceToParse          一个可以解析的数据流
     * @param ifSeqNo
     * @param ifPrimaryTerm
     * @param autoGeneratedTimestamp
     * @param isRetry
     * @return
     * @throws IOException
     */
    public Engine.IndexResult applyIndexOperationOnPrimary(long version, VersionType versionType, SourceToParse sourceToParse,
                                                           long ifSeqNo, long ifPrimaryTerm, long autoGeneratedTimestamp,
                                                           boolean isRetry)
        throws IOException {
        assert versionType.validateVersionForWrites(version);
        return applyIndexOperation(getEngine(), UNASSIGNED_SEQ_NO, getOperationPrimaryTerm(), version, versionType, ifSeqNo,
            ifPrimaryTerm, autoGeneratedTimestamp, isRetry, Engine.Operation.Origin.PRIMARY, sourceToParse);
    }

    /**
     * 在副本上处理请求  大体流程跟primary上处理一致 都会委托给engine
     * @param seqNo
     * @param opPrimaryTerm
     * @param version
     * @param autoGeneratedTimeStamp
     * @param isRetry
     * @param sourceToParse
     * @return
     * @throws IOException
     */
    public Engine.IndexResult applyIndexOperationOnReplica(long seqNo, long opPrimaryTerm, long version, long autoGeneratedTimeStamp,
                                                           boolean isRetry, SourceToParse sourceToParse)
        throws IOException {
        return applyIndexOperation(getEngine(), seqNo, opPrimaryTerm, version, null, UNASSIGNED_SEQ_NO, 0,
            autoGeneratedTimeStamp, isRetry, Engine.Operation.Origin.REPLICA, sourceToParse);
    }


    /**
     * 执行index操作  Index内部会携带一个source属性 就是本次要写入的原始数据
     * 数据会写入到lucene中
     * @param engine
     * @param seqNo                  在处理主分片的请求时 不会从外部传入seqNo 而是自动分配   在恢复数据阶段就是使用index上携带的seq
     * @param opPrimaryTerm
     * @param version
     * @param versionType            先假设为null
     * @param ifSeqNo                代表要校验该operation对应的seqNo 和 term
     * @param ifPrimaryTerm
     * @param autoGeneratedTimeStamp
     * @param isRetry
     * @param origin                 代表本次请求是由谁发起的 比如从本地事务日志中恢复数据
     * @param sourceToParse          该对象内部包含了待解析的原始数据流
     * @return
     * @throws IOException
     */
    private Engine.IndexResult applyIndexOperation(Engine engine, long seqNo, long opPrimaryTerm, long version,
                                                   @Nullable VersionType versionType, long ifSeqNo, long ifPrimaryTerm,
                                                   long autoGeneratedTimeStamp, boolean isRetry, Engine.Operation.Origin origin,
                                                   SourceToParse sourceToParse) throws IOException {
        assert opPrimaryTerm <= getOperationPrimaryTerm()
            : "op term [ " + opPrimaryTerm + " ] > shard term [" + getOperationPrimaryTerm() + "]";

        // 在执行写入操作前 需要先检测操作本身 与当前的状态是否是匹配的 不匹配的情况要抛出异常
        ensureWriteAllowed(origin);
        Engine.Index operation;
        try {
            // 将相关信息转换成 Index对象   docMapper()定义了一套模板 如何将数据流转换成json格式数据
            operation = prepareIndex(docMapper(), sourceToParse,
                seqNo, opPrimaryTerm, version, versionType, origin, autoGeneratedTimeStamp, isRetry, ifSeqNo, ifPrimaryTerm);
            Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
            // TODO 先忽略 当调用docMapper时 应该是不会生成mapping的 所以update应该为null
            if (update != null) {
                return new Engine.IndexResult(update);
            }
        } catch (Exception e) {
            // We treat any exception during parsing and or mapping update as a document level failure
            // with the exception side effects of closing the shard. Since we don't have the shard, we
            // can not raise an exception that may block any replication of previous operations to the
            // replicas
            verifyNotClosed(e);
            return new Engine.IndexResult(e, version, opPrimaryTerm, seqNo);
        }

        return index(engine, operation);
    }

    /**
     * 将sourceToParse 通过mapper对象进行转换 并依靠其余属性生成Index 对象
     *
     * @param docMapper 内部包含 Mapping 和 DocumentMapper
     * @param source  包含一些基础信息
     * @param seqNo
     * @param primaryTerm
     * @param version
     * @param versionType
     * @param origin
     * @param autoGeneratedIdTimestamp
     * @param isRetry
     * @param ifSeqNo
     * @param ifPrimaryTerm
     * @return
     */
    public static Engine.Index prepareIndex(DocumentMapperForType docMapper, SourceToParse source, long seqNo,
                                            long primaryTerm, long version, VersionType versionType, Engine.Operation.Origin origin,
                                            long autoGeneratedIdTimestamp, boolean isRetry,
                                            long ifSeqNo, long ifPrimaryTerm) {
        long startTime = System.nanoTime();
        // 得到解析后的结果 TODO 解析逻辑先不看了 因为不影响核心流程
        ParsedDocument doc = docMapper.getDocumentMapper().parse(source);
        // 当首次创建docMapper时 内部的mapping为空 不会触发addDynamic逻辑
        if (docMapper.getMapping() != null) {
            doc.addDynamicMappingsUpdate(docMapper.getMapping());
        }
        // 将doc.id 字段包装成了 term
        Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id()));
        return new Engine.Index(uid, doc, seqNo, primaryTerm, version, versionType, origin, startTime, autoGeneratedIdTimestamp, isRetry,
            ifSeqNo, ifPrimaryTerm);
    }

    /**
     * 将某个index写入到lucene中
     *
     * @param engine
     * @param index
     * @return
     * @throws IOException
     */
    private Engine.IndexResult index(Engine engine, Engine.Index index) throws IOException {
        // 表明此时引擎处于活跃状态
        active.set(true);
        final Engine.IndexResult result;
        // 触发前置监听器  目前只有记录一个统计项 先忽略
        index = indexingOperationListeners.preIndex(shardId, index);
        try {
            if (logger.isTraceEnabled()) {
                // don't use index.source().utf8ToString() here source might not be valid UTF-8
                logger.trace("index [{}] seq# [{}] allocation-id [{}] primaryTerm [{}] operationPrimaryTerm [{}] origin [{}]",
                    index.id(), index.seqNo(), routingEntry().allocationId(), index.primaryTerm(), getOperationPrimaryTerm(),
                    index.origin());
            }
            // 这里会将数据写入到lucene中 以及将操作记录存储到事务日志中
            result = engine.index(index);
            if (logger.isTraceEnabled()) {
                logger.trace("index-done [{}] seq# [{}] allocation-id [{}] primaryTerm [{}] operationPrimaryTerm [{}] origin [{}] " +
                        "result-seq# [{}] result-term [{}] failure [{}]",
                    index.id(), index.seqNo(), routingEntry().allocationId(), index.primaryTerm(), getOperationPrimaryTerm(),
                    index.origin(), result.getSeqNo(), result.getTerm(), result.getFailure());
            }
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.trace(new ParameterizedMessage(
                    "index-fail [{}] seq# [{}] allocation-id [{}] primaryTerm [{}] operationPrimaryTerm [{}] origin [{}]",
                    index.id(), index.seqNo(), routingEntry().allocationId(), index.primaryTerm(), getOperationPrimaryTerm(),
                    index.origin()
                ), e);
            }
            indexingOperationListeners.postIndex(shardId, index, e);
            throw e;
        }
        indexingOperationListeners.postIndex(shardId, index, result);
        return result;
    }

    /**
     * 基于传入的 seqNo term 生成一个 NOOP 对象
     *
     * @param seqNo
     * @param opPrimaryTerm
     * @param reason
     * @return
     * @throws IOException
     */
    public Engine.NoOpResult markSeqNoAsNoop(long seqNo, long opPrimaryTerm, String reason) throws IOException {
        return markSeqNoAsNoop(getEngine(), seqNo, opPrimaryTerm, reason, Engine.Operation.Origin.REPLICA);
    }

    /**
     * 将Noop对象写入事务文件/lucene中
     *
     * @param engine
     * @param seqNo
     * @param opPrimaryTerm
     * @param reason
     * @param origin
     * @return
     * @throws IOException
     */
    private Engine.NoOpResult markSeqNoAsNoop(Engine engine, long seqNo, long opPrimaryTerm, String reason,
                                              Engine.Operation.Origin origin) throws IOException {
        assert opPrimaryTerm <= getOperationPrimaryTerm()
            : "op term [ " + opPrimaryTerm + " ] > shard term [" + getOperationPrimaryTerm() + "]";
        long startTime = System.nanoTime();
        ensureWriteAllowed(origin);
        final Engine.NoOp noOp = new Engine.NoOp(seqNo, opPrimaryTerm, origin, startTime, reason);
        return noOp(engine, noOp);
    }

    private Engine.NoOpResult noOp(Engine engine, Engine.NoOp noOp) throws IOException {
        // 每当产生一次写入操作 就将active 标记成true
        active.set(true);
        if (logger.isTraceEnabled()) {
            logger.trace("noop (seq# [{}])", noOp.seqNo());
        }
        return engine.noOp(noOp);
    }

    /**
     * 将某个异常信息包装成一个 result对象
     *
     * @param e
     * @param version
     * @return
     */
    public Engine.IndexResult getFailedIndexResult(Exception e, long version) {
        return new Engine.IndexResult(e, version);
    }

    public Engine.DeleteResult getFailedDeleteResult(Exception e, long version) {
        return new Engine.DeleteResult(e, version, getOperationPrimaryTerm());
    }

    /**
     * 在主分片上执行删除操作
     * @param version
     * @param id
     * @param versionType
     * @param ifSeqNo
     * @param ifPrimaryTerm
     * @return
     * @throws IOException
     */
    public Engine.DeleteResult applyDeleteOperationOnPrimary(long version, String id, VersionType versionType,
                                                             long ifSeqNo, long ifPrimaryTerm)
        throws IOException {
        assert versionType.validateVersionForWrites(version);
        return applyDeleteOperation(getEngine(), UNASSIGNED_SEQ_NO, getOperationPrimaryTerm(), version, id, versionType,
            // 注意此时的 origin为 Primary  不同的origin会走不同的逻辑
            ifSeqNo, ifPrimaryTerm, Engine.Operation.Origin.PRIMARY);
    }

    /**
     * 在primary中进行操作 都是不设置seqNo 而通过engine自动生成
     * 而针对 replica操作 需要执行seqNo
     *
     * @param seqNo
     * @param opPrimaryTerm
     * @param version
     * @param id
     * @return
     * @throws IOException
     */
    public Engine.DeleteResult applyDeleteOperationOnReplica(long seqNo, long opPrimaryTerm, long version, String id)
        throws IOException {
        return applyDeleteOperation(
            getEngine(), seqNo, opPrimaryTerm, version, id, null, UNASSIGNED_SEQ_NO, 0, Engine.Operation.Origin.REPLICA);
    }

    /**
     * 执行删除操作
     *
     * @param engine
     * @param seqNo
     * @param opPrimaryTerm
     * @param version
     * @param id
     * @param versionType
     * @param ifSeqNo
     * @param ifPrimaryTerm
     * @param origin
     * @return
     * @throws IOException
     */
    private Engine.DeleteResult applyDeleteOperation(Engine engine, long seqNo, long opPrimaryTerm, long version, String id,
                                                     @Nullable VersionType versionType, long ifSeqNo, long ifPrimaryTerm,
                                                     Engine.Operation.Origin origin) throws IOException {
        assert opPrimaryTerm <= getOperationPrimaryTerm()
            : "op term [ " + opPrimaryTerm + " ] > shard term [" + getOperationPrimaryTerm() + "]";
        ensureWriteAllowed(origin);
        final Term uid = new Term(IdFieldMapper.NAME, Uid.encodeId(id));

        // 将相关参数包装成一个 delete对象
        final Engine.Delete delete = prepareDelete(id, uid, seqNo, opPrimaryTerm, version,
            versionType, origin, ifSeqNo, ifPrimaryTerm);
        return delete(engine, delete);
    }

    /**
     * 将Translog.Delete转换成 Engine.Delete
     *
     * @param id
     * @param uid
     * @param seqNo
     * @param primaryTerm
     * @param version
     * @param versionType
     * @param origin
     * @param ifSeqNo
     * @param ifPrimaryTerm
     * @return
     */
    private Engine.Delete prepareDelete(String id, Term uid, long seqNo, long primaryTerm, long version,
                                        VersionType versionType, Engine.Operation.Origin origin,
                                        long ifSeqNo, long ifPrimaryTerm) {
        long startTime = System.nanoTime();
        return new Engine.Delete(id, uid, seqNo, primaryTerm, version, versionType,
            origin, startTime, ifSeqNo, ifPrimaryTerm);
    }

    /**
     * 执行删除操作并返回结果
     *
     * @param engine
     * @param delete
     * @return
     * @throws IOException
     */
    private Engine.DeleteResult delete(Engine engine, Engine.Delete delete) throws IOException {
        // 每次执行操作时都会设置该标识   当其他后台线程尝试刷盘时 就会通过该标记判断是否有新数据写入
        active.set(true);
        final Engine.DeleteResult result;
        // 监听器只是做了一些数据统计工作
        delete = indexingOperationListeners.preDelete(shardId, delete);
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("delete [{}] (seq no [{}])", delete.uid().text(), delete.seqNo());
            }
            result = engine.delete(delete);
        } catch (Exception e) {
            indexingOperationListeners.postDelete(shardId, delete, e);
            throw e;
        }
        // 执行删除后会更新 MemoryController 内部的计数值
        indexingOperationListeners.postDelete(shardId, delete, result);
        return result;
    }

    /**
     * Get 只支持通过id去查询lucene.doc
     *
     * @param get
     * @return
     */
    public Engine.GetResult get(Engine.Get get) {
        readAllowed();
        // DocumentMapper 描述了如何映射doc (doc也就是从lucene中查询出来的结果)
        DocumentMapper mapper = mapperService.documentMapper();
        // 当映射服务的 docMapper对象还不存在  跳过查询  该对象一般会在创建index时生成
        if (mapper == null) {
            return GetResult.NOT_EXISTS;
        }
        // acquireSearcher 是一个工厂对象  通过scope生成searcher对象
        return getEngine().get(get, this::acquireSearcher);
    }

    /**
     * Writes all indexing changes to disk and opens a new searcher reflecting all changes.  This can throw {@link AlreadyClosedException}.
     * 在执行 relocate时 会先调用该方法   就是将此时lucene内还未commit的数据 以及update delete数据持久化 同时获取最新生成的segment的reader
     */
    public void refresh(String source) {
        verifyNotClosed();
        if (logger.isTraceEnabled()) {
            logger.trace("refresh with source [{}]", source);
        }
        getEngine().refresh(source);
    }

    /**
     * Returns how many bytes we are currently moving from heap to disk
     */
    public long getWritingBytes() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            return 0;
        }
        return engine.getWritingBytes();
    }

    public RefreshStats refreshStats() {
        int listeners = refreshListeners.pendingCount();
        return new RefreshStats(
            refreshMetric.count(),
            TimeUnit.NANOSECONDS.toMillis(refreshMetric.sum()),
            externalRefreshMetric.count(),
            TimeUnit.NANOSECONDS.toMillis(externalRefreshMetric.sum()),
            listeners);
    }

    public FlushStats flushStats() {
        return new FlushStats(flushMetric.count(), periodicFlushMetric.count(), TimeUnit.NANOSECONDS.toMillis(flushMetric.sum()));
    }

    public DocsStats docStats() {
        readAllowed();
        return getEngine().docStats();
    }

    /**
     * @return {@link CommitStats}
     * @throws AlreadyClosedException if shard is closed
     */
    public CommitStats commitStats() {
        return getEngine().commitStats();
    }

    /**
     * @return {@link SeqNoStats}
     * @throws AlreadyClosedException if shard is closed
     */
    public SeqNoStats seqNoStats() {
        return getEngine().getSeqNoStats(replicationTracker.getGlobalCheckpoint());
    }

    public IndexingStats indexingStats() {
        Engine engine = getEngineOrNull();
        final boolean throttled;
        final long throttleTimeInMillis;
        if (engine == null) {
            throttled = false;
            throttleTimeInMillis = 0;
        } else {
            throttled = engine.isThrottled();
            throttleTimeInMillis = engine.getIndexThrottleTimeInMillis();
        }
        return internalIndexingStats.stats(throttled, throttleTimeInMillis);
    }

    public SearchStats searchStats(String... groups) {
        return searchStats.stats(groups);
    }

    public GetStats getStats() {
        return getService.stats();
    }

    public StoreStats storeStats() {
        try {
            return store.stats();
        } catch (IOException e) {
            failShard("Failing shard because of exception during storeStats", e);
            throw new ElasticsearchException("io exception while building 'store stats'", e);
        }
    }

    public MergeStats mergeStats() {
        final Engine engine = getEngineOrNull();
        if (engine == null) {
            return new MergeStats();
        }
        return engine.getMergeStats();
    }

    public SegmentsStats segmentStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        SegmentsStats segmentsStats = getEngine().segmentsStats(includeSegmentFileSizes, includeUnloadedSegments);
        segmentsStats.addBitsetMemoryInBytes(shardBitsetFilterCache.getMemorySizeInBytes());
        return segmentsStats;
    }

    public WarmerStats warmerStats() {
        return shardWarmerService.stats();
    }

    public FieldDataStats fieldDataStats(String... fields) {
        return shardFieldData.stats(fields);
    }

    public TranslogStats translogStats() {
        return getEngine().getTranslogStats();
    }

    public CompletionStats completionStats(String... fields) {
        readAllowed();
        return getEngine().completionStats(fields);
    }

    public BulkStats bulkStats() {
        return bulkOperationListener.stats();
    }

    /**
     * Executes the given flush request against the engine.
     *
     * @param request the flush request
     *                将此时lucene内的数据强制commit/ 也会将事务日志内的数据刷盘
     */
    public void flush(FlushRequest request) {
        final boolean waitIfOngoing = request.waitIfOngoing();
        final boolean force = request.force();
        logger.trace("flush with {}", request);
        /*
         * We allow flushes while recovery since we allow operations to happen while recovering and we want to keep the translog under
         * control (up to deletes, which we do not GC). Yet, we do not use flush internally to clear deletes and flush the index writer
         * since we use Engine#writeIndexingBuffer for this now.
         */
        verifyNotClosed();
        final long time = System.nanoTime();
        getEngine().flush(force, waitIfOngoing);
        flushMetric.inc(System.nanoTime() - time);
    }

    /**
     * checks and removes translog files that no longer need to be retained. See
     * {@link org.elasticsearch.index.translog.TranslogDeletionPolicy} for details
     * 在 IndexService中 会开启定时任务 定期裁剪掉无用的事务日志
     */
    public void trimTranslog() {
        verifyNotClosed();
        final Engine engine = getEngine();
        engine.trimUnreferencedTranslogFiles();
    }

    /**
     * Rolls the tranlog generation and cleans unneeded.
     * 滚动到下一事务文件 将之前的数据刷盘 以及尝试丢弃些无用的reader
     */
    public void rollTranslogGeneration() {
        final Engine engine = getEngine();
        engine.rollTranslogGeneration();
    }

    /**
     * 强制触发merge 实际上也是委托给engine实现
     *
     * @param forceMerge
     * @throws IOException
     */
    public void forceMerge(ForceMergeRequest forceMerge) throws IOException {
        verifyActive();
        if (logger.isTraceEnabled()) {
            logger.trace("force merge with {}", forceMerge);
        }
        Engine engine = getEngine();
        engine.forceMerge(forceMerge.flush(), forceMerge.maxNumSegments(),
            forceMerge.onlyExpungeDeletes(), false, false, forceMerge.forceMergeUUID());
    }

    /**
     * Upgrades the shard to the current version of Lucene and returns the minimum segment version
     * 有关版本升级的先忽略
     */
    public org.apache.lucene.util.Version upgrade(UpgradeRequest upgrade) throws IOException {
        verifyActive();
        if (logger.isTraceEnabled()) {
            logger.trace("upgrade with {}", upgrade);
        }
        org.apache.lucene.util.Version previousVersion = minimumCompatibleVersion();
        // we just want to upgrade the segments, not actually forge merge to a single segment
        final Engine engine = getEngine();
        engine.forceMerge(true,  // we need to flush at the end to make sure the upgrade is durable
            Integer.MAX_VALUE, // we just want to upgrade the segments, not actually optimize to a single segment
            false, true, upgrade.upgradeOnlyAncientSegments(), null);
        org.apache.lucene.util.Version version = minimumCompatibleVersion();
        if (logger.isTraceEnabled()) {
            logger.trace("upgraded segments for {} from version {} to version {}", shardId, previousVersion, version);
        }

        return version;
    }

    public org.apache.lucene.util.Version minimumCompatibleVersion() {
        org.apache.lucene.util.Version luceneVersion = null;
        for (Segment segment : getEngine().segments(false)) {
            if (luceneVersion == null || luceneVersion.onOrAfter(segment.getVersion())) {
                luceneVersion = segment.getVersion();
            }
        }
        return luceneVersion == null ? indexSettings.getIndexVersionCreated().luceneVersion : luceneVersion;
    }

    /**
     * Creates a new {@link IndexCommit} snapshot from the currently running engine. All resources referenced by this
     * commit won't be freed until the commit / snapshot is closed.
     *
     * @param flushFirst <code>true</code> if the index should first be flushed to disk / a low level lucene commit should be executed
     *                   flushFirst 在执行前是否需要先刷盘
     */
    public Engine.IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        final IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.CLOSED) {
            return getEngine().acquireLastIndexCommit(flushFirst);
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    /**
     * Snapshots the most recent safe index commit from the currently running engine.
     * All index files referenced by this index commit won't be freed until the commit/snapshot is closed.
     */
    public Engine.IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        final IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.CLOSED) {
            return getEngine().acquireSafeIndexCommit();
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    /**
     * gets a {@link Store.MetadataSnapshot} for the current directory. This method is safe to call in all lifecycle of the index shard,
     * without having to worry about the current state of the engine and concurrent flushes.
     *
     * @throws org.apache.lucene.index.IndexNotFoundException     if no index is found in the current directory
     * @throws org.apache.lucene.index.CorruptIndexException      if the lucene index is corrupted. This can be caused by a checksum
     *                                                            mismatch or an unexpected exception when opening the index reading the
     *                                                            segments file.
     * @throws org.apache.lucene.index.IndexFormatTooOldException if the lucene index is too old to be opened.
     * @throws org.apache.lucene.index.IndexFormatTooNewException if the lucene index is too new to be opened.
     * @throws java.io.FileNotFoundException                      if one or more files referenced by a commit are not present.
     * @throws java.nio.file.NoSuchFileException                  if one or more files referenced by a commit are not present.
     *
     *                      将此时分片对应的dir 下所有文件信息 生成元数据
     */
    public Store.MetadataSnapshot snapshotStoreMetadata() throws IOException {
        assert Thread.holdsLock(mutex) == false : "snapshotting store metadata under mutex";
        Engine.IndexCommitRef indexCommit = null;
        store.incRef();
        try {
            synchronized (engineMutex) {
                // if the engine is not running, we can access the store directly, but we need to make sure no one starts
                // the engine on us. If the engine is running, we can get a snapshot via the deletion policy of the engine.
                final Engine engine = getEngineOrNull();
                if (engine != null) {
                    // 获取最新的 commit信息
                    indexCommit = engine.acquireLastIndexCommit(false);
                }
                // TODO 目前不会返回null
                if (indexCommit == null) {
                    return store.getMetadata(null, true);
                }
            }
            // 根据本次commit信息生成元数据
            return store.getMetadata(indexCommit.getIndexCommit());
        } finally {
            store.decRef();
            IOUtils.close(indexCommit);
        }
    }

    /**
     * Fails the shard and marks the shard store as corrupted if
     * <code>e</code> is caused by index corruption
     */
    public void failShard(String reason, @Nullable Exception e) {
        // fail the engine. This will cause this shard to also be removed from the node's index service.
        getEngine().failEngine(reason, e);
    }

    /**
     * Acquire a lightweight searcher which can be used to rewrite shard search requests.
     */
    public Engine.Searcher acquireCanMatchSearcher() {
        readAllowed();
        markSearcherAccessed();
        return getEngine().acquireSearcher("can_match", Engine.SearcherScope.EXTERNAL);
    }

    public Engine.Searcher acquireSearcher(String source) {
        return acquireSearcher(source, Engine.SearcherScope.EXTERNAL);
    }

    private void markSearcherAccessed() {
        lastSearcherAccess.lazySet(threadPool.relativeTimeInMillis());
    }

    /**
     * 通过 engine获取searcher对象 并进行包装
     *
     * @param source
     * @param scope
     * @return
     */
    private Engine.Searcher acquireSearcher(String source, Engine.SearcherScope scope) {
        readAllowed();
        markSearcherAccessed();
        final Engine engine = getEngine();
        final Engine.Searcher searcher = engine.acquireSearcher(source, scope);
        return wrapSearcher(searcher);
    }

    /**
     * 包装searcher对象
     * 包装函数是在初始化 IndexShard时传入的
     *
     * @param searcher
     * @return
     */
    private Engine.Searcher wrapSearcher(Engine.Searcher searcher) {
        assert ElasticsearchDirectoryReader.unwrap(searcher.getDirectoryReader())
            != null : "DirectoryReader must be an instance or ElasticsearchDirectoryReader";
        boolean success = false;
        try {
            final Engine.Searcher newSearcher = readerWrapper == null ? searcher : wrapSearcher(searcher, readerWrapper);
            assert newSearcher != null;
            success = true;
            return newSearcher;
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to wrap searcher", ex);
        } finally {
            if (success == false) {
                Releasables.close(success, searcher);
            }
        }
    }

    /**
     * 使用相关函数对searcher进行包装
     *
     * @param engineSearcher
     * @param readerWrapper
     * @return
     * @throws IOException
     */
    static Engine.Searcher wrapSearcher(Engine.Searcher engineSearcher,
                                        CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper) throws IOException {
        assert readerWrapper != null;
        // 尝试从层层嵌套中获取 EsReader对象
        // 在ESReader对象上 绑定了某个shard  同时针对每个segmentReader 有一个 subWrapper对象
        final ElasticsearchDirectoryReader elasticsearchDirectoryReader =
            ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(engineSearcher.getDirectoryReader());
        if (elasticsearchDirectoryReader == null) {
            throw new IllegalStateException("Can't wrap non elasticsearch directory reader");
        }
        NonClosingReaderWrapper nonClosingReaderWrapper = new NonClosingReaderWrapper(engineSearcher.getDirectoryReader());
        DirectoryReader reader = readerWrapper.apply(nonClosingReaderWrapper);
        // 这个wrapper函数 不能修改缓存键 以及ESReader
        if (reader != nonClosingReaderWrapper) {
            if (reader.getReaderCacheHelper() != elasticsearchDirectoryReader.getReaderCacheHelper()) {
                throw new IllegalStateException("wrapped directory reader doesn't delegate IndexReader#getCoreCacheKey," +
                    " wrappers must override this method and delegate to the original readers core cache key. Wrapped readers can't be " +
                    "used as cache keys since their are used only per request which would lead to subtle bugs");
            }
            if (ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(reader) != elasticsearchDirectoryReader) {
                // prevent that somebody wraps with a non-filter reader
                throw new IllegalStateException("wrapped directory reader hides actual ElasticsearchDirectoryReader but shouldn't");
            }
        }

        if (reader == nonClosingReaderWrapper) {
            return engineSearcher;
        } else {
            // we close the reader to make sure wrappers can release resources if needed....
            // our NonClosingReaderWrapper makes sure that our reader is not closed
            // 使用新的reader 生成searcher对象
            return new Engine.Searcher(engineSearcher.source(), reader,
                engineSearcher.getSimilarity(), engineSearcher.getQueryCache(), engineSearcher.getQueryCachingPolicy(),
                () -> IOUtils.close(reader, // this will close the wrappers excluding the NonClosingReaderWrapper
                    engineSearcher)); // this will run the closeable on the wrapped engine reader
        }
    }

    private static final class NonClosingReaderWrapper extends FilterDirectoryReader {

        private NonClosingReaderWrapper(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {

                /**
                 * 针对每个 segmentReader 不做任何处理
                 * @param reader
                 * @return
                 */
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return reader;
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new NonClosingReaderWrapper(in);
        }

        /**
         * 并不会关闭底层的文件句柄
         *
         * @throws IOException
         */
        @Override
        protected void doClose() throws IOException {
            // don't close here - mimic the MultiReader#doClose = false behavior that FilterDirectoryReader doesn't have
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

    }

    /**
     * 当removeShard时 会调用该方法
     *
     * @param reason
     * @param flushEngine
     * @throws IOException
     */
    public void close(String reason, boolean flushEngine) throws IOException {
        synchronized (engineMutex) {
            try {
                synchronized (mutex) {
                    changeState(IndexShardState.CLOSED, reason);
                }
            } finally {
                final Engine engine = this.currentEngineReference.getAndSet(null);
                try {
                    if (engine != null && flushEngine) {
                        engine.flushAndClose();
                    }
                } finally {
                    // playing safe here and close the engine even if the above succeeds - close can be called multiple times
                    // Also closing refreshListeners to prevent us from accumulating any more listeners
                    IOUtils.close(engine, globalCheckpointListeners, refreshListeners, pendingReplicationActions);
                    indexShardOperationPermits.close();
                }
            }
        }
    }

    /**
     * 在为某个分片恢复数据前 会调用该函数
     */
    public void preRecovery() {
        // 获取此时分片的状态 一般就是 recovering
        final IndexShardState currentState = this.state; // single volatile read
        if (currentState == IndexShardState.CLOSED) {
            throw new IndexShardNotRecoveringException(shardId, currentState);
        }
        assert currentState == IndexShardState.RECOVERING : "expected a recovering shard " + shardId + " but got " + currentState;
        // 触发前置钩子  目前也没有默认实现
        indexEventListener.beforeIndexShardRecovery(this, indexSettings);
    }

    /**
     * 当该分片的数据恢复流程结束时  修改分片状态
     * @param reason
     * @throws IndexShardStartedException
     * @throws IndexShardRelocatedException
     * @throws IndexShardClosedException
     */
    public void postRecovery(String reason) throws IndexShardStartedException, IndexShardRelocatedException, IndexShardClosedException {
        synchronized (postRecoveryMutex) {
            // we need to refresh again to expose all operations that were index until now. Otherwise
            // we may not expose operations that were indexed with a refresh listener that was immediately
            // responded to in addRefreshListener. The refresh must happen under the same mutex used in addRefreshListener
            // and before moving this shard to POST_RECOVERY state (i.e., allow to read from this shard).
            // 尝试刷新数据
            getEngine().refresh("post_recovery");
            synchronized (mutex) {
                if (state == IndexShardState.CLOSED) {
                    throw new IndexShardClosedException(shardId);
                }
                if (state == IndexShardState.STARTED) {
                    throw new IndexShardStartedException(shardId);
                }
                recoveryState.setStage(RecoveryState.Stage.DONE);
                changeState(IndexShardState.POST_RECOVERY, reason);
            }
        }
    }

    /**
     * called before starting to copy index files over
     */
    public void prepareForIndexRecovery() {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        // 将状态切换成正在恢复lucene的数据
        recoveryState.setStage(RecoveryState.Stage.INDEX);
        assert currentEngineReference.get() == null;
    }

    /**
     * A best effort to bring up this shard to the global checkpoint using the local translog before performing a peer recovery.
     *
     * @return a sequence number that an operation-based peer recovery can start with.
     * This is the first operation after the local checkpoint of the safe commit if exists.
     * 从本地恢复数据 直到 globalCheckpoint
     */
    public long recoverLocallyUpToGlobalCheckpoint() {
        assert Thread.holdsLock(mutex) == false : "recover locally under mutex";

        // 必须处于数据恢复阶段才应该调用该方法
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        assert recoveryState.getStage() == RecoveryState.Stage.INDEX : "unexpected recovery stage [" + recoveryState.getStage() + "]";
        assert routingEntry().recoverySource().getType() == RecoverySource.Type.PEER : "not a peer recovery [" + routingEntry() + "]";

        // safeCommit 就是全局检查点对应的 lucene提交数据
        final Optional<SequenceNumbers.CommitInfo> safeCommit;
        final long globalCheckpoint;
        try {
            // 获取最新的事务日志文件id
            final String translogUUID = store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY);
            // 最新的事务日志文件 记录的全局检查点是最可靠的 通过它反查 saftCommit信息
            // 传入事务id 只是为了校验 而不是通过该事务id去查找
            globalCheckpoint = Translog.readGlobalCheckpoint(translogConfig.getTranslogPath(), translogUUID);

            // 检测本地持久化的数据是否已经达到全局检查点 如果达到返回 Optional.Empty()
            safeCommit = store.findSafeIndexCommit(globalCheckpoint);
        } catch (org.apache.lucene.index.IndexNotFoundException e) {
            logger.trace("skip local recovery as no index commit found");
            return UNASSIGNED_SEQ_NO;
        } catch (Exception e) {
            logger.debug("skip local recovery as failed to find the safe commit", e);
            return UNASSIGNED_SEQ_NO;
        }

        // 代表本地持久化的数据已经超过了 globalCheckpoint 就不需要从本地恢复数据了
        if (safeCommit.isPresent() == false) {
            logger.trace("skip local recovery as no safe commit found");
            return UNASSIGNED_SEQ_NO;
        }
        assert safeCommit.get().localCheckpoint <= globalCheckpoint : safeCommit.get().localCheckpoint + " > " + globalCheckpoint;
        try {
            // 校验逻辑可以先忽略
            maybeCheckIndex(); // check index here and won't do it again if ops-based recovery occurs

            // 进入从事务日志中恢复数据的阶段
            recoveryState.setStage(RecoveryState.Stage.TRANSLOG);

            // 代表本地已经持久化的数据检查点刚好与全局检查点一致  本次不需要从事务日志中重做数据
            if (safeCommit.get().localCheckpoint == globalCheckpoint) {
                logger.trace("skip local recovery as the safe commit is up to date; safe commit {} global checkpoint {}",
                    safeCommit.get(), globalCheckpoint);
                recoveryState.getTranslog().totalLocal(0);
                // 代表从 globalCheckpoint + 1的位置开始拉取远端数据
                return globalCheckpoint + 1;
            }
            // TODO
            if (indexSettings.getIndexMetadata().getState() == IndexMetadata.State.CLOSE ||
                IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(indexSettings.getSettings())) {
                logger.trace("skip local recovery as the index was closed or not allowed to write; safe commit {} global checkpoint {}",
                    safeCommit.get(), globalCheckpoint);
                recoveryState.getTranslog().totalLocal(0);
                return safeCommit.get().localCheckpoint + 1;
            }
            try {
                /**
                 * 这里就是定义了如何通过事务日志文件恢复数据的函数
                 * 下面的操作和 primary从主分片恢复数据的逻辑是一致的
                 * @param engine 使用的引擎对象
                 * @param snapshot 此时日志文件下所有数据(OP) 结合成的快照对象
                 */
                final Engine.TranslogRecoveryRunner translogRecoveryRunner = (engine, snapshot) -> {

                    // 设置本次处理的事务日志文件总长度  每个operation 作为一个单位长度
                    recoveryState.getTranslog().totalLocal(snapshot.totalOperations());
                    // 代表总计恢复了多少 op
                    final int recoveredOps = runTranslogRecovery(engine, snapshot, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                        recoveryState.getTranslog()::incrementRecoveredOperations);
                    // 通过更新数量来设置total
                    recoveryState.getTranslog().totalLocal(recoveredOps); // adjust the total local to reflect the actual count
                    return recoveredOps;
                };
                // 新创建了一个engine对象
                innerOpenEngineAndTranslog(() -> globalCheckpoint);
                // 只需要从事务日志中将数据恢复到全局检查点的位置  而在primary上是尽可能的从本地恢复数据
                getEngine().recoverFromTranslog(translogRecoveryRunner, globalCheckpoint);
                logger.trace("shard locally recovered up to {}", getEngine().getSeqNoStats(globalCheckpoint));
            } finally {
                synchronized (engineMutex) {
                    IOUtils.close(currentEngineReference.getAndSet(null));
                }
            }
        } catch (Exception e) {
            logger.debug(new ParameterizedMessage("failed to recover shard locally up to global checkpoint {}", globalCheckpoint), e);
            return UNASSIGNED_SEQ_NO;
        }
        try {
            // we need to find the safe commit again as we should have created a new one during the local recovery
            final Optional<SequenceNumbers.CommitInfo> newSafeCommit = store.findSafeIndexCommit(globalCheckpoint);
            assert newSafeCommit.isPresent() : "no safe commit found after local recovery";
            return newSafeCommit.get().localCheckpoint + 1;
        } catch (Exception e) {
            logger.debug(new ParameterizedMessage(
                "failed to find the safe commit after recovering shard locally up to global checkpoint {}", globalCheckpoint), e);
            return UNASSIGNED_SEQ_NO;
        }
    }

    /**
     * 将该seq之前的数据清除
     *
     * @param aboveSeqNo
     */
    public void trimOperationOfPreviousPrimaryTerms(long aboveSeqNo) {
        getEngine().trimOperationsFromTranslog(getOperationPrimaryTerm(), aboveSeqNo);
    }

    /**
     * Returns the maximum auto_id_timestamp of all append-only requests have been processed by this shard or the auto_id_timestamp received
     * from the primary via {@link #updateMaxUnsafeAutoIdTimestamp(long)} at the beginning of a peer-recovery or a primary-replica resync.
     *
     * @see #updateMaxUnsafeAutoIdTimestamp(long)
     */
    public long getMaxSeenAutoIdTimestamp() {
        return getEngine().getMaxSeenAutoIdTimestamp();
    }

    /**
     * Since operations stored in soft-deletes do not have max_auto_id_timestamp, the primary has to propagate its max_auto_id_timestamp
     * (via {@link #getMaxSeenAutoIdTimestamp()} of all processed append-only requests to replicas at the beginning of a peer-recovery
     * or a primary-replica resync to force a replica to disable optimization for all append-only requests which are replicated via
     * replication while its retry variants are replicated via recovery without auto_id_timestamp.
     * <p>
     * Without this force-update, a replica can generate duplicate documents (for the same id) if it first receives
     * a retry append-only (without timestamp) via recovery, then an original append-only (with timestamp) via replication.
     */
    public void updateMaxUnsafeAutoIdTimestamp(long maxSeenAutoIdTimestampFromPrimary) {
        getEngine().updateMaxUnsafeAutoIdTimestamp(maxSeenAutoIdTimestampFromPrimary);
    }

    public Engine.Result applyTranslogOperation(Translog.Operation operation, Engine.Operation.Origin origin) throws IOException {
        return applyTranslogOperation(getEngine(), operation, origin);
    }

    /**
     * 执行从事务文件中还原的每个操作
     *
     * @param engine    使用的引擎对象
     * @param operation   本次用于恢复的单个操作
     * @param origin    本次operation 来源 比如从本地事务文件恢复
     * @return
     * @throws IOException
     */
    private Engine.Result applyTranslogOperation(Engine engine, Translog.Operation operation,
                                                 Engine.Operation.Origin origin) throws IOException {
        // If a translog op is replayed on the primary (eg. ccr), we need to use external instead of null for its version type.
        // TODO 先忽略 origin为PRIMARY的情况
        final VersionType versionType = (origin == Engine.Operation.Origin.PRIMARY) ? VersionType.EXTERNAL : null;
        final Engine.Result result;
        switch (operation.opType()) {
            case INDEX:
                final Translog.Index index = (Translog.Index) operation;
                // we set canHaveDuplicates to true all the time such that we de-optimze the translog case and ensure that all
                // autoGeneratedID docs that are coming from the primary are updated correctly.
                // 开始处理 index操作
                // 在engine的写入过程中会判断是否是recovery操作 如果是的话 就不需要再次写入到事务文件中了
                result = applyIndexOperation(engine, index.seqNo(), index.primaryTerm(), index.version(),
                    versionType, UNASSIGNED_SEQ_NO, 0, index.getAutoGeneratedIdTimestamp(), true, origin,
                    // 将index内部的数据抽取出来生成 sourceToParse对象
                    new SourceToParse(shardId.getIndexName(), index.id(), index.source(),
                        XContentHelper.xContentType(index.source()), index.routing()));
                break;
                // 代表事务日志中记录的某个op是删除操作
            case DELETE:
                final Translog.Delete delete = (Translog.Delete) operation;
                result = applyDeleteOperation(engine, delete.seqNo(), delete.primaryTerm(), delete.version(), delete.id(),
                    versionType, UNASSIGNED_SEQ_NO, 0, origin);
                break;
                // 代表本次执行的是一个 NOOP操作
            case NO_OP:
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                result = markSeqNoAsNoop(engine, noOp.seqNo(), noOp.primaryTerm(), noOp.reason(), origin);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
        return result;
    }

    /**
     * Replays translog operations from the provided translog {@code snapshot} to the current engine using the given {@code origin}.
     * The callback {@code onOperationRecovered} is notified after each translog operation is replayed successfully.
     * @param engine  本次相关的引擎对象
     * @param snapshot  涉及到的所有事务文件合并成一个快照对象
     * @param origin  记录了本次operation数据的来源  比如是基于本地事务文件获取
     * @param onOperationRecovered 每当完成一次恢复操作的后置钩子
     * 通过事务日志文件进行恢复
     */
    int runTranslogRecovery(Engine engine, Translog.Snapshot snapshot, Engine.Operation.Origin origin,
                            Runnable onOperationRecovered) throws IOException {
        int opsRecovered = 0;
        Translog.Operation operation;
        // 遍历每个operation对象   通过不断的迭代最终将所有事务日志中的数据都还原了
        // TODO 在恢复期间每次写入操作都应该要检测engine是否完成恢复 甚至不应该被外部发现  那么是怎么做到的呢
        while ((operation = snapshot.next()) != null) {
            try {
                logger.trace("[translog] recover op {}", operation);
                // 通过事务日志的operation对象恢复数据
                Engine.Result result = applyTranslogOperation(engine, operation, origin);
                switch (result.getResultType()) {
                    // 只要在恢复事务操作中 出现一次失败 立即抛出异常
                    case FAILURE:
                        throw result.getFailure();
                    case MAPPING_UPDATE_REQUIRED:
                        throw new IllegalArgumentException("unexpected mapping update: " + result.getRequiredMappingUpdate());
                    case SUCCESS:
                        break;
                    default:
                        throw new AssertionError("Unknown result type [" + result.getResultType() + "]");
                }

                opsRecovered++;
                onOperationRecovered.run();
            } catch (Exception e) {
                // TODO: Don't enable this leniency unless users explicitly opt-in
                if (origin == Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY && ExceptionsHelper.status(e) == RestStatus.BAD_REQUEST) {
                    // mainly for MapperParsingException and Failure to detect xcontent
                    logger.info("ignoring recovery of a corrupt translog entry", e);
                } else {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            }
        }
        return opsRecovered;
    }

    /**
     * 加载最新的全局检查点
     *
     * @throws IOException
     */
    private void loadGlobalCheckpointToReplicationTracker() throws IOException {
        // we have to set it before we open an engine and recover from the translog because
        // acquiring a snapshot from the translog causes a sync which causes the global checkpoint to be pulled in,
        // and an engine can be forced to close in ctor which also causes the global checkpoint to be pulled in.
        // 获取事务日志id
        final String translogUUID = store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY);
        // 每次生成事务文件时 伴随的还有checkPoint  然后内部有一个globalCheckpoint属性 描述了副本组数据的同步状态
        final long globalCheckpoint = Translog.readGlobalCheckpoint(translogConfig.getTranslogPath(), translogUUID);
        // 将从持久化数据中取出的全局检查点 回填到 副本tracker对象
        // 同时会触发挂载在 GlobalCheckpointListeners内的监听器
        // 在副本的数据恢复阶段  副本应该会感知到主分片传输的 globalCheckpoint 因为在创建续约对象时 会将globalCheckpoint 传输到所有副本
        replicationTracker.updateGlobalCheckpointOnReplica(globalCheckpoint, "read from translog checkpoint");
    }

    /**
     * opens the engine on top of the existing lucene engine and translog.
     * Operations from the translog will be replayed to bring lucene up to date.
     * 打开引擎对象 并从事务日志文件中恢复数据  就是将几个基本操作合并到一个方法里
     **/
    public void openEngineAndRecoverFromTranslog() throws IOException {
        assert recoveryState.getStage() == RecoveryState.Stage.INDEX : "unexpected recovery stage [" + recoveryState.getStage() + "]";
        // 校验阶段先不看
        maybeCheckIndex();
        // 切换到通过事务日志恢复数据的阶段
        recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
        // 在进行数据恢复时 存储临时数据的对象
        final RecoveryState.Translog translogRecoveryStats = recoveryState.getTranslog();

        // 这里定义了数据恢复的逻辑
        final Engine.TranslogRecoveryRunner translogRecoveryRunner = (engine, snapshot) -> {
            translogRecoveryStats.totalOperations(snapshot.totalOperations());
            translogRecoveryStats.totalOperationsOnStart(snapshot.totalOperations());
            // 开始执行恢复逻辑
            return runTranslogRecovery(engine, snapshot, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                translogRecoveryStats::incrementRecoveredOperations);
        };
        // 加载全局检查点
        loadGlobalCheckpointToReplicationTracker();
        // 打开引擎
        innerOpenEngineAndTranslog(replicationTracker);
        // 从事务日志中恢复数据
        getEngine().recoverFromTranslog(translogRecoveryRunner, Long.MAX_VALUE);
    }

    /**
     * Opens the engine on top of the existing lucene engine and translog.
     * The translog is kept but its operations won't be replayed.
     * 打开引擎对象 但是不进行数据恢复
     */
    public void openEngineAndSkipTranslogRecovery() throws IOException {
        assert routingEntry().recoverySource().getType() == RecoverySource.Type.PEER : "not a peer recovery [" + routingEntry() + "]";
        assert recoveryState.getStage() == RecoveryState.Stage.TRANSLOG : "unexpected recovery stage [" + recoveryState.getStage() + "]";
        // 尝试从事务日志中加载最新的全局检查点 如果此时内存中维护的更新就不需要处理
        loadGlobalCheckpointToReplicationTracker();
        innerOpenEngineAndTranslog(replicationTracker);
        // 这种情况是通过primary的事务日志进行恢复  所以跳过本地恢复阶段
        getEngine().skipTranslogRecovery();
    }

    /**
     * 开启引擎对象
     *
     * @param globalCheckpointSupplier 获取全局检查点的函数  实际上就对应 replicationTracker 该对象会记录最新的全局检查点
     * @throws IOException
     */
    private void innerOpenEngineAndTranslog(LongSupplier globalCheckpointSupplier) throws IOException {
        assert Thread.holdsLock(mutex) == false : "opening engine under mutex";
        // 此时shard必须处于数据恢复阶段才可以调用该函数
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }

        // 获取引擎配置对象
        final EngineConfig config = newEngineConfig(globalCheckpointSupplier);

        // we disable deletes since we allow for operations to be executed against the shard while recovering
        // but we need to make sure we don't loose deletes until we are done recovering
        // 标记成不可删除
        config.setEnableGcDeletes(false);
        // 从磁盘中读取最新的续约信息 并设置到replicationTracker中  某几种数据恢复方式都会写入一个空的续约信息到文件中
        updateRetentionLeasesOnReplica(loadRetentionLeases());
        assert recoveryState.getRecoverySource().expectEmptyRetentionLeases() == false || getRetentionLeases().leases().isEmpty()
            : "expected empty set of retention leases with recovery source [" + recoveryState.getRecoverySource()
            + "] but got " + getRetentionLeases();
        synchronized (engineMutex) {
            assert currentEngineReference.get() == null : "engine is running";
            verifyNotClosed();
            // we must create a new engine under mutex (see IndexShard#snapshotStoreMetadata).
            // 根据config信息生成引擎对象 如果此时已经有一个引擎对象了 那么进行覆盖
            final Engine newEngine = engineFactory.newReadWriteEngine(config);
            onNewEngine(newEngine);
            currentEngineReference.set(newEngine);
            // We set active because we are now writing operations to the engine; this way,
            // we can flush if we go idle after some time and become inactive.
            active.set(true);
        }
        // time elapses after the engine is created above (pulling the config settings) until we set the engine reference, during
        // which settings changes could possibly have happened, so here we forcefully push any config changes to the new engine.
        // 某些配置可能发生了变化
        onSettingsChanged();
        assert assertSequenceNumbersInCommit();
        assert recoveryState.getStage() == RecoveryState.Stage.TRANSLOG : "TRANSLOG stage expected but was: " + recoveryState.getStage();
    }

    private boolean assertSequenceNumbersInCommit() throws IOException {
        final Map<String, String> userData = SegmentInfos.readLatestCommit(store.directory()).getUserData();
        assert userData.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) : "commit point doesn't contains a local checkpoint";
        assert userData.containsKey(SequenceNumbers.MAX_SEQ_NO) : "commit point doesn't contains a maximum sequence number";
        assert userData.containsKey(Engine.HISTORY_UUID_KEY) : "commit point doesn't contains a history uuid";
        assert userData.get(Engine.HISTORY_UUID_KEY).equals(getHistoryUUID()) : "commit point history uuid ["
            + userData.get(Engine.HISTORY_UUID_KEY) + "] is different than engine [" + getHistoryUUID() + "]";
        assert userData.containsKey(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID) :
            "opening index which was created post 5.5.0 but " + Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID
                + " is not found in commit";
        return true;
    }

    /**
     * 这里将获取事务日志最后一条记录位置的函数设置到 refreshListeners中
     *
     * @param newEngine
     */
    private void onNewEngine(Engine newEngine) {
        assert Thread.holdsLock(engineMutex);
        refreshListeners.setCurrentRefreshLocationSupplier(newEngine::getTranslogLastWriteLocation);
    }

    /**
     * called if recovery has to be restarted after network error / delay **
     * 将recoveryState 重置成init
     */
    public void performRecoveryRestart() throws IOException {
        assert Thread.holdsLock(mutex) == false : "restart recovery under mutex";
        synchronized (engineMutex) {
            assert refreshListeners.pendingCount() == 0 : "we can't restart with pending listeners";
            IOUtils.close(currentEngineReference.getAndSet(null));
            resetRecoveryStage();
        }
    }

    /**
     * If a file-based recovery occurs, a recovery target calls this method to reset the recovery stage.
     * 重置恢复阶段
     */
    public void resetRecoveryStage() {
        assert routingEntry().recoverySource().getType() == RecoverySource.Type.PEER : "not a peer recovery [" + routingEntry() + "]";
        assert currentEngineReference.get() == null;
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState().setStage(RecoveryState.Stage.INIT);
    }

    /**
     * returns stats about ongoing recoveries, both source and target
     */
    public RecoveryStats recoveryStats() {
        return recoveryStats;
    }

    /**
     * Returns the current {@link RecoveryState} if this shard is recovering or has been recovering.
     * Returns null if the recovery has not yet started or shard was not recovered (created via an API).
     */
    @Override
    public RecoveryState recoveryState() {
        return this.recoveryState;
    }

    /**
     * perform the last stages of recovery once all translog operations are done.
     * note that you should still call {@link #postRecovery(String)}.
     * 代表恢复操作已经完成
     */
    public void finalizeRecovery() {
        recoveryState().setStage(RecoveryState.Stage.FINALIZE);
        Engine engine = getEngine();
        // 在fillSeqNoGaps 中可能会写入一些数据 这时就被动的将lucene的数据刷盘
        engine.refresh("recovery_finalization");
        // 因为恢复操作已经完成了 所以开启了删除开关
        engine.config().setEnableGcDeletes(true);
    }

    /**
     * Returns {@code true} if this shard can ignore a recovery attempt made to it (since the already doing/done it)
     */
    public boolean ignoreRecoveryAttempt() {
        IndexShardState state = state(); // one time volatile read
        return state == IndexShardState.POST_RECOVERY || state == IndexShardState.RECOVERING || state == IndexShardState.STARTED ||
            state == IndexShardState.CLOSED;
    }

    /**
     * 检测当前分片的状态是否支持查询数据
     *
     * @throws IllegalIndexShardStateException
     */
    public void readAllowed() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        // 只有started/post_recovery 才允许读取
        if (readAllowedStates.contains(state) == false) {
            throw new IllegalIndexShardStateException(shardId, state, "operations only allowed when shard state is one of " +
                readAllowedStates.toString());
        }
    }

    /**
     * returns true if the {@link IndexShardState} allows reading
     */
    public boolean isReadAllowed() {
        return readAllowedStates.contains(state);
    }

    /**
     * 确保此时是否能进行写入
     *
     * @param origin 主分片/副本
     * @throws IllegalIndexShardStateException
     */
    private void ensureWriteAllowed(Engine.Operation.Origin origin) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read

        // 本次来源是恢复相关的 那么此时的状态必须是恢复状态
        if (origin.isRecovery()) {
            if (state != IndexShardState.RECOVERING) {
                throw new IllegalIndexShardStateException(shardId, state,
                    "operation only allowed when recovering, origin [" + origin + "]");
            }
        } else {
            if (origin == Engine.Operation.Origin.PRIMARY) {
                assert assertPrimaryMode();
            } else if (origin == Engine.Operation.Origin.REPLICA) {
                assert assertReplicationTarget();
            } else {
                assert origin == Engine.Operation.Origin.LOCAL_RESET;
                assert getActiveOperationsCount() == OPERATIONS_BLOCKED
                    : "locally resetting without blocking operations, active operations are [" + getActiveOperations() + "]";
            }
            // 检测当前状态是否属于可写入状态
            if (writeAllowedStates.contains(state) == false) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard state is one of " +
                    writeAllowedStates + ", origin [" + origin + "]");
            }
        }
    }

    private boolean assertPrimaryMode() {
        assert shardRouting.primary() && replicationTracker.isPrimaryMode() : "shard " + shardRouting +
            " is not a primary shard in primary mode";
        return true;
    }

    private boolean assertReplicationTarget() {
        assert replicationTracker.isPrimaryMode() == false : "shard " + shardRouting + " in primary mode cannot be a replication target";
        return true;
    }

    private void verifyNotClosed() throws IllegalIndexShardStateException {
        verifyNotClosed(null);
    }

    /**
     * 确保此时 state 不属于closed状态
     *
     * @param suppressed
     * @throws IllegalIndexShardStateException
     */
    private void verifyNotClosed(Exception suppressed) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state == IndexShardState.CLOSED) {
            final IllegalIndexShardStateException exc = new IndexShardClosedException(shardId, "operation only allowed when not closed");
            if (suppressed != null) {
                exc.addSuppressed(suppressed);
            }
            throw exc;
        }
    }

    protected final void verifyActive() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED) {
            throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard is active");
        }
    }

    /**
     * Returns number of heap bytes used by the indexing buffer for this shard, or 0 if the shard is closed
     */
    public long getIndexBufferRAMBytesUsed() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            return 0;
        }
        try {
            return engine.getIndexBufferRAMBytesUsed();
        } catch (AlreadyClosedException ex) {
            return 0;
        }
    }

    /**
     * 增加一个处理分片失败的钩子
     *
     * @param onShardFailure
     */
    public void addShardFailureCallback(Consumer<ShardFailure> onShardFailure) {
        this.shardEventListener.delegates.add(onShardFailure);
    }

    /**
     * Called by {@link IndexingMemoryController} to check whether more than {@code inactiveTimeNS} has passed since the last
     * indexing operation, so we can flush the index.
     * 当当前时间距离上次写入时间 超过了静默时间后 允许将数据刷盘
     */
    public void flushOnIdle(long inactiveTimeNS) {
        // 首先确保engine没有被关闭
        Engine engineOrNull = getEngineOrNull();

        // 代表长时间没有刷盘
        if (engineOrNull != null && System.nanoTime() - engineOrNull.getLastWriteNanos() >= inactiveTimeNS) {
            // active 用于标记近期是否发生过操作 如果还是false 就不需要刷盘了
            boolean wasActive = active.getAndSet(false);
            if (wasActive) {
                logger.debug("flushing shard on inactive");
                threadPool.executor(ThreadPool.Names.FLUSH).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        if (state != IndexShardState.CLOSED) {
                            logger.warn("failed to flush shard on inactive", e);
                        }
                    }

                    /**
                     * 异步执行刷盘操作
                     */
                    @Override
                    protected void doRun() {
                        flush(new FlushRequest().waitIfOngoing(false).force(false));
                        periodicFlushMetric.inc();
                    }
                });
            }
        }
    }

    public boolean isActive() {
        return active.get();
    }

    public ShardPath shardPath() {
        return path;
    }

    /**
     * 从本地分片恢复数据   与从事务日志中恢复有什么不同???
     *
     * @param mappingUpdateConsumer
     * @param localShards           当前对象不就是indexShard吗  难道有一组跟当前对象存储一样数据的分片???
     * @param listener
     * @throws IOException
     */
    void recoverFromLocalShards(Consumer<MappingMetadata> mappingUpdateConsumer, List<IndexShard> localShards,
                                ActionListener<Boolean> listener) throws IOException {
        assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
        assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS : "invalid recovery type: " +
            recoveryState.getRecoverySource();
        final List<LocalShardSnapshot> snapshots = new ArrayList<>();
        // 插入一个前置钩子
        final ActionListener<Boolean> recoveryListener = ActionListener.runBefore(listener, () -> IOUtils.close(snapshots));
        boolean success = false;
        try {
            for (IndexShard shard : localShards) {
                snapshots.add(new LocalShardSnapshot(shard));
            }
            // we are the first primary, recover from the gateway
            // if its post api allocation, the index should exists
            assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
            StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
            // 这里会涉及到 mapping的使用
            storeRecovery.recoverFromLocalShards(mappingUpdateConsumer, this, snapshots, recoveryListener);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.close(snapshots);
            }
        }
    }

    /**
     * 从本地store的文件恢复shard的数据
     *
     * @param listener  处理结果的监听器
     */
    public void recoverFromStore(ActionListener<Boolean> listener) {
        // we are the first primary, recover from the gateway
        // if its post api allocation, the index should exists
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        assert shardRouting.initializing() : "can only start recovery on initializing shard";
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        storeRecovery.recoverFromStore(this, listener);
    }

    /**
     * 从仓库中恢复数据
     *
     * @param repository
     * @param listener
     */
    public void restoreFromRepository(Repository repository, ActionListener<Boolean> listener) {
        try {
            assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
            assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT : "invalid recovery type: " +
                recoveryState.getRecoverySource();
            StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
            // TODO 这里使用到了 repository 但是还不知道是什么时候写入仓库的所以先不细看
            storeRecovery.recoverFromRepository(this, repository, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Tests whether or not the engine should be flushed periodically.
     * This test is based on the current size of the translog compared to the configured flush threshold size.
     *
     * @return {@code true} if the engine should be flushed
     */
    boolean shouldPeriodicallyFlush() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                return engine.shouldPeriodicallyFlush();
            } catch (final AlreadyClosedException e) {
                // we are already closed, no need to flush or roll
            }
        }
        return false;
    }

    /**
     * Tests whether or not the translog generation should be rolled to a new generation. This test is based on the size of the current
     * generation compared to the configured generation threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    boolean shouldRollTranslogGeneration() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                return engine.shouldRollTranslogGeneration();
            } catch (final AlreadyClosedException e) {
                // we are already closed, no need to flush or roll
            }
        }
        return false;
    }

    /**
     * 当配置项发生变化时 触发该函数
     */
    public void onSettingsChanged() {
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null) {
            engineOrNull.onSettingsChanged();
        }
    }

    /**
     * Acquires a lock on the translog files and Lucene soft-deleted documents to prevent them from being trimmed
     */
    public Closeable acquireHistoryRetentionLock() {
        return getEngine().acquireHistoryRetentionLock();
    }

    /**
     * Checks if we have a completed history of operations since the given starting seqno (inclusive).
     * This method should be called after acquiring the retention lock; See {@link #acquireHistoryRetentionLock()}
     */
    public boolean hasCompleteHistoryOperations(String reason, long startingSeqNo) {
        return getEngine().hasCompleteOperationHistory(reason, startingSeqNo);
    }

    /**
     * Gets the minimum retained sequence number for this engine.
     *
     * @return the minimum retained sequence number
     */
    public long getMinRetainedSeqNo() {
        return getEngine().getMinRetainedSeqNo();
    }

    /**
     * Creates a new changes snapshot for reading operations whose seq_no are between {@code fromSeqNo}(inclusive)
     * and {@code toSeqNo}(inclusive). The caller has to close the returned snapshot after finishing the reading.
     *
     * @param source            the source of the request
     * @param fromSeqNo         the from seq_no (inclusive) to read
     * @param toSeqNo           the to seq_no (inclusive) to read
     * @param requiredFullRange if {@code true} then {@link Translog.Snapshot#next()} will throw {@link IllegalStateException}
     *                          if any operation between {@code fromSeqNo} and {@code toSeqNo} is missing.
     *                          This parameter should be only enabled when the entire requesting range is below the global checkpoint.
     */
    public Translog.Snapshot newChangesSnapshot(String source, long fromSeqNo,
                                                long toSeqNo, boolean requiredFullRange) throws IOException {
        return getEngine().newChangesSnapshot(source, mapperService, fromSeqNo, toSeqNo, requiredFullRange);
    }

    public List<Segment> segments(boolean verbose) {
        return getEngine().segments(verbose);
    }

    public String getHistoryUUID() {
        return getEngine().getHistoryUUID();
    }

    public IndexEventListener getIndexEventListener() {
        return indexEventListener;
    }

    /**
     * 将当前分片标记为阻塞状态  场景就是 IndexingMemoryController 定期检测 发现内存使用量超标 会将几个特别大的分片标记成阻塞状态
     */
    public void activateThrottling() {
        try {
            getEngine().activateThrottling();
        } catch (AlreadyClosedException ex) {
            // ignore
        }
    }

    /**
     * IndexMemoryController 会定期检测内存使用是否超量 是的话会将使用量比较高的几个分片标记成阻塞状态
     * 当内存使用量降下来时 会取消标记
     */
    public void deactivateThrottling() {
        try {
            getEngine().deactivateThrottling();
        } catch (AlreadyClosedException ex) {
            // ignore
        }
    }

    private void handleRefreshException(Exception e) {
        if (e instanceof AlreadyClosedException) {
            // ignore
        } else if (e instanceof RefreshFailedEngineException) {
            RefreshFailedEngineException rfee = (RefreshFailedEngineException) e;
            if (rfee.getCause() instanceof InterruptedException) {
                // ignore, we are being shutdown
            } else if (rfee.getCause() instanceof ClosedByInterruptException) {
                // ignore, we are being shutdown
            } else if (rfee.getCause() instanceof ThreadInterruptedException) {
                // ignore, we are being shutdown
            } else {
                if (state != IndexShardState.CLOSED) {
                    logger.warn("Failed to perform engine refresh", e);
                }
            }
        } else {
            if (state != IndexShardState.CLOSED) {
                logger.warn("Failed to perform engine refresh", e);
            }
        }
    }

    /**
     * Called when our shard is using too much heap and should move buffered indexed/deleted documents to disk.
     * 当此时分片消耗了太多内存时 会通过该方法将数据强制刷盘
     */
    public void writeIndexingBuffer() {
        try {
            Engine engine = getEngine();
            engine.writeIndexingBuffer();
        } catch (Exception e) {
            handleRefreshException(e);
        }
    }

    /**
     * Notifies the service to update the local checkpoint for the shard with the provided allocation ID. See
     * {@link ReplicationTracker#updateLocalCheckpoint(String, long)} for
     * details.
     *
     * @param allocationId the allocation ID of the shard to update the local checkpoint for   每个allocation 都会维护一个checkpoint
     * @param checkpoint   the local checkpoint for the shard
     *                     主分片会维护副本的 localCheckpoint/globalCheckpoint信息  这里就是某个副本上报了值
     */
    public void updateLocalCheckpointForShard(final String allocationId, final long checkpoint) {
        assert assertPrimaryMode();
        verifyNotClosed();
        replicationTracker.updateLocalCheckpoint(allocationId, checkpoint);
    }

    /**
     * Update the local knowledge of the persisted global checkpoint for the specified allocation ID.
     *
     * @param allocationId     the allocation ID to update the global checkpoint for
     * @param globalCheckpoint the global checkpoint
     *                         更新某个分片的全局检查点  (主分片会管理所有副本此时的检查点情况)
     */
    public void updateGlobalCheckpointForShard(final String allocationId, final long globalCheckpoint) {
        assert assertPrimaryMode();
        verifyNotClosed();
        replicationTracker.updateGlobalCheckpointForShard(allocationId, globalCheckpoint);
    }

    /**
     * Add a global checkpoint listener. If the global checkpoint is equal to or above the global checkpoint the listener is waiting for,
     * then the listener will be notified immediately via an executor (so possibly not on the current thread). If the specified timeout
     * elapses before the listener is notified, the listener will be notified with an {@link TimeoutException}. A caller may pass null to
     * specify no timeout.
     *
     * @param waitingForGlobalCheckpoint the global checkpoint the listener is waiting for
     * @param listener                   the listener
     * @param timeout                    the timeout
     *                                   只有当全局检查点达到指定的值时才会触发
     */
    public void addGlobalCheckpointListener(
        final long waitingForGlobalCheckpoint,
        final GlobalCheckpointListeners.GlobalCheckpointListener listener,
        final TimeValue timeout) {
        this.globalCheckpointListeners.add(waitingForGlobalCheckpoint, listener, timeout);
    }

    private void ensureSoftDeletesEnabled(String feature) {
        if (indexSettings.isSoftDeleteEnabled() == false) {
            String message = feature + " requires soft deletes but " + indexSettings.getIndex() + " does not have soft deletes enabled";
            assert false : message;
            throw new IllegalStateException(message);
        }
    }

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
    public Tuple<Boolean, RetentionLeases> getRetentionLeases(final boolean expireLeases) {
        assert expireLeases == false || assertPrimaryMode();
        verifyNotClosed();
        return replicationTracker.getRetentionLeases(expireLeases);
    }

    public RetentionLeaseStats getRetentionLeaseStats() {
        verifyNotClosed();
        return new RetentionLeaseStats(getRetentionLeases());
    }

    /**
     * Adds a new retention lease.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number  续约号码
     * @param source                  the source of the retention lease
     * @param listener                the callback when the retention lease is successfully added and synced to replicas
     * @return the new retention lease
     * @throws IllegalArgumentException if the specified retention lease already exists
     *                                  增加一个新的续约信息
     */
    public RetentionLease addRetentionLease(
        final String id,
        final long retainingSequenceNumber,
        final String source,
        final ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(listener);
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled("retention leases");
        // 获取续约锁
        try (Closeable ignore = acquireHistoryRetentionLock()) {
            // 当要保留所有的续约对象时 获取此时最小的续约号
            final long actualRetainingSequenceNumber =
                retainingSequenceNumber == RETAIN_ALL ? getMinRetainedSeqNo() : retainingSequenceNumber;
            return replicationTracker.addRetentionLease(id, actualRetainingSequenceNumber, source, listener);
        } catch (final IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Renews an existing retention lease.
     *
     * @param id                      the identifier of the retention lease  同于定位续约对象
     * @param retainingSequenceNumber the retaining sequence number
     * @param source                  the source of the retention lease
     * @return the renewed retention lease
     * @throws IllegalArgumentException if the specified retention lease does not exist
     *                                  刷新某个续约对象的数据
     */
    public RetentionLease renewRetentionLease(final String id, final long retainingSequenceNumber, final String source) {
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled("retention leases");
        try (Closeable ignore = acquireHistoryRetentionLock()) {
            final long actualRetainingSequenceNumber =
                retainingSequenceNumber == RETAIN_ALL ? getMinRetainedSeqNo() : retainingSequenceNumber;
            return replicationTracker.renewRetentionLease(id, actualRetainingSequenceNumber, source);
        } catch (final IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Removes an existing retention lease.
     *
     * @param id       the identifier of the retention lease
     * @param listener the callback when the retention lease is successfully removed and synced to replicas
     *                 移除某个续约对象
     */
    public void removeRetentionLease(final String id, final ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(listener);
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled("retention leases");
        replicationTracker.removeRetentionLease(id, listener);
    }

    /**
     * Updates retention leases on a replica.
     *
     * @param retentionLeases the retention leases
     *                        在副本上更新续约信息
     */
    public void updateRetentionLeasesOnReplica(final RetentionLeases retentionLeases) {
        assert assertReplicationTarget();
        verifyNotClosed();
        replicationTracker.updateRetentionLeasesOnReplica(retentionLeases);
    }

    /**
     * Loads the latest retention leases from their dedicated state file.
     *
     * @return the retention leases
     * @throws IOException if an I/O exception occurs reading the retention leases
     *                     对应写入续约信息的操作 从相同path下读取数据
     */
    public RetentionLeases loadRetentionLeases() throws IOException {
        verifyNotClosed();
        return replicationTracker.loadRetentionLeases(path.getShardStatePath());
    }

    /**
     * Persists the current retention leases to their dedicated state file.
     *
     * @throws WriteStateException if an exception occurs writing the state file
     * 将此时的续约信息进行持久化
     */
    public void persistRetentionLeases() throws WriteStateException {
        verifyNotClosed();
        replicationTracker.persistRetentionLeases(path.getShardStatePath());
    }

    public boolean assertRetentionLeasesPersisted() throws IOException {
        return replicationTracker.assertRetentionLeasesPersisted(path.getShardStatePath());
    }

    /**
     * Syncs the current retention leases to all replicas.
     * 在 IndexService中会定期执行续约任务
     */
    public void syncRetentionLeases() {
        assert assertPrimaryMode();
        verifyNotClosed();
        // 根据条件找到需要更新的续约对象 并进行更新
        replicationTracker.renewPeerRecoveryRetentionLeases();
        final Tuple<Boolean, RetentionLeases> retentionLeases = getRetentionLeases(true);
        if (retentionLeases.v1()) {
            logger.trace("syncing retention leases [{}] after expiration check", retentionLeases.v2());

            // 续约信息同步器
            retentionLeaseSyncer.sync(
                shardId,
                shardRouting.allocationId().getId(),
                getPendingPrimaryTerm(),
                retentionLeases.v2(),
                ActionListener.wrap(
                    r -> {
                    },
                    e -> logger.warn(new ParameterizedMessage(
                            "failed to sync retention leases [{}] after expiration check",
                            retentionLeases),
                        e)));
        } else {
            logger.trace("background syncing retention leases [{}] after expiration check", retentionLeases.v2());
            retentionLeaseSyncer.backgroundSync(
                shardId, shardRouting.allocationId().getId(), getPendingPrimaryTerm(), retentionLeases.v2());
        }
    }

    /**
     * Called when the recovery process for a shard has opened the engine on the target shard. Ensures that the right data structures
     * have been set up locally to track local checkpoint information for the shard and that the shard is added to the replication group.
     *
     * @param allocationId the allocation ID of the shard for which recovery was initiated
     *                     将某个CheckpointState.track 标识设置为true
     */
    public void initiateTracking(final String allocationId) {
        assert assertPrimaryMode();
        replicationTracker.initiateTracking(allocationId);
    }

    /**
     * Marks the shard with the provided allocation ID as in-sync with the primary shard. See
     * {@link ReplicationTracker#markAllocationIdAsInSync(String, long)}
     * for additional details.
     *
     * @param allocationId    the allocation ID of the shard to mark as in-sync
     * @param localCheckpoint the current local checkpoint on the shard
     *                        当某个副本通过primary 完成了recovery 就会进入到primary.in-sync 队列中
     */
    public void markAllocationIdAsInSync(final String allocationId, final long localCheckpoint) throws InterruptedException {
        assert assertPrimaryMode();
        replicationTracker.markAllocationIdAsInSync(allocationId, localCheckpoint);
    }

    /**
     * Returns the persisted local checkpoint for the shard.
     *
     * @return the local checkpoint
     */
    public long getLocalCheckpoint() {
        return getEngine().getPersistedLocalCheckpoint();
    }

    /**
     * Returns the global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public long getLastKnownGlobalCheckpoint() {
        return replicationTracker.getGlobalCheckpoint();
    }

    /**
     * Returns the latest global checkpoint value that has been persisted in the underlying storage (i.e. translog's checkpoint)
     */
    public long getLastSyncedGlobalCheckpoint() {
        return getEngine().getLastSyncedGlobalCheckpoint();
    }

    /**
     * Get the local knowledge of the global checkpoints for all in-sync allocation IDs.
     *
     * @return a map from allocation ID to the local knowledge of the global checkpoint for that allocation ID
     * 返回所有 insync 为true的CheckpointState对应的 globalCheckpoint
     */
    public ObjectLongMap<String> getInSyncGlobalCheckpoints() {
        assert assertPrimaryMode();
        verifyNotClosed();
        return replicationTracker.getInSyncGlobalCheckpoints();
    }

    /**
     * Syncs the global checkpoint to the replicas if the global checkpoint on at least one replica is behind the global checkpoint on the
     * primary.
     * 在该indexShard相关的 IndexService中 每间隔一段时间就会触发该函数 同步全局检查点
     * 主要功能都是通过globalCheckpointSyncer 实现的
     */
    public void maybeSyncGlobalCheckpoint(final String reason) {
        verifyNotClosed();
        assert shardRouting.primary() : "only call maybeSyncGlobalCheckpoint on primary shard";

        // 首先要求当前分片是主分片 并且已经完成recovery过程
        if (replicationTracker.isPrimaryMode() == false) {
            return;
        }
        assert assertPrimaryMode();
        // only sync if there are no operations in flight, or when using async durability
        // stats 只是一个简单的bean对象
        final SeqNoStats stats = getEngine().getSeqNoStats(replicationTracker.getGlobalCheckpoint());
        final boolean asyncDurability = indexSettings().getTranslogDurability() == Translog.Durability.ASYNC;

        // TODO 之后再好好理解这些条件的含义
        if (stats.getMaxSeqNo() == stats.getGlobalCheckpoint() || asyncDurability) {

            // 只需要更新处于 in-sync 内的副本的globalCheckpoint
            final ObjectLongMap<String> globalCheckpoints = getInSyncGlobalCheckpoints();
            final long globalCheckpoint = replicationTracker.getGlobalCheckpoint();
            // async durability means that the local checkpoint might lag (as it is only advanced on fsync)
            // periodically ask for the newest local checkpoint by syncing the global checkpoint, so that ultimately the global
            // checkpoint can be synced. Also take into account that a shard might be pending sync, which means that it isn't
            // in the in-sync set just yet but might be blocked on waiting for its persisted local checkpoint to catch up to
            // the global checkpoint.
            // 检测是否需要同步全局检查点
            final boolean syncNeeded =
                (asyncDurability && (stats.getGlobalCheckpoint() < stats.getMaxSeqNo() || replicationTracker.pendingInSync()))
                    // check if the persisted global checkpoint
                    // 下面的条件是发现 主分片维护的每个副本此时的全局检查点 有某个小于主分片最新生成的全局检查点
                    || StreamSupport
                    .stream(globalCheckpoints.values().spliterator(), false)
                    .anyMatch(v -> v.value < globalCheckpoint);
            // only sync if index is not closed and there is a shard lagging the primary
            if (syncNeeded && indexSettings.getIndexMetadata().getState() == IndexMetadata.State.OPEN) {
                logger.trace("syncing global checkpoint for [{}]", reason);

                // 实际上是 IndicesClusterStateService#updateGlobalCheckpointForShard
                globalCheckpointSyncer.run();
            }
        }
    }

    /**
     * Returns the current replication group for the shard.
     *
     * @return the replication group
     * 获取某个分片的副本组
     */
    public ReplicationGroup getReplicationGroup() {
        assert assertPrimaryMode();
        verifyNotClosed();
        ReplicationGroup replicationGroup = replicationTracker.getReplicationGroup();
        // PendingReplicationActions is dependent on ReplicationGroup. Every time we expose ReplicationGroup,
        // ensure PendingReplicationActions is updated with the newest version to prevent races.
        pendingReplicationActions.accept(replicationGroup);
        return replicationGroup;
    }

    /**
     * Returns the pending replication actions for the shard.
     *
     * @return the pending replication actions
     */
    public PendingReplicationActions getPendingReplicationActions() {
        assert assertPrimaryMode();
        verifyNotClosed();
        return pendingReplicationActions;
    }

    /**
     * Updates the global checkpoint on a replica shard after it has been updated by the primary.
     *
     * @param globalCheckpoint the global checkpoint    当前全局检查点
     * @param reason           the reason the global checkpoint was updated
     *                         比如执行了需要通知到所有分片的任务 在主分片执行完成后会携带主分片的globalCheckpoint
     *                         并传播到副本分片上 如果发生比起之前的全局检查点大 就进行更新
     */
    public void updateGlobalCheckpointOnReplica(final long globalCheckpoint, final String reason) {
        assert assertReplicationTarget();
        final long localCheckpoint = getLocalCheckpoint();
        if (globalCheckpoint > localCheckpoint) {
            /*
             * This can happen during recovery when the shard has started its engine but recovery is not finalized and is receiving global
             * checkpoint updates. However, since this shard is not yet contributing to calculating the global checkpoint, it can be the
             * case that the global checkpoint update from the primary is ahead of the local checkpoint on this shard. In this case, we
             * ignore the global checkpoint update. This can happen if we are in the translog stage of recovery. Prior to this, the engine
             * is not opened and this shard will not receive global checkpoint updates, and after this the shard will be contributing to
             * calculations of the global checkpoint. However, we can not assert that we are in the translog stage of recovery here as
             * while the global checkpoint update may have emanated from the primary when we were in that state, we could subsequently move
             * to recovery finalization, or even finished recovery before the update arrives here.
             */
            assert state() != IndexShardState.POST_RECOVERY && state() != IndexShardState.STARTED :
                "supposedly in-sync shard copy received a global checkpoint [" + globalCheckpoint + "] " +
                    "that is higher than its local checkpoint [" + localCheckpoint + "]";
            return;
        }
        replicationTracker.updateGlobalCheckpointOnReplica(globalCheckpoint, reason);
    }

    /**
     * Updates the known allocation IDs and the local checkpoints for the corresponding allocations from a primary relocation source.
     *
     * @param primaryContext the sequence number context
     */
    public void activateWithPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) {
        assert shardRouting.primary() && shardRouting.isRelocationTarget() :
            "only primary relocation target can update allocation IDs from primary context: " + shardRouting;
        assert primaryContext.getCheckpointStates().containsKey(routingEntry().allocationId().getId()) :
            "primary context [" + primaryContext + "] does not contain relocation target [" + routingEntry() + "]";
        assert getLocalCheckpoint() == primaryContext.getCheckpointStates().get(routingEntry().allocationId().getId())
            .getLocalCheckpoint() || indexSettings().getTranslogDurability() == Translog.Durability.ASYNC :
            "local checkpoint [" + getLocalCheckpoint() + "] does not match checkpoint from primary context [" + primaryContext + "]";
        synchronized (mutex) {
            replicationTracker.activateWithPrimaryContext(primaryContext); // make changes to primaryMode flag only under mutex
        }
        ensurePeerRecoveryRetentionLeasesExist();
    }

    private void ensurePeerRecoveryRetentionLeasesExist() {
        threadPool.generic().execute(() -> replicationTracker.createMissingPeerRecoveryRetentionLeases(ActionListener.wrap(
            r -> logger.trace("created missing peer recovery retention leases"),
            e -> logger.debug("failed creating missing peer recovery retention leases", e))));
    }

    /**
     * Check if there are any recoveries pending in-sync.
     *
     * @return {@code true} if there is at least one shard pending in-sync, otherwise false
     * 当前是否在等待某个节点的同步  也就是某个节点此时落后了
     */
    public boolean pendingInSync() {
        assert assertPrimaryMode();
        return replicationTracker.pendingInSync();
    }

    /**
     * Should be called for each no-op update operation to increment relevant statistics.
     */
    public void noopUpdate() {
        internalIndexingStats.noopUpdate();
    }

    /**
     * 进入检验index的阶段
     */
    public void maybeCheckIndex() {
        recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX);
        // 代表需要校验索引文件是否没被修改
        if (Booleans.isTrue(checkIndexOnStartup) || "checksum".equals(checkIndexOnStartup)) {
            try {
                checkIndex();
            } catch (IOException ex) {
                throw new RecoveryFailedException(recoveryState, "check index failed", ex);
            }
        }
    }

    void checkIndex() throws IOException {
        if (store.tryIncRef()) {
            try {
                doCheckIndex();
            } catch (IOException e) {
                store.markStoreCorrupted(e);
                throw e;
            } finally {
                store.decRef();
            }
        }
    }

    /**
     * 检查索引文件
     *
     * @throws IOException
     */
    private void doCheckIndex() throws IOException {
        long timeNS = System.nanoTime();
        if (!Lucene.indexExists(store.directory())) {
            return;
        }
        BytesStreamOutput os = new BytesStreamOutput();
        PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());

        if ("checksum".equals(checkIndexOnStartup)) {
            // physical verification only: verify all checksums for the latest commit
            IOException corrupt = null;
            // 读取segment_N 文件 获取此时的checkSum 文件长度等等信息
            MetadataSnapshot metadata = snapshotStoreMetadata();
            for (Map.Entry<String, StoreFileMetadata> entry : metadata.asMap().entrySet()) {
                try {
                    Store.checkIntegrity(entry.getValue(), store.directory());
                    out.println("checksum passed: " + entry.getKey());
                } catch (IOException exc) {
                    out.println("checksum failed: " + entry.getKey());
                    exc.printStackTrace(out);
                    corrupt = exc;
                }
            }
            out.flush();
            if (corrupt != null) {
                logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                throw corrupt;
            }
        } else {
            // full checkindex
            final CheckIndex.Status status = store.checkIndex(out);
            out.flush();
            if (!status.clean) {
                if (state == IndexShardState.CLOSED) {
                    // ignore if closed....
                    return;
                }
                logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                throw new IOException("index check failure");
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("check index [success]\n{}", os.bytes().utf8ToString());
        }

        recoveryState.getVerifyIndex().checkIndexTime(Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - timeNS)));
    }

    Engine getEngine() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            throw new AlreadyClosedException("engine is closed");
        }
        return engine;
    }

    /**
     * NOTE: returns null if engine is not yet started (e.g. recovery phase 1, copying over index files, is still running), or if engine is
     * closed.
     */
    protected Engine getEngineOrNull() {
        return this.currentEngineReference.get();
    }

    /**
     * 每个indexShard在被创建后 就会通过recoverySource还原数据
     *
     * @param recoveryState         描述恢复相关的状态 比如现在所处的阶段
     * @param recoveryTargetService 当需要从其它节点获取数据时 会使用这个对象
     * @param recoveryListener      通过在恢复数据的过程中插入钩子 实现一些功能
     * @param repositoriesService
     * @param mappingUpdateConsumer
     * @param indicesService
     */
    public void startRecovery(RecoveryState recoveryState, PeerRecoveryTargetService recoveryTargetService,
                              PeerRecoveryTargetService.RecoveryListener recoveryListener, RepositoriesService repositoriesService,
                              Consumer<MappingMetadata> mappingUpdateConsumer,
                              IndicesService indicesService) {
        // TODO: Create a proper object to encapsulate the recovery context
        // all of the current methods here follow a pattern of:
        // resolve context which isn't really dependent on the local shards and then async
        // call some external method with this pointer.
        // with a proper recovery context object we can simply change this to:
        // startRecovery(RecoveryState recoveryState, ShardRecoverySource source ) {
        //     markAsRecovery("from " + source.getShortDescription(), recoveryState);
        //     threadPool.generic().execute()  {
        //           onFailure () { listener.failure() };
        //           doRun() {
        //                if (source.recover(this)) {
        //                  recoveryListener.onRecoveryDone(recoveryState);
        //                }
        //           }
        //     }}
        // }
        assert recoveryState.getRecoverySource().equals(shardRouting.recoverySource());
        switch (recoveryState.getRecoverySource().getType()) {
            case EMPTY_STORE:
            case EXISTING_STORE:
                // 代表通过之前事务日志中存储的数据进行恢复
                executeRecovery("from store", recoveryState, recoveryListener, this::recoverFromStore);
                break;
                // 副本节点会通过 拉取primary的数据进行恢复
            case PEER:
                try {
                    markAsRecovering("from " + recoveryState.getSourceNode(), recoveryState);
                    // 从远端拉取数据进行恢复   recoveryState.getSourceNode() 对应主分片所在的节点
                    recoveryTargetService.startRecovery(this, recoveryState.getSourceNode(), recoveryListener);
                } catch (Exception e) {
                    failShard("corrupted preexisting index", e);
                    recoveryListener.onRecoveryFailure(recoveryState,
                        new RecoveryFailedException(recoveryState, null, e), true);
                }
                break;
            // 从repository中恢复数据 暂时还不理解
            case SNAPSHOT:
                final String repo = ((SnapshotRecoverySource) recoveryState.getRecoverySource()).snapshot().getRepository();
                executeRecovery("from snapshot",
                    recoveryState, recoveryListener, l -> restoreFromRepository(repositoriesService.repository(repo), l));
                break;
            case LOCAL_SHARDS:
                final IndexMetadata indexMetadata = indexSettings().getIndexMetadata();
                final Index resizeSourceIndex = indexMetadata.getResizeSourceIndex();
                final List<IndexShard> startedShards = new ArrayList<>();
                final IndexService sourceIndexService = indicesService.indexService(resizeSourceIndex);
                final Set<ShardId> requiredShards;
                final int numShards;
                if (sourceIndexService != null) {
                    requiredShards = IndexMetadata.selectRecoverFromShards(shardId().id(),
                        sourceIndexService.getMetadata(), indexMetadata.getNumberOfShards());
                    for (IndexShard shard : sourceIndexService) {
                        if (shard.state() == IndexShardState.STARTED && requiredShards.contains(shard.shardId())) {
                            startedShards.add(shard);
                        }
                    }
                    numShards = requiredShards.size();
                } else {
                    numShards = -1;
                    requiredShards = Collections.emptySet();
                }

                if (numShards == startedShards.size()) {
                    assert requiredShards.isEmpty() == false;
                    executeRecovery("from local shards", recoveryState, recoveryListener,
                        l -> recoverFromLocalShards(mappingUpdateConsumer,
                            startedShards.stream().filter((s) -> requiredShards.contains(s.shardId())).collect(Collectors.toList()), l));
                } else {
                    final RuntimeException e;
                    if (numShards == -1) {
                        e = new IndexNotFoundException(resizeSourceIndex);
                    } else {
                        e = new IllegalStateException("not all required shards of index " + resizeSourceIndex
                            + " are started yet, expected " + numShards + " found " + startedShards.size() + " can't recover shard "
                            + shardId());
                    }
                    throw e;
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown recovery source " + recoveryState.getRecoverySource());
        }
    }

    /**
     * 开始执行数据恢复操作
     * @param reason
     * @param recoveryState
     * @param recoveryListener
     * @param action
     */
    private void executeRecovery(String reason, RecoveryState recoveryState, PeerRecoveryTargetService.RecoveryListener recoveryListener,
                                 CheckedConsumer<ActionListener<Boolean>, Exception> action) {
        // 标记成正在恢复中
        markAsRecovering(reason, recoveryState); // mark the shard as recovering on the cluster state thread
        // 异步执行数据恢复任务 并在完成时  通知 recoveryListener
        threadPool.generic().execute(ActionRunnable.wrap(ActionListener.wrap(r -> {
                // r代表本次恢复操作的结果 为true时才需要走下一步
                if (r) {
                    recoveryListener.onRecoveryDone(recoveryState);
                }
            },
            // 处理出现的异常 这里主要是把失败信息发送到leader节点 由主节点重新为分片指定节点
            // 如果本分片是副本 那么不需要走恢复流程 静默处理
            e -> recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true)), action));
    }

    /**
     * Returns whether the shard is a relocated primary, i.e. not in charge anymore of replicating changes (see {@link ReplicationTracker}).
     */
    public boolean isRelocatedPrimary() {
        assert shardRouting.primary() : "only call isRelocatedPrimary on primary shard";
        return replicationTracker.isRelocated();
    }

    public RetentionLease addPeerRecoveryRetentionLease(String nodeId, long globalCheckpoint,
                                                        ActionListener<ReplicationResponse> listener) {
        assert assertPrimaryMode();
        // only needed for BWC reasons involving rolling upgrades from versions that do not support PRRLs:
        assert indexSettings.getIndexVersionCreated().before(Version.V_7_4_0) || indexSettings.isSoftDeleteEnabled() == false;
        return replicationTracker.addPeerRecoveryRetentionLease(nodeId, globalCheckpoint, listener);
    }

    /**
     * 生成一个续约对象的副本
     *
     * @param nodeId
     * @param listener
     * @return
     */
    public RetentionLease cloneLocalPeerRecoveryRetentionLease(String nodeId, ActionListener<ReplicationResponse> listener) {
        assert assertPrimaryMode();
        return replicationTracker.cloneLocalPeerRecoveryRetentionLease(nodeId, listener);
    }

    public void removePeerRecoveryRetentionLease(String nodeId, ActionListener<ReplicationResponse> listener) {
        assert assertPrimaryMode();
        replicationTracker.removePeerRecoveryRetentionLease(nodeId, listener);
    }

    /**
     * Returns a list of retention leases for peer recovery installed in this shard copy.
     */
    public List<RetentionLease> getPeerRecoveryRetentionLeases() {
        return replicationTracker.getPeerRecoveryRetentionLeases();
    }

    public boolean useRetentionLeasesInPeerRecovery() {
        return useRetentionLeasesInPeerRecovery;
    }

    private SafeCommitInfo getSafeCommitInfo() {
        final Engine engine = getEngineOrNull();
        return engine == null ? SafeCommitInfo.EMPTY : engine.getSafeCommitInfo();
    }

    /**
     * Engine.EventListener 当执行engine出现异常时触发该方法
     */
    class ShardEventListener implements Engine.EventListener {

        /**
         * 转发到内部的listener处理
         */
        private final CopyOnWriteArrayList<Consumer<ShardFailure>> delegates = new CopyOnWriteArrayList<>();

        // called by the current engine   在engine.failEngine时会触发该方法
        @Override
        public void onFailedEngine(String reason, @Nullable Exception failure) {
            final ShardFailure shardFailure = new ShardFailure(shardRouting, reason, failure);
            // 转发到了这里的监听器 有点像适配器的套路
            for (Consumer<ShardFailure> listener : delegates) {
                try {
                    listener.accept(shardFailure);
                } catch (Exception inner) {
                    inner.addSuppressed(failure);
                    logger.warn("exception while notifying engine failure", inner);
                }
            }
        }
    }

    /**
     * 将最新的元数据进行持久化
     *
     * @param shardPath      当前创建的分片的文件路径
     * @param indexSettings
     * @param newRouting
     * @param currentRouting
     * @param logger
     * @throws IOException
     */
    private static void persistMetadata(
        final ShardPath shardPath,
        final IndexSettings indexSettings,
        final ShardRouting newRouting,
        final @Nullable ShardRouting currentRouting,
        final Logger logger) throws IOException {
        assert newRouting != null : "newRouting must not be null";

        // only persist metadata if routing information that is persisted in shard state metadata actually changed
        final ShardId shardId = newRouting.shardId();
        // 只有在前后路由信息发生变化时 才有必要进行处理
        // 只有state的变化 是不会触发写入的
        if (currentRouting == null
            || currentRouting.primary() != newRouting.primary()
            || currentRouting.allocationId().equals(newRouting.allocationId()) == false) {
            assert currentRouting == null || currentRouting.isSameAllocation(newRouting);
            final String writeReason;
            if (currentRouting == null) {
                writeReason = "initial state with allocation id [" + newRouting.allocationId() + "]";
            } else {
                writeReason = "routing changed from " + currentRouting + " to " + newRouting;
            }
            logger.trace("{} writing shard state, reason [{}]", shardId, writeReason);

            // 描述分片状态的元数据信息
            final ShardStateMetadata newShardStateMetadata =
                new ShardStateMetadata(newRouting.primary(), indexSettings.getUUID(), newRouting.allocationId());
            // 将最新的元数据信息写入到 shard对应的目录下
            // 这是文件存储的标准套路  类似于使用mysql存储数据  每个数据以一条记录的形式存在 而在基于文件系统做存储时 可以将每个记录/元数据/普通数据作为文件存储
            // 文件本身成为了一种标识
            ShardStateMetadata.FORMAT.writeAndCleanup(newShardStateMetadata, shardPath.getShardStatePath());
        } else {
            logger.trace("{} skip writing shard state, has been written before", shardId);
        }
    }


    private DocumentMapperForType docMapper() {
        return mapperService.documentMapperWithAutoCreate();
    }

    /**
     * 根据相关信息生成一个引擎配置对象
     *
     * @param globalCheckpointSupplier
     * @return
     */
    private EngineConfig newEngineConfig(LongSupplier globalCheckpointSupplier) {
        // 生成描述排序顺序的对象
        final Sort indexSort = indexSortSupplier.get();

        // 每当 externalReaderManager刷新时 就会通过该对象进行预热
        final Engine.Warmer warmer = reader -> {
            assert Thread.holdsLock(mutex) == false : "warming engine under mutex";
            assert reader != null;
            if (this.warmer != null) {
                this.warmer.warm(reader);
            }
        };

        // 这里都是一些基本的赋值操作
        return new EngineConfig(shardId, shardRouting.allocationId().getId(),
            threadPool, indexSettings, warmer, store, indexSettings.getMergePolicy(),
            mapperService != null ? mapperService.indexAnalyzer() : null,
            similarityService.similarity(mapperService), codecService, shardEventListener,
            indexCache != null ? indexCache.query() : null, cachingPolicy, translogConfig,
            IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.get(indexSettings.getSettings()),
            // 对应externalReaderManager的监听器
            List.of(refreshListeners, refreshPendingLocationListener),
            Collections.singletonList(new RefreshMetricUpdater(refreshMetric)),
            indexSort, circuitBreakerService, globalCheckpointSupplier, replicationTracker::getRetentionLeases,
            () -> getOperationPrimaryTerm(), tombstoneDocSupplier());
    }

    /**
     * Acquire a primary operation permit whenever the shard is ready for indexing. If a permit is directly available, the provided
     * ActionListener will be called on the calling thread. During relocation hand-off, permit acquisition can be delayed. The provided
     * ActionListener will then be called using the provided executor.
     *
     * @param debugInfo an extra information that can be useful when tracing an unreleased permit. When assertions are enabled
     *                  the tracing will capture the supplied object's {@link Object#toString()} value. Otherwise the object
     *                  isn't used
     *                  先获取门票 获取成功后才可以执行任务
     *                  而在 wrapPrimaryOperationPermitListener 要求当前shard必须是primary 才可以正常触发监听器
     */
    public void acquirePrimaryOperationPermit(ActionListener<Releasable> onPermitAcquired, String executorOnDelay, Object debugInfo) {
        verifyNotClosed();
        assert shardRouting.primary() : "acquirePrimaryOperationPermit should only be called on primary shard: " + shardRouting;

        indexShardOperationPermits.acquire(wrapPrimaryOperationPermitListener(onPermitAcquired), executorOnDelay, false, debugInfo);
    }

    /**
     * Acquire all primary operation permits. Once all permits are acquired, the provided ActionListener is called.
     * It is the responsibility of the caller to close the {@link Releasable}.
     * 获取该分片的全部操作许可
     */
    public void acquireAllPrimaryOperationsPermits(final ActionListener<Releasable> onPermitAcquired, final TimeValue timeout) {
        verifyNotClosed();
        assert shardRouting.primary() : "acquireAllPrimaryOperationsPermits should only be called on primary shard: " + shardRouting;

        asyncBlockOperations(wrapPrimaryOperationPermitListener(onPermitAcquired), timeout.duration(), timeout.timeUnit());
    }

    /**
     * Wraps the action to run on a primary after acquiring permit. This wrapping is used to check if the shard is in primary mode before
     * executing the action.
     *
     * @param listener the listener to wrap
     * @return the wrapped listener
     * 包装一层后必须当前是主分片才可以正常触发监听器
     */
    private ActionListener<Releasable> wrapPrimaryOperationPermitListener(final ActionListener<Releasable> listener) {
        return ActionListener.delegateFailure(
            listener,
            (l, r) -> {
                // shard在当前节点必须是 primary分片才可以正常触发监听器
                if (replicationTracker.isPrimaryMode()) {
                    l.onResponse(r);
                } else {
                    r.close();
                    l.onFailure(new ShardNotInPrimaryModeException(shardId, state));
                }
            });
    }

    /**
     * 等待直到抢占该分片的操作权
     *
     * @param onPermitAcquired 当获取到操作权后触发
     * @param timeout
     * @param timeUnit
     */
    private void asyncBlockOperations(ActionListener<Releasable> onPermitAcquired, long timeout, TimeUnit timeUnit) {
        // 执行强制刷新 并在结束时释放计数器
        final Releasable forceRefreshes = refreshListeners.forceRefreshes();

        // 当成功抢占所有许可证后 会触发该监听器  并将处理逻辑转接到 onPermitAcquired 上
        final ActionListener<Releasable> wrappedListener = ActionListener.wrap(r -> {
            forceRefreshes.close();
            onPermitAcquired.onResponse(r);
        }, e -> {
            forceRefreshes.close();
            onPermitAcquired.onFailure(e);
        });
        try {
            // 发起一个阻塞操作 这样在直到 onPermitAcquired 手动释放之前 针对该分片的其他操作都会被阻塞
            indexShardOperationPermits.asyncBlockOperations(wrappedListener, timeout, timeUnit);
        } catch (Exception e) {
            forceRefreshes.close();
            throw e;
        }
    }

    /**
     * Runs the specified runnable under a permit and otherwise calling back the specified failure callback. This method is really a
     * convenience for {@link #acquirePrimaryOperationPermit(ActionListener, String, Object)} where the listener equates to
     * try-with-resources closing the releasable after executing the runnable on successfully acquiring the permit, an otherwise calling
     * back the failure callback.
     *
     * @param runnable        the runnable to execute under permit    执行的函数
     * @param onFailure       the callback on failure   当出现异常时 触发该函数
     * @param executorOnDelay the executor to execute the runnable on if permit acquisition is blocked
     * @param debugInfo       debug info
     */
    public void runUnderPrimaryPermit(
        final Runnable runnable,
        final Consumer<Exception> onFailure,
        final String executorOnDelay,
        final Object debugInfo) {
        verifyNotClosed();
        assert shardRouting.primary() : "runUnderPrimaryPermit should only be called on primary shard but was " + shardRouting;
        final ActionListener<Releasable> onPermitAcquired = ActionListener.wrap(
            releasable -> {
                try (Releasable ignore = releasable) {
                    runnable.run();
                }
            },
            onFailure);
        acquirePrimaryOperationPermit(onPermitAcquired, executorOnDelay, debugInfo);
    }

    /**
     * 代表远端请求的主分片任期 与当前节点的主分片任期不一致
     *
     * @param newPrimaryTerm
     * @param onBlocked
     * @param combineWithAction
     * @param <E>
     */
    private <E extends Exception> void bumpPrimaryTerm(final long newPrimaryTerm,
                                                       final CheckedRunnable<E> onBlocked,
                                                       @Nullable ActionListener<Releasable> combineWithAction) {
        assert Thread.holdsLock(mutex);
        assert newPrimaryTerm > pendingPrimaryTerm || (newPrimaryTerm >= pendingPrimaryTerm && combineWithAction != null);
        assert getOperationPrimaryTerm() <= pendingPrimaryTerm;
        final CountDownLatch termUpdated = new CountDownLatch(1);

        // 当抢占到这个分片的所有许可证后 会触发监听器
        asyncBlockOperations(new ActionListener<Releasable>() {
            @Override
            public void onFailure(final Exception e) {
                try {
                    innerFail(e);
                } finally {
                    if (combineWithAction != null) {
                        combineWithAction.onFailure(e);
                    }
                }
            }

            /**
             * 当该分片处理失败后 关闭底层的engine
             * @param e
             */
            private void innerFail(final Exception e) {
                try {
                    failShard("exception during primary term transition", e);
                } catch (AlreadyClosedException ace) {
                    // ignore, shard is already closed
                }
            }

            /**
             * 当正常接收到响应结果后触发该方法
             * @param releasable
             */
            @Override
            public void onResponse(final Releasable releasable) {
                // 这里释放了之前抢占的所有许可证
                final RunOnce releaseOnce = new RunOnce(releasable::close);
                try {
                    assert getOperationPrimaryTerm() <= pendingPrimaryTerm;
                    termUpdated.await();
                    // indexShardOperationPermits doesn't guarantee that async submissions are executed
                    // in the order submitted. We need to guard against another term bump
                    // 代表发起操作的任期此时还是落后于请求中的 主任期
                    if (getOperationPrimaryTerm() < newPrimaryTerm) {
                        replicationTracker.setOperationPrimaryTerm(newPrimaryTerm);
                        onBlocked.run();
                    }
                } catch (final Exception e) {
                    if (combineWithAction == null) {
                        // otherwise leave it to combineWithAction to release the permit
                        releaseOnce.run();
                    }
                    innerFail(e);
                } finally {
                    if (combineWithAction != null) {
                        combineWithAction.onResponse(releasable);
                    } else {
                        releaseOnce.run();
                    }
                }
            }
        }, 30, TimeUnit.MINUTES);

        // 更新当前节点维护的主分片任期
        pendingPrimaryTerm = newPrimaryTerm;
        termUpdated.countDown();
    }

    /**
     * Acquire a replica operation permit whenever the shard is ready for indexing (see
     * {@link #acquirePrimaryOperationPermit(ActionListener, String, Object)}). If the given primary term is lower than then one in
     * {@link #shardRouting}, the {@link ActionListener#onFailure(Exception)} method of the provided listener is invoked with an
     * {@link IllegalStateException}. If permit acquisition is delayed, the listener will be invoked on the executor with the specified
     * name.
     *
     * @param opPrimaryTerm              the operation primary term
     * @param globalCheckpoint           the global checkpoint associated with the request
     * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates (index operations overwrite Lucene) or deletes captured on the primary
     *                                   after this replication request was executed on it (see {@link #getMaxSeqNoOfUpdatesOrDeletes()}
     * @param onPermitAcquired           the listener for permit acquisition
     * @param executorOnDelay            the name of the executor to invoke the listener on if permit acquisition is delayed
     * @param debugInfo                  an extra information that can be useful when tracing an unreleased permit. When assertions are
     *                                   enabled the tracing will capture the supplied object's {@link Object#toString()} value.
     *                                   Otherwise the object isn't used
     */
    public void acquireReplicaOperationPermit(final long opPrimaryTerm, final long globalCheckpoint, final long maxSeqNoOfUpdatesOrDeletes,
                                              final ActionListener<Releasable> onPermitAcquired, final String executorOnDelay,
                                              final Object debugInfo) {
        innerAcquireReplicaOperationPermit(opPrimaryTerm, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes, onPermitAcquired, false,
            (listener) -> indexShardOperationPermits.acquire(listener, executorOnDelay, true, debugInfo));
    }

    /**
     * Acquire all replica operation permits whenever the shard is ready for indexing (see
     * {@link #acquireAllPrimaryOperationsPermits(ActionListener, TimeValue)}. If the given primary term is lower than then one in
     * {@link #shardRouting}, the {@link ActionListener#onFailure(Exception)} method of the provided listener is invoked with an
     * {@link IllegalStateException}.
     *
     * @param opPrimaryTerm              the operation primary term
     * @param globalCheckpoint           the global checkpoint associated with the request
     * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates (index operations overwrite Lucene) or deletes captured on the primary
     *                                   after this replication request was executed on it (see {@link #getMaxSeqNoOfUpdatesOrDeletes()}
     * @param onPermitAcquired           the listener for permit acquisition
     * @param timeout                    the maximum time to wait for the in-flight operations block
     */
    public void acquireAllReplicaOperationsPermits(final long opPrimaryTerm,
                                                   final long globalCheckpoint,
                                                   final long maxSeqNoOfUpdatesOrDeletes,
                                                   final ActionListener<Releasable> onPermitAcquired,
                                                   final TimeValue timeout) {
        innerAcquireReplicaOperationPermit(opPrimaryTerm, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes,
            onPermitAcquired, true,
            listener -> asyncBlockOperations(listener, timeout.duration(), timeout.timeUnit())
        );
    }

    /**
     * 从副本分片上获取执行操作需要的许可证(单个)
     *
     * @param opPrimaryTerm                              请求体中携带的主分片任期
     * @param globalCheckpoint                           当前的全局检查点
     * @param maxSeqNoOfUpdatesOrDeletes
     * @param onPermitAcquired                  当获取到许可后会调用该方法 继续执行操作
     * @param allowCombineOperationWithPrimaryTermUpdate  代表本次是否需要获取所有许可证
     * @param operationExecutor
     */
    private void innerAcquireReplicaOperationPermit(final long opPrimaryTerm,
                                                    final long globalCheckpoint,
                                                    final long maxSeqNoOfUpdatesOrDeletes,
                                                    final ActionListener<Releasable> onPermitAcquired,
                                                    final boolean allowCombineOperationWithPrimaryTermUpdate,
                                                    final Consumer<ActionListener<Releasable>> operationExecutor) {
        verifyNotClosed();

        // This listener is used for the execution of the operation. If the operation requires all the permits for its
        // execution and the primary term must be updated first, we can combine the operation execution with the
        // primary term update. Since indexShardOperationPermits doesn't guarantee that async submissions are executed
        // in the order submitted, combining both operations ensure that the term is updated before the operation is
        // executed. It also has the side effect of acquiring all the permits one time instead of two.
        // 当获取到许可证后触发该监听器
        final ActionListener<Releasable> operationListener = ActionListener.delegateFailure(onPermitAcquired,
            (delegatedListener, releasable) -> {
                // 在处理前先检测 主分片任期是否已经落后了  因为可能之前发生了某种获取所有许可证的操作 这种操作可能就是由于primaryTerm的更新引发的
                if (opPrimaryTerm < getOperationPrimaryTerm()) {
                    releasable.close();
                    final String message = String.format(
                        Locale.ROOT,
                        "%s operation primary term [%d] is too old (current [%d])",
                        shardId,
                        opPrimaryTerm,
                        getOperationPrimaryTerm());
                    delegatedListener.onFailure(new IllegalStateException(message));
                } else {
                    assert assertReplicationTarget();
                    try {
                        // 这个是主分片上的全局检查点 尝试在副本上进行同步
                        updateGlobalCheckpointOnReplica(globalCheckpoint, "operation");
                        // 这里也是将副本的该值 与主分片的该值同步
                        advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfUpdatesOrDeletes);
                    } catch (Exception e) {
                        releasable.close();
                        delegatedListener.onFailure(e);
                        return;
                    }
                    delegatedListener.onResponse(releasable);
                }
            });

        // 检测请求体中携带的主分片任期 是否超过了当前分片记录的任期
        // TODO 这里先忽略
        if (requirePrimaryTermUpdate(opPrimaryTerm, allowCombineOperationWithPrimaryTermUpdate)) {
            synchronized (mutex) {
                if (requirePrimaryTermUpdate(opPrimaryTerm, allowCombineOperationWithPrimaryTermUpdate)) {
                    final IndexShardState shardState = state();
                    // only roll translog and update primary term if shard has made it past recovery
                    // Having a new primary term here means that the old primary failed and that there is a new primary, which again
                    // means that the master will fail this shard as all initializing shards are failed when a primary is selected
                    // We abort early here to prevent an ongoing recovery from the failed primary to mess with the global / local checkpoint
                    // 如果当前分片的状态还不稳定  不允许在本地更新 primary的任期
                    if (shardState != IndexShardState.POST_RECOVERY &&
                        shardState != IndexShardState.STARTED) {
                        throw new IndexShardNotStartedException(shardId, shardState);
                    }

                    // 尝试更新主分片任期
                    bumpPrimaryTerm(opPrimaryTerm,
                        // 如果发现 operate的任期 比主分片任期小 那么触发该方法
                        () -> {
                            // 尝试更新全局检查点
                            updateGlobalCheckpointOnReplica(globalCheckpoint, "primary term transition");
                            // 获取当前的全局检查点
                            final long currentGlobalCheckpoint = getLastKnownGlobalCheckpoint();
                            final long maxSeqNo = seqNoStats().getMaxSeqNo();
                            logger.info("detected new primary with primary term [{}], global checkpoint [{}], max_seq_no [{}]",
                                opPrimaryTerm, currentGlobalCheckpoint, maxSeqNo);
                            // 当此时的全局检查点 小于最大的序列号时 使用这个全局检查点去重置engine
                            if (currentGlobalCheckpoint < maxSeqNo) {
                                resetEngineToGlobalCheckpoint();
                            } else {
                                // 切换到下一个事务日志
                                getEngine().rollTranslogGeneration();
                            }
                        }, allowCombineOperationWithPrimaryTermUpdate ? operationListener : null);

                    // 如果指定了 在update后触发监听器  现在可以直接返回   并且之后在回调中会触发 operationListener
                    if (allowCombineOperationWithPrimaryTermUpdate) {
                        logger.debug("operation execution has been combined with primary term update");
                        return;
                    }
                }
            }
        }
        assert opPrimaryTerm <= pendingPrimaryTerm
            : "operation primary term [" + opPrimaryTerm + "] should be at most [" + pendingPrimaryTerm + "]";

        // 获取许可证 并在成功后触发 operationListener
        operationExecutor.accept(operationListener);
    }

    /**
     * 判断从主分片上获取到的 term是否比副本上要大
     * @param opPrimaryTerm
     * @param allPermits
     * @return
     */
    private boolean requirePrimaryTermUpdate(final long opPrimaryTerm, final boolean allPermits) {
        return (opPrimaryTerm > pendingPrimaryTerm) || (allPermits && opPrimaryTerm > getOperationPrimaryTerm());
    }

    public static final int OPERATIONS_BLOCKED = -1;

    /**
     * Obtain the active operation count, or {@link IndexShard#OPERATIONS_BLOCKED} if all permits are held (even if there are
     * outstanding operations in flight).
     *
     * @return the active operation count, or {@link IndexShard#OPERATIONS_BLOCKED} when all permits are held.
     */
    public int getActiveOperationsCount() {
        return indexShardOperationPermits.getActiveOperationsCount();
    }

    /**
     * @return a list of describing each permit that wasn't released yet. The description consist of the debugInfo supplied
     * when the permit was acquired plus a stack traces that was captured when the permit was request.
     */
    public List<String> getActiveOperations() {
        return indexShardOperationPermits.getActiveOperations();
    }

    /**
     * 多个线程尝试执行IO 任务  会被汇总到一个线程中执行
     */
    private final AsyncIOProcessor<Translog.Location> translogSyncProcessor;

    /**
     * 执行IO 写入逻辑
     *
     * @param logger
     * @param threadContext
     * @param engineSupplier
     * @return
     */
    private static AsyncIOProcessor<Translog.Location> createTranslogSyncProcessor(Logger logger, ThreadContext threadContext,
                                                                                   Supplier<Engine> engineSupplier) {
        return new AsyncIOProcessor<>(logger, 1024, threadContext) {

            /**
             * @param candidates
             * @throws IOException
             */
            @Override
            protected void write(List<Tuple<Translog.Location, Consumer<Exception>>> candidates) throws IOException {
                try {
                    engineSupplier.get().ensureTranslogSynced(candidates.stream().map(Tuple::v1));
                } catch (AlreadyClosedException ex) {
                    // that's fine since we already synced everything on engine close - this also is conform with the methods
                    // documentation
                } catch (IOException ex) { // if this fails we are in deep shit - fail the request
                    logger.debug("failed to sync translog", ex);
                    throw ex;
                }
            }
        };
    }

    /**
     * Syncs the given location with the underlying storage unless already synced. This method might return immediately without
     * actually fsyncing the location until the sync listener is called. Yet, unless there is already another thread fsyncing
     * the transaction log the caller thread will be hijacked to run the fsync for all pending fsync operations.
     * This method allows indexing threads to continue indexing without blocking on fsync calls. We ensure that there is only
     * one thread blocking on the sync an all others can continue indexing.
     * NOTE: if the syncListener throws an exception when it's processed the exception will only be logged. Users should make sure that the
     * listener handles all exception cases internally.
     */
    public final void sync(Translog.Location location, Consumer<Exception> syncListener) {
        verifyNotClosed();
        translogSyncProcessor.put(location, syncListener);
    }

    /**
     * 将事务日志刷盘
     *
     * @throws IOException
     */
    public void sync() throws IOException {
        verifyNotClosed();
        getEngine().syncTranslog();
    }

    /**
     * Checks if the underlying storage sync is required.
     */
    public boolean isSyncNeeded() {
        return getEngine().isTranslogSyncNeeded();
    }

    /**
     * Returns the current translog durability mode
     */
    public Translog.Durability getTranslogDurability() {
        return indexSettings.getTranslogDurability();
    }

    // we can not protect with a lock since we "release" on a different thread
    // 代表此时正在进行事务文件的刷盘 或者 滚动事务文件
    private final AtomicBoolean flushOrRollRunning = new AtomicBoolean();

    /**
     * Schedules a flush or translog generation roll if needed but will not schedule more than one concurrently. The operation will be
     * executed asynchronously on the flush thread pool.
     * 检测是否需要开启自动刷盘
     */
    public void afterWriteOperation() {
        // shouldRollTranslogGeneration 当前事务文件写入的数据过多时 会返回true 推荐滚动到下一个事务文件
        if (shouldPeriodicallyFlush() || shouldRollTranslogGeneration()) {
            if (flushOrRollRunning.compareAndSet(false, true)) {
                /*
                 * We have to check again since otherwise there is a race when a thread passes the first check next to another thread which
                 * performs the operation quickly enough to  finish before the current thread could flip the flag. In that situation, we
                 * have an extra operation.
                 *
                 * Additionally, a flush implicitly executes a translog generation roll so if we execute a flush then we do not need to
                 * check if we should roll the translog generation.
                 * 代表需要开启定时刷盘
                 */
                if (shouldPeriodicallyFlush()) {
                    logger.debug("submitting async flush request");
                    final AbstractRunnable flush = new AbstractRunnable() {
                        @Override
                        public void onFailure(final Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to flush index", e);
                            }
                        }

                        @Override
                        protected void doRun() throws IOException {
                            // 每次flush 实际上都会自动滚动到下一个事务文件 长时间未刷盘 且写入了大量数据到事务文件中 就会被动的触发roll
                            flush(new FlushRequest());
                            periodicFlushMetric.inc();
                        }

                        @Override
                        public void onAfter() {
                            flushOrRollRunning.compareAndSet(true, false);
                            afterWriteOperation();
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(flush);
                } else if (shouldRollTranslogGeneration()) {
                    logger.debug("submitting async roll translog generation request");
                    final AbstractRunnable roll = new AbstractRunnable() {
                        @Override
                        public void onFailure(final Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to roll translog generation", e);
                            }
                        }

                        @Override
                        protected void doRun() throws Exception {
                            rollTranslogGeneration();
                        }

                        @Override
                        public void onAfter() {
                            flushOrRollRunning.compareAndSet(true, false);
                            afterWriteOperation();
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(roll);
                } else {
                    flushOrRollRunning.compareAndSet(true, false);
                }
            }
        }
    }

    /**
     * Build {@linkplain RefreshListeners} for this shard.
     * 构建刷新监听器组  会感知资源对象的刷新动作 并触发相关钩子
     */
    private RefreshListeners buildRefreshListeners() {
        return new RefreshListeners(
            indexSettings::getMaxRefreshListeners,
            // 定义了数据刷新的逻辑
            () -> refresh("too_many_listeners"),
            logger, threadPool.getThreadContext(),
            externalRefreshMetric);
    }

    /**
     * Simple struct encapsulating a shard failure
     *
     * @see IndexShard#addShardFailureCallback(Consumer)
     * 表示分片的某种失败信息
     */
    public static final class ShardFailure {

        /**
         * 内部包含了此时分片所在的节点信息等
         */
        public final ShardRouting routing;
        public final String reason;
        @Nullable
        public final Exception cause;

        public ShardFailure(ShardRouting routing, String reason, @Nullable Exception cause) {
            this.routing = routing;
            this.reason = reason;
            this.cause = cause;
        }
    }

    EngineFactory getEngineFactory() {
        return engineFactory;
    }

    // for tests
    ReplicationTracker getReplicationTracker() {
        return replicationTracker;
    }

    /**
     * Executes a scheduled refresh if necessary.
     *
     * @return <code>true</code> iff the engine got refreshed otherwise <code>false</code>
     * 执行刷新任务
     */
    public boolean scheduledRefresh() {
        verifyNotClosed();
        boolean listenerNeedsRefresh = refreshListeners.refreshNeeded();
        if (isReadAllowed() && (listenerNeedsRefresh || getEngine().refreshNeeded())) {
            if (listenerNeedsRefresh == false // if we have a listener that is waiting for a refresh we need to force it
                && isSearchIdle()
                && indexSettings.isExplicitRefresh() == false
                && active.get()) { // it must be active otherwise we might not free up segment memory once the shard became inactive
                // lets skip this refresh since we are search idle and
                // don't necessarily need to refresh. the next searcher access will register a refreshListener and that will
                // cause the next schedule to refresh.
                final Engine engine = getEngine();
                engine.maybePruneDeletes(); // try to prune the deletes in the engine if we accumulated some
                setRefreshPending(engine);
                return false;
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("refresh with source [schedule]");
                }
                return getEngine().maybeRefresh("schedule");
            }
        }
        final Engine engine = getEngine();
        engine.maybePruneDeletes(); // try to prune the deletes in the engine if we accumulated some
        return false;
    }

    /**
     * Returns true if this shards is search idle
     */
    public final boolean isSearchIdle() {
        return (threadPool.relativeTimeInMillis() - lastSearcherAccess.get()) >= indexSettings.getSearchIdleAfter().getMillis();
    }

    /**
     * Returns the last timestamp the searcher was accessed. This is a relative timestamp in milliseconds.
     */
    final long getLastSearcherAccess() {
        return lastSearcherAccess.get();
    }

    /**
     * Returns true if this shard has some scheduled refresh that is pending because of search-idle.
     */
    public final boolean hasRefreshPending() {
        return pendingRefreshLocation.get() != null;
    }

    private void setRefreshPending(Engine engine) {
        final Translog.Location lastWriteLocation = engine.getTranslogLastWriteLocation();
        pendingRefreshLocation.updateAndGet(curr -> {
            if (curr == null || curr.compareTo(lastWriteLocation) <= 0) {
                return lastWriteLocation;
            } else {
                return curr;
            }
        });
    }

    /**
     * 该对象会监听 externalReaderManager的刷新事件
     */
    private class RefreshPendingLocationListener implements ReferenceManager.RefreshListener {

        /**
         * 在执行刷新前 先获取此时engine内translog记录的最新的记录对应的location
         */
        Translog.Location lastWriteLocation;

        @Override
        public void beforeRefresh() {
            try {
                lastWriteLocation = getEngine().getTranslogLastWriteLocation();
            } catch (AlreadyClosedException exc) {
                // shard is closed - no location is fine
                lastWriteLocation = null;
            }
        }

        @Override
        public void afterRefresh(boolean didRefresh) {
            if (didRefresh && lastWriteLocation != null) {
                // pendingRefreshLocation 代表该事务日志位置对应的写入lucene的数据需要刷盘 当某次refresh时最后的位置已经超过了pending值 就可以将它置空 避免其他地方重复刷盘
                pendingRefreshLocation.updateAndGet(pendingLocation -> {
                    if (pendingLocation == null || pendingLocation.compareTo(lastWriteLocation) <= 0) {
                        return null;
                    } else {
                        return pendingLocation;
                    }
                });
            }
        }
    }

    /**
     * Registers the given listener and invokes it once the shard is active again and all
     * pending refresh translog location has been refreshed. If there is no pending refresh location registered the listener will be
     * invoked immediately.
     *
     * @param listener the listener to invoke once the pending refresh location is visible. The listener will be called with
     *                 <code>true</code> if the listener was registered to wait for a refresh.
     */
    public final void awaitShardSearchActive(Consumer<Boolean> listener) {
        markSearcherAccessed(); // move the shard into non-search idle
        final Translog.Location location = pendingRefreshLocation.get();
        if (location != null) {
            // 等待刷新任务完成 并触发监听器
            addRefreshListener(location, (b) -> {
                pendingRefreshLocation.compareAndSet(location, null);
                listener.accept(true);
            });
        } else {
            // 如果不需要刷新 直接触发监听器
            listener.accept(false);
        }
    }

    /**
     * Add a listener for refreshes.
     *
     * @param location the location to listen for
     * @param listener for the refresh. Called with true if registering the listener ran it out of slots and forced a refresh. Called with
     *                 false otherwise.
     *                插入一个监听刷新动作的监听器
     */
    public void addRefreshListener(Translog.Location location, Consumer<Boolean> listener) {
        final boolean readAllowed;
        // 检测当前状态是否允许读取数据
        if (isReadAllowed()) {
            readAllowed = true;
        } else {
            // check again under postRecoveryMutex. this is important to create a happens before relationship
            // between the switch to POST_RECOVERY + associated refresh. Otherwise we may respond
            // to a listener before a refresh actually happened that contained that operation.
            synchronized (postRecoveryMutex) {
                readAllowed = isReadAllowed();
            }
        }
        if (readAllowed) {
            refreshListeners.addOrNotify(location, listener);
        } else {
            // we're not yet ready fo ready for reads, just ignore refresh cycles
            // 如果本身状态不允许读取数据 那么直接触发等待刷新的监听器
            listener.accept(false);
        }
    }

    /**
     * 该监听器会设置在 internalReaderManager
     */
    private static class RefreshMetricUpdater implements ReferenceManager.RefreshListener {

        /**
         * 当刷新完成后 将耗时信息设置到统计对象中
         */
        private final MeanMetric refreshMetric;

        /**
         * 记录某次开始刷新的时间
         */
        private long currentRefreshStartTime;
        /**
         * 记录执行刷新任务的线程
         */
        private Thread callingThread = null;

        private RefreshMetricUpdater(MeanMetric refreshMetric) {
            this.refreshMetric = refreshMetric;
        }

        /**
         * 在执行刷新操作前
         * @throws IOException
         */
        @Override
        public void beforeRefresh() throws IOException {
            if (Assertions.ENABLED) {
                assert callingThread == null : "beforeRefresh was called by " + callingThread.getName() +
                    " without a corresponding call to afterRefresh";
                callingThread = Thread.currentThread();
            }
            currentRefreshStartTime = System.nanoTime();
        }

        /**
         * 刷新完成时 进行一些数据统计
         * @param didRefresh
         * @throws IOException
         */
        @Override
        public void afterRefresh(boolean didRefresh) throws IOException {
            if (Assertions.ENABLED) {
                assert callingThread != null : "afterRefresh called but not beforeRefresh";
                assert callingThread == Thread.currentThread() : "beforeRefreshed called by a different thread. current ["
                    + Thread.currentThread().getName() + "], thread that called beforeRefresh [" + callingThread.getName() + "]";
                callingThread = null;
            }
            refreshMetric.inc(System.nanoTime() - currentRefreshStartTime);
        }
    }

    /**
     * 该函数定义了如何返回 坟墓doc
     * @return
     */
    private EngineConfig.TombstoneDocSupplier tombstoneDocSupplier() {
        final RootObjectMapper.Builder noopRootMapper = new RootObjectMapper.Builder("__noop");
        final DocumentMapper noopDocumentMapper = mapperService != null ?
            new DocumentMapper.Builder(noopRootMapper, mapperService).build(mapperService) :
            null;
        return new EngineConfig.TombstoneDocSupplier() {

            /**
             * 针对某次 delete操作生成一个 doc对象
             * @param id
             * @return
             */
            @Override
            public ParsedDocument newDeleteTombstoneDoc(String id) {
                return docMapper().getDocumentMapper().createDeleteTombstoneDoc(shardId.getIndexName(), id);
            }

            @Override
            public ParsedDocument newNoopTombstoneDoc(String reason) {
                return noopDocumentMapper.createNoopTombstoneDoc(shardId.getIndexName(), reason);
            }
        };
    }

    /**
     * Rollback the current engine to the safe commit, then replay local translog up to the global checkpoint.
     * 重置引擎 回到全局检查点的位置
     */
    void resetEngineToGlobalCheckpoint() throws IOException {
        assert Thread.holdsLock(mutex) == false : "resetting engine under mutex";
        assert getActiveOperationsCount() == OPERATIONS_BLOCKED
            : "resetting engine without blocking operations; active operations are [" + getActiveOperations() + ']';
        // 先对相关数据做持久化
        sync(); // persist the global checkpoint to disk
        // 通过全局检查点找到对应的数据统计对象
        final SeqNoStats seqNoStats = seqNoStats();
        // 获取事务日志的数据统计对象
        final TranslogStats translogStats = translogStats();
        // flush to make sure the latest commit, which will be opened by the read-only engine, includes all operations.
        // 这里是针对 lucene中的数据进行刷盘
        flush(new FlushRequest().waitIfOngoing(true));

        SetOnce<Engine> newEngineReference = new SetOnce<>();
        // 获取全局检查点
        final long globalCheckpoint = getLastKnownGlobalCheckpoint();
        assert globalCheckpoint == getLastSyncedGlobalCheckpoint();
        synchronized (engineMutex) {
            verifyNotClosed();
            // we must create both new read-only engine and new read-write engine under engineMutex to ensure snapshotStoreMetadata,
            // acquireXXXCommit and close works.
            // 在这个时候 又创建了一个只读引擎对象
            final Engine readOnlyEngine =
                new ReadOnlyEngine(newEngineConfig(replicationTracker), seqNoStats, translogStats, false, Function.identity(), true) {
                    @Override
                    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) {
                        synchronized (engineMutex) {
                            if (newEngineReference.get() == null) {
                                throw new AlreadyClosedException("engine was closed");
                            }
                            // ignore flushFirst since we flushed above and we do not want to interfere with ongoing translog replay
                            return newEngineReference.get().acquireLastIndexCommit(false);
                        }
                    }

                    @Override
                    public IndexCommitRef acquireSafeIndexCommit() {
                        synchronized (engineMutex) {
                            if (newEngineReference.get() == null) {
                                throw new AlreadyClosedException("engine was closed");
                            }
                            return newEngineReference.get().acquireSafeIndexCommit();
                        }
                    }

                    @Override
                    public void close() throws IOException {
                        assert Thread.holdsLock(engineMutex);

                        Engine newEngine = newEngineReference.get();
                        if (newEngine == currentEngineReference.get()) {
                            // we successfully installed the new engine so do not close it.
                            newEngine = null;
                        }
                        IOUtils.close(super::close, newEngine);
                    }
                };
            // 使用只读引擎替换之前的引擎对象  同时关闭之前的引擎
            IOUtils.close(currentEngineReference.getAndSet(readOnlyEngine));
            // 重新创建引擎对象 并设置到currentEngineReference
            newEngineReference.set(engineFactory.newReadWriteEngine(newEngineConfig(replicationTracker)));
            // 使用新创建的引擎对象的事务偏移量去覆盖当前的事务偏移量
            onNewEngine(newEngineReference.get());
        }

        // 转发到runTranslogRecovery 进行数据恢复
        final Engine.TranslogRecoveryRunner translogRunner = (engine, snapshot) -> runTranslogRecovery(
            engine, snapshot, Engine.Operation.Origin.LOCAL_RESET,
            // 每当恢复一个operation时触发的钩子
            () -> {
                // TODO: add a dedicate recovery stats for the reset translog
            });
        // 新的引擎对象数据仅恢复到全局检查点的位置 这样就完成了替换  在此期间都是readOnlyEngine在起作用
        newEngineReference.get().recoverFromTranslog(translogRunner, globalCheckpoint);

        // 将此时最新的数据加载进来 实际上就是加载之前恢复的数据
        newEngineReference.get().refresh("reset_engine");
        synchronized (engineMutex) {
            verifyNotClosed();
            // 将新的引擎设置到 currentEngine中 同时关闭之前的 readOnlyEngine
            IOUtils.close(currentEngineReference.getAndSet(newEngineReference.get()));
            // We set active because we are now writing operations to the engine; this way,
            // if we go idle after some time and become inactive, we still give sync'd flush a chance to run.
            active.set(true);
        }
        // time elapses after the engine is created above (pulling the config settings) until we set the engine reference, during
        // which settings changes could possibly have happened, so here we forcefully push any config changes to the new engine.
        onSettingsChanged();
    }

    /**
     * Returns the maximum sequence number of either update or delete operations have been processed in this shard
     * or the sequence number from {@link #advanceMaxSeqNoOfUpdatesOrDeletes(long)}. An index request is considered
     * as an update operation if it overwrites the existing documents in Lucene index with the same document id.
     * <p>
     * The primary captures this value after executes a replication request, then transfers it to a replica before
     * executing that replication request on a replica.
     */
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return getEngine().getMaxSeqNoOfUpdatesOrDeletes();
    }

    /**
     * A replica calls this method to advance the max_seq_no_of_updates marker of its engine to at least the max_seq_no_of_updates
     * value (piggybacked in a replication request) that it receives from its primary before executing that replication request.
     * The receiving value is at least as high as the max_seq_no_of_updates on the primary was when any of the operations of that
     * replication request were processed on it.
     * <p>
     * A replica shard also calls this method to bootstrap the max_seq_no_of_updates marker with the value that it received from
     * the primary in peer-recovery, before it replays remote translog operations from the primary. The receiving value is at least
     * as high as the max_seq_no_of_updates on the primary was when any of these operations were processed on it.
     * <p>
     * These transfers guarantee that every index/delete operation when executing on a replica engine will observe this marker a value
     * which is at least the value of the max_seq_no_of_updates marker on the primary after that operation was executed on the primary.
     *
     * @see #acquireReplicaOperationPermit(long, long, long, ActionListener, String, Object)
     * @see RecoveryTarget#indexTranslogOperations(List, int, long, long, RetentionLeases, long, ActionListener)
     */
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long seqNo) {
        getEngine().advanceMaxSeqNoOfUpdatesOrDeletes(seqNo);
    }

    /**
     * Performs the pre-closing checks on the {@link IndexShard}.
     *
     * @throws IllegalStateException if the sanity checks failed
     */
    public void verifyShardBeforeIndexClosing() throws IllegalStateException {
        getEngine().verifyEngineBeforeIndexClosing();
    }

    RetentionLeaseSyncer getRetentionLeaseSyncer() {
        return retentionLeaseSyncer;
    }
}
