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

package org.elasticsearch.index;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.Assertions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.gateway.WriteStateException;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.SearchIndexNameMatcher;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.shard.ShardNotInPrimaryModeException;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * 索引服务
 * 每个节点下包含一个  IndicesService 之后针对每个index 会单独生成一个 IndexService
 *
 * AllocatedIndex 接口代表着indexService对应的index 已经分配完成 并且该对象在indexMetadata发生变化时 可以作出处理 以及根据shardId 移除/获取 shard
 */
public class IndexService extends AbstractIndexComponent implements IndicesClusterStateService.AllocatedIndex<IndexShard> {

    /**
     * 该对象监听 index 发生的各种事件
     */
    private final IndexEventListener eventListener;
    /**
     * 该对象可以以field为单位查询数据  并且内部还嵌入了缓存功能
     */
    private final IndexFieldDataService indexFieldData;

    /**
     * 就没看到哪里体现了 filter  内部以query为单位进行预热 提前将数据存入到缓存中
     */
    private final BitsetFilterCache bitsetFilterCache;
    private final NodeEnvironment nodeEnv;
    /**
     * 这个就是indicesService
     * 可以删除 shardStore 或者addPendingDelete
     */
    private final ShardStoreDeleter shardStoreDeleter;

    /**
     * 通过 shardPath 可以获取到该分片数据所在的目录
     */
    private final IndexStorePlugin.DirectoryFactory directoryFactory;

    /**
     * 对reader对象进行包装  应该就是 ESReader
     */
    private final CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper;

    /**
     * 该对象内部整合了 queryCache/bitsetFilterCache
     */
    private final IndexCache indexCache;
    private final MapperService mapperService;
    private final NamedXContentRegistry xContentRegistry;
    private final NamedWriteableRegistry namedWriteableRegistry;
    /**
     * 先忽略这啥因子的
     */
    private final SimilarityService similarityService;
    /**
     * 每个索引对应一个引擎对象
     */
    private final EngineFactory engineFactory;
    /**
     * 预热对象 实际上就是提前将数据加载到缓存中
     */
    private final IndexWarmer warmer;
    /**
     * 管理该index 下所有的分片
     */
    private volatile Map<Integer, IndexShard> shards = emptyMap();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean deleted = new AtomicBoolean(false);
    private final IndexSettings indexSettings;

    /**
     * 当发生查询操作时 触发相关的钩子
     */
    private final List<SearchOperationListener> searchOperationListeners;
    /**
     * 针对索引操作的钩子
     */
    private final List<IndexingOperationListener> indexingOperationListeners;
    private final BooleanSupplier allowExpensiveQueries;
    /**
     * 定期触发所有 indexShard 的刷新任务
     */
    private volatile AsyncRefreshTask refreshTask;
    private volatile AsyncTranslogFSync fsyncTask;
    /**
     * 同步全局检查点的定时任务
     */
    private volatile AsyncGlobalCheckpointTask globalCheckpointTask;

    /**
     * 续约任务 为分片做续约么  跟eureka类似的套路???
     */
    private volatile AsyncRetentionLeaseSyncTask retentionLeaseSyncTask;

    // don't convert to Setting<> and register... we only set this in tests and register via a plugin
    private final String INDEX_TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING = "index.translog.retention.check_interval";

    /**
     * 定期裁剪事务日志的任务
     */
    private final AsyncTrimTranslogTask trimTranslogTask;
    private final ThreadPool threadPool;
    /**
     * 做了池化处理 减少GC压力
     */
    private final BigArrays bigArrays;
    /**
     * 脚本服务 还不知道是做什么的
     */
    private final ScriptService scriptService;
    /**
     * 该对象负责感知集群中的变化
     */
    private final ClusterService clusterService;
    /**
     * 向其他node 发起请求的client
     */
    private final Client client;
    private final CircuitBreakerService circuitBreakerService;
    private final IndexNameExpressionResolver expressionResolver;

    /**
     * 通过该函数可以获取到 排序对象 sort
     */
    private Supplier<Sort> indexSortSupplier;
    private ValuesSourceRegistry valuesSourceRegistry;

    /**
     *
     * @param indexSettings
     * @param indexCreationContext  代表当前在什么场景下创建的indexService
     * @param nodeEnv
     * @param xContentRegistry
     * @param similarityService
     * @param shardStoreDeleter
     * @param indexAnalyzers
     * @param engineFactory
     * @param circuitBreakerService
     * @param bigArrays
     * @param threadPool
     * @param scriptService
     * @param clusterService
     * @param client
     * @param queryCache
     * @param directoryFactory
     * @param eventListener
     * @param wrapperFactory   通过某个indexService  返回一个wrapper函数 负责将某个reader包装
     * @param mapperRegistry
     * @param indicesFieldDataCache
     * @param searchOperationListeners
     * @param indexingOperationListeners
     * @param namedWriteableRegistry
     * @param idFieldDataEnabled
     * @param allowExpensiveQueries
     * @param expressionResolver
     * @param valuesSourceRegistry
     */
    public IndexService(
            IndexSettings indexSettings,
            IndexCreationContext indexCreationContext,
            NodeEnvironment nodeEnv,
            NamedXContentRegistry xContentRegistry,
            SimilarityService similarityService,
            ShardStoreDeleter shardStoreDeleter,
            IndexAnalyzers indexAnalyzers, EngineFactory engineFactory,
            CircuitBreakerService circuitBreakerService,
            BigArrays bigArrays,
            ThreadPool threadPool,
            ScriptService scriptService,
            ClusterService clusterService,
            Client client,
            QueryCache queryCache,
            IndexStorePlugin.DirectoryFactory directoryFactory,
            IndexEventListener eventListener,
            Function<IndexService, CheckedFunction<DirectoryReader, DirectoryReader, IOException>> wrapperFactory,
            MapperRegistry mapperRegistry,
            IndicesFieldDataCache indicesFieldDataCache,
            List<SearchOperationListener> searchOperationListeners,
            List<IndexingOperationListener> indexingOperationListeners,
            NamedWriteableRegistry namedWriteableRegistry,
            BooleanSupplier idFieldDataEnabled,
            BooleanSupplier allowExpensiveQueries,
            IndexNameExpressionResolver expressionResolver,
            ValuesSourceRegistry valuesSourceRegistry) {
        super(indexSettings);
        this.allowExpensiveQueries = allowExpensiveQueries;
        this.indexSettings = indexSettings;
        this.xContentRegistry = xContentRegistry;
        this.similarityService = similarityService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.circuitBreakerService = circuitBreakerService;
        this.expressionResolver = expressionResolver;
        this.valuesSourceRegistry =  valuesSourceRegistry;
        if (needsMapperService(indexSettings, indexCreationContext)) {
            assert indexAnalyzers != null;
            // 生成映射服务
            this.mapperService = new MapperService(indexSettings, indexAnalyzers, xContentRegistry, similarityService, mapperRegistry,
                // we parse all percolator queries as they would be parsed on shard 0
                () -> newQueryShardContext(0, null, System::currentTimeMillis, null), idFieldDataEnabled);
            // 生成数据服务
            this.indexFieldData = new IndexFieldDataService(indexSettings, indicesFieldDataCache, circuitBreakerService, mapperService);
            // 如果定义了排序规则 需要进行排序
            if (indexSettings.getIndexSortConfig().hasIndexSort()) {
                // we delay the actual creation of the sort order for this index because the mapping has not been merged yet.
                // The sort order is validated right after the merge of the mapping later in the process.
                this.indexSortSupplier = () -> indexSettings.getIndexSortConfig().buildIndexSort(
                    mapperService::fieldType,
                    indexFieldData::getForField
                );
            } else {
                this.indexSortSupplier = () -> null;
            }
            indexFieldData.setListener(new FieldDataCacheListener(this));
            this.bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetCacheListener(this));
            // 将bitsetFilterCache 的监听器设置到 indexWarmer中监听预热事件  之后会以query为单位预加载数据
            this.warmer = new IndexWarmer(threadPool, indexFieldData, bitsetFilterCache.createListener(threadPool));

            // indexCache作为一个整合了内部所有cache的对象
            this.indexCache = new IndexCache(indexSettings, queryCache, bitsetFilterCache);
        } else {
            // 只有当前metadata == CLOSE 且 indexCreationContext == CREATE_INDEX 才会进入下面
            assert indexAnalyzers == null;
            this.mapperService = null;
            this.indexFieldData = null;
            this.indexSortSupplier = () -> null;
            this.bitsetFilterCache = null;
            this.warmer = null;
            this.indexCache = null;
        }

        this.shardStoreDeleter = shardStoreDeleter;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.client = client;
        this.eventListener = eventListener;
        this.nodeEnv = nodeEnv;
        this.directoryFactory = directoryFactory;
        this.engineFactory = Objects.requireNonNull(engineFactory);
        // initialize this last -- otherwise if the wrapper requires any other member to be non-null we fail with an NPE
        this.readerWrapper = wrapperFactory.apply(this);
        this.searchOperationListeners = Collections.unmodifiableList(searchOperationListeners);
        this.indexingOperationListeners = Collections.unmodifiableList(indexingOperationListeners);
        // kick off async ops for the first shard in this index
        this.refreshTask = new AsyncRefreshTask(this);
        this.trimTranslogTask = new AsyncTrimTranslogTask(this);
        this.globalCheckpointTask = new AsyncGlobalCheckpointTask(this);
        this.retentionLeaseSyncTask = new AsyncRetentionLeaseSyncTask(this);
        // 刷盘任务需要检测是否要启动
        updateFsyncTaskIfNecessary();
    }

    static boolean needsMapperService(IndexSettings indexSettings, IndexCreationContext indexCreationContext) {
        return false == (indexSettings.getIndexMetadata().getState() == IndexMetadata.State.CLOSE &&
            indexCreationContext == IndexCreationContext.CREATE_INDEX); // metadata verification needs a mapper service
    }

    /**
     * 创建还分为2种么
     */
    public enum IndexCreationContext {
        CREATE_INDEX,
        /**
         * 元数据校验是什么
         */
        METADATA_VERIFICATION
    }

    public int numberOfShards() {
        return shards.size();
    }

    public IndexEventListener getIndexEventListener() {
        return this.eventListener;
    }

    @Override
    public Iterator<IndexShard> iterator() {
        return shards.values().iterator();
    }

    public boolean hasShard(int shardId) {
        return shards.containsKey(shardId);
    }

    /**
     * Return the shard with the provided id, or null if there is no such shard.
     */
    @Override
    @Nullable
    public IndexShard getShardOrNull(int shardId) {
        return shards.get(shardId);
    }

    /**
     * Return the shard with the provided id, or throw an exception if it doesn't exist.
     */
    public IndexShard getShard(int shardId) {
        IndexShard indexShard = getShardOrNull(shardId);
        if (indexShard == null) {
            throw new ShardNotFoundException(new ShardId(index(), shardId));
        }
        return indexShard;
    }

    public Set<Integer> shardIds() {
        return shards.keySet();
    }

    public IndexCache cache() {
        return indexCache;
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.mapperService.getIndexAnalyzers();
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public SimilarityService similarityService() {
        return similarityService;
    }

    public Supplier<Sort> getIndexSortSupplier() {
        return indexSortSupplier;
    }

    public synchronized void close(final String reason, boolean delete) throws IOException {
        if (closed.compareAndSet(false, true)) {
            deleted.compareAndSet(false, delete);
            try {
                final Set<Integer> shardIds = shardIds();
                for (final int shardId : shardIds) {
                    try {
                        removeShard(shardId, reason);
                    } catch (Exception e) {
                        logger.warn("failed to close shard", e);
                    }
                }
            } finally {
                IOUtils.close(
                        bitsetFilterCache,
                        indexCache,
                        indexFieldData,
                        mapperService,
                        refreshTask,
                        fsyncTask,
                        trimTranslogTask,
                        globalCheckpointTask,
                        retentionLeaseSyncTask);
            }
        }
    }

    // method is synchronized so that IndexService can't be closed while we're writing out dangling indices information
    // 首先本index是一个摇摆索引 并且当收到最新的 indexMetadata时 会触发该方法
    public synchronized void writeDanglingIndicesInfo() {
        if (closed.get()) {
            return;
        }
        try {
            IndexMetadata.FORMAT.writeAndCleanup(getMetadata(), nodeEnv.indexPaths(index()));
        } catch (WriteStateException e) {
            logger.warn(() -> new ParameterizedMessage("failed to write dangling indices state for index {}", index()), e);
        }
    }

    // method is synchronized so that IndexService can't be closed while we're deleting dangling indices information
    // 如果 indicesService 不支持使用摇摆索引 那么进行删除
    public synchronized void deleteDanglingIndicesInfo() {
        if (closed.get()) {
            return;
        }
        try {
            MetadataStateFormat.deleteMetaState(nodeEnv.indexPaths(index()));
        } catch (IOException e) {
            logger.warn(() -> new ParameterizedMessage("failed to delete dangling indices state for index {}", index()), e);
        }
    }

    public String indexUUID() {
        return indexSettings.getUUID();
    }

    // NOTE: O(numShards) cost, but numShards should be smallish?
    private long getAvgShardSizeInBytes() throws IOException {
        long sum = 0;
        int count = 0;
        for (IndexShard indexShard : this) {
            sum += indexShard.store().stats().sizeInBytes();
            count++;
        }
        if (count == 0) {
            return -1L;
        } else {
            return sum / count;
        }
    }

    /**
     * 创建一个新的分片
     * @param routing  描述分片路由信息
     * @param globalCheckpointSyncer 全局检查点同步器是什么鬼
     * @param retentionLeaseSyncer   续约同步器  TODO 先忽略续约吧
     * @return
     * @throws IOException
     */
    public synchronized IndexShard createShard(
            final ShardRouting routing,
            final Consumer<ShardId> globalCheckpointSyncer,
            final RetentionLeaseSyncer retentionLeaseSyncer) throws IOException {
        Objects.requireNonNull(retentionLeaseSyncer);
        /*
         * TODO: we execute this in parallel but it's a synced method. Yet, we might
         * be able to serialize the execution via the cluster state in the future. for now we just
         * keep it synced.
         */
        if (closed.get()) {
            throw new IllegalStateException("Can't create shard " + routing.shardId() + ", closed");
        }
        final Settings indexSettings = this.indexSettings.getSettings();
        // 路由信息中包含了分片id
        final ShardId shardId = routing.shardId();
        boolean success = false;
        Store store = null;
        IndexShard indexShard = null;
        ShardLock lock = null;
        try {
            // 首先获取一个shard级别的锁
            lock = nodeEnv.shardLock(shardId, "shard creation", TimeUnit.SECONDS.toMillis(5));
            eventListener.beforeIndexShardCreated(shardId, indexSettings);
            ShardPath path;
            try {
                // 生成该分片对应的目录
                path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings.customDataPath());
            } catch (IllegalStateException ex) {
                logger.warn("{} failed to load shard path, trying to remove leftover", shardId);
                try {
                    ShardPath.deleteLeftoverShardDirectory(logger, nodeEnv, lock, this.indexSettings);
                    path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings.customDataPath());
                } catch (Exception inner) {
                    ex.addSuppressed(inner);
                    throw ex;
                }
            }

            // 没有找到目录
            if (path == null) {
                // TODO: we should, instead, hold a "bytes reserved" of how large we anticipate this shard will be, e.g. for a shard
                // that's being relocated/replicated we know how large it will become once it's done copying:
                // Count up how many shards are currently on each data path:
                // 寻找该index下其他分片的根路径
                Map<Path, Integer> dataPathToShardCount = new HashMap<>();
                for (IndexShard shard : this) {
                    // TODO 这个是不一样的是吗  该index下所有分片分在了不同的 dataPath下
                    Path dataPath = shard.shardPath().getRootStatePath();
                    Integer curCount = dataPathToShardCount.get(dataPath);
                    if (curCount == null) {
                        curCount = 0;
                    }
                    dataPathToShardCount.put(dataPath, curCount + 1);
                }
                path = ShardPath.selectNewPathForShard(nodeEnv, shardId, this.indexSettings,
                    // 当没有指定期望大小时 使用平均大小
                    routing.getExpectedShardSize() == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
                        ? getAvgShardSizeInBytes() : routing.getExpectedShardSize(),
                    dataPathToShardCount);
                logger.debug("{} creating using a new path [{}]", shardId, path);
            } else {
                logger.debug("{} creating using an existing path [{}]", shardId, path);
            }

            if (shards.containsKey(shardId.id())) {
                throw new IllegalStateException(shardId + " already exists");
            }

            logger.debug("creating shard_id {}", shardId);
            // if we are on a shared FS we only own the shard (ie. we can safely delete it) if we are the primary.
            final Engine.Warmer engineWarmer = (reader) -> {
                IndexShard shard =  getShardOrNull(shardId.getId());
                if (shard != null) {
                    warmer.warm(reader, shard, IndexService.this.indexSettings);
                }
            };
            Directory directory = directoryFactory.newDirectory(this.indexSettings, path);
            store = new Store(shardId, this.indexSettings, directory, lock,
                    new StoreCloseListener(shardId, () -> eventListener.onStoreClosed(shardId)));
            eventListener.onStoreCreated(shardId);
            indexShard = new IndexShard(
                    routing,
                    this.indexSettings,
                    path,
                    store,
                    indexSortSupplier,
                    indexCache,
                    mapperService,
                    similarityService,
                    engineFactory,
                    eventListener,
                    readerWrapper,
                    threadPool,
                    bigArrays,
                    engineWarmer,
                    searchOperationListeners,
                    indexingOperationListeners,
                    () -> globalCheckpointSyncer.accept(shardId),
                    retentionLeaseSyncer,
                    circuitBreakerService);
            eventListener.indexShardStateChanged(indexShard, null, indexShard.state(), "shard created");
            eventListener.afterIndexShardCreated(indexShard);
            shards = Maps.copyMapWithAddedEntry(shards, shardId.id(), indexShard);
            success = true;
            return indexShard;
        } catch (ShardLockObtainFailedException e) {
            throw new IOException("failed to obtain in-memory shard lock", e);
        } finally {
            if (success == false) {
                if (lock != null) {
                    IOUtils.closeWhileHandlingException(lock);
                }
                closeShard("initialization failed", shardId, indexShard, store, eventListener);
            }
        }
    }

    @Override
    public synchronized void removeShard(int shardId, String reason) {
        final ShardId sId = new ShardId(index(), shardId);
        final IndexShard indexShard;
        if (shards.containsKey(shardId) == false) {
            return;
        }
        logger.debug("[{}] closing... (reason: [{}])", shardId, reason);
        HashMap<Integer, IndexShard> newShards = new HashMap<>(shards);
        indexShard = newShards.remove(shardId);
        shards = unmodifiableMap(newShards);
        closeShard(reason, sId, indexShard, indexShard.store(), indexShard.getIndexEventListener());
        logger.debug("[{}] closed (reason: [{}])", shardId, reason);
    }

    private void closeShard(String reason, ShardId sId, IndexShard indexShard, Store store, IndexEventListener listener) {
        final int shardId = sId.id();
        final Settings indexSettings = this.getIndexSettings().getSettings();
        try {
            try {
                listener.beforeIndexShardClosed(sId, indexShard, indexSettings);
            } finally {
                // this logic is tricky, we want to close the engine so we rollback the changes done to it
                // and close the shard so no operations are allowed to it
                if (indexShard != null) {
                    try {
                        // only flush we are we closed (closed index or shutdown) and if we are not deleted
                        final boolean flushEngine = deleted.get() == false && closed.get();
                        indexShard.close(reason, flushEngine);
                    } catch (Exception e) {
                        logger.debug(() -> new ParameterizedMessage("[{}] failed to close index shard", shardId), e);
                        // ignore
                    }
                }
                // call this before we close the store, so we can release resources for it
                listener.afterIndexShardClosed(sId, indexShard, indexSettings);
            }
        } finally {
            try {
                if (store != null) {
                    store.close();
                } else {
                    logger.trace("[{}] store not initialized prior to closing shard, nothing to close", shardId);
                }
            } catch (Exception e) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "[{}] failed to close store on shard removal (reason: [{}])", shardId, reason), e);
            }
        }
    }


    private void onShardClose(ShardLock lock) {
        if (deleted.get()) { // we remove that shards content if this index has been deleted
            try {
                try {
                    eventListener.beforeIndexShardDeleted(lock.getShardId(), indexSettings.getSettings());
                } finally {
                    shardStoreDeleter.deleteShardStore("delete index", lock, indexSettings);
                    eventListener.afterIndexShardDeleted(lock.getShardId(), indexSettings.getSettings());
                }
            } catch (IOException e) {
                shardStoreDeleter.addPendingDelete(lock.getShardId(), indexSettings);
                logger.debug(
                    () -> new ParameterizedMessage(
                        "[{}] failed to delete shard content - scheduled a retry", lock.getShardId().id()), e);
            }
        }
    }

    @Override
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Creates a new QueryShardContext.
     *
     * Passing a {@code null} {@link IndexSearcher} will return a valid context, however it won't be able to make
     * {@link IndexReader}-specific optimizations, such as rewriting containing range queries.
     */
    public QueryShardContext newQueryShardContext(int shardId, IndexSearcher searcher, LongSupplier nowInMillis, String clusterAlias) {
        final SearchIndexNameMatcher indexNameMatcher =
            new SearchIndexNameMatcher(index().getName(), clusterAlias, clusterService, expressionResolver);
        return new QueryShardContext(
            shardId, indexSettings, bigArrays, indexCache.bitsetFilterCache(), indexFieldData::getForField, mapperService(),
            similarityService(), scriptService, xContentRegistry, namedWriteableRegistry, client, searcher, nowInMillis, clusterAlias,
            indexNameMatcher, allowExpensiveQueries, valuesSourceRegistry);
    }

    /**
     * The {@link ThreadPool} to use for this index.
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    /**
     * The {@link BigArrays} to use for this index.
     */
    public BigArrays getBigArrays() {
        return bigArrays;
    }

    /**
     * The {@link ScriptService} to use for this index.
     */
    public ScriptService getScriptService() {
        return scriptService;
    }

    List<IndexingOperationListener> getIndexOperationListeners() { // pkg private for testing
        return indexingOperationListeners;
    }

    List<SearchOperationListener> getSearchOperationListener() { // pkg private for testing
        return searchOperationListeners;
    }

    @Override
    public boolean updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) throws IOException {
        if (mapperService == null) {
            return false;
        }
        return mapperService.updateMapping(currentIndexMetadata, newIndexMetadata);
    }

    private class StoreCloseListener implements Store.OnClose {
        private final ShardId shardId;
        private final Closeable[] toClose;

        StoreCloseListener(ShardId shardId, Closeable... toClose) {
            this.shardId = shardId;
            this.toClose = toClose;
        }

        @Override
        public void accept(ShardLock lock) {
            try {
                assert lock.getShardId().equals(shardId) : "shard id mismatch, expected: " + shardId + " but got: " + lock.getShardId();
                onShardClose(lock);
            } finally {
                try {
                    IOUtils.close(toClose);
                } catch (IOException ex) {
                    logger.debug("failed to close resource", ex);
                }
            }

        }
    }

    /**
     * 以某个query为条件查询出来的 docIdSet结果被缓存时 触发OnCache
     * 钩子中对应的操作也是进行一些统计工作
     */
    private static final class BitsetCacheListener implements BitsetFilterCache.Listener {
        final IndexService indexService;

        private BitsetCacheListener(IndexService indexService) {
            this.indexService = indexService;
        }

        @Override
        public void onCache(ShardId shardId, Accountable accountable) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    long ramBytesUsed = accountable != null ? accountable.ramBytesUsed() : 0L;
                    shard.shardBitsetFilterCache().onCached(ramBytesUsed);
                }
            }
        }

        @Override
        public void onRemoval(ShardId shardId, Accountable accountable) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    long ramBytesUsed = accountable != null ? accountable.ramBytesUsed() : 0L;
                    shard.shardBitsetFilterCache().onRemoval(ramBytesUsed);
                }
            }
        }
    }

    /**
     * 监听 IndexFieldCache 内部的数据变化  比如加载到对应field的数据时 触发 onCache 当index被移除(导致下面所有的fieldCache都需要被移除时) 触发onRemoval
     */
    private final class FieldDataCacheListener implements IndexFieldDataCache.Listener {
        final IndexService indexService;

        FieldDataCacheListener(IndexService indexService) {
            this.indexService = indexService;
        }

        /**
         * 获取到某个
         * @param shardId  每个reader对象对应一个shardId
         * @param fieldName
         * @param ramUsage
         */
        @Override
        public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
            if (shardId != null) {
                // 获取对应的分片
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    // 触发钩子 实际上只是做了统计操作
                    shard.fieldData().onCache(shardId, fieldName, ramUsage);
                }
            }
        }

        @Override
        public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onRemoval(shardId, fieldName, wasEvicted, sizeInBytes);
                }
            }
        }
    }

    public IndexMetadata getMetadata() {
        return indexSettings.getIndexMetadata();
    }

    private final CopyOnWriteArrayList<Consumer<IndexMetadata>> metadataListeners = new CopyOnWriteArrayList<>();

    public void addMetadataListener(Consumer<IndexMetadata> listener) {
        metadataListeners.add(listener);
    }


    /**
     * 根据前后变化的元数据 更新indexService
     * @param currentIndexMetadata the current index metadata
     * @param newIndexMetadata the new index metadata
     */
    @Override
    public synchronized void updateMetadata(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) {
        final boolean updateIndexSettings = indexSettings.updateIndexMetadata(newIndexMetadata);

        if (Assertions.ENABLED && currentIndexMetadata != null) {
            final long currentSettingsVersion = currentIndexMetadata.getSettingsVersion();
            final long newSettingsVersion = newIndexMetadata.getSettingsVersion();
            if (currentSettingsVersion == newSettingsVersion) {
                assert updateIndexSettings == false;
            } else {
                assert updateIndexSettings;
                assert currentSettingsVersion < newSettingsVersion :
                        "expected current settings version [" + currentSettingsVersion + "] "
                                + "to be less than new settings version [" + newSettingsVersion + "]";
            }
        }

        if (updateIndexSettings) {
            for (final IndexShard shard : this.shards.values()) {
                try {
                    shard.onSettingsChanged();
                } catch (Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "[{}] failed to notify shard about setting change", shard.shardId().id()), e);
                }
            }
            if (refreshTask.getInterval().equals(indexSettings.getRefreshInterval()) == false) {
                // once we change the refresh interval we schedule yet another refresh
                // to ensure we are in a clean and predictable state.
                // it doesn't matter if we move from or to <code>-1</code>  in both cases we want
                // docs to become visible immediately. This also flushes all pending indexing / search requests
                // that are waiting for a refresh.
                threadPool.executor(ThreadPool.Names.REFRESH).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("forced refresh failed after interval change", e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        maybeRefreshEngine(true);
                    }

                    @Override
                    public boolean isForceExecution() {
                        return true;
                    }
                });
                rescheduleRefreshTasks();
            }
            updateFsyncTaskIfNecessary();
        }

        metadataListeners.forEach(c -> c.accept(newIndexMetadata));
    }

    /**
     * 检测是否要开启 fsyncTask
     */
    private void updateFsyncTaskIfNecessary() {
        // 代表基于API 调用才会执行刷盘操作
        if (indexSettings.getTranslogDurability() == Translog.Durability.REQUEST) {
            try {
                if (fsyncTask != null) {
                    fsyncTask.close();
                }
            } finally {
                fsyncTask = null;
            }
        } else if (fsyncTask == null) {
            fsyncTask = new AsyncTranslogFSync(this);
        } else {
            fsyncTask.updateIfNeeded();
        }
    }

    private void rescheduleRefreshTasks() {
        try {
            refreshTask.close();
        } finally {
            refreshTask = new AsyncRefreshTask(this);
        }
    }


    public interface ShardStoreDeleter {
        void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException;

        void addPendingDelete(ShardId shardId, IndexSettings indexSettings);
    }

    public final EngineFactory getEngineFactory() {
        return engineFactory;
    }

    final CheckedFunction<DirectoryReader, DirectoryReader, IOException> getReaderWrapper() {
        return readerWrapper;
    } // pkg private for testing

    final IndexStorePlugin.DirectoryFactory getDirectoryFactory() {
        return directoryFactory;
    } // pkg private for testing

    private void maybeFSyncTranslogs() {
        // 确保事务日志本身是异步的
        if (indexSettings.getTranslogDurability() == Translog.Durability.ASYNC) {
            for (IndexShard shard : this.shards.values()) {
                try {
                    if (shard.isSyncNeeded()) {
                        shard.sync();
                    }
                } catch (AlreadyClosedException ex) {
                    // fine - continue;
                } catch (IOException e) {
                    logger.warn("failed to sync translog", e);
                }
            }
        }
    }

    /**
     * 执行一次刷新任务
     * @param force
     */
    private void maybeRefreshEngine(boolean force) {
        if (indexSettings.getRefreshInterval().millis() > 0 || force) {
            for (IndexShard shard : this.shards.values()) {
                try {
                    shard.scheduledRefresh();
                } catch (IndexShardClosedException | AlreadyClosedException ex) {
                    // fine - continue;
                }
            }
        }
    }

    /**
     * 裁剪掉无用的事务日志
     */
    private void maybeTrimTranslog() {
        for (IndexShard shard : this.shards.values()) {
            switch (shard.state()) {
                case CREATED:
                case RECOVERING:
                case CLOSED:
                    continue;
                case POST_RECOVERY:
                case STARTED:
                    try {
                        shard.trimTranslog();
                    } catch (IndexShardClosedException | AlreadyClosedException ex) {
                        // fine - continue;
                    }
                    continue;
                default:
                    throw new IllegalStateException("unknown state: " + shard.state());
            }
        }
    }

    private void maybeSyncGlobalCheckpoints() {
        sync(is -> is.maybeSyncGlobalCheckpoint("background"), "global checkpoint");
    }

    private void syncRetentionLeases() {
        sync(IndexShard::syncRetentionLeases, "retention lease");
    }

    /**
     * 使用当前线程 针对所有indexShard 执行函数
     * @param sync
     * @param source
     */
    private void sync(final Consumer<IndexShard> sync, final String source) {
        for (final IndexShard shard : this.shards.values()) {
            // 只处理活跃的主分片
            if (shard.routingEntry().active() && shard.routingEntry().primary()) {
                switch (shard.state()) {
                    case CLOSED:
                    case CREATED:
                    case RECOVERING:
                        continue;
                    case POST_RECOVERY:
                        assert false : "shard " + shard.shardId() + " is in post-recovery but marked as active";
                        continue;
                        // 只处理此时正在使用的分片
                    case STARTED:
                        try {
                            shard.runUnderPrimaryPermit(
                                    () -> sync.accept(shard),
                                    // 处理异常 只是打印日志
                                    e -> {
                                        if (e instanceof AlreadyClosedException == false
                                            && e instanceof IndexShardClosedException == false
                                            && e instanceof ShardNotInPrimaryModeException == false) {
                                            logger.warn(
                                                    new ParameterizedMessage(
                                                            "{} failed to execute {} sync", shard.shardId(), source), e);
                                        }
                                    },
                                    ThreadPool.Names.SAME,
                                    source + " sync");
                        } catch (final AlreadyClosedException | IndexShardClosedException e) {
                            // the shard was closed concurrently, continue
                        }
                        continue;
                    default:
                        throw new IllegalStateException("unknown state [" + shard.state() + "]");
                }
            }
        }
    }

    /**
     * AbstractAsyncTask 定义了一个定时任务的模板
     */
    abstract static class BaseAsyncTask extends AbstractAsyncTask {

        protected final IndexService indexService;

        BaseAsyncTask(final IndexService indexService, final TimeValue interval) {
            super(indexService.logger, indexService.threadPool, interval, true);
            this.indexService = indexService;
            // 在初始化时启动任务
            rescheduleIfNecessary();
        }

        /**
         * 每次在加入开启下次定时前 会先通过该方法判断是否支持继续执行
         * @return
         */
        @Override
        protected boolean mustReschedule() {
            // don't re-schedule if the IndexService instance is closed or if the index is closed
            // 当index变为关闭时 自动停止定时任务
            return indexService.closed.get() == false
                && indexService.indexSettings.getIndexMetadata().getState() == IndexMetadata.State.OPEN;
        }
    }

    /**
     * FSyncs the translog for all shards of this index in a defined interval.
     * 定期对事务日志做持久化
     */
    static final class AsyncTranslogFSync extends BaseAsyncTask {

        AsyncTranslogFSync(IndexService indexService) {
            super(indexService, indexService.getIndexSettings().getTranslogSyncInterval());
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.FLUSH;
        }

        @Override
        protected void runInternal() {
            indexService.maybeFSyncTranslogs();
        }

        /**
         * 看来是监听了动态配置  当配置发生变化时 更新定时任务
         */
        void updateIfNeeded() {
            final TimeValue newInterval = indexService.getIndexSettings().getTranslogSyncInterval();
            if (newInterval.equals(getInterval()) == false) {
                setInterval(newInterval);
            }
        }

        @Override
        public String toString() {
            return "translog_sync";
        }
    }

    /**
     * 定义了任务的逻辑
     */
    final class AsyncRefreshTask extends BaseAsyncTask {

        AsyncRefreshTask(IndexService indexService) {
            super(indexService, indexService.getIndexSettings().getRefreshInterval());
        }

        /**
         * 每隔一定时间尝试进行一次 refresh
         */
        @Override
        protected void runInternal() {
            indexService.maybeRefreshEngine(false);
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.REFRESH;
        }

        @Override
        public String toString() {
            return "refresh";
        }
    }

    /**
     * 定期裁剪掉无用的事务日志
     */
    final class AsyncTrimTranslogTask extends BaseAsyncTask {

        AsyncTrimTranslogTask(IndexService indexService) {
            super(indexService, indexService.getIndexSettings()
                .getSettings().getAsTime(INDEX_TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING, TimeValue.timeValueMinutes(10)));
        }

        @Override
        protected boolean mustReschedule() {
            return indexService.closed.get() == false;
        }

        @Override
        protected void runInternal() {
            indexService.maybeTrimTranslog();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "trim_translog";
        }
    }

    // this setting is intentionally not registered, it is only used in tests
    public static final Setting<TimeValue> GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING =
            Setting.timeSetting(
                    "index.global_checkpoint_sync.interval",
                    new TimeValue(30, TimeUnit.SECONDS),
                    new TimeValue(0, TimeUnit.MILLISECONDS),
                    Property.Dynamic,
                    Property.IndexScope);

    // this setting is intentionally not registered, it is only used in tests
    public static final Setting<TimeValue> RETENTION_LEASE_SYNC_INTERVAL_SETTING =
            Setting.timeSetting(
                    "index.soft_deletes.retention_lease.sync_interval",
                    new TimeValue(30, TimeUnit.SECONDS),
                    new TimeValue(0, TimeUnit.MILLISECONDS),
                    Property.Dynamic,
                    Property.IndexScope);

    /**
     * Background task that syncs the global checkpoint to replicas.
     * 同步全局检查点
     */
    final class AsyncGlobalCheckpointTask extends BaseAsyncTask {

        AsyncGlobalCheckpointTask(final IndexService indexService) {
            // index.global_checkpoint_sync_interval is not a real setting, it is only registered in tests
            super(indexService, GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.get(indexService.getIndexSettings().getSettings()));
        }

        @Override
        protected void runInternal() {
            indexService.maybeSyncGlobalCheckpoints();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "global_checkpoint_sync";
        }
    }

    /**
     * 定期执行续约任务
     */
    final class AsyncRetentionLeaseSyncTask extends BaseAsyncTask {

        AsyncRetentionLeaseSyncTask(final IndexService indexService) {
            super(indexService, RETENTION_LEASE_SYNC_INTERVAL_SETTING.get(indexService.getIndexSettings().getSettings()));
        }

        @Override
        protected void runInternal() {
            indexService.syncRetentionLeases();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.MANAGEMENT;
        }

        @Override
        public String toString() {
            return "retention_lease_sync";
        }

    }

    AsyncRefreshTask getRefreshTask() { // for tests
        return refreshTask;
    }

    AsyncTranslogFSync getFsyncTask() { // for tests
        return fsyncTask;
    }

    AsyncTrimTranslogTask getTrimTranslogTask() { // for tests
        return trimTranslogTask;
    }

    /**
     * Clears the caches for the given shard id if the shard is still allocated on this node
     * @param queryCache 是否要清除 queryCache
     * @param fieldDataCache 是否要清除 fieldDataCache
     * @param fields 代表仅清除符合的field 数据
     */
    public boolean clearCaches(boolean queryCache, boolean fieldDataCache, String...fields) {
        boolean clearedAtLeastOne = false;
        if (queryCache) {
            clearedAtLeastOne = true;
            indexCache.query().clear("api");
        }
        if (fieldDataCache) {
            clearedAtLeastOne = true;
            // 在没有指定field数量的时候 清除所有数据
            if (fields.length == 0) {
                indexFieldData.clear();
            } else {
                for (String field : fields) {
                    indexFieldData.clearField(field);
                }
            }
        }
        // 如果没有声明要删除哪些缓存
        if (clearedAtLeastOne == false) {
            // 如果fields 为空 就认为是要清除所有缓存
            if (fields.length ==  0) {
                indexCache.clear("api");
                indexFieldData.clear();
            } else {
                // 如果fields 不为空 就认为只需要清理 indexFieldData
                // only clear caches relating to the specified fields
                for (String field : fields) {
                    indexFieldData.clearField(field);
                }
            }
        }
        return clearedAtLeastOne;
    }

}
