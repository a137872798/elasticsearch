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

package org.elasticsearch.index.cache.bitset;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexWarmer;
import org.elasticsearch.index.IndexWarmer.TerminationHandle;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * This is a cache for {@link BitDocIdSet} based filters and is unbounded by size or time.
 * <p>
 * Use this cache with care, only components that require that a filter is to be materialized as a {@link BitDocIdSet}
 * and require that it should always be around should use this cache, otherwise the
 * {@link org.elasticsearch.index.cache.query.QueryCache} should be used instead.
 * 基于位图的缓存对象
 * 该对象与index一一对应
 * 而IndicesFieldDataCache与indicesService一一对应
 */
public final class BitsetFilterCache extends AbstractIndexComponent
    implements IndexReader.ClosedListener, RemovalListener<IndexReader.CacheKey, Cache<Query, BitsetFilterCache.Value>>, Closeable {

    public static final Setting<Boolean> INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING =
        Setting.boolSetting("index.load_fixed_bitset_filters_eagerly", true, Property.IndexScope);

    private final boolean loadRandomAccessFiltersEagerly;
    /**
     * 每个缓存键 对应一个缓存对象  缓存键应该是以index为维度进行划分的
     * 而在二级缓存中 又是以Query作为key
     */
    private final Cache<IndexReader.CacheKey, Cache<Query, Value>> loadedFilters;
    private final Listener listener;

    /**
     * @param indexSettings  索引配置项
     * @param listener  进行一些数据统计的监听器 先忽略
     */
    public BitsetFilterCache(IndexSettings indexSettings, Listener listener) {
        super(indexSettings);
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        // 只有该标识为true 才会开启预热
        this.loadRandomAccessFiltersEagerly = this.indexSettings.getValue(INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING);
        this.loadedFilters = CacheBuilder.<IndexReader.CacheKey, Cache<Query, Value>>builder().removalListener(this).build();
        this.listener = listener;
    }

    public static BitSet bitsetFromQuery(Query query, LeafReaderContext context) throws IOException {
        // 获取最上层的 reader对象  在使用过程中 还没有看到以 父子级关系连接的逻辑
        final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
        final IndexSearcher searcher = new IndexSearcher(topLevelContext);
        searcher.setQueryCache(null);
        final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        Scorer s = weight.scorer(context);
        if (s == null) {
            return null;
        } else {
            // 代表query 查询到了数据 将docIdSet 和最大的doc号 封装成一个位图对象
            return BitSet.of(s.iterator(), context.reader().maxDoc());
        }
    }

    /**
     * 生成基于 liveDoc的缓存对象
     * @param threadPool
     * @return
     */
    public IndexWarmer.Listener createListener(ThreadPool threadPool) {
        return new BitSetProducerWarmer(threadPool);
    }


    public BitSetProducer getBitSetProducer(Query query) {
        return new QueryWrapperBitSetProducer(query);
    }

    @Override
    public void onClose(IndexReader.CacheKey ownerCoreCacheKey) {
        loadedFilters.invalidate(ownerCoreCacheKey);
    }

    @Override
    public void close() {
        clear("close");
    }

    public void clear(String reason) {
        logger.debug("clearing all bitsets because [{}]", reason);
        loadedFilters.invalidateAll();
    }

    /**
     * 通过 query 去context内部的reader中读取数据 并将结果docIdSet 包装成位图  以及存储到缓存中
     *
     * @param query
     * @param context
     * @return
     * @throws ExecutionException
     */
    private BitSet getAndLoadIfNotPresent(final Query query, final LeafReaderContext context) throws ExecutionException {
        final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
        if (cacheHelper == null) {
            // 如果不支持缓存会直接抛出异常
            throw new IllegalArgumentException("Reader " + context.reader() + " does not support caching");
        }
        final IndexReader.CacheKey coreCacheReader = cacheHelper.getKey();
        // 从reader上获取分片id
        final ShardId shardId = ShardUtils.extractShardId(context.reader());
        if (indexSettings.getIndex().equals(shardId.getIndex()) == false) {
            // insanity
            throw new IllegalStateException("Trying to load bit set for index " + shardId.getIndex()
                + " with cache of index " + indexSettings.getIndex());
        }
        Cache<Query, Value> filterToFbs = loadedFilters.computeIfAbsent(coreCacheReader, key -> {
            // 当reader资源关闭时 触发钩子
            cacheHelper.addClosedListener(BitsetFilterCache.this);
            return CacheBuilder.<Query, Value>builder().build();
        });

        return filterToFbs.computeIfAbsent(query, key -> {
            final BitSet bitSet = bitsetFromQuery(query, context);
            Value value = new Value(bitSet, shardId);
            listener.onCache(shardId, value.bitset);
            return value;
        }).bitset;
    }

    /**
     * 当某个缓存移除时触发
     * 因为这里使用的是二级缓存  代表当某个reader过期时  就要将下面所有的基于 query查询的缓存结果移除
     *
     * @param notification
     */
    @Override
    public void onRemoval(RemovalNotification<IndexReader.CacheKey, Cache<Query, Value>> notification) {
        if (notification.getKey() == null) {
            return;
        }

        Cache<Query, Value> valueCache = notification.getValue();
        if (valueCache == null) {
            return;
        }

        for (Value value : valueCache.values()) {
            listener.onRemoval(value.shardId, value.bitset);
            // if null then this means the shard has already been removed and the stats are 0 anyway for the shard this key belongs to
        }
    }

    /**
     * 包装了 某个query查询到的 docIdSet  以及reader相关的分片
     */
    public static final class Value {

        final BitSet bitset;
        final ShardId shardId;

        public Value(BitSet bitset, ShardId shardId) {
            this.bitset = bitset;
            this.shardId = shardId;
        }
    }

    /**
     * 通过readerContext 对象 可以使用query查询数据 并将结果docIdSet存入到缓存中
     */
    final class QueryWrapperBitSetProducer implements BitSetProducer {

        final Query query;

        QueryWrapperBitSetProducer(Query query) {
            this.query = Objects.requireNonNull(query);
        }

        @Override
        public BitSet getBitSet(LeafReaderContext context) throws IOException {
            try {
                return getAndLoadIfNotPresent(query, context);
            } catch (ExecutionException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
        }

        @Override
        public String toString() {
            return "random_access(" + query + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof QueryWrapperBitSetProducer)) return false;
            return this.query.equals(((QueryWrapperBitSetProducer) o).query);
        }

        @Override
        public int hashCode() {
            return 31 * getClass().hashCode() + query.hashCode();
        }
    }

    /**
     * 该对象本身实现了 预热对象的监听器接口  可以针对indexShard进行预热
     * 默认的预热实现就是将数据提前加载到内存中
     */
    final class BitSetProducerWarmer implements IndexWarmer.Listener {

        private final Executor executor;

        BitSetProducerWarmer(ThreadPool threadPool) {
            this.executor = threadPool.executor(ThreadPool.Names.WARMER);
        }

        /**
         * 进行预热操作
         *
         * @param indexShard
         * @param reader
         * @return  调用TerminationHandler 结束时就代表预热完成了
         */
        @Override
        public IndexWarmer.TerminationHandle warmReader(final IndexShard indexShard, final ElasticsearchDirectoryReader reader) {
            // 这种情况应该不会发生吧
            if (indexSettings.getIndex().equals(indexShard.indexSettings().getIndex()) == false) {
                // this is from a different index
                return TerminationHandle.NO_WAIT;
            }

            // 代表是否支持开启预热
            if (!loadRandomAccessFiltersEagerly) {
                return TerminationHandle.NO_WAIT;
            }

            boolean hasNested = false;
            final Set<Query> warmUp = new HashSet<>();
            final MapperService mapperService = indexShard.mapperService();
            DocumentMapper docMapper = mapperService.documentMapper();
            // TODO 先不管预热逻辑吧 反正就是预先加载到缓存 不影响正常流程
            if (docMapper != null) {
                if (docMapper.hasNestedObjects()) {
                    hasNested = true;
                    for (ObjectMapper objectMapper : docMapper.objectMappers().values()) {
                        if (objectMapper.nested().isNested()) {
                            ObjectMapper parentObjectMapper = objectMapper.getParentObjectMapper(mapperService);
                            if (parentObjectMapper != null && parentObjectMapper.nested().isNested()) {
                                warmUp.add(parentObjectMapper.nestedTypeFilter());
                            }
                        }
                    }
                }
            }

            if (hasNested) {
                warmUp.add(Queries.newNonNestedFilter());
            }

            final CountDownLatch latch = new CountDownLatch(reader.leaves().size() * warmUp.size());
            // 预热的实现是类似的 只是变成了通过query 定位数据
            for (final LeafReaderContext ctx : reader.leaves()) {
                for (final Query filterToWarm : warmUp) {
                    executor.execute(() -> {
                        try {
                            final long start = System.nanoTime();
                            getAndLoadIfNotPresent(filterToWarm, ctx);
                            if (indexShard.warmerService().logger().isTraceEnabled()) {
                                indexShard.warmerService().logger().trace("warmed bitset for [{}], took [{}]",
                                    filterToWarm, TimeValue.timeValueNanos(System.nanoTime() - start));
                            }
                        } catch (Exception e) {
                            indexShard.warmerService().logger().warn(() -> new ParameterizedMessage("failed to load " +
                                "bitset for [{}]", filterToWarm), e);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
            }
            return () -> latch.await();
        }

    }

    Cache<IndexReader.CacheKey, Cache<Query, Value>> getLoadedFilters() {
        return loadedFilters;
    }

    /**
     * A listener interface that is executed for each onCache / onRemoval event
     */
    public interface Listener {
        /**
         * Called for each cached bitset on the cache event.
         *
         * @param shardId     the shard id the bitset was cached for. This can be <code>null</code>
         * @param accountable the bitsets ram representation
         */
        void onCache(ShardId shardId, Accountable accountable);

        /**
         * Called for each cached bitset on the removal event.
         *
         * @param shardId     the shard id the bitset was cached for. This can be <code>null</code>
         * @param accountable the bitsets ram representation
         */
        void onRemoval(ShardId shardId, Accountable accountable);
    }
}
