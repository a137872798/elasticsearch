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

package org.elasticsearch.indices.fielddata.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.ToLongBiFunction;

/**
 * 二层缓存 首先这是一个总缓存对象
 * 然后细化到每个field 还有一个子缓存
 */
public class IndicesFieldDataCache implements RemovalListener<IndicesFieldDataCache.Key, Accountable>, Releasable {

    private static final Logger logger = LogManager.getLogger(IndicesFieldDataCache.class);

    public static final Setting<ByteSizeValue> INDICES_FIELDDATA_CACHE_SIZE_KEY =
        Setting.memorySizeSetting("indices.fielddata.cache.size", new ByteSizeValue(-1), Property.NodeScope);

    /**
     * 当插入缓存/移除缓存时触发
     */
    private final IndexFieldDataCache.Listener indicesFieldDataCacheListener;
    private final Cache<Key, Accountable> cache;


    /**
     * @param settings
     * @param indicesFieldDataCacheListener 该监听器包含 添加缓存/移除缓存时的钩子
     *                                      触发时减少熔断器此时使用量
     */
    public IndicesFieldDataCache(Settings settings, IndexFieldDataCache.Listener indicesFieldDataCacheListener) {
        this.indicesFieldDataCacheListener = indicesFieldDataCacheListener;
        final long sizeInBytes = INDICES_FIELDDATA_CACHE_SIZE_KEY.get(settings).getBytes();
        CacheBuilder<Key, Accountable> cacheBuilder = CacheBuilder.<Key, Accountable>builder()
            .removalListener(this);
        if (sizeInBytes > 0) {
            cacheBuilder.setMaximumWeight(sizeInBytes).weigher(new FieldDataWeigher());
        }
        cache = cacheBuilder.build();
    }

    @Override
    public void close() {
        cache.invalidateAll();
    }

    /**
     * 生成针对field 级别的缓存对象
     * 在通过 IndexFieldDataService 查询某个field的数据时 当允许使用缓存 而缓存未创建时 就会通过该方法生成field级别的缓存
     *
     * @param listener  就是 FieldDataCacheListener 只是做了一些数据统计工作
     * @param index     本次针对哪个index生成缓存  因为所有indices 共用该对象
     * @param fieldName 本次针对哪个field
     * @return
     */
    public IndexFieldDataCache buildIndexFieldDataCache(IndexFieldDataCache.Listener listener, Index index, String fieldName) {
        return new IndexFieldCache(logger, cache, index, fieldName, indicesFieldDataCacheListener, listener);
    }

    public Cache<Key, Accountable> getCache() {
        return cache;
    }

    /**
     * 当某个键值对从缓存中移除时触发
     *
     * @param notification
     */
    @Override
    public void onRemoval(RemovalNotification<Key, Accountable> notification) {
        Key key = notification.getKey();
        assert key != null && key.listeners != null;
        IndexFieldCache indexCache = key.indexCache;
        final Accountable value = notification.getValue();
        // 批量触发 key内部维护的监听器
        for (IndexFieldDataCache.Listener listener : key.listeners) {
            try {
                listener.onRemoval(
                    key.shardId, indexCache.fieldName,
                    notification.getRemovalReason() == RemovalNotification.RemovalReason.EVICTED, value.ramBytesUsed()
                );
            } catch (Exception e) {
                // load anyway since listeners should not throw exceptions
                logger.error("Failed to call listener on field data cache unloading", e);
            }
        }
    }

    /**
     * 负责计算权重值
     * 默认情况下 cache中每个kv 的权重值都是固定的
     * 当权重值超过了缓存的最大权重时 就会触发清理操作
     */
    public static class FieldDataWeigher implements ToLongBiFunction<Key, Accountable> {
        @Override
        public long applyAsLong(Key key, Accountable ramUsage) {
            // 每个缓存对象根据内存使用量来计算权重
            int weight = (int) Math.min(ramUsage.ramBytesUsed(), Integer.MAX_VALUE);
            return weight == 0 ? 1 : weight;
        }
    }

    /**
     * A specific cache instance for the relevant parameters of it (index, fieldNames, fieldType).
     * 针对field级别创建的缓存
     */
    static class IndexFieldCache implements IndexFieldDataCache, IndexReader.ClosedListener {
        private final Logger logger;
        final Index index;
        final String fieldName;

        /**
         * 基于lru算法实现的缓存
         * 每个indexFieldCache 对象的缓存 都是从 IndicesFieldDataCache中获取过来的
         */
        private final Cache<Key, Accountable> cache;

        /**
         * 生成缓存 以及移除缓存时的钩子
         * 对应 FieldDataCacheListener
         */
        private final Listener[] listeners;


        /**
         * @param logger
         * @param cache     外层针对所有index的缓存
         * @param index     对应index
         * @param fieldName 该缓存对应的field
         * @param listeners 这里包含 indicesFieldDataCache的默认监听器 以及由indexFieldDataCache构建field级别缓存时使用的监听器
         */
        IndexFieldCache(Logger logger, final Cache<Key, Accountable> cache, Index index, String fieldName, Listener... listeners) {
            this.logger = logger;
            this.listeners = listeners;
            this.index = index;
            this.fieldName = fieldName;
            this.cache = cache;
        }


        /**
         * 当通过IndexWarmer加载数据时 会转发到该方法
         * @param context  根据上下文信息 以及 IndexFieldData 可以获取LeafFieldData
         * @param indexFieldData
         * @param <FD>
         * @param <IFD>
         * @return
         * @throws Exception
         */
        @Override
        @SuppressWarnings("unchecked")
        public <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(final LeafReaderContext context,
                                                                                  final IFD indexFieldData) throws Exception {
            // 此时的reader对象一定是ES 封装过的 所以可以获取到分片id
            final ShardId shardId = ShardUtils.extractShardId(context.reader());
            final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
            if (cacheHelper == null) {
                throw new IllegalArgumentException("Reader " + context.reader() + " does not support caching");
            }
            // 生成缓存键
            final Key key = new Key(this, cacheHelper.getKey(), shardId);
            // 这里是加载某个field的数据 并存储到缓存中
            final Accountable accountable = cache.computeIfAbsent(key, k -> {
                cacheHelper.addClosedListener(IndexFieldCache.this);
                // 如果key发生了冲突 将listener 合并
                Collections.addAll(k.listeners, this.listeners);
                // 这里会执行lucene的标准操作 通过指定一个field 将包含该field的所有doc取出来  所有field的docValue 会合并成一个迭代器
                final LeafFieldData fieldData = indexFieldData.loadDirect(context);
                for (Listener listener : k.listeners) {
                    try {
                        listener.onCache(shardId, fieldName, fieldData);
                    } catch (Exception e) {
                        // load anyway since listeners should not throw exceptions
                        logger.error("Failed to call listener on atomic field data loading", e);
                    }
                }
                return fieldData;
            });
            return (FD) accountable;
        }

        /**
         * 都是通过 indexFieldData 从某个reader中加载某个field的数据 并存储在缓存中
         * Global 对应的是所有segment    DirectoryReader 就能获取某个目录下所有的reader对象
         *
         * @param indexReader
         * @param indexFieldData
         * @param <FD>
         * @param <IFD>
         * @return
         * @throws Exception
         */
        @Override
        @SuppressWarnings("unchecked")
        public <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(final DirectoryReader indexReader,
                                                                                          final IFD indexFieldData) throws Exception {
            final ShardId shardId = ShardUtils.extractShardId(indexReader);
            final IndexReader.CacheHelper cacheHelper = indexReader.getReaderCacheHelper();
            if (cacheHelper == null) {
                throw new IllegalArgumentException("Reader " + indexReader + " does not support caching");
            }
            final Key key = new Key(this, cacheHelper.getKey(), shardId);
            final Accountable accountable = cache.computeIfAbsent(key, k -> {
                // 将自身作为监听器 设置到 reader上 并且将内部的监听器转移到 key.listener上
                ElasticsearchDirectoryReader.addReaderCloseListener(indexReader, IndexFieldCache.this);
                Collections.addAll(k.listeners, this.listeners);
                // 获取到数据后 触发钩子
                final Accountable ifd = (Accountable) indexFieldData.localGlobalDirect(indexReader);
                for (Listener listener : k.listeners) {
                    try {
                        listener.onCache(shardId, fieldName, ifd);
                    } catch (Exception e) {
                        // load anyway since listeners should not throw exceptions
                        logger.error("Failed to call listener on global ordinals loading", e);
                    }
                }
                return ifd;
            });
            return (IFD) accountable;
        }

        /**
         * 当某个field相关的数据被移除时 清除相关的缓存   在cache对象中每个数据都是某个fieldData
         *
         * @param key
         */
        @Override
        public void onClose(CacheKey key) {
            cache.invalidate(new Key(this, key, null));
            // don't call cache.cleanUp here as it would have bad performance implications
        }

        @Override
        public void clear() {
            for (Key key : cache.keys()) {
                // 找到外层 存储所有fieldData中 匹配的数据 进行清理
                if (key.indexCache.index.equals(index)) {
                    cache.invalidate(key);
                }
            }
            // force eviction
            // 清除过期数据
            cache.refresh();
        }

        @Override
        public void clear(String fieldName) {
            for (Key key : cache.keys()) {
                if (key.indexCache.index.equals(index)) {
                    if (key.indexCache.fieldName.equals(fieldName)) {
                        cache.invalidate(key);
                    }
                }
            }
            // we call refresh because this is a manual operation, should happen
            // rarely and probably means the user wants to see memory returned as
            // soon as possible
            cache.refresh();
        }
    }

    /**
     * 该缓存对象内存储的是另一个缓存 (IndexFieldCache)
     */
    public static class Key {

        /**
         * 针对field级别的缓存对象
         */
        public final IndexFieldCache indexCache;
        public final IndexReader.CacheKey readerKey;
        /**
         * 分片id
         */
        public final ShardId shardId;

        public final List<IndexFieldDataCache.Listener> listeners = new ArrayList<>();

        Key(IndexFieldCache indexCache, IndexReader.CacheKey readerKey, @Nullable ShardId shardId) {
            this.indexCache = indexCache;
            this.readerKey = readerKey;
            this.shardId = shardId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            if (!indexCache.equals(key.indexCache)) return false;
            if (!readerKey.equals(key.readerKey)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = indexCache.hashCode();
            result = 31 * result + readerKey.hashCode();
            return result;
        }
    }


}
