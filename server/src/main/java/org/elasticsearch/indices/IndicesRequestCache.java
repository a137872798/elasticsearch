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

package org.elasticsearch.indices;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * The indices request cache allows to cache a shard level request stage responses, helping with improving
 * similar requests that are potentially expensive (because of aggs for example). The cache is fully coherent
 * with the semantics of NRT (the index reader cache key is part of the cache key), and relies on size based
 * eviction to evict old reader associated cache entries as well as scheduler reaper to clean readers that
 * are no longer used or closed shards.
 * <p>
 * Currently, the cache is only enabled for count requests, and can only be opted in on an index
 * level setting that can be dynamically changed and defaults to false.
 * <p>
 * There are still several TODOs left in this class, some easily addressable, some more complex, but the support
 * is functional.
 * 该对象本身还实现了  RemovalListener 当缓存内的数据被移除时 触发监听器
 */
public final class IndicesRequestCache implements RemovalListener<IndicesRequestCache.Key, BytesReference>, Closeable {

    private static final Logger logger = LogManager.getLogger(IndicesRequestCache.class);

    /**
     * A setting to enable or disable request caching on an index level. Its dynamic by default
     * since we are checking on the cluster state IndexMetadata always.
     */
    public static final Setting<Boolean> INDEX_CACHE_REQUEST_ENABLED_SETTING =
        Setting.boolSetting("index.requests.cache.enable", true, Property.Dynamic, Property.IndexScope);
    public static final Setting<ByteSizeValue> INDICES_CACHE_QUERY_SIZE =
        Setting.memorySizeSetting("indices.requests.cache.size", "1%", Property.NodeScope);
    public static final Setting<TimeValue> INDICES_CACHE_QUERY_EXPIRE =
        Setting.positiveTimeSetting("indices.requests.cache.expire", new TimeValue(0), Property.NodeScope);

    /**
     * 每当往缓存中存入一个新数据并成功时  会生成一个cleanupKey对象 并设置到该容器中  并且该cleanupKey 是设置了 readerCacheKey
     */
    private final ConcurrentMap<CleanupKey, Boolean> registeredClosedListeners = ConcurrentCollections.newConcurrentMap();

    /**
     * 因为reader被关闭会将一些cleanupKey 从registeredClosedListeners 移动到该容器中
     */
    private final Set<CleanupKey> keysToClean = ConcurrentCollections.newConcurrentSet();
    private final ByteSizeValue size;

    /**
     * 缓存过期时间
     */
    private final TimeValue expire;

    /**
     * key 对应缓存键 维护一些重要信息  value则是数据流
     */
    private final Cache<Key, BytesReference> cache;

    /**
     * 通过settings的缓存配置初始化缓存
     *
     * @param settings
     */
    IndicesRequestCache(Settings settings) {
        // 指的是缓存大小占jvm.getHeapMax的比率
        this.size = INDICES_CACHE_QUERY_SIZE.get(settings);
        // 如果未设置缓存超时时间 代表缓存数据不会过期
        this.expire = INDICES_CACHE_QUERY_EXPIRE.exists(settings) ? INDICES_CACHE_QUERY_EXPIRE.get(settings) : null;

        // 获取缓存大小
        long sizeInBytes = size.getBytes();
        CacheBuilder<Key, BytesReference> cacheBuilder = CacheBuilder.<Key, BytesReference>builder()
            // 每个Cache对象 仅允许设置一个removalListener
            .setMaximumWeight(sizeInBytes).weigher((k, v) -> k.ramBytesUsed() + v.ramBytesUsed()).removalListener(this);
        if (expire != null) {
            // cache 本身使用lru算法 也就是当某个元素长时间没有被访问时 会从lru链表中移除 同时lru链表的清理 在被动情况下必须等待weight达到maxWeight 才会触发
            cacheBuilder.setExpireAfterAccess(expire);
        }
        cache = cacheBuilder.build();
    }

    @Override
    public void close() {
        // 当本对象被关闭时 会清空缓存
        cache.invalidateAll();
    }

    /**
     * @param entity
     */
    void clear(CacheEntity entity) {
        // 先将清理数据加入到keysToClean
        keysToClean.add(new CleanupKey(entity, null));
        // 在cache中清理匹配的数据
        cleanCache();
    }

    /**
     * 每个缓存数据被移除时触发的逻辑就是由 cacheEntry决定的
     *
     * @param notification
     */
    @Override
    public void onRemoval(RemovalNotification<Key, BytesReference> notification) {
        notification.getKey().entity.onRemoval(notification);
    }

    // NORELEASE The cacheKeyRenderer has been added in order to debug
    // https://github.com/elastic/elasticsearch/issues/32827, it should be
    // removed when this issue is solved
    // 从缓存中获取数据 如果不存在则使用函数生成数据
    BytesReference getOrCompute(CacheEntity cacheEntity, CheckedSupplier<BytesReference, IOException> loader,
                                DirectoryReader reader, BytesReference cacheKey, Supplier<String> cacheKeyRenderer) throws Exception {
        assert reader.getReaderCacheHelper() != null;
        final Key key = new Key(cacheEntity, reader.getReaderCacheHelper().getKey(), cacheKey);

        // 将外部传入的 key->value 转换函数包装成loader (做了一层适配 因为cache只能使用CacheLoader)
        Loader cacheLoader = new Loader(cacheEntity, loader);
        // 将数据插入到缓存中
        BytesReference value = cache.computeIfAbsent(key, cacheLoader);
        // 代表插入成功了 所以调用了loader 读取数据
        if (cacheLoader.isLoaded()) {
            // 插入成功返回来也意味着 缓存未命中
            key.entity.onMiss();
            if (logger.isTraceEnabled()) {
                logger.trace("Cache miss for reader version [{}], max_doc[{}] and request:\n {}",
                    reader.getVersion(), reader.maxDoc(), cacheKeyRenderer.get());
            }
            // see if its the first time we see this reader, and make sure to register a cleanup key
            // 这里又生成了一个对应的 清除键  并存入到registeredClosedListeners
            CleanupKey cleanupKey = new CleanupKey(cacheEntity, reader.getReaderCacheHelper().getKey());
            if (!registeredClosedListeners.containsKey(cleanupKey)) {
                Boolean previous = registeredClosedListeners.putIfAbsent(cleanupKey, Boolean.TRUE);
                if (previous == null) {
                    // 一般来说不会出现重复插入 在插入成功时 还会将cleanupKey 设置到reader上 作为close监听器
                    // 在reader被关闭的时候会触发 onClose钩子  并将cleanupKey 从registeredClosedListeners 移动到keysToClean
                    ElasticsearchDirectoryReader.addReaderCloseListener(reader, cleanupKey);
                }
            }
        } else {
            // 当命中时 只是简单打印日志
            key.entity.onHit();
            if (logger.isTraceEnabled()) {
                logger.trace("Cache hit for reader version [{}], max_doc[{}] and request:\n {}",
                    reader.getVersion(), reader.maxDoc(), cacheKeyRenderer.get());
            }
        }
        return value;
    }

    /**
     * Invalidates the given the cache entry for the given key and it's context
     *
     * @param cacheEntity the cache entity to invalidate for
     * @param reader      the reader to invalidate the cache entry for
     * @param cacheKey    the cache key to invalidate
     *                    将某个缓存标记成无效
     */
    void invalidate(CacheEntity cacheEntity, DirectoryReader reader, BytesReference cacheKey) {
        assert reader.getReaderCacheHelper() != null;
        cache.invalidate(new Key(cacheEntity, reader.getReaderCacheHelper().getKey(), cacheKey));
    }

    /**
     * 该类定义了如何通过key 获取到value
     */
    private static class Loader implements CacheLoader<Key, BytesReference> {

        private final CacheEntity entity;

        /**
         * 实际上是转发到这个函数
         */
        private final CheckedSupplier<BytesReference, IOException> loader;
        /**
         * 代表已经加载过数据了
         */
        private boolean loaded;

        Loader(CacheEntity entity, CheckedSupplier<BytesReference, IOException> loader) {
            this.entity = entity;
            this.loader = loader;
        }

        public boolean isLoaded() {
            return this.loaded;
        }

        @Override
        public BytesReference load(Key key) throws Exception {
            BytesReference value = loader.get();
            // 调用该方法时 代表已经准备将数据存储到缓存中了  所以触发 onCached
            entity.onCached(key, value);
            loaded = true;
            return value;
        }
    }

    /**
     * Basic interface to make this cache testable.
     * 该对象还定义了 关联的缓存Key 被移除时该如何处理
     */
    interface CacheEntity extends Accountable {

        /**
         * Called after the value was loaded.
         * 当键值对即将存储到缓存时触发
         */
        void onCached(Key key, BytesReference value);

        /**
         * Returns <code>true</code> iff the resource behind this entity is still open ie.
         * entities associated with it can remain in the cache. ie. IndexShard is still open.
         * 这个数据体此时是否处于打开状态
         */
        boolean isOpen();

        /**
         * Returns the cache identity. this is, similar to {@link #isOpen()} the resource identity behind this cache entity.
         * For instance IndexShard is the identity while a CacheEntity is per DirectoryReader. Yet, we group by IndexShard instance.
         */
        Object getCacheIdentity();

        /**
         * Called each time this entity has a cache hit.
         * 缓存命中时触发
         */
        void onHit();

        /**
         * Called each time this entity has a cache miss.
         * 每当缓存未命中时 触发该方法  这个方法怎么触发多次 只要发现未命中不就加入到缓存中了么 ???
         */
        void onMiss();

        /**
         * Called when this entity instance is removed
         */
        void onRemoval(RemovalNotification<Key, BytesReference> notification);
    }

    /**
     * 该cache内部的key 都是 Key
     */
    static class Key implements Accountable {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Key.class);

        /**
         * 该对象定义了缓存失效时的处理逻辑
         */
        public final CacheEntity entity; // use as identity equality
        /**
         * 这是lucene的缓存键 这个对象之前就没怎么看过 只是一个空对象
         */
        public final IndexReader.CacheKey readerCacheKey;
        /**
         * 对应的数据流
         */
        public final BytesReference value;

        Key(CacheEntity entity, IndexReader.CacheKey readerCacheKey, BytesReference value) {
            this.entity = entity;
            this.readerCacheKey = Objects.requireNonNull(readerCacheKey);
            this.value = value;
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + entity.ramBytesUsed() + value.length();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            // TODO: more detailed ram usage?
            return Collections.emptyList();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            if (Objects.equals(readerCacheKey, key.readerCacheKey) == false) return false;
            if (!entity.getCacheIdentity().equals(key.entity.getCacheIdentity())) return false;
            if (!value.equals(key.value)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = entity.getCacheIdentity().hashCode();
            result = 31 * result + readerCacheKey.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }
    }

    /**
     * 当lucene的缓存被清理时 触发该对象
     */
    private class CleanupKey implements IndexReader.ClosedListener {
        final CacheEntity entity;
        final IndexReader.CacheKey readerCacheKey;

        private CleanupKey(CacheEntity entity, IndexReader.CacheKey readerCacheKey) {
            this.entity = entity;
            this.readerCacheKey = readerCacheKey;
        }

        @Override
        public void onClose(IndexReader.CacheKey cacheKey) {
            Boolean remove = registeredClosedListeners.remove(this);
            if (remove != null) {
                keysToClean.add(this);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CleanupKey that = (CleanupKey) o;
            if (Objects.equals(readerCacheKey, that.readerCacheKey) == false) return false;
            if (!entity.getCacheIdentity().equals(that.entity.getCacheIdentity())) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = entity.getCacheIdentity().hashCode();
            result = 31 * result + Objects.hashCode(readerCacheKey);
            return result;
        }
    }


    /**
     * 处理 keysToClean 内部的数据
     */
    synchronized void cleanCache() {
        final ObjectSet<CleanupKey> currentKeysToClean = new ObjectHashSet<>();
        final ObjectSet<Object> currentFullClean = new ObjectHashSet<>();
        currentKeysToClean.clear();
        currentFullClean.clear();
        for (Iterator<CleanupKey> iterator = keysToClean.iterator(); iterator.hasNext(); ) {
            CleanupKey cleanupKey = iterator.next();
            iterator.remove();
            if (cleanupKey.readerCacheKey == null || cleanupKey.entity.isOpen() == false) {
                // null indicates full cleanup, as does a closed shard
                // null 代表着全部清理  或者此时entity处于未打开
                currentFullClean.add(cleanupKey.entity.getCacheIdentity());
            } else {
                // 这里的只按照 readerCacheKey 进行清理
                currentKeysToClean.add(cleanupKey);
            }
        }

        // 某些满足条件的key 会直接从lru链表中移除
        if (!currentKeysToClean.isEmpty() || !currentFullClean.isEmpty()) {
            for (Iterator<Key> iterator = cache.keys().iterator(); iterator.hasNext(); ) {
                Key key = iterator.next();
                // 这里是匹配 CacheIdentity 下面是匹配 entity和 readerCacheKey
                if (currentFullClean.contains(key.entity.getCacheIdentity())) {
                    iterator.remove();
                } else {
                    if (currentKeysToClean.contains(new CleanupKey(key.entity, key.readerCacheKey))) {
                        iterator.remove();
                    }
                }
            }
        }

        // 检测是否有长时间未使用的 也进行清理
        cache.refresh();
    }


    /**
     * Returns the current size of the cache
     */
    int count() {
        return cache.count();
    }

    int numRegisteredCloseListeners() { // for testing
        return registeredClosedListeners.size();
    }
}
