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

package org.elasticsearch.common.cache;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.ToLongBiFunction;

/**
 * A simple concurrent cache.
 * <p>
 * Cache is a simple concurrent cache that supports time-based and weight-based evictions, with notifications for all
 * evictions. The design goals for this cache were simplicity and read performance. This means that we are willing to
 * accept reduced write performance in exchange for easy-to-understand code. Cache statistics for hits, misses and
 * evictions are exposed.
 * <p>
 * The design of the cache is relatively simple. The cache is segmented into 256 segments which are backed by HashMaps.
 * Each segment is protected by a re-entrant read/write lock. The read/write locks permit multiple concurrent readers
 * without contention, and the segments gives us write throughput without impacting readers (so readers are blocked only
 * if they are reading a segment that a writer is writing to).
 * <p>
 * The LRU functionality is backed by a single doubly-linked list chaining the entries in order of insertion. This
 * LRU list is protected by a lock that serializes all writes to it. There are opportunities for improvements
 * here if write throughput is a concern.
 * <ol>
 * <li>LRU list mutations could be inserted into a blocking queue that a single thread is reading from
 * and applying to the LRU list.</li>
 * <li>Promotions could be deferred for entries that were "recently" promoted.</li>
 * <li>Locks on the list could be taken per node being modified instead of globally.</li>
 * </ol>
 * <p>
 * Evictions only occur after a mutation to the cache (meaning an entry promotion, a cache insertion, or a manual
 * invalidation) or an explicit call to {@link #refresh()}.
 *
 * @param <K> The type of the keys
 * @param <V> The type of the values
 *           ES封装的缓存对象
 */
public class Cache<K, V> {

    // positive if entries have an expiration
    private long expireAfterAccessNanos = -1;

    // true if entries can expire after access
    private boolean entriesExpireAfterAccess;

    // positive if entries have an expiration after write
    private long expireAfterWriteNanos = -1;

    // true if entries can expire after initial insertion
    private boolean entriesExpireAfterWrite;

    // the number of entries in the cache
    // 记录整个cache中有多少元素
    private int count = 0;

    // the weight of the entries in the cache
    private long weight = 0;

    // the maximum weight that this cache supports
    private long maximumWeight = -1;

    // the weigher of entries
    // 该对象可以为不同的键值对设置权重值  每当插入一个新的键值对时 会通过该映射函数转换成权重值累加到 weight变量上
    private ToLongBiFunction<K, V> weigher = (k, v) -> 1;

    // the removal callback
    private RemovalListener<K, V> removalListener = notification -> {
    };

    // use CacheBuilder to construct
    Cache() {
    }

    void setExpireAfterAccessNanos(long expireAfterAccessNanos) {
        if (expireAfterAccessNanos <= 0) {
            throw new IllegalArgumentException("expireAfterAccessNanos <= 0");
        }
        this.expireAfterAccessNanos = expireAfterAccessNanos;
        this.entriesExpireAfterAccess = true;
    }

    // pkg-private for testing
    long getExpireAfterAccessNanos() {
        return this.expireAfterAccessNanos;
    }

    void setExpireAfterWriteNanos(long expireAfterWriteNanos) {
        if (expireAfterWriteNanos <= 0) {
            throw new IllegalArgumentException("expireAfterWriteNanos <= 0");
        }
        this.expireAfterWriteNanos = expireAfterWriteNanos;
        this.entriesExpireAfterWrite = true;
    }

    // pkg-private for testing
    long getExpireAfterWriteNanos() {
        return this.expireAfterWriteNanos;
    }

    void setMaximumWeight(long maximumWeight) {
        if (maximumWeight < 0) {
            throw new IllegalArgumentException("maximumWeight < 0");
        }
        this.maximumWeight = maximumWeight;
    }

    void setWeigher(ToLongBiFunction<K, V> weigher) {
        Objects.requireNonNull(weigher);
        this.weigher = weigher;
    }

    void setRemovalListener(RemovalListener<K, V> removalListener) {
        Objects.requireNonNull(removalListener);
        this.removalListener = removalListener;
    }

    /**
     * The relative time used to track time-based evictions.
     *
     * @return the current relative time
     * 在插入新的键值对时 如果没有手动指定时间 会通过该函数获取当前时间
     */
    protected long now() {
        // System.nanoTime takes non-negligible time, so we only use it if we need it
        // use System.nanoTime because we want relative time, not absolute time
        // 只有设置了过期属性 才会需要时间参数
        return entriesExpireAfterAccess || entriesExpireAfterWrite ? System.nanoTime() : 0;
    }

    // the state of an entry in the LRU list
    enum State {
        /**
         * 代表一个新建的 Entry 此时还不存在于 lru链表中
         */
        NEW,
        /**
         * entry 已经设置到lru中
         */
        EXISTING,
        /**
         * entry从设置到lru 又到被移除
         */
        DELETED
    }

    /**
     * 存入到cache的每组键值对 都会被包装成entry对象
     * @param <K>
     * @param <V>
     */
    static class Entry<K, V> {
        final K key;
        final V value;
        long writeTime;

        /**
         * lru 列表就是根据最后的访问时间来清理缓存的  所以每次get都会更新这个时间  而如果不需要过期功能的话 writeTime 和 accessTime就可以为0
         */
        volatile long accessTime;

        // 这2个指针是用于实现lru链表的
        Entry<K, V> before;
        Entry<K, V> after;

        // 插入一个entry时 默认为 NEW
        State state = State.NEW;

        Entry(K key, V value, long writeTime) {
            this.key = key;
            this.value = value;

            // 当缓存没有指定过期属性时 传入值为0
            this.writeTime = this.accessTime = writeTime;
        }
    }

    /**
     * A cache segment.
     * <p>
     * A CacheSegment is backed by a HashMap and is protected by a read/write lock.
     *
     * @param <K> the type of the keys
     * @param <V> the type of the values
     *           Cache 本身是拆分成多个片段的
     */
    private static class CacheSegment<K, V> {
        // read/write lock protecting mutations to the segment
        ReadWriteLock segmentLock = new ReentrantReadWriteLock();

        // 分别对读写锁做了一层包装 还是原对象
        ReleasableLock readLock = new ReleasableLock(segmentLock.readLock());
        ReleasableLock writeLock = new ReleasableLock(segmentLock.writeLock());

        /**
         * 每个段内部使用普通的 hashMap 配合读写锁提升性能
         */
        Map<K, CompletableFuture<Entry<K, V>>> map = new HashMap<>();

        SegmentStats segmentStats = new SegmentStats();

        /**
         * get an entry from the segment; expired entries will be returned as null but not removed from the cache until the LRU list is
         * pruned or a manual {@link Cache#refresh()} is performed however a caller can take action using the provided callback
         *
         * @param key       the key of the entry to get from the cache
         * @param now       the access time of this entry
         * @param isExpired test if the entry is expired   检测entry是否过期
         * @param onExpiration a callback if the entry associated to the key is expired  处理过期数据的函数
         * @return the entry if there was one, otherwise null
         * 从某个segment中找到目标数据
         */
        Entry<K, V> get(K key, long now, Predicate<Entry<K, V>> isExpired, Consumer<Entry<K, V>> onExpiration) {
            CompletableFuture<Entry<K, V>> future;
            // 因为插入时hashMap中的链表没有线程安全 可能有一个临界点 既读取不到旧数据 也读取不到新数据 所以这里使用读写锁做互斥
            try (ReleasableLock ignored = readLock.acquire()) {
                future = map.get(key);
            }
            if (future != null) {
                Entry<K, V> entry;
                try {
                    // entry 被future包裹了一层
                    entry = future.get();
                } catch (ExecutionException e) {
                    assert future.isCompletedExceptionally();
                    segmentStats.miss();
                    return null;
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                // 如果已过期 用相关函数处理 并返回null
                if (isExpired.test(entry)) {
                    segmentStats.miss();
                    onExpiration.accept(entry);
                    return null;
                } else {
                    segmentStats.hit();
                    // 更新最后访问时间   这样就不会在lru列表中被删除了
                    entry.accessTime = now;
                    return entry;
                }
            } else {
                segmentStats.miss();
                return null;
            }
        }

        /**
         * put an entry into the segment
         *
         * @param key   the key of the entry to add to the cache
         * @param value the value of the entry to add to the cache
         * @param now   the access time of this entry
         * @return a tuple of the new entry and the existing entry, if there was one otherwise null
         * 将一组键值对插入到缓存segment中
         */
        Tuple<Entry<K, V>, Entry<K, V>> put(K key, V value, long now) {
            // 将相关数据封装成一个 entry对象
            Entry<K, V> entry = new Entry<>(key, value, now);
            Entry<K, V> existing = null;
            // 是为了避免相同的key 覆盖了之前的值么 所以需要用读写锁做互斥
            try (ReleasableLock ignored = writeLock.acquire()) {
                try {
                    // 直接往future对象中插入了结果
                    CompletableFuture<Entry<K, V>> future = map.put(key, CompletableFuture.completedFuture(entry));
                    // 如果替换了一个旧值 将旧值取出来 并包装成一个 tuple对象
                    if (future != null) {
                        existing = future.handle((ok, ex) -> {
                            if (ok != null) {
                                return ok;
                            } else {
                                return null;
                            }
                        }).get();
                    }
                } catch (ExecutionException | InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            return Tuple.tuple(entry, existing);
        }

        /**
         * remove an entry from the segment
         *
         * @param key       the key of the entry to remove from the cache
         * @param onRemoval a callback for the removed entry
         *                  按照key 将匹配的entry移除
         */
        void remove(K key, Consumer<CompletableFuture<Entry<K, V>>> onRemoval) {
            CompletableFuture<Entry<K, V>> future;
            try (ReleasableLock ignored = writeLock.acquire()) {
                future = map.remove(key);
            }
            if (future != null) {
                segmentStats.eviction();
                onRemoval.accept(future);
            }
        }

        /**
         * remove an entry from the segment iff the future is done and the value is equal to the
         * expected value
         *
         * @param key the key of the entry to remove from the cache
         * @param value the value expected to be associated with the key
         * @param onRemoval a callback for the removed entry
         *                  从map中精确的移除掉某组键值对
         */
        void remove(K key, V value, Consumer<CompletableFuture<Entry<K, V>>> onRemoval) {
            CompletableFuture<Entry<K, V>> future;
            boolean removed = false;
            try (ReleasableLock ignored = writeLock.acquire()) {
                future = map.get(key);
                try {
                    if (future != null) {
                        if (future.isDone()) {
                            Entry<K, V> entry = future.get();
                            // 只有value也相同时 才会移除对象
                            if (Objects.equals(value, entry.value)) {
                                removed = map.remove(key, future);
                            }
                        }
                    }
                } catch (ExecutionException | InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }

            // 当成功移除掉entry后 触发相关钩子 默认情况下钩子是 NOOP
            if (future != null && removed) {
                segmentStats.eviction();
                onRemoval.accept(future);
            }
        }

        /**
         * 缓存本身被拆分成多个segment 每个segment都会统计缓存命中次数 丢失次数  并且使用LongAdder这种分段锁
         */
        private static class SegmentStats {
            private final LongAdder hits = new LongAdder();
            private final LongAdder misses = new LongAdder();

            /**
             * 记录map中移除了多少元素
             */
            private final LongAdder evictions = new LongAdder();

            void hit() {
                hits.increment();
            }

            void miss() {
                misses.increment();
            }

            void eviction() {
                evictions.increment();
            }
        }
    }

    public static final int NUMBER_OF_SEGMENTS = 256;

    /**
     * 外层通过hash桶离散冲突  单个segment又有严格的并发控制
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private final CacheSegment<K, V>[] segments = new CacheSegment[NUMBER_OF_SEGMENTS];

    {
        for (int i = 0; i < segments.length; i++) {
            segments[i] = new CacheSegment<>();
        }
    }

    /**
     * 对应lru链表的头尾节点
     */
    Entry<K, V> head;
    Entry<K, V> tail;

    // lock protecting mutations to the LRU list
    private final ReleasableLock lruLock = new ReleasableLock(new ReentrantLock());

    /**
     * Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or null if this map contains no mapping for the key
     */
    public V get(K key) {
        return get(key, now(), e -> {});
    }

    /**
     * 在读取缓存数据时 还会传入时间参数 以检测数据是否过期
     * @param key
     * @param now
     * @param onExpiration
     * @return
     */
    private V get(K key, long now, Consumer<Entry<K, V>> onExpiration) {
        // 通过hash函数找到目标segment
        CacheSegment<K, V> segment = getCacheSegment(key);
        // e -> isExpired(e, now) 检测某个entry是否已经过期 如果过期交给 onExpiration处理
        Entry<K, V> entry = segment.get(key, now, e -> isExpired(e, now), onExpiration);
        if (entry == null) {
            return null;
        } else {
            // 返回值不为null 代表没有过期 这里将元素重新设置到lru链表头部
            promote(entry, now);
            return entry.value;
        }
    }

    /**
     * If the specified key is not already associated with a value (or is mapped to null), attempts to compute its
     * value using the given mapping function and enters it into this map unless null. The load method for a given key
     * will be invoked at most once.
     *
     * Use of different {@link CacheLoader} implementations on the same key concurrently may result in only the first
     * loader function being called and the second will be returned the result provided by the first including any exceptions
     * thrown during the execution of the first.
     *
     * @param key    the key whose associated value is to be returned or computed for if non-existent
     * @param loader the function to compute a value given a key
     * @return the current (existing or computed) non-null value associated with the specified key
     * @throws ExecutionException thrown if loader throws an exception or returns a null value
     * 非替换性插入
     */
    public V computeIfAbsent(K key, CacheLoader<K, V> loader) throws ExecutionException {
        long now = now();
        // we have to eagerly evict expired entries or our putIfAbsent call below will fail
        // 原本在 get中如果某个entry已经过期了不做任何处理 暂时存储在lru链表中  但是在这种场景下 过期了就要被删除 确保本次能正常插入
        V value = get(key, now, e -> {
            try (ReleasableLock ignored = lruLock.acquire()) {
                evictEntry(e);
            }
        });
        // 允许插入新数据
        if (value == null) {
            // we need to synchronize loading of a value for a given key; however, holding the segment lock while
            // invoking load can lead to deadlock against another thread due to dependent key loading; therefore, we
            // need a mechanism to ensure that load is invoked at most once, but we are not invoking load while holding
            // the segment lock; to do this, we atomically put a future in the map that can load the value, and then
            // get the value from this future on the thread that won the race to place the future into the segment map
            CacheSegment<K, V> segment = getCacheSegment(key);
            CompletableFuture<Entry<K, V>> future;
            CompletableFuture<Entry<K, V>> completableFuture = new CompletableFuture<>();

            try (ReleasableLock ignored = segment.writeLock.acquire()) {
                future = segment.map.putIfAbsent(key, completableFuture);
            }

            // 将结果填充到future后会触发该函数
            BiFunction<? super Entry<K, V>, Throwable, ? extends V> handler = (ok, ex) -> {
                if (ok != null) {
                    try (ReleasableLock ignored = lruLock.acquire()) {
                        // 当正常处理时 将entry插入到lru链表中
                        promote(ok, now);
                    }
                    return ok.value;
                } else {
                    // 代表中途发生了异常 将该key 从map中移除
                    try (ReleasableLock ignored = segment.writeLock.acquire()) {
                        CompletableFuture<Entry<K, V>> sanity = segment.map.get(key);
                        if (sanity != null && sanity.isCompletedExceptionally()) {
                            segment.map.remove(key);
                        }
                    }
                    return null;
                }
            };

            CompletableFuture<V> completableValue;
            if (future == null) {
                future = completableFuture;
                // 设置当future完成时的后置逻辑
                completableValue = future.handle(handler);
                V loaded;
                try {
                    // 根据key 从某个地方获取到了value
                    loaded = loader.load(key);
                } catch (Exception e) {
                    future.completeExceptionally(e);
                    throw new ExecutionException(e);
                }
                if (loaded == null) {
                    NullPointerException npe = new NullPointerException("loader returned a null value");
                    future.completeExceptionally(npe);
                    throw new ExecutionException(npe);
                } else {
                    // 将结果填充到future中 并触发handler
                    future.complete(new Entry<>(key, loaded, now));
                }
            } else {
                completableValue = future.handle(handler);
            }

            try {
                value = completableValue.get();
                // check to ensure the future hasn't been completed with an exception
                if (future.isCompletedExceptionally()) {
                    future.get(); // call get to force the exception to be thrown for other concurrent callers
                    throw new IllegalStateException("the future was completed exceptionally but no exception was thrown");
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
        return value;
    }

    /**
     * Associates the specified value with the specified key in this map. If the map previously contained a mapping for
     * the key, the old value is replaced.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     *              往缓存中插入一组键值对
     */
    public void put(K key, V value) {
        // 默认会将当前时间戳传入
        long now = now();
        put(key, value, now);
    }

    private void put(K key, V value, long now) {
        // 首先将key 通过hash函数映射到某个segment
        CacheSegment<K, V> segment = getCacheSegment(key);
        // t1 代表本次插入的新数据  t2 代表被替换的旧数据
        // 如果直接用一个简单的链表来实现lru功能 那么效率会很低 这里先通过hash桶实现快速查找  当出现冲突时 将旧数据存储到lru中
        // 因为在缓存概念中这样不应该进行替换  新旧数据需要同时保留
        Tuple<Entry<K, V>, Entry<K, V>> tuple = segment.put(key, value, now);
        boolean replaced = false;
        try (ReleasableLock ignored = lruLock.acquire()) {
            // 旧的数据存在于lru链表中 因为本次插入了新的数据 所以之前的数据会被替换掉 这时也要从lru链表中移除
            if (tuple.v2() != null && tuple.v2().state == State.EXISTING) {
                if (unlink(tuple.v2())) {
                    replaced = true;
                }
            }
            // 首次插入的新数据会通过该函数设置到lru链表中
            promote(tuple.v1(), now);
        }
        // 代表缓存数据是由于替换发生的删除
        if (replaced) {
            removalListener.onRemoval(new RemovalNotification<>(tuple.v2().key, tuple.v2().value,
                RemovalNotification.RemovalReason.REPLACED));
        }
    }

    /**
     * 代表某个entry 已经无效了 需要从lru中移除 以及触发监听器
     */
    private final Consumer<CompletableFuture<Entry<K, V>>> invalidationConsumer = f -> {
        try {
            Entry<K, V> entry = f.get();
            try (ReleasableLock ignored = lruLock.acquire()) {
                delete(entry, RemovalNotification.RemovalReason.INVALIDATED);
            }
        } catch (ExecutionException e) {
            // ok
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    };

    /**
     * Invalidate the association for the specified key. A removal notification will be issued for invalidated
     * entries with {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} INVALIDATED.
     *
     * @param key the key whose mapping is to be invalidated from the cache
     *            将某个key 对应的值移除
     */
    public void invalidate(K key) {
        CacheSegment<K, V> segment = getCacheSegment(key);
        segment.remove(key, invalidationConsumer);
    }

    /**
     * Invalidate the entry for the specified key and value. If the value provided is not equal to the value in
     * the cache, no removal will occur. A removal notification will be issued for invalidated
     * entries with {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} INVALIDATED.
     *
     * @param key the key whose mapping is to be invalidated from the cache
     * @param value the expected value that should be associated with the key
     */
    public void invalidate(K key, V value) {
        CacheSegment<K, V> segment = getCacheSegment(key);
        segment.remove(key, value, invalidationConsumer);
    }

    /**
     * Invalidate all cache entries. A removal notification will be issued for invalidated entries with
     * {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} INVALIDATED.
     * 将缓存中所有元素都标记成失效
     */
    public void invalidateAll() {
        Entry<K, V> h;

        boolean[] haveSegmentLock = new boolean[NUMBER_OF_SEGMENTS];
        try {
            // 先对所有segment上锁 避免又插入新的元素
            for (int i = 0; i < NUMBER_OF_SEGMENTS; i++) {
                segments[i].segmentLock.writeLock().lock();
                haveSegmentLock[i] = true;
            }
            try (ReleasableLock ignored = lruLock.acquire()) {
                h = head;
                Arrays.stream(segments).forEach(segment -> segment.map = new HashMap<>());
                Entry<K, V> current = head;
                while (current != null) {
                    current.state = State.DELETED;
                    current = current.after;
                }
                head = tail = null;
                count = 0;
                weight = 0;
            }
        } finally {
            for (int i = NUMBER_OF_SEGMENTS - 1; i >= 0; i--) {
                if (haveSegmentLock[i]) {
                    segments[i].segmentLock.writeLock().unlock();
                }
            }
        }
        // 在锁外执行监听器逻辑 避免长时间占用锁
        while (h != null) {
            removalListener.onRemoval(new RemovalNotification<>(h.key, h.value, RemovalNotification.RemovalReason.INVALIDATED));
            h = h.after;
        }
    }

    /**
     * Force any outstanding size-based and time-based evictions to occur
     * 强制刷新  将lru中此时已经过期的数据清理掉  原本是惰性清理
     */
    public void refresh() {
        long now = now();
        try (ReleasableLock ignored = lruLock.acquire()) {
            evict(now);
        }
    }

    /**
     * The number of entries in the cache.
     *
     * @return the number of entries in the cache
     */
    public int count() {
        return count;
    }

    /**
     * The weight of the entries in the cache.
     *
     * @return the weight of the entries in the cache
     */
    public long weight() {
        return weight;
    }

    /**
     * An LRU sequencing of the keys in the cache that supports removal. This sequence is not protected from mutations
     * to the cache (except for {@link Iterator#remove()}. The result of iteration under any other mutation is
     * undefined.
     *
     * @return an LRU-ordered {@link Iterable} over the keys in the cache
     */
    public Iterable<K> keys() {
        return () -> new Iterator<K>() {
            private CacheIterator iterator = new CacheIterator(head);

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public K next() {
                return iterator.next().key;
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    /**
     * An LRU sequencing of the values in the cache. This sequence is not protected from mutations
     * to the cache (except for {@link Iterator#remove()}. The result of iteration under any other mutation is
     * undefined.
     *
     * @return an LRU-ordered {@link Iterable} over the values in the cache
     */
    public Iterable<V> values() {
        return () -> new Iterator<V>() {
            private CacheIterator iterator = new CacheIterator(head);

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public V next() {
                return iterator.next().value;
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    private class CacheIterator implements Iterator<Entry<K, V>> {
        private Entry<K, V> current;
        private Entry<K, V> next;

        CacheIterator(Entry<K, V> head) {
            current = null;
            next = head;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Entry<K, V> next() {
            current = next;
            next = next.after;
            return current;
        }

        @Override
        public void remove() {
            Entry<K, V> entry = current;
            if (entry != null) {
                CacheSegment<K, V> segment = getCacheSegment(entry.key);
                segment.remove(entry.key, entry.value, f -> {});
                try (ReleasableLock ignored = lruLock.acquire()) {
                    current = null;
                    delete(entry, RemovalNotification.RemovalReason.INVALIDATED);
                }
            }
        }
    }

    /**
     * The cache statistics tracking hits, misses and evictions. These are taken on a best-effort basis meaning that
     * they could be out-of-date mid-flight.
     *
     * @return the current cache statistics
     */
    public CacheStats stats() {
        long hits = 0;
        long misses = 0;
        long evictions = 0;
        for (int i = 0; i < segments.length; i++) {
            hits += segments[i].segmentStats.hits.longValue();
            misses += segments[i].segmentStats.misses.longValue();
            evictions += segments[i].segmentStats.evictions.longValue();
        }
        return new CacheStats(hits, misses, evictions);
    }

    public static class CacheStats {
        private long hits;
        private long misses;
        private long evictions;

        public CacheStats(long hits, long misses, long evictions) {
            this.hits = hits;
            this.misses = misses;
            this.evictions = evictions;
        }

        public long getHits() {
            return hits;
        }

        public long getMisses() {
            return misses;
        }

        public long getEvictions() {
            return evictions;
        }
    }

    /**
     * 在读取缓存数据时  当成功get到数据 触发该函数 (代表数据未过期)
     * @param entry
     * @param now
     * @return
     */
    private boolean promote(Entry<K, V> entry, long now) {
        boolean promoted = true;
        try (ReleasableLock ignored = lruLock.acquire()) {
            switch (entry.state) {
                case DELETED:
                    promoted = false;
                    break;
                case EXISTING:
                    // 最常被访问的元素又回到了链表头
                    relinkAtHead(entry);
                    break;
                    // 首次获取时为NEW
                case NEW:
                    linkAtHead(entry);
                    break;
            }

            // 每当插入一个新元素时才检测是否要进行一次清理 因为频繁的清理会带来太多消耗  所有只有当weight 超过maxWeight时 才会开始检测
            if (promoted) {
                evict(now);
            }
        }
        return promoted;
    }

    private void evict(long now) {
        assert lruLock.isHeldByCurrentThread();

        // 从尾部开始往前检测是否有需要被移除的元素
        while (tail != null && shouldPrune(tail, now)) {
            evictEntry(tail);
        }
    }

    /**
     * 当满足驱逐条件时 执行驱逐操作
     * @param entry
     */
    private void evictEntry(Entry<K, V> entry) {
        assert lruLock.isHeldByCurrentThread();

        CacheSegment<K, V> segment = getCacheSegment(entry.key);
        if (segment != null) {
            // 需要同时匹配 key 和value 才能移除 因为相同key 会覆盖之前的数据 那么此时数据仅存在于lru链表中 并没有存在于map中
            segment.remove(entry.key, entry.value, f -> {});
        }
        delete(entry, RemovalNotification.RemovalReason.EVICTED);
    }

    /**
     * 将指定的entry 从lru链表中移除 同时包装notification 并通知监听器
     * @param entry
     * @param removalReason
     */
    private void delete(Entry<K, V> entry, RemovalNotification.RemovalReason removalReason) {
        assert lruLock.isHeldByCurrentThread();

        if (unlink(entry)) {
            removalListener.onRemoval(new RemovalNotification<>(entry.key, entry.value, removalReason));
        }
    }

    private boolean shouldPrune(Entry<K, V> entry, long now) {
        return exceedsWeight() || isExpired(entry, now);
    }

    /**
     * 此时所有entry累加的总权重 已经超过了一开始设定的值 这时才有必要进行修剪 (或者说将元素从lru链表中移除)
     * @return
     */
    private boolean exceedsWeight() {
        return maximumWeight != -1 && weight > maximumWeight;
    }

    /**
     * 检查entry是否已经过期
     * 总计2种条件 一种是自生成以来过了多久 还有一种是距离上次get过了多久
     * @param entry
     * @param now
     * @return
     */
    private boolean isExpired(Entry<K, V> entry, long now) {
        return (entriesExpireAfterAccess && now - entry.accessTime > expireAfterAccessNanos) ||
                (entriesExpireAfterWrite && now - entry.writeTime > expireAfterWriteNanos);
    }

    /**
     * 将某个entry从链表中移除
     * @param entry
     * @return
     */
    private boolean unlink(Entry<K, V> entry) {
        assert lruLock.isHeldByCurrentThread();

        // 一般存在于lru链表中的 都是EXISTING
        if (entry.state == State.EXISTING) {
            final Entry<K, V> before = entry.before;
            final Entry<K, V> after = entry.after;

            if (before == null) {
                // removing the head
                assert head == entry;
                head = after;
                if (head != null) {
                    head.before = null;
                }
            } else {
                // removing inner element
                before.after = after;
                entry.before = null;
            }

            if (after == null) {
                // removing tail
                assert tail == entry;
                tail = before;
                if (tail != null) {
                    tail.after = null;
                }
            } else {
                // removing inner element
                after.before = before;
                entry.after = null;
            }

            // 处理完毕后 将权重值减少 同时将state修改成DELETED
            count--;
            weight -= weigher.applyAsLong(entry.key, entry.value);
            entry.state = State.DELETED;
            return true;
        } else {
            return false;
        }
    }

    /**
     * 将缓存数据插入到 lru链表头部
     * @param entry
     */
    private void linkAtHead(Entry<K, V> entry) {
        assert lruLock.isHeldByCurrentThread();

        // 这几行就是构建链表 同时entry成为了新的head节点
        Entry<K, V> h = head;
        entry.before = null;
        entry.after = head;
        head = entry;
        if (h == null) {
            tail = entry;
        } else {
            h.before = entry;
        }

        count++;
        weight += weigher.applyAsLong(entry.key, entry.value);
        entry.state = State.EXISTING;
    }

    /**
     * 将某个entry 重新设置到lru头部
     * @param entry
     */
    private void relinkAtHead(Entry<K, V> entry) {
        assert lruLock.isHeldByCurrentThread();

        if (head != entry) {
            unlink(entry);
            linkAtHead(entry);
        }
    }

    private CacheSegment<K, V> getCacheSegment(K key) {
        return segments[key.hashCode() & 0xff];
    }
}
