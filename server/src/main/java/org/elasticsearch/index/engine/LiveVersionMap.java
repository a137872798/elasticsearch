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

import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.KeyedLock;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maps _uid value to its version information.
 * 该对象是监控某个资源的刷新
 */
final class LiveVersionMap implements ReferenceManager.RefreshListener, Accountable {

    /**
     * 该对象内 以id为单位进行并发控制
     */
    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();


    /**
     * 用于查询版本号的对象
     */
    private static final class VersionLookup {

        /** Tracks bytes used by current map, i.e. what is freed on refresh. For deletes, which are also added to tombstones,
         *  we only account for the CHM entry here, and account for BytesRef/VersionValue against the tombstones, since refresh would not
         *  clear this RAM. */
        final AtomicLong ramBytesUsed = new AtomicLong();

        private static final VersionLookup EMPTY = new VersionLookup(Collections.emptyMap());

        /**
         * 不同的数据流都可以对应到一个 唯一的版本号信息
         */
        private final Map<BytesRef, VersionValue> map;

        // each version map has a notion of safe / unsafe which allows us to apply certain optimization in the auto-generated ID usecase
        // where we know that documents can't have any duplicates so we can skip the version map entirely. This reduces
        // the memory pressure significantly for this use-case where we often get a massive amount of small document (metrics).
        // if the version map is in safeAccess mode we track all version in the version map. yet if a document comes in that needs
        // safe access but we are not in this mode we force a refresh and make the map as safe access required. All subsequent ops will
        // respect that and fill the version map. The nice part here is that we are only really requiring this for a single ID and since
        // we hold the ID lock in the engine while we do all this it's safe to do it globally unlocked.
        // NOTE: these values can both be non-volatile since it's ok to read a stale value per doc ID. We serialize changes in the engine
        // that will prevent concurrent updates to the same document ID and therefore we can rely on the happens-before guanratee of the
        // map reference itself.
        // 描述是否安全  默认为false
        private boolean unsafe;

        // minimum timestamp of delete operations that were made while this map was active. this is used to make sure they are kept in
        // the tombstone
        private final AtomicLong minDeleteTimestamp = new AtomicLong(Long.MAX_VALUE);

        /**
         * 存储byte流 与 版本号的关联关系
         * @param map
         */
        private VersionLookup(Map<BytesRef, VersionValue> map) {
            this.map = map;
        }

        VersionValue get(BytesRef key) {
            return map.get(key);
        }

        VersionValue put(BytesRef key, VersionValue value) {
            return map.put(key, value);
        }

        boolean isEmpty() {
            return map.isEmpty();
        }

        int size() {
            return map.size();
        }

        boolean isUnsafe() {
            return unsafe;
        }

        void markAsUnsafe() {
            unsafe = true;
        }

        public VersionValue remove(BytesRef uid) {
            return map.remove(uid);
        }

        /**
         * 每当发生一次remove的时候 会触发该方法 记录deleteVersionValue中最小的时间
         * @param delete
         */
        public void updateMinDeletedTimestamp(DeleteVersionValue delete) {
            long time = delete.time;
            minDeleteTimestamp.updateAndGet(prev -> Math.min(time, prev));
        }

    }


    private static final class Maps {

        // 存在 current/old 2个查询不同版本的map对象

        // All writes (adds and deletes) go into here:
        final VersionLookup current;

        // Used while refresh is running, and to hold adds/deletes until refresh finishes.  We read from both current and old on lookup:
        final VersionLookup old;

        // this is not volatile since we don't need to maintain a happens before relation ship across doc IDs so it's enough to
        // have the volatile read of the Maps reference to make it visible even across threads.
        boolean needsSafeAccess;
        final boolean previousMapsNeededSafeAccess;


        /**
         *
         * @param current
         * @param old
         * @param previousMapsNeededSafeAccess  默认为false
         */
        Maps(VersionLookup current, VersionLookup old, boolean previousMapsNeededSafeAccess) {
            this.current = current;
            this.old = old;
            this.previousMapsNeededSafeAccess = previousMapsNeededSafeAccess;
        }


        Maps() {
            this(new VersionLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency()),
                VersionLookup.EMPTY, false);
        }

        /**
         * 需要安全访问 或者上一个map需要安全访问
         * @return
         */
        boolean isSafeAccessMode() {
            return needsSafeAccess || previousMapsNeededSafeAccess;
        }

        /**
         * 是否需要继承在安全模式下访问
         * @return
         */
        boolean shouldInheritSafeAccess() {
            // 如果之前的maps中没有数据  并且是不安全的
            final boolean mapHasNotSeenAnyOperations = current.isEmpty() && current.isUnsafe() == false;
            return needsSafeAccess
                // we haven't seen any ops and map before needed it so we maintain it
                || (mapHasNotSeenAnyOperations && previousMapsNeededSafeAccess);
        }

        /**
         * Builds a new map for the refresh transition this should be called in beforeRefresh()
         */
        Maps buildTransitionMap() {
            return new Maps(new VersionLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.size())), current,
                shouldInheritSafeAccess());
        }

        /**
         * builds a new map that invalidates the old map but maintains the current. This should be called in afterRefresh()
         * 只是将 old修改成无效值了 其余属性不变
         */
        Maps invalidateOldMap() {
            return new Maps(current, VersionLookup.EMPTY, previousMapsNeededSafeAccess);
        }

        void put(BytesRef uid, VersionValue version) {
            long uidRAMBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;
            long ramAccounting = BASE_BYTES_PER_CHM_ENTRY + version.ramBytesUsed() + uidRAMBytesUsed;
            VersionValue previousValue = current.put(uid, version);
            // 代表ram的变化 如果顶替了旧数据 那么差值就要反应到 VersionLookup上
            ramAccounting += previousValue == null ? 0 : -(BASE_BYTES_PER_CHM_ENTRY + previousValue.ramBytesUsed() + uidRAMBytesUsed);
            adjustRam(ramAccounting);
        }

        void adjustRam(long value) {
            if (value != 0) {
                long v = current.ramBytesUsed.addAndGet(value);
                assert v >= 0 : "bytes=" + v;
            }
        }

        /**
         * 将某个byte 对应的版本号移除
         * @param uid
         * @param deleted  在普通的VersionValue基础上增加了一个delete属性
         */
        void remove(BytesRef uid, DeleteVersionValue deleted) {
            VersionValue previousValue = current.remove(uid);
            // 更新删除的最小时间
            current.updateMinDeletedTimestamp(deleted);
            if (previousValue != null) {
                long uidRAMBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;
                adjustRam(-(BASE_BYTES_PER_CHM_ENTRY + previousValue.ramBytesUsed() + uidRAMBytesUsed));
            }
            // old也要移除啊 为什么会需要管理2个 lookup
            if (old != VersionLookup.EMPTY) {
                // we also need to remove it from the old map here to make sure we don't read this stale value while
                // we are in the middle of a refresh. Most of the time the old map is an empty map so we can skip it there.
                old.remove(uid);
            }
        }

        long getMinDeleteTimestamp() {
            return Math.min(current.minDeleteTimestamp.get(), old.minDeleteTimestamp.get());
        }
    }

    // All deletes also go here, and delete "tombstones" are retained after refresh:
    private final Map<BytesRef, DeleteVersionValue> tombstones = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private volatile Maps maps = new Maps();
    // we maintain a second map that only receives the updates that we skip on the actual map (unsafe ops)
    // this map is only maintained if assertions are enabled
    private volatile Maps unsafeKeysMap = new Maps();

    /**
     * Bytes consumed for each BytesRef UID:
     * In this base value, we account for the {@link BytesRef} object itself as
     * well as the header of the byte[] array it holds, and some lost bytes due
     * to object alignment. So consumers of this constant just have to add the
     * length of the byte[] (assuming it is not shared between multiple
     * instances).
     */
    private static final long BASE_BYTES_PER_BYTESREF =
        // shallow memory usage of the BytesRef object
        RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) +
            // header of the byte[] array
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER +
            // with an alignment size (-XX:ObjectAlignmentInBytes) of 8 (default),
            // there could be between 0 and 7 lost bytes, so we account for 3
            // lost bytes on average
            3;

    /**
     * Bytes used by having CHM point to a key/value.
     */
    private static final long BASE_BYTES_PER_CHM_ENTRY;

    static {
        // use the same impl as the Maps does
        Map<Integer, Integer> map = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        map.put(0, 0);
        long chmEntryShallowSize = RamUsageEstimator.shallowSizeOf(map.entrySet().iterator().next());
        // assume a load factor of 50%
        // for each entry, we need two object refs, one for the entry itself
        // and one for the free space that is due to the fact hash tables can
        // not be fully loaded
        BASE_BYTES_PER_CHM_ENTRY = chmEntryShallowSize + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }

    /**
     * Tracks bytes used by tombstones (deletes)
     * 墓碑对象总计占用了多少byte
     */
    private final AtomicLong ramBytesUsedTombstones = new AtomicLong();

    /**
     * 本对象会设置到 internalReaderManager上  当执行refresh前会触发该钩子
     * @throws IOException
     */
    @Override
    public void beforeRefresh() throws IOException {
        // Start sending all updates after this point to the new
        // map.  While reopen is running, any lookup will first
        // try this new map, then fallback to old, then to the
        // current searcher:
        // 当执行刷新操作前  需要更新maps内的数据
        maps = maps.buildTransitionMap();
        assert (unsafeKeysMap = unsafeKeysMap.buildTransitionMap()) != null;
        // This is not 100% correct, since concurrent indexing ops can change these counters in between our execution of the previous
        // line and this one, but that should be minor, and the error won't accumulate over time:
    }

    /**
     * 当某次刷新动作完成后触发
     * 那么只要发生了refresh 之前的id对应的version信息一定会被清除
     * @param didRefresh   本次是否发生了数据的变化
     * @throws IOException
     */
    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        // We can now drop old because these operations are now visible via the newly opened searcher.  Even if didRefresh is false, which
        // means Lucene did not actually open a new reader because it detected no changes, it's possible old has some entries in it, which
        // is fine: it means they were actually already included in the previously opened reader, so we can still safely drop them in that
        // case.  This is because we assign new maps (in beforeRefresh) slightly before Lucene actually flushes any segments for the
        // reopen, and so any concurrent indexing requests can still sneak in a few additions to that current map that are in fact
        // reflected in the previous reader.   We don't touch tombstones here: they expire on their own index.gc_deletes timeframe:
        // 将old置空
        maps = maps.invalidateOldMap();
        assert (unsafeKeysMap = unsafeKeysMap.invalidateOldMap()) != null;

    }

    /**
     * Returns the live version (add or delete) for this uid.
     * 获取该id对应的版本号信息
     */
    VersionValue getUnderLock(final BytesRef uid) {
        return getUnderLock(uid, maps);
    }

    /**
     * 查找某个id对应的版本号
     * @param uid
     * @param currentMaps
     * @return
     */
    private VersionValue getUnderLock(final BytesRef uid, Maps currentMaps) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        // First try to get the "live" value:
        // 从3个容器中分别查找
        VersionValue value = currentMaps.current.get(uid);
        if (value != null) {
            return value;
        }

        value = currentMaps.old.get(uid);
        if (value != null) {
            return value;
        }

        // 墓碑容器中仅 记录已经被删除的数据
        return tombstones.get(uid);
    }

    VersionValue getVersionForAssert(final BytesRef uid) {
        VersionValue value = getUnderLock(uid, maps);
        if (value == null) {
            value = getUnderLock(uid, unsafeKeysMap);
        }
        return value;
    }

    boolean isUnsafe() {
        return maps.current.isUnsafe() || maps.old.isUnsafe();
    }

    void enforceSafeAccess() {
        maps.needsSafeAccess = true;
    }

    boolean isSafeAccessRequired() {
        return maps.isSafeAccessMode();
    }

    /**
     * Adds this uid/version to the pending adds map iff the map needs safe access.
     * 将某个id对应的 indexVersion信息插入到maps中
     */
    void maybePutIndexUnderLock(BytesRef uid, IndexVersionValue version) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        Maps maps = this.maps;
        // 在安全模式下访问的话 需要加锁
        if (maps.isSafeAccessMode()) {
            putIndexUnderLock(uid, version);
        } else {
            // Even though we don't store a record of the indexing operation (and mark as unsafe),
            // we should still remove any previous delete for this uuid (avoid accidental accesses).
            // Not this should not hurt performance because the tombstone is small (or empty) when unsafe is relevant.
            removeTombstoneUnderLock(uid);
            maps.current.markAsUnsafe();
            assert putAssertionMap(uid, version);
        }
    }

    /**
     * 将某个id 以及相关的版本号存储到容器中
     * @param uid
     * @param version
     */
    void putIndexUnderLock(BytesRef uid, IndexVersionValue version) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        maps.put(uid, version);
        // 因为uid对应的值有效了 所以从 tombstone中移除
        removeTombstoneUnderLock(uid);
    }

    private boolean putAssertionMap(BytesRef uid, IndexVersionValue version) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        unsafeKeysMap.put(uid, version);
        return true;
    }

    /**
     * 记录某个id此时最新的versionValue信息 比如是一次index操作 或者是一次delete操作
     * @param uid
     * @param version
     */
    void putDeleteUnderLock(BytesRef uid, DeleteVersionValue version) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        // 因为是删除操作 所以将对应的数据存储到 tombstone容器中
        putTombstone(uid, version);
        // id 仅能存在于 maps和tombstones 中的一个
        maps.remove(uid, version);
    }

    private void putTombstone(BytesRef uid, DeleteVersionValue version) {
        long uidRAMBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;
        // Also enroll the delete into tombstones, and account for its RAM too:
        final VersionValue prevTombstone = tombstones.put(uid, version);
        long accountRam = (BASE_BYTES_PER_CHM_ENTRY + version.ramBytesUsed() + uidRAMBytesUsed);
        // Deduct tombstones bytes used for the version we just removed or replaced:
        if (prevTombstone != null) {
            accountRam -= (BASE_BYTES_PER_CHM_ENTRY + prevTombstone.ramBytesUsed() + uidRAMBytesUsed);
        }
        if (accountRam != 0) {
            long v = ramBytesUsedTombstones.addAndGet(accountRam);
            assert v >= 0: "bytes=" + v;
        }
    }

    /**
     * Removes this uid from the pending deletes map.
     * 将墓碑中某个 uid对应的数据清理掉
     */
    void removeTombstoneUnderLock(BytesRef uid) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        long uidRAMBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;
        final VersionValue prev = tombstones.remove(uid);
        if (prev != null) {
            assert prev.isDelete();
            long v = ramBytesUsedTombstones.addAndGet(-(BASE_BYTES_PER_CHM_ENTRY + prev.ramBytesUsed() + uidRAMBytesUsed));
            assert v >= 0 : "bytes=" + v;
        }
    }

    /**
     * 检测该数据是否应该被删除
     * 删除数据是为了减少内存压力吧
     * @param maxTimestampToPrune   在该时间戳之前的记录需要被删除
     * @param maxSeqNoToPrune   在该seq之前的记录需要被删除
     * @param versionValue   本次检测是否允许被删除的数据
     * @return
     */
    private boolean canRemoveTombstone(long maxTimestampToPrune, long maxSeqNoToPrune, DeleteVersionValue versionValue) {
        // check if the value is old enough and safe to be removed
        final boolean isTooOld = versionValue.time < maxTimestampToPrune;
        final boolean isSafeToPrune = versionValue.seqNo <= maxSeqNoToPrune;
        // version value can't be removed it's
        // not yet flushed to lucene ie. it's part of this current maps object
        // 在这里还有个限制条件  当前时间必须小于 minDeleteTimestamp
        final boolean isNotTrackedByCurrentMaps = versionValue.time < maps.getMinDeleteTimestamp();
        return isTooOld && isSafeToPrune && isNotTrackedByCurrentMaps;
    }

    /**
     * Try to prune tombstones whose timestamp is less than maxTimestampToPrune and seqno at most the maxSeqNoToPrune.
     * @param maxSeqNoToPrune
     * @param maxTimestampToPrune
     *                            按照条件来清除墓碑数据
     */
    void pruneTombstones(long maxTimestampToPrune, long maxSeqNoToPrune) {
        // 每隔一段时间会尝试清理 墓碑中的数据   墓碑中的数据是每当发生一次删除操作 记录下来的
        for (Map.Entry<BytesRef, DeleteVersionValue> entry : tombstones.entrySet()) {
            // we do check before we actually lock the key - this way we don't need to acquire the lock for tombstones that are not
            // prune-able. If the tombstone changes concurrently we will re-read and step out below since if we can't collect it now w
            // we won't collect the tombstone below since it must be newer than this one.

            // 检测记录是否允许被删除
            if (canRemoveTombstone(maxTimestampToPrune, maxSeqNoToPrune, entry.getValue())) {
                final BytesRef uid = entry.getKey();

                // 这里是为了确保此时只有一条线程在处理该id相关的数据
                try (Releasable lock = keyedLock.tryAcquire(uid)) {
                    // we use tryAcquire here since this is a best effort and we try to be least disruptive
                    // this method is also called under lock in the engine under certain situations such that this can lead to deadlocks
                    // if we do use a blocking acquire. see #28714
                    if (lock != null) { // did we get the lock?
                        // Must re-get it here, vs using entry.getValue(), in case the uid was indexed/deleted since we pulled the iterator:
                        final DeleteVersionValue versionValue = tombstones.get(uid);
                        if (versionValue != null) {
                            if (canRemoveTombstone(maxTimestampToPrune, maxSeqNoToPrune, versionValue)) {
                                // 从tombstone中移除该id对应的记录
                                removeTombstoneUnderLock(uid);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Called when this index is closed.
     */
    synchronized void clear() {
        maps = new Maps();
        tombstones.clear();
        // NOTE: we can't zero this here, because a refresh thread could be calling InternalEngine.pruneDeletedTombstones at the same time,
        // and this will lead to an assert trip.  Presumably it's fine if our ramBytesUsedTombstones is non-zero after clear since the
        // index is being closed:
        //ramBytesUsedTombstones.set(0);
    }

    @Override
    public long ramBytesUsed() {
        return maps.current.ramBytesUsed.get() + ramBytesUsedTombstones.get();
    }

    /**
     * Returns how much RAM would be freed up by refreshing. This is {@link #ramBytesUsed} except does not include tombstones because they
     * don't clear on refresh.
     */
    long ramBytesUsedForRefresh() {
        return maps.current.ramBytesUsed.get();
    }

    /**
     * Returns how much RAM is current being freed up by refreshing.  This is {@link #ramBytesUsed()}
     * except does not include tombstones because they don't clear on refresh.
     */
    long getRefreshingBytes() {
        return maps.old.ramBytesUsed.get();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        // TODO: useful to break down RAM usage here?
        return Collections.emptyList();
    }

    /**
     * Returns the current internal versions as a point in time snapshot
     */
    Map<BytesRef, VersionValue> getAllCurrent() {
        return maps.current.map;
    }

    /** Iterates over all deleted versions, including new ones (not yet exposed via reader) and old ones
     *  (exposed via reader but not yet GC'd). */
    Map<BytesRef, DeleteVersionValue> getAllTombstones() {
        return tombstones;
    }

    /**
     * Acquires a releaseable lock for the given uId. All *UnderLock methods require
     * this lock to be hold by the caller otherwise the visibility guarantees of this version
     * map are broken. We assert on this lock to be hold when calling these methods.
     * @see KeyedLock
     */
    Releasable acquireLock(BytesRef uid) {
        return keyedLock.acquire(uid);
    }

    boolean assertKeyedLockHeldByCurrentThread(BytesRef uid) {
        assert keyedLock.isHeldByCurrentThread(uid) : "Thread [" + Thread.currentThread().getName() +
            "], uid [" + uid.utf8ToString() + "]";
        return true;
    }
}
