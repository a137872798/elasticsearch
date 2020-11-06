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

package org.elasticsearch.index.translog;

import org.apache.lucene.util.Counter;
import org.elasticsearch.Assertions;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 事务日志的删除策略
 */
public final class TranslogDeletionPolicy {

    private final Map<Object, RuntimeException> openTranslogRef;

    public void assertNoOpenTranslogRefs() {
        if (openTranslogRef.isEmpty() == false) {
            AssertionError e = new AssertionError("not all translog generations have been released");
            openTranslogRef.values().forEach(e::addSuppressed);
            throw e;
        }
    }

    /**
     * Records how many retention locks are held against each
     * translog generation
     * 维护了每个shard对应的 translog对象
     * 在该容器中的 就代表此时translog正在被使用
     */
    private final Map<Long, Counter> translogRefCounts = new HashMap<>();

    /**
     * 维护了一个提交点 每次更新该值时 只允许往上更新
     */
    private long localCheckpointOfSafeCommit = SequenceNumbers.NO_OPS_PERFORMED;


    public TranslogDeletionPolicy() {
        if (Assertions.ENABLED) {
            openTranslogRef = new ConcurrentHashMap<>();
        } else {
            openTranslogRef = null;
        }
    }

    public synchronized void setLocalCheckpointOfSafeCommit(long newCheckpoint) {
        if (newCheckpoint < this.localCheckpointOfSafeCommit) {
            throw new IllegalArgumentException("local checkpoint of the safe commit can't go backwards: " +
                "current [" + this.localCheckpointOfSafeCommit + "] new [" + newCheckpoint + "]");
        }
        this.localCheckpointOfSafeCommit = newCheckpoint;
    }

    /**
     * acquires the basis generation for a new snapshot. Any translog generation above, and including, the returned generation
     * will not be deleted until the returned {@link Releasable} is closed.
     * 应该是每个获取该gen对应的 translog时 同时都会获得一个 close对象 类似引用计数的操作 只有引用计数归0时才会触发真正的close操作
     */
    synchronized Releasable acquireTranslogGen(final long translogGen) {
        // 为每个 translog维护单独的计数器对象
        translogRefCounts.computeIfAbsent(translogGen, l -> Counter.newCounter(false)).addAndGet(1);
        final AtomicBoolean closed = new AtomicBoolean();
        assert assertAddTranslogRef(closed);
        return () -> {
            if (closed.compareAndSet(false, true)) {
                releaseTranslogGen(translogGen);
                assert assertRemoveTranslogRef(closed);
            }
        };
    }

    private boolean assertAddTranslogRef(Object reference) {
        final RuntimeException existing = openTranslogRef.put(reference, new RuntimeException());
        if (existing != null) {
            throw new AssertionError("double adding of closing reference", existing);
        }
        return true;
    }

    private boolean assertRemoveTranslogRef(Object reference) {
        return openTranslogRef.remove(reference) != null;
    }

    /**
     * returns the number of generations that were acquired for snapshots
     * 此刻正有多少translog 被访问
     */
    synchronized int pendingTranslogRefCount() {
        return translogRefCounts.size();
    }

    /**
     * releases a generation that was acquired by {@link #acquireTranslogGen(long)}
     * 减少引用计数
     */
    private synchronized void releaseTranslogGen(long translogGen) {
        Counter current = translogRefCounts.get(translogGen);
        if (current == null || current.get() <= 0) {
            throw new IllegalArgumentException("translog gen [" + translogGen + "] wasn't acquired");
        }
        if (current.addAndGet(-1) == 0) {
            // 当引用计数归0时 将该translog从管理的map中移除
            translogRefCounts.remove(translogGen);
        }
    }

    /**
     * Returns the minimum translog generation that is still required by the locks (via {@link #acquireTranslogGen(long)}.
     * 获取最小的 gen
     */
    synchronized long getMinTranslogGenRequiredByLocks() {
        return translogRefCounts.keySet().stream().reduce(Math::min).orElse(Long.MAX_VALUE);
    }

    /**
     * Returns the local checkpoint of the safe commit. This value is used to calculate the min required generation for recovery.
     */
    public synchronized long getLocalCheckpointOfSafeCommit() {
        return localCheckpointOfSafeCommit;
    }


    /**
     * 获取某个gen对应的计数值
     * @param gen
     * @return
     */
    synchronized long getTranslogRefCount(long gen) {
        final Counter counter = translogRefCounts.get(gen);
        return counter == null ? 0 : counter.get();
    }
}
