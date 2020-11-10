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

package org.elasticsearch.common.util.concurrent;


import org.elasticsearch.common.lease.Releasable;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class manages locks. Locks can be accessed with an identifier and are
 * created the first time they are acquired and removed if no thread hold the
 * lock. The latter is important to assure that the list of locks does not grow
 * infinitely.
 * Note: this lock is reentrant
 * 感觉像是在用 CAS 实现一个串行锁
 * */
public final class KeyedLock<T> {

    /**
     * KeyLock 内部包含锁对象和一个计数器
     */
    private final ConcurrentMap<T, KeyLock> map = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final boolean fair;

    /**
     * Creates a new lock
     * @param fair Use fair locking, ie threads get the lock in the order they requested it
     */
    public KeyedLock(boolean fair) {
        this.fair = fair;
    }

    /**
     * Creates a non-fair lock
     */
    public KeyedLock() {
        this(false);
    }

    /**
     * Acquires a lock for the given key. The key is compared by it's equals method not by object identity. The lock can be acquired
     * by the same thread multiple times. The lock is released by closing the returned {@link Releasable}.
     * 通过key 找到对应的锁对象
     */
    public Releasable acquire(T key) {
        while (true) {
            KeyLock perNodeLock = map.get(key);
            if (perNodeLock == null) {
                // 创建公平锁/非公平锁
                ReleasableLock newLock = tryCreateNewLock(key);
                if (newLock != null) {
                    return newLock;
                }
            } else {
                assert perNodeLock != null;
                int i = perNodeLock.count.get();
                // 计数器为0 代表当前没有上锁
                if (i > 0 && perNodeLock.count.compareAndSet(i, i + 1)) {
                    // 这里可能会发生阻塞的
                    perNodeLock.lock();
                    return new ReleasableLock(key, perNodeLock);
                }
            }
        }
    }

    /**
     * Tries to acquire the lock for the given key and returns it. If the lock can't be acquired null is returned.
     * 尝试获取key对应的锁
     */
    public Releasable tryAcquire(T key) {
        final KeyLock perNodeLock = map.get(key);
        if (perNodeLock == null) {
            // 锁还不存在  创建一个新锁
            return tryCreateNewLock(key);
        }
        if (perNodeLock.tryLock()) { // ok we got it - make sure we increment it accordingly otherwise release it again
            int i;
            // 如果此时没有线程持有锁了 那么引用计数应该就是0
            while ((i = perNodeLock.count.get()) > 0) {
                // we have to do this in a loop here since even if the count is > 0
                // there could be a concurrent blocking acquire that changes the count and then this CAS fails. Since we already got
                // the lock we should retry and see if we can still get it or if the count is 0. If that is the case and we give up.
                if (perNodeLock.count.compareAndSet(i, i + 1)) {
                    return new ReleasableLock(key, perNodeLock);
                }
            }
            // 对冲上面的tryLock动作
            perNodeLock.unlock(); // make sure we unlock and don't leave the lock in a locked state
        }
        return null;
    }

    private ReleasableLock tryCreateNewLock(T key) {
        KeyLock newLock = new KeyLock(fair);
        // 生成锁的时候直接上锁吗
        newLock.lock();
        KeyLock keyLock = map.putIfAbsent(key, newLock);
        if (keyLock == null) {
            return new ReleasableLock(key, newLock);
        }
        return null;
    }

    /**
     * Returns <code>true</code> iff the caller thread holds the lock for the given key
     */
    public boolean isHeldByCurrentThread(T key) {
        KeyLock lock = map.get(key);
        if (lock == null) {
            return false;
        }
        return lock.isHeldByCurrentThread();
    }

    /**
     * 减少一次引用计数
     * @param key
     * @param lock
     */
    private void release(T key, KeyLock lock) {
        assert lock == map.get(key);
        final int decrementAndGet = lock.count.decrementAndGet();
        lock.unlock();
        if (decrementAndGet == 0) {
            map.remove(key, lock);
        }
        assert decrementAndGet >= 0 : decrementAndGet + " must be >= 0 but wasn't";
    }


    private final class ReleasableLock implements Releasable {
        final T key;
        final KeyLock lock;
        final AtomicBoolean closed = new AtomicBoolean();

        private ReleasableLock(T key, KeyLock lock) {
            this.key = key;
            this.lock = lock;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                release(key, lock);
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class KeyLock extends ReentrantLock {
        KeyLock(boolean fair) {
            super(fair);
        }

        private final AtomicInteger count = new AtomicInteger(1);
    }

    /**
     * Returns <code>true</code> if this lock has at least one locked key.
     */
    public boolean hasLockedKeys() {
        return map.isEmpty() == false;
    }

}
