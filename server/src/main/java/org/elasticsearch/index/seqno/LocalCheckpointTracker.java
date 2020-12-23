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

package org.elasticsearch.index.seqno;

import com.carrotsearch.hppc.LongObjectHashMap;
import org.elasticsearch.common.SuppressForbidden;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class generates sequences numbers and keeps track of the so-called "local checkpoint" which is the highest number for which all
 * previous sequence numbers have been processed (inclusive).
 * 本地检查点的链路追踪对象
 */
public class LocalCheckpointTracker {

    /**
     * We keep a bit for each sequence number that is still pending. To optimize allocation, we do so in multiple sets allocating them on
     * demand and cleaning up while completed. This constant controls the size of the sets.
     */
    static final short BIT_SET_SIZE = 1024;

    /**
     * A collection of bit sets representing processed sequence numbers. Each sequence number is mapped to a bit set by dividing by the
     * bit set size.
     * 会使用位图记录已经处理过的seqNo
     */
    final LongObjectHashMap<CountedBitSet> processedSeqNo = new LongObjectHashMap<>();

    /**
     * A collection of bit sets representing durably persisted sequence numbers. Each sequence number is mapped to a bit set by dividing by
     * the bit set size.
     * 存储被持久化的 seqNo???
     */
    final LongObjectHashMap<CountedBitSet> persistedSeqNo = new LongObjectHashMap<>();

    /**
     * The current local checkpoint, i.e., all sequence numbers no more than this number have been processed.
     * 记录此时已经处理过的seqNo  之后要处理的必须比这个大
     */
    final AtomicLong processedCheckpoint = new AtomicLong();

    /**
     * The current persisted local checkpoint, i.e., all sequence numbers no more than this number have been durably persisted.
     */
    final AtomicLong persistedCheckpoint = new AtomicLong();

    /**
     * The next available sequence number.
     * 下一个预期更新的seq
     */
    final AtomicLong nextSeqNo = new AtomicLong();

    /**
     * Initialize the local checkpoint service. The {@code maxSeqNo} should be set to the last sequence number assigned, or
     * {@link SequenceNumbers#NO_OPS_PERFORMED} and {@code localCheckpoint} should be set to the last known local checkpoint,
     * or {@link SequenceNumbers#NO_OPS_PERFORMED}.
     *
     * @param maxSeqNo        the last sequence number assigned, or {@link SequenceNumbers#NO_OPS_PERFORMED}
     * @param localCheckpoint the last known local checkpoint, or {@link SequenceNumbers#NO_OPS_PERFORMED}
     *                        从segment_N.userData中获取 maxSeqNo 以及 localCheckPoint信息
     *                        进行初始化
     */
    public LocalCheckpointTracker(final long maxSeqNo, final long localCheckpoint) {
        if (localCheckpoint < 0 && localCheckpoint != SequenceNumbers.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException(
                "local checkpoint must be non-negative or [" + SequenceNumbers.NO_OPS_PERFORMED + "] "
                    + "but was [" + localCheckpoint + "]");
        }
        if (maxSeqNo < 0 && maxSeqNo != SequenceNumbers.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException(
                "max seq. no. must be non-negative or [" + SequenceNumbers.NO_OPS_PERFORMED + "] but was [" + maxSeqNo + "]");
        }
        nextSeqNo.set(maxSeqNo + 1);
        processedCheckpoint.set(localCheckpoint);
        persistedCheckpoint.set(localCheckpoint);
    }

    /**
     * Issue the next sequence number.
     *
     * @return the next assigned sequence number
     */
    public long generateSeqNo() {
        return nextSeqNo.getAndIncrement();
    }

    /**
     * Marks the provided sequence number as seen and updates the max_seq_no if needed.
     */
    public void advanceMaxSeqNo(final long seqNo) {
        // 比较传入的参数 与原值  保留更大的那个
        nextSeqNo.accumulateAndGet(seqNo + 1, Math::max);
    }

    /**
     * Marks the provided sequence number as processed and updates the processed checkpoint if possible.
     *
     * @param seqNo the sequence number to mark as processed
     */
    public synchronized void markSeqNoAsProcessed(final long seqNo) {
        markSeqNo(seqNo, processedCheckpoint, processedSeqNo);
    }

    /**
     * Marks the provided sequence number as persisted and updates the checkpoint if possible.
     *
     * @param seqNo the sequence number to mark as persisted
     *              记录对应的operation 已经被写入到事务文件中了
     *              在 translogWriter中 只有刷盘的时候才会执行该函数
     */
    public synchronized void markSeqNoAsPersisted(final long seqNo) {
        markSeqNo(seqNo, persistedCheckpoint, persistedSeqNo);
    }

    /**
     * 标记某个seq 已经处理过了  可能是processed/persisted
     * @param seqNo  本次处理完毕的seq
     * @param checkPoint  之前的检查点
     * @param bitSetMap
     */
    private void markSeqNo(final long seqNo, final AtomicLong checkPoint, final LongObjectHashMap<CountedBitSet> bitSetMap) {
        assert Thread.holdsLock(this);
        // make sure we track highest seen sequence number
        // 尝试更新nextSeq
        advanceMaxSeqNo(seqNo);
        if (seqNo <= checkPoint.get()) {
            // this is possible during recovery where we might replay an operation that was also replicated
            return;
        }
        // seq 使用位图来存储 当某个seq被确认后 会设置到位图中
        final CountedBitSet bitSet = getBitSetForSeqNo(bitSetMap, seqNo);
        // 位运算 得到要设置的位置
        final int offset = seqNoToBitSetOffset(seqNo);
        bitSet.set(offset);
        // 当此时seq 已经超过了checkPoint时 更新checkPoint的值   也就是会尽可能保持 seq与checkPoint的一致性
        if (seqNo == checkPoint.get() + 1) {
            updateCheckpoint(checkPoint, bitSetMap);
        }
    }

    /**
     * The current checkpoint which can be advanced by {@link #markSeqNoAsProcessed(long)}.
     *
     * @return the current checkpoint
     */
    public long getProcessedCheckpoint() {
        return processedCheckpoint.get();
    }

    /**
     * The current persisted checkpoint which can be advanced by {@link #markSeqNoAsPersisted(long)}.
     *
     * @return the current persisted checkpoint
     */
    public long getPersistedCheckpoint() {
        return persistedCheckpoint.get();
    }

    /**
     * The maximum sequence number issued so far.
     *
     * @return the maximum sequence number
     */
    public long getMaxSeqNo() {
        return nextSeqNo.get() - 1;
    }


    /**
     * constructs a {@link SeqNoStats} object, using local state and the supplied global checkpoint
     *
     * This is needed to make sure the persisted local checkpoint and max seq no are consistent
     */
    public synchronized SeqNoStats getStats(final long globalCheckpoint) {
        return new SeqNoStats(getMaxSeqNo(), getPersistedCheckpoint(), globalCheckpoint);
    }

    /**
     * Waits for all operations up to the provided sequence number to complete.
     *
     * @param seqNo the sequence number that the checkpoint must advance to before this method returns
     * @throws InterruptedException if the thread was interrupted while blocking on the condition
     * 阻塞直到指定的seq 已经完成处理
     * 在正常处理流程下是单调递增的
     */
    @SuppressForbidden(reason = "Object#wait")
    public synchronized void waitForProcessedOpsToComplete(final long seqNo) throws InterruptedException {
        while (processedCheckpoint.get() < seqNo) {
            // notified by updateCheckpoint
            this.wait();
        }
    }

    /**
     * Checks if the given sequence number was marked as processed in this tracker.
     * 每个操作都有一个 seq  通过判断seq 与本地检查点对象维护的 processedCheckpoint 来检测某个操作是否已经执行过了
     */
    public boolean hasProcessed(final long seqNo) {
        assert seqNo >= 0 : "invalid seq_no=" + seqNo;
        if (seqNo >= nextSeqNo.get()) {
            return false;
        }
        if (seqNo <= processedCheckpoint.get()) {
            return true;
        }
        final long bitSetKey = getBitSetKey(seqNo);
        final int bitSetOffset = seqNoToBitSetOffset(seqNo);
        synchronized (this) {
            // check again under lock
            if (seqNo <= processedCheckpoint.get()) {
                return true;
            }
            // 如果已经设置到位图中了 必然已经被处理过了
            final CountedBitSet bitSet = processedSeqNo.get(bitSetKey);
            return bitSet != null && bitSet.get(bitSetOffset);
        }
    }

    /**
     * Moves the checkpoint to the last consecutively processed sequence number. This method assumes that the sequence number
     * following the current checkpoint is processed.
     * @param checkPoint 当前检查点的值
     * @param bitSetMap 存储seq的位图
     * 更新检查点  每次增加1
     */
    @SuppressForbidden(reason = "Object#notifyAll")
    private void updateCheckpoint(AtomicLong checkPoint, LongObjectHashMap<CountedBitSet> bitSetMap) {
        assert Thread.holdsLock(this);
        assert getBitSetForSeqNo(bitSetMap, checkPoint.get() + 1).get(seqNoToBitSetOffset(checkPoint.get() + 1)) :
            "updateCheckpoint is called but the bit following the checkpoint is not set";
        try {
            // keep it simple for now, get the checkpoint one by one; in the future we can optimize and read words
            // 转换成位图的key
            long bitSetKey = getBitSetKey(checkPoint.get());
            CountedBitSet current = bitSetMap.get(bitSetKey);
            if (current == null) {
                // the bit set corresponding to the checkpoint has already been removed, set ourselves up for the next bit set
                assert checkPoint.get() % BIT_SET_SIZE == BIT_SET_SIZE - 1;
                // 当checkpoint 刚好是某个位图的最后一个 seq时   会将数据存储到下一个位图 并清理上个位图
                current = bitSetMap.get(++bitSetKey);
            }
            do {
                checkPoint.incrementAndGet();
                /*
                 * The checkpoint always falls in the current bit set or we have already cleaned it; if it falls on the last bit of the
                 * current bit set, we can clean it.
                 * 代表是当前这个位图的最后一个seq
                 * 选择抛弃这个位图 并roll到下一个
                 */
                if (checkPoint.get() == lastSeqNoInBitSet(bitSetKey)) {
                    assert current != null;
                    final CountedBitSet removed = bitSetMap.remove(bitSetKey);
                    assert removed == current;
                    current = bitSetMap.get(++bitSetKey);
                }
                // 当false时 退出循环      默认情况下 在外层方法 checkpoint的下一个位置(seqNo) 已经被设置了  这里是前进2位 应该是未设置 就可以了
                // 实际上就是为了让checkpoint 与seqNo同步
            } while (current != null && current.get(seqNoToBitSetOffset(checkPoint.get() + 1)));
        } finally {
            // notifies waiters in waitForProcessedOpsToComplete
            // 一旦更新了 processedCheckpoint/persistedCheckpoint 就可以考虑唤醒阻塞的线程了
            this.notifyAll();
        }
    }

    /**
     * 获取key 匹配的位图的最后一个seq
     * @param bitSetKey
     * @return
     */
    private static long lastSeqNoInBitSet(final long bitSetKey) {
        return (1 + bitSetKey) * BIT_SET_SIZE - 1;
    }

    /**
     * Return the bit set for the provided sequence number, possibly allocating a new set if needed.
     *
     * @param seqNo the sequence number to obtain the bit set for
     * @return the bit set corresponding to the provided sequence number
     */
    private static long getBitSetKey(final long seqNo) {
        return seqNo / BIT_SET_SIZE;
    }

    /**
     * 将新的 seq记录到位图中 并返回位图
     * @param bitSetMap
     * @param seqNo
     * @return
     */
    private CountedBitSet getBitSetForSeqNo(final LongObjectHashMap<CountedBitSet> bitSetMap, final long seqNo) {
        assert Thread.holdsLock(this);
        // 也是用了分段思想 最外层用hash定位下标 好处是不会一次创建太大的内存块
        final long bitSetKey = getBitSetKey(seqNo);
        final int index = bitSetMap.indexOf(bitSetKey);
        final CountedBitSet bitSet;
        if (bitSetMap.indexExists(index)) {
            bitSet = bitSetMap.indexGet(index);
        } else {
            bitSet = new CountedBitSet(BIT_SET_SIZE);
            bitSetMap.indexInsert(index, bitSetKey, bitSet);
        }
        return bitSet;
    }

    /**
     * Obtain the position in the bit set corresponding to the provided sequence number. The bit set corresponding to the sequence number
     * can be obtained via {@link #getBitSetForSeqNo(LongObjectHashMap, long)}.
     *
     * @param seqNo the sequence number to obtain the position for
     * @return the position in the bit set corresponding to the provided sequence number
     */
    private static int seqNoToBitSetOffset(final long seqNo) {
        return Math.toIntExact(seqNo % BIT_SET_SIZE);
    }

}
