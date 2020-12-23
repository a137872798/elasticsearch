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

import com.carrotsearch.hppc.LongObjectHashMap;
import org.elasticsearch.index.seqno.CountedBitSet;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

/**
 * A snapshot composed out of multiple snapshots
 * 这是一个快照组  对应某个事务文件不同时间点的信息
 * 能够迭代内部包含的所有Operation
 */
final class MultiSnapshot implements Translog.Snapshot {

    /**
     * 一组快照
     */
    private final TranslogSnapshot[] translogs;
    /**
     * 这组快照下存储的operation总数
     */
    private final int totalOperations;
    /**
     * 总计发生了多少次覆盖操作  指的就是在位图中插入数据 发现存在旧数据
     */
    private int overriddenOperations;
    private final Closeable onClose;
    private int index;

    /**
     * 通过将一组位图连接起来 在逻辑上形成一个巨大的位图  好处是不会提前创建大块连续内存 避免浪费
     */
    private final SeqNoSet seenSeqNo;

    /**
     * Creates a new point in time snapshot of the given snapshots. Those snapshots are always iterated in-order.
     */
    MultiSnapshot(TranslogSnapshot[] translogs, Closeable onClose) {
        this.translogs = translogs;
        // 计算快照数总和
        this.totalOperations = Arrays.stream(translogs).mapToInt(TranslogSnapshot::totalOperations).sum();
        this.overriddenOperations = 0;
        this.onClose = onClose;
        this.seenSeqNo = new SeqNoSet();
        this.index = translogs.length - 1;
    }

    @Override
    public int totalOperations() {
        return totalOperations;
    }

    /**
     * 此时已经跳过了多少个操作
     * @return
     */
    @Override
    public int skippedOperations() {
        return Arrays.stream(translogs).mapToInt(TranslogSnapshot::skippedOperations).sum() + overriddenOperations;
    }

    /**
     * 将这组快照看作一个连续的对象 并迭代内部的operation
     * @return
     * @throws IOException
     */
    @Override
    public Translog.Operation next() throws IOException {
        // TODO: Read translog forward in 9.0+
        for (; index >= 0; index--) {
            // 每个快照内部有一个指针 记录此时读取到第几个快照
            final TranslogSnapshot current = translogs[index];
            Translog.Operation op;
            while ((op = current.next()) != null) {
                // 为什么是反向遍历啊
                if (op.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO || seenSeqNo.getAndSet(op.seqNo()) == false) {
                    return op;
                } else {
                    overriddenOperations++;
                }
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        onClose.close();
    }

    /**
     * 可能本身在逻辑上是一个很大的位图 所以先进行拆分  每个被拆分出来的位图就是seqNo
     */
    static final class SeqNoSet {
        static final short BIT_SET_SIZE = 1024;
        private final LongObjectHashMap<CountedBitSet> bitSets = new LongObjectHashMap<>();

        /**
         * Marks this sequence number and returns {@code true} if it is seen before.
         */
        boolean getAndSet(long value) {
            assert value >= 0;
            // 生成对应的序号
            final long key = value / BIT_SET_SIZE;
            // 通过序号找到位图
            CountedBitSet bitset = bitSets.get(key);
            if (bitset == null) {
                bitset = new CountedBitSet(BIT_SET_SIZE);
                bitSets.put(key, bitset);
            }
            // 计算在某个位图下应该设置的位置
            final int index = Math.toIntExact(value % BIT_SET_SIZE);
            final boolean wasOn = bitset.get(index);
            bitset.set(index);
            return wasOn;
        }
    }
}
