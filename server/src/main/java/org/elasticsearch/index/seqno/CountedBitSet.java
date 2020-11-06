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

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A {@link CountedBitSet} wraps a {@link FixedBitSet} but automatically releases the internal bitset
 * when all bits are set to reduce memory usage. This structure can work well for sequence numbers as
 * these numbers are likely to form contiguous ranges (eg. filling all bits).
 * 与普通位图的区别就是 会记录总计有多少数据写入到位图中
 */
public final class CountedBitSet {
    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(CountedBitSet.class);

    /**
     * 每当成功往位图中写入一位时 将该值+1
     */
    private short onBits; // Number of bits are set.

    /**
     * 该对象使用固定大小的数组实现 好处是实现简单 缺点是 当位图稀疏时会浪费空间
     */
    private FixedBitSet bitset;

    /**
     * 总计会写入多少位
     * @param numBits
     */
    public CountedBitSet(short numBits) {
        if (numBits <= 0) {
            throw new IllegalArgumentException("Number of bits must be positive. Given [" + numBits + "]");
        }
        this.onBits = 0;
        // 根据预估值初始化位图内部的数组
        this.bitset = new FixedBitSet(numBits);
    }

    public boolean get(int index) {
        assert 0 <= index && index < this.length();
        assert bitset == null || onBits < bitset.length() : "Bitset should be released when all bits are set";
        // lucene的套路 如果所有位都被填满 就不会初始化位图对象
        return bitset == null ? true : bitset.get(index);
    }

    public void set(int index) {
        assert 0 <= index && index < this.length();
        assert bitset == null || onBits < bitset.length() : "Bitset should be released when all bits are set";

        // Ignore set when bitset is full.
        if (bitset != null) {
            final boolean wasOn = bitset.getAndSet(index);
            if (wasOn == false) {
                onBits++;
                // Once all bits are set, we can simply just return YES for all indexes.
                // This allows us to clear the internal bitset and use null check as the guard.
                // 代表位图被填满了  将位图置空
                if (onBits == bitset.length()) {
                    bitset = null;
                }
            }
        }
    }

    // Below methods are pkg-private for testing

    int cardinality() {
        return onBits;
    }

    int length() {
        return bitset == null ? onBits : bitset.length();
    }

    boolean isInternalBitsetReleased() {
        return bitset == null;
    }
}
