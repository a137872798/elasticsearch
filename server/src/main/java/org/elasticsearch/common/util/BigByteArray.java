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

package org.elasticsearch.common.util;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;

/**
 * Byte array abstraction able to support more than 2B values. This implementation slices data into fixed-sized blocks of
 * configurable length.
 * ByteArray的默认实现 表示一个支持扩容的大块数组
 */
final class BigByteArray extends AbstractBigArray implements ByteArray {

    /**
     * 这只是用于预估消耗多少byte的 使用一个默认的page
     */
    private static final BigByteArray ESTIMATOR = new BigByteArray(0, BigArrays.NON_RECYCLING_INSTANCE, false);

    /**
     * 大块数组对象实际上就是多个page 每个page对应一个byte[]
     */
    private byte[][] pages;

    /** Constructor. */
    BigByteArray(long size, BigArrays bigArrays, boolean clearOnResize) {
        super(BYTE_PAGE_SIZE, bigArrays, clearOnResize);
        this.size = size;
        pages = new byte[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newBytePage(i);
        }
    }

    /**
     * 向获取相关数据时 就是将index 转换成page下标 以及相对偏移量
     * @param index
     * @return
     */
    @Override
    public byte get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    @Override
    public byte set(long index, byte value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final byte[] page = pages[pageIndex];
        final byte ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    /**
     * 找到指定位置并将数据填充到 ref中
     * @param index
     * @param len
     * @param ref
     * @return
     */
    @Override
    public boolean get(long index, int len, BytesRef ref) {
        assert index + len <= size();
        int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        // 代表不需要换页
        if (indexInPage + len <= pageSize()) {
            // 更新ref 内部的数据
            ref.bytes = pages[pageIndex];
            ref.offset = indexInPage;
            ref.length = len;
            return false;
        } else {
            // 代表要读取至少2个页的数据 先将上个页剩余的部分读取出来 并循环不断读取下个页的数据 直到满足条件
            // 这样就没法直接维护引用了 必须重新创建一个数组
            ref.bytes = new byte[len];
            ref.offset = 0;
            // 代表总计要读取多少数据
            ref.length = pageSize() - indexInPage;
            // 先拷贝上个page 剩余的数据
            System.arraycopy(pages[pageIndex], indexInPage, ref.bytes, 0, ref.length);
            do {
                ++pageIndex;
                // 代表还需要拷贝多少数据
                final int copyLength = Math.min(pageSize(), len - ref.length);
                System.arraycopy(pages[pageIndex], 0, ref.bytes, ref.length, copyLength);
                ref.length += copyLength;
            } while (ref.length < len);
            return true;
        }
    }

    /**
     * 在set过程中 也要检测需要插入到第几页
     * @param index
     * @param buf
     * @param offset
     * @param len
     */
    @Override
    public void set(long index, byte[] buf, int offset, int len) {
        assert index + len <= size();
        int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        // 不需要换页的情况下 简单的数组拷贝就行
        if (indexInPage + len <= pageSize()) {
            System.arraycopy(buf, offset, pages[pageIndex], indexInPage, len);
        } else {
            int copyLen = pageSize() - indexInPage;
            System.arraycopy(buf, offset, pages[pageIndex], indexInPage, copyLen);
            do {
                ++pageIndex;
                offset += copyLen;
                len -= copyLen;
                copyLen = Math.min(len, pageSize());
                System.arraycopy(buf, offset, pages[pageIndex], 0, copyLen);
            } while (len > copyLen);
        }
    }

    @Override
    public void fill(long fromIndex, long toIndex, byte value) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException();
        }
        final int fromPage = pageIndex(fromIndex);
        final int toPage = pageIndex(toIndex - 1);
        if (fromPage == toPage) {
            Arrays.fill(pages[fromPage], indexInPage(fromIndex), indexInPage(toIndex - 1) + 1, value);
        } else {
            Arrays.fill(pages[fromPage], indexInPage(fromIndex), pages[fromPage].length, value);
            for (int i = fromPage + 1; i < toPage; ++i) {
                Arrays.fill(pages[i], value);
            }
            Arrays.fill(pages[toPage], 0, indexInPage(toIndex - 1) + 1, value);
        }
    }

    /**
     * BigByteArray中每个元素 都只占用一个byte
     * @return
     */
    @Override
    protected int numBytesPerElement() {
        return 1;
    }

    /**
     * Change the size of this array. Content between indexes <code>0</code> and <code>min(size(), newSize)</code> will be preserved.
     * @param newSize 根据传入的长度大小条件 pages
     */
    @Override
    public void resize(long newSize) {
        final int numPages = numPages(newSize);
        if (numPages > pages.length) {
            // 针对page数进行扩容
            pages = Arrays.copyOf(pages, ArrayUtil.oversize(numPages, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
        }
        // 生成新的page页内部的 byte[]
        for (int i = numPages - 1; i >= 0 && pages[i] == null; --i) {
            pages[i] = newBytePage(i);
        }
        // 如果本次是一个缩容操作 释放多余page内部的byte[] 并归还到recycle对象中
        for (int i = numPages; i < pages.length && pages[i] != null; ++i) {
            pages[i] = null;
            releasePage(i);
        }
        this.size = newSize;
    }

    /**
     * Estimates the number of bytes that would be consumed by an array of the given size.
     * @param size 预估申请这么大的size 会消耗多少 byte
     */
    public static long estimateRamBytes(final long size) {
        return ESTIMATOR.ramBytesEstimated(size);
    }

}
