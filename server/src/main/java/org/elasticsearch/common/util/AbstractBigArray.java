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
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.recycler.Recycler;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Common implementation for array lists that slice data into fixed-size blocks.
 * 这里存储了一些公共方法 以及 通过recycle 获取/回收数据块的逻辑
 */
abstract class AbstractBigArray extends AbstractArray {

    private final PageCacheRecycler recycler;

    /**
     * 存储可以释放之前申请的内存块的句柄
     */
    private Recycler.V<?>[] cache;

    private final int pageShift;
    private final int pageMask;
    protected long size;

    /**
     *
     * @param pageSize  总计有多少个数据页
     * @param bigArrays
     * @param clearOnResize
     */
    protected AbstractBigArray(int pageSize, BigArrays bigArrays, boolean clearOnResize) {
        super(bigArrays, clearOnResize);
        this.recycler = bigArrays.recycler;
        if (pageSize < 128) {
            throw new IllegalArgumentException("pageSize must be >= 128");
        }
        if ((pageSize & (pageSize - 1)) != 0) {
            throw new IllegalArgumentException("pageSize must be a power of two");
        }
        this.pageShift = Integer.numberOfTrailingZeros(pageSize);
        this.pageMask = pageSize - 1;
        size = 0;

        // 初始状态创建大小为16的句柄数组
        if (this.recycler != null) {
            cache = new Recycler.V<?>[16];
        } else {
            cache = null;
        }
    }

    /**
     * 在初始化大小时 会计算有多少个page
     * @param capacity
     * @return
     */
    final int numPages(long capacity) {
        final long numPages = (capacity + pageMask) >>> pageShift;
        if (numPages > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pageSize=" + (pageMask + 1) + " is too small for such as capacity: " + capacity);
        }
        return (int) numPages;
    }

    final int pageSize() {
        return pageMask + 1;
    }

    final int pageIndex(long index) {
        return (int) (index >>> pageShift);
    }

    final int indexInPage(long index) {
        return (int) (index & pageMask);
    }

    @Override
    public final long size() {
        return size;
    }

    public abstract void resize(long newSize);

    protected abstract int numBytesPerElement();

    @Override
    public final long ramBytesUsed() {
        return ramBytesEstimated(size);
    }

    /** Given the size of the array, estimate the number of bytes it will use. */
    public final long ramBytesEstimated(final long size) {
        // rough approximate, we only take into account the size of the values, not the overhead of the array objects
        return ((long) pageIndex(size - 1) + 1) * pageSize() * numBytesPerElement();
    }

    /**
     * @param array
     * @param minSize
     * @param <T>
     * @return
     */
    private static <T> T[] grow(T[] array, int minSize) {
        if (array.length < minSize) {
            // 在分配大小时 会考虑虚拟机的运行环境  比如在64位环境下  每次尽可能分配8byte的倍数  minSize 代表要分配多少个单位长度 NUM_BYTES_OBJECT_REF 对应每个单位长度的byte
            final int newLen = ArrayUtil.oversize(minSize, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            array = Arrays.copyOf(array, newLen);
        }
        return array;
    }

    /**
     * 这里是将回收每个page内部数据的 句柄存起来
     * @param v
     * @param page
     * @param expectedSize  某页下 V内部数组 对应的大小
     * @param <T>
     * @return
     */
    private <T> T registerNewPage(Recycler.V<T> v, int page, int expectedSize) {
        // 为了避免缓存大小不足 进行扩容
        cache = grow(cache, page + 1);
        assert cache[page] == null;
        cache[page] = v;
        assert Array.getLength(v.v()) == expectedSize;
        return v.v();
    }

    /**
     * 指定数据页 并生成数组对象
     * @param page
     * @return
     */
    protected final byte[] newBytePage(int page) {
        // 如果存在循环对象的话 先尝试从该对象中获取
        if (recycler != null) {
            // 当之前没有可复用内存块时 新创建的内存块大小默认就是BYTE_PAGE_SIZE 所以下面在register是默认传入的就是BYTE_PAGE_SIZE
            final Recycler.V<byte[]> v = recycler.bytePage(clearOnResize);
            return registerNewPage(v, page, PageCacheRecycler.BYTE_PAGE_SIZE);
        } else {
            return new byte[PageCacheRecycler.BYTE_PAGE_SIZE];
        }
    }

    protected final int[] newIntPage(int page) {
        if (recycler != null) {
            final Recycler.V<int[]> v = recycler.intPage(clearOnResize);
            return registerNewPage(v, page, PageCacheRecycler.INT_PAGE_SIZE);
        } else {
            return new int[PageCacheRecycler.INT_PAGE_SIZE];
        }
    }

    protected final long[] newLongPage(int page) {
        if (recycler != null) {
            final Recycler.V<long[]> v = recycler.longPage(clearOnResize);
            return registerNewPage(v, page, PageCacheRecycler.LONG_PAGE_SIZE);
        } else {
            return new long[PageCacheRecycler.LONG_PAGE_SIZE];
        }
    }

    protected final Object[] newObjectPage(int page) {
        if (recycler != null) {
            final Recycler.V<Object[]> v = recycler.objectPage();
            return registerNewPage(v, page, PageCacheRecycler.OBJECT_PAGE_SIZE);
        } else {
            return new Object[PageCacheRecycler.OBJECT_PAGE_SIZE];
        }
    }

    /**
     * 将对象通过recycle回收
     * @param page
     */
    protected final void releasePage(int page) {
        if (recycler != null) {
            cache[page].close();
            cache[page] = null;
        }
    }

    /**
     * 在关闭该对象时 将所有分配的数组都通过 recycle维护
     */
    @Override
    protected final void doClose() {
        if (recycler != null) {
            Releasables.close(cache);
            cache = null;
        }
    }

}
