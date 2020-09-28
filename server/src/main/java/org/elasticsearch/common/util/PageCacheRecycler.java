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

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.recycler.AbstractRecyclerC;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.common.recycler.Recyclers.concurrent;
import static org.elasticsearch.common.recycler.Recyclers.concurrentDeque;
import static org.elasticsearch.common.recycler.Recyclers.dequeFactory;
import static org.elasticsearch.common.recycler.Recyclers.none;

/**
 * A recycler of fixed-size pages.
 * 缓存数据的对象 这里的数据页指的就是存储数据的数组
 */
public class PageCacheRecycler {

    public static final Setting<Type> TYPE_SETTING =
        new Setting<>("cache.recycler.page.type", Type.CONCURRENT.name(), Type::parse, Property.NodeScope);
    public static final Setting<ByteSizeValue> LIMIT_HEAP_SETTING  =
        Setting.memorySizeSetting("cache.recycler.page.limit.heap", "10%", Property.NodeScope);
    public static final Setting<Double> WEIGHT_BYTES_SETTING  =
        Setting.doubleSetting("cache.recycler.page.weight.bytes", 1d, 0d, Property.NodeScope);
    public static final Setting<Double> WEIGHT_LONG_SETTING  =
        Setting.doubleSetting("cache.recycler.page.weight.longs", 1d, 0d, Property.NodeScope);
    public static final Setting<Double> WEIGHT_INT_SETTING  =
        Setting.doubleSetting("cache.recycler.page.weight.ints", 1d, 0d, Property.NodeScope);
    // object pages are less useful to us so we give them a lower weight by default
    public static final Setting<Double> WEIGHT_OBJECTS_SETTING  =
        Setting.doubleSetting("cache.recycler.page.weight.objects", 0.1d, 0d, Property.NodeScope);

    /** Page size in bytes: 16KB */
    public static final int PAGE_SIZE_IN_BYTES = 1 << 14;
    public static final int OBJECT_PAGE_SIZE = PAGE_SIZE_IN_BYTES / RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    public static final int LONG_PAGE_SIZE = PAGE_SIZE_IN_BYTES / Long.BYTES;
    public static final int INT_PAGE_SIZE = PAGE_SIZE_IN_BYTES / Integer.BYTES;
    public static final int BYTE_PAGE_SIZE = PAGE_SIZE_IN_BYTES;

    /**
     * 利用了对象池技术  在网络连接框架中经常可以看到对象池技术 比如netty自带的recycle对象 tomcat的对象池
     */
    private final Recycler<byte[]> bytePage;
    private final Recycler<int[]> intPage;
    private final Recycler<long[]> longPage;
    private final Recycler<Object[]> objectPage;

    public static final PageCacheRecycler NON_RECYCLING_INSTANCE;

    static {
        NON_RECYCLING_INSTANCE = new PageCacheRecycler(Settings.builder().put(LIMIT_HEAP_SETTING.getKey(), "0%").build());
    }

    /**
     * 使用配置对象 创建 pageCacheRecycler对象
     * @param settings
     */
    public PageCacheRecycler(Settings settings) {
        // 默认使用基于分段锁的recycle对象
        final Type type = TYPE_SETTING.get(settings);
        // 对象池允许使用的堆大小
        final long limit = LIMIT_HEAP_SETTING.get(settings).getBytes();
        // 获取进程数  recycle对象如果是基于分段锁 会参考该值
        final int allocatedProcessors = EsExecutors.allocatedProcessors(settings);

        // We have a global amount of memory that we need to divide across data types.
        // Since some types are more useful than other ones we give them different weights.
        // Trying to store all of them in a single stack would be problematic because eg.
        // a work load could fill the recycler with only byte[] pages and then another
        // workload that would work with double[] pages couldn't recycle them because there
        // is no space left in the stack/queue. LRU/LFU policies are not an option either
        // because they would make obtain/release too costly: we really need constant-time
        // operations.
        // Ultimately a better solution would be to only store one kind of data and have the
        // ability to interpret it either as a source of bytes, doubles, longs, etc. eg. thanks
        // to direct ByteBuffers or sun.misc.Unsafe on a byte[] but this would have other issues
        // that would need to be addressed such as garbage collection of native memory or safety
        // of Unsafe writes.
        // 为每种类型数据分配的大小权重
        final double bytesWeight = WEIGHT_BYTES_SETTING .get(settings);
        final double intsWeight = WEIGHT_INT_SETTING .get(settings);
        final double longsWeight = WEIGHT_LONG_SETTING .get(settings);
        final double objectsWeight = WEIGHT_OBJECTS_SETTING .get(settings);

        final double totalWeight = bytesWeight + intsWeight + longsWeight + objectsWeight;
        // 最多允许使用多少数据页 每个数据页对应一个长度为BYTE_PAGE_SIZE的数组对象
        final int maxPageCount = (int) Math.min(Integer.MAX_VALUE, limit / PAGE_SIZE_IN_BYTES);

        // 根据权重创建不同的数据页
        final int maxBytePageCount = (int) (bytesWeight * maxPageCount / totalWeight);
        bytePage = build(type, maxBytePageCount, allocatedProcessors,
            // 这里定义了创建/回收数据的逻辑
            new AbstractRecyclerC<byte[]>() {
            @Override
            public byte[] newInstance() {
                return new byte[BYTE_PAGE_SIZE];
            }
            @Override
            public void recycle(byte[] value) {
                // nothing to do
                // 因为基于堆内存 会被GC回收 所以不需要做处理
            }
        });

        final int maxIntPageCount = (int) (intsWeight * maxPageCount / totalWeight);
        intPage = build(type, maxIntPageCount, allocatedProcessors, new AbstractRecyclerC<int[]>() {
            @Override
            public int[] newInstance() {
                return new int[INT_PAGE_SIZE];
            }
            @Override
            public void recycle(int[] value) {
                // nothing to do
            }
        });

        final int maxLongPageCount = (int) (longsWeight * maxPageCount / totalWeight);
        longPage = build(type, maxLongPageCount, allocatedProcessors, new AbstractRecyclerC<long[]>() {
            @Override
            public long[] newInstance() {
                return new long[LONG_PAGE_SIZE];
            }
            @Override
            public void recycle(long[] value) {
                // nothing to do
            }
        });

        final int maxObjectPageCount = (int) (objectsWeight * maxPageCount / totalWeight);
        objectPage = build(type, maxObjectPageCount, allocatedProcessors, new AbstractRecyclerC<Object[]>() {
            @Override
            public Object[] newInstance() {
                return new Object[OBJECT_PAGE_SIZE];
            }
            @Override
            public void recycle(Object[] value) {
                Arrays.fill(value, null); // we need to remove the strong refs on the objects stored in the array
            }
        });

        assert PAGE_SIZE_IN_BYTES * (maxBytePageCount + maxIntPageCount + maxLongPageCount + maxObjectPageCount) <= limit;
    }

    public Recycler.V<byte[]> bytePage(boolean clear) {
        final Recycler.V<byte[]> v = bytePage.obtain();
        if (v.isRecycled() && clear) {
            Arrays.fill(v.v(), (byte) 0);
        }
        return v;
    }

    public Recycler.V<int[]> intPage(boolean clear) {
        final Recycler.V<int[]> v = intPage.obtain();
        if (v.isRecycled() && clear) {
            Arrays.fill(v.v(), 0);
        }
        return v;
    }

    public Recycler.V<long[]> longPage(boolean clear) {
        final Recycler.V<long[]> v = longPage.obtain();
        if (v.isRecycled() && clear) {
            Arrays.fill(v.v(), 0L);
        }
        return v;
    }

    public Recycler.V<Object[]> objectPage() {
        // object pages are cleared on release anyway
        return objectPage.obtain();
    }

    /**
     * 构建数据页对象
     * @param type  recycle类型
     * @param limit
     * @param availableProcessors
     * @param c
     * @param <T>
     * @return
     */
    private static <T> Recycler<T> build(Type type, int limit, int availableProcessors, Recycler.C<T> c) {
        final Recycler<T> recycler;
        if (limit == 0) {
            recycler = none(c);
        } else {
            recycler = type.build(c, limit, availableProcessors);
        }
        return recycler;
    }

    /**
     * 代表使用的对象池类型
     */
    public enum Type {
        QUEUE {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return concurrentDeque(c, limit);
            }
        },
        CONCURRENT {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                // limit / availableProcessors 代表队列的长度信息
                // dequeFactory 创建的仅是基于普通双端队列的 recycle对象 不具备线程安全
                // concurrent 赋予 普通回收对象线程安全的能力
                // concurrent() 使用了分段锁的思想 降低每个recycle的竞争 借此提高性能
                return concurrent(dequeFactory(c, limit / availableProcessors), availableProcessors);
            }
        },
        NONE {
            @Override
            <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors) {
                return none(c);
            }
        };

        public static Type parse(String type) {
            try {
                return Type.valueOf(type.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("no type support [" + type + "]");
            }
        }

        abstract <T> Recycler<T> build(Recycler.C<T> c, int limit, int availableProcessors);
    }
}
