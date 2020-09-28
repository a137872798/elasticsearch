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

package org.elasticsearch.common.recycler;


import java.util.Deque;

/**
 * A {@link Recycler} implementation based on a {@link Deque}. This implementation is NOT thread-safe.
 * 将对象存储在双端队列中  本对象本身不支持线程安全需要用户手动处理
 */
public class DequeRecycler<T> extends AbstractRecycler<T> {

    final Deque<T> deque;
    final int maxSize;

    /**
     *
     * @param c  定义了对象的创建和回收等逻辑
     * @param queue
     * @param maxSize
     */
    public DequeRecycler(C<T> c, Deque<T> queue, int maxSize) {
        super(c);
        this.deque = queue;
        this.maxSize = maxSize;
    }

    @Override
    public V<T> obtain() {
        final T v = deque.pollFirst();
        if (v == null) {
            // 将对象包装成DV
            return new DV(c.newInstance(), false);
        }
        return new DV(v, true);
    }

    /** Called before releasing an object, returns true if the object should be recycled and false otherwise. */
    protected boolean beforeRelease() {
        return deque.size() < maxSize;
    }

    /** Called after a release. */
    protected void afterRelease(boolean recycled) {
        // nothing to do
    }

    /**
     * 该对象类似于一个 ValueHolder
     */
    private class DV implements Recycler.V<T> {

        T value;
        /**
         * 该数据是否从对象池中获取
         */
        final boolean recycled;

        DV(T value, boolean recycled) {
            this.value = value;
            this.recycled = recycled;
        }

        @Override
        public T v() {
            return value;
        }

        @Override
        public boolean isRecycled() {
            return recycled;
        }

        @Override
        public void close() {
            if (value == null) {
                throw new IllegalStateException("recycler entry already released...");
            }
            // 通过释放前的钩子决定是否回收该对象  比如针对双端队列 只要队列没有填满就可以进行回收
            final boolean recycle = beforeRelease();
            if (recycle) {
                // 回收对象
                c.recycle(value);
                // 这里将对象的回收 与 需要做的额外行为拆解开了???  为什么不在 recycle中加入到队列
                deque.addFirst(value);
            }
            else {
                c.destroy(value);
            }
            value = null;
            // 后置钩子
            afterRelease(recycle);
        }
    }
}
