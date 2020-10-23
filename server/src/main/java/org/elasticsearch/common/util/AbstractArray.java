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

import org.apache.lucene.util.Accountable;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;


abstract class AbstractArray implements BigArray {

    /**
     * 该对象是通过哪个  BigArrays分配的
     * 使用者一般通过 BigArrays 创建各种 BigArray对象 为了避免短时间内创建的内存太大 这里就会有一个限流机制
     */
    private final BigArrays bigArrays;
    /**
     * 每当通过recycle 获取之前回收的数组对象时 是否做清理操作
     */
    public final boolean clearOnResize;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    AbstractArray(BigArrays bigArrays, boolean clearOnResize) {
        this.bigArrays = bigArrays;
        this.clearOnResize = clearOnResize;
    }

    /**
     * 因为这个对象被关闭 就可以放宽限流器对其他用户创建 BigArray的限制
     */
    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                bigArrays.adjustBreaker(-ramBytesUsed(), true);
            } finally {
                doClose();
            }
        }
    }

    protected abstract void doClose();

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }
}
