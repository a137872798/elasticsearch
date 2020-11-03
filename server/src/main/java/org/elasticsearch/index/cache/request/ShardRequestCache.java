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

package org.elasticsearch.index.cache.request;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.metrics.CounterMetric;

/**
 * Tracks the portion of the request cache in use for a particular shard.
 * 该对象维护了各种统计信息
 */
public final class ShardRequestCache {

    final CounterMetric evictionsMetric = new CounterMetric();
    /**
     * 这是记录总的缓存占用多少bytes的
     */
    final CounterMetric totalMetric = new CounterMetric();
    final CounterMetric hitCount = new CounterMetric();
    final CounterMetric missCount = new CounterMetric();

    public RequestCacheStats stats() {
        return new RequestCacheStats(totalMetric.count(), evictionsMetric.count(), hitCount.count(), missCount.count());
    }

    public void onHit() {
        hitCount.inc();
    }

    public void onMiss() {
        missCount.inc();
    }

    public void onCached(Accountable key, BytesReference value) {
        totalMetric.inc(key.ramBytesUsed() + value.ramBytesUsed());
    }

    /**
     * 当缓存被移除时 从计数器中减去对应的数值
     * @param key
     * @param value
     * @param evicted
     */
    public void onRemoval(Accountable key, BytesReference value, boolean evicted) {
        if (evicted) {
            evictionsMetric.inc();
        }
        long dec = 0;
        if (key != null) {
            dec += key.ramBytesUsed();
        }
        if (value != null) {
            dec += value.ramBytesUsed();
        }
        totalMetric.dec(dec);
    }
}
