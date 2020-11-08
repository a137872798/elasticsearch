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

package org.elasticsearch.index.bulk.stats;

import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.index.shard.IndexShard;

import java.util.concurrent.TimeUnit;

/**
 * Internal class that maintains relevant shard bulk statistics / metrics.
 * @see IndexShard
 * bulk事件指的是什么  该对象也是基于监听器钩子 进行数据统计
 */
public class ShardBulkStats implements BulkOperationListener {

    private final StatsHolder totalStats = new StatsHolder();
    private static final double ALPHA = 0.1;

    public BulkStats stats() {
        return totalStats.stats();
    }

    @Override
    public void afterBulk(long shardBulkSizeInBytes, long tookInNanos) {
        totalStats.totalSizeInBytes.inc(shardBulkSizeInBytes);
        totalStats.shardBulkMetric.inc(tookInNanos);
        totalStats.timeInMillis.addValue(tookInNanos);
        totalStats.sizeInBytes.addValue(shardBulkSizeInBytes);
    }

    static final class StatsHolder {
        final MeanMetric shardBulkMetric = new MeanMetric();
        final CounterMetric totalSizeInBytes = new CounterMetric();
        ExponentiallyWeightedMovingAverage timeInMillis = new ExponentiallyWeightedMovingAverage(ALPHA, 0.0);
        ExponentiallyWeightedMovingAverage sizeInBytes = new ExponentiallyWeightedMovingAverage(ALPHA, 0.0);

        BulkStats stats() {
            return new BulkStats(
                shardBulkMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(shardBulkMetric.sum()),
                totalSizeInBytes.count(),
                TimeUnit.NANOSECONDS.toMillis((long) timeInMillis.getAverage()),
                (long) sizeInBytes.getAverage());
        }
    }
}
