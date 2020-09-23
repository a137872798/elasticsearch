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
package org.elasticsearch.cluster.routing;

import java.util.List;

/**
 * Allows to iterate over unrelated shards.
 * 分片迭代器
 */
public interface ShardsIterator extends Iterable<ShardRouting> {

    /**
     * Resets the iterator to its initial state.
     * 应该是回到迭代器的头部
     */
    void reset();

    /**
     * The number of shard routing instances.
     *
     * @return number of shard routing instances in this iterator
     */
    int size();

    /**
     * The number of active shard routing instances
     *
     * @return number of active shard routing instances
     * 当前活跃的分片数量
     */
    int sizeActive();

    /**
     * Returns the next shard, or {@code null} if none available.
     * 尝试获取下一个分片 如果没有则返回null
     */
    ShardRouting nextOrNull();

    /**
     * Return the number of shards remaining in this {@link ShardsIterator}
     *
     * @return number of shard remaining
     * 迭代器中还有多少分片未遍历
     */
    int remaining();

    @Override
    int hashCode();

    @Override
    boolean equals(Object other);

    /**
     * Returns the {@link ShardRouting}s that this shards iterator holds.
     * 将迭代器内的数据 以list形式返回
     */
    List<ShardRouting> getShardRoutings();
}

