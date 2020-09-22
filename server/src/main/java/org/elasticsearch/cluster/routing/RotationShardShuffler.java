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

import org.elasticsearch.common.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic {@link ShardShuffler} implementation that uses an {@link AtomicInteger} to generate seeds and uses a rotation to permute shards.
 * 这个实际上就是轮询啊  但是在 Shuffler层采用了更高的抽象 得到一个影响排序结果的seed 以及使用seed进行重排序的api
 */
public class RotationShardShuffler extends ShardShuffler {

    /**
     * 内部使用一个单调递增的seed
     */
    private final AtomicInteger seed;

    public RotationShardShuffler(int seed) {
        this.seed = new AtomicInteger(seed);
    }

    @Override
    public int nextSeed() {
        return seed.getAndIncrement();
    }

    @Override
    public List<ShardRouting> shuffle(List<ShardRouting> shards, int seed) {
        // 这里返回了一个 RotateList 实际上没有对元素做重排序 而是在基于index读取元素时增加了一个变量 (seed)
        return CollectionUtils.rotate(shards, seed);
    }

}
