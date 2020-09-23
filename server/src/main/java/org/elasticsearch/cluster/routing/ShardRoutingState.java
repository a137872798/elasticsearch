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


/**
 * Represents the current state of a {@link ShardRouting} as defined by the
 * cluster.
 * 分片路由状态
 */
public enum ShardRoutingState {
    /**
     * The shard is not assigned to any node.
     * 代表当前分片没有指派到任何节点   (分片需要先指定到哪个节点么  分片是由谁来创建的呢???  感觉像是有一个中控节点完成这些操作)
     */
    UNASSIGNED((byte) 1),
    /**
     * The shard is initializing (probably recovering from either a peer shard
     * or gateway).
     * 正在进行分片的初始化工作
     */
    INITIALIZING((byte) 2),
    /**
     * The shard is started.
     * 分片已启动
     */
    STARTED((byte) 3),
    /**
     * The shard is in the process being relocated.
     * 分片在重新定位  看来可能是增加新的节点引发的数据迁移
     */
    RELOCATING((byte) 4);

    private byte value;

    ShardRoutingState(byte value) {
        this.value = value;
    }

    /**
     * Byte value of this {@link ShardRoutingState}
     * @return Byte value of this {@link ShardRoutingState}
     */
    public byte value() {
        return this.value;
    }

    public static ShardRoutingState fromValue(byte value) {
        switch (value) {
            case 1:
                return UNASSIGNED;
            case 2:
                return INITIALIZING;
            case 3:
                return STARTED;
            case 4:
                return RELOCATING;
            default:
                throw new IllegalStateException("No routing state mapped for [" + value + "]");
        }
    }
}
