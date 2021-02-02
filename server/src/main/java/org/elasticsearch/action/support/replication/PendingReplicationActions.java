/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * 该对象会监控 副本组的变化
 */
public class PendingReplicationActions implements Consumer<ReplicationGroup>, Releasable {

    /**
     * 代表需要往哪些分片执行同步最新写入primary数据的任务
     */
    private final Map<String, Set<RetryableAction<?>>> onGoingReplicationActions = ConcurrentCollections.newConcurrentMap();
    private final ShardId shardId;
    private final ThreadPool threadPool;
    /**
     * 记录上一次消费副本组时的版本号
     */
    private volatile long replicationGroupVersion = -1;

    public PendingReplicationActions(ShardId shardId, ThreadPool threadPool) {
        this.shardId = shardId;
        this.threadPool = threadPool;
    }

    /**
     * 在主分片通过 ReplicationOperation 向副本发起请求时 会将任务添加到该容器中
     * @param allocationId    对应副本的id
     * @param replicationAction    索引操作支持重试
     */
    public void addPendingAction(String allocationId, RetryableAction<?> replicationAction) {
        Set<RetryableAction<?>> ongoingActionsOnNode = onGoingReplicationActions.get(allocationId);
        if (ongoingActionsOnNode != null) {
            // 因为针对同一个分片可以同时执行多个索引操作 比如同时发起多个请求 所以这里可以存在多个重试任务
            ongoingActionsOnNode.add(replicationAction);
            if (onGoingReplicationActions.containsKey(allocationId) == false) {
                replicationAction.cancel(new IndexShardClosedException(shardId,
                    "Replica unavailable - replica could have left ReplicationGroup or IndexShard might have closed"));
            }
        } else {

            // 只有tracked为true的分片才允许执行索引操作
            replicationAction.cancel(new IndexShardClosedException(shardId,
                "Replica unavailable - replica could have left ReplicationGroup or IndexShard might have closed"));
        }
    }

    public void removeReplicationAction(String allocationId, RetryableAction<?> action) {
        Set<RetryableAction<?>> ongoingActionsOnNode = onGoingReplicationActions.get(allocationId);
        if (ongoingActionsOnNode != null) {
            ongoingActionsOnNode.remove(action);
        }
    }

    /**
     *
     * @param replicationGroup  处理最新的副本组
     */
    @Override
    public void accept(ReplicationGroup replicationGroup) {
        // 通过版本号解决幂等问题
        if (isNewerVersion(replicationGroup)) {
            synchronized (this) {
                if (isNewerVersion(replicationGroup)) {
                    // 更新需要持续同步数据的分片
                    acceptNewTrackedAllocationIds(replicationGroup.getTrackedAllocationIds());
                    replicationGroupVersion = replicationGroup.getVersion();
                }
            }
        }
    }

    private boolean isNewerVersion(ReplicationGroup replicationGroup) {
        // Relative comparison to mitigate long overflow
        return replicationGroup.getVersion() - replicationGroupVersion > 0;
    }

    /**
     * 代表需要同步leader最新写入的数据的分片发生了变化
     * @param trackedAllocationIds
     */
    synchronized void acceptNewTrackedAllocationIds(Set<String> trackedAllocationIds) {
        for (String targetAllocationId : trackedAllocationIds) {
            onGoingReplicationActions.putIfAbsent(targetAllocationId, ConcurrentCollections.newConcurrentSet());
        }
        ArrayList<Set<RetryableAction<?>>> toCancel = new ArrayList<>();
        // 将不需要继续维护同步的分片移除
        for (String allocationId : onGoingReplicationActions.keySet()) {
            if (trackedAllocationIds.contains(allocationId) == false) {
                toCancel.add(onGoingReplicationActions.remove(allocationId));
            }
        }

        // value是个可重试任务 要进行关闭
        cancelActions(toCancel, "Replica left ReplicationGroup");
    }

    @Override
    public synchronized void close() {
        ArrayList<Set<RetryableAction<?>>> toCancel = new ArrayList<>(onGoingReplicationActions.values());
        onGoingReplicationActions.clear();

        cancelActions(toCancel, "Primary closed.");
    }

    /**
     * 因为这组allocation 已经被移除了所以相关的任务都要被关闭
     * @param toCancel
     * @param message
     */
    private void cancelActions(ArrayList<Set<RetryableAction<?>>> toCancel, String message) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> toCancel.stream()
            .flatMap(Collection::stream)
            .forEach(action -> action.cancel(new IndexShardClosedException(shardId, message))));
    }
}
