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

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class holds a collection of all on going recoveries on the current node (i.e., the node is the target node
 * of those recoveries). The class is used to guarantee concurrent semantics such that once a recoveries was done/cancelled/failed
 * no other thread will be able to find it. Last, the {@link RecoveryRef} inner class verifies that recovery temporary files
 * and store will only be cleared once on going usage is finished.
 * 每个target都对应集群中的一个节点 代表从这个节点获取数据并恢复本地节点
 */
public class RecoveriesCollection {

    /**
     * This is the single source of truth for ongoing recoveries. If it's not here, it was canceled or done
     * 管理此时 node下所有的replicaShard的恢复任务
     * key: recoveryId
     */
    private final ConcurrentMap<Long, RecoveryTarget> onGoingRecoveries = ConcurrentCollections.newConcurrentMap();

    private final Logger logger;
    private final ThreadPool threadPool;

    public RecoveriesCollection(Logger logger, ThreadPool threadPool) {
        this.logger = logger;
        this.threadPool = threadPool;
    }

    /**
     * Starts are new recovery for the given shard, source node and state
     *
     * @param indexShard 本次要恢复数据的分片
     * @param sourceNode 数据将会从该节点上获取
     * @param listener 当副本分片恢复完数据后 也会向leader节点上报 申请进入start状态
     * @return the id of the new recovery.
     * 本次要开始为某个shard恢复数据 记录在该对象中
     * 每个恢复任务 对应一个recoveryId
     */
    public long startRecovery(IndexShard indexShard, DiscoveryNode sourceNode,
                              PeerRecoveryTargetService.RecoveryListener listener, TimeValue activityTimeout) {
        // 该对象内部包含了完成恢复流程需要的所有逻辑
        RecoveryTarget recoveryTarget = new RecoveryTarget(indexShard, sourceNode, listener);
        // 开始执行恢复任务
        startRecoveryInternal(recoveryTarget, activityTimeout);
        return recoveryTarget.recoveryId();
    }

    /**
     * 在启动恢复操作时 会开启一个监控器  监控recovery任务是否正常运行
     * @param recoveryTarget
     * @param activityTimeout  恢复任务最多允许的耗时
     */
    private void startRecoveryInternal(RecoveryTarget recoveryTarget, TimeValue activityTimeout) {
        RecoveryTarget existingTarget = onGoingRecoveries.putIfAbsent(recoveryTarget.recoveryId(), recoveryTarget);
        assert existingTarget == null : "found two RecoveryStatus instances with the same id";
        logger.trace("{} started recovery from {}, id [{}]", recoveryTarget.shardId(), recoveryTarget.sourceNode(),
            recoveryTarget.recoveryId());
        threadPool.schedule(new RecoveryMonitor(recoveryTarget.recoveryId(), recoveryTarget.lastAccessTime(), activityTimeout),
            activityTimeout, ThreadPool.Names.GENERIC);
    }

    /**
     * Resets the recovery and performs a recovery restart on the currently recovering index shard
     *
     * @see IndexShard#performRecoveryRestart()
     * @return newly created RecoveryTarget
     * 重启某个恢复操作
     */
    public RecoveryTarget resetRecovery(final long recoveryId, final TimeValue activityTimeout) {
        RecoveryTarget oldRecoveryTarget = null;
        final RecoveryTarget newRecoveryTarget;

        try {
            synchronized (onGoingRecoveries) {
                // swap recovery targets in a synchronized block to ensure that the newly added recovery target is picked up by
                // cancelRecoveriesForShard whenever the old recovery target is picked up
                oldRecoveryTarget = onGoingRecoveries.remove(recoveryId);
                if (oldRecoveryTarget == null) {
                    return null;
                }

                // 生成一个新的恢复对象 同时开始监控这个新对象
                newRecoveryTarget = oldRecoveryTarget.retryCopy();
                startRecoveryInternal(newRecoveryTarget, activityTimeout);
            }

            // Closes the current recovery target
            // 将之前的 recoveryState 重置成 init    当从该方法返回时 已经从阻塞状态解除了
            boolean successfulReset = oldRecoveryTarget.resetRecovery(newRecoveryTarget.cancellableThreads());
            if (successfulReset) {
                logger.trace("{} restarted recovery from {}, id [{}], previous id [{}]", newRecoveryTarget.shardId(),
                    newRecoveryTarget.sourceNode(), newRecoveryTarget.recoveryId(), oldRecoveryTarget.recoveryId());
                return newRecoveryTarget;
            } else {
                logger.trace("{} recovery could not be reset as it is already cancelled, recovery from {}, id [{}], previous id [{}]",
                    newRecoveryTarget.shardId(), newRecoveryTarget.sourceNode(), newRecoveryTarget.recoveryId(),
                    oldRecoveryTarget.recoveryId());
                cancelRecovery(newRecoveryTarget.recoveryId(), "recovery cancelled during reset");
                return null;
            }
        } catch (Exception e) {
            // fail shard to be safe
            oldRecoveryTarget.notifyListener(new RecoveryFailedException(oldRecoveryTarget.state(), "failed to retry recovery", e), true);
            return null;
        }
    }

    public RecoveryTarget getRecoveryTarget(long id) {
        return onGoingRecoveries.get(id);
    }

    /**
     * gets the {@link RecoveryTarget } for a given id. The RecoveryStatus returned has it's ref count already incremented
     * to make sure it's safe to use. However, you must call {@link RecoveryTarget#decRef()} when you are done with it, typically
     * by using this method in a try-with-resources clause.
     * <p>
     * Returns null if recovery is not found
     * 包装某个恢复任务
     */
    public RecoveryRef getRecovery(long id) {
        RecoveryTarget status = onGoingRecoveries.get(id);
        if (status != null && status.tryIncRef()) {
            // 在获取该对象时增加引用计数 同时在返回时减少引用计数
            return new RecoveryRef(status);
        }
        return null;
    }

    /** Similar to {@link #getRecovery(long)} but throws an exception if no recovery is found */
    public RecoveryRef getRecoverySafe(long id, ShardId shardId) {
        RecoveryRef recoveryRef = getRecovery(id);
        if (recoveryRef == null) {
            throw new IndexShardClosedException(shardId);
        }
        assert recoveryRef.target().shardId().equals(shardId);
        return recoveryRef;
    }

    /**
     * cancel the recovery with the given id (if found) and remove it from the recovery collection
     * 一般是当某个恢复操作已经完成时触发
     */
    public boolean cancelRecovery(long id, String reason) {
        RecoveryTarget removed = onGoingRecoveries.remove(id);
        boolean cancelled = false;
        if (removed != null) {
            logger.trace("{} canceled recovery from {}, id [{}] (reason [{}])",
                    removed.shardId(), removed.sourceNode(), removed.recoveryId(), reason);
            removed.cancel(reason);
            cancelled = true;
        }
        return cancelled;
    }

    /**
     * fail the recovery with the given id (if found) and remove it from the recovery collection
     *
     * @param id               id of the recovery to fail
     * @param e                exception with reason for the failure
     * @param sendShardFailure true a shard failed message should be sent to the master
     *                         某次恢复任务失败   可能是在一定的时间间隔后 没有获取到更新的恢复用数据
     */
    public void failRecovery(long id, RecoveryFailedException e, boolean sendShardFailure) {
        RecoveryTarget removed = onGoingRecoveries.remove(id);
        if (removed != null) {
            logger.trace("{} failing recovery from {}, id [{}]. Send shard failure: [{}]", removed.shardId(), removed.sourceNode(),
                removed.recoveryId(), sendShardFailure);
            removed.fail(e, sendShardFailure);
        }
    }

    /**
     * mark the recovery with the given id as done (if found)
     * 标记某个分片已经完成了数据恢复
     */
    public void markRecoveryAsDone(long id) {
        RecoveryTarget removed = onGoingRecoveries.remove(id);
        if (removed != null) {
            logger.trace("{} marking recovery from {} as done, id [{}]", removed.shardId(), removed.sourceNode(), removed.recoveryId());
            // 修改recoveryState的状态为已完成
            removed.markAsDone();
        }
    }

    /** the number of ongoing recoveries */
    public int size() {
        return onGoingRecoveries.size();
    }

    /**
     * cancel all ongoing recoveries for the given shard
     *
     * @param reason       reason for cancellation
     * @param shardId      shardId for which to cancel recoveries
     * @return true if a recovery was cancelled
     * 针对同一个分片允许同时存在多个 recoveryId 吗???
     */
    public boolean cancelRecoveriesForShard(ShardId shardId, String reason) {
        boolean cancelled = false;
        List<RecoveryTarget> matchedRecoveries = new ArrayList<>();
        synchronized (onGoingRecoveries) {
            for (Iterator<RecoveryTarget> it = onGoingRecoveries.values().iterator(); it.hasNext(); ) {
                RecoveryTarget status = it.next();
                if (status.shardId().equals(shardId)) {
                    matchedRecoveries.add(status);
                    it.remove();
                }
            }
        }
        for (RecoveryTarget removed : matchedRecoveries) {
            logger.trace("{} canceled recovery from {}, id [{}] (reason [{}])",
                removed.shardId(), removed.sourceNode(), removed.recoveryId(), reason);
            removed.cancel(reason);
            cancelled = true;
        }
        return cancelled;
    }

    /**
     * a reference to {@link RecoveryTarget}, which implements {@link AutoCloseable}. closing the reference
     * causes {@link RecoveryTarget#decRef()} to be called. This makes sure that the underlying resources
     * will not be freed until {@link RecoveryRef#close()} is called.
     */
    public static class RecoveryRef implements AutoCloseable {

        private final RecoveryTarget status;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        /**
         * Important: {@link RecoveryTarget#tryIncRef()} should
         * be *successfully* called on status before
         */
        public RecoveryRef(RecoveryTarget status) {
            this.status = status;
            this.status.setLastAccessTime();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                status.decRef();
            }
        }

        public RecoveryTarget target() {
            return status;
        }
    }

    /**
     * 监控某个恢复任务
     */
    private class RecoveryMonitor extends AbstractRunnable {

        /**
         * 被监控的恢复任务对应的id
         * 被监控的恢复任务对应的id
         */
        private final long recoveryId;

        /**
         * 每次触发检查的时间间隔
         */
        private final TimeValue checkInterval;

        /**
         * 记录最近一次看到的 recoveryId对应的status对象记录的时间戳
         */
        private volatile long lastSeenAccessTime;

        private RecoveryMonitor(long recoveryId, long lastSeenAccessTime, TimeValue checkInterval) {
            this.recoveryId = recoveryId;
            this.checkInterval = checkInterval;
            this.lastSeenAccessTime = lastSeenAccessTime;
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected error while monitoring recovery [{}]", recoveryId), e);
        }

        /**
         * 每间隔一定时间检测 recovery任务的运行情况
         * @throws Exception
         */
        @Override
        protected void doRun() throws Exception {
            RecoveryTarget status = onGoingRecoveries.get(recoveryId);

            // 代表恢复任务已经完成
            if (status == null) {
                logger.trace("[monitor] no status found for [{}], shutting down", recoveryId);
                return;
            }

            long accessTime = status.lastAccessTime();
            // 代表在一定时间内 没有拉取到新的数据
            if (accessTime == lastSeenAccessTime) {
                String message = "no activity after [" + checkInterval + "]";
                failRecovery(recoveryId,
                        new RecoveryFailedException(status.state(), message, new ElasticsearchTimeoutException(message)),
                        // 代表失败信息需要通知到 leader节点
                        true // to be safe, we don't know what go stuck
                );
                return;
            }
            lastSeenAccessTime = accessTime;
            logger.trace("[monitor] rescheduling check for [{}]. last access time is [{}]", recoveryId, lastSeenAccessTime);
            threadPool.schedule(this, checkInterval, ThreadPool.Names.GENERIC);
        }
    }

}

