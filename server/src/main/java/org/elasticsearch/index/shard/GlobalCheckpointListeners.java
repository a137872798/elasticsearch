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

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Assertions;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Represents a collection of global checkpoint listeners. This collection can be added to, and all listeners present at the time of an
 * update will be notified together. All listeners will be notified when the shard is closed.
 * 每个indexShard对应一个该对象   当感知到indexShard对应replicationTracker的globalCheckpoint发生了变化 会触发相关函数
 */
public class GlobalCheckpointListeners implements Closeable {

    /**
     * A global checkpoint listener consisting of a callback that is notified when the global checkpoint is updated or the shard is closed.
     */
    public interface GlobalCheckpointListener {

        /**
         * The executor on which the listener is notified.
         *
         * @return the executor
         * 在监听到相关操作时 可以委托给不同的执行者执行任务
         */
        Executor executor();

        /**
         * Callback when the global checkpoint is updated or the shard is closed. If the shard is closed, the value of the global checkpoint
         * will be set to {@link org.elasticsearch.index.seqno.SequenceNumbers#UNASSIGNED_SEQ_NO} and the exception will be non-null and an
         * instance of {@link IndexShardClosedException }. If the listener timed out waiting for notification then the exception will be
         * non-null and an instance of {@link TimeoutException}. If the global checkpoint is updated, the exception will be null.
         *
         * @param globalCheckpoint the updated global checkpoint   本次更新的全局检查点的位置
         * @param e                if non-null, the shard is closed or the listener timed out
         */
        void accept(long globalCheckpoint, Exception e);

    }

    // guarded by this  这组监听器是否被关闭
    private boolean closed;
    /**
     * 每个监听器都会对应一个 tuple对象 long应该就是全局检查点 而通过executor执行的临时结果对象应该就是 future对象
     */
    private final Map<GlobalCheckpointListener, Tuple<Long, ScheduledFuture<?>>> listeners = new LinkedHashMap<>();

    /**
     * 在初始阶段 还不清楚全局检查点
     */
    private long lastKnownGlobalCheckpoint = UNASSIGNED_SEQ_NO;

    /**
     * 表明监听的是哪个分片
     */
    private final ShardId shardId;
    private final ScheduledExecutorService scheduler;
    private final Logger logger;

    /**
     * Construct a global checkpoint listeners collection.
     *
     * @param shardId   the shard ID on which global checkpoint updates can be listened to
     * @param scheduler the executor used for scheduling timeouts
     * @param logger    a shard-level logger
     */
    GlobalCheckpointListeners(
        final ShardId shardId,
        final ScheduledExecutorService scheduler,
        final Logger logger) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.logger = Objects.requireNonNull(logger, "logger");
    }

    /**
     * Add a global checkpoint listener. If the global checkpoint is equal to or above the global checkpoint the listener is waiting for,
     * then the listener will be asynchronously notified on the executor used to construct this collection of global checkpoint listeners.
     * If the shard is closed then the listener will be asynchronously notified on the executor used to construct this collection of global
     * checkpoint listeners. The listener will only be notified of at most one event, either the global checkpoint is updated above the
     * global checkpoint the listener is waiting for, or the shard is closed. A listener must re-register after one of these events to
     * receive subsequent events. Callers may add a timeout to be notified after if the timeout elapses. In this case, the listener will be
     * notified with a {@link TimeoutException}. Passing null fo the timeout means no timeout will be associated to the listener.
     *
     * @param waitingForGlobalCheckpoint the current global checkpoint known to the listener
     *                                   代表监听器此时知道的全局偏移量  只有到触发时的偏移量更大才能执行监听器
     * @param listener                   the listener
     * @param timeout                    the listener timeout, or null if no timeout
     *                                   每个监听对象有一个等待时间  要求在时间内必须触发一次  也就是要在这段时间内感知到全局检查点
     *                                   往该总控对象中追加一个新的全局检查点监听器
     */
    synchronized void add(final long waitingForGlobalCheckpoint, final GlobalCheckpointListener listener, final TimeValue timeout) {
        if (closed) {
            // 当本对象已经被关闭时 直接触发监听器  注意没有加入到listeners 中
            notifyListener(listener, UNASSIGNED_SEQ_NO, new IndexShardClosedException(shardId));
            return;
        }
        // 代表此时已经获取到了更大的检查点  所以可以直接触发监听器
        if (lastKnownGlobalCheckpoint >= waitingForGlobalCheckpoint) {
            // notify directly
            notifyListener(listener, lastKnownGlobalCheckpoint, null);
        } else {
            // 代表没有时间限制
            if (timeout == null) {
                listeners.put(listener, Tuple.tuple(waitingForGlobalCheckpoint, null));
            } else {
                listeners.put(
                    listener,
                    Tuple.tuple(
                        waitingForGlobalCheckpoint,
                        // 在一定延时后 通过一个UNASSIGNED_SEQ_NO 触发监听器
                        // 代表在这段时间内没有检测到更大的checkpoint
                        scheduler.schedule(
                            () -> {
                                final boolean removed;
                                synchronized (this) {
                                    /*
                                     * We know that this listener has a timeout associated with it (otherwise we would not be
                                     * here) so the future component of the return value from remove being null is an indication
                                     * that we are not in the map. This can happen if a notification collected us into listeners
                                     * to be notified and removed us from the map, and then our scheduled execution occurred
                                     * before we could be cancelled by the notification. In this case, our listener here would
                                     * not be in the map and we should not fire the timeout logic.
                                     */
                                    removed = listeners.remove(listener) != null;
                                }
                                if (removed) {
                                    final TimeoutException e = new TimeoutException(timeout.getStringRep());
                                    logger.trace("global checkpoint listener timed out", e);
                                    notifyListener(listener, UNASSIGNED_SEQ_NO, e);
                                }
                            },
                            timeout.nanos(),
                            TimeUnit.NANOSECONDS)));
            }
        }
    }

    /**
     * 当本对象被关闭时 触发所有监听器
     *
     * @throws IOException
     */
    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            assert listeners.isEmpty() : listeners;
        }
        closed = true;
        notifyListeners(UNASSIGNED_SEQ_NO, new IndexShardClosedException(shardId));
    }

    /**
     * The number of listeners currently pending for notification.
     *
     * @return the number of listeners pending notification
     */
    synchronized int pendingListeners() {
        return listeners.size();
    }

    /**
     * The scheduled future for a listener that has a timeout associated with it, otherwise null.
     *
     * @param listener the listener to get the scheduled future for
     * @return a scheduled future representing the timeout future for the listener, otherwise null
     * 获取某个监听器相关的超时任务
     */
    synchronized ScheduledFuture<?> getTimeoutFuture(final GlobalCheckpointListener listener) {
        return listeners.get(listener).v2();
    }

    /**
     * Invoke to notify all registered listeners of an updated global checkpoint.
     *
     * @param globalCheckpoint the updated global checkpoint
     *                         该对象负责监控全局检查点的变化
     */
    synchronized void globalCheckpointUpdated(final long globalCheckpoint) {
        assert globalCheckpoint >= NO_OPS_PERFORMED;
        assert globalCheckpoint > lastKnownGlobalCheckpoint
            : "updated global checkpoint [" + globalCheckpoint + "]"
            + " is not more than the last known global checkpoint [" + lastKnownGlobalCheckpoint + "]";
        // 更新当前已知的全局检查点
        lastKnownGlobalCheckpoint = globalCheckpoint;
        // 触发所有相关的监听器
        notifyListeners(globalCheckpoint, null);
    }

    /**
     * 每当全局检查点更新时 判断是否需要触发监听器
     *
     * @param globalCheckpoint
     * @param e                可能是由于异常情况关闭的  这时就会传入exception
     */
    private void notifyListeners(final long globalCheckpoint, final IndexShardClosedException e) {
        assert Thread.holdsLock(this) : Thread.currentThread();

        // early return if there are no listeners
        // 此时没有监听器 不需要做处理   在shard数据恢复阶段还没有设置监听器
        if (listeners.isEmpty()) {
            return;
        }

        final Map<GlobalCheckpointListener, Tuple<Long, ScheduledFuture<?>>> listenersToNotify;
        // 不是由于某种特殊情况
        if (globalCheckpoint != UNASSIGNED_SEQ_NO) {
            listenersToNotify =
                listeners
                    .entrySet()
                    .stream()
                    // 找到已感知的检查点小于本次传入的检查点的 监听器
                    .filter(entry -> entry.getValue().v1() <= globalCheckpoint)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            // 因为这些本次会被通知 所以可以从list中移除
            listenersToNotify.keySet().forEach(listeners::remove);
        } else {
            // 当发生特殊情况时  比如 close 那么就需要移除所有的监听器
            listenersToNotify = new HashMap<>(listeners);
            listeners.clear();
        }
        // 这里触发监听器
        if (listenersToNotify.isEmpty() == false) {
            listenersToNotify
                .forEach((listener, t) -> {
                    /*
                     * We do not want to interrupt any timeouts that fired, these will detect that the listener has been notified and not
                     * trigger the timeout.
                     * 关闭定时任务  因为原本超时的情况 会使用UNASSIGNED_SEQ_NO 触发监听器
                     * 在通知的外层增加了 synchronized 关键字 能够确保定时任务的不会被触发2次
                     */
                    FutureUtils.cancel(t.v2());
                    notifyListener(listener, globalCheckpoint, e);
                });
        }
    }

    /**
     * 触发监听器的相关钩子
     *
     * @param listener         处理全局检查点的相关监听器
     * @param globalCheckpoint 触发钩子所用的全局检查点
     * @param e
     */
    private void notifyListener(final GlobalCheckpointListener listener, final long globalCheckpoint, final Exception e) {
        assertNotification(globalCheckpoint, e);

        // 使用executor执行任务
        listener.executor().execute(() -> {
            try {
                listener.accept(globalCheckpoint, e);
            } catch (final Exception caught) {
                if (globalCheckpoint != UNASSIGNED_SEQ_NO) {
                    logger.warn(
                        new ParameterizedMessage(
                            "error notifying global checkpoint listener of updated global checkpoint [{}]",
                            globalCheckpoint),
                        caught);
                } else if (e instanceof IndexShardClosedException) {
                    logger.warn("error notifying global checkpoint listener of closed shard", caught);
                } else {
                    logger.warn("error notifying global checkpoint listener of timeout", caught);
                }
            }
        });
    }

    private void assertNotification(final long globalCheckpoint, final Exception e) {
        if (Assertions.ENABLED) {
            assert globalCheckpoint >= UNASSIGNED_SEQ_NO : globalCheckpoint;
            if (globalCheckpoint != UNASSIGNED_SEQ_NO) {
                assert e == null : e;
            } else {
                assert e != null;
                assert e instanceof IndexShardClosedException || e instanceof TimeoutException : e;
            }
        }
    }

}
