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

import org.elasticsearch.Assertions;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Tracks shard operation permits. Each operation on the shard obtains a permit. When we need to block operations (e.g., to transition
 * between terms) we immediately delay all operations to a queue, obtain all available permits, and wait for outstanding operations to drain
 * and return their permits. Delayed operations will acquire permits and be completed after the operation that blocked all operations has
 * completed.
 * 分片的操作需要门票
 */
final class IndexShardOperationPermits implements Closeable {

    /**
     * 本次针对的某个分片
     */
    private final ShardId shardId;
    private final ThreadPool threadPool;

    static final int TOTAL_PERMITS = Integer.MAX_VALUE;
    /**
     * 初始状态下 门票数量是无限制的
     */
    final Semaphore semaphore = new Semaphore(TOTAL_PERMITS, true); // fair to ensure a blocking thread is not starved

    /**
     * 存储了一组延时操作 只是简单的bean对象
     */
    private final List<DelayedOperation> delayedOperations = new ArrayList<>(); // operations that are delayed
    private volatile boolean closed;
    /**
     * 如果不为0 就代表发起了一次 阻塞操作  这样之后尝试通过该对象执行的operations 都会被暂停
     */
    private int queuedBlockOperations; // does not need to be volatile as all accesses are done under a lock on this

    // only valid when assertions are enabled. Key is AtomicBoolean associated with each permit to ensure close once semantics.
    // Value is a tuple, with a some debug information supplied by the caller and a stack trace of the acquiring thread
    private final Map<AtomicBoolean, Tuple<String, StackTraceElement[]>> issuedPermits;

    /**
     * Construct operation permits for the specified shards.
     *
     * @param shardId    the shard
     * @param threadPool the thread pool (used to execute delayed operations)
     */
    IndexShardOperationPermits(final ShardId shardId, final ThreadPool threadPool) {
        this.shardId = shardId;
        this.threadPool = threadPool;
        if (Assertions.ENABLED) {
            issuedPermits = new ConcurrentHashMap<>();
        } else {
            issuedPermits = null;
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    /**
     * Wait for in-flight operations to finish and executes {@code onBlocked} under the guarantee that no new operations are started. Queues
     * operations that are occurring in the meanwhile and runs them once {@code onBlocked} has executed.
     *
     * @param timeout   the maximum time to wait for the in-flight operations block
     * @param timeUnit  the time unit of the {@code timeout} argument
     * @param onBlocked the action to run once the block has been acquired
     * @param <E>       the type of checked exception thrown by {@code onBlocked}
     * @throws InterruptedException      if calling thread is interrupted
     * @throws TimeoutException          if timed out waiting for in-flight operations to finish
     * @throws IndexShardClosedException if operation permit has been closed
     * 外部执行任务时 需要获取门票
     * 而该方法直接抢占了所有的门票 导致其他任务都被暂停
     */
    <E extends Exception> void blockOperations(
            final long timeout,
            final TimeUnit timeUnit,
            final CheckedRunnable<E> onBlocked  // 需要在所有operations都暂停的时候才方便执行的操作
    ) throws InterruptedException, TimeoutException, E {
        delayOperations();
        // 先获取门票 如果有其他线程抢走了门票就要阻塞
        try (Releasable ignored = acquireAll(timeout, timeUnit)) {
            onBlocked.run();
        } finally {
            releaseDelayedOperations();
        }
    }

    /**
     * Immediately delays operations and on another thread waits for in-flight operations to finish and then acquires all permits. When all
     * permits are acquired, the provided {@link ActionListener} is called under the guarantee that no new operations are started. Delayed
     * operations are run once the {@link Releasable} is released or if a failure occurs while acquiring all permits; in this case the
     * {@code onFailure} handler will be invoked after delayed operations are released.
     *
     * @param onAcquired {@link ActionListener} that is invoked once acquisition is successful or failed
     * @param timeout    the maximum time to wait for the in-flight operations block
     * @param timeUnit   the time unit of the {@code timeout} argument
     *                   异步执行一个需要在block模式下执行的操作
     */
    public void asyncBlockOperations(final ActionListener<Releasable> onAcquired, final long timeout, final TimeUnit timeUnit)  {
        // 增加一个阻塞操作
        delayOperations();
        threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {

            final RunOnce released = new RunOnce(() -> releaseDelayedOperations());

            @Override
            public void onFailure(final Exception e) {
                try {
                    // 失败时 也需要执行之前被阻塞的所有任务
                    released.run(); // resume delayed operations as soon as possible
                } finally {
                    onAcquired.onFailure(e);
                }
            }

            @Override
            protected void doRun() throws Exception {
                final Releasable releasable = acquireAll(timeout, timeUnit);
                // 传入的函数应该是由 onAcquired 来决定在什么时候触发的
                onAcquired.onResponse(() -> {
                    try {
                        // 释放门票
                        releasable.close();
                    } finally {
                        // 执行之前被阻塞的任务
                        released.run();
                    }
                });
            }
        });
    }

    /**
     * 修改标识 代表发起了一次blockOperations请求
     */
    private void delayOperations() {
        if (closed) {
            throw new IndexShardClosedException(shardId);
        }
        synchronized (this) {
            assert queuedBlockOperations > 0 || delayedOperations.isEmpty();
            queuedBlockOperations++;
        }
    }

    /**
     * 每次都是获取所有的门票 照理说一次获取一个门票是不会使用完的 不过既然如此的话 为什么不设置为1呢
     * @param timeout
     * @param timeUnit
     * @return
     * @throws InterruptedException
     * @throws TimeoutException
     */
    private Releasable acquireAll(final long timeout, final TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        if (Assertions.ENABLED) {
            // since delayed is not volatile, we have to synchronize even here for visibility
            synchronized (this) {
                assert queuedBlockOperations > 0;
            }
        }
        if (semaphore.tryAcquire(TOTAL_PERMITS, timeout, timeUnit)) {
            final RunOnce release = new RunOnce(() -> {
                assert semaphore.availablePermits() == 0;
                semaphore.release(TOTAL_PERMITS);
            });
            // 返回的是这个函数  使用者在执行完真正的逻辑后 才会通过调用该函数释放门票
            return release::run;
        } else {
            // 出现了超时异常
            throw new TimeoutException("timeout while blocking operations");
        }
    }

    /**
     * 当需要在block模式下执行的操作完成时  这里要处理之前被阻塞的任务
     */
    private void releaseDelayedOperations() {
        final List<DelayedOperation> queuedActions;
        synchronized (this) {
            assert queuedBlockOperations > 0;
            // 相当于这个方法是可重入的 当重复触发该方法时 不会真的继续获取门票 而是增加计数器 同时只有当计数器归0的时候 才真正执行后续逻辑
            queuedBlockOperations--;
            if (queuedBlockOperations == 0) {
                queuedActions = new ArrayList<>(delayedOperations);
                delayedOperations.clear();
            } else {
                queuedActions = Collections.emptyList();
            }
        }
        if (!queuedActions.isEmpty()) {
            /*
             * Try acquiring permits on fresh thread (for two reasons):
             *   - blockOperations can be called on a recovery thread which can be expected to be interrupted when recovery is cancelled;
             *     interruptions are bad here as permit acquisition will throw an interrupted exception which will be swallowed by
             *     the threaded action listener if the queue of the thread pool on which it submits is full
             *   - if a permit is acquired and the queue of the thread pool which the threaded action listener uses is full, the
             *     onFailure handler is executed on the calling thread; this should not be the recovery thread as it would delay the
             *     recovery
             *
             */
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                for (DelayedOperation queuedAction : queuedActions) {
                    acquire(queuedAction.listener, null, false, queuedAction.debugInfo, queuedAction.stackTrace);
                }
            });
        }
    }

    /**
     * Acquires a permit whenever permit acquisition is not blocked. If the permit is directly available, the provided
     * {@link ActionListener} will be called on the calling thread. During calls of
     * {@link #blockOperations(long, TimeUnit, CheckedRunnable)}, permit acquisition can be delayed.
     * The {@link ActionListener#onResponse(Object)} method will then be called using the provided executor once operations are no
     * longer blocked. Note that the executor will not be used for {@link ActionListener#onFailure(Exception)} calls. Those will run
     * directly on the calling thread, which in case of delays, will be a generic thread. Callers should thus make sure
     * that the {@link ActionListener#onFailure(Exception)} method provided here only contains lightweight operations.
     *
     * @param onAcquired      {@link ActionListener} that is invoked once acquisition is successful or failed
     *                        当获取到门票时触发该钩子
     * @param executorOnDelay executor to use for the possibly delayed {@link ActionListener#onResponse(Object)} call
     *                        代表在触发相关逻辑时使用指定的线程池去处理
     * @param forceExecution  whether the runnable should force its execution in case it gets rejected
     * @param debugInfo       an extra information that can be useful when tracing an unreleased permit. When assertions are enabled
     *                        the tracing will capture the supplied object's {@link Object#toString()} value. Otherwise the object
     *                        isn't used
     *                        正常情况下入口是这个方法
     */
    public void acquire(final ActionListener<Releasable> onAcquired, final String executorOnDelay, final boolean forceExecution,
                        final Object debugInfo) {
        final StackTraceElement[] stackTrace;
        // TODO 先忽略断言
        if (Assertions.ENABLED) {
            stackTrace = Thread.currentThread().getStackTrace();
        } else {
            stackTrace = null;
        }
        acquire(onAcquired, executorOnDelay, forceExecution, debugInfo, stackTrace);
    }

    /**
     *
     * @param onAcquired  当本线程获取到门票后执行  一般就是定义了要执行的逻辑
     * @param executorOnDelay  代表某个线程池的名字 如果指定的话 就是将逻辑交由这个线程池处理
     * @param forceExecution
     * @param debugInfo
     * @param stackTrace  当前线程栈信息
     */
    private void acquire(final ActionListener<Releasable> onAcquired, final String executorOnDelay, final boolean forceExecution,
                        final Object debugInfo, final StackTraceElement[] stackTrace) {
        if (closed) {
            // 当本对象已经被关闭时 使用异常触发钩子
            onAcquired.onFailure(new IndexShardClosedException(shardId));
            return;
        }
        final Releasable releasable;
        try {
            synchronized (this) {
                // 正常情况下 任务阻塞获取到门票后就可以执行了
                // 如果该值不为1 代表外部发起了一个阻塞操作  这会抢占所有的门票 导致其他任务无法正常执行 但是又不应该丢弃这些未执行的任务 所以将他们包装后设置到一个队列中
                if (queuedBlockOperations > 0) {
                    final Supplier<StoredContext> contextSupplier = threadPool.getThreadContext().newRestorableContext(false);
                    final ActionListener<Releasable> wrappedListener;
                    if (executorOnDelay != null) {
                        // 该监听器在使用时 会先切换当前上下文对象
                        wrappedListener = ActionListener.delegateFailure(new ContextPreservingActionListener<>(contextSupplier, onAcquired),
                            (l, r) -> threadPool.executor(executorOnDelay).execute(new ActionRunnable<>(l) {
                                @Override
                                public boolean isForceExecution() {
                                    return forceExecution;
                                }

                                @Override
                                protected void doRun() {
                                    listener.onResponse(r);
                                }

                                @Override
                                public void onRejection(Exception e) {
                                    IOUtils.closeWhileHandlingException(r);
                                    super.onRejection(e);
                                }
                            }));
                    } else {
                        wrappedListener = new ContextPreservingActionListener<>(contextSupplier, onAcquired);
                    }
                    // 可以看到原本要执行的逻辑 本包装成了一个 延迟执行对象
                    delayedOperations.add(new DelayedOperation(wrappedListener, debugInfo, stackTrace));
                    return;
                } else {
                    // 首次进入 queuedBlockOperations 为0 进入这个分支
                    releasable = acquire(debugInfo, stackTrace);
                }
            }
        } catch (final InterruptedException e) {
            onAcquired.onFailure(e);
            return;
        }
        // execute this outside the synchronized block!
        onAcquired.onResponse(releasable);
    }

    /**
     * 尝试获取一个门票 并返回一个释放门票的函数
     * @param debugInfo
     * @param stackTrace
     * @return
     * @throws InterruptedException
     */
    private Releasable acquire(Object debugInfo, StackTraceElement[] stackTrace) throws InterruptedException {
        assert Thread.holdsLock(this);
        // 阻塞直到获取到门票为止
        if (semaphore.tryAcquire(1, 0, TimeUnit.SECONDS)) { // the un-timed tryAcquire methods do not honor the fairness setting
            final AtomicBoolean closed = new AtomicBoolean();
            final Releasable releasable = () -> {
                if (closed.compareAndSet(false, true)) {
                    if (Assertions.ENABLED) {
                        Tuple<String, StackTraceElement[]> existing = issuedPermits.remove(closed);
                        assert existing != null;
                    }
                    semaphore.release(1);
                }
            };
            if (Assertions.ENABLED) {
                issuedPermits.put(closed, new Tuple<>(debugInfo.toString(), stackTrace));
            }
            // 忽略断言相关的逻辑  就只是semaphore.release
            return releasable;
        } else {
            // this should never happen, if it does something is deeply wrong
            throw new IllegalStateException("failed to obtain permit but operations are not delayed");
        }
    }

    /**
     * Obtain the active operation count, or {@link IndexShard#OPERATIONS_BLOCKED} if all permits are held.
     *
     * @return the active operation count, or {@link IndexShard#OPERATIONS_BLOCKED} when all permits are held.
     * 此时有多少活跃的operations  每个操作都会先获取门票再执行
     */
    int getActiveOperationsCount() {
        int availablePermits = semaphore.availablePermits();
        if (availablePermits == 0) {
            return IndexShard.OPERATIONS_BLOCKED; // This occurs when blockOperations() has acquired all the permits.
        } else {
            return TOTAL_PERMITS - availablePermits;
        }
    }


    synchronized boolean isBlocked() {
        return queuedBlockOperations > 0;
    }

    /**
     * @return a list of describing each permit that wasn't released yet. The description consist of the debugInfo supplied
     *         when the permit was acquired plus a stack traces that was captured when the permit was request.
     */
    List<String> getActiveOperations() {
        return issuedPermits.values().stream().map(
            t -> t.v1() + "\n" + ExceptionsHelper.formatStackTrace(t.v2()))
            .collect(Collectors.toList());
    }


    /**
     * 代表一个延迟操作  只是存储了一些属性 并没有什么处理逻辑
     */
    private static class DelayedOperation {

        /**
         * 实际上之前某个要执行的任务被封装成了监听器
         */
        private final ActionListener<Releasable> listener;
        private final String debugInfo;
        private final StackTraceElement[] stackTrace;

        private DelayedOperation(ActionListener<Releasable> listener, Object debugInfo, StackTraceElement[] stackTrace) {
            this.listener = listener;
            if (Assertions.ENABLED) {
                this.debugInfo = "[delayed] " + debugInfo;
                this.stackTrace = stackTrace;
            } else {
                this.debugInfo = null;
                this.stackTrace = null;
            }
        }
    }
}
