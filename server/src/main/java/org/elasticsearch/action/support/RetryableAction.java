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

package org.elasticsearch.action.support;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A action that will be retried on failure if {@link RetryableAction#shouldRetry(Exception)} returns true.
 * The executor the action will be executed on can be defined in the constructor. Otherwise, SAME is the
 * default. The action will be retried with exponentially increasing delay periods until the timeout period
 * has been reached.
 * 代表一个可重试的指令  <T> 定义了指令结果
 */
public abstract class RetryableAction<Response> {

    private final Logger logger;

    private final AtomicBoolean isDone = new AtomicBoolean(false);
    private final ThreadPool threadPool;
    private final long initialDelayMillis;
    /**
     * 该任务算上重试 一共允许执行多长时间
     */
    private final long timeoutMillis;
    private final long startMillis;
    /**
     * 当真正产生结果时才触发监听器 如果只是失败 会尝试进行重试
     */
    private final ActionListener<Response> finalListener;
    private final String executor;

    /**
     * 执行失败时在一定延时后重试
     */
    private volatile Scheduler.ScheduledCancellable retryTask;

    public RetryableAction(Logger logger, ThreadPool threadPool, TimeValue initialDelay, TimeValue timeoutValue,
                           ActionListener<Response> listener) {
        this(logger, threadPool, initialDelay, timeoutValue, listener, ThreadPool.Names.SAME);
    }

    public RetryableAction(Logger logger, ThreadPool threadPool, TimeValue initialDelay, TimeValue timeoutValue,
                           ActionListener<Response> listener, String executor) {
        this.logger = logger;
        this.threadPool = threadPool;
        this.initialDelayMillis = initialDelay.getMillis();
        if (initialDelayMillis < 1) {
            throw new IllegalArgumentException("Initial delay was less than 1 millisecond: " + initialDelay);
        }
        this.timeoutMillis = timeoutValue.getMillis();
        this.startMillis = threadPool.relativeTimeInMillis();
        this.finalListener = listener;
        this.executor = executor;
    }

    /**
     * 开始执行重试任务
     */
    public void run() {
        final RetryingListener retryingListener = new RetryingListener(initialDelayMillis, null);
        // 将监听器和 本action执行的逻辑打包在一起
        final Runnable runnable = createRunnable(retryingListener);
        threadPool.executor(executor).execute(runnable);
    }

    /**
     * 强制关闭任务时 也就不需要再重试了
     * @param e
     */
    public void cancel(Exception e) {
        if (isDone.compareAndSet(false, true)) {
            Scheduler.ScheduledCancellable localRetryTask = this.retryTask;
            // 如果重试任务已经被设置 则关闭
            if (localRetryTask != null) {
                localRetryTask.cancel();
            }
            onFinished();
            finalListener.onFailure(e);
        }
    }

    /**
     * 当任务执行逻辑 与监听器绑定在一起
     * @param retryingListener
     * @return
     */
    private Runnable createRunnable(RetryingListener retryingListener) {
        return new ActionRunnable<>(retryingListener) {

            /**
             * 覆盖runnable的 run 方法
             */
            @Override
            protected void doRun() {
                retryTask = null;
                // It is possible that the task was cancelled in between the retry being dispatched and now
                if (isDone.get() == false) {
                    tryAction(listener);
                }
            }

            /**
             * 当任务被executor拒绝时 触发onFailure  之后会触发内部的监听器 也就是 retryingListener
             * @param e
             */
            @Override
            public void onRejection(Exception e) {
                retryTask = null;
                // TODO: The only implementations of this class use SAME which means the execution will not be
                //  rejected. Future implementations can adjust this functionality as needed.
                onFailure(e);
            }
        };
    }

    /**
     * action中真正要执行的逻辑 要放到这个方法里面
     * @param listener
     */
    public abstract void tryAction(ActionListener<Response> listener);

    public abstract boolean shouldRetry(Exception e);

    /**
     * 代表这个重试任务执行成功 或者被关闭
     */
    public void onFinished() {
    }

    /**
     * 该监听器会监听失败 并根据异常情况判定是否要开启定时 在一定延时后重启任务
     */
    private class RetryingListener implements ActionListener<Response> {

        private static final int MAX_EXCEPTIONS = 4;

        private final long delayMillisBound;
        private ArrayDeque<Exception> caughtExceptions;

        private RetryingListener(long delayMillisBound, ArrayDeque<Exception> caughtExceptions) {
            this.delayMillisBound = delayMillisBound;
            this.caughtExceptions = caughtExceptions;
        }

        @Override
        public void onResponse(Response response) {
            if (isDone.compareAndSet(false, true)) {
                onFinished();
                finalListener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            // 根据异常类型检测是否支持重试
            if (shouldRetry(e)) {
                // 距离执行过了多久
                final long elapsedMillis = threadPool.relativeTimeInMillis() - startMillis;
                // 该任务包含重试在内有一个总的超时时间 当超过该时间后 任务就不应该再执行了
                if (elapsedMillis >= timeoutMillis) {
                    logger.debug(() -> new ParameterizedMessage("retryable action timed out after {}",
                        TimeValue.timeValueMillis(elapsedMillis)), e);
                    addException(e);
                    if (isDone.compareAndSet(false, true)) {
                        onFinished();
                        finalListener.onFailure(buildFinalException());
                    }
                } else {
                    addException(e);

                    // 增加延迟时长 并启动定时任务
                    final long nextDelayMillisBound = Math.min(delayMillisBound * 2, Integer.MAX_VALUE);
                    final RetryingListener retryingListener = new RetryingListener(nextDelayMillisBound, caughtExceptions);
                    final Runnable runnable = createRunnable(retryingListener);
                    final long delayMillis = Randomness.get().nextInt(Math.toIntExact(delayMillisBound)) + 1;
                    if (isDone.get() == false) {
                        final TimeValue delay = TimeValue.timeValueMillis(delayMillis);
                        logger.debug(() -> new ParameterizedMessage("retrying action that failed in {}", delay), e);
                        retryTask = threadPool.schedule(runnable, delay, executor);
                    }
                }
            } else {
                addException(e);
                if (isDone.compareAndSet(false,true)) {
                    onFinished();
                    finalListener.onFailure(buildFinalException());
                }
            }
        }

        private Exception buildFinalException() {
            final Exception topLevel = caughtExceptions.removeFirst();
            Exception suppressed;
            while ((suppressed = caughtExceptions.pollFirst()) != null) {
                topLevel.addSuppressed(suppressed);
            }
            return topLevel;
        }

        private void addException(Exception e) {
            if (caughtExceptions != null) {
                if (caughtExceptions.size() == MAX_EXCEPTIONS) {
                    caughtExceptions.removeLast();
                }
            } else {
                caughtExceptions = new ArrayDeque<>(MAX_EXCEPTIONS);
            }
            caughtExceptions.addFirst(e);
        }
    }
}
