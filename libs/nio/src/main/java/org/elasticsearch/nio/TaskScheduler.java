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

package org.elasticsearch.nio;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * A basic priority queue backed timer service. The service is thread local and should only be used by a
 * single nio selector event loop thread.
 * 一个简单的基于优先队列的定时器对象
 */
public class TaskScheduler {

    /**
     * 所有待执行任务被包装成 DelayedTask  并根据任务触发时间进行排序
     */
    private final PriorityQueue<DelayedTask> tasks = new PriorityQueue<>(Comparator.comparingLong(DelayedTask::getDeadline));

    /**
     * Schedule a task at the defined relative nanotime. When {@link #pollTask(long)} is called with a
     * relative nanotime after the scheduled time, the task will be returned. This method returns a
     * {@link Runnable} that can be run to cancel the scheduled task.
     *
     * @param task to schedule
     * @param relativeNanos defining when to execute the task
     * @return runnable that will cancel the task
     */
    public Runnable scheduleAtRelativeTime(Runnable task, long relativeNanos) {
        DelayedTask delayedTask = new DelayedTask(relativeNanos, task);
        tasks.offer(delayedTask);
        return delayedTask;
    }

    /**
     * 获取首个到达触发时间的任务
     * @param relativeNanos
     * @return
     */
    public Runnable pollTask(long relativeNanos) {
        DelayedTask task;
        while ((task = tasks.peek()) != null) {
            if (relativeNanos - task.deadline >= 0) {
                tasks.remove();
                if (task.cancelled == false) {
                    return task.runnable;
                }
            } else {
                return null;
            }
        }
        return null;
    }

    /**
     * 获取距离下个任务触发还有多久时间
     * @param relativeNanos
     * @return
     */
    long nanosUntilNextTask(long relativeNanos) {
        DelayedTask nextTask = tasks.peek();
        if (nextTask == null) {
            return Long.MAX_VALUE;
        } else {
            return Math.max(nextTask.deadline - relativeNanos, 0);
        }
    }

    private static class DelayedTask implements Runnable {

        /**
         * 代表任务触发时间
         */
        private final long deadline;
        private final Runnable runnable;
        /**
         * 任务是否被关闭  一般使用者会持有该对象的引用 这样就可以随时关闭任务
         */
        private boolean cancelled = false;

        private DelayedTask(long deadline, Runnable runnable) {
            this.deadline = deadline;
            this.runnable = runnable;
        }

        private long getDeadline() {
            return deadline;
        }

        @Override
        public void run() {
            cancelled = true;
        }
    }
}
