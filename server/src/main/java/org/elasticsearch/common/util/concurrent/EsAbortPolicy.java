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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.metrics.CounterMetric;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * es自带的任务拒绝处理器
 */
public class EsAbortPolicy implements XRejectedExecutionHandler {

    /**
     * 一个并发计数器
     */
    private final CounterMetric rejected = new CounterMetric();

    /**
     * 每当拒绝任务时 增加计数器
     * @param r
     * @param executor
     */
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        // AbstractRunnable 包含一个 是否强制执行的api
        if (r instanceof AbstractRunnable) {
            if (((AbstractRunnable) r).isForceExecution()) {
                BlockingQueue<Runnable> queue = executor.getQueue();
                // 设置强制执行任务的线程池对应的工作队列必须是SizeBlockingQueue
                if (!(queue instanceof SizeBlockingQueue)) {
                    throw new IllegalStateException("forced execution, but expected a size queue");
                }
                try {
                    // 强制设置到队列中
                    ((SizeBlockingQueue) queue).forcePut(r);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("forced execution, but got interrupted", e);
                }
                return;
            }
        }
        // 其余情况才会增加计数器 同时抛出异常
        rejected.inc();
        throw new EsRejectedExecutionException("rejected execution of " + r + " on " + executor, executor.isShutdown());
    }

    /**
     * 返回拒绝的总数
     * @return
     */
    @Override
    public long rejected() {
        return rejected.count();
    }
}
