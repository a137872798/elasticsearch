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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * This async IO processor allows to batch IO operations and have a single writer processing the write operations.
 * This can be used to ensure that threads can continue with other work while the actual IO operation is still processed
 * by a single worker. A worker in this context can be any caller of the {@link #put(Object, Consumer)} method since it will
 * hijack a worker if nobody else is currently processing queued items. If the internal queue has reached it's capacity incoming threads
 * might be blocked until other items are processed
 * 异步IO处理器
 */
public abstract class AsyncIOProcessor<Item> {
    private final Logger logger;
    /**
     * 暂存任务的队列
     * 可以看出一个思想 就是尽可能让一个线程执行多个写入任务  而不采用并行的方式 因为并行并不能提高IO形成  多线程下磁头的旋转反而会降低性能
     */
    private final ArrayBlockingQueue<Tuple<Item, Consumer<Exception>>> queue;
    private final ThreadContext threadContext;
    /**
     * 只包含一个门票的对象
     */
    private final Semaphore promiseSemaphore = new Semaphore(1);

    protected AsyncIOProcessor(Logger logger, int queueSize, ThreadContext threadContext) {
        this.logger = logger;
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.threadContext = threadContext;
    }

    /**
     * Adds the given item to the queue. The listener is notified once the item is processed
     * 将某个任务设置到队列中 并且在处理完毕时触发监听器
     */
    public final void put(Item item, Consumer<Exception> listener) {
        Objects.requireNonNull(item, "item must not be null");
        Objects.requireNonNull(listener, "listener must not be null");
        // the algorithm here tires to reduce the load on each individual caller.
        // we try to have only one caller that processes pending items to disc while others just add to the queue but
        // at the same time never overload the node by pushing too many items into the queue.

        // we first try make a promise that we are responsible for the processing
        final boolean promised = promiseSemaphore.tryAcquire();
        // 看来这些任务被设计成了串行执行  首先尝试直接执行任务
        if (promised == false) {
            // in this case we are not responsible and can just block until there is space
            try {
                queue.put(new Tuple<>(item, preserveContext(listener)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.accept(e);
            }
        }

        // here we have to try to make the promise again otherwise there is a race when a thread puts an entry without making the promise
        // while we are draining that mean we might exit below too early in the while loop if the drainAndSync call is fast.
        // 这里还会尝试执行一次
        if (promised || promiseSemaphore.tryAcquire()) {
            final List<Tuple<Item, Consumer<Exception>>> candidates = new ArrayList<>();
            // 因为 candidates + queue 才是要执行的任务 当promised为false 时 任务已经加入到queue中了所以不需要再加入一次
            if (promised) {
                // we are responsible for processing we don't need to add the tuple to the queue we can just add it to the candidates
                // no need to preserve context for listener since it runs in current thread.
                candidates.add(new Tuple<>(item, listener));
            }
            // since we made the promise to process we gotta do it here at least once
            // 竞争到门票的线程 尽可能的将之前未处理的任务一起处理了
            drainAndProcessAndRelease(candidates);
            // 线程在之后还会不断的扫描任务啊 这样的好处就是不会有任务残留
            while (queue.isEmpty() == false && promiseSemaphore.tryAcquire()) {
                // yet if the queue is not empty AND nobody else has yet made the promise to take over we continue processing
                drainAndProcessAndRelease(candidates);
            }
        }
    }

    /**
     * 将队列中的任务 引流到 candidates中
     * @param candidates
     */
    private void drainAndProcessAndRelease(List<Tuple<Item, Consumer<Exception>>> candidates) {
        Exception exception;
        try {
            queue.drainTo(candidates);
            // 处理这组任务并使用异常触发监听器
            exception = processList(candidates);
        } finally {
            promiseSemaphore.release();
        }
        notifyList(candidates, exception);
        candidates.clear();
    }

    private Exception processList(List<Tuple<Item, Consumer<Exception>>> candidates) {
        Exception exception = null;
        if (candidates.isEmpty() == false) {
            try {
                write(candidates);
            } catch (Exception ex) { // if this fails we are in deep shit - fail the request
                logger.debug("failed to write candidates", ex);
                // this exception is passed to all listeners - we don't retry. if this doesn't work we are in deep shit
                exception = ex;
            }
        }
        return exception;
    }

    /**
     * 执行监听器的异常处理 这是放在信号量锁外的  避免影响其他线程的写入
     * @param candidates
     * @param exception
     */
    private void notifyList(List<Tuple<Item, Consumer<Exception>>> candidates, Exception exception) {
        for (Tuple<Item, Consumer<Exception>> tuple : candidates) {
            Consumer<Exception> consumer = tuple.v2();
            try {
                consumer.accept(exception);
            } catch (Exception ex) {
                logger.warn("failed to notify callback", ex);
            }
        }
    }

    /**
     * 包装了一层异常处理对象 也就是在处理前会更换内部的context
     * @param consumer
     * @return
     */
    private Consumer<Exception> preserveContext(Consumer<Exception> consumer) {
        Supplier<ThreadContext.StoredContext> restorableContext = threadContext.newRestorableContext(false);
        return e -> {
            try (ThreadContext.StoredContext ignore = restorableContext.get()) {
                consumer.accept(e);
            }
        };
    }

    /**
     * Writes or processes the items out or to disk.
     * 针对这组任务进行写入操作 (IO任务)
     */
    protected abstract void write(List<Tuple<Item, Consumer<Exception>>> candidates) throws IOException;
}
