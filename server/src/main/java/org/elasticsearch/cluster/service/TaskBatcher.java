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

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Batching support for {@link PrioritizedEsThreadPoolExecutor}
 * Tasks that share the same batching key are batched (see {@link BatchedTask#batchingKey})
 * 一个批量执行任务的对象
 */
public abstract class TaskBatcher {

    private final Logger logger;
    /**
     * 该线程池以优先队列作为阻塞队列
     */
    private final PrioritizedEsThreadPoolExecutor threadExecutor;
    /**
     * 每批任务 对应一个 batchingKey对象
     */
    final Map<Object, LinkedHashSet<BatchedTask>> tasksPerBatchingKey = new HashMap<>();

    public TaskBatcher(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor) {
        this.logger = logger;
        this.threadExecutor = threadExecutor;
    }

    /**
     * 往当前批对象中提交一组任务
     * @param tasks  提交的一组任务
     * @param timeout  任务超时时间  在该时间内未完成的将不会执行
     * @throws EsRejectedExecutionException
     */
    public void submitTasks(List<? extends BatchedTask> tasks, @Nullable TimeValue timeout) throws EsRejectedExecutionException {
        if (tasks.isEmpty()) {
            return;
        }
        final BatchedTask firstTask = tasks.get(0);
        assert tasks.stream().allMatch(t -> t.batchingKey == firstTask.batchingKey) :
            "tasks submitted in a batch should share the same batching key: " + tasks;
        // convert to an identity map to check for dups based on task identity
        // 这里主要是校验某个task 是否已经添加到 tasksPerBatchingKey 中了
        final Map<Object, BatchedTask> tasksIdentity = tasks.stream().collect(Collectors.toMap(
            BatchedTask::getTask,
            Function.identity(),
            (a, b) -> { throw new IllegalStateException("cannot add duplicate task: " + a); },
            // 因为是针对Object类型 所以无法用普通的基于 equals的hashMap  只能使用 "==" 的 IdentityHashMap
            IdentityHashMap::new));

        synchronized (tasksPerBatchingKey) {
            // 以 batchingKey 作为key 存储该批任务
            LinkedHashSet<BatchedTask> existingTasks = tasksPerBatchingKey.computeIfAbsent(firstTask.batchingKey,
                k -> new LinkedHashSet<>(tasks.size()));
            // 重复检测
            for (BatchedTask existing : existingTasks) {
                // check that there won't be two tasks with the same identity for the same batching key
                BatchedTask duplicateTask = tasksIdentity.get(existing.getTask());
                if (duplicateTask != null) {
                    throw new IllegalStateException("task [" + duplicateTask.describeTasks(
                        Collections.singletonList(existing)) + "] with source [" + duplicateTask.source + "] is already queued");
                }
            }
            existingTasks.addAll(tasks);
        }

        // 只提交首个任务 看来每个任务中已经包含了自动执行下游任务的逻辑
        if (timeout != null) {
            threadExecutor.execute(firstTask, timeout, () -> onTimeoutInternal(tasks, timeout));
        } else {
            threadExecutor.execute(firstTask);
        }
    }

    /**
     * 由于超时导致任务没有执行
     * @param tasks   注意本批任务并不代表当前所有未执行的批任务  因为之后也可以创建相同 batchingkey的任务 这时创建的超时回调还无法访问到这些对象
     *                所以每个超时回调只处理当前插入的任务
     * @param timeout
     */
    private void onTimeoutInternal(List<? extends BatchedTask> tasks, TimeValue timeout) {
        final ArrayList<BatchedTask> toRemove = new ArrayList<>();
        for (BatchedTask task : tasks) {
            if (task.processed.getAndSet(true) == false) {
                logger.debug("task [{}] timed out after [{}]", task.source, timeout);
                toRemove.add(task);
            }
        }
        if (toRemove.isEmpty() == false) {
            BatchedTask firstTask = toRemove.get(0);
            Object batchingKey = firstTask.batchingKey;
            assert tasks.stream().allMatch(t -> t.batchingKey == batchingKey) :
                "tasks submitted in a batch should share the same batching key: " + tasks;
            // 将本次插入的批任务从 相同batchingKey 对应的队列中移除
            synchronized (tasksPerBatchingKey) {
                LinkedHashSet<BatchedTask> existingTasks = tasksPerBatchingKey.get(batchingKey);
                if (existingTasks != null) {
                    existingTasks.removeAll(toRemove);
                    if (existingTasks.isEmpty()) {
                        tasksPerBatchingKey.remove(batchingKey);
                    }
                }
            }
            // 触发超时钩子
            onTimeout(toRemove, timeout);
        }
    }

    /**
     * Action to be implemented by the specific batching implementation.
     * All tasks have the same batching key.
     */
    protected abstract void onTimeout(List<? extends BatchedTask> tasks, TimeValue timeout);

    /**
     * 在线程池中 当检测到一个批任务时 它同时也当作一个触发器 会将本批次所有的任务一起取出来执行
     * @param updateTask
     */
    void runIfNotProcessed(BatchedTask updateTask) {
        // if this task is already processed, it shouldn't execute other tasks with same batching key that arrived later,
        // to give other tasks with different batching key a chance to execute.
        if (updateTask.processed.get() == false) {
            final List<BatchedTask> toExecute = new ArrayList<>();

            // 按照source 对任务进行分组   首先决定是否是同一批任务通过 batchingKey 来决定 之后任务本身可能按照不同的source创建
            final Map<String, List<BatchedTask>> processTasksBySource = new HashMap<>();
            synchronized (tasksPerBatchingKey) {
                LinkedHashSet<BatchedTask> pending = tasksPerBatchingKey.remove(updateTask.batchingKey);
                if (pending != null) {
                    for (BatchedTask task : pending) {
                        // 从这里可以看出 整批任务中只要有一个被处理 就会将其他任务标记成处理完成的状态
                        if (task.processed.getAndSet(true) == false) {
                            logger.trace("will process {}", task);
                            toExecute.add(task);
                            processTasksBySource.computeIfAbsent(task.source, s -> new ArrayList<>()).add(task);
                        } else {
                            logger.trace("skipping {}, already processed", task);
                        }
                    }
                }
            }

            // 这里包含所有待执行的任务
            if (toExecute.isEmpty() == false) {
                final String tasksSummary = processTasksBySource.entrySet().stream().map(entry -> {
                    String tasks = updateTask.describeTasks(entry.getValue());
                    return tasks.isEmpty() ? entry.getKey() : entry.getKey() + "[" + tasks + "]";
                }).reduce((s1, s2) -> s1 + ", " + s2).orElse("");

                // 执行批量任务
                run(updateTask.batchingKey, toExecute, tasksSummary);
            }
        }
    }

    /**
     * Action to be implemented by the specific batching implementation
     * All tasks have the given batching key.
     * @param batchingKey 标识批任务的key
     * @param tasks  本次所有任务对象
     * @param tasksSummary 有关任务的描述信息
     * 执行批量任务
     */
    protected abstract void run(Object batchingKey, List<? extends BatchedTask> tasks, String tasksSummary);

    /**
     * Represents a runnable task that supports batching.
     * Implementors of TaskBatcher can subclass this to add a payload to the task.
     * 代表某批任务中的单个任务
     */
    protected abstract class BatchedTask extends SourcePrioritizedRunnable {
        /**
         * whether the task has been processed already
         * 代表任务已经执行完毕了
         */
        protected final AtomicBoolean processed = new AtomicBoolean();

        /**
         * the object that is used as batching key
         * 用于标注当前任务属于哪个批次
         */
        protected final Object batchingKey;
        /**
         * the task object that is wrapped
         */
        protected final Object task;

        protected BatchedTask(Priority priority, String source, Object batchingKey, Object task) {
            super(priority, source);
            this.batchingKey = batchingKey;
            this.task = task;
        }

        @Override
        public void run() {
            runIfNotProcessed(this);
        }

        @Override
        public String toString() {
            String taskDescription = describeTasks(Collections.singletonList(this));
            if (taskDescription.isEmpty()) {
                return "[" + source + "]";
            } else {
                return "[" + source + "[" + taskDescription + "]]";
            }
        }

        public abstract String describeTasks(List<? extends BatchedTask> tasks);

        public Object getTask() {
            return task;
        }
    }
}
