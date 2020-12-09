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
package org.elasticsearch.persistent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Represents a executor node operation that corresponds to a persistent task
 * 代表一个正在执行的持久化任务  该任务本身是可以被关闭的
 */
public class AllocatedPersistentTask extends CancellableTask {

    private static final Logger logger = LogManager.getLogger(AllocatedPersistentTask.class);

    /**
     * 任务当前状态
     */
    private final AtomicReference<State> state;

    private volatile String persistentTaskId;
    private volatile long allocationId;
    private volatile @Nullable Exception failure;
    /**
     * 任务对象本身是被持久化服务调控的
     */
    private volatile PersistentTasksService persistentTasksService;

    /**
     * 任务在执行时会插入到任务管理器中
     */
    private volatile TaskManager taskManager;

    /**
     *
     * @param id
     * @param type
     * @param action
     * @param description
     * @param parentTask  每个task对象可能会有父任务
     * @param headers   也可能会有一些任务头信息
     */
    public AllocatedPersistentTask(long id, String type, String action, String description, TaskId parentTask,
                                   Map<String, String> headers) {
        super(id, type, action, description, parentTask, headers);
        this.state = new AtomicReference<>(State.STARTED);
    }

    /**
     * 当该任务被关闭时 会将子任务一起关闭
     * @return
     */
    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    // In case of persistent tasks we always need to return: `false`
    // because in case of persistent task the parent task isn't a task in the task manager, but in cluster state.
    // This instructs the task manager not to try to kill this persistent task when the task manager cannot find
    // a fake parent node id "cluster" in the cluster state
    @Override
    public final boolean cancelOnParentLeaving() {
        return false;
    }

    @Override
    public Status getStatus() {
        return new PersistentTasksNodeService.Status(state.get());
    }

    /**
     * Updates the persistent state for the corresponding persistent task.
     * <p>
     * This doesn't affect the status of this allocated task.
     * @param state
     * @param listener
     * 当需要更新任务状态的时候会调用该方法
     */
    public void updatePersistentTaskState(final PersistentTaskState state,
                                          final ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener) {
        // 借由持久化服务发送一个更新任务状态的请求
        persistentTasksService.sendUpdateStateRequest(persistentTaskId, allocationId, state, listener);
    }

    public String getPersistentTaskId() {
        return persistentTaskId;
    }

    /**
     * 对该任务做初始化工作
     * @param persistentTasksService
     * @param taskManager   负责维护该任务的管理器
     * @param persistentTaskId  通过persistent服务为当前task分配一个id
     * @param allocationId
     */
    protected void init(PersistentTasksService persistentTasksService, TaskManager taskManager,
                        String persistentTaskId, long allocationId) {
        this.persistentTasksService = persistentTasksService;
        this.taskManager = taskManager;
        this.persistentTaskId = persistentTaskId;
        this.allocationId = allocationId;
    }

    public Exception getFailure() {
        return failure;
    }

    public long getAllocationId() {
        return allocationId;
    }

    /**
     * Waits for a given persistent task to comply with a given predicate, then call back the listener accordingly.
     *
     * @param predicate the persistent task predicate to evaluate  通过该谓语判断此时是否满足条件
     * @param timeout a timeout for waiting
     * @param listener the callback listener
     *                 等待任务状态直到满足某个条件
     */
    public void waitForPersistentTask(final Predicate<PersistentTasksCustomMetadata.PersistentTask<?>> predicate,
                                      final @Nullable TimeValue timeout,
                                      final PersistentTasksService.WaitForPersistentTaskListener<?> listener) {
        persistentTasksService.waitForPersistentTaskCondition(persistentTaskId, predicate, timeout, listener);
    }

    protected final boolean isCompleted() {
        return state.get() == State.COMPLETED;
    }

    boolean markAsCancelled() {
        return state.compareAndSet(State.STARTED, State.PENDING_CANCEL);
    }

    public void markAsCompleted() {
        completeAndNotifyIfNeeded(null);
    }

    public void markAsFailed(Exception e) {
        if (CancelTasksRequest.DEFAULT_REASON.equals(getReasonCancelled())) {
            completeAndNotifyIfNeeded(null);
        } else {
            completeAndNotifyIfNeeded(e);
        }
    }

    /**
     * 将当前任务标记成完成状态
     * @param failure
     */
    private void completeAndNotifyIfNeeded(@Nullable Exception failure) {
        final State prevState = state.getAndSet(State.COMPLETED);
        if (prevState == State.COMPLETED) {
            logger.warn("attempt to complete task [{}] with id [{}] in the [{}] state", getAction(), getPersistentTaskId(), prevState);
        } else {
            if (failure != null) {
                logger.warn(() -> new ParameterizedMessage("task {} failed with an exception", getPersistentTaskId()), failure);
            }
            try {
                this.failure = failure;
                // 只有当任务首次从started转换时 才需要做处理工作
                if (prevState == State.STARTED) {
                    logger.trace("sending notification for completed task [{}] with id [{}]", getAction(), getPersistentTaskId());
                    persistentTasksService.sendCompletionRequest(getPersistentTaskId(), getAllocationId(), failure, new
                            ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>>() {
                                @Override
                                public void onResponse(PersistentTasksCustomMetadata.PersistentTask<?> persistentTask) {
                                    logger.trace("notification for task [{}] with id [{}] was successful", getAction(),
                                            getPersistentTaskId());
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    logger.warn(() -> new ParameterizedMessage(
                                        "notification for task [{}] with id [{}] failed", getAction(), getPersistentTaskId()), e);
                                }
                            });
                }
            } finally {
                // 因为当前任务已经完成了 所以需要从管理器中注销
                taskManager.unregister(this);
            }
        }
    }

    /**
     * 描述 persistentTask此时的运行状态
     */
    public enum State {
        STARTED,  // the task is currently running
        PENDING_CANCEL, // the task is cancelled on master, cancelling it locally
        COMPLETED     // the task is done running and trying to notify caller
    }
}
