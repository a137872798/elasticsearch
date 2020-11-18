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

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Transport action that can be used to cancel currently running cancellable tasks.
 * <p>
 * For a task to be cancellable it has to return an instance of
 * {@link CancellableTask} from {@link TransportRequest#createTask}
 * 该对象实现了关闭任务的功能
 */
public class TransportCancelTasksAction extends TransportTasksAction<CancellableTask, CancelTasksRequest, CancelTasksResponse, TaskInfo> {

    public static final String BAN_PARENT_ACTION_NAME = "internal:admin/tasks/ban";

    /**
     * 需要的参数从容器中获得
     * @param clusterService
     * @param transportService
     * @param actionFilters
     */
    @Inject
    public TransportCancelTasksAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
        super(CancelTasksAction.NAME, clusterService, transportService, actionFilters,
            CancelTasksRequest::new, CancelTasksResponse::new, TaskInfo::new, ThreadPool.Names.MANAGEMENT);
        // 该对象会将 禁止子任务的处理器注册到transportService上
        transportService.registerRequestHandler(BAN_PARENT_ACTION_NAME, ThreadPool.Names.SAME, BanParentTaskRequest::new,
            new BanParentRequestHandler());
    }

    /**
     * 定义如何将相关的结果包装成一个返回给调用者的res
     * @param request
     * @param tasks
     * @param taskOperationFailures
     * @param failedNodeExceptions
     * @return
     */
    @Override
    protected CancelTasksResponse newResponse(CancelTasksRequest request, List<TaskInfo> tasks, List<TaskOperationFailure>
        taskOperationFailures, List<FailedNodeException> failedNodeExceptions) {
        return new CancelTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    /**
     * 这里是寻找符合条件的task 并交给consumer进行处理
     * @param request
     * @param operation  处理的函数
     */
    @Override
    protected void processTasks(CancelTasksRequest request, Consumer<CancellableTask> operation) {
        // 如果请求体直接声明了某个task
        if (request.getTaskId().isSet()) {
            // we are only checking one task, we can optimize it
            CancellableTask task = taskManager.getCancellableTask(request.getTaskId().getId());
            if (task != null) {
                if (request.match(task)) {
                    operation.accept(task);
                } else {
                    throw new IllegalArgumentException("task [" + request.getTaskId() + "] doesn't support this operation");
                }
            } else {
                if (taskManager.getTask(request.getTaskId().getId()) != null) {
                    // The task exists, but doesn't support cancellation
                    throw new IllegalArgumentException("task [" + request.getTaskId() + "] doesn't support cancellation");
                } else {
                    throw new ResourceNotFoundException("task [{}] is not found", request.getTaskId());
                }
            }
        } else {
            // 寻找所有匹配的可关闭任务
            for (CancellableTask task : taskManager.getCancellableTasks().values()) {
                if (request.match(task)) {
                    operation.accept(task);
                }
            }
        }
    }

    /**
     * 尝试关闭每个任务 并将结果通知给监听器
     * @param request
     * @param cancellableTask
     * @param listener
     */
    @Override
    protected void taskOperation(CancelTasksRequest request, CancellableTask cancellableTask, ActionListener<TaskInfo> listener) {
        String nodeId = clusterService.localNode().getId();
        // 将任务 以及子级任务一起关闭
        cancelTaskAndDescendants(cancellableTask, request.getReason(), request.waitForCompletion(),
            ActionListener.map(listener, r -> cancellableTask.taskInfo(nodeId, false)));
    }


    /**
     * 关闭当前任务
     * @param task
     * @param reason
     * @param waitForCompletion  是否要等待整个任务关闭
     * @param listener
     */
    void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {

        // 获取任务的id
        final TaskId taskId = task.taskInfo(clusterService.localNode().getId(), false).getTaskId();
        // 是否需要连同 子任务一起关闭
        if (task.shouldCancelChildrenOnCancellation()) {
            logger.trace("cancelling task [{}] and its descendants", taskId);
            StepListener<Void> completedListener = new StepListener<>();
            // 该监听器指定了内部的监听器数量为3
            GroupedActionListener<Void> groupedListener = new GroupedActionListener<>(ActionListener.map(completedListener, r -> null), 3);
            // 这个是监听子任务完成的
            // childrenNodes 对应执行子任务的节点
            Collection<DiscoveryNode> childrenNodes = taskManager.startBanOnChildrenNodes(task.getId(), () -> {
                logger.trace("child tasks of parent [{}] are completed", taskId);
                groupedListener.onResponse(null);
            });
            // 关闭当前任务
            taskManager.cancel(task, reason, () -> {
                logger.trace("task [{}] is cancelled", taskId);
                groupedListener.onResponse(null);
            });
            StepListener<Void> banOnNodesListener = new StepListener<>();
            // 将子级任务都标记成禁止状态
            setBanOnNodes(reason, waitForCompletion, task, childrenNodes, banOnNodesListener);
            banOnNodesListener.whenComplete(groupedListener::onResponse, groupedListener::onFailure);
            // If we start unbanning when the last child task completed and that child task executed with a specific user, then unban
            // requests are denied because internal requests can't run with a user. We need to remove bans with the current thread context.
            final Runnable removeBansRunnable = transportService.getThreadPool().getThreadContext()
                .preserveContext(() -> removeBanOnNodes(task, childrenNodes));
            // We remove bans after all child tasks are completed although in theory we can do it on a per-node basis.
            // 因为每个任务最后都会调用close  在所有任务都处理完毕后 就可以停止ban了
            completedListener.whenComplete(r -> removeBansRunnable.run(), e -> removeBansRunnable.run());
            // if wait_for_completion is true, then only return when (1) bans are placed on child nodes, (2) child tasks are
            // completed or failed, (3) the main task is cancelled. Otherwise, return after bans are placed on child nodes.
            if (waitForCompletion) {
                completedListener.whenComplete(r -> listener.onResponse(null), listener::onFailure);
            } else {
                banOnNodesListener.whenComplete(r -> listener.onResponse(null), listener::onFailure);
            }
        } else {
            logger.trace("task [{}] doesn't have any children that should be cancelled", taskId);
            // 如果等待完成才返回结果 也就是追加监听器  否则只要发起一个cancel 就可以返回结果了 (不在乎是否成功关闭)
            // 内部会调用 onCancel()
            if (waitForCompletion) {
                taskManager.cancel(task, reason, () -> listener.onResponse(null));
            } else {
                taskManager.cancel(task, reason, () -> {});
                listener.onResponse(null);
            }
        }
    }

    /**
     * 发送禁止执行任务的请求到相关节点上
     * @param reason
     * @param waitForCompletion 是否要等待整个流程完成
     * @param task
     * @param childNodes  子级任务所在的节点
     * @param listener
     */
    private void setBanOnNodes(String reason, boolean waitForCompletion, CancellableTask task,
                               Collection<DiscoveryNode> childNodes, ActionListener<Void> listener) {
        if (childNodes.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        // 主要就是以这个id 作为parentId 去查询task
        final TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());
        logger.trace("cancelling child tasks of [{}] on child nodes {}", taskId, childNodes);
        // 当发往所有节点的请求都被处理后 触发监听器
        GroupedActionListener<Void> groupedListener =
            new GroupedActionListener<>(ActionListener.map(listener, r -> null), childNodes.size());
        final BanParentTaskRequest banRequest = BanParentTaskRequest.createSetBanParentTaskRequest(taskId, reason, waitForCompletion);
        for (DiscoveryNode node : childNodes) {
            transportService.sendRequest(node, BAN_PARENT_ACTION_NAME, banRequest,
                new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        logger.trace("sent ban for tasks with the parent [{}] to the node [{}]", taskId, node);
                        groupedListener.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assert ExceptionsHelper.unwrapCause(exp) instanceof ElasticsearchSecurityException == false;
                        logger.warn("Cannot send ban for tasks with the parent [{}] to the node [{}]", taskId, node);
                        groupedListener.onFailure(exp);
                    }
                });
        }
    }

    /**
     * 这里创建接触 ban状态的请求
     * @param task
     * @param childNodes
     */
    private void removeBanOnNodes(CancellableTask task, Collection<DiscoveryNode> childNodes) {
        final BanParentTaskRequest request =
            BanParentTaskRequest.createRemoveBanParentTaskRequest(new TaskId(clusterService.localNode().getId(), task.getId()));
        for (DiscoveryNode node : childNodes) {
            logger.trace("Sending remove ban for tasks with the parent [{}] to the node [{}]", request.parentTaskId, node);
            transportService.sendRequest(node, BAN_PARENT_ACTION_NAME, request, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                @Override
                public void handleException(TransportException exp) {
                    assert ExceptionsHelper.unwrapCause(exp) instanceof ElasticsearchSecurityException == false;
                    logger.info("failed to remove the parent ban for task {} on node {}", request.parentTaskId, node);
                }
            });
        }
    }

    /**
     * 这个请求会被发送到各个子任务所在的节点 通知他们停止任务
     */
    private static class BanParentTaskRequest extends TransportRequest {

        private final TaskId parentTaskId;
        private final boolean ban;
        private final boolean waitForCompletion;
        private final String reason;

        /**
         * 创建一个禁止子任务的请求
         * @param parentTaskId
         * @param reason
         * @param waitForCompletion
         * @return
         */
        static BanParentTaskRequest createSetBanParentTaskRequest(TaskId parentTaskId, String reason, boolean waitForCompletion) {
            return new BanParentTaskRequest(parentTaskId, reason, waitForCompletion);
        }

        static BanParentTaskRequest createRemoveBanParentTaskRequest(TaskId parentTaskId) {
            return new BanParentTaskRequest(parentTaskId);
        }

        private BanParentTaskRequest(TaskId parentTaskId, String reason, boolean waitForCompletion) {
            this.parentTaskId = parentTaskId;
            this.ban = true;
            this.reason = reason;
            this.waitForCompletion = waitForCompletion;
        }

        private BanParentTaskRequest(TaskId parentTaskId) {
            this.parentTaskId = parentTaskId;
            this.ban = false;
            this.reason = null;
            this.waitForCompletion = false;
        }

        private BanParentTaskRequest(StreamInput in) throws IOException {
            super(in);
            parentTaskId = TaskId.readFromStream(in);
            ban = in.readBoolean();
            reason = ban ? in.readString() : null;
            if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
                waitForCompletion = in.readBoolean();
            } else {
                waitForCompletion = false;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            parentTaskId.writeTo(out);
            out.writeBoolean(ban);
            if (ban) {
                out.writeString(reason);
            }
            if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
                out.writeBoolean(waitForCompletion);
            }
        }
    }

    /**
     * 处理禁止子任务的req
     */
    class BanParentRequestHandler implements TransportRequestHandler<BanParentTaskRequest> {
        @Override
        public void messageReceived(final BanParentTaskRequest request, final TransportChannel channel, Task task) throws Exception {
            // 代表要禁止任务
            if (request.ban) {
                logger.debug("Received ban for the parent [{}] on the node [{}], reason: [{}]", request.parentTaskId,
                    clusterService.localNode().getId(), request.reason);
                // 将子任务标记成禁止状态
                final List<CancellableTask> childTasks = taskManager.setBan(request.parentTaskId, request.reason);
                final GroupedActionListener<Void> listener = new GroupedActionListener<>(ActionListener.map(
                    new ChannelActionListener<>(channel, BAN_PARENT_ACTION_NAME, request), r -> TransportResponse.Empty.INSTANCE),
                    childTasks.size() + 1);
                for (CancellableTask childTask : childTasks) {
                    // 这里会继续检测是否存在子级任务 并继续进行关闭
                    cancelTaskAndDescendants(childTask, request.reason, request.waitForCompletion, listener);
                }
                listener.onResponse(null);
            } else {
                // 代表从禁止状态解除
                logger.debug("Removing ban for the parent [{}] on the node [{}]", request.parentTaskId,
                    clusterService.localNode().getId());
                taskManager.removeBan(request.parentTaskId);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }

}
