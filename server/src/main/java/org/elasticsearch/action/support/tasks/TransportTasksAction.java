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

package org.elasticsearch.action.support.tasks;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.NoSuchNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeShouldNotConnectException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;

/**
 * The base class for transport actions that are interacting with currently running tasks.
 * 这个action是专门用于处理 有关task的指令的
 */
public abstract class TransportTasksAction<
        OperationTask extends Task,
        TasksRequest extends BaseTasksRequest<TasksRequest>,
        TasksResponse extends BaseTasksResponse,
        TaskResponse extends Writeable
    > extends HandledTransportAction<TasksRequest, TasksResponse> {

    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final Writeable.Reader<TasksRequest> requestReader;
    protected final Writeable.Reader<TasksResponse> responsesReader;
    protected final Writeable.Reader<TaskResponse> responseReader;

    protected final String transportNodeAction;

    /**
     *
     * @param actionName
     * @param clusterService
     * @param transportService
     * @param actionFilters
     * 下面几个reader对象定义了如何将数据流转换为实体
     * @param requestReader
     * @param responsesReader
     * @param responseReader
     * @param nodeExecutor
     */
    protected TransportTasksAction(String actionName, ClusterService clusterService, TransportService transportService,
                                   ActionFilters actionFilters, Writeable.Reader<TasksRequest> requestReader,
                                   Writeable.Reader<TasksResponse> responsesReader, Writeable.Reader<TaskResponse> responseReader,
                                   String nodeExecutor) {
        super(actionName, transportService, actionFilters, requestReader);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.transportNodeAction = actionName + "[n]";
        this.requestReader = requestReader;
        this.responsesReader = responsesReader;
        this.responseReader = responseReader;

        // 原本该对象在初始化时就会注册一个处理器到传输层
        // 这里又额外注册了一组处理器  区别是这个actionName 额外携带了 [n]
        transportService.registerRequestHandler(transportNodeAction, nodeExecutor, NodeTaskRequest::new, new NodeTransportHandler());
    }

    /**
     * 当通过NodeClient处理请求时 最先转发到这个方法
     * @param task
     * @param request
     * @param listener
     */
    @Override
    protected void doExecute(Task task, TasksRequest request, ActionListener<TasksResponse> listener) {
        new AsyncAction(task, request, listener).start();
    }

    /**
     * 真正处理请求的函数
     * @param nodeTaskRequest
     * @param listener
     */
    private void nodeOperation(NodeTaskRequest nodeTaskRequest, ActionListener<NodeTasksResponse> listener) {
        TasksRequest request = nodeTaskRequest.tasksRequest;

        // 找到条件匹配的task 并设置到列表中
        List<OperationTask> tasks = new ArrayList<>();
        processTasks(request, tasks::add);
        // 代表没有匹配的task 不需要处理
        if (tasks.isEmpty()) {
            listener.onResponse(new NodeTasksResponse(clusterService.localNode().getId(), emptyList(), emptyList()));
            return;
        }
        AtomicArray<Tuple<TaskResponse, Exception>> responses = new AtomicArray<>(tasks.size());
        final AtomicInteger counter = new AtomicInteger(tasks.size());
        for (int i = 0; i < tasks.size(); i++) {
            final int taskIndex = i;
            ActionListener<TaskResponse> taskListener = new ActionListener<TaskResponse>() {
                @Override
                public void onResponse(TaskResponse response) {
                    responses.setOnce(taskIndex, response == null ? null : new Tuple<>(response, null));
                    respondIfFinished();
                }

                @Override
                public void onFailure(Exception e) {
                    responses.setOnce(taskIndex, new Tuple<>(null, e));
                    respondIfFinished();
                }

                /**
                 * 每当某个task处理有结果时
                 */
                private void respondIfFinished() {
                    if (counter.decrementAndGet() != 0) {
                        return;
                    }
                    List<TaskResponse> results = new ArrayList<>();
                    List<TaskOperationFailure> exceptions = new ArrayList<>();
                    for (Tuple<TaskResponse, Exception> response : responses.asList()) {
                        if (response.v1() == null) {
                            assert response.v2() != null;
                            exceptions.add(new TaskOperationFailure(clusterService.localNode().getId(), tasks.get(taskIndex).getId(),
                                    response.v2()));
                        } else {
                            assert response.v2() == null;
                            results.add(response.v1());
                        }
                    }
                    listener.onResponse(new NodeTasksResponse(clusterService.localNode().getId(), results, exceptions));
                }
            };
            try {
                // 处理每个任务 并将结果通知给监听器   由子类实现
                taskOperation(request, tasks.get(taskIndex), taskListener);
            } catch (Exception e) {
                taskListener.onFailure(e);
            }
        }
    }

    protected String[] filterNodeIds(DiscoveryNodes nodes, String[] nodesIds) {
        return nodesIds;
    }

    /**
     * 解析nodeIds
     * @param request
     * @param clusterState
     * @return
     */
    protected String[] resolveNodes(TasksRequest request, ClusterState clusterState) {
        // 在rest请求参数中可能会携带task信息 这样就可以直接对应到目标节点
        if (request.getTaskId().isSet()) {
            return new String[]{request.getTaskId().getNodeId()};
        } else {
            // 否则从nodeIds中找到有效的参数   这里一系列的匹配规则就先不看了
            return clusterState.nodes().resolveNodes(request.getNodes());
        }
    }

    /**
     * 从任务管理器中找到匹配的任务 并进行处理
     * @param request
     * @param operation  处理的函数
     */
    protected void processTasks(TasksRequest request, Consumer<OperationTask> operation) {
        // 这里只是寻找任务
        if (request.getTaskId().isSet()) {
            // we are only checking one task, we can optimize it
            Task task = taskManager.getTask(request.getTaskId().getId());
            if (task != null) {
                if (request.match(task)) {
                    operation.accept((OperationTask) task);
                } else {
                    throw new ResourceNotFoundException("task [{}] doesn't support this operation", request.getTaskId());
                }
            } else {
                throw new ResourceNotFoundException("task [{}] is missing", request.getTaskId());
            }
        } else {
            // 找到所有符合条件的task
            for (Task task : taskManager.getTasks().values()) {
                if (request.match(task)) {
                    operation.accept((OperationTask) task);
                }
            }
        }
    }

    protected abstract TasksResponse newResponse(TasksRequest request, List<TaskResponse> tasks, List<TaskOperationFailure>
        taskOperationFailures, List<FailedNodeException> failedNodeExceptions);


    /**
     *
     * @param request
     * @param responses  数组中每个元素对应一个结果
     * @return
     */
    @SuppressWarnings("unchecked")
    protected TasksResponse newResponse(TasksRequest request, AtomicReferenceArray responses) {
        List<TaskResponse> tasks = new ArrayList<>();
        List<FailedNodeException> failedNodeExceptions = new ArrayList<>();
        List<TaskOperationFailure> taskOperationFailures = new ArrayList<>();
        // 将响应结果进行分组
        for (int i = 0; i < responses.length(); i++) {
            Object response = responses.get(i);
            if (response instanceof FailedNodeException) {
                failedNodeExceptions.add((FailedNodeException) response);
            } else {
                NodeTasksResponse tasksResponse = (NodeTasksResponse) response;
                if (tasksResponse.results != null) {
                    tasks.addAll(tasksResponse.results);
                }
                if (tasksResponse.exceptions != null) {
                    taskOperationFailures.addAll(tasksResponse.exceptions);
                }
            }
        }
        // 当本次没有有效的node时 传入2个空列表
        return newResponse(request, tasks, taskOperationFailures, failedNodeExceptions);
    }

    /**
     * Perform the required operation on the task. It is OK start an asynchronous operation or to throw an exception but not both.
     */
    protected abstract void taskOperation(TasksRequest request, OperationTask task, ActionListener<TaskResponse> listener);

    /**
     * 当接收到一个请求时 生成一个该对象 并进行处理  在本节点直接处理时 还不会切换线程
     */
    private class AsyncAction {

        private final TasksRequest request;
        private final String[] nodesIds;
        private final DiscoveryNode[] nodes;
        private final ActionListener<TasksResponse> listener;

        /**
         * 每个node 会对应一个res
         */
        private final AtomicReferenceArray<Object> responses;
        private final AtomicInteger counter = new AtomicInteger();
        private final Task task;

        /**
         *
         * @param task  请求会先被包装成task对象
         * @param request
         * @param listener
         */
        private AsyncAction(Task task, TasksRequest request, ActionListener<TasksResponse> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;
            ClusterState clusterState = clusterService.state();
            // 根据请求体信息 解析出一组nodeId
            String[] nodesIds = resolveNodes(request, clusterState);
            // filterNodeIds是开放给子类的钩子 默认是NOOP
            this.nodesIds = filterNodeIds(clusterState.nodes(), nodesIds);
            ImmutableOpenMap<String, DiscoveryNode> nodes = clusterState.nodes().getNodes();
            // 从state中 通过nodeId找到匹配的node对象
            this.nodes = new DiscoveryNode[nodesIds.length];
            for (int i = 0; i < this.nodesIds.length; i++) {
                this.nodes[i] = nodes.get(this.nodesIds[i]);
            }
            this.responses = new AtomicReferenceArray<>(this.nodesIds.length);
        }

        /**
         * 当确定了本次相关任务所在的多个node后 开始处理
         */
        private void start() {
            // 对应的node不存在与集群中 或者其他原因 这些node都是无效的 直接触发监听器
            if (nodesIds.length == 0) {
                // nothing to do
                try {
                    listener.onResponse(newResponse(request, responses));
                } catch (Exception e) {
                    logger.debug("failed to generate empty response", e);
                    listener.onFailure(e);
                }
            } else {
                // 将请求发往相关节点 并处理task (比如关闭/查询)
                TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
                if (request.getTimeout() != null) {
                    builder.withTimeout(request.getTimeout());
                }
                for (int i = 0; i < nodesIds.length; i++) {
                    final String nodeId = nodesIds[i];
                    final int idx = i;
                    final DiscoveryNode node = nodes[i];
                    try {
                        // 某个节点不存在 直接以失败作为结果
                        if (node == null) {
                            onFailure(idx, nodeId, new NoSuchNodeException(nodeId));
                        } else {
                            NodeTaskRequest nodeRequest = new NodeTaskRequest(request);
                            // 这些请求都是由最早的请求衍生的 所以构建链路关系
                            nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                            // 注意这里使用的action 是加上[n] 的 其他节点接收后会通过 nodeOperation进行处理
                            // 这个node也可能会包含自己
                            transportService.sendRequest(node, transportNodeAction, nodeRequest, builder.build(),
                                new TransportResponseHandler<NodeTasksResponse>() {
                                    @Override
                                    public NodeTasksResponse read(StreamInput in) throws IOException {
                                        return new NodeTasksResponse(in);
                                    }

                                    @Override
                                    public void handleResponse(NodeTasksResponse response) {
                                        onOperation(idx, response);
                                    }

                                    @Override
                                    public void handleException(TransportException exp) {
                                        onFailure(idx, node.getId(), exp);
                                    }

                                    @Override
                                    public String executor() {
                                        return ThreadPool.Names.SAME;
                                    }
                                });
                        }
                    } catch (Exception e) {
                        onFailure(idx, nodeId, e);
                    }
                }
            }
        }

        private void onOperation(int idx, NodeTasksResponse nodeResponse) {
            responses.set(idx, nodeResponse);
            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        /**
         * 在往某个节点发送请求时有关处理外的异常 都会通过该方法进行处理
         * @param idx
         * @param nodeId
         * @param t
         */
        private void onFailure(int idx, String nodeId, Throwable t) {
            if (logger.isDebugEnabled() && !(t instanceof NodeShouldNotConnectException)) {
                logger.debug(new ParameterizedMessage("failed to execute on node [{}]", nodeId), t);
            }

            responses.set(idx, new FailedNodeException(nodeId, "Failed node [" + nodeId + "]", t));

            if (counter.incrementAndGet() == responses.length()) {
                finishHim();
            }
        }

        /**
         * 代表发往每个node的请求都已经收到回复 可以根据现有的信息生成最终结果了
         */
        private void finishHim() {
            TasksResponse finalResponse;
            try {
                finalResponse = newResponse(request, responses);
            } catch (Exception e) {
                logger.debug("failed to combine responses from nodes", e);
                listener.onFailure(e);
                return;
            }
            listener.onResponse(finalResponse);
        }
    }

    /**
     * 当传入 actionName[n] 时不会通过触发handler的execute处理请求 而是通过nodeOperation处理
     */
    class NodeTransportHandler implements TransportRequestHandler<NodeTaskRequest> {

        @Override
        public void messageReceived(final NodeTaskRequest request, final TransportChannel channel, Task task) throws Exception {
            nodeOperation(request, ActionListener.wrap(channel::sendResponse,
                e -> {
                    try {
                        channel.sendResponse(e);
                    } catch (IOException e1) {
                        e1.addSuppressed(e);
                        logger.warn("Failed to send failure", e1);
                    }
                }
            ));
        }
    }

    private class NodeTaskRequest extends TransportRequest {
        private TasksRequest tasksRequest;

        protected NodeTaskRequest(StreamInput in) throws IOException {
            super(in);
            this.tasksRequest = requestReader.read(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            tasksRequest.writeTo(out);
        }

        protected NodeTaskRequest(TasksRequest tasksRequest) {
            super();
            this.tasksRequest = tasksRequest;
        }

    }

    private class NodeTasksResponse extends TransportResponse {
        protected String nodeId;
        protected List<TaskOperationFailure> exceptions;
        protected List<TaskResponse> results;

        NodeTasksResponse(StreamInput in) throws IOException {
            super(in);
            nodeId = in.readString();
            int resultsSize = in.readVInt();
            results = new ArrayList<>(resultsSize);
            for (; resultsSize > 0; resultsSize--) {
                final TaskResponse result = in.readBoolean() ? responseReader.read(in) : null;
                results.add(result);
            }
            if (in.readBoolean()) {
                int taskFailures = in.readVInt();
                exceptions = new ArrayList<>(taskFailures);
                for (int i = 0; i < taskFailures; i++) {
                    exceptions.add(new TaskOperationFailure(in));
                }
            } else {
                exceptions = null;
            }
        }

        NodeTasksResponse(String nodeId,
                                 List<TaskResponse> results,
                                 List<TaskOperationFailure> exceptions) {
            this.nodeId = nodeId;
            this.results = results;
            this.exceptions = exceptions;
        }

        public String getNodeId() {
            return nodeId;
        }

        public List<TaskOperationFailure> getExceptions() {
            return exceptions;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeVInt(results.size());
            for (TaskResponse result : results) {
                if (result != null) {
                    out.writeBoolean(true);
                    result.writeTo(out);
                } else {
                    out.writeBoolean(false);
                }
            }
            out.writeBoolean(exceptions != null);
            if (exceptions != null) {
                int taskFailures = exceptions.size();
                out.writeVInt(taskFailures);
                for (TaskOperationFailure exception : exceptions) {
                    exception.writeTo(out);
                }
            }
        }
    }
}
