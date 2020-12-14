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

package org.elasticsearch.client.node;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskListener;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Client that executes actions on the local node.
 * 该client会处理node内部所有的请求
 */
public class NodeClient extends AbstractClient {

    /**
     * 应用层action 与传输层action的映射
     */
    @SuppressWarnings("rawtypes")
    private Map<ActionType, TransportAction> actions;

    /**
     * 该对象负责处理所有的任务
     */
    private TaskManager taskManager;

    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
     * {@link #executeLocally(ActionType, ActionRequest, TaskListener)}.
     * 获取本地nodeId
     */
    private Supplier<String> localNodeId;
    /**
     * 某些功能的实现可能需要读取集群中所有信息 而不能仅依靠当前节点 所以需要一个能与集群通信的服务
     */
    private RemoteClusterService remoteClusterService;

    public NodeClient(Settings settings, ThreadPool threadPool) {
        super(settings, threadPool);
    }

    /**
     * @param actions 从IOC容器中获取所有键值对
     * @param taskManager 任务管理器 每当client处理请求前 都会在注册到该对象上
     * @param localNodeId
     * @param remoteClusterService  与远端集群通信的服务
     */
    @SuppressWarnings("rawtypes")
    public void initialize(Map<ActionType, TransportAction> actions, TaskManager taskManager, Supplier<String> localNodeId,
                           RemoteClusterService remoteClusterService) {
        this.actions = actions;
        this.taskManager = taskManager;
        this.localNodeId = localNodeId;
        this.remoteClusterService = remoteClusterService;
    }

    @Override
    public void close() {
        // nothing really to do
    }

    /**
     * ES节点对外开放REST接口 当收到某个请求时 会转发给NodeClient处理
     * @param action  req对应的指令类型
     * @param request
     * @param listener  当处理完成后通过监听器处理结果 也就是异步的  通常就是将结果通过 RESTChannel返回给客户端
     *                  因为底层使用了reactor模型线程  比如Netty 所以触发execute的线程都是IO线程  所以处理逻辑都需要通过业务线程执行
     * @param <Request>
     * @param <Response>
     */
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse>
    void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        // Discard the task because the Client interface doesn't use it.
        executeLocally(action, request, listener);
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link Task} used to track it, and linking an {@link ActionListener}.
     * Prefer this method if you don't need access to the task when listening for the response. This is the method used to
     * implement the {@link Client} interface.
     */
    public <Request extends ActionRequest, Response extends ActionResponse>
    Task executeLocally(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        return taskManager.registerAndExecute("transport", transportAction(action), request,
            (t, r) -> listener.onResponse(r), (t, e) -> listener.onFailure(e));
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link Task} used to track it, and linking an {@link TaskListener}.
     * Prefer this method if you need access to the task when listening for the response.
     */
    public <    Request extends ActionRequest,
                Response extends ActionResponse
            > Task executeLocally(ActionType<Response> action, Request request, TaskListener<Response> listener) {
        return taskManager.registerAndExecute("transport", transportAction(action), request,
            listener::onResponse, listener::onFailure);
    }

    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
     * {@link #executeLocally(ActionType, ActionRequest, TaskListener)}.
     */
    public String getLocalNodeId() {
        return localNodeId.get();
    }

    /**
     * Get the {@link TransportAction} for an {@link ActionType}, throwing exceptions if the action isn't available.
     * 在应用层的action 与传输层的action 存在一个映射关系
     */
    @SuppressWarnings("unchecked")
    private <    Request extends ActionRequest,
                Response extends ActionResponse
            > TransportAction<Request, Response> transportAction(ActionType<Response> action) {
        if (actions == null) {
            throw new IllegalStateException("NodeClient has not been initialized");
        }
        TransportAction<Request, Response> transportAction = actions.get(action);
        if (transportAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        return transportAction;
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return remoteClusterService.getRemoteClusterClient(threadPool(), clusterAlias);
    }
}
