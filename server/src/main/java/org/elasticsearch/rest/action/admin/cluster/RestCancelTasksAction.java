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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.admin.cluster.RestListTasksAction.listTasksResponseListener;

/**
 * 该处理器是处理rest请求中的 task_cancel
 */
public class RestCancelTasksAction extends BaseRestHandler {
    private final Supplier<DiscoveryNodes> nodesInCluster;

    public RestCancelTasksAction(Supplier<DiscoveryNodes> nodesInCluster) {
        this.nodesInCluster = nodesInCluster;
    }

    @Override
    public String getName() {
        return "cancel_tasks_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_tasks/_cancel"),
            new Route(POST, "/_tasks/{task_id}/_cancel"));
    }

    /**
     * 定义了如何处理请求对象
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return
     * @throws IOException
     */
    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        // 可能有多种途径可以关闭任务

        // 从请求体中 获取参数信息 拆解成一组nodeid
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodes"));
        // 每当要处理请求时 就是先封装成一个Task对象 这里可以从外部直接关闭任务吗
        final TaskId taskId = new TaskId(request.param("task_id"));
        final String[] actions = Strings.splitStringByCommaToArray(request.param("actions"));
        final TaskId parentTaskId = new TaskId(request.param("parent_task_id"));
        final String groupBy = request.param("group_by", "nodes");

        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTaskId(taskId);
        cancelTasksRequest.setNodes(nodesIds);
        cancelTasksRequest.setActions(actions);
        cancelTasksRequest.setParentTaskId(parentTaskId);
        // 是否要阻塞等待任务被关闭
        cancelTasksRequest.setWaitForCompletion(request.paramAsBoolean("wait_for_completion", cancelTasksRequest.waitForCompletion()));
        return channel ->
            // listTasksResponseListener 这个监听器只是定义了 res会以什么形式展示
            client.admin().cluster().cancelTasks(cancelTasksRequest, listTasksResponseListener(nodesInCluster, groupBy, channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

}
