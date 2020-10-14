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
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;

import java.util.List;

/**
 * 该对象用于处理 从集群中移除节点的task
 */
public class NodeRemovalClusterStateTaskExecutor implements ClusterStateTaskExecutor<NodeRemovalClusterStateTaskExecutor.Task>,
    ClusterStateTaskListener {

    /**
     * 分配服务 主要是为分片 决定分配的node
     */
    private final AllocationService allocationService;
    private final Logger logger;

    public static class Task {

        /**
         * 准备从集群移除的node
         */
        private final DiscoveryNode node;
        private final String reason;

        public Task(final DiscoveryNode node, final String reason) {
            this.node = node;
            this.reason = reason;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String reason() {
            return reason;
        }

        @Override
        public String toString() {
            return node + " reason: " + reason;
        }
    }

    public NodeRemovalClusterStateTaskExecutor(
            final AllocationService allocationService,
            final Logger logger) {
        this.allocationService = allocationService;
        this.logger = logger;
    }

    /**
     * 执行移除任务
     * @param currentState
     * @param tasks
     * @return
     * @throws Exception
     */
    @Override
    public ClusterTasksResult<Task> execute(final ClusterState currentState, final List<Task> tasks) throws Exception {
        // 先根据当前集群中的节点生成 DiscoveryNodes
        final DiscoveryNodes.Builder remainingNodesBuilder = DiscoveryNodes.builder(currentState.nodes());
        boolean removed = false;
        for (final Task task : tasks) {
            if (currentState.nodes().nodeExists(task.node())) {
                remainingNodesBuilder.remove(task.node());
                removed = true;
            } else {
                logger.debug("node [{}] does not exist in cluster state, ignoring", task);
            }
        }

        // 代表有节点被移除
        if (!removed) {
            // no nodes to remove, keep the current cluster state
            return ClusterTasksResult.<Task>builder().successes(tasks).build(currentState);
        }

        // 更新集群状态
        final ClusterState remainingNodesClusterState = remainingNodesClusterState(currentState, remainingNodesBuilder);
        // 将每个任务的执行情况 以及最新的集群状态合成 taskResult
        return getTaskClusterTasksResult(currentState, tasks, remainingNodesClusterState);
    }

    protected ClusterTasksResult<Task> getTaskClusterTasksResult(ClusterState currentState, List<Task> tasks,
                                                                 ClusterState remainingNodesClusterState) {
        // 随着某些node的移除 不再需要维护他们的持久化任务
        ClusterState ptasksDisassociatedState = PersistentTasksCustomMetadata.disassociateDeadNodes(remainingNodesClusterState);
        final ClusterTasksResult.Builder<Task> resultBuilder = ClusterTasksResult.<Task>builder().successes(tasks);
        // 将分配到这些节点上的分片重新分配到其他节点
        return resultBuilder.build(allocationService.disassociateDeadNodes(ptasksDisassociatedState, true, describeTasks(tasks)));
    }

    // visible for testing
    // hook is used in testing to ensure that correct cluster state is used to test whether a
    // rejoin or reroute is needed
    protected ClusterState remainingNodesClusterState(final ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
        return ClusterState.builder(currentState).nodes(remainingNodesBuilder).build();
    }

    @Override
    public void onFailure(final String source, final Exception e) {
        logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
    }

    @Override
    public void onNoLongerMaster(String source) {
        logger.debug("no longer master while processing node removal [{}]", source);
    }

}
