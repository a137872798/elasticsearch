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

package org.elasticsearch.action.admin.cluster.reroute;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.command.AbstractAllocateAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 在 allocationExplain 和 reroute中都使用到了allocationService
 */
public class TransportClusterRerouteAction extends TransportMasterNodeAction<ClusterRerouteRequest, ClusterRerouteResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterRerouteAction.class);

    private final AllocationService allocationService;

    @Inject
    public TransportClusterRerouteAction(TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, AllocationService allocationService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ClusterRerouteAction.NAME, transportService, clusterService, threadPool, actionFilters,
              ClusterRerouteRequest::new, indexNameExpressionResolver);
        this.allocationService = allocationService;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    /**
     * 修改路由信息是一种修改元数据的操作
     * @param request
     * @param state
     * @return
     */
    @Override
    protected ClusterBlockException checkBlock(ClusterRerouteRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterRerouteResponse read(StreamInput in) throws IOException {
        return new ClusterRerouteResponse(in);
    }

    /**
     * 在主节点上处理
     * @param task
     * @param request
     * @param state
     * @param listener
     */
    @Override
    protected void masterOperation(Task task, final ClusterRerouteRequest request, final ClusterState state,
                                   final ActionListener<ClusterRerouteResponse> listener) {
        Map<String, List<AbstractAllocateAllocationCommand>> stalePrimaryAllocations = new HashMap<>();
        for (AllocationCommand command : request.getCommands().commands()) {
            // 这里只处理某种特定类型的分配指令
            // 针对陈旧的主分片的分配指令
            if (command instanceof AllocateStalePrimaryAllocationCommand) {
                final AllocateStalePrimaryAllocationCommand cmd = (AllocateStalePrimaryAllocationCommand) command;
                stalePrimaryAllocations.computeIfAbsent(cmd.index(), k -> new ArrayList<>()).add(cmd);
            }
        }
        if (stalePrimaryAllocations.isEmpty()) {
            submitStateUpdate(request, listener);
        } else {
            verifyThenSubmitUpdate(request, listener, stalePrimaryAllocations);
        }
    }

    /**
     * 当本次的处理命令中包含 AllocateStalePrimaryAllocationCommand 时 触发该方法
     * @param request
     * @param listener
     * @param stalePrimaryAllocations
     */
    private void verifyThenSubmitUpdate(ClusterRerouteRequest request, ActionListener<ClusterRerouteResponse> listener,
        Map<String, List<AbstractAllocateAllocationCommand>> stalePrimaryAllocations) {
        // 还是交给本节点执行 这里替换成了另一种指令
        transportService.sendRequest(transportService.getLocalNode(), IndicesShardStoresAction.NAME,
            new IndicesShardStoresRequest().indices(stalePrimaryAllocations.keySet().toArray(Strings.EMPTY_ARRAY)),
            new ActionListenerResponseHandler<>(
                ActionListener.wrap(
                    response -> {
                        ImmutableOpenMap<String, ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>>> status =
                            response.getStoreStatuses();
                        Exception e = null;
                        for (Map.Entry<String, List<AbstractAllocateAllocationCommand>> entry : stalePrimaryAllocations.entrySet()) {
                            final String index = entry.getKey();
                            final ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>> indexStatus = status.get(index);
                            if (indexStatus == null) {
                                // The index in the stale primary allocation request was green and hence filtered out by the store status
                                // request. We ignore it here since the relevant exception will be thrown by the reroute action later on.
                                continue;
                            }
                            for (AbstractAllocateAllocationCommand command : entry.getValue()) {
                                final List<IndicesShardStoresResponse.StoreStatus> shardStatus =
                                    indexStatus.get(command.shardId());
                                if (shardStatus == null || shardStatus.isEmpty()) {
                                    e = ExceptionsHelper.useOrSuppress(e, new IllegalArgumentException(
                                        "No data for shard [" + command.shardId() + "] of index [" + index + "] found on any node")
                                    );
                                } else if (shardStatus.stream().noneMatch(storeStatus -> {
                                    final DiscoveryNode node = storeStatus.getNode();
                                    final String nodeInCommand = command.node();
                                    return nodeInCommand.equals(node.getName()) || nodeInCommand.equals(node.getId());
                                })) {
                                    e = ExceptionsHelper.useOrSuppress(e, new IllegalArgumentException(
                                        "No data for shard [" + command.shardId() + "] of index [" + index + "] found on node ["
                                            + command.node() + ']'));
                                }
                            }
                        }
                        if (e == null) {
                            submitStateUpdate(request, listener);
                        } else {
                            listener.onFailure(e);
                        }
                    }, listener::onFailure
                ), IndicesShardStoresResponse::new));
    }

    /**
     * 当不包含陈旧的主分片时 触发该方法
     * @param request
     * @param listener
     */
    private void submitStateUpdate(final ClusterRerouteRequest request, final ActionListener<ClusterRerouteResponse> listener) {
        clusterService.submitStateUpdateTask("cluster_reroute (api)",
            new ClusterRerouteResponseAckedClusterStateUpdateTask(logger, allocationService, request,
                ActionListener.map(listener,
                    response -> {
                        if (request.dryRun() == false) {
                            response.getExplanations().getYesDecisionMessages().forEach(logger::info);
                        }
                        return response;
                    })));
    }

    /**
     * 更新集群的任务 并且在集群的大多数节点处理完后 会触发内部的监听器
     */
    static class ClusterRerouteResponseAckedClusterStateUpdateTask extends AckedClusterStateUpdateTask<ClusterRerouteResponse> {

        private final ClusterRerouteRequest request;
        private final ActionListener<ClusterRerouteResponse> listener;
        private final Logger logger;
        private final AllocationService allocationService;
        private volatile ClusterState clusterStateToSend;
        private volatile RoutingExplanations explanations;

        ClusterRerouteResponseAckedClusterStateUpdateTask(Logger logger, AllocationService allocationService, ClusterRerouteRequest request,
                                                          ActionListener<ClusterRerouteResponse> listener) {
            super(Priority.IMMEDIATE, request, listener);
            this.request = request;
            this.listener = listener;
            this.logger = logger;
            this.allocationService = allocationService;
        }

        @Override
        protected ClusterRerouteResponse newResponse(boolean acknowledged) {
            return new ClusterRerouteResponse(acknowledged, clusterStateToSend, explanations);
        }

        @Override
        public void onAckTimeout() {
            listener.onResponse(new ClusterRerouteResponse(false, clusterStateToSend, new RoutingExplanations()));
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.debug(() -> new ParameterizedMessage("failed to perform [{}]", source), e);
            super.onFailure(source, e);
        }

        /**
         * 根据现有的集群状态进行更新 并将新的状态发布给集群的其他节点
         * @param currentState
         * @return
         */
        @Override
        public ClusterState execute(ClusterState currentState) {
            AllocationService.CommandsResult commandsResult =
                allocationService.reroute(currentState, request.getCommands(), request.explain(), request.isRetryFailed());
            clusterStateToSend = commandsResult.getClusterState();
            explanations = commandsResult.explanations();
            if (request.dryRun()) {
                return currentState;
            }
            return commandsResult.getClusterState();
        }
    }
}
