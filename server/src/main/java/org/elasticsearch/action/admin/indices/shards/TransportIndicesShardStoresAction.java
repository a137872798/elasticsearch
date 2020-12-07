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
package org.elasticsearch.action.admin.indices.shards;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterShardHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.gateway.AsyncShardFetch.Lister;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Transport action that reads the cluster state for shards with the requested criteria (see {@link ClusterHealthStatus}) of specific
 * indices and fetches store information from all the nodes using {@link TransportNodesListGatewayStartedShards}
 * 获取所有分片当前存储状态的请求
 */
public class TransportIndicesShardStoresAction
        extends TransportMasterNodeReadAction<IndicesShardStoresRequest, IndicesShardStoresResponse> {

    private static final Logger logger = LogManager.getLogger(TransportIndicesShardStoresAction.class);

    private final NodeClient client;

    @Inject
    public TransportIndicesShardStoresAction(TransportService transportService, ClusterService clusterService,
                                             ThreadPool threadPool, ActionFilters actionFilters,
                                             IndexNameExpressionResolver indexNameExpressionResolver, NodeClient client) {
        super(IndicesShardStoresAction.NAME, transportService, clusterService, threadPool, actionFilters,
            IndicesShardStoresRequest::new, indexNameExpressionResolver);
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected IndicesShardStoresResponse read(StreamInput in) throws IOException {
        return new IndicesShardStoresResponse(in);
    }

    /**
     * 在主节点处理请求
     * @param task
     * @param request
     * @param state
     * @param listener
     */
    @Override
    protected void masterOperation(Task task, IndicesShardStoresRequest request, ClusterState state,
                                   ActionListener<IndicesShardStoresResponse> listener) {
        final RoutingTable routingTables = state.routingTable();
        final RoutingNodes routingNodes = state.getRoutingNodes();
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        final Set<Tuple<ShardId, String>> shardsToFetch = new HashSet<>();

        logger.trace("using cluster state version [{}] to determine shards", state.version());
        // collect relevant shard ids of the requested indices for fetching store infos
        for (String index : concreteIndices) {
            IndexRoutingTable indexShardRoutingTables = routingTables.index(index);
            if (indexShardRoutingTables == null) {
                continue;
            }
            // 可能在配置中会设置 自定义的数据路径
            final String customDataPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(state.metadata().index(index).getSettings());
            // 以 shardId 作为分界线
            for (IndexShardRoutingTable routing : indexShardRoutingTables) {
                final int shardId = routing.shardId().id();
                ClusterShardHealth shardHealth = new ClusterShardHealth(shardId, routing);
                // req携带了什么 status 就代表想要获取哪些状态的分片信息
                if (request.shardStatuses().contains(shardHealth.getStatus())) {
                    shardsToFetch.add(Tuple.tuple(routing.shardId(), customDataPath));
                }
            }
        }

        // async fetch store infos from all the nodes
        // NOTE: instead of fetching shard store info one by one from every node (nShards * nNodes requests)
        // we could fetch all shard store info from every node once (nNodes requests)
        // we have to implement a TransportNodesAction instead of using TransportNodesListGatewayStartedShards
        // for fetching shard stores info, that operates on a list of shards instead of a single shard
        // 开始发起一个从分片仓库拉取数据的任务
        new AsyncShardStoresInfoFetches(state.nodes(), routingNodes, shardsToFetch, listener).start();
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesShardStoresRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
            indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    /**
     * 获取当前所有分片存储状态的请求
     */
    private class AsyncShardStoresInfoFetches {
        private final DiscoveryNodes nodes;
        private final RoutingNodes routingNodes;
        private final Set<Tuple<ShardId, String>> shards;
        private final ActionListener<IndicesShardStoresResponse> listener;
        private CountDown expectedOps;

        /**
         * 每个shard处理后的结果会存储到queue中
         */
        private final Queue<InternalAsyncFetch.Response> fetchResponses;

        /**
         *
         * @param nodes  记录当前集群中所有节点的信息
         * @param routingNodes  以node为单位描述node下所有的分片信息
         * @param shards  哪些shard需要恢复数据
         * @param listener
         */
        AsyncShardStoresInfoFetches(DiscoveryNodes nodes, RoutingNodes routingNodes, Set<Tuple<ShardId, String>> shards,
                                    ActionListener<IndicesShardStoresResponse> listener) {
            this.nodes = nodes;
            this.routingNodes = routingNodes;
            this.shards = shards;
            this.listener = listener;
            this.fetchResponses = new ConcurrentLinkedQueue<>();
            // 只有当所有分片都处理完成时  才触发监听器
            this.expectedOps = new CountDown(shards.size());
        }

        void start() {
            if (shards.isEmpty()) {
                listener.onResponse(new IndicesShardStoresResponse());
            } else {
                // explicitely type lister, some IDEs (Eclipse) are not able to correctly infer the function type
                // 这个对象定义了如何拉取数据的逻辑
                Lister<BaseNodesResponse<NodeGatewayStartedShards>, NodeGatewayStartedShards> lister = this::listStartedShards;
                for (Tuple<ShardId, String> shard : shards) {
                    // 以分片为单位 开始拉取数据
                    InternalAsyncFetch fetch = new InternalAsyncFetch(logger, "shard_stores", shard.v1(), shard.v2(), lister);
                    fetch.fetchData(nodes, Collections.<String>emptySet());
                }
            }
        }


        /**
         * 开始处理某个分片的拉取任务
         * @param shardId 拉取的分片id
         * @param customDataPath
         * @param nodes
         * @param listener
         */
        private void listStartedShards(ShardId shardId, String customDataPath, DiscoveryNode[] nodes,
                                       ActionListener<BaseNodesResponse<NodeGatewayStartedShards>> listener) {
            var request = new TransportNodesListGatewayStartedShards.Request(shardId, customDataPath, nodes);
            client.executeLocally(TransportNodesListGatewayStartedShards.TYPE, request,
                ActionListener.wrap(listener::onResponse, listener::onFailure));
        }

        /**
         * 以shard为单位  执行拉取store数据的任务
         */
        private class InternalAsyncFetch extends AsyncShardFetch<NodeGatewayStartedShards> {

            InternalAsyncFetch(Logger logger, String type, ShardId shardId, String customDataPath,
                               Lister<? extends BaseNodesResponse<NodeGatewayStartedShards>, NodeGatewayStartedShards> action) {
                super(logger, type, shardId, customDataPath, action);
            }

            /**
             * 这里已经覆盖了父类的方法  就是当单个分片在所有相关节点上都收到结果后触发
             * 因为一个shardId 会对应一个primary 多个replica
             * @param responses 所有收到的成功的结果
             * @param failures 在该集群下有关该shardId 所有请求中失败的结果
             * @param fetchingRound 代表当前是第几轮发起的请求
             */
            @Override
            protected synchronized void processAsyncFetch(List<NodeGatewayStartedShards> responses, List<FailedNodeException> failures,
                                                          long fetchingRound) {
                fetchResponses.add(new Response(shardId, responses, failures));
                if (expectedOps.countDown()) {
                    finish();
                }
            }

            /**
             * 当所有分片的元数据信息都获取到后 进入第二个阶段
             */
            void finish() {
                ImmutableOpenMap.Builder<String, ImmutableOpenIntMap<java.util.List<IndicesShardStoresResponse.StoreStatus>>>
                    indicesStoreStatusesBuilder = ImmutableOpenMap.builder();

                java.util.List<IndicesShardStoresResponse.Failure> failureBuilder = new ArrayList<>();

                // 每个response 都代表一个分片在集群上所有相关节点当前状态的查询结果
                for (Response fetchResponse : fetchResponses) {
                    // 该分片在每个节点上对应的结果就是 StoreStatus
                    ImmutableOpenIntMap<java.util.List<IndicesShardStoresResponse.StoreStatus>> indexStoreStatuses =
                        indicesStoreStatusesBuilder.get(fetchResponse.shardId.getIndexName());
                    final ImmutableOpenIntMap.Builder<java.util.List<IndicesShardStoresResponse.StoreStatus>> indexShardsBuilder;
                    if (indexStoreStatuses == null) {
                        indexShardsBuilder = ImmutableOpenIntMap.builder();
                    } else {
                        indexShardsBuilder = ImmutableOpenIntMap.builder(indexStoreStatuses);
                    }
                    // 获取此时分片对应的状态信息
                    java.util.List<IndicesShardStoresResponse.StoreStatus> storeStatuses = indexShardsBuilder
                        .get(fetchResponse.shardId.id());
                    if (storeStatuses == null) {
                        storeStatuses = new ArrayList<>();
                    }
                    // 仅处理已经分配到节点上的分片信息
                    for (NodeGatewayStartedShards response : fetchResponse.responses) {
                        if (shardExistsInNode(response)) {
                            // node + shardId 可以定位到某个具体的分片  因为同一个shardId的不同子分片必须分布在不同的node上
                            // 这时可以确定分配到该node上的分片是 primary 还是replica
                            IndicesShardStoresResponse.StoreStatus.AllocationStatus allocationStatus = getAllocationStatus(
                                fetchResponse.shardId.getIndexName(), fetchResponse.shardId.id(), response.getNode());
                            storeStatuses.add(new IndicesShardStoresResponse.StoreStatus(response.getNode(), response.allocationId(),
                                allocationStatus, response.storeException()));
                        }
                    }
                    CollectionUtil.timSort(storeStatuses);
                    // 将结果存储到map中
                    indexShardsBuilder.put(fetchResponse.shardId.id(), storeStatuses);
                    indicesStoreStatusesBuilder.put(fetchResponse.shardId.getIndexName(), indexShardsBuilder.build());
                    for (FailedNodeException failure : fetchResponse.failures) {
                        failureBuilder.add(new IndicesShardStoresResponse.Failure(failure.nodeId(), fetchResponse.shardId.getIndexName(),
                            fetchResponse.shardId.id(), failure.getCause()));
                    }
                }
                listener.onResponse(new IndicesShardStoresResponse(indicesStoreStatusesBuilder.build(),
                    Collections.unmodifiableList(failureBuilder)));
            }

            /**
             * 获取某个具体分片此时的分配状态
             * @param index
             * @param shardID
             * @param node
             * @return
             */
            private IndicesShardStoresResponse.StoreStatus.AllocationStatus getAllocationStatus(String index, int shardID,
                                                                                                DiscoveryNode node) {
                for (ShardRouting shardRouting : routingNodes.node(node.getId())) {
                    ShardId shardId = shardRouting.shardId();
                    if (shardId.id() == shardID && shardId.getIndexName().equals(index)) {
                        if (shardRouting.primary()) {
                            return IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY;
                        } else if (shardRouting.assignedToNode()) {
                            return IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA;
                        } else {
                            return IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED;
                        }
                    }
                }
                return IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED;
            }

            /**
             * A shard exists/existed in a node only if shard state file exists in the node
             * 代表该分片已经分配到某个node上了
             */
            private boolean shardExistsInNode(final NodeGatewayStartedShards response) {
                return response.storeException() != null || response.allocationId() != null;
            }

            @Override
            protected void reroute(ShardId shardId, String reason) {
                // no-op
            }

            public class Response {
                private final ShardId shardId;
                private final List<NodeGatewayStartedShards> responses;
                private final List<FailedNodeException> failures;

                Response(ShardId shardId, List<NodeGatewayStartedShards> responses, List<FailedNodeException> failures) {
                    this.shardId = shardId;
                    this.responses = responses;
                    this.failures = failures;
                }
            }
        }
    }
}
