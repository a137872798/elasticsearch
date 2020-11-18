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

package org.elasticsearch.action.admin.cluster.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation.DebugMode;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * The {@code TransportClusterAllocationExplainAction} is responsible for actually executing the explanation of a shard's allocation on the
 * master node in the cluster.
 * 获取位置信息
 */
public class TransportClusterAllocationExplainAction
        extends TransportMasterNodeAction<ClusterAllocationExplainRequest, ClusterAllocationExplainResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterAllocationExplainAction.class);

    private final ClusterInfoService clusterInfoService;
    private final AllocationDeciders allocationDeciders;
    private final ShardsAllocator shardAllocator;
    private final AllocationService allocationService;

    @Inject
    public TransportClusterAllocationExplainAction(TransportService transportService, ClusterService clusterService,
                                                   ThreadPool threadPool, ActionFilters actionFilters,
                                                   IndexNameExpressionResolver indexNameExpressionResolver,
                                                   ClusterInfoService clusterInfoService, AllocationDeciders allocationDeciders,
                                                   ShardsAllocator shardAllocator, AllocationService allocationService) {
        super(ClusterAllocationExplainAction.NAME, transportService, clusterService, threadPool, actionFilters,
            ClusterAllocationExplainRequest::new, indexNameExpressionResolver);
        this.clusterInfoService = clusterInfoService;
        this.allocationDeciders = allocationDeciders;
        this.shardAllocator = shardAllocator;
        this.allocationService = allocationService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected ClusterAllocationExplainResponse read(StreamInput in) throws IOException {
        return new ClusterAllocationExplainResponse(in);
    }

    /**
     * 获取分配信息详情 对应的是读取元数据的操作
     * @param request
     * @param state
     * @return
     */
    @Override
    protected ClusterBlockException checkBlock(ClusterAllocationExplainRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    /**
     * 当前节点是leader节点才会触发该方法   TODO 分配信息为什么必须在leader节点才可以获取
     * @param task
     * @param request
     * @param state
     * @param listener
     */
    @Override
    protected void masterOperation(Task task, final ClusterAllocationExplainRequest request, final ClusterState state,
                                   final ActionListener<ClusterAllocationExplainResponse> listener) {
        final RoutingNodes routingNodes = state.getRoutingNodes();
        // 这里记录了整个集群使用的内存量 分片数量等等信息
        final ClusterInfo clusterInfo = clusterInfoService.getClusterInfo();
        final RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, state,
                clusterInfo, System.nanoTime());

        // 找到req定位的范围对应的路由信息 如果没有明确指定某个分片 优先返回未分配的 其次是 started阶段的
        ShardRouting shardRouting = findShardToExplain(request, allocation);
        logger.debug("explaining the allocation for [{}], found shard [{}]", request, shardRouting);

        // 生成某个分片的描述信息
        ClusterAllocationExplanation cae = explainShard(shardRouting, allocation,
            request.includeDiskInfo() ? clusterInfo : null, request.includeYesDecisions(), allocationService);
        listener.onResponse(new ClusterAllocationExplainResponse(cae));
    }

    /**
     *
     * @param shardRouting  本次要展示的路由信息
     * @param allocation   路由分配器
     * @param clusterInfo   描述此时集群内存使用量 分片数量 等等
     * @param includeYesDecisions   是否要包含yes决策信息
     * @param allocationService
     * @return
     */
    public static ClusterAllocationExplanation explainShard(ShardRouting shardRouting, RoutingAllocation allocation,
                                                            ClusterInfo clusterInfo, boolean includeYesDecisions,
                                                            AllocationService allocationService) {
        allocation.setDebugMode(includeYesDecisions ? DebugMode.ON : DebugMode.EXCLUDE_YES_DECISIONS);

        ShardAllocationDecision shardDecision;
        // 以下2种状态是没有决策信息的  只有未分配 或者启动状态的分片才有决策信息
        if (shardRouting.initializing() || shardRouting.relocating()) {
            shardDecision = ShardAllocationDecision.NOT_TAKEN;
        } else {
            // TODO 有关分配的这块暂时还看不懂
            shardDecision = allocationService.explainShardAllocation(shardRouting, allocation);
        }

        return new ClusterAllocationExplanation(shardRouting,
            shardRouting.currentNodeId() != null ? allocation.nodes().get(shardRouting.currentNodeId()) : null,
            shardRouting.relocatingNodeId() != null ? allocation.nodes().get(shardRouting.relocatingNodeId()) : null,
            clusterInfo, shardDecision);
    }

    /**
     *
     * @param request  用于限定查询范围
     * @param allocation  所有路由信息都存储在该对象中
     * @return
     */
    public static ShardRouting findShardToExplain(ClusterAllocationExplainRequest request, RoutingAllocation allocation) {
        ShardRouting foundShard = null;
        // 当没有设定查询范围时  就认为查找所有 unassigned分片
        if (request.useAnyUnassignedShard()) {
            // If we can use any shard, just pick the first unassigned one (if there are any)
            RoutingNodes.UnassignedShards.UnassignedIterator ui = allocation.routingNodes().unassigned().iterator();
            if (ui.hasNext()) {
                foundShard = ui.next();
            }
            if (foundShard == null) {
                throw new IllegalArgumentException("unable to find any unassigned shards to explain [" + request + "]");
            }
        } else {
            // 另一种情况的话 index shard primary 都是必填的
            String index = request.getIndex();
            int shard = request.getShard();
            if (request.isPrimary()) {
                // If we're looking for the primary shard, there's only one copy, so pick it directly
                // 定位到相关分片
                foundShard = allocation.routingTable().shardRoutingTable(index, shard).primaryShard();
                // 检测节点id是否一致
                if (request.getCurrentNode() != null) {
                    DiscoveryNode primaryNode = allocation.nodes().resolveNode(request.getCurrentNode());
                    // the primary is assigned to a node other than the node specified in the request
                    if (primaryNode.getId().equals(foundShard.currentNodeId()) == false) {
                        throw new IllegalArgumentException(
                                "unable to find primary shard assigned to node [" + request.getCurrentNode() + "]");
                    }
                }
            } else {
                // If looking for a replica, go through all the replica shards
                // 先找到所有副本分片路由信息
                List<ShardRouting> replicaShardRoutings = allocation.routingTable().shardRoutingTable(index, shard).replicaShards();
                // 如果有指定某个node  检测是否有分片在该node上
                if (request.getCurrentNode() != null) {
                    // the request is to explain a replica shard already assigned on a particular node,
                    // so find that shard copy
                    DiscoveryNode replicaNode = allocation.nodes().resolveNode(request.getCurrentNode());
                    for (ShardRouting replica : replicaShardRoutings) {
                        if (replicaNode.getId().equals(replica.currentNodeId())) {
                            foundShard = replica;
                            break;
                        }
                    }
                    if (foundShard == null) {
                        throw new IllegalArgumentException("unable to find a replica shard assigned to node [" +
                                                            request.getCurrentNode() + "]");
                    }
                } else {
                    if (replicaShardRoutings.size() > 0) {
                        // Pick the first replica at the very least
                        foundShard = replicaShardRoutings.get(0);
                        for (ShardRouting replica : replicaShardRoutings) {
                            // In case there are multiple replicas where some are assigned and some aren't,
                            // try to find one that is unassigned at least
                            // 优先返回未分配的分片
                            if (replica.unassigned()) {
                                foundShard = replica;
                                break;
                            // 其次展示 started节点的分片
                            } else if (replica.started() && (foundShard.initializing() || foundShard.relocating())) {
                                // prefer started shards to initializing or relocating shards because started shards
                                // can be explained
                                foundShard = replica;
                            }
                        }
                    }
                }
            }
        }

        if (foundShard == null) {
            throw new IllegalArgumentException("unable to find any shards to explain [" + request + "] in the routing table");
        }
        return foundShard;
    }
}
