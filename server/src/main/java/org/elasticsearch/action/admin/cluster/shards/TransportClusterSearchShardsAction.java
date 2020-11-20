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

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 查询shard
 *
 * TransportMasterNodeReadAction 在req允许的情况下 可以直接通过本节点缓存的clusterState查询分片 否则必须通过leader节点查询
 */
public class TransportClusterSearchShardsAction extends
    TransportMasterNodeReadAction<ClusterSearchShardsRequest, ClusterSearchShardsResponse> {

    /**
     * 索引服务 所有相关的分片都通过该服务来访问
     */
    private final IndicesService indicesService;

    @Inject
    public TransportClusterSearchShardsAction(TransportService transportService, ClusterService clusterService,
                                              IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters,
                                              IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ClusterSearchShardsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            ClusterSearchShardsRequest::new, indexNameExpressionResolver);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        // all in memory work here...
        return ThreadPool.Names.SAME;
    }

    /**
     * 检测某些index 的元数据读取是否被阻塞   看来这个block还可以对应多种维度
     * @param request
     * @param state
     * @return
     */
    @Override
    protected ClusterBlockException checkBlock(ClusterSearchShardsRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected ClusterSearchShardsResponse read(StreamInput in) throws IOException {
        return new ClusterSearchShardsResponse(in);
    }

    /**
     * 处理主入口
     * @param task
     * @param request
     * @param state
     * @param listener
     */
    @Override
    protected void masterOperation(Task task, final ClusterSearchShardsRequest request, final ClusterState state,
                                   final ActionListener<ClusterSearchShardsResponse> listener) {
        ClusterState clusterState = clusterService.state();
        // 本次要获取哪些index 在处理前会通过checkBlock 所以这些index必然是可以正常获取信息的
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        // 获取索引的路由信息  实际上基本都是通过传入的routing来设置的
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(state, request.routing(), request.indices());
        Map<String, AliasFilter> indicesAndFilters = new HashMap<>();
        // 解析本次请求需要获取的index
        Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        for (String index : concreteIndices) {
            final AliasFilter aliasFilter = indicesService.buildAliasFilter(clusterState, index, indicesAndAliases);
            final String[] aliases = indexNameExpressionResolver.indexAliases(clusterState, index, aliasMetadata -> true, true,
                indicesAndAliases);
            indicesAndFilters.put(index, new AliasFilter(aliasFilter.getQueryBuilder(), aliases));
        }

        Set<String> nodeIds = new HashSet<>();
        GroupShardsIterator<ShardIterator> groupShardsIterator = clusterService.operationRouting()
            .searchShards(clusterState, concreteIndices, routingMap, request.preference());
        ShardRouting shard;
        ClusterSearchShardsGroup[] groupResponses = new ClusterSearchShardsGroup[groupShardsIterator.size()];
        int currentGroup = 0;
        for (ShardIterator shardIt : groupShardsIterator) {
            ShardId shardId = shardIt.shardId();
            ShardRouting[] shardRoutings = new ShardRouting[shardIt.size()];
            int currentShard = 0;
            shardIt.reset();
            while ((shard = shardIt.nextOrNull()) != null) {
                shardRoutings[currentShard++] = shard;
                nodeIds.add(shard.currentNodeId());
            }
            groupResponses[currentGroup++] = new ClusterSearchShardsGroup(shardId, shardRoutings);
        }
        DiscoveryNode[] nodes = new DiscoveryNode[nodeIds.size()];
        int currentNode = 0;
        for (String nodeId : nodeIds) {
            nodes[currentNode++] = clusterState.getNodes().get(nodeId);
        }
        listener.onResponse(new ClusterSearchShardsResponse(groupResponses, nodes, indicesAndFilters));
    }
}
