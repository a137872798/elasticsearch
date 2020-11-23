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
        // 找到此时存在的所有与查询条件匹配的indexName
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        // 将每个索引对应的查找范围包装成一个map
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(state, request.routing(), request.indices());
        Map<String, AliasFilter> indicesAndFilters = new HashMap<>();
        // 将所以表达式解析后的结果  还没有去匹配相关的索引  比如某个索引键可能被替换成了别名 而别名又对应了一组索引 那么当前结果就与 concreteIndexNames 不相同了
        Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());

        // 这里是精确匹配的索引名
        for (String index : concreteIndices) {
            // 将索引对应的所有 AliasMetadata内的数据流转换成 QueryBuilder对象 并包装成 Filter对象
            final AliasFilter aliasFilter = indicesService.buildAliasFilter(clusterState, index, indicesAndAliases);
            // 该索引相关的所有别名  这里传入的函数为  aliasMetadata -> true  与默认的函数不一样 所以获取的别名列表也会不一样
            final String[] aliases = indexNameExpressionResolver.indexAliases(clusterState, index, aliasMetadata -> true, true,
                indicesAndAliases);
            indicesAndFilters.put(index, new AliasFilter(aliasFilter.getQueryBuilder(), aliases));
        }

        // 相关分片出现在哪些node下
        Set<String> nodeIds = new HashSet<>();

        // 如果有偏向信息也会传入  这里返回满足路由信息以及偏好信息的分片
        // 路由信息可以通过一定公式换算成 shardId
        GroupShardsIterator<ShardIterator> groupShardsIterator = clusterService.operationRouting()
            .searchShards(clusterState, concreteIndices, routingMap, request.preference());

        // 这个是最小维度的分片信息
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
