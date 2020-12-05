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

package org.elasticsearch.action.admin.indices.segments;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * 获取索引segment信息
 * 该请求会按照shard维度进行拆解
 */
public class TransportIndicesSegmentsAction
        extends TransportBroadcastByNodeAction<IndicesSegmentsRequest, IndicesSegmentResponse, ShardSegments> {

    private final IndicesService indicesService;

    @Inject
    public TransportIndicesSegmentsAction(ClusterService clusterService, TransportService transportService,
                                          IndicesService indicesService, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver) {
        super(IndicesSegmentsAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                IndicesSegmentsRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    /**
     * Segments goes across *all* active shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, IndicesSegmentsRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndicesSegmentsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndicesSegmentsRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardSegments readShardResult(StreamInput in) throws IOException {
        return new ShardSegments(in);
    }

    @Override
    protected IndicesSegmentResponse newResponse(IndicesSegmentsRequest request, int totalShards, int successfulShards, int failedShards,
                                                 List<ShardSegments> results, List<DefaultShardOperationFailedException> shardFailures,
                                                 ClusterState clusterState) {
        return new IndicesSegmentResponse(results.toArray(new ShardSegments[results.size()]), totalShards, successfulShards, failedShards,
            shardFailures);
    }

    @Override
    protected IndicesSegmentsRequest readRequestFrom(StreamInput in) throws IOException {
        return new IndicesSegmentsRequest(in);
    }

    /**
     * 如何处理每个分片
     * @param request      the node-level request
     * @param shardRouting the shard on which to execute the operation
     * @return
     */
    @Override
    protected ShardSegments shardOperation(IndicesSegmentsRequest request, ShardRouting shardRouting) {
        // 一个index下对应多个分片 每个分片的segment信息总和 才是一个index的segment信息吧
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.index());
        IndexShard indexShard = indexService.getShard(shardRouting.id());
        // 主要就是通过engine实现该功能 并将相关数据包装一下作为结果
        return new ShardSegments(indexShard.routingEntry(), indexShard.segments(request.verbose()));
    }
}
