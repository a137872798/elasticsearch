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

package org.elasticsearch.action.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Performs the get operation.
 * 执行一个get请求  该请求只会在某个分片上执行
 */
public class TransportGetAction extends TransportSingleShardAction<GetRequest, GetResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportGetAction(ClusterService clusterService, TransportService transportService,
                              IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters,
                              IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                GetRequest::new, ThreadPool.Names.GET);
        this.indicesService = indicesService;
    }

    @Override
    protected boolean resolveIndex(GetRequest request) {
        return true;
    }

    /**
     * 通过请求中携带的信息 定位到符合条件的分片上
     * @param state
     * @param request
     * @return
     */
    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return clusterService.operationRouting()
                .getShards(clusterService.state(), request.concreteIndex(), request.request().id(), request.request().routing(),
                    request.request().preference());
    }

    /**
     * 在生成AsyncSingleAction时   会对请求体做处理
     * @param state
     * @param request
     */
    @Override
    protected void resolveRequest(ClusterState state, InternalRequest request) {
        // update the routing (request#index here is possibly an alias)
        request.request().routing(state.metadata().resolveIndexRouting(request.request().routing(), request.request().index()));
        // Fail fast on the node that received the request.
        if (request.request().routing() == null && state.getMetadata().routingRequired(request.concreteIndex())) {
            throw new RoutingMissingException(request.concreteIndex(), request.request().id());
        }
    }

    /**
     * 这里覆盖了父类的方法  增加了一些处理
     * 此时已经是在目标分片所在的节点上处理请求了
     * @param request
     * @param shardId
     * @param listener
     * @throws IOException
     */
    @Override
    protected void asyncShardOperation(GetRequest request, ShardId shardId, ActionListener<GetResponse> listener) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        // 如果本次请求是一次实时请求  那么不做其他处理
        if (request.realtime()) { // we are not tied to a refresh cycle here anyway
            super.asyncShardOperation(request, shardId, listener);
        } else {
            // 如果不是实时请求 并且当前分片处于init状态 那么就等待状态变更成active状态  (将自身注册到 refreshListeners中 )
            indexShard.awaitShardSearchActive(b -> {
                try {
                    super.asyncShardOperation(request, shardId, listener);
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            });
        }
    }

    /**
     * 这里是处理 get请求
     * @param request
     * @param shardId
     * @return
     */
    @Override
    protected GetResponse shardOperation(GetRequest request, ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());

        // 非实时就代表允许延后触发  那么就好的时机就是下一次刷新后
        if (request.refresh() && !request.realtime()) {
            indexShard.refresh("refresh_flag_get");
        }

        GetResult result = indexShard.getService().get(request.id(), request.storedFields(),
                request.realtime(), request.version(), request.versionType(), request.fetchSourceContext());
        return new GetResponse(result);
    }

    @Override
    protected Writeable.Reader<GetResponse> getResponseReader() {
        return GetResponse::new;
    }

    @Override
    protected String getExecutor(GetRequest request, ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getIndexSettings().isSearchThrottled() ? ThreadPool.Names.SEARCH_THROTTLED : super.getExecutor(request,
            shardId);
    }
}
