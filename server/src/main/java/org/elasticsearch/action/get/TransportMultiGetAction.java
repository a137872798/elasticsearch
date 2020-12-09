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
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 批量获取请求
 */
public class TransportMultiGetAction extends HandledTransportAction<MultiGetRequest, MultiGetResponse> {

    private final ClusterService clusterService;
    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportMultiGetAction(TransportService transportService, ClusterService clusterService,
                                   NodeClient client, ActionFilters actionFilters,
                                   IndexNameExpressionResolver resolver) {
        super(MultiGetAction.NAME, transportService, actionFilters, MultiGetRequest::new);
        this.clusterService = clusterService;
        this.client = client;
        this.indexNameExpressionResolver = resolver;
    }

    /**
     * 处理请求
     * @param task
     * @param request
     * @param listener
     */
    @Override
    protected void doExecute(Task task, final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        ClusterState clusterState = clusterService.state();
        // 检测read操作是否此时处于被阻塞的状态
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        // 单个请求被叫做 ItemRequest
        final AtomicArray<MultiGetItemResponse> responses = new AtomicArray<>(request.items.size());
        final Map<ShardId, MultiGetShardRequest> shardRequests = new HashMap<>();

        for (int i = 0; i < request.items.size(); i++) {
            MultiGetRequest.Item item = request.items.get(i);

            String concreteSingleIndex;
            try {
                concreteSingleIndex = indexNameExpressionResolver.concreteSingleIndex(clusterState, item).getName();

                // 解析路由信息后回填到 routing() 字段
                item.routing(clusterState.metadata().resolveIndexRouting(item.routing(), item.index()));
                // 如果该index需要路由信息 而本次请求中没有携带  不过routing 好像只有在分区的场景下才会使用 所以可以先忽略
                if ((item.routing() == null) && (clusterState.getMetadata().routingRequired(concreteSingleIndex))) {
                    responses.set(i, newItemFailure(concreteSingleIndex, item.id(),
                        new RoutingMissingException(concreteSingleIndex, item.id())));
                    continue;
                }
            } catch (Exception e) {
                responses.set(i, newItemFailure(item.index(), item.id(), e));
                continue;
            }

            // 找到本次请求应该发往哪个分片
            ShardId shardId = clusterService.operationRouting()
                    .getShards(clusterState, concreteSingleIndex, item.id(), item.routing(), null)
                    .shardId();

            // 将发往同一 shardId的请求合并
            MultiGetShardRequest shardRequest = shardRequests.get(shardId);
            if (shardRequest == null) {
                shardRequest = new MultiGetShardRequest(request, shardId.getIndexName(), shardId.getId());
                shardRequests.put(shardId, shardRequest);
            }
            shardRequest.add(i, item);
        }

        // 有效信息不足 无法发起multiGet请求
        if (shardRequests.isEmpty()) {
            // only failures..
            listener.onResponse(new MultiGetResponse(responses.toArray(new MultiGetItemResponse[responses.length()])));
        }

        executeShardAction(listener, responses, shardRequests);
    }

    /**
     * 原始请求按照shardId分组后 再处理
     * @param listener
     * @param responses
     * @param shardRequests
     */
    protected void executeShardAction(ActionListener<MultiGetResponse> listener,
                                      AtomicArray<MultiGetItemResponse> responses,
                                      Map<ShardId, MultiGetShardRequest> shardRequests) {
        final AtomicInteger counter = new AtomicInteger(shardRequests.size());

        for (final MultiGetShardRequest shardRequest : shardRequests.values()) {
            client.executeLocally(TransportShardMultiGetAction.TYPE, shardRequest, new ActionListener<>() {
                @Override
                public void onResponse(MultiGetShardResponse response) {
                    for (int i = 0; i < response.locations.size(); i++) {
                        MultiGetItemResponse itemResponse = new MultiGetItemResponse(response.responses.get(i), response.failures.get(i));
                        responses.set(response.locations.get(i), itemResponse);
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // create failures for all relevant requests
                    for (int i = 0; i < shardRequest.locations.size(); i++) {
                        MultiGetRequest.Item item = shardRequest.items.get(i);
                        responses.set(shardRequest.locations.get(i), newItemFailure(shardRequest.index(), item.id(), e));
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    listener.onResponse(new MultiGetResponse(responses.toArray(new MultiGetItemResponse[responses.length()])));
                }
            });
        }
    }

    private static MultiGetItemResponse newItemFailure(String index, String id, Exception exception) {
        return new MultiGetItemResponse(null, new MultiGetResponse.Failure(index, id, exception));
    }
}
