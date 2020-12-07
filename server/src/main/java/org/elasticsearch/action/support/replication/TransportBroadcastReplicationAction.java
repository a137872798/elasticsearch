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

package org.elasticsearch.action.support.replication;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Base class for requests that should be executed on all shards of an index or several indices.
 * This action sends shard requests to all primary shards of the indices and they are then replicated like write requests
 * 会将请求以shard为单位进行拆分
 */
public abstract class TransportBroadcastReplicationAction<Request extends BroadcastRequest<Request>, Response extends BroadcastResponse,
        ShardRequest extends ReplicationRequest<ShardRequest>, ShardResponse extends ReplicationResponse>
        extends HandledTransportAction<Request, Response> {

    private final ActionType<ShardResponse> replicatedBroadcastShardAction;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final NodeClient client;


    /**
     *
     * @param name 代表本次广播的action类型是什么
     * @param requestReader
     * @param clusterService
     * @param transportService
     * @param client
     * @param actionFilters
     * @param indexNameExpressionResolver
     * @param replicatedBroadcastShardAction
     */
    public TransportBroadcastReplicationAction(String name, Writeable.Reader<Request> requestReader, ClusterService clusterService,
                                               TransportService transportService, NodeClient client,
                                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                               ActionType<ShardResponse> replicatedBroadcastShardAction) {
        super(name, transportService, actionFilters, requestReader);
        this.client = client;
        this.replicatedBroadcastShardAction = replicatedBroadcastShardAction;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }


    /**
     * 当上层发起命令时 交由该方法处理
     * @param task
     * @param request
     * @param listener
     */
    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        // 获取当前的集群状态
        final ClusterState clusterState = clusterService.state();
        // 刷盘是以分片为单位的  这里先获取总计有多少shardId
        List<ShardId> shards = shards(request, clusterState);
        final CopyOnWriteArrayList<ShardResponse> shardsResponses = new CopyOnWriteArrayList<>();
        // 如果没有可用的分片 就不需要处理了 直接触发监听器
        if (shards.size() == 0) {
            finishAndNotifyListener(listener, shardsResponses);
        }
        final CountDown responsesCountDown = new CountDown(shards.size());

        // 开始以shard为单位 发送请求
        for (final ShardId shardId : shards) {
            ActionListener<ShardResponse> shardActionListener = new ActionListener<ShardResponse>() {
                @Override
                public void onResponse(ShardResponse shardResponse) {
                    shardsResponses.add(shardResponse);
                    logger.trace("{}: got response from {}", actionName, shardId);
                    if (responsesCountDown.countDown()) {
                        finishAndNotifyListener(listener, shardsResponses);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.trace("{}: got failure from {}", actionName, shardId);
                    int totalNumCopies = clusterState.getMetadata().getIndexSafe(shardId.getIndex()).getNumberOfReplicas() + 1;
                    ShardResponse shardResponse = newShardResponse();
                    ReplicationResponse.ShardInfo.Failure[] failures;
                    if (TransportActions.isShardNotAvailableException(e)) {
                        failures = new ReplicationResponse.ShardInfo.Failure[0];
                    } else {
                        ReplicationResponse.ShardInfo.Failure failure = new ReplicationResponse.ShardInfo.Failure(shardId, null, e,
                            ExceptionsHelper.status(e), true);
                        failures = new ReplicationResponse.ShardInfo.Failure[totalNumCopies];
                        Arrays.fill(failures, failure);
                    }
                    shardResponse.setShardInfo(new ReplicationResponse.ShardInfo(totalNumCopies, 0, failures));
                    shardsResponses.add(shardResponse);
                    if (responsesCountDown.countDown()) {
                        finishAndNotifyListener(listener, shardsResponses);
                    }
                }
            };
            shardExecute(task, request, shardId, shardActionListener);
        }
    }

    /**
     * 以 shard为单位 发送请求
     * @param task
     * @param request
     * @param shardId
     * @param shardActionListener
     */
    protected void shardExecute(Task task, Request request, ShardId shardId, ActionListener<ShardResponse> shardActionListener) {
        ShardRequest shardRequest = newShardRequest(request, shardId);
        shardRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        // 在本节点使用replicatedBroadcastShardAction 对应的action进行处理
        client.executeLocally(replicatedBroadcastShardAction, shardRequest, shardActionListener);
    }

    /**
     * @return all shard ids the request should run on
     * 将路由表中的 IndexShardRoutingTable信息的 shardId抽取出来 并返回
     */
    protected List<ShardId> shards(Request request, ClusterState clusterState) {
        List<ShardId> shardIds = new ArrayList<>();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        for (String index : concreteIndices) {
            IndexMetadata indexMetadata = clusterState.metadata().getIndices().get(index);
            if (indexMetadata != null) {
                for (IntObjectCursor<IndexShardRoutingTable> shardRouting
                        : clusterState.getRoutingTable().indicesRouting().get(index).getShards()) {
                    shardIds.add(shardRouting.value.shardId());
                }
            }
        }
        return shardIds;
    }

    protected abstract ShardResponse newShardResponse();

    protected abstract ShardRequest newShardRequest(Request request, ShardId shardId);

    /**
     * 触发监听器
     * @param listener
     * @param shardsResponses
     */
    private void finishAndNotifyListener(ActionListener listener, CopyOnWriteArrayList<ShardResponse> shardsResponses) {
        logger.trace("{}: got all shard responses", actionName);
        int successfulShards = 0;
        int failedShards = 0;
        int totalNumCopies = 0;
        List<DefaultShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.size(); i++) {
            ReplicationResponse shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // non active shard, ignore
            } else {
                failedShards += shardResponse.getShardInfo().getFailed();
                successfulShards += shardResponse.getShardInfo().getSuccessful();
                totalNumCopies += shardResponse.getShardInfo().getTotal();
                if (shardFailures == null) {
                    shardFailures = new ArrayList<>();
                }
                for (ReplicationResponse.ShardInfo.Failure failure : shardResponse.getShardInfo().getFailures()) {
                    shardFailures.add(new DefaultShardOperationFailedException(
                        new BroadcastShardOperationFailedException(failure.fullShardId(), failure.getCause())));
                }
            }
        }
        listener.onResponse(newResponse(successfulShards, failedShards, totalNumCopies, shardFailures));
    }

    protected abstract BroadcastResponse newResponse(int successfulShards, int failedShards, int totalNumCopies,
                                                     List<DefaultShardOperationFailedException> shardFailures);
}
