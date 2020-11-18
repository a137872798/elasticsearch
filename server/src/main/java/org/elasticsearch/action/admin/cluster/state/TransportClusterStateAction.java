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

package org.elasticsearch.action.admin.cluster.state;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.Custom;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * 有关集群状态的处理器
 */
public class TransportClusterStateAction extends TransportMasterNodeReadAction<ClusterStateRequest, ClusterStateResponse> {

    private final Logger logger = LogManager.getLogger(getClass());

    @Inject
    public TransportClusterStateAction(TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ClusterStateAction.NAME, false, transportService, clusterService, threadPool, actionFilters,
              ClusterStateRequest::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        // very lightweight operation in memory, no need to fork to a thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterStateResponse read(StreamInput in) throws IOException {
        return new ClusterStateResponse(in);
    }

    /**
     * 获取集群状态的请求不会被阻塞
     * @param request
     * @param state
     * @return
     */
    @Override
    protected ClusterBlockException checkBlock(ClusterStateRequest request, ClusterState state) {
        // cluster state calls are done also on a fully blocked cluster to figure out what is going
        // on in the cluster. For example, which nodes have joined yet the recovery has not yet kicked
        // in, we need to make sure we allow those calls
        // return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
        return null;
    }

    /**
     * 入口
     * @param task
     * @param request
     * @param state
     * @param listener
     * @throws IOException
     */
    @Override
    protected void masterOperation(Task task, final ClusterStateRequest request, final ClusterState state,
                                   final ActionListener<ClusterStateResponse> listener) throws IOException {

        // 请求中可以携带一个元数据版本信息  未设置时该谓语总是返回true
        // 设置了则代表  要求此时集群状态的元数据版本>= 要求的版本
        final Predicate<ClusterState> acceptableClusterStatePredicate
            = request.waitForMetadataVersion() == null ? clusterState -> true
            : clusterState -> clusterState.metadata().version() >= request.waitForMetadataVersion();

        final Predicate<ClusterState> acceptableClusterStateOrNotMasterPredicate = request.local()
            ? acceptableClusterStatePredicate
            // 当local ==false 就代表此时在leader节点触发的该方法   那么当本节点不再是leader节点时 也无法处理请求了 就可以从waitForNextChange状态解除
            : acceptableClusterStatePredicate.or(clusterState -> clusterState.nodes().isLocalNodeElectedMaster() == false);

        // 首先检测此时集群状态的元数据信息版本是否超过要求值
        if (acceptableClusterStatePredicate.test(state)) {
            // 此时符合要求 通过buildResponse生成结果 并触发监听器
            ActionListener.completeWith(listener, () -> buildResponse(request, state));
        } else {
            assert acceptableClusterStateOrNotMasterPredicate.test(state) == false;
            // 需要等待clusterState的变化
            new ClusterStateObserver(state, clusterService, request.waitForTimeout(), logger, threadPool.getThreadContext())
                .waitForNextChange(new ClusterStateObserver.Listener() {

                @Override
                public void onNewClusterState(ClusterState newState) {
                    if (acceptableClusterStatePredicate.test(newState)) {
                        ActionListener.completeWith(listener, () -> buildResponse(request, newState));
                    } else {
                        listener.onFailure(new NotMasterException(
                            "master stepped down waiting for metadata version " + request.waitForMetadataVersion()));
                    }
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    try {
                        listener.onResponse(new ClusterStateResponse(state.getClusterName(), null, true));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }
            }, acceptableClusterStateOrNotMasterPredicate);
        }
    }

    /**
     * 生成集群状态res对象
     * @param request
     * @param currentState
     * @return
     */
    private ClusterStateResponse buildResponse(final ClusterStateRequest request,
                                               final ClusterState currentState) {
        logger.trace("Serving cluster state request using version {}", currentState.version());
        ClusterState.Builder builder = ClusterState.builder(currentState.getClusterName());
        builder.version(currentState.version());
        builder.stateUUID(currentState.stateUUID());

        if (request.nodes()) {
            builder.nodes(currentState.nodes());
        }
        if (request.routingTable()) {
            if (request.indices().length > 0) {
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
                // 根据请求体中携带的indices 转换成相关的索引名
                String[] indices = indexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    if (currentState.routingTable().getIndicesRouting().containsKey(filteredIndex)) {
                        routingTableBuilder.add(currentState.routingTable().getIndicesRouting().get(filteredIndex));
                    }
                }
                builder.routingTable(routingTableBuilder.build());
            } else {
                builder.routingTable(currentState.routingTable());
            }
        }
        if (request.blocks()) {
            builder.blocks(currentState.blocks());
        }

        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.clusterUUID(currentState.metadata().clusterUUID());
        mdBuilder.coordinationMetadata(currentState.coordinationMetadata());

        // 如果需要携带元数据信息
        if (request.metadata()) {
            if (request.indices().length > 0) {
                mdBuilder.version(currentState.metadata().version());
                String[] indices = indexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    IndexMetadata indexMetadata = currentState.metadata().index(filteredIndex);
                    if (indexMetadata != null) {
                        mdBuilder.put(indexMetadata, false);
                    }
                }
            } else {
                mdBuilder = Metadata.builder(currentState.metadata());
            }

            // filter out metadata that shouldn't be returned by the API
            for (ObjectObjectCursor<String, Custom> custom : currentState.metadata().customs()) {
                if (custom.value.context().contains(Metadata.XContentContext.API) == false) {
                    mdBuilder.removeCustom(custom.key);
                }
            }
        }
        builder.metadata(mdBuilder);

        if (request.customs()) {
            for (ObjectObjectCursor<String, ClusterState.Custom> custom : currentState.customs()) {
                if (custom.value.isPrivate() == false) {
                    builder.putCustom(custom.key, custom.value);
                }
            }
        }

        return new ClusterStateResponse(currentState.getClusterName(), builder.build(), false);
    }

}
