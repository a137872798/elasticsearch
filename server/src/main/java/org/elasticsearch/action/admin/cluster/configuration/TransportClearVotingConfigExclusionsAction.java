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
package org.elasticsearch.action.admin.cluster.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateObserver.Listener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * 清理exclusion配置信息  必须在leader节点上执行
 */
public class TransportClearVotingConfigExclusionsAction
    extends TransportMasterNodeAction<ClearVotingConfigExclusionsRequest, ClearVotingConfigExclusionsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClearVotingConfigExclusionsAction.class);

    @Inject
    public TransportClearVotingConfigExclusionsAction(TransportService transportService, ClusterService clusterService,
                                                      ThreadPool threadPool, ActionFilters actionFilters,
                                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ClearVotingConfigExclusionsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            ClearVotingConfigExclusionsRequest::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return Names.SAME;
    }

    @Override
    protected ClearVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
        return new ClearVotingConfigExclusionsResponse(in);
    }

    /**
     * 当本节点是leader节点时 进行处理
     * @param task
     * @param request
     * @param initialState
     * @param listener
     * @throws Exception
     */
    @Override
    protected void masterOperation(Task task, ClearVotingConfigExclusionsRequest request, ClusterState initialState,
                                   ActionListener<ClearVotingConfigExclusionsResponse> listener) throws Exception {

        final long startTimeMillis = threadPool.relativeTimeInMillis();

        // 检测是否参选的节点已经不存在于集群了
        final Predicate<ClusterState> allExclusionsRemoved = newState -> {
            for (VotingConfigExclusion tombstone : initialState.getVotingConfigExclusions()) {
                // NB checking for the existence of any node with this persistent ID, because persistent IDs are how votes are counted.
                if (newState.nodes().nodeExists(tombstone.getNodeId())) {
                    return false;
                }
            }
            return true;
        };

        // 如果要等待节点从集群中移除  推测是为了降低选举的复杂度
        if (request.getWaitForRemoval() && allExclusionsRemoved.test(initialState) == false) {
            final ClusterStateObserver clusterStateObserver = new ClusterStateObserver(initialState, clusterService, request.getTimeout(),
                logger, threadPool.getThreadContext());

            clusterStateObserver.waitForNextChange(new Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    submitClearVotingConfigExclusionsTask(request, startTimeMillis, listener);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new ElasticsearchException("cluster service closed while waiting for removal of nodes "
                        + initialState.getVotingConfigExclusions()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new ElasticsearchTimeoutException(
                        "timed out waiting for removal of nodes; if nodes should not be removed, set waitForRemoval to false. "
                        + initialState.getVotingConfigExclusions()));
                }
            }, allExclusionsRemoved);
        } else {
            // 如果请求没有要求这些node 从集群中被移除 那么直接清除元数据就可以
            submitClearVotingConfigExclusionsTask(request, startTimeMillis, listener);
        }
    }

    /**
     * 此时原本被排除的节点都已经不在集群中了
     * @param request
     * @param startTimeMillis
     * @param listener
     */
    private void submitClearVotingConfigExclusionsTask(ClearVotingConfigExclusionsRequest request, long startTimeMillis,
                                                       ActionListener<ClearVotingConfigExclusionsResponse> listener) {
        clusterService.submitStateUpdateTask("clear-voting-config-exclusions", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                // 就是清空 voteConfigExclusion配置后 发布到集群中
                final CoordinationMetadata newCoordinationMetadata =
                        CoordinationMetadata.builder(currentState.coordinationMetadata()).clearVotingConfigExclusions().build();
                final Metadata newMetadata = Metadata.builder(currentState.metadata()).
                        coordinationMetadata(newCoordinationMetadata).build();
                return ClusterState.builder(currentState).metadata(newMetadata).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public TimeValue timeout() {
                return TimeValue.timeValueMillis(request.getTimeout().millis() + startTimeMillis - threadPool.relativeTimeInMillis());
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new ClearVotingConfigExclusionsResponse());
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(ClearVotingConfigExclusionsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
