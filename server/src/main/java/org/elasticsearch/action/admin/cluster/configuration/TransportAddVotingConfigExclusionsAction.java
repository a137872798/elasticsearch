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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 匹配 AddVotingConfigExclusionsAction
 * 在选举阶段 会将某些节点排除 避免他们成为leader节点
 */
public class TransportAddVotingConfigExclusionsAction extends TransportMasterNodeAction<AddVotingConfigExclusionsRequest,
    AddVotingConfigExclusionsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportAddVotingConfigExclusionsAction.class);

    public static final Setting<Integer> MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING
        = Setting.intSetting("cluster.max_voting_config_exclusions", 10, 1, Property.Dynamic, Property.NodeScope);

    private volatile int maxVotingConfigExclusions;

    @Inject
    public TransportAddVotingConfigExclusionsAction(Settings settings, ClusterSettings clusterSettings, TransportService transportService,
                                                    ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                                    IndexNameExpressionResolver indexNameExpressionResolver) {
        super(AddVotingConfigExclusionsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            AddVotingConfigExclusionsRequest::new, indexNameExpressionResolver);

        maxVotingConfigExclusions = MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.get(settings);
        // 有关最大数量的 exclusion是一个动态配置
        clusterSettings.addSettingsUpdateConsumer(MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING, this::setMaxVotingConfigExclusions);
    }

    private void setMaxVotingConfigExclusions(int maxVotingConfigExclusions) {
        this.maxVotingConfigExclusions = maxVotingConfigExclusions;
    }

    /**
     * 指定 masterOperation 在当前线程执行
     * @return
     */
    @Override
    protected String executor() {
        return Names.SAME;
    }

    @Override
    protected AddVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
        return new AddVotingConfigExclusionsResponse(in);
    }

    /**
     * 当本节点是集群中的leader节点时触发       修改集群元数据的操作也只能在leader节点上执行
     * 在传输层 接收到请求后 会立刻将请求对应的handler对象在线程池中执行  所以这时线程已经切换了
     * @param task
     * @param request
     * @param state
     * @param listener
     * @throws Exception
     */
    @Override
    protected void masterOperation(Task task, AddVotingConfigExclusionsRequest request, ClusterState state,
                                   ActionListener<AddVotingConfigExclusionsResponse> listener) throws Exception {

        // 检验确保排除数没有达到上限
        resolveVotingConfigExclusionsAndCheckMaximum(request, state, maxVotingConfigExclusions);
        // throws IAE if no nodes matched or maximum exceeded

        // 更新集群状态  当前节点必然是leader节点 那么任务就会直接在本节点触发
        clusterService.submitStateUpdateTask("add-voting-config-exclusions", new ClusterStateUpdateTask(Priority.URGENT) {

            private Set<VotingConfigExclusion> resolvedExclusions;

            /**
             * 这里定义了更新状态的逻辑    之后新的集群状态会通过 协调对象通知到集群中所有的节点
             * @param currentState
             * @return
             */
            @Override
            public ClusterState execute(ClusterState currentState) {
                assert resolvedExclusions == null : resolvedExclusions;
                // 重新检测一下 本次增加的配置
                final int finalMaxVotingConfigExclusions = TransportAddVotingConfigExclusionsAction.this.maxVotingConfigExclusions;
                resolvedExclusions = resolveVotingConfigExclusionsAndCheckMaximum(request, currentState, finalMaxVotingConfigExclusions);

                // 返回新的集群元数据
                final CoordinationMetadata.Builder builder = CoordinationMetadata.builder(currentState.coordinationMetadata());
                resolvedExclusions.forEach(builder::addVotingConfigExclusion);
                final Metadata newMetadata = Metadata.builder(currentState.metadata()).coordinationMetadata(builder.build()).build();
                final ClusterState newState = ClusterState.builder(currentState).metadata(newMetadata).build();
                assert newState.getVotingConfigExclusions().size() <= finalMaxVotingConfigExclusions;
                return newState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            /**
             * 任务完成的时候会触发监听器   也就是发布任务并不能确保已经完成了   必须确保超半数节点修改成功 才能触发相关钩子
             * @param source
             * @param oldState
             * @param newState
             */
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

                final ClusterStateObserver observer
                    = new ClusterStateObserver(clusterService, request.getTimeout(), logger, threadPool.getThreadContext());

                final Set<String> excludedNodeIds = resolvedExclusions.stream().map(VotingConfigExclusion::getNodeId)
                    .collect(Collectors.toSet());

                // 当这些节点不再出现在参选节点列表后
                final Predicate<ClusterState> allNodesRemoved = clusterState -> {
                    final Set<String> votingConfigNodeIds = clusterState.getLastCommittedConfiguration().getNodeIds();
                    return excludedNodeIds.stream().noneMatch(votingConfigNodeIds::contains);
                };

                final Listener clusterStateListener = new Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        listener.onResponse(new AddVotingConfigExclusionsResponse());
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new ElasticsearchException("cluster service closed while waiting for voting config exclusions " +
                            resolvedExclusions + " to take effect"));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(new ElasticsearchTimeoutException("timed out waiting for voting config exclusions "
                            + resolvedExclusions + " to take effect"));
                    }
                };

                // TODO 这个监听器应该是要发布到超过半数  role:master 才会触发onNewClusterState吧
                if (allNodesRemoved.test(newState)) {
                    clusterStateListener.onNewClusterState(newState);
                } else {
                    observer.waitForNextChange(clusterStateListener, allNodesRemoved);
                }
            }
        });
    }

    /**
     * 校验确保总计排除的 master节点不超过上限
     * @param request
     * @param state
     * @param maxVotingConfigExclusions
     * @return
     */
    private static Set<VotingConfigExclusion> resolveVotingConfigExclusionsAndCheckMaximum(AddVotingConfigExclusionsRequest request,
                                                                                           ClusterState state,
                                                                                           int maxVotingConfigExclusions) {
        return request.resolveVotingConfigExclusionsAndCheckMaximum(state, maxVotingConfigExclusions,
            MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.getKey());
    }

    /**
     * 当处理某个请求时 检测是否应该返回被阻塞的异常
     * 该action是修改参选的节点  属于修改元数据的操作 所以是 METADATA_WRITE
     * @param request
     * @param state
     * @return
     */
    @Override
    protected ClusterBlockException checkBlock(AddVotingConfigExclusionsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
