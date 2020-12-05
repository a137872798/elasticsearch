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

package org.elasticsearch.action.admin.indices.alias;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexAliasesService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.unmodifiableList;

/**
 * Add/remove aliases action
 * 添加/删除别名 信息   只能在leader节点处理
 */
public class TransportIndicesAliasesAction extends TransportMasterNodeAction<IndicesAliasesRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportIndicesAliasesAction.class);

    /**
     * 所有别名的处理都交由该服务
     */
    private final MetadataIndexAliasesService indexAliasesService;
    private final RequestValidators<IndicesAliasesRequest> requestValidators;

    @Inject
    public TransportIndicesAliasesAction(
            final TransportService transportService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final MetadataIndexAliasesService indexAliasesService,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final RequestValidators<IndicesAliasesRequest> requestValidators) {
        super(IndicesAliasesAction.NAME, transportService, clusterService, threadPool, actionFilters, IndicesAliasesRequest::new,
            indexNameExpressionResolver);
        this.indexAliasesService = indexAliasesService;
        this.requestValidators = Objects.requireNonNull(requestValidators);
    }

    @Override
    protected String executor() {
        // we go async right away...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesAliasesRequest request, ClusterState state) {
        Set<String> indices = new HashSet<>();
        for (AliasActions aliasAction : request.aliasActions()) {
            Collections.addAll(indices, aliasAction.indices());
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices.toArray(new String[indices.size()]));
    }

    /**
     * 处理别名请求
     * @param task
     * @param request
     * @param state
     * @param listener
     */
    @Override
    protected void masterOperation(Task task, final IndicesAliasesRequest request, final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {

        //Expand the indices names
        // 获取本次执行的所有action
        List<AliasActions> actions = request.aliasActions();
        List<AliasAction> finalActions = new ArrayList<>();

        // Resolve all the AliasActions into AliasAction instances and gather all the aliases
        Set<String> aliases = new HashSet<>();
        for (AliasActions action : actions) {
            // 获取所有相关的索引
            final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request.indicesOptions(), action.indices());
            // 通过校验器 检测req的合法性  TODO 先不细看实现逻辑
            final Optional<Exception> maybeException = requestValidators.validateRequest(request, state, concreteIndices);
            if (maybeException.isPresent()) {
                listener.onFailure(maybeException.get());
                return;
            }
            Collections.addAll(aliases, action.getOriginalAliases());
            // 某次action操作涉及到的所有index
            // TODO 不纠结 反正也是map操作
            for (final Index index : concreteIndices) {
                switch (action.actionType()) {
                case ADD:
                    // 代表本次处理的是一个添加请求   只会返回当前req中携带的所有别名
                    for (String alias : concreteAliases(action, state.metadata(), index.getName())) {
                        // 将action中的其他信息包装成 Add 对象后插入到最终要执行的action列表中
                        finalActions.add(new AliasAction.Add(index.getName(), alias, action.filter(), action.indexRouting(),
                            action.searchRouting(), action.writeIndex(), action.isHidden()));
                    }
                    break;
                case REMOVE:
                    for (String alias : concreteAliases(action, state.metadata(), index.getName())) {
                        finalActions.add(new AliasAction.Remove(index.getName(), alias));
                    }
                    break;
                case REMOVE_INDEX:
                    finalActions.add(new AliasAction.RemoveIndex(index.getName()));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported action [" + action.actionType() + "]");
                }
            }
        }
        if (finalActions.isEmpty() && false == actions.isEmpty()) {
            throw new AliasesNotFoundException(aliases.toArray(new String[aliases.size()]));
        }
        request.aliasActions().clear();
        IndicesAliasesClusterStateUpdateRequest updateRequest = new IndicesAliasesClusterStateUpdateRequest(unmodifiableList(finalActions))
                .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout());

        indexAliasesService.indicesAliases(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Exception t) {
                logger.debug("failed to perform aliases", t);
                listener.onFailure(t);
            }
        });
    }

    /**
     * 处理index 获取相关别名
     * @param action  包含本次操作的具体信息
     * @param metadata
     * @param concreteIndex
     * @return
     */
    private static String[] concreteAliases(AliasActions action, Metadata metadata, String concreteIndex) {
        // 删除操作支持使用通配符  添加操作不支持
        if (action.expandAliasesWildcards()) {
            //for DELETE we expand the aliases
            String[] indexAsArray = {concreteIndex};
            // 将本次index对应的所有别名 以及当前action中携带的别名插入到列表中
            ImmutableOpenMap<String, List<AliasMetadata>> aliasMetadata = metadata.findAliases(action, indexAsArray);
            List<String> finalAliases = new ArrayList<>();
            for (ObjectCursor<List<AliasMetadata>> curAliases : aliasMetadata.values()) {
                for (AliasMetadata aliasMeta: curAliases.value) {
                    finalAliases.add(aliasMeta.alias());
                }
            }
            return finalAliases.toArray(new String[finalAliases.size()]);
        } else {
            //for ADD and REMOVE_INDEX we just return the current aliases
            return action.aliases();
        }
    }
}
