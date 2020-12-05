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

package org.elasticsearch.action.admin.indices.get;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Get index action.
 * 获取某些索引信息
 */
public class TransportGetIndexAction extends TransportClusterInfoAction<GetIndexRequest, GetIndexResponse> {

    private final IndicesService indicesService;
    private final IndexScopedSettings indexScopedSettings;
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportGetIndexAction(TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, SettingsFilter settingsFilter, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver, IndicesService indicesService,
                                   IndexScopedSettings indexScopedSettings) {
        super(GetIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, GetIndexRequest::new,
                indexNameExpressionResolver);
        this.indicesService = indicesService;
        this.settingsFilter = settingsFilter;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    protected String executor() {
        // very lightweight operation, no need to fork
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected GetIndexResponse read(StreamInput in) throws IOException {
        return new GetIndexResponse(in);
    }

    /**
     *
     * @param request
     * @param concreteIndices  本次要获取哪些索引信息
     * @param state
     * @param listener
     */
    @Override
    protected void doMasterOperation(final GetIndexRequest request, String[] concreteIndices, final ClusterState state,
                                     final ActionListener<GetIndexResponse> listener) {
        ImmutableOpenMap<String, MappingMetadata> mappingsResult = ImmutableOpenMap.of();
        ImmutableOpenMap<String, List<AliasMetadata>> aliasesResult = ImmutableOpenMap.of();
        ImmutableOpenMap<String, Settings> settings = ImmutableOpenMap.of();
        ImmutableOpenMap<String, Settings> defaultSettings = ImmutableOpenMap.of();

        // 本次请求中会描述要获取index的哪些特性
        Feature[] features = request.features();
        boolean doneAliases = false;
        boolean doneMappings = false;
        boolean doneSettings = false;
        // 将对应各个 feature的数据取出来 合并后返回
        for (Feature feature : features) {
            switch (feature) {
                // 代表要获取index对应的mappings信息
            case MAPPINGS:
                    if (!doneMappings) {
                        try {
                            mappingsResult = state.metadata().findMappings(concreteIndices, indicesService.getFieldFilter());
                            doneMappings = true;
                        } catch (IOException e) {
                            listener.onFailure(e);
                            return;
                        }
                    }
                    break;
            case ALIASES:
                    if (!doneAliases) {
                        aliasesResult = state.metadata().findAllAliases(concreteIndices);
                        doneAliases = true;
                    }
                    break;
            case SETTINGS:
                    if (!doneSettings) {
                        ImmutableOpenMap.Builder<String, Settings> settingsMapBuilder = ImmutableOpenMap.builder();
                        ImmutableOpenMap.Builder<String, Settings> defaultSettingsMapBuilder = ImmutableOpenMap.builder();
                        for (String index : concreteIndices) {
                            Settings indexSettings = state.metadata().index(index).getSettings();
                            if (request.humanReadable()) {
                                indexSettings = IndexMetadata.addHumanReadableSettings(indexSettings);
                            }
                            settingsMapBuilder.put(index, indexSettings);
                            if (request.includeDefaults()) {
                                Settings defaultIndexSettings =
                                    settingsFilter.filter(indexScopedSettings.diff(indexSettings, Settings.EMPTY));
                                defaultSettingsMapBuilder.put(index, defaultIndexSettings);
                            }
                        }
                        settings = settingsMapBuilder.build();
                        defaultSettings = defaultSettingsMapBuilder.build();
                        doneSettings = true;
                    }
                    break;

                default:
                    throw new IllegalStateException("feature [" + feature + "] is not valid");
            }
        }
        listener.onResponse(
            new GetIndexResponse(concreteIndices, mappingsResult, aliasesResult, settings, defaultSettings)
        );
    }
}
