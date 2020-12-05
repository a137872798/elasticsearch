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

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentFieldMappers;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Collections.singletonMap;

/**
 * Transport action used to retrieve the mappings related to fields that belong to a specific index
 * 以index为单位 获取fieldMapping信息
 */
public class TransportGetFieldMappingsIndexAction
        extends TransportSingleShardAction<GetFieldMappingsIndexRequest, GetFieldMappingsResponse> {

    private static final String ACTION_NAME = GetFieldMappingsAction.NAME + "[index]";
    public static final ActionType<GetFieldMappingsResponse> TYPE = new ActionType<>(ACTION_NAME, GetFieldMappingsResponse::new);

    protected final ClusterService clusterService;
    private final IndicesService indicesService;

    /**
     * 通过依赖注入的方式 使得在创建该对象时不需要知道明确的注入参数对象
     * @param clusterService
     * @param transportService
     * @param indicesService
     * @param threadPool
     * @param actionFilters
     * @param indexNameExpressionResolver
     */
    @Inject
    public TransportGetFieldMappingsIndexAction(ClusterService clusterService, TransportService transportService,
                                                IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters,
                                                IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ACTION_NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                GetFieldMappingsIndexRequest::new, ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    protected boolean resolveIndex(GetFieldMappingsIndexRequest request) {
        //internal action, index already resolved
        return false;
    }

    /**
     * 只返回当前所有活跃分片
     * @param state
     * @param request
     * @return
     */
    @Override
    protected ShardsIterator shards(ClusterState state, InternalRequest request) {
        // Will balance requests between shards
        return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
    }

    /**
     * 以分片为单位处理请求
     * @param request
     * @param shardId
     * @return
     */
    @Override
    protected GetFieldMappingsResponse shardOperation(final GetFieldMappingsIndexRequest request, ShardId shardId) {
        assert shardId != null;
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        Version indexCreatedVersion = indexService.mapperService().getIndexSettings().getIndexVersionCreated();
        // 判断这个fieldMapping是否是某个元数据的
        Predicate<String> metadataFieldPredicate = (f) -> indicesService.isMetadataField(indexCreatedVersion, f);
        // 可以不是元数据的fieldMapping  只需要通过 filter校验  (getFieldFilter() 通过指定的index名可以找到对应的filter)
        Predicate<String> fieldPredicate = metadataFieldPredicate.or(indicesService.getFieldFilter().apply(shardId.getIndexName()));

        // 获取这个index 所映射出的 document
        DocumentMapper documentMapper = indexService.mapperService().documentMapper();
        // 从中找到符合条件的 fieldMapping
        Map<String, FieldMappingMetadata> fieldMapping = findFieldMappings(fieldPredicate, documentMapper, request);
        return new GetFieldMappingsResponse(singletonMap(shardId.getIndexName(), fieldMapping));
    }

    @Override
    protected Writeable.Reader<GetFieldMappingsResponse> getResponseReader() {
        return GetFieldMappingsResponse::new;
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_READ, request.concreteIndex());
    }

    private static final ToXContent.Params includeDefaultsParams = new ToXContent.Params() {

        static final String INCLUDE_DEFAULTS = "include_defaults";

        @Override
        public String param(String key) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return null;
        }

        @Override
        public String param(String key, String defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return defaultValue;
        }

        @Override
        public boolean paramAsBoolean(String key, boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }

        @Override
        public Boolean paramAsBoolean(String key, Boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }
    };


    /**
     * 从documentMapper中找到符合条件的fieldMapping
     * @param fieldPredicate
     * @param documentMapper
     * @param request
     * @return
     */
    private static Map<String, FieldMappingMetadata> findFieldMappings(Predicate<String> fieldPredicate,
                                                                             DocumentMapper documentMapper,
                                                                             GetFieldMappingsIndexRequest request) {
        // 如果documentMapper为空就无法寻找fieldMapping
        if (documentMapper == null) {
            return Collections.emptyMap();
        }
        Map<String, FieldMappingMetadata> fieldMappings = new HashMap<>();
        // 该对象会负责管理所有 fieldMapping
        final DocumentFieldMappers allFieldMappers = documentMapper.mappers();
        for (String field : request.fields()) {
            // 如果请求中有 "*" 那么会把所有fieldMapping 都填充进去
            if (Regex.isMatchAllPattern(field)) {
                for (Mapper fieldMapper : allFieldMappers) {
                    // 将满足条件的fieldMapping 插入到容器中
                    addFieldMapper(fieldPredicate, fieldMapper.name(), fieldMapper, fieldMappings, request.includeDefaults());
                }
            } else if (Regex.isSimpleMatchPattern(field)) {
                for (Mapper fieldMapper : allFieldMappers) {
                    // 如果包含正则匹配 将符合条件的插入到容器中
                    if (Regex.simpleMatch(field, fieldMapper.name())) {
                        addFieldMapper(fieldPredicate,  fieldMapper.name(),
                                fieldMapper, fieldMappings, request.includeDefaults());
                    }
                }
            } else {
                // not a pattern
                Mapper fieldMapper = allFieldMappers.getMapper(field);
                if (fieldMapper != null) {
                    addFieldMapper(fieldPredicate, field, fieldMapper, fieldMappings, request.includeDefaults());
                }
            }
        }
        return Collections.unmodifiableMap(fieldMappings);
    }

    /**
     * 将命中条件的fieldMapper 插入到map中
     * @param fieldPredicate
     * @param field
     * @param fieldMapper
     * @param fieldMappings
     * @param includeDefaults
     */
    private static void addFieldMapper(Predicate<String> fieldPredicate,
                                       String field, Mapper fieldMapper, Map<String, FieldMappingMetadata> fieldMappings,
                                       boolean includeDefaults) {

        // 避免重复插入
        if (fieldMappings.containsKey(field)) {
            return;
        }
        // 当通过谓语检测后 才能插入到map中
        if (fieldPredicate.test(field)) {
            try {
                BytesReference bytes = XContentHelper.toXContent(fieldMapper, XContentType.JSON,
                        includeDefaults ? includeDefaultsParams : ToXContent.EMPTY_PARAMS, false);
                fieldMappings.put(field, new FieldMappingMetadata(fieldMapper.name(), bytes));
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize XContent of field [" + field + "]", e);
            }
        }
    }
}
