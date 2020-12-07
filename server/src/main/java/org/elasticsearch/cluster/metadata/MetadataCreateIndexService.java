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

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.ack.CreateIndexClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.IndexCreationException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

/**
 * Service responsible for submitting create index requests
 */
public class MetadataCreateIndexService {
    private static final Logger logger = LogManager.getLogger(MetadataCreateIndexService.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public static final int MAX_INDEX_NAME_BYTES = 255;

    /**
     * 全局配置
     */
    private final Settings settings;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final AliasValidator aliasValidator;
    private final Environment env;
    /**
     * 索引级别的配置
     */
    private final IndexScopedSettings indexScopedSettings;
    private final ActiveShardsObserver activeShardsObserver;
    private final NamedXContentRegistry xContentRegistry;
    private final Collection<SystemIndexDescriptor> systemIndexDescriptors;
    private final boolean forbidPrivateIndexSettings;

    public MetadataCreateIndexService(
        final Settings settings,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final AllocationService allocationService,
        final AliasValidator aliasValidator,
        final Environment env,
        final IndexScopedSettings indexScopedSettings,
        final ThreadPool threadPool,
        final NamedXContentRegistry xContentRegistry,
        final Collection<SystemIndexDescriptor> systemIndexDescriptors,
        final boolean forbidPrivateIndexSettings) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.aliasValidator = aliasValidator;
        this.env = env;
        this.indexScopedSettings = indexScopedSettings;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
        this.xContentRegistry = xContentRegistry;
        this.systemIndexDescriptors = systemIndexDescriptors;
        this.forbidPrivateIndexSettings = forbidPrivateIndexSettings;
    }

    /**
     * Validate the name for an index against some static rules and a cluster state.
     * 检测某个索引名是否合法
     */
    public void validateIndexName(String index, ClusterState state) {
        validateIndexOrAliasName(index, InvalidIndexNameException::new);
        if (!index.toLowerCase(Locale.ROOT).equals(index)) {
            throw new InvalidIndexNameException(index, "must be lowercase");
        }

        // NOTE: dot-prefixed index names are validated after template application, not here

        // 如果当前state中已经存在这个索引了 抛出异常
        if (state.routingTable().hasIndex(index)) {
            throw new ResourceAlreadyExistsException(state.routingTable().index(index).getIndex());
        }
        if (state.metadata().hasIndex(index)) {
            throw new ResourceAlreadyExistsException(state.metadata().index(index).getIndex());
        }
        if (state.metadata().hasAlias(index)) {
            throw new InvalidIndexNameException(index, "already exists as alias");
        }
    }

    /**
     * Validates (if this index has a dot-prefixed name) whether it follows the rules for dot-prefixed indices.
     * @param index The name of the index in question
     * @param state The current cluster state
     * @param isHidden Whether or not this is a hidden index  当前索引是否是一个隐藏索引
     *                 检测索引中是否携带 "."
     */
    public void validateDotIndex(String index, ClusterState state, @Nullable Boolean isHidden) {
        if (index.charAt(0) == '.') {
            // 找到匹配的索引描述器
            List<SystemIndexDescriptor> matchingDescriptors = systemIndexDescriptors.stream()
                .filter(descriptor -> descriptor.matchesIndexPattern(index))
                .collect(toList());
            if (matchingDescriptors.isEmpty() && (isHidden == null || isHidden == Boolean.FALSE)) {
                deprecationLogger.deprecatedAndMaybeLog("index_name_starts_with_dot",
                    "index name [{}] starts with a dot '.', in the next major version, index names " +
                    "starting with a dot are reserved for hidden indices and system indices", index);
            // 当匹配结果超过1个时 抛出异常
            } else if (matchingDescriptors.size() > 1) {
                // This should be prevented by erroring on overlapping patterns at startup time, but is here just in case.
                StringBuilder errorMessage = new StringBuilder()
                    .append("index name [")
                    .append(index)
                    .append("] is claimed as a system index by multiple system index patterns: [")
                    .append(matchingDescriptors.stream()
                        .map(descriptor -> "pattern: [" + descriptor.getIndexPattern() +
                            "], description: [" + descriptor.getDescription() + "]").collect(Collectors.joining("; ")));
                // Throw AssertionError if assertions are enabled, or a regular exception otherwise:
                assert false : errorMessage.toString();
                throw new IllegalStateException(errorMessage.toString());
            }
        }
    }

    /**
     * Validate the name for an index or alias against some static rules.
     */
    public static void validateIndexOrAliasName(String index, BiFunction<String, String, ? extends RuntimeException> exceptionCtor) {
        if (!Strings.validFileName(index)) {
            throw exceptionCtor.apply(index, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (index.contains("#")) {
            throw exceptionCtor.apply(index, "must not contain '#'");
        }
        if (index.contains(":")) {
            throw exceptionCtor.apply(index, "must not contain ':'");
        }
        if (index.charAt(0) == '_' || index.charAt(0) == '-' || index.charAt(0) == '+') {
            throw exceptionCtor.apply(index, "must not start with '_', '-', or '+'");
        }
        int byteCount = 0;
        try {
            byteCount = index.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new ElasticsearchException("Unable to determine length of index name", e);
        }
        if (byteCount > MAX_INDEX_NAME_BYTES) {
            throw exceptionCtor.apply(index, "index name is too long, (" + byteCount + " > " + MAX_INDEX_NAME_BYTES + ")");
        }
        if (index.equals(".") || index.equals("..")) {
            throw exceptionCtor.apply(index, "must not be '.' or '..'");
        }
    }

    /**
     * Creates an index in the cluster state and waits for the specified number of shard copies to
     * become active (as specified in {@link CreateIndexClusterStateUpdateRequest#waitForActiveShards()})
     * before sending the response on the listener. If the index creation was successfully applied on
     * the cluster state, then {@link CreateIndexClusterStateUpdateResponse#isAcknowledged()} will return
     * true, otherwise it will return false and no waiting will occur for started shards
     * ({@link CreateIndexClusterStateUpdateResponse#isShardsAcknowledged()} will also be false).  If the index
     * creation in the cluster state was successful and the requisite shard copies were started before
     * the timeout, then {@link CreateIndexClusterStateUpdateResponse#isShardsAcknowledged()} will
     * return true, otherwise if the operation timed out, then it will return false.
     *
     * @param request the index creation cluster state update request
     * @param listener the listener on which to send the index creation cluster state update response
     *                 发布一个更新clusterState的任务
     */
    public void createIndex(final CreateIndexClusterStateUpdateRequest request,
                            final ActionListener<CreateIndexClusterStateUpdateResponse> listener) {
        logger.trace("createIndex[{}]", request);
        onlyCreateIndex(request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                logger.trace("[{}] index creation acknowledged, waiting for active shards [{}]",
                    request.index(), request.waitForActiveShards());
                activeShardsObserver.waitForActiveShards(new String[]{request.index()}, request.waitForActiveShards(), request.ackTimeout(),
                    shardsAcknowledged -> {
                        if (shardsAcknowledged == false) {
                            logger.debug("[{}] index created, but the operation timed out while waiting for " +
                                             "enough shards to be started.", request.index());
                        } else {
                            logger.trace("[{}] index created and shards acknowledged", request.index());
                        }
                        listener.onResponse(new CreateIndexClusterStateUpdateResponse(response.isAcknowledged(), shardsAcknowledged));
                    }, listener::onFailure);
            } else {
                logger.trace("index creation not acknowledged for [{}]", request);
                listener.onResponse(new CreateIndexClusterStateUpdateResponse(false, false));
            }
        }, listener::onFailure));
    }

    /**
     * 创建索引 并且将相关数据填充到 ClusterState中 并发布到集群中
     * @param request
     * @param listener
     */
    private void  onlyCreateIndex(final CreateIndexClusterStateUpdateRequest request,
                                 final ActionListener<ClusterStateUpdateResponse> listener) {
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        // 将req中的所有配置都加上 "index." 前缀
        Settings build = updatedSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        indexScopedSettings.validate(build, true); // we do validate here - index setting must be consistent
        request.settings(build);

        // 发起一个更新任务
        clusterService.submitStateUpdateTask(
            "create-index [" + request.index() + "], cause [" + request.cause() + "]",
            new AckedClusterStateUpdateTask<>(Priority.URGENT, request, listener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return applyCreateIndexRequest(currentState, request, false);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    if (e instanceof ResourceAlreadyExistsException) {
                        logger.trace(() -> new ParameterizedMessage("[{}] failed to create", request.index()), e);
                    } else {
                        logger.debug(() -> new ParameterizedMessage("[{}] failed to create", request.index()), e);
                    }
                    super.onFailure(source, e);
                }
            });
    }

    /**
     * Handles the cluster state transition to a version that reflects the {@link CreateIndexClusterStateUpdateRequest}.
     * All the requested changes are firstly validated before mutating the {@link ClusterState}.
     * @param currentState 当前集群状态
     * @param request 申请创建索引的请求
     * @param silent 是否静默处理
     */
    public ClusterState applyCreateIndexRequest(ClusterState currentState, CreateIndexClusterStateUpdateRequest request, boolean silent,
                                                BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer) throws Exception {
        logger.trace("executing IndexCreationTask for [{}] against cluster state version [{}]", request, currentState.version());

        // 校验indexName是否已经存在 以及配置项是否合法
        validate(request, currentState);

        // 代表该index 会从哪个索引获取数据  (初始化数据的过程被称为 recover)
        final Index recoverFromIndex = request.recoverFrom();
        // 如果指定了recoverFrom 使用该index的元数据
        final IndexMetadata sourceMetadata = recoverFromIndex == null ? null : currentState.metadata().getIndexSafe(recoverFromIndex);

        // TODO
        if (sourceMetadata != null) {
            // If source metadata was provided, it means we're recovering from an existing index,
            // in which case templates don't apply, so create the index from the source metadata
            return applyCreateIndexRequestWithExistingMetadata(currentState, request, silent, sourceMetadata, metadataTransformer);
        } else {
            // Hidden indices apply templates slightly differently (ignoring wildcard '*'
            // templates), so we need to check to see if the request is creating a hidden index
            // prior to resolving which templates it matches
            // 当使用 DataStream 创建索引数据时  会设置hidden 为true
            final Boolean isHiddenFromRequest = IndexMetadata.INDEX_HIDDEN_SETTING.exists(request.settings()) ?
                IndexMetadata.INDEX_HIDDEN_SETTING.get(request.settings()) : null;

            // Check to see if a v2 template matched
            // 检测是否使用的是 V2版本的模板
            final String v2Template = MetadataIndexTemplateService.findV2Template(currentState.metadata(),
                request.index(), isHiddenFromRequest == null ? false : isHiddenFromRequest);
            final boolean preferV2Templates = resolvePreferV2Templates(request);

            if (v2Template != null && preferV2Templates) {
                // If a v2 template was found, it takes precedence over all v1 templates, so create
                // the index using that template and the request's specified settings
                // 处理V2版本的模板 以及req中的信息 抽取出来生成indexMetadata后设置到ClusterState中 并返回
                return applyCreateIndexRequestWithV2Template(currentState, request, silent, v2Template, metadataTransformer);
            } else {
                // TODO 忽略旧模板 因为V1版本可能会在之后被移除 并且逻辑跟V2的应该差不多
                // 当没有找到匹配的V2版本模板时 使用V1版本的模板
                if (v2Template != null) {
                    logger.debug("ignoring matching index template [{}] as [prefer_v2_templates] is set to false", v2Template);
                }
                // A v2 template wasn't found (or is not preferred), check the v1 templates, in the
                // event no templates are found creation still works using the request's specified
                // index settings
                final List<IndexTemplateMetadata> v1Templates = MetadataIndexTemplateService.findV1Templates(currentState.metadata(),
                    request.index(), isHiddenFromRequest);

                if (v1Templates.size() > 1) {
                    deprecationLogger.deprecatedAndMaybeLog("index_template_multiple_match", "index [{}] matches multiple v1 templates " +
                        "[{}], v2 index templates will only match a single index template", request.index(),
                        v1Templates.stream().map(IndexTemplateMetadata::name).sorted().collect(Collectors.joining(", ")));
                }

                // 从V1版本的模板中抽取相关信息 并更新ClusterState
                return applyCreateIndexRequestWithV1Templates(currentState, request, silent, v1Templates, metadataTransformer);
            }
        }
    }

    private static boolean resolvePreferV2Templates(CreateIndexClusterStateUpdateRequest request) {
        return request.preferV2Templates() == null ?
            IndexMetadata.PREFER_V2_TEMPLATES_SETTING.get(request.settings()) : request.preferV2Templates();
    }

    /**
     * 使用请求去更新 clusterState    是一个创建index的请求
     * @param currentState
     * @param request
     * @param silent  是否静默处理
     * @return
     * @throws Exception
     */
    public ClusterState applyCreateIndexRequest(ClusterState currentState, CreateIndexClusterStateUpdateRequest request,
                                                boolean silent) throws Exception {
        return applyCreateIndexRequest(currentState, request, silent, null);
    }

    /**
     * Given the state and a request as well as the metadata necessary to build a new index,
     * validate the configuration with an actual index service as return a new cluster state with
     * the index added (and rerouted)
     * @param currentState the current state to base the new state off of
     * @param request the create index request
     * @param silent a boolean for whether logging should be at a lower or higher level
     * @param sourceMetadata when recovering from an existing index, metadata that should be copied to the new index
     * @param temporaryIndexMeta metadata for the new index built from templates, source metadata, and request settings
     * @param mappings a map of mappings for the new index
     * @param aliasSupplier a function that takes the real {@link IndexService} and returns a list of {@link AliasMetadata} aliases
     * @param templatesApplied a list of the names of the templates applied, for logging
     * @param metadataTransformer if provided, a function that may alter cluster metadata in the same cluster state update that
     *                            creates the index
     * @return a new cluster state with the index added
     * 临时创建一个索引 并进行检测
     */
    private ClusterState applyCreateIndexWithTemporaryService(final ClusterState currentState,
                                                              final CreateIndexClusterStateUpdateRequest request,
                                                              final boolean silent,
                                                              final IndexMetadata sourceMetadata,  // 当前index的创建可能借鉴了之前的某个indexMetadata 就会传入该值
                                                              final IndexMetadata temporaryIndexMeta,   // 根据现有信息创建的临时性的indexMetadata 还会做修改
                                                              final Map<String, Object> mappings,   // 将template 与req中的json字符串解析并合并后的容器
                                                              final Function<IndexService, List<AliasMetadata>> aliasSupplier,
                                                              final List<String> templatesApplied,
                                                              final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer)
                                                                                        throws Exception {
        // create the index here (on the master) to validate it can be created, as well as adding the mapping
        // 这里生成IndexService 并进行处理后 最终还是会关闭indexService的  但是已经生成了最新的元数据  并且此时还没有开始为分片recovery数据
        return indicesService.<ClusterState, Exception>withTempIndexService(temporaryIndexMeta,
            // 当临时性的 indexService被创建后 使用该函数进行处理
            indexService -> {
            try {
                // 这里会将mappings 按照各种信息拆解并生成Mapper对象  最终被包装成DocumentMapper对象  并填充到这个indexService相关的 MapperService中
                updateIndexMappingsAndBuildSortOrder(indexService, mappings, sourceMetadata);
            } catch (Exception e) {
                logger.debug("failed on parsing mappings on index creation [{}]", request.index());
                throw e;
            }

            // 获取 req/IndexTemplate/ComponentTemplate 下所有的 aliasMetadata 
            final List<AliasMetadata> aliases = aliasSupplier.apply(indexService);

            final IndexMetadata indexMetadata;
            try {
                // 通过之前生成的各种信息 生成一个新的indexMetadata
                indexMetadata = buildIndexMetadata(request.index(), aliases, indexService.mapperService()::documentMapper,
                    temporaryIndexMeta.getSettings(), temporaryIndexMeta.getRoutingNumShards(), sourceMetadata);
            } catch (Exception e) {
                logger.info("failed to build index metadata [{}]", request.index());
                throw e;
            }

            logger.log(silent ? Level.DEBUG : Level.INFO, "[{}] creating index, cause [{}], templates {}, shards [{}]/[{}], mappings {}",
                request.index(), request.cause(), templatesApplied, indexMetadata.getNumberOfShards(),
                indexMetadata.getNumberOfReplicas(), mappings.keySet());

            // 可以看到此时index 还没有加入到cluster中
            indexService.getIndexEventListener().beforeIndexAddedToCluster(indexMetadata.getIndex(),
                indexMetadata.getSettings());
            // 为index创建分片 并将相关信息填充到routingTable
            // 伴随着的还有为这些分片设置路由信息(借助allocationService)
            return clusterStateCreateIndex(currentState, request.blocks(), indexMetadata, allocationService::reroute, metadataTransformer);
        });
    }

    /**
     * Given a state and index settings calculated after applying templates, validate metadata for
     * the new index, returning an {@link IndexMetadata} for the new index
     * @param aggregatedIndexSettings 此时已经整合了相关配置 以及填充了一些缺省值
     * @param routingNumShards 应该是 shardid的数量
     * @param request 本次创建index的请求对象
     *
     *                根据相关参数 构建本次index的元数据对象
     */
    private IndexMetadata buildAndValidateTemporaryIndexMetadata(final ClusterState currentState,
                                                                 final Settings aggregatedIndexSettings,
                                                                 final CreateIndexClusterStateUpdateRequest request,
                                                                 final int routingNumShards,
                                                                 final boolean preferV2Templates) {

        // 判断当前index是否应该被隐藏  当使用dataStream创建index时 会默认设置成true
        final boolean isHiddenAfterTemplates = IndexMetadata.INDEX_HIDDEN_SETTING.get(aggregatedIndexSettings);
        // 就是校验index是否包含 "." 先忽略
        validateDotIndex(request.index(), currentState, isHiddenAfterTemplates);

        // remove the setting it's temporary and is only relevant once we create the index
        final Settings.Builder settingsBuilder = Settings.builder().put(aggregatedIndexSettings);
        // 这里将 routing_shards 的配置项移除掉了 又插入了 prefer_v2 配置
        settingsBuilder.remove(IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey());
        settingsBuilder.put(IndexMetadata.PREFER_V2_TEMPLATES_SETTING.getKey(), preferV2Templates);
        final Settings indexSettings = settingsBuilder.build();

        final IndexMetadata.Builder tmpImdBuilder = IndexMetadata.builder(request.index());
        // 相当于使用 IndexMetadata中的分片数来替代settings的分片数么
        tmpImdBuilder.setRoutingNumShards(routingNumShards);
        tmpImdBuilder.settings(indexSettings);

        // Set up everything, now locally create the index to see that things are ok, and apply
        IndexMetadata tempMetadata = tmpImdBuilder.build();
        // 检测当前分片数是否会达到本次req要求的分片数
        validateActiveShardCount(request.waitForActiveShards(), tempMetadata);

        return tempMetadata;
    }

    // TODO: this method can be removed in 9.0 because we will no longer use v1 templates to create indices (only v2 templates)
    private ClusterState applyCreateIndexRequestWithV1Templates(final ClusterState currentState,
                                                                final CreateIndexClusterStateUpdateRequest request,
                                                                final boolean silent,
                                                                final List<IndexTemplateMetadata> templates,
                                                                final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer)
                                                                                        throws Exception {
        logger.info("applying create index request using v1 templates {}",
            templates.stream().map(IndexTemplateMetadata::name).collect(Collectors.toList()));

        final Map<String, Object> mappings = Collections.unmodifiableMap(parseV1Mappings(request.mappings(),
            templates.stream().map(IndexTemplateMetadata::getMappings).collect(toList()), xContentRegistry));

        final Settings aggregatedIndexSettings =
            aggregateIndexSettings(currentState, request, MetadataIndexTemplateService.resolveSettings(templates), mappings,
                null, settings, indexScopedSettings);
        int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, null);
        IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(currentState, aggregatedIndexSettings, request, routingNumShards,
            resolvePreferV2Templates(request));

        return applyCreateIndexWithTemporaryService(currentState, request, silent, null, tmpImd, mappings,
            indexService -> resolveAndValidateAliases(request.index(), request.aliases(),
                MetadataIndexTemplateService.resolveAliases(templates), currentState.metadata(), aliasValidator,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                xContentRegistry, indexService.newQueryShardContext(0, null, () -> 0L, null)),
            templates.stream().map(IndexTemplateMetadata::getName).collect(toList()), metadataTransformer);
    }

    /**
     * 如果本次创建的index 最匹配的模板是 V2Template
     * @param currentState
     * @param request
     * @param silent
     * @param templateName
     * @param metadataTransformer
     * @return
     * @throws Exception
     */
    private ClusterState applyCreateIndexRequestWithV2Template(final ClusterState currentState,
                                                               final CreateIndexClusterStateUpdateRequest request,
                                                               final boolean silent,
                                                               final String templateName,
                                                               final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer)
                                                                                    throws Exception {
        logger.info("applying create index request using v2 template [{}]", templateName);

        // 将所有相关的mappings作为json格式字符串 转换成map对象后 进行合并返回
        final Map<String, Object> mappings = resolveV2Mappings(request.mappings(), currentState, templateName, xContentRegistry);

        final Settings aggregatedIndexSettings =
            // 在通过template解析基础配置后 又增加了一组默认配置
            aggregateIndexSettings(currentState, request,
                // 从模板中获取所有相关的配置
                MetadataIndexTemplateService.resolveSettings(currentState.metadata(), templateName),
                mappings, null, settings, indexScopedSettings);

        // 获取当前总计会创建的shardId数量
        int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, null);
        // 为本次索引创建元数据
        IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(currentState, aggregatedIndexSettings, request, routingNumShards,
            // req中会携带是否倾向于使用V2Template
            resolvePreferV2Templates(request));

        // 这里将所有相关信息都填充到clusterState了 并且没有真正的打开indexService (包含解析json后生成的Mapping对象也设置到 indexMetadata中了)
        return applyCreateIndexWithTemporaryService(currentState, request, silent, null, tmpImd, mappings,
            // 这个方法最终就是将 req/template的所有别名合并后返回
            indexService -> resolveAndValidateAliases(request.index(), request.aliases(),
                // 找到模板中存储的别名 套路是一样的  就是将IndexTemplate 以及 ComponentTemplate的别名取出来 并合并在一起
                MetadataIndexTemplateService.resolveAliases(currentState.metadata(), templateName), currentState.metadata(), aliasValidator,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                xContentRegistry, indexService.newQueryShardContext(0, null, () -> 0L, null)),
            Collections.singletonList(templateName), metadataTransformer);
    }

    /**
     * 处理mappings 对象
     * @param requestMappings   req中携带的mappings数据
     * @param currentState
     * @param templateName
     * @param xContentRegistry
     * @return
     * @throws Exception
     */
    public static Map<String, Object> resolveV2Mappings(final String requestMappings,
                                                        final ClusterState currentState,
                                                        final String templateName,
                                                        final NamedXContentRegistry xContentRegistry) throws Exception {
        final Map<String, Object> mappings = Collections.unmodifiableMap(
            // 将 请求体 indexTemplate 以及相关的 componentTemplate 都合并到一个map后 返回
            parseV2Mappings(requestMappings,
            // resolveMappings 会从元数据中找到 templateName对应的template对象 以及他需要的一组 componentTemplate 并抽取出名字返回
            MetadataIndexTemplateService.resolveMappings(currentState, templateName), xContentRegistry));
        return mappings;
    }

    private ClusterState applyCreateIndexRequestWithExistingMetadata(final ClusterState currentState,
                                                                     final CreateIndexClusterStateUpdateRequest request,
                                                                     final boolean silent,
                                                                     final IndexMetadata sourceMetadata,
                                                                     final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer)
                                                                                            throws Exception {
        logger.info("applying create index request using existing index [{}] metadata", sourceMetadata.getIndex().getName());

        final Map<String, Object> mappings = Collections.unmodifiableMap(MapperService.parseMapping(xContentRegistry, request.mappings()));

        final Settings aggregatedIndexSettings =
            aggregateIndexSettings(currentState, request, Settings.EMPTY, mappings, sourceMetadata, settings, indexScopedSettings);
        final int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, sourceMetadata);
        IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(currentState, aggregatedIndexSettings, request, routingNumShards,
            IndexMetadata.PREFER_V2_TEMPLATES_SETTING.get(sourceMetadata.getSettings()));

        return applyCreateIndexWithTemporaryService(currentState, request, silent, sourceMetadata, tmpImd, mappings,
            indexService -> resolveAndValidateAliases(request.index(), request.aliases(), Collections.emptyList(),
                currentState.metadata(), aliasValidator, xContentRegistry,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                // QueryShardContext 只是作为校验用的参数 可以先不看
                indexService.newQueryShardContext(0, null, () -> 0L, null)),
            List.of(), metadataTransformer);
    }

    /**
     * Parses the provided mappings json and the inheritable mappings from the templates (if any)
     * into a map.
     *
     * The template mappings are applied in the order they are encountered in the list, with the
     * caveat that mapping fields are only merged at the top-level, meaning that field settings are
     * not merged, instead they replace any previous field definition.
     * @param mappingsJson 请求体中携带的 mapping字符串
     * @param templateMappings 从本次最合适的template中抽取出来了各种mapping  最后一个对应的IndexTemplate的主模板 其余都是componentTemplate
     * @param xContentRegistry 这个工厂应该就是进行映射转换的地方
     * 解析V2    这里最终都是只获取 _doc 属性
     *
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> parseV2Mappings(String mappingsJson, List<CompressedXContent> templateMappings,
                                               NamedXContentRegistry xContentRegistry) throws Exception {
        // 将req中的json格式字符串转换成map对象
        Map<String, Object> requestMappings = MapperService.parseMapping(xContentRegistry, mappingsJson);
        // apply templates, merging the mappings into the request mapping if exists
        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> nonProperties = new HashMap<>();

        // 遍历本次模板中用到的数据结构
        for (CompressedXContent mapping : templateMappings) {
            if (mapping != null) {
                // 解析json字符串 转换成map对象
                Map<String, Object> templateMapping = MapperService.parseMapping(xContentRegistry, mapping.string());
                // 代表是一个空的json 比如 {}
                if (templateMapping.isEmpty()) {
                    // Someone provided an empty '{}' for mappings, which is okay, but to avoid
                    // tripping the below assertion, we can safely ignore it
                    continue;
                }
                assert templateMapping.size() == 1 : "expected exactly one mapping value, got: " + templateMapping;

                // 获取"_doc" 属性 并且应该也是一个map对象
                if (templateMapping.get(MapperService.SINGLE_MAPPING_NAME) instanceof Map == false) {
                    throw new IllegalStateException("invalid mapping definition, expected a single map underneath [" +
                        MapperService.SINGLE_MAPPING_NAME + "] but it was: [" + templateMapping + "]");
                }

                Map<String, Object> innerTemplateMapping = (Map<String, Object>) templateMapping.get(MapperService.SINGLE_MAPPING_NAME);
                Map<String, Object> innerTemplateNonProperties = new HashMap<>(innerTemplateMapping);
                // _doc 下应该有 properties属性
                Map<String, Object> maybeProperties = (Map<String, Object>) innerTemplateNonProperties.remove("properties");

                // 看来每次都会将nonProperties 合并到innerTemplateNonProperties中 同时所有properties 会合并到另一个容器
                XContentHelper.mergeDefaults(innerTemplateNonProperties, nonProperties);
                // 赋值成 剔除 properties属性后的json对象
                nonProperties = innerTemplateNonProperties;

                // 如果当前json对象中 包含 properties属性
                if (maybeProperties != null) {
                    // 将maybeProperties 的合并到properties 中
                    properties = mergeIgnoringDots(properties, maybeProperties);
                }
            }
        }

        // 在上面完成了 非 properties的数据 以及 properties的数据合并 不过这时候应该只是一个模板 还没有填充任何数据吧
        // 将请求体中的数据合并到模板中
        if (requestMappings.get(MapperService.SINGLE_MAPPING_NAME) != null) {
            Map<String, Object> innerRequestMappings = (Map<String, Object>) requestMappings.get(MapperService.SINGLE_MAPPING_NAME);
            Map<String, Object> innerRequestNonProperties = new HashMap<>(innerRequestMappings);
            Map<String, Object> maybeRequestProperties = (Map<String, Object>) innerRequestNonProperties.remove("properties");

            XContentHelper.mergeDefaults(innerRequestNonProperties, nonProperties);
            nonProperties = innerRequestNonProperties;

            if (maybeRequestProperties != null) {
                properties = mergeIgnoringDots(properties, maybeRequestProperties);
            }
        }

        // 重新插入 properties 并将最终的map返回
        Map<String, Object> finalMappings = new HashMap<>(nonProperties);
        finalMappings.put("properties", properties);

        // 最后返回的是一个 只包含 _doc 键值对的 map
        return Collections.singletonMap(MapperService.SINGLE_MAPPING_NAME, finalMappings);
    }

    /**
     * Add the objects in the second map to the first, where the keys in the {@code second} map have
     * higher predecence and overwrite the keys in the {@code first} map. In the event of a key with
     * a dot in it (ie, "foo.bar"), the keys are treated as only the prefix counting towards
     * equality. If the {@code second} map has a key such as "foo", all keys starting from "foo." in
     * the {@code first} map are discarded.
     * 将 second的数据填充到 first中
     */
    static Map<String, Object> mergeIgnoringDots(Map<String, Object> first, Map<String, Object> second) {
        Objects.requireNonNull(first, "merging requires two non-null maps but the first map was null");
        Objects.requireNonNull(second, "merging requires two non-null maps but the second map was null");
        Map<String, Object> results = new HashMap<>(first);
        // 只获取 second的前缀
        Set<String> prefixes = second.keySet().stream().map(MetadataCreateIndexService::prefix).collect(Collectors.toSet());
        // 这里second 内的数据会覆盖 first的
        results.keySet().removeIf(k -> prefixes.contains(prefix(k)));
        results.putAll(second);
        return results;
    }

    /**
     * 如果字符串中携带了“.” 只获取前面的部分
     * @param s
     * @return
     */
    private static String prefix(String s) {
        return s.split("\\.", 2)[0];
    }

    /**
     * Parses the provided mappings json and the inheritable mappings from the templates (if any)
     * into a map.
     *
     * The template mappings are applied in the order they are encountered in the list (clients
     * should make sure the lower index, closer to the head of the list, templates have the highest
     * {@link IndexTemplateMetadata#order()}). This merging makes no distinction between field
     * definitions, as may result in an invalid field definition
     */
    static Map<String, Object> parseV1Mappings(String mappingsJson, List<CompressedXContent> templateMappings,
                                               NamedXContentRegistry xContentRegistry) throws Exception {
        Map<String, Object> mappings = MapperService.parseMapping(xContentRegistry, mappingsJson);
        // apply templates, merging the mappings into the request mapping if exists
        for (CompressedXContent mapping : templateMappings) {
            if (mapping != null) {
                Map<String, Object> templateMapping = MapperService.parseMapping(xContentRegistry, mapping.string());
                if (templateMapping.isEmpty()) {
                    // Someone provided an empty '{}' for mappings, which is okay, but to avoid
                    // tripping the below assertion, we can safely ignore it
                    continue;
                }
                assert templateMapping.size() == 1 : "expected exactly one mapping value, got: " + templateMapping;
                // pre-8x templates may have a wrapper type other than _doc, so we re-wrap things here
                templateMapping = Collections.singletonMap(MapperService.SINGLE_MAPPING_NAME,
                    templateMapping.values().iterator().next());
                if (mappings.isEmpty()) {
                    mappings = templateMapping;
                } else {
                    XContentHelper.mergeDefaults(mappings, templateMapping);
                }
            }
        }
        return mappings;
    }

    /**
     * Validates and creates the settings for the new index based on the explicitly configured settings via the
     * {@link CreateIndexClusterStateUpdateRequest}, inherited from templates and, if recovering from another index (ie. split, shrink,
     * clone), the resize settings.
     *
     * The template mappings are applied in the order they are encountered in the list (clients should make sure the lower index, closer
     * to the head of the list, templates have the highest {@link IndexTemplateMetadata#order()})
     *
     * @param currentState 当前集群状态
     * @param templateSettings  当前 IndexTemplate 以及相关的ComponentTemplate 的所有配置项
     * @param mappings 从模板 以及req中抽取出来的json字符串 转换为map对象后 并合并产生的
     * @param settings 当前对象在初始化时 传入的配置
     * @param indexScopedSettings  索引范围的配置
     * @return the aggregated settings for the new index
     *
     */
    static Settings aggregateIndexSettings(ClusterState currentState, CreateIndexClusterStateUpdateRequest request,
                                           Settings templateSettings, Map<String, Object> mappings,
                                           @Nullable IndexMetadata sourceMetadata, Settings settings,
                                           IndexScopedSettings indexScopedSettings) {
        Settings.Builder indexSettingsBuilder = Settings.builder();

        // 默认为null  会将当前template中已经存在的所有 settings设置到builder中
        if (sourceMetadata == null) {
            indexSettingsBuilder.put(templateSettings);
        }
        // now, put the request settings, so they override templates
        // 将请求中携带的settings也插入进去
        indexSettingsBuilder.put(request.settings());

        // 这里是插入一些内置的配置

        // 1.写入索引数据对应的lucene的版本号
        if (indexSettingsBuilder.get(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey()) == null) {
            final DiscoveryNodes nodes = currentState.nodes();
            final Version createdVersion = Version.min(Version.CURRENT, nodes.getSmallestNonClientNodeVersion());
            indexSettingsBuilder.put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), createdVersion);
        }
        // 2.该索引下有多少分片
        if (indexSettingsBuilder.get(SETTING_NUMBER_OF_SHARDS) == null) {
            indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 1));
        }
        // 3.有多少副本  总计的分片数是 SETTING_NUMBER_OF_SHARDS*(1+SETTING_NUMBER_OF_REPLICAS)
        if (indexSettingsBuilder.get(SETTING_NUMBER_OF_REPLICAS) == null) {
            indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
        }
        // 4.是否开启自适应条件分片的功能
        if (settings.get(SETTING_AUTO_EXPAND_REPLICAS) != null && indexSettingsBuilder.get(SETTING_AUTO_EXPAND_REPLICAS) == null) {
            indexSettingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, settings.get(SETTING_AUTO_EXPAND_REPLICAS));
        }

        // 5.追加创建时间
        if (indexSettingsBuilder.get(SETTING_CREATION_DATE) == null) {
            indexSettingsBuilder.put(SETTING_CREATION_DATE, Instant.now().toEpochMilli());
        }
        indexSettingsBuilder.put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, request.getProvidedName());
        indexSettingsBuilder.put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID());

        // TODO
        if (sourceMetadata != null) {
            assert request.resizeType() != null;
            prepareResizeIndexSettings(
                currentState,
                mappings.keySet(),
                indexSettingsBuilder,
                request.recoverFrom(),
                request.index(),
                request.resizeType(),
                request.copySettings(),
                indexScopedSettings);
        }

        // 此时根据所有需要的配置已经生成了最新的配置项
        Settings indexSettings = indexSettingsBuilder.build();
        /*
         * We can not check the shard limit until we have applied templates, otherwise we do not know the actual number of shards
         * that will be used to create this index.
         * 检测当前分片总数是否会超过上限
         */
        MetadataCreateIndexService.checkShardLimit(indexSettings, currentState);
        // 当8.0的版本后必须要设置软删除
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexSettings) == false
            && IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings).onOrAfter(Version.V_8_0_0)) {
            throw new IllegalArgumentException("Creating indices with soft-deletes disabled is no longer supported. " +
                "Please do not specify a value for setting [index.soft_deletes.enabled].");
        }
        validateTranslogRetentionSettings(indexSettings);
        return indexSettings;
    }

    /**
     * Calculates the number of routing shards based on the configured value in indexSettings or if recovering from another index
     * it will return the value configured for that index.
     * 计算总计会创建多少shardId
     */
    static int getIndexNumberOfRoutingShards(Settings indexSettings, @Nullable IndexMetadata sourceMetadata) {
        // 会创建多少 shardId
        final int numTargetShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings);
        final Version indexVersionCreated = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings);
        final int routingNumShards;
        if (sourceMetadata == null || sourceMetadata.getNumberOfShards() == 1) {
            // in this case we either have no index to recover from or
            // we have a source index with 1 shard and without an explicit split factor
            // or one that is valid in that case we can split into whatever and auto-generate a new factor.
            // 如果在配置中声明了总计创建多少分片 直接使用这个分片数
            if (IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(indexSettings)) {
                routingNumShards = IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(indexSettings);
            } else {
                // 进行计算
                routingNumShards = calculateNumRoutingShards(numTargetShards, indexVersionCreated);
            }
            // TODO
        } else {
            assert IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(indexSettings) == false
                : "index.number_of_routing_shards should not be present on the target index on resize";
            routingNumShards = sourceMetadata.getRoutingNumShards();
        }
        return routingNumShards;
    }

    /**
     * Validate and resolve the aliases explicitly set for the index, together with the ones inherited from the specified
     * templates.
     *
     * The template mappings are applied in the order they are encountered in the list (clients should make sure the lower index, closer
     * to the head of the list, templates have the highest {@link IndexTemplateMetadata#order()})
     *
     * @param index 本次要处理的索引名
     * @param aliases 从req中获取到的所有别名
     * @param templateAliases 从 IndexTemplate/ComponentTemplate 取出来的所有别名
     * @return the list of resolved aliases, with the explicitly provided aliases occurring first (having a higher priority) followed by
     * the ones inherited from the templates
     */
    public static List<AliasMetadata> resolveAndValidateAliases(String index, Set<Alias> aliases,
                                                                List<Map<String, AliasMetadata>> templateAliases, Metadata metadata,
                                                                AliasValidator aliasValidator, NamedXContentRegistry xContentRegistry,
                                                                QueryShardContext queryShardContext) {
        List<AliasMetadata> resolvedAliases = new ArrayList<>();
        for (Alias alias : aliases) {
            // 通过校验器 确保别名有效  TODO 校验逻辑先不看了
            aliasValidator.validateAlias(alias, index, metadata);
            if (Strings.hasLength(alias.filter())) {
                aliasValidator.validateAliasFilter(alias.name(), alias.filter(), queryShardContext, xContentRegistry);
            }
            // 将req中的别名信息包装成元数据对象 并设置到 管理所有别名的列表中
            AliasMetadata aliasMetadata = AliasMetadata.builder(alias.name()).filter(alias.filter())
                .indexRouting(alias.indexRouting()).searchRouting(alias.searchRouting()).writeIndex(alias.writeIndex())
                .isHidden(alias.isHidden()).build();
            resolvedAliases.add(aliasMetadata);
        }

        Map<String, AliasMetadata> templatesAliases = new HashMap<>();
        for (Map<String, AliasMetadata> templateAliasConfig : templateAliases) {
            // handle aliases
            for (Map.Entry<String, AliasMetadata> entry : templateAliasConfig.entrySet()) {
                AliasMetadata aliasMetadata = entry.getValue();
                // if an alias with same name came with the create index request itself,
                // ignore this one taken from the index template
                // req中的别名优先级比 template中的高
                if (aliases.contains(new Alias(aliasMetadata.alias()))) {
                    continue;
                }
                // if an alias with same name was already processed, ignore this one
                // 别名重复的时候 忽略
                if (templatesAliases.containsKey(entry.getKey())) {
                    continue;
                }

                // Allow templatesAliases to be templated by replacing a token with the
                // name of the index that we are applying it to
                // template的别名中是允许出现index占位符的   会使用当前索引名去替换
                if (aliasMetadata.alias().contains("{index}")) {
                    String templatedAlias = aliasMetadata.alias().replace("{index}", index);
                    aliasMetadata = AliasMetadata.newAliasMetadata(aliasMetadata, templatedAlias);
                }

                aliasValidator.validateAliasMetadata(aliasMetadata, index, metadata);
                if (aliasMetadata.filter() != null) {
                    aliasValidator.validateAliasFilter(aliasMetadata.alias(), aliasMetadata.filter().uncompressed(),
                        queryShardContext, xContentRegistry);
                }
                templatesAliases.put(aliasMetadata.alias(), aliasMetadata);
                resolvedAliases.add((aliasMetadata));
            }
        }
        return resolvedAliases;
    }

    /**
     * Creates the index into the cluster state applying the provided blocks. The final cluster state will contain an updated routing
     * table based on the live nodes.
     * @param clusterBlocks 本次插入indexMetadata 可能伴随着某些集群操作被阻塞
     * @param metadataTransformer 默认为null
     * 本次要将某个index加入到集群中 伴随着的还有分片的路由信息分配 以及 clusterState的变化
     */
    static ClusterState clusterStateCreateIndex(ClusterState currentState, Set<ClusterBlock> clusterBlocks, IndexMetadata indexMetadata,
                                                BiFunction<ClusterState, String, ClusterState> rerouteRoutingTable,
                                                BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer) {

        // 在metadata中插入一个新的indexMetadata
        Metadata.Builder builder = Metadata.builder(currentState.metadata())
            .put(indexMetadata, false);
        // TODO 先不考虑这个
        if (metadataTransformer != null) {
            metadataTransformer.accept(builder, indexMetadata);
        }
        Metadata newMetadata = builder.build();

        String indexName = indexMetadata.getIndex().getName();
        // 此时可能会阻止集群中的一些操作   基于之前旧的block数据 以及此时的新数据 生成新的blockBuilder对象
        ClusterBlocks.Builder blocks = createClusterBlocksBuilder(currentState, indexName, clusterBlocks);
        // 使用元数据中的 blocks 覆盖之前的数据
        blocks.updateBlocks(indexMetadata);

        // 更新 ClusterState
        ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).metadata(newMetadata).build();

        // 更新路由表信息
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
            .addAsNew(updatedState.metadata().index(indexName));
        updatedState = ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build();
        // 因为路由表发生了变化 进行reroute
        return rerouteRoutingTable.apply(updatedState, "index [" + indexName + "] created");
    }


    /**
     *
     * @param indexName 本次要处理的索引名
     * @param aliases   所有相关的别名的元数据
     * @param documentMapperSupplier   通过该函数可以获取该index对应的 indexService.MapperService.DocumentMapper 该对象内部包含了各种mapper对象 规定了数据的存储方式以及一些相关属性
     * @param indexSettings
     * @param routingNumShards      描述有多少分片
     * @param sourceMetadata        创建该indexMetadata时 可能会参考其他metadata
     * @return
     */
    static IndexMetadata buildIndexMetadata(String indexName, List<AliasMetadata> aliases,
                                            Supplier<DocumentMapper> documentMapperSupplier, Settings indexSettings, int routingNumShards,
                                            @Nullable IndexMetadata sourceMetadata) {
        IndexMetadata.Builder indexMetadataBuilder = createIndexMetadataBuilder(indexName, sourceMetadata, indexSettings, routingNumShards);
        // now, update the mappings with the actual source
        Map<String, MappingMetadata> mappingsMetadata = new HashMap<>();
        DocumentMapper mapper = documentMapperSupplier.get();
        if (mapper != null) {
            MappingMetadata mappingMd = new MappingMetadata(mapper);
            mappingsMetadata.put(mapper.type(), mappingMd);
        }

        for (MappingMetadata mappingMd : mappingsMetadata.values()) {
            // 将映射信息设置到 indexMetadata中
            indexMetadataBuilder.putMapping(mappingMd);
        }

        // apply the aliases in reverse order as the lower index ones have higher order
        for (int i = aliases.size() - 1; i >= 0; i--) {
            indexMetadataBuilder.putAlias(aliases.get(i));
        }

        indexMetadataBuilder.state(IndexMetadata.State.OPEN);
        return indexMetadataBuilder.build();
    }

    /**
     * Creates an {@link IndexMetadata.Builder} for the provided index and sets a valid primary term for all the shards if a source
     * index meta data is provided (this represents the case where we're shrinking/splitting an index and the primary term for the newly
     * created index needs to be gte than the maximum term in the source index).
     * 根据现有信息生成一个 IndexMetadata.Builder
     */
    private static IndexMetadata.Builder createIndexMetadataBuilder(String indexName, @Nullable IndexMetadata sourceMetadata,
                                                                    Settings indexSettings, int routingNumShards) {
        final IndexMetadata.Builder builder = IndexMetadata.builder(indexName);
        builder.setRoutingNumShards(routingNumShards);
        builder.settings(indexSettings);

        if (sourceMetadata != null) {
            /*
             * We need to arrange that the primary term on all the shards in the shrunken index is at least as large as
             * the maximum primary term on all the shards in the source index. This ensures that we have correct
             * document-level semantics regarding sequence numbers in the shrunken index.
             */
            final long primaryTerm =
                IntStream
                    .range(0, sourceMetadata.getNumberOfShards())
                    .mapToLong(sourceMetadata::primaryTerm)
                    .max()
                    .getAsLong();
            for (int shardId = 0; shardId < builder.numberOfShards(); shardId++) {
                builder.primaryTerm(shardId, primaryTerm);
            }
        }
        return builder;
    }

    /**
     * 根据现有信息 以及新的 blocks 生成一个新的 ClusterBlocks.Builder
     * @param currentState
     * @param index
     * @param blocks
     * @return
     */
    private static ClusterBlocks.Builder createClusterBlocksBuilder(ClusterState currentState, String index, Set<ClusterBlock> blocks) {
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());
        if (!blocks.isEmpty()) {
            for (ClusterBlock block : blocks) {
                blocksBuilder.addIndexBlock(index, block);
            }
        }
        return blocksBuilder;
    }

    /**
     *
     * @param indexService  本次待处理的索引服务
     * @param mappings  解析 创建index请求的json字符串 与index匹配的template的所有json字符串解析后生成的map对象
     * @param sourceMetadata   本次创建的index可能是参考了之前的某个元数据  可为null
     * @throws IOException
     */
    private static void updateIndexMappingsAndBuildSortOrder(IndexService indexService, Map<String, Object> mappings,
                                                             @Nullable IndexMetadata sourceMetadata) throws IOException {
        MapperService mapperService = indexService.mapperService();
        // 在基于 CREATE_INDEX 的场景下 创建的indexService就会在初始化时 创建 MapperService
        if (!mappings.isEmpty()) {
            assert mappings.size() == 1 : mappings;
            // 将解析后的mapping对象设置到了mapperService中
            mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mappings, MergeReason.MAPPING_UPDATE);
        }

        // 如果没有其他IndexMetadata (或者说被作为模板的元数据) 这里调用函数主要是校验是否会出现异常吧
        if (sourceMetadata == null) {
            // now that the mapping is merged we can validate the index sort.
            // we cannot validate for index shrinking since the mapping is empty
            // at this point. The validation will take place later in the process
            // (when all shards are copied in a single place).
            indexService.getIndexSortSupplier().get();
        }
    }

    /**
     *
     * @param waitForActiveShards  本次要求要达到多少分片数
     * @param indexMetadata
     */
    private static void validateActiveShardCount(ActiveShardCount waitForActiveShards, IndexMetadata indexMetadata) {
        // 如果没有设置 使用元数据中存储的值
        if (waitForActiveShards == ActiveShardCount.DEFAULT) {
            waitForActiveShards = indexMetadata.getWaitForActiveShards();
        }
        // 这个值必须小于 primary + replica
        if (waitForActiveShards.validate(indexMetadata.getNumberOfReplicas()) == false) {
            throw new IllegalArgumentException("invalid wait_for_active_shards[" + waitForActiveShards +
                "]: cannot be greater than number of shard copies [" +
                (indexMetadata.getNumberOfReplicas() + 1) + "]");
        }
    }

    private void validate(CreateIndexClusterStateUpdateRequest request, ClusterState state) {
        validateIndexName(request.index(), state);
        validateIndexSettings(request.index(), request.settings(), forbidPrivateIndexSettings);
    }

    /**
     * 检验某个索引配置
     * @param indexName
     * @param settings
     * @param forbidPrivateIndexSettings
     * @throws IndexCreationException
     */
    public void validateIndexSettings(String indexName, final Settings settings, final boolean forbidPrivateIndexSettings)
        throws IndexCreationException {
        List<String> validationErrors = getIndexSettingsValidationErrors(settings, forbidPrivateIndexSettings);

        if (validationErrors.isEmpty() == false) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new IndexCreationException(indexName, validationException);
        }
    }

    /**
     * Checks whether an index can be created without going over the cluster shard limit.
     *
     * @param settings     the settings of the index to be created
     * @param clusterState the current cluster state
     * @throws ValidationException if creating this index would put the cluster over the cluster shard limit
     *
     */
    public static void checkShardLimit(final Settings settings, final ClusterState clusterState) {
        // 下面计算出的分片数应该是仅针对一个index的

        // 获取配置中的分片数量 默认值为1  最大范围为1024 也就是超出该范围时 会抛出异常
        final int numberOfShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);
        // 获取副本数量
        final int numberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings);
        // 得到总的分片数 (shardId数量 * (primary + replica))
        final int shardsToCreate = numberOfShards * (1 + numberOfReplicas);

        final Optional<String> shardLimit = IndicesService.checkShardLimit(shardsToCreate, clusterState);
        // 代表此时集群已经承载不下这么多分片了
        if (shardLimit.isPresent()) {
            final ValidationException e = new ValidationException();
            e.addValidationError(shardLimit.get());
            throw e;
        }
    }

    List<String> getIndexSettingsValidationErrors(final Settings settings, final boolean forbidPrivateIndexSettings) {
        List<String> validationErrors = validateIndexCustomPath(settings, env.sharedDataFile());
        if (forbidPrivateIndexSettings) {
            validationErrors.addAll(validatePrivateSettingsNotExplicitlySet(settings, indexScopedSettings));
        }
        return validationErrors;
    }

    private static List<String> validatePrivateSettingsNotExplicitlySet(Settings settings, IndexScopedSettings indexScopedSettings) {
        List<String> validationErrors = new ArrayList<>();
        for (final String key : settings.keySet()) {
            final Setting<?> setting = indexScopedSettings.get(key);
            if (setting == null) {
                assert indexScopedSettings.isPrivateSetting(key);
            } else if (setting.isPrivateIndex()) {
                validationErrors.add("private index setting [" + key + "] can not be set explicitly");
            }
        }
        return validationErrors;
    }

    /**
     * Validates that the configured index data path (if any) is a sub-path of the configured shared data path (if any)
     *
     * @param settings the index configured settings
     * @param sharedDataPath the configured `path.shared_data` (if any)
     * @return a list containing validaton errors or an empty list if there aren't any errors
     * 检测 sharedDataPath 是否是 customPath 的子路径
     */
    private static List<String> validateIndexCustomPath(Settings settings, @Nullable Path sharedDataPath) {
        // 获取数据路径
        String customPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(settings);
        List<String> validationErrors = new ArrayList<>();
        if (!Strings.isEmpty(customPath)) {
            if (sharedDataPath == null) {
                validationErrors.add("path.shared_data must be set in order to use custom data paths");
            } else {
                Path resolvedPath = PathUtils.get(new Path[]{sharedDataPath}, customPath);
                if (resolvedPath == null) {
                    validationErrors.add("custom path [" + customPath +
                        "] is not a sub-path of path.shared_data [" + sharedDataPath + "]");
                }
            }
        }
        return validationErrors;
    }

    /**
     * Validates the settings and mappings for shrinking an index.
     *
     * @return the list of nodes at least one instance of the source index shards are allocated
     */
    static List<String> validateShrinkIndex(ClusterState state, String sourceIndex,
                                            Set<String> targetIndexMappingsTypes, String targetIndexName,
                                            Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexMappingsTypes, targetIndexName, targetIndexSettings);
        assert IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings);
        IndexMetadata.selectShrinkShards(0, sourceMetadata, IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));

        if (sourceMetadata.getNumberOfShards() == 1) {
            throw new IllegalArgumentException("can't shrink an index with only one shard");
        }

        // now check that index is all on one node
        final IndexRoutingTable table = state.routingTable().index(sourceIndex);
        Map<String, AtomicInteger> nodesToNumRouting = new HashMap<>();
        int numShards = sourceMetadata.getNumberOfShards();
        for (ShardRouting routing : table.shardsWithState(ShardRoutingState.STARTED)) {
            nodesToNumRouting.computeIfAbsent(routing.currentNodeId(), (s) -> new AtomicInteger(0)).incrementAndGet();
        }
        List<String> nodesToAllocateOn = new ArrayList<>();
        for (Map.Entry<String, AtomicInteger> entries : nodesToNumRouting.entrySet()) {
            int numAllocations = entries.getValue().get();
            assert numAllocations <= numShards : "wait what? " + numAllocations + " is > than num shards " + numShards;
            if (numAllocations == numShards) {
                nodesToAllocateOn.add(entries.getKey());
            }
        }
        if (nodesToAllocateOn.isEmpty()) {
            throw new IllegalStateException("index " + sourceIndex +
                " must have all shards allocated on the same node to shrink index");
        }
        return nodesToAllocateOn;
    }

    static void validateSplitIndex(ClusterState state, String sourceIndex,
                                   Set<String> targetIndexMappingsTypes, String targetIndexName,
                                   Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexMappingsTypes, targetIndexName, targetIndexSettings);
        IndexMetadata.selectSplitShard(0, sourceMetadata, IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
    }

    static void validateCloneIndex(ClusterState state, String sourceIndex,
                                   Set<String> targetIndexMappingsTypes, String targetIndexName,
                                   Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexMappingsTypes, targetIndexName, targetIndexSettings);
        IndexMetadata.selectCloneShard(0, sourceMetadata, IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
    }

    static IndexMetadata validateResize(ClusterState state, String sourceIndex,
                                        Set<String> targetIndexMappingsTypes, String targetIndexName,
                                        Settings targetIndexSettings) {
        if (state.metadata().hasIndex(targetIndexName)) {
            throw new ResourceAlreadyExistsException(state.metadata().index(targetIndexName).getIndex());
        }
        final IndexMetadata sourceMetadata = state.metadata().index(sourceIndex);
        if (sourceMetadata == null) {
            throw new IndexNotFoundException(sourceIndex);
        }
        // ensure index is read-only
        if (state.blocks().indexBlocked(ClusterBlockLevel.WRITE, sourceIndex) == false) {
            throw new IllegalStateException("index " + sourceIndex + " must be read-only to resize index. use \"index.blocks.write=true\"");
        }

        if (targetIndexMappingsTypes.size() > 0) {
            throw new IllegalArgumentException("mappings are not allowed when resizing indices" +
                ", all mappings are copied from the source index");
        }

        if (IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings)) {
            // this method applies all necessary checks ie. if the target shards are less than the source shards
            // of if the source shards are divisible by the number of target shards
            IndexMetadata.getRoutingFactor(sourceMetadata.getNumberOfShards(),
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
        }
        return sourceMetadata;
    }

    static void prepareResizeIndexSettings(
            final ClusterState currentState,
            final Set<String> mappingKeys,
            final Settings.Builder indexSettingsBuilder,
            final Index resizeSourceIndex,
            final String resizeIntoName,
            final ResizeType type,
            final boolean copySettings,
            final IndexScopedSettings indexScopedSettings) {

        // we use "i.r.a.initial_recovery" rather than "i.r.a.require|include" since we want the replica to allocate right away
        // once we are allocated.
        final String initialRecoveryIdFilter = IndexMetadata.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getKey() + "_id";

        final IndexMetadata sourceMetadata = currentState.metadata().index(resizeSourceIndex.getName());
        if (type == ResizeType.SHRINK) {
            final List<String> nodesToAllocateOn = validateShrinkIndex(currentState, resizeSourceIndex.getName(),
                mappingKeys, resizeIntoName, indexSettingsBuilder.build());
            indexSettingsBuilder.put(initialRecoveryIdFilter, Strings.arrayToCommaDelimitedString(nodesToAllocateOn.toArray()));
        } else if (type == ResizeType.SPLIT) {
            validateSplitIndex(currentState, resizeSourceIndex.getName(), mappingKeys, resizeIntoName, indexSettingsBuilder.build());
            indexSettingsBuilder.putNull(initialRecoveryIdFilter);
        } else if (type == ResizeType.CLONE) {
            validateCloneIndex(currentState, resizeSourceIndex.getName(), mappingKeys, resizeIntoName, indexSettingsBuilder.build());
            indexSettingsBuilder.putNull(initialRecoveryIdFilter);
        } else {
            throw new IllegalStateException("unknown resize type is " + type);
        }

        final Settings.Builder builder = Settings.builder();
        if (copySettings) {
            // copy all settings and non-copyable settings and settings that have already been set (e.g., from the request)
            for (final String key : sourceMetadata.getSettings().keySet()) {
                final Setting<?> setting = indexScopedSettings.get(key);
                if (setting == null) {
                    assert indexScopedSettings.isPrivateSetting(key) : key;
                } else if (setting.getProperties().contains(Setting.Property.NotCopyableOnResize)) {
                    continue;
                }
                // do not override settings that have already been set (for example, from the request)
                if (indexSettingsBuilder.keys().contains(key)) {
                    continue;
                }
                builder.copy(key, sourceMetadata.getSettings());
            }
        } else {
            final Predicate<String> sourceSettingsPredicate =
                    (s) -> (s.startsWith("index.similarity.") || s.startsWith("index.analysis.") ||
                            s.startsWith("index.sort.") || s.equals("index.soft_deletes.enabled"))
                            && indexSettingsBuilder.keys().contains(s) == false;
            builder.put(sourceMetadata.getSettings().filter(sourceSettingsPredicate));
        }

        indexSettingsBuilder
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), sourceMetadata.getCreationVersion())
            .put(IndexMetadata.SETTING_VERSION_UPGRADED, sourceMetadata.getUpgradedVersion())
            .put(builder.build())
            .put(IndexMetadata.SETTING_ROUTING_PARTITION_SIZE, sourceMetadata.getRoutingPartitionSize())
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), resizeSourceIndex.getName())
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.getKey(), resizeSourceIndex.getUUID());
    }

    /**
     * Returns a default number of routing shards based on the number of shards of the index. The default number of routing shards will
     * allow any index to be split at least once and at most 10 times by a factor of two. The closer the number or shards gets to 1024
     * the less default split operations are supported
     * 计算总计创建多少分片数
     */
    public static int calculateNumRoutingShards(int numShards, Version indexVersionCreated) {
        // TODO
        if (indexVersionCreated.onOrAfter(Version.V_7_0_0)) {
            // only select this automatically for indices that are created on or after 7.0 this will prevent this new behaviour
            // until we have a fully upgraded cluster. Additionally it will make integratin testing easier since mixed clusters
            // will always have the behavior of the min node in the cluster.
            //
            // We use as a default number of routing shards the higher number that can be expressed
            // as {@code numShards * 2^x`} that is less than or equal to the maximum number of shards: 1024.
            int log2MaxNumShards = 10; // logBase2(1024)
            int log2NumShards = 32 - Integer.numberOfLeadingZeros(numShards - 1); // ceil(logBase2(numShards))
            int numSplits = log2MaxNumShards - log2NumShards;
            numSplits = Math.max(1, numSplits); // Ensure the index can be split at least once
            return numShards * 1 << numSplits;
        } else {
            return numShards;
        }
    }

    /**
     * 进行有关事务日志持久化的配置项校验
     * @param indexSettings
     */
    public static void validateTranslogRetentionSettings(Settings indexSettings) {
        // 当版本号在8.0之后 不再支持 retentionAge  retentionSize配置了
        if (IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings).onOrAfter(Version.V_8_0_0) &&
            (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(indexSettings)
                || IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(indexSettings))) {
            throw new IllegalArgumentException("Translog retention settings [index.translog.retention.age] " +
                "and [index.translog.retention.size] are no longer supported. Please do not specify values for these settings");
        }
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexSettings) &&
            (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(indexSettings)
                || IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(indexSettings))) {
            deprecationLogger.deprecatedAndMaybeLog("translog_retention", "Translog retention settings [index.translog.retention.age] "
                + "and [index.translog.retention.size] are deprecated and effectively ignored. They will be removed in a future version.");
        }
    }
}
