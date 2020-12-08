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

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.IngestActionForwarder;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * Groups bulk request items by shard, optionally creating non-existent indices and
 * delegates to {@link TransportShardBulkAction} for shard-level bulk execution
 * 处理大块数据请求
 */
public class TransportBulkAction extends HandledTransportAction<BulkRequest, BulkResponse> {

    private static final Logger logger = LogManager.getLogger(TransportBulkAction.class);

    private final ThreadPool threadPool;
    private final AutoCreateIndex autoCreateIndex;
    private final ClusterService clusterService;
    private final IngestService ingestService;
    private final LongSupplier relativeTimeProvider;
    private final IngestActionForwarder ingestForwarder;
    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private static final String DROPPED_ITEM_WITH_AUTO_GENERATED_ID = "auto-generated";

    /**
     * 可以看到除了ES内部一些强依赖的类 或者说内聚性很强的类没有使用IOC容器外 其他类大都采用依赖注入的方式来减少耦合性
     * 也便于以后对这些ACTION的修改
     * @param threadPool
     * @param transportService
     * @param clusterService
     * @param ingestService
     * @param client
     * @param actionFilters
     * @param indexNameExpressionResolver
     * @param autoCreateIndex
     */
    @Inject
    public TransportBulkAction(ThreadPool threadPool, TransportService transportService,
                               ClusterService clusterService, IngestService ingestService,
                               NodeClient client, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               AutoCreateIndex autoCreateIndex) {
        this(threadPool, transportService, clusterService, ingestService, client, actionFilters,
            indexNameExpressionResolver, autoCreateIndex, System::nanoTime);
    }

    public TransportBulkAction(ThreadPool threadPool, TransportService transportService,
                               ClusterService clusterService, IngestService ingestService,
                               NodeClient client, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               AutoCreateIndex autoCreateIndex, LongSupplier relativeTimeProvider) {
        super(BulkAction.NAME, transportService, actionFilters, BulkRequest::new, ThreadPool.Names.WRITE);
        Objects.requireNonNull(relativeTimeProvider);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.ingestService = ingestService;
        this.autoCreateIndex = autoCreateIndex;
        this.relativeTimeProvider = relativeTimeProvider;
        this.ingestForwarder = new IngestActionForwarder(transportService);
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        clusterService.addStateApplier(this.ingestForwarder);
    }

    /**
     * Retrieves the {@link IndexRequest} from the provided {@link DocWriteRequest} for index or upsert actions.  Upserts are
     * modeled as {@link IndexRequest} inside the {@link UpdateRequest}. Ignores {@link org.elasticsearch.action.delete.DeleteRequest}'s
     *
     * @param docWriteRequest The request to find the {@link IndexRequest}
     * @return the found {@link IndexRequest} or {@code null} if one can not be found.
     * 将某个操作doc的请求 转换成一个索引请求
     */
    public static IndexRequest getIndexWriteRequest(DocWriteRequest docWriteRequest) {
        IndexRequest indexRequest = null;
        // 代表本次的操作类型是 index类型 也可以理解成创建类型
        if (docWriteRequest instanceof IndexRequest) {
            indexRequest = (IndexRequest) docWriteRequest;
            // 本次是一次更新操作
        } else if (docWriteRequest instanceof UpdateRequest) {
            UpdateRequest updateRequest = (UpdateRequest) docWriteRequest;
            // TODO 什么意思???
            indexRequest = updateRequest.docAsUpsert() ? updateRequest.doc() : updateRequest.upsertRequest();
        }
        return indexRequest;
    }

    /**
     * 接收到请求后 转发到该方法处理
     * @param task
     * @param bulkRequest
     * @param listener
     */
    @Override
    protected void doExecute(Task task, BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
        // 获取当前时间
        final long startTime = relativeTime();
        final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());

        // 是否通过管道进行处理
        boolean hasIndexRequestsWithPipelines = false;
        final Metadata metadata = clusterService.state().getMetadata();
        final Version minNodeVersion = clusterService.state().getNodes().getMinNodeVersion();
        // 某次bulk操作 可以针对多个doc 实际上 DocWriterRequest 就对应底层Engine的 UPDATE/DELETE/INDEX 三种操作类型
        for (DocWriteRequest<?> actionRequest : bulkRequest.requests) {
            // 从请求体中 抽取出 indexRequest
            IndexRequest indexRequest = getIndexWriteRequest(actionRequest);
            if (indexRequest != null) {
                // Each index request needs to be evaluated, because this method also modifies the IndexRequest
                // 本次是否使用V2版本的模板 记得在index创建时
                boolean preferV2Templates = bulkRequest.preferV2Templates() == null ?
                    IndexMetadata.PREFER_V2_TEMPLATES_SETTING.getDefault(Settings.EMPTY) : bulkRequest.preferV2Templates();

                boolean indexRequestHasPipeline = resolvePipelines(actionRequest, indexRequest, preferV2Templates, metadata);
                hasIndexRequestsWithPipelines |= indexRequestHasPipeline;
            }

            // 如果本次请求就是属于 index请求  (还可能是 update请求 和 delete请求)
            if (actionRequest instanceof IndexRequest) {
                IndexRequest ir = (IndexRequest) actionRequest;
                // 检测版本兼容 先忽略
                ir.checkAutoIdWithOpTypeCreateSupportedByVersion(minNodeVersion);
                // 在这种情况下不应该指定 自动创建的时间戳
                if (ir.getAutoGeneratedTimestamp() != IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP) {
                    throw new IllegalArgumentException("autoGeneratedTimestamp should not be set externally");
                }
            }
        }

        // 代表基于pipeline进行处理
        if (hasIndexRequestsWithPipelines) {
            // this method (doExecute) will be called again, but with the bulk requests updated from the ingest node processing but
            // also with IngestService.NOOP_PIPELINE_NAME on each request. This ensures that this on the second time through this method,
            // this path is never taken.
            try {
                if (Assertions.ENABLED) {
                    final boolean arePipelinesResolved = bulkRequest.requests()
                        .stream()
                        .map(TransportBulkAction::getIndexWriteRequest)
                        .filter(Objects::nonNull)
                        .allMatch(IndexRequest::isPipelineResolved);
                    assert arePipelinesResolved : bulkRequest;
                }
                if (clusterService.localNode().isIngestNode()) {
                    // 如果当前节点是一个摄取节点  就走不同的逻辑 并且在处理完后直接触发监听器  实际上listener就是重走doExecute 但是能保证下次的pipeline为空 下次就不会进入 if (hasIndexRequestsWithPipelines)这个分支
                    processBulkIndexIngestRequest(task, bulkRequest, listener);
                } else {
                    // 如果当前节点不是摄取节点   在接收到请求后 会转发到集群中的某个摄取节点
                    ingestForwarder.forwardIngestRequest(BulkAction.INSTANCE, bulkRequest, listener);
                }
                // 如果此时集群中不包含摄取节点 会抛出异常
            } catch (Exception e) {
                listener.onFailure(e);
            }
            return;
        }

        // 在经过pipeline处理过一轮后 进入下面的处理逻辑
        if (needToCheck()) {
            // Attempt to create all the indices that we're going to need during the bulk before we start.
            // Step 1: collect all the indices in the request
            // 在开始执行bulk操作前 先找到所有index  如果之后发现这些索引还未创建  那么会先创建索引
            final Set<String> indices = bulkRequest.requests.stream()
                    // delete requests should not attempt to create the index (if the index does not
                    // exists), unless an external versioning is used
                // 找到非 DELETE类型的操作  或者虽然是DELETE类型 但是版本类型属于 EXTERNAL 还是允许生成indices
                .filter(request ->
                    request.opType() != DocWriteRequest.OpType.DELETE || request.versionType() == VersionType.EXTERNAL || request.versionType() == VersionType.EXTERNAL_GTE)
                .map(DocWriteRequest::index)
                .collect(Collectors.toSet());
            /* Step 2: filter that to indices that don't exist and we can create. At the same time build a map of indices we can't create
             * that we'll use when we try to run the requests.
             * 无法自动创建的索引会插入到这个map中
             * */
            final Map<String, IndexNotFoundException> indicesThatCannotBeCreated = new HashMap<>();

            // 支持自动创建的索引会加入到这个set中
            Set<String> autoCreateIndices = new HashSet<>();
            ClusterState state = clusterService.state();
            for (String index : indices) {
                boolean shouldAutoCreate;
                try {
                    // 如果不支持自动创建该索引 会抛出 indexNotFound异常
                    shouldAutoCreate = shouldAutoCreate(index, state);
                } catch (IndexNotFoundException e) {
                    shouldAutoCreate = false;
                    indicesThatCannotBeCreated.put(index, e);
                }
                if (shouldAutoCreate) {
                    autoCreateIndices.add(index);
                }
            }
            // Step 3: create all the indices that are missing, if there are any missing. start the bulk after all the creates come back.
            // 这个时候认为已经尽可能的创建索引了
            if (autoCreateIndices.isEmpty()) {
                executeBulk(task, bulkRequest, startTime, listener, responses, indicesThatCannotBeCreated);
            } else {
                final AtomicInteger counter = new AtomicInteger(autoCreateIndices.size());
                for (String index : autoCreateIndices) {
                    createIndex(index, bulkRequest.preferV2Templates(), bulkRequest.timeout(), new ActionListener<>() {
                        @Override
                        public void onResponse(CreateIndexResponse result) {
                            if (counter.decrementAndGet() == 0) {
                                threadPool.executor(ThreadPool.Names.WRITE).execute(
                                    () -> executeBulk(task, bulkRequest, startTime, listener, responses, indicesThatCannotBeCreated));
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (!(ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException)) {
                                // fail all requests involving this index, if create didn't work
                                for (int i = 0; i < bulkRequest.requests.size(); i++) {
                                    DocWriteRequest<?> request = bulkRequest.requests.get(i);
                                    if (request != null && setResponseFailureIfIndexMatches(responses, i, request, index, e)) {
                                        bulkRequest.requests.set(i, null);
                                    }
                                }
                            }
                            if (counter.decrementAndGet() == 0) {
                                executeBulk(task, bulkRequest, startTime, ActionListener.wrap(listener::onResponse, inner -> {
                                    inner.addSuppressed(e);
                                    listener.onFailure(inner);
                                }), responses, indicesThatCannotBeCreated);
                            }
                        }
                    });
                }
            }
        } else {
            executeBulk(task, bulkRequest, startTime, listener, responses, emptyMap());
        }
    }

    /**
     * 对管道信息进行解析
     * @param originalRequest 原始的请求体对象 针对某个doc
     * @param indexRequest    从originalRequest中抽取出来的 有关index的请求
     * @param preferV2Templates  是否倾向于使用V2的模板
     * @param metadata   元数据信息
     * @return
     */
    static boolean resolvePipelines(final DocWriteRequest<?> originalRequest, final IndexRequest indexRequest,
                                    final boolean preferV2Templates, final Metadata metadata) {
        // 是否已经完成了管道的解析  只有未完成解析时 才有下面处理的必要
        if (indexRequest.isPipelineResolved() == false) {
            final String requestPipeline = indexRequest.getPipeline();

            // 先设置2个默认值
            indexRequest.setPipeline(IngestService.NOOP_PIPELINE_NAME);
            indexRequest.setFinalPipeline(IngestService.NOOP_PIPELINE_NAME);
            String defaultPipeline = null;
            String finalPipeline = null;
            // start to look for default or final pipelines via settings found in the index meta data
            // 找到请求对应的索引元数据信息
            IndexMetadata indexMetadata = metadata.indices().get(originalRequest.index());
            // check the alias for the index request (this is how normal index requests are modeled)
            // TODO 这种情况为什么会出现呢  通过lookup做了一步中转
            if (indexMetadata == null && indexRequest.index() != null) {
                IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(indexRequest.index());
                if (indexAbstraction != null) {
                    // 可以通过这种方式 定位到真正要写入的索引元数据
                    indexMetadata = indexAbstraction.getWriteIndex();
                }
            }
            // check the alias for the action request (this is how upserts are modeled)
            // TODO ???
            if (indexMetadata == null && originalRequest.index() != null) {
                IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(originalRequest.index());
                if (indexAbstraction != null) {
                    indexMetadata = indexAbstraction.getWriteIndex();
                }
            }
            if (indexMetadata != null) {
                final Settings indexSettings = indexMetadata.getSettings();
                // 如果找到了默认管道 以及 final管道 则覆盖内部的属性
                if (IndexSettings.DEFAULT_PIPELINE.exists(indexSettings)) {
                    // find the default pipeline if one is defined from an existing index setting
                    defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(indexSettings);
                    indexRequest.setPipeline(defaultPipeline);
                }
                if (IndexSettings.FINAL_PIPELINE.exists(indexSettings)) {
                    // find the final pipeline if one is defined from an existing index setting
                    finalPipeline = IndexSettings.FINAL_PIPELINE.get(indexSettings);
                    indexRequest.setFinalPipeline(finalPipeline);
                }
                // TODO 为什么啊 这种情况下 还能没有indexMetadata吗  索引创建的时候应该就存在元数据了啊
            } else if (indexRequest.index() != null) {
                // the index does not exist yet (and this is a valid request), so match index
                // templates to look for pipelines in either a matching V2 template (which takes
                // precedence), or if a V2 template does not match, any V1 templates
                // 在元数据中会存在一组V2Template 有自己的匹配规则  在命中的一组模板中返回优先级最高的模板
                String v2Template = MetadataIndexTemplateService.findV2Template(metadata, indexRequest.index(), false);
                // 还需要 倾向于使用V2版本的模板 才会使用   默认为true 就不考虑V1版本的模板了
                if (v2Template != null && preferV2Templates) {
                    // 这里会将template信息 以及它相关的 componentTemplate 插入到settings中
                    Settings settings = MetadataIndexTemplateService.resolveSettings(metadata, v2Template);
                    if (defaultPipeline == null && IndexSettings.DEFAULT_PIPELINE.exists(settings)) {
                        defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(settings);
                        // we can not break in case a lower-order template has a final pipeline that we need to collect
                    }
                    if (finalPipeline == null && IndexSettings.FINAL_PIPELINE.exists(settings)) {
                        finalPipeline = IndexSettings.FINAL_PIPELINE.get(settings);
                        // we can not break in case a lower-order template has a default pipeline that we need to collect
                    }
                    // 更新pipeline信息
                    indexRequest.setPipeline(Objects.requireNonNullElse(defaultPipeline, IngestService.NOOP_PIPELINE_NAME));
                    indexRequest.setFinalPipeline(Objects.requireNonNullElse(finalPipeline, IngestService.NOOP_PIPELINE_NAME));
                } else {
                    // TODO V1 template 先忽略
                    List<IndexTemplateMetadata> templates =
                            MetadataIndexTemplateService.findV1Templates(metadata, indexRequest.index(), null);
                    assert (templates != null);
                    // order of templates are highest order first
                    for (final IndexTemplateMetadata template : templates) {
                        final Settings settings = template.settings();
                        if (defaultPipeline == null && IndexSettings.DEFAULT_PIPELINE.exists(settings)) {
                            defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(settings);
                            // we can not break in case a lower-order template has a final pipeline that we need to collect
                        }
                        if (finalPipeline == null && IndexSettings.FINAL_PIPELINE.exists(settings)) {
                            finalPipeline = IndexSettings.FINAL_PIPELINE.get(settings);
                            // we can not break in case a lower-order template has a default pipeline that we need to collect
                        }
                        if (defaultPipeline != null && finalPipeline != null) {
                            // we can break if we have already collected a default and final pipeline
                            break;
                        }
                    }
                    indexRequest.setPipeline(Objects.requireNonNullElse(defaultPipeline, IngestService.NOOP_PIPELINE_NAME));
                    indexRequest.setFinalPipeline(Objects.requireNonNullElse(finalPipeline, IngestService.NOOP_PIPELINE_NAME));
                }
            }

            if (requestPipeline != null) {
                indexRequest.setPipeline(requestPipeline);
            }

            /*
             * We have to track whether or not the pipeline for this request has already been resolved. It can happen that the
             * pipeline for this request has already been derived yet we execute this loop again. That occurs if the bulk request
             * has been forwarded by a non-ingest coordinating node to an ingest node. In this case, the coordinating node will have
             * already resolved the pipeline for this request. It is important that we are able to distinguish this situation as we
             * can not double-resolve the pipeline because we will not be able to distinguish the case of the pipeline having been
             * set from a request pipeline parameter versus having been set by the resolution. We need to be able to distinguish
             * these cases as we need to reject the request if the pipeline was set by a required pipeline and there is a request
             * pipeline parameter too.
             */
            indexRequest.isPipelineResolved(true);
        }


        // return whether this index request has a pipeline
        // 只要有某个pipeline信息不是 NOOP 就代表解析成功
        return IngestService.NOOP_PIPELINE_NAME.equals(indexRequest.getPipeline()) == false
            || IngestService.NOOP_PIPELINE_NAME.equals(indexRequest.getFinalPipeline()) == false;
    }

    boolean needToCheck() {
        return autoCreateIndex.needToCheck();
    }

    /**
     * 检测某个index是否支持自动创建
     * @param index
     * @param state
     * @return
     */
    boolean shouldAutoCreate(String index, ClusterState state) {
        return autoCreateIndex.shouldAutoCreate(index, state);
    }

    void createIndex(String index, Boolean preferV2Templates, TimeValue timeout, ActionListener<CreateIndexResponse> listener) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest();
        createIndexRequest.index(index);
        createIndexRequest.cause("auto(bulk api)");
        createIndexRequest.masterNodeTimeout(timeout);
        createIndexRequest.preferV2Templates(preferV2Templates);
        client.admin().indices().create(createIndexRequest, listener);
    }

    private boolean setResponseFailureIfIndexMatches(AtomicArray<BulkItemResponse> responses, int idx, DocWriteRequest<?> request,
                                                     String index, Exception e) {
        if (index.equals(request.index())) {
            responses.set(idx, new BulkItemResponse(idx, request.opType(), new BulkItemResponse.Failure(request.index(),
                request.id(), e)));
            return true;
        }
        return false;
    }

    private long buildTookInMillis(long startTimeNanos) {
        return TimeUnit.NANOSECONDS.toMillis(relativeTime() - startTimeNanos);
    }

    /**
     * retries on retryable cluster blocks, resolves item requests,
     * constructs shard bulk requests and delegates execution to shard bulk action
     * 描述一个 bulk操作
     * */
    private final class BulkOperation extends ActionRunnable<BulkResponse> {
        private final Task task;
        private final BulkRequest bulkRequest;
        private final AtomicArray<BulkItemResponse> responses;
        private final long startTimeNanos;
        /**
         * 当CS不满足某种状态时 就需要观测CS的变化 并在满足条件时进行处理
         */
        private final ClusterStateObserver observer;

        /**
         * 本次处理的所有req中 某些对应的index 此时无法在集群中找到 并且也不支持自动创建
         */
        private final Map<String, IndexNotFoundException> indicesThatCannotBeCreated;

        BulkOperation(Task task, BulkRequest bulkRequest, ActionListener<BulkResponse> listener, AtomicArray<BulkItemResponse> responses,
                long startTimeNanos, Map<String, IndexNotFoundException> indicesThatCannotBeCreated) {
            super(listener);
            this.task = task;
            this.bulkRequest = bulkRequest;
            this.responses = responses;
            this.startTimeNanos = startTimeNanos;
            this.indicesThatCannotBeCreated = indicesThatCannotBeCreated;
            this.observer = new ClusterStateObserver(clusterService, bulkRequest.timeout(), logger, threadPool.getThreadContext());
        }

        /**
         * 当执行 run() 方法时会转发到该方法
         * 本次处理的就是一个完整的 bulkReq    bulk大块数据请求就是体现在一次对多个index执行 某种操作吧
         */
        @Override
        protected void doRun() {
            final ClusterState clusterState = observer.setAndGetObservedState();
            // 如果本次处理出现异常,中止处理
            if (handleBlockExceptions(clusterState)) {
                return;
            }
            final ConcreteIndices concreteIndices = new ConcreteIndices(clusterState, indexNameExpressionResolver);
            Metadata metadata = clusterState.metadata();

            // 在这个for loop 中是在为req填充相关属性
            for (int i = 0; i < bulkRequest.requests.size(); i++) {
                DocWriteRequest<?> docWriteRequest = bulkRequest.requests.get(i);
                //the request can only be null because we set it to null in the previous step, so it gets ignored
                if (docWriteRequest == null) {
                    continue;
                }
                // 检测对应的索引是否可用 不可用则添加到失败列表中  主要就是检测index是否存在 是否被关闭 是否是writeIndex
                if (addFailureIfIndexIsUnavailable(docWriteRequest, i, concreteIndices, metadata)) {
                    continue;
                }
                Index concreteIndex = concreteIndices.resolveIfAbsent(docWriteRequest);
                try {
                    // 根据不同的操作类型 生成不同的请求对象
                    switch (docWriteRequest.opType()) {
                        case CREATE:
                        case INDEX:
                            IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                            final IndexMetadata indexMetadata = metadata.index(concreteIndex);
                            // 每个索引会有一个mapping对象 描述了这个index的数据应该是什么结构
                            MappingMetadata mappingMd = indexMetadata.mapping();
                            Version indexCreated = indexMetadata.getCreationVersion();
                            // 从元数据中 获取该index的路由信息
                            indexRequest.resolveRouting(metadata);
                            // 最后设置一些属性  为这个req设置了一个id
                            indexRequest.process(indexCreated, mappingMd, concreteIndex.getName());
                            break;
                        case UPDATE:
                            // 解析路由信息设置到req中
                            TransportUpdateAction.resolveAndValidateRouting(metadata, concreteIndex.getName(),
                                (UpdateRequest) docWriteRequest);
                            break;
                        case DELETE:
                            docWriteRequest.routing(metadata.resolveWriteIndexRouting(docWriteRequest.routing(), docWriteRequest.index()));
                            // check if routing is required, if so, throw error if routing wasn't specified
                            if (docWriteRequest.routing() == null && metadata.routingRequired(concreteIndex.getName())) {
                                throw new RoutingMissingException(concreteIndex.getName(), docWriteRequest.id());
                            }
                            break;
                        default: throw new AssertionError("request type not supported: [" + docWriteRequest.opType() + "]");
                    }
                } catch (ElasticsearchParseException | IllegalArgumentException | RoutingMissingException e) {
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(concreteIndex.getName(),
                        docWriteRequest.id(), e);
                    BulkItemResponse bulkItemResponse = new BulkItemResponse(i, docWriteRequest.opType(), failure);
                    responses.set(i, bulkItemResponse);
                    // make sure the request gets never processed again
                    bulkRequest.requests.set(i, null);
                }
            }

            // first, go over all the requests and create a ShardId -> Operations mapping
            // 这里就是遍历一个完整的 bulkReq下的小请求
            Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
            for (int i = 0; i < bulkRequest.requests.size(); i++) {
                DocWriteRequest<?> request = bulkRequest.requests.get(i);
                // 代表在之前不满足处理条件 已经被移除了
                if (request == null) {
                    continue;
                }

                String concreteIndex = concreteIndices.getConcreteIndex(request.index()).getName();
                // id 作为一个随机因子会间接决定本次处理的 shardId
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, concreteIndex, request.id(),
                    request.routing()).shardId();
                // 每次针对该 shardId的请求被包装成一个 BulkItemReq  一次完整的bulkReq中 可能会包含对同一个index同一shardId的多次操作 所有有必要使用列表
                List<BulkItemRequest> shardRequests = requestsByShard.computeIfAbsent(shardId, shard -> new ArrayList<>());
                shardRequests.add(new BulkItemRequest(i, request));
            }

            // 代表本次没有需要处理的请求了 比如所有索引都不存在
            if (requestsByShard.isEmpty()) {
                listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]),
                    buildTookInMillis(startTimeNanos)));
                return;
            }

            final AtomicInteger counter = new AtomicInteger(requestsByShard.size());
            String nodeId = clusterService.localNode().getId();
            for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
                final ShardId shardId = entry.getKey();
                final List<BulkItemRequest> requests = entry.getValue();
                BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, bulkRequest.getRefreshPolicy(),
                        requests.toArray(new BulkItemRequest[requests.size()]));
                bulkShardRequest.waitForActiveShards(bulkRequest.waitForActiveShards());
                bulkShardRequest.timeout(bulkRequest.timeout());
                bulkShardRequest.routedBasedOnClusterVersion(clusterState.version());
                if (task != null) {
                    bulkShardRequest.setParentTask(nodeId, task.getId());
                }
                client.executeLocally(TransportShardBulkAction.TYPE, bulkShardRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(BulkShardResponse bulkShardResponse) {
                        for (BulkItemResponse bulkItemResponse : bulkShardResponse.getResponses()) {
                            // we may have no response if item failed
                            if (bulkItemResponse.getResponse() != null) {
                                bulkItemResponse.getResponse().setShardInfo(bulkShardResponse.getShardInfo());
                            }
                            responses.set(bulkItemResponse.getItemId(), bulkItemResponse);
                        }
                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // create failures for all relevant requests
                        for (BulkItemRequest request : requests) {
                            final String indexName = concreteIndices.getConcreteIndex(request.index()).getName();
                            DocWriteRequest<?> docWriteRequest = request.request();
                            responses.set(request.id(), new BulkItemResponse(request.id(), docWriteRequest.opType(),
                                    new BulkItemResponse.Failure(indexName, docWriteRequest.id(), e)));
                        }
                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }

                    private void finishHim() {
                        listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]),
                            buildTookInMillis(startTimeNanos)));
                    }
                });
            }
        }

        /**
         * 检测当前集群中是否对 wrier操作进行了阻塞
         * @param state
         * @return
         */
        private boolean handleBlockExceptions(ClusterState state) {
            ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
            if (blockException != null) {
                if (blockException.retryable()) {
                    logger.trace("cluster is blocked, scheduling a retry", blockException);
                    // 如果允许重试 那么等待CS发生变化
                    retry(blockException);
                } else {
                    onFailure(blockException);
                }
                return true;
            }
            return false;
        }

        /**
         * 代表本次操作被阻塞  但是允许重试
         * @param failure
         */
        void retry(Exception failure) {
            assert failure != null;
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                onFailure(failure);
                return;
            }
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    run();
                }

                @Override
                public void onClusterServiceClose() {
                    onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    // Try one more time...
                    run();
                }
            });
        }

        /**
         * 针对无法处理的索引 加入到失败列表中
         * @param request
         * @param idx   当前处理到第几个请求
         * @param concreteIndices  该容器用于存储索引
         * @param metadata
         * @return
         */
        private boolean addFailureIfIndexIsUnavailable(DocWriteRequest<?> request, int idx, final ConcreteIndices concreteIndices,
                final Metadata metadata) {
            // 代表该index不存在
            IndexNotFoundException cannotCreate = indicesThatCannotBeCreated.get(request.index());
            if (cannotCreate != null) {
                // 添加到处理失败的容器中
                addFailure(request, idx, cannotCreate);
                return true;
            }
            // 每个被处理过的索引会加入到concreteIndices中
            Index concreteIndex = concreteIndices.getConcreteIndex(request.index());
            if (concreteIndex == null) {
                try {
                    // 将req对应的index存储到 concreteIndices 中
                    concreteIndex = concreteIndices.resolveIfAbsent(request);
                    // 代表无法找到对应的 writeIndex 比如req中携带的索引信息可能是一个别名
                } catch (IndexClosedException | IndexNotFoundException ex) {
                    addFailure(request, idx, ex);
                    return true;
                }
            }
            IndexMetadata indexMetadata = metadata.getIndexSafe(concreteIndex);
            // 如果此时索引已经被关闭了 也加入到失败的列表中
            if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                addFailure(request, idx, new IndexClosedException(concreteIndex));
                return true;
            }
            return false;
        }

        /**
         * 代表某个req处理失败
         * @param request
         * @param idx
         * @param unavailableException
         */
        private void addFailure(DocWriteRequest<?> request, int idx, Exception unavailableException) {
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(request.index(), request.id(),
                    unavailableException);
            BulkItemResponse bulkItemResponse = new BulkItemResponse(idx, request.opType(), failure);
            responses.set(idx, bulkItemResponse);
            // make sure the request gets never processed again
            // 避免该请求被重复处理
            bulkRequest.requests.set(idx, null);
        }
    }

    /**
     * 执行bulk操作
     * @param task
     * @param bulkRequest
     * @param startTimeNanos
     * @param listener
     * @param responses
     * @param indicesThatCannotBeCreated  本次创建失败的索引都存储在这个列表中
     */
    void executeBulk(Task task, final BulkRequest bulkRequest, final long startTimeNanos, final ActionListener<BulkResponse> listener,
            final AtomicArray<BulkItemResponse> responses, Map<String, IndexNotFoundException> indicesThatCannotBeCreated) {
        new BulkOperation(task, bulkRequest, listener, responses, startTimeNanos, indicesThatCannotBeCreated).run();
    }

    /**
     * docWriterReq 被解析后会变成index 并存储到该对象中
     */
    private static class ConcreteIndices  {
        private final ClusterState state;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final Map<String, Index> indices = new HashMap<>();

        ConcreteIndices(ClusterState state, IndexNameExpressionResolver indexNameExpressionResolver) {
            this.state = state;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
        }

        Index getConcreteIndex(String indexOrAlias) {
            return indices.get(indexOrAlias);
        }

        /**
         * 每次要处理的index 将会存储到该对象内部
         * @param request
         * @return
         */
        Index resolveIfAbsent(DocWriteRequest<?> request) {
            Index concreteIndex = indices.get(request.index());
            if (concreteIndex == null) {
                // 找到 writeIndex  为什么会有写索引与普通索引呢  如果没有找到writeIndex 也会抛出异常
                concreteIndex = indexNameExpressionResolver.concreteWriteIndex(state, request);
                indices.put(request.index(), concreteIndex);
            }
            return concreteIndex;
        }
    }

    private long relativeTime() {
        return relativeTimeProvider.getAsLong();
    }

    /**
     * 当本节点作为摄取节点时 走不同的逻辑进行处理
     * @param task
     * @param original
     * @param listener
     */
    private void processBulkIndexIngestRequest(Task task, BulkRequest original, ActionListener<BulkResponse> listener) {
        final long ingestStartTimeInNanos = System.nanoTime();
        final BulkRequestModifier bulkRequestModifier = new BulkRequestModifier(original);
        // 开始执行摄取工作   内部会使用一套 pipeline进行处理   同时将req内部的pipeline置空
        ingestService.executeBulkRequest(
            original.numberOfActions(),
            () -> bulkRequestModifier,
            // 当遇到异常时 通过该函数进行处理
            bulkRequestModifier::markItemAsFailed,
            // 当处理完毕时 触发该方法  param1 对应线程  param2 对应异常信息
            (originalThread, exception) -> {
                if (exception != null) {
                    logger.debug("failed to execute pipeline for a bulk request", exception);
                    listener.onFailure(exception);
                } else {
                    // 当正常处理时 触发该函数
                    long ingestTookInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ingestStartTimeInNanos);

                    // 为下一轮doExecute 更新了请求对象
                    BulkRequest bulkRequest = bulkRequestModifier.getBulkRequest();
                    // 对监听器做了一层包装
                    ActionListener<BulkResponse> actionListener = bulkRequestModifier.wrapActionListenerIfNeeded(ingestTookInMillis,
                        listener);

                    // 因为本次请求实际上没有对任何doc 进行处理 所以以一个空结果触发监听器
                    if (bulkRequest.requests().isEmpty()) {
                        // at this stage, the transport bulk action can't deal with a bulk request with no requests,
                        // so we stop and send an empty response back to the client.
                        // (this will happen if pre-processing all items in the bulk failed)
                        actionListener.onResponse(new BulkResponse(new BulkItemResponse[0], 0));
                    } else {
                        // If a processor went async and returned a response on a different thread then
                        // before we continue the bulk request we should fork back on a write thread:
                        // 代表在执行前后线程没有切换  也就是说在processor的处理中 没有切换线程
                        if (originalThread == Thread.currentThread()) {
                            assert Thread.currentThread().getName().contains(ThreadPool.Names.WRITE);

                            // 这里又调用了 doExecute 重新回到主流程
                            doExecute(task, bulkRequest, actionListener);
                        } else {
                            threadPool.executor(ThreadPool.Names.WRITE).execute(new AbstractRunnable() {
                                @Override
                                public void onFailure(Exception e) {
                                    listener.onFailure(e);
                                }

                                @Override
                                protected void doRun() throws Exception {
                                    doExecute(task, bulkRequest, actionListener);
                                }

                                @Override
                                public boolean isForceExecution() {
                                    // If we fork back to a write thread we **not** should fail, because tp queue is full.
                                    // (Otherwise the work done during ingest will be lost)
                                    // It is okay to force execution here. Throttling of write requests happens prior to
                                    // ingest when a node receives a bulk request.
                                    return true;
                                }
                            });
                        }
                    }
                }
            },
            // 对应 onDrop函数
            bulkRequestModifier::markItemAsDropped
        );
    }

    /**
     * 该对象便于修改 bulk请求对象
     * 同时该对象可以遍历内部的 DocWriterRequest对象
     */
    static final class BulkRequestModifier implements Iterator<DocWriteRequest<?>> {

        private static final Logger logger = LogManager.getLogger(BulkRequestModifier.class);

        /**
         * 原始请求对象
         */
        final BulkRequest bulkRequest;
        /**
         * 稀疏位图  失败的req会被记录在位图内
         */
        final SparseFixedBitSet failedSlots;
        /**
         * 存储每个req的处理结果
         */
        final List<BulkItemResponse> itemResponses;
        final AtomicIntegerArray originalSlots;

        // 当前 reqs的下标
        volatile int currentSlot = -1;

        BulkRequestModifier(BulkRequest bulkRequest) {
            this.bulkRequest = bulkRequest;
            // 请求数量会很多么 因为稀疏位图本身的应用场景 就是不连续且min/max相差比较大
            this.failedSlots = new SparseFixedBitSet(bulkRequest.requests().size());
            this.itemResponses = new ArrayList<>(bulkRequest.requests().size());
            this.originalSlots = new AtomicIntegerArray(bulkRequest.requests().size()); // oversize, but that's ok
        }

        @Override
        public DocWriteRequest<?> next() {
            return bulkRequest.requests().get(++currentSlot);
        }

        @Override
        public boolean hasNext() {
            return (currentSlot + 1) < bulkRequest.requests().size();
        }

        /**
         * 原bulkReq对象在经过处理后 生成一个新的bulkReq
         * @return
         */
        BulkRequest getBulkRequest() {
            // 代表没有提前出现错误 返回原始请求对象
            if (itemResponses.isEmpty()) {
                return bulkRequest;
            } else {
                BulkRequest modifiedBulkRequest = new BulkRequest();
                modifiedBulkRequest.setRefreshPolicy(bulkRequest.getRefreshPolicy());
                modifiedBulkRequest.waitForActiveShards(bulkRequest.waitForActiveShards());
                modifiedBulkRequest.timeout(bulkRequest.timeout());


                // 可以看到这里忽略了之前失败 或者被丢弃的req
                int slot = 0;
                List<DocWriteRequest<?>> requests = bulkRequest.requests();
                for (int i = 0; i < requests.size(); i++) {
                    // 这里将req原样返回
                    DocWriteRequest<?> request = requests.get(i);
                    if (failedSlots.get(i) == false) {
                        modifiedBulkRequest.add(request);
                        originalSlots.set(slot++, i);
                    }
                }
                return modifiedBulkRequest;
            }
        }

        /**
         * 对监听器做一层包装
         * @param ingestTookInMillis
         * @param actionListener
         * @return
         */
        ActionListener<BulkResponse> wrapActionListenerIfNeeded(long ingestTookInMillis, ActionListener<BulkResponse> actionListener) {
            if (itemResponses.isEmpty()) {
                return ActionListener.map(actionListener,
                    // 在处理结果时 先做一层映射
                    response -> new BulkResponse(response.getItems(), response.getTook().getMillis(), ingestTookInMillis));
            } else {
                return ActionListener.delegateFailure(actionListener,
                    // 在正常收到 res后 对处理逻辑进行了增强
                    (delegatedListener, response) -> {
                    // 将结果插入到合适的位置
                    BulkItemResponse[] items = response.getItems();
                    for (int i = 0; i < items.length; i++) {
                        itemResponses.add(originalSlots.get(i), response.getItems()[i]);
                    }
                    delegatedListener.onResponse(
                        new BulkResponse(
                            itemResponses.toArray(new BulkItemResponse[0]), response.getTook().getMillis(), ingestTookInMillis));
                });
            }
        }

        /**
         * slot对应的req在使用pipeline处理时 数据被丢弃了
         * @param slot
         */
        synchronized void markItemAsDropped(int slot) {
            IndexRequest indexRequest = getIndexWriteRequest(bulkRequest.requests().get(slot));
            failedSlots.set(slot);
            // 在这种情况下使用自动生成的id
            final String id = indexRequest.id() == null ? DROPPED_ITEM_WITH_AUTO_GENERATED_ID : indexRequest.id();
            itemResponses.add(
                new BulkItemResponse(slot, indexRequest.opType(),
                    // TODO 这的意思是 被处理后 doc被丢弃了 所以是一个更新请求 并且是丢弃源数据的请求 ???
                    new UpdateResponse(
                        new ShardId(indexRequest.index(), IndexMetadata.INDEX_UUID_NA_VALUE, 0),
                        id, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                        indexRequest.version(), DocWriteResponse.Result.NOOP
                    )
                )
            );
        }

        /**
         * 处理异常信息
         * @param slot
         * @param e
         */
        synchronized void markItemAsFailed(int slot, Exception e) {
            // 获取当前正在处理的req  getIndexWriteRequest()函数会将UpdateRequest内部的indexRequest取出来
            IndexRequest indexRequest = getIndexWriteRequest(bulkRequest.requests().get(slot));
            logger.debug(() -> new ParameterizedMessage("failed to execute pipeline [{}] for document [{}/{}]",
                indexRequest.getPipeline(), indexRequest.index(), indexRequest.id()), e);

            // We hit a error during preprocessing a request, so we:
            // 1) Remember the request item slot from the bulk, so that we're done processing all requests we know what failed
            // 2) Add a bulk item failure for this request
            // 3) Continue with the next request in the bulk.
            // 通过位图记录 哪些slot对应的req处理失败了
            failedSlots.set(slot);
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(indexRequest.index(), indexRequest.id(), e);
            // 失败结果在包装后 也要存储到response中
            itemResponses.add(new BulkItemResponse(slot, indexRequest.opType(), failure));
        }

    }
}
