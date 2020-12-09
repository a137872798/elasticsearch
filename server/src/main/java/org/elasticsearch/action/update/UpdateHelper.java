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

package org.elasticsearch.action.update;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.UpdateScript;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Helper for translating an update request to an index, delete request or update response.
 */
public class UpdateHelper {

    private static final Logger logger = LogManager.getLogger(UpdateHelper.class);

    private final ScriptService scriptService;

    public UpdateHelper(ScriptService scriptService) {
        this.scriptService = scriptService;
    }

    /**
     * Prepares an update request by converting it into an index or delete request or an update response (no action).
     * @param request 本次更新请求
     * @param indexShard 本次要处理的分片
     */
    public Result prepare(UpdateRequest request, IndexShard indexShard, LongSupplier nowInMillis) {
        // 根据参数信息从engine中获取结果信息
        final GetResult getResult = indexShard.getService().getForUpdate(
            request.id(), request.ifSeqNo(), request.ifPrimaryTerm());
        return prepare(indexShard.shardId(), request, getResult, nowInMillis);
    }

    /**
     * Prepares an update request by converting it into an index or delete request or an update response (no action, in the event of a
     * noop).
     * 根据本次的查询结果 为下一步更新做准备
     */
    protected Result prepare(ShardId shardId, UpdateRequest request, final GetResult getResult, LongSupplier nowInMillis) {
        // 代表实际上本次没有查询到数据  那么本次实际上是一次插入操作
        if (getResult.isExists() == false) {
            // If the document didn't exist, execute the update request as an upsert
            return prepareUpsert(shardId, request, getResult, nowInMillis);
            // 如果查询到的结果没有source(field) 那么抛出异常
        } else if (getResult.internalSourceRef() == null) {
            // no source, we can't do anything, throw a failure...
            throw new DocumentSourceMissingException(shardId, request.id());
            // 如果请求中没有包含脚本更新的描述信息  使用携带的doc
        } else if (request.script() == null && request.doc() != null) {
            // The request has no script, it is a new doc that should be merged with the old document
            // 处理普通的更新请求 核心实现就是将2个map进行合并
            return prepareUpdateIndexRequest(shardId, request, getResult, request.detectNoop());
        } else {
            // The request has a script (or empty script), execute the script and prepare a new index request
            // 通过脚本服务进行更新  TODO 先忽略 ES默认没有内置的脚本
            return prepareUpdateScriptRequest(shardId, request, getResult, nowInMillis);
        }
    }

    /**
     * Execute a scripted upsert, where there is an existing upsert document and a script to be executed. The script is executed and a new
     * Tuple of operation and updated {@code _source} is returned.
     * 使用脚本更新map数据
     */
    Tuple<UpdateOpType, Map<String, Object>> executeScriptedUpsert(Map<String, Object> upsertDoc, Script script, LongSupplier nowInMillis) {
        Map<String, Object> ctx = new HashMap<>(3);
        // Tell the script that this is a create and not an update
        // 插入一些特殊字段
        ctx.put(ContextFields.OP, UpdateOpType.CREATE.toString());
        ctx.put(ContextFields.SOURCE, upsertDoc);
        ctx.put(ContextFields.NOW, nowInMillis.getAsLong());

        // 使用脚本对象对内部数据进行更新
        ctx = executeScript(script, ctx);

        // 找到本次更新的操作类型
        UpdateOpType operation = UpdateOpType.lenientFromString((String) ctx.get(ContextFields.OP), logger, script.getIdOrCode());

        // 当经过script处理过后 source内部的数据已经发生了变化
        Map<String, Object> newSource = (Map<String, Object>) ctx.get(ContextFields.SOURCE);

        if (operation != UpdateOpType.CREATE && operation != UpdateOpType.NONE) {
            // Only valid options for an upsert script are "create" (the default) or "none", meaning abort upsert
            logger.warn("Invalid upsert operation [{}] for script [{}], doing nothing...", operation, script.getIdOrCode());
            operation = UpdateOpType.NONE;
        }

        return new Tuple<>(operation, newSource);
    }

    /**
     * Prepare the request for upsert, executing the upsert script if present, and returning a {@code Result} containing a new
     * {@code IndexRequest} to be executed on the primary and replicas.
     * @param shardId 本次处理数据对应的分片id
     * @param request 本次更新的请求体
     * @param getResult 之前的查询结果 此时内部应该是没有数据的 isExist为false
     * 采用相关参数作为条件 无法get到数据 那么本次实质就是一次插入请求
     */
    Result prepareUpsert(ShardId shardId, UpdateRequest request, final GetResult getResult, LongSupplier nowInMillis) {
            if (request.upsertRequest() == null && !request.docAsUpsert()) {
                throw new DocumentMissingException(shardId, request.id());
            }
            // 根据情况选择不同的indexRequest
            IndexRequest indexRequest = request.docAsUpsert() ? request.doc() : request.upsertRequest();
            // 代表尝试使用 脚本来进行更新
            if (request.scriptedUpsert() && request.script() != null) {
                // Run the script to perform the create logic
                IndexRequest upsert = request.upsertRequest();
                // 使用脚本来更新source数据
                Tuple<UpdateOpType, Map<String, Object>> upsertResult = executeScriptedUpsert(upsert.sourceAsMap(), request.script,
                    nowInMillis);
                switch (upsertResult.v1()) {
                    case CREATE:
                        // 更新indexReq的数据
                        indexRequest = Requests.indexRequest(request.index()).source(upsertResult.v2());
                        break;
                    case NONE:
                        // 代表本次实际上是一个 NOOP 操作
                        UpdateResponse update = new UpdateResponse(shardId, getResult.getId(),
                                getResult.getSeqNo(), getResult.getPrimaryTerm(), getResult.getVersion(), DocWriteResponse.Result.NOOP);
                        update.setGetResult(getResult);
                        return new Result(update, DocWriteResponse.Result.NOOP, upsertResult.v2(), XContentType.JSON);
                    default:
                        // It's fine to throw an exception here, the leniency is handled/logged by `executeScriptedUpsert`
                        throw new IllegalArgumentException("unknown upsert operation, got: " + upsertResult.v1());
                }
            }

            // 其余信息 会沿用之前的req
            indexRequest.index(request.index())
                    .id(request.id()).setRefreshPolicy(request.getRefreshPolicy()).routing(request.routing())
                    .timeout(request.timeout()).waitForActiveShards(request.waitForActiveShards())
                    // it has to be a "create!"
                    .create(true);

            if (request.versionType() != VersionType.INTERNAL) {
                // in all but the internal versioning mode, we want to create the new document using the given version.
                indexRequest.version(request.version()).versionType(request.versionType());
            }

            return new Result(indexRequest, DocWriteResponse.Result.CREATED, null, null);
    }

    /**
     * Calculate a routing value to be used, either the included index request's routing, or retrieved document's routing when defined.
     * 计算路由信息
     */
    @Nullable
    static String calculateRouting(GetResult getResult, @Nullable IndexRequest updateIndexRequest) {
        // 如果请求中已经携带了路由信息 直接返回
        if (updateIndexRequest != null && updateIndexRequest.routing() != null) {
            return updateIndexRequest.routing();
            // 首先 req中没有routing信息  其次get结果中包含routing的信息
        } else if (getResult.getFields().containsKey(RoutingFieldMapper.NAME)) {
            return getResult.field(RoutingFieldMapper.NAME).getValue().toString();
        } else {
            return null;
        }
    }

    /**
     * Prepare the request for merging the existing document with a new one, can optionally detect a noop change. Returns a {@code Result}
     * containing a new {@code IndexRequest} to be executed on the primary and replicas.
     * @param shardId 本次处理的分片id
     * @param request 包含本次更新请求的具体参数
     * @param getResult 本次get请求的结果
     * @param detectNoop 在执行更新操作前是否要检测本次是不是 NOOP操作
     */
    Result prepareUpdateIndexRequest(ShardId shardId, UpdateRequest request, GetResult getResult, boolean detectNoop) {

        final IndexRequest currentRequest = request.doc();
        // 从req 或者get结果的 routing.field中获取路由信息
        final String routing = calculateRouting(getResult, currentRequest);
        // 将内部的source转换成结构化数据
        final Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final XContentType updateSourceContentType = sourceAndContent.v1();
        final Map<String, Object> updatedSourceAsMap = sourceAndContent.v2();

        // 尝试将原始数据对应的map与本次传入的map进行合并  实际上就是map操作   返回结果就是数据是否发生了变化
        final boolean noop = !XContentHelper.update(updatedSourceAsMap, currentRequest.sourceAsMap(), detectNoop);

        // We can only actually turn the update into a noop if detectNoop is true to preserve backwards compatibility and to handle cases
        // where users repopulating multi-fields or adding synonyms, etc.
        // 如果本次没有数据发生变化
        if (detectNoop && noop) {
            UpdateResponse update = new UpdateResponse(shardId, getResult.getId(),
                getResult.getSeqNo(), getResult.getPrimaryTerm(), getResult.getVersion(), DocWriteResponse.Result.NOOP);
            update.setGetResult(extractGetResult(request, request.index(), getResult.getSeqNo(), getResult.getPrimaryTerm(),
                getResult.getVersion(), updatedSourceAsMap, updateSourceContentType, getResult.internalSourceRef()));
            // 将本次结果包装成result返回
            return new Result(update, DocWriteResponse.Result.NOOP, updatedSourceAsMap, updateSourceContentType);
        } else {
            // 更新本次的indexResult  同时将更新后的map作为结果
            final IndexRequest finalIndexRequest = Requests.indexRequest(request.index())
                    .id(request.id()).routing(routing)
                    .source(updatedSourceAsMap, updateSourceContentType)
                    .setIfSeqNo(getResult.getSeqNo()).setIfPrimaryTerm(getResult.getPrimaryTerm())
                    .waitForActiveShards(request.waitForActiveShards()).timeout(request.timeout())
                    .setRefreshPolicy(request.getRefreshPolicy());
            return new Result(finalIndexRequest, DocWriteResponse.Result.UPDATED, updatedSourceAsMap, updateSourceContentType);
        }
    }

    /**
     * Prepare the request for updating an existing document using a script. Executes the script and returns a {@code Result} containing
     * either a new {@code IndexRequest} or {@code DeleteRequest} (depending on the script's returned "op" value) to be executed on the
     * primary and replicas.
     */
    Result prepareUpdateScriptRequest(ShardId shardId, UpdateRequest request, GetResult getResult, LongSupplier nowInMillis) {
        final IndexRequest currentRequest = request.doc();
        final String routing = calculateRouting(getResult, currentRequest);
        final Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        final XContentType updateSourceContentType = sourceAndContent.v1();
        final Map<String, Object> sourceAsMap = sourceAndContent.v2();

        Map<String, Object> ctx = new HashMap<>(16);
        ctx.put(ContextFields.OP, UpdateOpType.INDEX.toString()); // The default operation is "index"
        ctx.put(ContextFields.INDEX, getResult.getIndex());
        ctx.put(ContextFields.TYPE, MapperService.SINGLE_MAPPING_NAME);
        ctx.put(ContextFields.ID, getResult.getId());
        ctx.put(ContextFields.VERSION, getResult.getVersion());
        ctx.put(ContextFields.ROUTING, routing);
        ctx.put(ContextFields.SOURCE, sourceAsMap);
        ctx.put(ContextFields.NOW, nowInMillis.getAsLong());

        ctx = executeScript(request.script, ctx);

        UpdateOpType operation = UpdateOpType.lenientFromString((String) ctx.get(ContextFields.OP), logger, request.script.getIdOrCode());

        @SuppressWarnings("unchecked")
        final Map<String, Object> updatedSourceAsMap = (Map<String, Object>) ctx.get(ContextFields.SOURCE);

        switch (operation) {
            case INDEX:
                final IndexRequest indexRequest = Requests.indexRequest(request.index())
                        .id(request.id()).routing(routing)
                        .source(updatedSourceAsMap, updateSourceContentType)
                        .setIfSeqNo(getResult.getSeqNo()).setIfPrimaryTerm(getResult.getPrimaryTerm())
                        .waitForActiveShards(request.waitForActiveShards()).timeout(request.timeout())
                        .setRefreshPolicy(request.getRefreshPolicy());
                return new Result(indexRequest, DocWriteResponse.Result.UPDATED, updatedSourceAsMap, updateSourceContentType);
            case DELETE:
                DeleteRequest deleteRequest = Requests.deleteRequest(request.index())
                        .id(request.id()).routing(routing)
                        .setIfSeqNo(getResult.getSeqNo()).setIfPrimaryTerm(getResult.getPrimaryTerm())
                        .waitForActiveShards(request.waitForActiveShards())
                        .timeout(request.timeout()).setRefreshPolicy(request.getRefreshPolicy());
                return new Result(deleteRequest, DocWriteResponse.Result.DELETED, updatedSourceAsMap, updateSourceContentType);
            default:
                // If it was neither an INDEX or DELETE operation, treat it as a noop
                UpdateResponse update = new UpdateResponse(shardId, getResult.getId(),
                        getResult.getSeqNo(), getResult.getPrimaryTerm(), getResult.getVersion(), DocWriteResponse.Result.NOOP);
                update.setGetResult(extractGetResult(request, request.index(), getResult.getSeqNo(), getResult.getPrimaryTerm(),
                    getResult.getVersion(), updatedSourceAsMap, updateSourceContentType, getResult.internalSourceRef()));
                return new Result(update, DocWriteResponse.Result.NOOP, updatedSourceAsMap, updateSourceContentType);
        }
    }

    /**
     * 使用脚本更新source数据
     * @param script
     * @param ctx
     * @return
     */
    private Map<String, Object> executeScript(Script script, Map<String, Object> ctx) {
        try {
            if (scriptService != null) {
                // 脚本服务会借助 script内的信息 找到scriptEngine 配合 scriptContext 会生成一个脚本工厂
                UpdateScript.Factory factory = scriptService.compile(script, UpdateScript.CONTEXT);
                // 相当于就是将原本简单的更新逻辑 复杂化了 抽取了一个被成为脚本的对象进行更新 而获取脚本对象又需要借助其他一系列的类
                // 目前ES内只有一个mock实现
                UpdateScript executableScript = factory.newInstance(script.getParams(), ctx);
                executableScript.execute();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to execute script", e);
        }
        return ctx;
    }

    /**
     * Applies {@link UpdateRequest#fetchSource()} to the _source of the updated document to be returned in a update response.
     * 将相关信息包装成 getResult对象
     */
    public static GetResult extractGetResult(final UpdateRequest request, String concreteIndex, long seqNo, long primaryTerm, long version,
                                             final Map<String, Object> source, XContentType sourceContentType,
                                             @Nullable final BytesReference sourceAsBytes) {
        // 如果没有获取source属性 就不需要处理
        if (request.fetchSource() == null || request.fetchSource().fetchSource() == false) {
            return null;
        }

        BytesReference sourceFilteredAsBytes = sourceAsBytes;
        if (request.fetchSource().includes().length > 0 || request.fetchSource().excludes().length > 0) {
            SourceLookup sourceLookup = new SourceLookup();
            sourceLookup.setSource(source);
            // 使用 FetchSourceContext内部的filter去处理source数据
            Object value = sourceLookup.filter(request.fetchSource());
            try {
                // 将处理后的结果填充到输出流中
                final int initialCapacity = Math.min(1024, sourceAsBytes.length());
                BytesStreamOutput streamOutput = new BytesStreamOutput(initialCapacity);
                try (XContentBuilder builder = new XContentBuilder(sourceContentType.xContent(), streamOutput)) {
                    builder.value(value);
                    sourceFilteredAsBytes = BytesReference.bytes(builder);
                }
            } catch (IOException e) {
                throw new ElasticsearchException("Error filtering source", e);
            }
        }

        // TODO when using delete/none, we can still return the source as bytes by generating it (using the sourceContentType)
        // 总的来说就是将 source通过 FetchSourceContext处理后 重新生成GetResult
        return new GetResult(concreteIndex, request.id(), seqNo, primaryTerm, version, true, sourceFilteredAsBytes,
            Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * 表示某次更新的结果
     */
    public static class Result {

        private final Writeable action;
        private final DocWriteResponse.Result result;
        private final Map<String, Object> updatedSourceAsMap;
        private final XContentType updateSourceContentType;

        public Result(Writeable action, DocWriteResponse.Result result, Map<String, Object> updatedSourceAsMap,
                      XContentType updateSourceContentType) {
            this.action = action;
            this.result = result;
            this.updatedSourceAsMap = updatedSourceAsMap;
            this.updateSourceContentType = updateSourceContentType;
        }

        @SuppressWarnings("unchecked")
        public <T extends Writeable> T action() {
            return (T) action;
        }

        public DocWriteResponse.Result getResponseResult() {
            return result;
        }

        public Map<String, Object> updatedSourceAsMap() {
            return updatedSourceAsMap;
        }

        public XContentType updateSourceContentType() {
            return updateSourceContentType;
        }
    }

    /**
     * After executing the script, this is the type of operation that will be used for subsequent actions. This corresponds to the "ctx.op"
     * variable inside of scripts.
     */
    enum UpdateOpType {
        CREATE("create"),
        INDEX("index"),
        DELETE("delete"),
        NONE("none");

        private final String name;

        UpdateOpType(String name) {
            this.name = name;
        }

        public static UpdateOpType lenientFromString(String operation, Logger logger, String scriptId) {
            switch (operation) {
                case "create":
                    return UpdateOpType.CREATE;
                case "index":
                    return UpdateOpType.INDEX;
                case "delete":
                    return UpdateOpType.DELETE;
                case "none":
                    return UpdateOpType.NONE;
                default:
                    // TODO: can we remove this leniency yet??
                    logger.warn("Used upsert operation [{}] for script [{}], doing nothing...", operation, scriptId);
                    return UpdateOpType.NONE;
            }
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Field names used to populate the script context
     */
    public static class ContextFields {
        public static final String CTX = "ctx";
        public static final String OP = "op";
        public static final String SOURCE = "_source";
        public static final String NOW = "_now";
        public static final String INDEX = "_index";
        public static final String TYPE = "_type";
        public static final String ID = "_id";
        public static final String VERSION = "_version";
        public static final String ROUTING = "_routing";
    }
}
