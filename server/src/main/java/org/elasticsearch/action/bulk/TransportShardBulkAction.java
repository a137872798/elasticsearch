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
import org.apache.logging.log4j.util.MessageSupplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Performs shard-level bulk (index, delete or update) operations
 * 以分片为单位处理 bulk请求
 */
public class TransportShardBulkAction extends TransportWriteAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = BulkAction.NAME + "[s]";
    public static final ActionType<BulkShardResponse> TYPE = new ActionType<>(ACTION_NAME, BulkShardResponse::new);

    private static final Logger logger = LogManager.getLogger(TransportShardBulkAction.class);

    /**
     * 更新的辅助对象
     */
    private final UpdateHelper updateHelper;
    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                    MappingUpdatedAction mappingUpdatedAction, UpdateHelper updateHelper, ActionFilters actionFilters) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
            BulkShardRequest::new, BulkShardRequest::new, ThreadPool.Names.WRITE, false);
        this.updateHelper = updateHelper;
        this.mappingUpdatedAction = mappingUpdatedAction;
    }

    @Override
    protected TransportRequestOptions transportOptions(Settings settings) {
        return BulkAction.INSTANCE.transportOptions(settings);
    }

    @Override
    protected BulkShardResponse newResponseInstance(StreamInput in) throws IOException {
        return new BulkShardResponse(in);
    }

    /**
     * bulk请求在主分片上处理
     * @param request     该对象包含了一组 bulk请求
     * @param primary      the primary shard to perform the operation on
     * @param listener listener for the result of the operation on primary, including current translog location and operation response
     */
    @Override
    protected void shardOperationOnPrimary(BulkShardRequest request, IndexShard primary,
            ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener) {
        ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.timeout(), logger, threadPool.getThreadContext());
        performOnPrimary(request, primary, updateHelper, threadPool::absoluteTimeInMillis,
            (update, shardId, mappingListener) -> {
                assert update != null;
                assert shardId != null;
                mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), update, mappingListener);
            },
            mappingUpdateListener -> observer.waitForNextChange(new ClusterStateObserver.Listener() {

                /**
                 * 等待集群发生变化 通知监听器
                 * @param state
                 */
                @Override
                public void onNewClusterState(ClusterState state) {
                    mappingUpdateListener.onResponse(null);
                }

                @Override
                public void onClusterServiceClose() {
                    mappingUpdateListener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    mappingUpdateListener.onFailure(new MapperException("timed out while waiting for a dynamic mapping update"));
                }
            }),
            // 将处理逻辑桥接到这个监听器上
            listener, threadPool
        );
    }

    /**
     * 在主分片执行写入操作
     * @param request  该对象包含了本次针对该shardId的多个请求
     * @param primary   此时存在于该节点上 用于表示shard的indexShard对象
     * @param updateHelper  该对象在update时起到一定的辅助作用
     * @param nowInMillisSupplier  用于获取当前时间戳的函数
     * @param mappingUpdater   包含更新逻辑
     * @param waitForMappingUpdate   配合mappingUpdater使用
     * @param listener   业务层监听器
     * @param threadPool
     */
    public static void performOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        UpdateHelper updateHelper,
        LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater,
        Consumer<ActionListener<Void>> waitForMappingUpdate,
        ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener,
        ThreadPool threadPool) {
        new ActionRunnable<>(listener) {

            // 使用专门的 write线程进行处理
            private final Executor executor = threadPool.executor(ThreadPool.Names.WRITE);

            // 在处理这么多req的过程中 始终使用同一个上下文
            private final BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(request, primary);

            final long startBulkTime = System.nanoTime();

            /**
             * 执行处理逻辑
             * @throws Exception
             */
            @Override
            protected void doRun() throws Exception {
                // 只要此时还有req可以被处理
                while (context.hasMoreOperationsToExecute()) {
                    if (executeBulkItemRequest(context, updateHelper, nowInMillisSupplier, mappingUpdater, waitForMappingUpdate,
                        // 执行某个 bulkItemReq 当处理成功时 会自动切换到下一个
                        ActionListener.wrap(v -> executor.execute(this), this::onRejection)) == false) {
                        // We are waiting for a mapping update on another thread, that will invoke this action again once its done
                        // so we just break out here.
                        return;
                    }
                    assert context.isInitial(); // either completed and moved to next or reset
                }
                // 目前默认实现中只有一些stats对象
                primary.getBulkOperationListener().afterBulk(request.totalSizeInBytes(), System.nanoTime() - startBulkTime);
                // We're done, there's no more operations to execute so we resolve the wrapped listener
                finishRequest();
            }

            /**
             * 在处理update操作时 可能遇到了异常 触发该钩子
             * @param e
             */
            @Override
            public void onRejection(Exception e) {
                // We must finish the outstanding request. Finishing the outstanding request can include
                //refreshing and fsyncing. Therefore, we must force execution on the WRITE thread.
                // 一旦在上面某个req的处理失败后 就会终止剩余req的处理  同时剩余的请求以异常作为结果触发监听器
                executor.execute(new ActionRunnable<>(listener) {

                    @Override
                    protected void doRun() {
                        // Fail all operations after a bulk rejection hit an action that waited for a mapping update and finish the request
                        while (context.hasMoreOperationsToExecute()) {
                            context.setRequestToExecute(context.getCurrent());
                            final DocWriteRequest<?> docWriteRequest = context.getRequestToExecute();
                            onComplete(
                                // 生成一个异常结果
                                exceptionToResult(
                                    e, primary, docWriteRequest.opType() == DocWriteRequest.OpType.DELETE, docWriteRequest.version()),
                                context, null);
                        }
                        finishRequest();
                    }

                    @Override
                    public boolean isForceExecution() {
                        return true;
                    }
                });
            }

            /**
             * 当在某个shard.primary上的相关操作完成后 触发监听器
             */
            private void finishRequest() {
                ActionListener.completeWith(listener,

                    // 该函数产生结果 并触发listener
                    () -> new WritePrimaryResult<>(
                        context.getBulkShardRequest(), context.buildShardResponse(), context.getLocationToSync(), null,
                        context.getPrimary(), logger));
            }
            // 立即触发 doRun()
        }.run();
    }

    /**
     * Executes bulk item requests and handles request execution exceptions.
     * @param context 处理过程中使用的上下文
     * @return {@code true} if request completed on this thread and the listener was invoked, {@code false} if the request triggered
     *                      a mapping update that will finish and invoke the listener on a different thread
     *                      处理单个写入操作
     */
    static boolean executeBulkItemRequest(BulkPrimaryExecutionContext context, UpdateHelper updateHelper, LongSupplier nowInMillisSupplier,
                                       MappingUpdatePerformer mappingUpdater, Consumer<ActionListener<Void>> waitForMappingUpdate,
                                       ActionListener<Void> itemDoneListener) throws Exception {
        // 获取本次操作类型
        final DocWriteRequest.OpType opType = context.getCurrent().opType();

        final UpdateHelper.Result updateResult;
        // 如果本次操作类型是一个更新操作
        if (opType == DocWriteRequest.OpType.UPDATE) {
            final UpdateRequest updateRequest = (UpdateRequest) context.getCurrent();
            try {
                // 执行更新操作需要一个准备动作  这里面包含了查询原数据是否存在 不存在的情况就要更改成插入操作
                updateResult = updateHelper.prepare(updateRequest, context.getPrimary(), nowInMillisSupplier);
            } catch (Exception failure) {
                // we may fail translating a update to index or delete operation
                // we use index result to communicate failure while translating update request
                final Engine.Result result =
                    new Engine.IndexResult(failure, updateRequest.version());
                context.setRequestToExecute(updateRequest);
                context.markOperationAsExecuted(result);
                context.markAsCompleted(context.getExecutionResult());
                return true;
            }
            // execute translated update request
            switch (updateResult.getResponseResult()) {
                case CREATED:
                case UPDATED:
                    // 实际上插入以及更新对于lucene来说都是简单插入一条记录
                    IndexRequest indexRequest = updateResult.action();
                    IndexMetadata metadata = context.getPrimary().indexSettings().getIndexMetadata();
                    MappingMetadata mappingMd = metadata.mapping();
                    // 实际上就是为 indexReq设置id
                    indexRequest.process(metadata.getCreationVersion(), mappingMd, updateRequest.concreteIndex());
                    context.setRequestToExecute(indexRequest);
                    break;
                case DELETED:
                    context.setRequestToExecute(updateResult.action());
                    break;
                case NOOP:
                    context.markOperationAsNoOp(updateResult.action());
                    context.markAsCompleted(context.getExecutionResult());
                    return true;
                default:
                    throw new IllegalStateException("Illegal update operation " + updateResult.getResponseResult());
            }
        } else {
            context.setRequestToExecute(context.getCurrent());
            updateResult = null;
        }

        assert context.getRequestToExecute() != null; // also checks that we're in TRANSLATED state

        final IndexShard primary = context.getPrimary();
        final long version = context.getRequestToExecute().version();
        final boolean isDelete = context.getRequestToExecute().opType() == DocWriteRequest.OpType.DELETE;
        final Engine.Result result;
        // 使用 indexShard执行不同的操作  实际上就会委托给内部的engine执行任务
        if (isDelete) {
            final DeleteRequest request = context.getRequestToExecute();
            result = primary.applyDeleteOperationOnPrimary(version, request.id(), request.versionType(),
                request.ifSeqNo(), request.ifPrimaryTerm());
        } else {
            final IndexRequest request = context.getRequestToExecute();
            result = primary.applyIndexOperationOnPrimary(version, request.versionType(), new SourceToParse(
                    request.index(), request.id(), request.source(), request.getContentType(), request.routing()),
                    request.ifSeqNo(), request.ifPrimaryTerm(), request.getAutoGeneratedTimestamp(), request.isRetry());
        }

        // TODO 先忽略需要更新数据的场景
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {

            try {
                primary.mapperService().merge(MapperService.SINGLE_MAPPING_NAME,
                    new CompressedXContent(result.getRequiredMappingUpdate(), XContentType.JSON, ToXContent.EMPTY_PARAMS),
                    MapperService.MergeReason.MAPPING_UPDATE_PREFLIGHT);
            } catch (Exception e) {
                logger.info(() -> new ParameterizedMessage("{} mapping update rejected by primary", primary.shardId()), e);
                onComplete(exceptionToResult(e, primary, isDelete, version), context, updateResult);
                return true;
            }

            mappingUpdater.updateMappings(result.getRequiredMappingUpdate(), primary.shardId(),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void v) {
                        context.markAsRequiringMappingUpdate();
                        waitForMappingUpdate.accept(
                            ActionListener.runAfter(new ActionListener<>() {
                                @Override
                                public void onResponse(Void v) {
                                    assert context.requiresWaitingForMappingUpdate();
                                    context.resetForExecutionForRetry();
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    context.failOnMappingUpdate(e);
                                }
                            }, () -> itemDoneListener.onResponse(null))
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onComplete(exceptionToResult(e, primary, isDelete, version), context, updateResult);
                        // Requesting mapping update failed, so we don't have to wait for a cluster state update
                        assert context.isInitial();
                        itemDoneListener.onResponse(null);
                    }
                });
            return false;
        } else {
            onComplete(result, context, updateResult);
        }
        return true;
    }

    private static Engine.Result exceptionToResult(Exception e, IndexShard primary, boolean isDelete, long version) {
        return isDelete ? primary.getFailedDeleteResult(e, version) : primary.getFailedIndexResult(e, version);
    }

    /**
     * 此时已经将某个操作作用到了 engine内部
     * @param r   在engine处理后的结果
     * @param context
     * @param updateResult  在prepare阶段生成的结果对象
     */
    private static void onComplete(Engine.Result r, BulkPrimaryExecutionContext context, UpdateHelper.Result updateResult) {

        // 使用engine处理后的结果 标记context为已完成状态
        context.markOperationAsExecuted(r);

        // 根据当前操作类型判断是否是一个更新请求
        final DocWriteRequest<?> docWriteRequest = context.getCurrent();
        final DocWriteRequest.OpType opType = docWriteRequest.opType();
        final boolean isUpdate = opType == DocWriteRequest.OpType.UPDATE;

        // 查看本次处理结果是否失败
        final BulkItemResponse executionResult = context.getExecutionResult();
        final boolean isFailed = executionResult.isFailed();
        // 如果是版本冲突异常 会进行重试
        if (isUpdate && isFailed && isConflictException(executionResult.getFailure().getCause())
            && context.getRetryCounter() < ((UpdateRequest) docWriteRequest).retryOnConflict()) {
            context.resetForExecutionForRetry();
            return;
        }
        final BulkItemResponse response;
        if (isUpdate) {
            // 更新成功 对BulkItemResponse做一些处理
            response = processUpdateResponse((UpdateRequest) docWriteRequest, context.getConcreteIndex(), executionResult, updateResult);
        } else {
            // 失败只是打印日志
            if (isFailed) {
                final Exception failure = executionResult.getFailure().getCause();
                final MessageSupplier messageSupplier = () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                    context.getPrimary().shardId(), opType.getLowercase(), docWriteRequest);
                if (TransportShardBulkAction.isConflictException(failure)) {
                    logger.trace(messageSupplier, failure);
                } else {
                    logger.debug(messageSupplier, failure);
                }
            }
            response = executionResult;
        }
        context.markAsCompleted(response);
        assert context.isInitial();
    }

    private static boolean isConflictException(final Exception e) {
        return ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException;
    }

    /**
     * Creates a new bulk item result from the given requests and result of performing the update operation on the shard.
     * 处理更新成功后的结果
     */
    private static BulkItemResponse processUpdateResponse(final UpdateRequest updateRequest, final String concreteIndex,
                                                          BulkItemResponse operationResponse, final UpdateHelper.Result translate) {
        final BulkItemResponse response;
        // 如果本次操作失败 相当于就是生成一个结果副本 并返回
        if (operationResponse.isFailed()) {
            response = new BulkItemResponse(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, operationResponse.getFailure());
        } else {
            // 获取本次操作类型
            final DocWriteResponse.Result translatedResult = translate.getResponseResult();
            final UpdateResponse updateResponse;
            if (translatedResult == DocWriteResponse.Result.CREATED || translatedResult == DocWriteResponse.Result.UPDATED) {

                // 获取作用在engine上的req
                final IndexRequest updateIndexRequest = translate.action();
                final IndexResponse indexResponse = operationResponse.getResponse();

                // 转换成 updateResponse
                updateResponse = new UpdateResponse(indexResponse.getShardInfo(), indexResponse.getShardId(),
                    indexResponse.getId(), indexResponse.getSeqNo(), indexResponse.getPrimaryTerm(),
                    indexResponse.getVersion(), indexResponse.getResult());

                // 本次是否有获取 source field
                if (updateRequest.fetchSource() != null && updateRequest.fetchSource().fetchSource()) {
                    final BytesReference indexSourceAsBytes = updateIndexRequest.source();
                    final Tuple<XContentType, Map<String, Object>> sourceAndContent =
                        XContentHelper.convertToMap(indexSourceAsBytes, true, updateIndexRequest.getContentType());

                    // 将source通过 context处理后 生成GetResult对象 并设置到response中
                    updateResponse.setGetResult(UpdateHelper.extractGetResult(updateRequest, concreteIndex,
                        indexResponse.getSeqNo(), indexResponse.getPrimaryTerm(),
                        indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
                }
                // 本次是删除操作
            } else if (translatedResult == DocWriteResponse.Result.DELETED) {
                // 套路与 CREATE/UPDATE 类似
                final DeleteResponse deleteResponse = operationResponse.getResponse();
                updateResponse = new UpdateResponse(deleteResponse.getShardInfo(), deleteResponse.getShardId(),
                    deleteResponse.getId(), deleteResponse.getSeqNo(), deleteResponse.getPrimaryTerm(),
                    deleteResponse.getVersion(), deleteResponse.getResult());

                final GetResult getResult = UpdateHelper.extractGetResult(updateRequest, concreteIndex,
                    deleteResponse.getSeqNo(), deleteResponse.getPrimaryTerm(), deleteResponse.getVersion(),
                    translate.updatedSourceAsMap(), translate.updateSourceContentType(), null);

                updateResponse.setGetResult(getResult);
            } else {
                throw new IllegalArgumentException("unknown operation type: " + translatedResult);
            }
            response = new BulkItemResponse(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, updateResponse);
        }
        return response;
    }

    /**
     * 在副本上处理该请求  TODO 将结果写入到多少副本才算成功呢 会跟kafka一样 写入超过一半么???
     * @param request
     * @param replica      the replica shard to perform the operation on
     * @return
     * @throws Exception
     */
    @Override
    public WriteReplicaResult<BulkShardRequest> shardOperationOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        final long startBulkTime = System.nanoTime();
        final Translog.Location location = performOnReplica(request, replica);
        replica.getBulkOperationListener().afterBulk(request.totalSizeInBytes(), System.nanoTime() - startBulkTime);
        return new WriteReplicaResult<>(request, location, null, replica, logger);
    }

    /**
     * 在主分片上处理请求是不需要获取 location信息的  但是在分片上处理就需要location信息
     * @param request
     * @param replica
     * @return
     * @throws Exception
     */
    public static Translog.Location performOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            final BulkItemRequest item = request.items()[i];
            // 因为处理过程是先走primary 之后转发到replica  而在primary处理时已经为每个itemReq设置了对应的 response了
            final BulkItemResponse response = item.getPrimaryResponse();
            final Engine.Result operationResult;

            // 如果在主分片处理失败了   在engine处理后返回的 Result中会携带seqNo信息  至少能确定该seqNo之前的数据都需要同步
            if (item.getPrimaryResponse().isFailed()) {
                // 如果连seq都无法确认 就无法继续处理了
                if (response.getFailure().getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    continue; // ignore replication as we didn't generate a sequence number for this request.
                }

                final long primaryTerm;
                // TODO 可能是主分片已经过时 ???
                if (response.getFailure().getTerm() == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                    // primary is on older version, just take the current primary term
                    // 使用副本当前知晓的最新的主分片term
                    primaryTerm = replica.getOperationPrimaryTerm();
                } else {
                    primaryTerm = response.getFailure().getTerm();
                }
                // 在这个seqNo位 生成了一个 noop对象
                operationResult = replica.markSeqNoAsNoop(response.getFailure().getSeqNo(), primaryTerm,
                    response.getFailure().getMessage());
            } else {
                // 本次虽然操作成功 但是操作类型就是 NOOP 那么不做处理
                if (response.getResponse().getResult() == DocWriteResponse.Result.NOOP) {
                    continue; // ignore replication as it's a noop
                }
                assert response.getResponse().getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO;
                // 其余情况才需要进行真正的处理
                operationResult = performOpOnReplica(response.getResponse(), item.request(), replica);
            }
            assert operationResult != null : "operation result must never be null when primary response has no failure";

            // 将location信息同步到本次操作后的result的位置信息
            location = syncOperationResultOrThrow(operationResult, location);
        }
        return location;
    }

    /**
     * 在副本上处理请求
     * @param primaryResponse
     * @param docWriteRequest   这个原请求对象 在primary上处理时 已经调用过prepare进行数据更新了
     * @param replica
     * @return
     * @throws Exception
     */
    private static Engine.Result performOpOnReplica(DocWriteResponse primaryResponse, DocWriteRequest<?> docWriteRequest,
                                                    IndexShard replica) throws Exception {
        final Engine.Result result;
        switch (docWriteRequest.opType()) {
            case CREATE:
            case INDEX:
                final IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                final ShardId shardId = replica.shardId();
                final SourceToParse sourceToParse = new SourceToParse(shardId.getIndexName(), indexRequest.id(),
                    indexRequest.source(), indexRequest.getContentType(), indexRequest.routing());
                result = replica.applyIndexOperationOnReplica(primaryResponse.getSeqNo(), primaryResponse.getPrimaryTerm(),
                    primaryResponse.getVersion(), indexRequest.getAutoGeneratedTimestamp(), indexRequest.isRetry(), sourceToParse);
                break;
            case DELETE:
                DeleteRequest deleteRequest = (DeleteRequest) docWriteRequest;
                result = replica.applyDeleteOperationOnReplica(primaryResponse.getSeqNo(), primaryResponse.getPrimaryTerm(),
                    primaryResponse.getVersion(), deleteRequest.id());
                break;
            default:
                assert false : "Unexpected request operation type on replica: " + docWriteRequest + ";primary result: " + primaryResponse;
                throw new IllegalStateException("Unexpected request operation type on replica: " + docWriteRequest.opType().getLowercase());
        }

        // TODO
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
            // Even though the primary waits on all nodes to ack the mapping changes to the master
            // (see MappingUpdatedAction.updateMappingOnMaster) we still need to protect against missing mappings
            // and wait for them. The reason is concurrent requests. Request r1 which has new field f triggers a
            // mapping update. Assume that that update is first applied on the primary, and only later on the replica
            // (it’s happening concurrently). Request r2, which now arrives on the primary and which also has the new
            // field f might see the updated mapping (on the primary), and will therefore proceed to be replicated
            // to the replica. When it arrives on the replica, there’s no guarantee that the replica has already
            // applied the new mapping, so there is no other option than to wait.
            throw new TransportReplicationAction.RetryOnReplicaException(replica.shardId(),
                "Mappings are not available on the replica yet, triggered update: " + result.getRequiredMappingUpdate());
        }
        return result;
    }
}
