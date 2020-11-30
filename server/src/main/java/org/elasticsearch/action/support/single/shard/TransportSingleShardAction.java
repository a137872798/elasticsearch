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

package org.elasticsearch.action.support.single.shard;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;

/**
 * A base class for operations that need to perform a read operation on a single shard copy. If the operation fails,
 * the read operation can be performed on other shard copies. Concrete implementations can provide their own list
 * of candidate shards to try the read operation on.
 * 以分片为单位处理数据
 */
public abstract class TransportSingleShardAction<Request extends SingleShardRequest<Request>, Response extends ActionResponse>
        extends TransportAction<Request, Response> {

    protected final ThreadPool threadPool;
    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    private final String transportShardAction;
    private final String executor;

    protected TransportSingleShardAction(String actionName, ThreadPool threadPool, ClusterService clusterService,
                                         TransportService transportService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver, Writeable.Reader<Request> request,
                                         String executor) {
        super(actionName, actionFilters, transportService.getTaskManager());
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        this.transportShardAction = actionName + "[s]";
        this.executor = executor;

        // 该操作是否会产生一系列的子操作
        if (!isSubAction()) {
            transportService.registerRequestHandler(actionName, ThreadPool.Names.SAME, request, new TransportHandler());
        }

        // 一般的请求是通过ShardTransportHandler 来处理的
        transportService.registerRequestHandler(transportShardAction, ThreadPool.Names.SAME, request, new ShardTransportHandler());
    }

    /**
     * Tells whether the action is a main one or a subaction. Used to decide whether we need to register
     * the main transport handler. In fact if the action is a subaction, its execute method
     * will be called locally to its parent action.
     */
    protected boolean isSubAction() {
        return false;
    }

    /**
     * 处理请求的入口
     * @param task
     * @param request
     * @param listener
     */
    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    /**
     * 请求处理委托给该方法
     * @param request
     * @param shardId
     * @return
     * @throws IOException
     */
    protected abstract Response shardOperation(Request request, ShardId shardId) throws IOException;

    /**
     *
     * @param request
     * @param shardId
     * @param listener
     * @throws IOException
     */
    protected void asyncShardOperation(Request request, ShardId shardId, ActionListener<Response> listener) throws IOException {
        threadPool.executor(getExecutor(request, shardId))
            // () -> shardOperation 结果会触发监听器  也就是先产生处理完请求 之后才通过监听器处理结果
            .execute(ActionRunnable.supply(listener, () -> shardOperation(request, shardId)));
    }

    protected abstract Writeable.Reader<Response> getResponseReader();

    protected abstract boolean resolveIndex(Request request);

    protected ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.concreteIndex());
    }

    protected void resolveRequest(ClusterState state, InternalRequest request) {

    }

    /**
     * Returns the candidate shards to execute the operation on or <code>null</code> the execute
     * the operation locally (the node that received the request)
     */
    @Nullable
    protected abstract ShardsIterator shards(ClusterState state, InternalRequest request);

    /**
     * 以shard为单位处理数据
     */
    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        /**
         * 某个索引下的分片迭代器
         */
        private final ShardsIterator shardIt;
        private final InternalRequest internalRequest;
        private final DiscoveryNodes nodes;
        private volatile Exception lastFailure;

        private AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.listener = listener;

            ClusterState clusterState = clusterService.state();
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] based on cluster state version [{}]", request, clusterState.version());
            }
            nodes = clusterState.nodes();
            // 检测本次操作是否被阻塞  是的话返回异常信息
            ClusterBlockException blockException = checkGlobalBlock(clusterState);
            if (blockException != null) {
                throw blockException;
            }

            String concreteSingleIndex;
            if (resolveIndex(request)) {
                // 代表明确声明了 只应该返回一个索引信息 否则抛出异常
                concreteSingleIndex = indexNameExpressionResolver.concreteSingleIndex(clusterState, request).getName();
            } else {
                concreteSingleIndex = request.index();
            }
            this.internalRequest = new InternalRequest(request, concreteSingleIndex);
            resolveRequest(clusterState, internalRequest);

            // 检测针对该index的操作是否被阻塞
            blockException = checkRequestBlock(clusterState, internalRequest);
            if (blockException != null) {
                throw blockException;
            }

            // 获取索引下面的所有分片  clusterState在集群中应该是同步到所有节点的 所以在任何节点获取到的数据应该是一样的
            this.shardIt = shards(clusterState, internalRequest);
        }

        /**
         * 处理每个分片
         */
        public void start() {
            // 当没有解析出concreteSingleIndex时 会走该分支
            if (shardIt == null) {
                // just execute it on the local node
                final Writeable.Reader<Response> reader = getResponseReader();
                transportService.sendRequest(clusterService.localNode(), transportShardAction, internalRequest.request(),
                    new TransportResponseHandler<Response>() {
                    @Override
                    public Response read(StreamInput in) throws IOException {
                        return reader.read(in);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(final Response response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        listener.onFailure(exp);
                    }
                });
            } else {
                perform(null);
            }
        }

        private void onFailure(ShardRouting shardRouting, Exception e) {
            if (e != null) {
                logger.trace(() -> new ParameterizedMessage("{}: failed to execute [{}]", shardRouting,
                    internalRequest.request()), e);
            }
            perform(e);
        }

        /**
         * 串行处理每个分片
         * @param currentFailure
         */
        private void perform(@Nullable final Exception currentFailure) {
            Exception lastFailure = this.lastFailure;
            // 当前没有异常 或者出现了非分片原因导致的异常时   更新异常信息
            if (lastFailure == null || TransportActions.isReadOverrideException(currentFailure)) {
                lastFailure = currentFailure;
                this.lastFailure = currentFailure;
            }
            final ShardRouting shardRouting = shardIt.nextOrNull();
            if (shardRouting == null) {
                Exception failure = lastFailure;
                if (failure == null || isShardNotAvailableException(failure)) {
                    failure = new NoShardAvailableActionException(null,
                        LoggerMessageFormat.format("No shard available for [{}]", internalRequest.request()), failure);
                } else {
                    logger.debug(() -> new ParameterizedMessage("{}: failed to execute [{}]", null,
                        internalRequest.request()), failure);
                }
                listener.onFailure(failure);
                return;
            }
            DiscoveryNode node = nodes.get(shardRouting.currentNodeId());
            if (node == null) {
                onFailure(shardRouting, new NoShardAvailableActionException(shardRouting.shardId()));
            } else {
                // 设置当前正在处理的分片id
                internalRequest.request().internalShardId = shardRouting.shardId();
                if (logger.isTraceEnabled()) {
                    logger.trace(
                            "sending request [{}] to shard [{}] on node [{}]",
                            internalRequest.request(),
                            internalRequest.request().internalShardId,
                            node
                    );
                }
                final Writeable.Reader<Response> reader = getResponseReader();
                // 将请求发送到 当前要处理的shard对应的node上
                transportService.sendRequest(node, transportShardAction, internalRequest.request(),
                    new TransportResponseHandler<Response>() {

                        @Override
                        public Response read(StreamInput in) throws IOException {
                            return reader.read(in);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        /**
                         * 处理接收到的响应结果
                         * @param response
                         */
                        @Override
                        public void handleResponse(final Response response) {
                            listener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            onFailure(shardRouting, exp);
                        }
                });
            }
        }
    }

    /**
     * 当在传输层收到结果时触发函数
     * 当subAction 为 false时 将该对象注册到响应处理器上
     */
    private class TransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(Request request, final TransportChannel channel, Task task) throws Exception {
            // if we have a local operation, execute it on a thread since we don't spawn
            // 这个操作会创建一个新的AsyncSingleAction 并进行处理
            execute(task, request, new ChannelActionListener<>(channel, actionName, request));
        }
    }

    /**
     * 处理 shard相关的请求
     */
    private class ShardTransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(final Request request, final TransportChannel channel, Task task) throws Exception {
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] on shard [{}]", request, request.internalShardId);
            }
            // 收到请求的第一个节点会获取所有相关的分片 然后遍历并将当前正在处理的分片标记在req内部
            asyncShardOperation(request, request.internalShardId, new ChannelActionListener<>(channel, transportShardAction, request));
        }
    }
    /**
     * Internal request class that gets built on each node. Holds the original request plus additional info.
     */
    protected class InternalRequest {
        final Request request;
        final String concreteIndex;

        InternalRequest(Request request, String concreteIndex) {
            this.request = request;
            this.concreteIndex = concreteIndex;
        }

        public Request request() {
            return request;
        }

        public String concreteIndex() {
            return concreteIndex;
        }
    }

    protected String getExecutor(Request request, ShardId shardId) {
        return executor;
    }
}
