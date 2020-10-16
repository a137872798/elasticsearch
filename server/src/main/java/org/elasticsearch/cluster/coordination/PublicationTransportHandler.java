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
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 有关发布操作的传输层处理器
 */
public class PublicationTransportHandler {

    private static final Logger logger = LogManager.getLogger(PublicationTransportHandler.class);

    public static final String PUBLISH_STATE_ACTION_NAME = "internal:cluster/coordination/publish_state";
    public static final String COMMIT_STATE_ACTION_NAME = "internal:cluster/coordination/commit_state";

    /**
     * 传输层服务对象
     */
    private final TransportService transportService;
    /**
     * 该对象注册了 某些class 通过什么reader对象可以解析数据 反序列化
     */
    private final NamedWriteableRegistry namedWriteableRegistry;

    /**
     * 该对象会根据 发布请求生成对应的响应结果
     */
    private final Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest;

    /**
     * 记录最后一次看到的集群状态
     * 每当收到一个 publish请求时 就会更新该字段
     */
    private AtomicReference<ClusterState> lastSeenClusterState = new AtomicReference<>();

    // the master needs the original non-serialized state as the cluster state contains some volatile information that we
    // don't want to be replicated because it's not usable on another node (e.g. UnassignedInfo.unassignedTimeNanos) or
    // because it's mostly just debugging info that would unnecessarily blow up CS updates (I think there was one in
    // snapshot code).
    // TODO: look into these and check how to get rid of them
    // 当某个节点将发布请求发送给自己时 设置这个变量  只要收到结果 就将该值置空  发往远端时集群状态可能会采用压缩算法 如果直接发往本地 只要存储就可以了 使用时再取出来
    private AtomicReference<PublishRequest> currentPublishRequestToSelf = new AtomicReference<>();

    /**
     * 收到了几次集群状态
     */
    private final AtomicLong fullClusterStateReceivedCount = new AtomicLong();
    /**
     * 收到几次不兼容的集群状态
     */
    private final AtomicLong incompatibleClusterStateDiffReceivedCount = new AtomicLong();
    /**
     * 几次兼容的集群状态
     */
    private final AtomicLong compatibleClusterStateDiffReceivedCount = new AtomicLong();
    // -> no need to put a timeout on the options here, because we want the response to eventually be received
    //  and not log an error if it arrives after the timeout
    private final TransportRequestOptions stateRequestOptions = TransportRequestOptions.builder()
        .withType(TransportRequestOptions.Type.STATE).build();

    /**
     *
     * @param transportService
     * @param namedWriteableRegistry
     * @param handlePublishRequest
     * @param handleApplyCommit  该对象处理commit请求
     */
    public PublicationTransportHandler(TransportService transportService, NamedWriteableRegistry namedWriteableRegistry,
                                       Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest,
                                       BiConsumer<ApplyCommitRequest, ActionListener<Void>> handleApplyCommit) {
        this.transportService = transportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.handlePublishRequest = handlePublishRequest;

        // 注册有关 发布和提交的请求处理器
        transportService.registerRequestHandler(PUBLISH_STATE_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            BytesTransportRequest::new, (request, channel, task) -> channel.sendResponse(handleIncomingPublishRequest(request)));

        transportService.registerRequestHandler(COMMIT_STATE_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            ApplyCommitRequest::new,
            (request, channel, task) -> handleApplyCommit.accept(request, transportCommitCallback(channel)));
    }

    /**
     * 生成一个虚拟的监听器 实际逻辑通过转发给channel对象
     * @param channel
     * @return
     */
    private ActionListener<Void> transportCommitCallback(TransportChannel channel) {
        return new ActionListener<Void>() {

            @Override
            public void onResponse(Void aVoid) {
                try {
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                } catch (IOException e) {
                    logger.debug("failed to send response on commit", e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (IOException ie) {
                    e.addSuppressed(ie);
                    logger.debug("failed to send response on commit", e);
                }
            }
        };
    }

    public PublishClusterStateStats stats() {
        return new PublishClusterStateStats(
            fullClusterStateReceivedCount.get(),
            incompatibleClusterStateDiffReceivedCount.get(),
            compatibleClusterStateDiffReceivedCount.get());
    }

    public interface PublicationContext {

        void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                ActionListener<PublishWithJoinResponse> responseActionListener);

        void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommitRequest,
                             ActionListener<TransportResponse.Empty> responseActionListener);

    }


    /**
     * 将这个集群的变化事件发布出去
     * @param clusterChangedEvent
     * @return
     */
    public PublicationContext newPublicationContext(ClusterChangedEvent clusterChangedEvent) {

        // 此时集群中的所有节点
        final DiscoveryNodes nodes = clusterChangedEvent.state().nodes();
        final ClusterState newState = clusterChangedEvent.state();
        final ClusterState previousState = clusterChangedEvent.previousState();
        // TODO 先前描述阻塞相关的信息中 只要有一个 不能将状态持久化 就要将不同版本对应的集群数据都写入
        final boolean sendFullVersion = clusterChangedEvent.previousState().getBlocks().disableStatePersistence();
        final Map<Version, BytesReference> serializedStates = new HashMap<>();
        final Map<Version, BytesReference> serializedDiffs = new HashMap<>();

        // we build these early as a best effort not to commit in the case of error.
        // sadly this is not water tight as it may that a failed diff based publishing to a node
        // will cause a full serialization based on an older version, which may fail after the
        // change has been committed.
        buildDiffAndSerializeStates(clusterChangedEvent.state(), clusterChangedEvent.previousState(),
            nodes, sendFullVersion, serializedStates, serializedDiffs);

        return new PublicationContext() {

            /**
             * 将发布请求发往某个node
             * @param destination
             * @param publishRequest
             * @param originalListener
             */
            @Override
            public void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                           ActionListener<PublishWithJoinResponse> originalListener) {
                assert publishRequest.getAcceptedState() == clusterChangedEvent.state() : "state got switched on us";
                assert transportService.getThreadPool().getThreadContext().isSystemContext();
                final ActionListener<PublishWithJoinResponse> responseActionListener;

                // 发布的目标就是将集群最新的状态 通知给集群中所有节点  但是只要满足 > 1/2 的条件此时就认为发布成功了  并且自身也算在内
                if (destination.equals(nodes.getLocalNode())) {
                    // if publishing to self, use original request instead (see currentPublishRequestToSelf for explanation)
                    final PublishRequest previousRequest = currentPublishRequestToSelf.getAndSet(publishRequest);
                    // we might override an in-flight publication to self in case where we failed as master and became master again,
                    // and the new publication started before the previous one completed (which fails anyhow because of higher current term)
                    assert previousRequest == null || previousRequest.getAcceptedState().term() < publishRequest.getAcceptedState().term();
                    responseActionListener = new ActionListener<PublishWithJoinResponse>() {

                        /**
                         * 收到响应结果时 将currentPublishRequestToSelf置空
                         * @param publishWithJoinResponse
                         */
                        @Override
                        public void onResponse(PublishWithJoinResponse publishWithJoinResponse) {
                            currentPublishRequestToSelf.compareAndSet(publishRequest, null); // only clean-up our mess
                            originalListener.onResponse(publishWithJoinResponse);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            currentPublishRequestToSelf.compareAndSet(publishRequest, null); // only clean-up our mess
                            originalListener.onFailure(e);
                        }
                    };
                } else {
                    responseActionListener = originalListener;
                }
                // 如果本次要求发送全版本数据 或者发往一个新节点 都只能发送全量数据
                if (sendFullVersion || !previousState.nodes().nodeExists(destination)) {
                    logger.trace("sending full cluster state version {} to {}", newState.version(), destination);
                    PublicationTransportHandler.this.sendFullClusterState(newState, serializedStates, destination, responseActionListener);
                } else {
                    logger.trace("sending cluster state diff for version {} to {}", newState.version(), destination);
                    PublicationTransportHandler.this.sendClusterStateDiff(newState, serializedDiffs, serializedStates, destination,
                        responseActionListener);
                }
            }

            /**
             * 提交跟发布啥关系
             * @param destination
             * @param applyCommitRequest
             * @param responseActionListener
             */
            @Override
            public void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommitRequest,
                                        ActionListener<TransportResponse.Empty> responseActionListener) {
                assert transportService.getThreadPool().getThreadContext().isSystemContext();
                transportService.sendRequest(destination, COMMIT_STATE_ACTION_NAME, applyCommitRequest, stateRequestOptions,
                    new TransportResponseHandler<TransportResponse.Empty>() {

                        @Override
                        public TransportResponse.Empty read(StreamInput in) {
                            return TransportResponse.Empty.INSTANCE;
                        }

                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            responseActionListener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            responseActionListener.onFailure(exp);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.GENERIC;
                        }
                    });
            }
        };
    }

    /**
     * 通过传输层将请求 发往目标节点
     * @param clusterState
     * @param bytes
     * @param node
     * @param responseActionListener
     * @param sendDiffs
     * @param serializedStates
     */
    private void sendClusterStateToNode(ClusterState clusterState, BytesReference bytes, DiscoveryNode node,
                                        ActionListener<PublishWithJoinResponse> responseActionListener, boolean sendDiffs,
                                        Map<Version, BytesReference> serializedStates) {
        try {
            final BytesTransportRequest request = new BytesTransportRequest(bytes, node.getVersion());
            final Consumer<TransportException> transportExceptionHandler = exp -> {
                // 当尝试发送部分数据失败时 选择发送全量数据
                if (sendDiffs && exp.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
                    logger.debug("resending full cluster state to node {} reason {}", node, exp.getDetailedMessage());
                    sendFullClusterState(clusterState, serializedStates, node, responseActionListener);
                } else {
                    logger.debug(() -> new ParameterizedMessage("failed to send cluster state to {}", node), exp);
                    responseActionListener.onFailure(exp);
                }
            };

            /**
             * 该对象负责接收 publish的结果 应该就是增加投票箱票数  当publish 发布到超过半数节点时 代表写入成功
             */
            final TransportResponseHandler<PublishWithJoinResponse> publishWithJoinResponseHandler =
                new TransportResponseHandler<PublishWithJoinResponse>() {

                    @Override
                    public PublishWithJoinResponse read(StreamInput in) throws IOException {
                        return new PublishWithJoinResponse(in);
                    }

                    @Override
                    public void handleResponse(PublishWithJoinResponse response) {
                        responseActionListener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        transportExceptionHandler.accept(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                };
            transportService.sendRequest(node, PUBLISH_STATE_ACTION_NAME, request, stateRequestOptions, publishWithJoinResponseHandler);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("error sending cluster state to {}", node), e);
            responseActionListener.onFailure(e);
        }
    }

    /**
     *
     * @param clusterState
     * @param previousState
     * @param discoveryNodes
     * @param sendFullVersion   存在多少个版本的node 就要生成多少数据流
     * @param serializedStates   每个版本对应的clusterState都是全量数据
     * @param serializedDiffs   每个版本对应的clusterState都是增量数据
     */
    private static void buildDiffAndSerializeStates(ClusterState clusterState, ClusterState previousState, DiscoveryNodes discoveryNodes,
                                                    boolean sendFullVersion, Map<Version, BytesReference> serializedStates,
                                                    Map<Version, BytesReference> serializedDiffs) {
        Diff<ClusterState> diff = null;
        // 遍历此时集群中所有的节点
        for (DiscoveryNode node : discoveryNodes) {
            try {
                // 如果发送所有版本 就要检查所有node.version   否则就是只检查新增节点的版本
                if (sendFullVersion || !previousState.nodes().nodeExists(node)) {
                    // 发现了之前没使用过的版本 加入数据
                    if (serializedStates.containsKey(node.getVersion()) == false) {
                        serializedStates.put(node.getVersion(), serializeFullClusterState(clusterState, node.getVersion()));
                    }
                } else {
                    // will send a diff
                    if (diff == null) {
                        diff = clusterState.diff(previousState);
                    }
                    // 真的之前就存在的node 只写入增量数据
                    if (serializedDiffs.containsKey(node.getVersion()) == false) {
                        serializedDiffs.put(node.getVersion(), serializeDiffClusterState(diff, node.getVersion()));
                    }
                }
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize cluster state for publishing to node {}", e, node);
            }
        }
    }

    /**
     * 通过传输层将结果发往目标节点
     * @param clusterState
     * @param serializedStates
     * @param node
     * @param responseActionListener
     */
    private void sendFullClusterState(ClusterState clusterState, Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, ActionListener<PublishWithJoinResponse> responseActionListener) {
        BytesReference bytes = serializedStates.get(node.getVersion());
        if (bytes == null) {
            try {
                bytes = serializeFullClusterState(clusterState, node.getVersion());
                serializedStates.put(node.getVersion(), bytes);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to serialize cluster state before publishing it to node {}", node), e);
                responseActionListener.onFailure(e);
                return;
            }
        }
        sendClusterStateToNode(clusterState, bytes, node, responseActionListener, false, serializedStates);
    }

    private void sendClusterStateDiff(ClusterState clusterState,
                                      Map<Version, BytesReference> serializedDiffs, Map<Version, BytesReference> serializedStates,
                                      DiscoveryNode node, ActionListener<PublishWithJoinResponse> responseActionListener) {
        final BytesReference bytes = serializedDiffs.get(node.getVersion());
        assert bytes != null : "failed to find serialized diff for node " + node + " of version [" + node.getVersion() + "]";
        sendClusterStateToNode(clusterState, bytes, node, responseActionListener, true, serializedStates);
    }


    /**
     * 将集群状态 以及版本号信息写入到一个数据流中
     * @param clusterState
     * @param nodeVersion
     * @return
     * @throws IOException
     */
    public static BytesReference serializeFullClusterState(ClusterState clusterState, Version nodeVersion) throws IOException {
        final BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.COMPRESSOR.streamOutput(bStream)) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(true);
            clusterState.writeTo(stream);
        }
        return bStream.bytes();
    }

    public static BytesReference serializeDiffClusterState(Diff diff, Version nodeVersion) throws IOException {
        final BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.COMPRESSOR.streamOutput(bStream)) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(false);
            diff.writeTo(stream);
        }
        return bStream.bytes();
    }

    /**
     * 收到发布请求代表集群中的master节点此时更新了集群状态 并通知到其他节点
     * @param request
     * @return
     * @throws IOException
     */
    private PublishWithJoinResponse handleIncomingPublishRequest(BytesTransportRequest request) throws IOException {
        // 根据数据量长度判断是否采用了压缩算法
        final Compressor compressor = CompressorFactory.compressor(request.bytes());
        StreamInput in = request.bytes().streamInput();
        try {
            if (compressor != null) {
                in = compressor.streamInput(in);
            }
            in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
            in.setVersion(request.version());
            // If true we received full cluster state - otherwise diffs
            // 如果是true 代表接收到的是全量数据
            if (in.readBoolean()) {
                final ClusterState incomingState;
                try {
                    // 通过数据流还原整个集群状态   因为集群状态本身是一个大对象 所以通常会采用压缩算法
                    incomingState = ClusterState.readFrom(in, transportService.getLocalNode());
                } catch (Exception e){
                    logger.warn("unexpected error while deserializing an incoming cluster state", e);
                    throw e;
                }
                fullClusterStateReceivedCount.incrementAndGet();
                logger.debug("received full cluster state version [{}] with size [{}]", incomingState.version(),
                    request.bytes().length());

                // 根据接收到的集群状态 生成res对象
                final PublishWithJoinResponse response = acceptState(incomingState);
                lastSeenClusterState.set(incomingState);
                return response;
            } else {
                // 根据最近一次获取的集群状态对象 配合增量数据 得到最终结果
                final ClusterState lastSeen = lastSeenClusterState.get();
                if (lastSeen == null) {
                    logger.debug("received diff for but don't have any local cluster state - requesting full state");
                    incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                    throw new IncompatibleClusterStateVersionException("have no local cluster state");
                } else {
                    ClusterState incomingState;
                    try {
                        // 获取变化的数据
                        Diff<ClusterState> diff = ClusterState.readDiffFrom(in, lastSeen.nodes().getLocalNode());
                        incomingState = diff.apply(lastSeen); // might throw IncompatibleClusterStateVersionException
                    } catch (IncompatibleClusterStateVersionException e) {
                        incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                        throw e;
                    } catch (Exception e){
                        logger.warn("unexpected error while deserializing an incoming cluster state", e);
                        throw e;
                    }
                    compatibleClusterStateDiffReceivedCount.incrementAndGet();
                    logger.debug("received diff cluster state version [{}] with uuid [{}], diff size [{}]",
                        incomingState.version(), incomingState.stateUUID(), request.bytes().length());
                    // 同样通过 handlePublishRequest 生成结果
                    final PublishWithJoinResponse response = acceptState(incomingState);
                    lastSeenClusterState.compareAndSet(lastSeen, incomingState);
                    return response;
                }
            }
        } finally {
            IOUtils.close(in);
        }
    }

    /**
     * 接收到某个集群状态后 生成结果对象
     * @param incomingState
     * @return
     */
    private PublishWithJoinResponse acceptState(ClusterState incomingState) {
        // if the state is coming from the current node, use original request instead (see currentPublishRequestToSelf for explanation)
        // 在发布过程中 还会发送给自己
        if (transportService.getLocalNode().equals(incomingState.nodes().getMasterNode())) {
            final PublishRequest publishRequest = currentPublishRequestToSelf.get();
            // 只有master节点将请求发往本地时 才会设置该标识
            if (publishRequest == null || publishRequest.getAcceptedState().stateUUID().equals(incomingState.stateUUID()) == false) {
                throw new IllegalStateException("publication to self failed for " + publishRequest);
            } else {
                return handlePublishRequest.apply(publishRequest);
            }
        }
        return handlePublishRequest.apply(new PublishRequest(incomingState));
    }
}
