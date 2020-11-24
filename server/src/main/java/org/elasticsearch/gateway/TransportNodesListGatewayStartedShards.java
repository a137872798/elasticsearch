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

package org.elasticsearch.gateway;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.ShardStateMetadata;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * This transport action is used to fetch the shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 * 处理往某个节点获取某个shard的数据
 */
public class TransportNodesListGatewayStartedShards extends
    // TransportNodesAction 代表着请求将会发往多个节点
    TransportNodesAction<TransportNodesListGatewayStartedShards.Request,
        TransportNodesListGatewayStartedShards.NodesGatewayStartedShards,
        TransportNodesListGatewayStartedShards.NodeRequest,
        TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> {

    public static final String ACTION_NAME = "internal:gateway/local/started_shards";
    public static final ActionType<NodesGatewayStartedShards> TYPE = new ActionType<>(ACTION_NAME, NodesGatewayStartedShards::new);

    private final Settings settings;
    private final NodeEnvironment nodeEnv;
    private final IndicesService indicesService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportNodesListGatewayStartedShards(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                                  TransportService transportService, ActionFilters actionFilters,
                                                  NodeEnvironment env, IndicesService indicesService,
                                                  NamedXContentRegistry namedXContentRegistry) {
        super(ACTION_NAME, threadPool, clusterService, transportService, actionFilters,
            Request::new, NodeRequest::new, ThreadPool.Names.FETCH_SHARD_STARTED, NodeGatewayStartedShards.class);
        this.settings = settings;
        this.nodeEnv = env;
        this.indicesService = indicesService;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeGatewayStartedShards newNodeResponse(StreamInput in) throws IOException {
        return new NodeGatewayStartedShards(in);
    }

    /**
     * 将通往每个节点的处理结果包装成一个对象  即使访问失败也会记录失败信息
     * @param request The associated request.
     * @param responses All successful node-level responses.
     * @param failures All node-level failures.
     * @return
     */
    @Override
    protected NodesGatewayStartedShards newResponse(Request request,
                                                    List<NodeGatewayStartedShards> responses, List<FailedNodeException> failures) {
        return new NodesGatewayStartedShards(clusterService.getClusterName(), responses, failures);
    }

    /**
     * 定义了在每个节点上如何处理请求
     * @param request
     * @param task
     * @return
     */
    @Override
    protected NodeGatewayStartedShards nodeOperation(NodeRequest request, Task task) {
        try {
            final ShardId shardId = request.getShardId();
            logger.trace("{} loading local shard state info", shardId);
            // 通过最外层元数据文件 还原有关这个shard的所有信息
            ShardStateMetadata shardStateMetadata = ShardStateMetadata.FORMAT.loadLatestState(logger, namedXContentRegistry,
                // 从env中找到该分片所在的目录
                nodeEnv.availableShardPaths(request.shardId));
            if (shardStateMetadata != null) {

                // 代表对应的IndexShard对象还没有创建
                if (indicesService.getShardOrNull(shardId) == null) {
                    // 这里是获取数据文件路径
                    final String customDataPath;
                    if (request.getCustomDataPath() != null) {
                        customDataPath = request.getCustomDataPath();
                    } else {
                        // TODO: Fallback for BWC with older ES versions. Remove once request.getCustomDataPath() always returns non-null
                        final IndexMetadata metadata = clusterService.state().metadata().index(shardId.getIndex());
                        if (metadata != null) {
                            customDataPath = new IndexSettings(metadata, settings).customDataPath();
                        } else {
                            logger.trace("{} node doesn't have meta data for the requests index", shardId);
                            throw new ElasticsearchException("node doesn't have meta data for index " + shardId.getIndex());
                        }
                    }
                    // we don't have an open shard on the store, validate the files on disk are openable
                    // 分片的路径通过这个特殊对象来表示
                    ShardPath shardPath = null;
                    try {
                        // 从env中获取node的数据目录等 然后拼接成数据目录
                        shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, customDataPath);
                        if (shardPath == null) {
                            throw new IllegalStateException(shardId + " no shard path found");
                        }
                        // 这里主要是校验index级别数据是否被损坏的
                        Store.tryOpenIndex(shardPath.resolveIndex(), shardId, nodeEnv::shardLock, logger);
                    } catch (Exception exception) {
                        final ShardPath finalShardPath = shardPath;
                        logger.trace(() -> new ParameterizedMessage(
                                "{} can't open index for shard [{}] in path [{}]",
                                shardId,
                                shardStateMetadata,
                                (finalShardPath != null) ? finalShardPath.resolveIndex() : ""),
                            exception);
                        String allocationId = shardStateMetadata.allocationId != null ?
                            shardStateMetadata.allocationId.getId() : null;
                        return new NodeGatewayStartedShards(clusterService.localNode(), allocationId, shardStateMetadata.primary,
                            exception);
                    }
                }

                logger.debug("{} shard state info found: [{}]", shardId, shardStateMetadata);
                String allocationId = shardStateMetadata.allocationId != null ?
                    shardStateMetadata.allocationId.getId() : null;
                // 在结果中标识了当前节点是否是 shard.primary所在的节点 已经此时分配的id 每次分配动作完成后 就会生成一个id
                return new NodeGatewayStartedShards(clusterService.localNode(), allocationId, shardStateMetadata.primary);
            }
            logger.trace("{} no local shard info found", shardId);
            // 代表在当前node上没有找到shard信息
            // 这种情况是正常的  因为allocator对象在一开始并不明确shard分布的节点情况 所以会往集群内所有节点发起请求
            // TODO 但是ShardStateMetadata 准确的生成时机是什么时候呢???
            return new NodeGatewayStartedShards(clusterService.localNode(), null, false);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load started shards", e);
        }
    }

    /**
     * 该请求用于从shard所在的某个node上拉取元数据
     */
    public static class Request extends BaseNodesRequest<Request> {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
                customDataPath = in.readString();
            } else {
                customDataPath = null;
            }
        }

        public Request(ShardId shardId, String customDataPath, DiscoveryNode[] nodes) {
            super(nodes);
            this.shardId = Objects.requireNonNull(shardId);
            this.customDataPath = Objects.requireNonNull(customDataPath);
        }

        public ShardId shardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
                out.writeString(customDataPath);
            }
        }
    }

    public static class NodesGatewayStartedShards extends BaseNodesResponse<NodeGatewayStartedShards> {

        public NodesGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayStartedShards(ClusterName clusterName, List<NodeGatewayStartedShards> nodes,
                                         List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGatewayStartedShards> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeGatewayStartedShards::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayStartedShards> nodes) throws IOException {
            out.writeList(nodes);
        }
    }


    public static class NodeRequest extends TransportRequest {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
                customDataPath = in.readString();
            } else {
                customDataPath = null;
            }
        }

        public NodeRequest(Request request) {
            this.shardId = Objects.requireNonNull(request.shardId());
            this.customDataPath = Objects.requireNonNull(request.getCustomDataPath());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
                assert customDataPath != null;
                out.writeString(customDataPath);
            }
        }

        public ShardId getShardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }
    }

    public static class NodeGatewayStartedShards extends BaseNodeResponse {

        private final String allocationId;
        private final boolean primary;
        /**
         * 代表在目标节点校验index文件数据时出现异常
         */
        private final Exception storeException;

        public NodeGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
            allocationId = in.readOptionalString();
            primary = in.readBoolean();
            if (in.readBoolean()) {
                storeException = in.readException();
            } else {
                storeException = null;
            }
        }

        public NodeGatewayStartedShards(DiscoveryNode node, String allocationId, boolean primary) {
            this(node, allocationId, primary, null);
        }

        public NodeGatewayStartedShards(DiscoveryNode node, String allocationId, boolean primary, Exception storeException) {
            super(node);
            this.allocationId = allocationId;
            this.primary = primary;
            this.storeException = storeException;
        }

        public String allocationId() {
            return this.allocationId;
        }

        public boolean primary() {
            return this.primary;
        }

        public Exception storeException() {
            return this.storeException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(allocationId);
            out.writeBoolean(primary);
            if (storeException != null) {
                out.writeBoolean(true);
                out.writeException(storeException);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            NodeGatewayStartedShards that = (NodeGatewayStartedShards) o;

            return primary == that.primary && Objects.equals(allocationId, that.allocationId)
                && Objects.equals(storeException, that.storeException);
        }

        @Override
        public int hashCode() {
            int result = (allocationId != null ? allocationId.hashCode() : 0);
            result = 31 * result + (primary ? 1 : 0);
            result = 31 * result + (storeException != null ? storeException.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("NodeGatewayStartedShards[")
               .append("allocationId=").append(allocationId)
               .append(",primary=").append(primary);
            if (storeException != null) {
                buf.append(",storeException=").append(storeException);
            }
            buf.append("]");
            return buf.toString();
        }
    }
}
