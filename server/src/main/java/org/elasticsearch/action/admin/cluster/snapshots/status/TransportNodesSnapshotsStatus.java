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

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Transport action that collects snapshot shard statuses from data nodes
 * 以节点为单位 获取快照信息
 * 现在是站在某个node的角度去处理请求
 */
public class TransportNodesSnapshotsStatus extends TransportNodesAction<TransportNodesSnapshotsStatus.Request,
                                                                        TransportNodesSnapshotsStatus.NodesSnapshotStatus,
                                                                        TransportNodesSnapshotsStatus.NodeRequest,
                                                                        TransportNodesSnapshotsStatus.NodeSnapshotStatus> {

    public static final String ACTION_NAME = SnapshotsStatusAction.NAME + "[nodes]";
    public static final ActionType<NodesSnapshotStatus> TYPE = new ActionType<>(ACTION_NAME, NodesSnapshotStatus::new);

    private final SnapshotShardsService snapshotShardsService;

    @Inject
    public TransportNodesSnapshotsStatus(ThreadPool threadPool, ClusterService clusterService,
                                         TransportService transportService, SnapshotShardsService snapshotShardsService,
                                         ActionFilters actionFilters) {
        super(ACTION_NAME, threadPool, clusterService, transportService, actionFilters,
            Request::new, NodeRequest::new, ThreadPool.Names.GENERIC, NodeSnapshotStatus.class);
        this.snapshotShardsService = snapshotShardsService;
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeSnapshotStatus newNodeResponse(StreamInput in) throws IOException {
        return new NodeSnapshotStatus(in);
    }

    @Override
    protected NodesSnapshotStatus newResponse(Request request, List<NodeSnapshotStatus> responses, List<FailedNodeException> failures) {
        return new NodesSnapshotStatus(clusterService.getClusterName(), responses, failures);
    }

    /**
     * 在当前节点上获取快照信息
     * @param request
     * @param task
     * @return
     */
    @Override
    protected NodeSnapshotStatus nodeOperation(NodeRequest request, Task task) {
        Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> snapshotMapBuilder = new HashMap<>();
        try {
            final String nodeId = clusterService.localNode().getId();
            for (Snapshot snapshot : request.snapshots) {
                // 因为快照是跟着shard走的 而某些shard可能没有出现在该节点 所以shardsStatus可能为空
                Map<ShardId, IndexShardSnapshotStatus> shardsStatus = snapshotShardsService.currentSnapshotShards(snapshot);
                if (shardsStatus == null) {
                    continue;
                }
                Map<ShardId, SnapshotIndexShardStatus> shardMapBuilder = new HashMap<>();
                // 将 IndexShardSnapshotStatus转换成SnapshotIndexShardStatus
                for (Map.Entry<ShardId, IndexShardSnapshotStatus> shardEntry : shardsStatus.entrySet()) {
                    final ShardId shardId = shardEntry.getKey();

                    final IndexShardSnapshotStatus.Copy lastSnapshotStatus = shardEntry.getValue().asCopy();
                    final IndexShardSnapshotStatus.Stage stage = lastSnapshotStatus.getStage();

                    String shardNodeId = null;
                    // 只有还处于运行状态的快照才需要返回node信息
                    if (stage != IndexShardSnapshotStatus.Stage.DONE && stage != IndexShardSnapshotStatus.Stage.FAILURE) {
                        // Store node id for the snapshots that are currently running.
                        shardNodeId = nodeId;
                    }
                    shardMapBuilder.put(shardEntry.getKey(), new SnapshotIndexShardStatus(shardId, lastSnapshotStatus, shardNodeId));
                }
                snapshotMapBuilder.put(snapshot, unmodifiableMap(shardMapBuilder));
            }
            return new NodeSnapshotStatus(clusterService.localNode(), unmodifiableMap(snapshotMapBuilder));
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load metadata", e);
        }
    }

    public static class Request extends BaseNodesRequest<Request> {

        private Snapshot[] snapshots;

        public Request(StreamInput in) throws IOException {
            super(in);
            // This operation is never executed remotely
            throw new UnsupportedOperationException("shouldn't be here");
        }

        public Request(String[] nodesIds) {
            super(nodesIds);
        }

        public Request snapshots(Snapshot[] snapshots) {
            this.snapshots = snapshots;
            return this;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // This operation is never executed remotely
            throw new UnsupportedOperationException("shouldn't be here");
        }
    }

    public static class NodesSnapshotStatus extends BaseNodesResponse<NodeSnapshotStatus> {

        public NodesSnapshotStatus(StreamInput in) throws IOException {
            super(in);
        }

        public NodesSnapshotStatus(ClusterName clusterName, List<NodeSnapshotStatus> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeSnapshotStatus> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeSnapshotStatus::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeSnapshotStatus> nodes) throws IOException {
            out.writeList(nodes);
        }
    }


    public static class NodeRequest extends TransportRequest {

        private List<Snapshot> snapshots;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            snapshots = in.readList(Snapshot::new);
        }

        NodeRequest(TransportNodesSnapshotsStatus.Request request) {
            snapshots = Arrays.asList(request.snapshots);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(snapshots);
        }
    }

    public static class NodeSnapshotStatus extends BaseNodeResponse {

        private Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> status;

        public NodeSnapshotStatus(StreamInput in) throws IOException {
            super(in);
            int numberOfSnapshots = in.readVInt();
            Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> snapshotMapBuilder = new HashMap<>(numberOfSnapshots);
            for (int i = 0; i < numberOfSnapshots; i++) {
                Snapshot snapshot = new Snapshot(in);
                int numberOfShards = in.readVInt();
                Map<ShardId, SnapshotIndexShardStatus> shardMapBuilder = new HashMap<>(numberOfShards);
                for (int j = 0; j < numberOfShards; j++) {
                    ShardId shardId =  new ShardId(in);
                    SnapshotIndexShardStatus status = new SnapshotIndexShardStatus(in);
                    shardMapBuilder.put(shardId, status);
                }
                snapshotMapBuilder.put(snapshot, unmodifiableMap(shardMapBuilder));
            }
            status = unmodifiableMap(snapshotMapBuilder);
        }

        public NodeSnapshotStatus(DiscoveryNode node, Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> status) {
            super(node);
            this.status = status;
        }

        public Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> status() {
            return status;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (status != null) {
                out.writeVInt(status.size());
                for (Map.Entry<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> entry : status.entrySet()) {
                    entry.getKey().writeTo(out);
                    out.writeVInt(entry.getValue().size());
                    for (Map.Entry<ShardId, SnapshotIndexShardStatus> shardEntry : entry.getValue().entrySet()) {
                        shardEntry.getKey().writeTo(out);
                        shardEntry.getValue().writeTo(out);
                    }
                }
            } else {
                out.writeVInt(0);
            }
        }
    }
}
