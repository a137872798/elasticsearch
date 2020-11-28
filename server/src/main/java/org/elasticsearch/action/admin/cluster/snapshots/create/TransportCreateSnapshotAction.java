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

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for create snapshot operation
 * 创建快照的操作  这个也要求必须在leader节点上执行吗
 * 快照本身是存储在 Repository内部的
 */
public class TransportCreateSnapshotAction extends TransportMasterNodeAction<CreateSnapshotRequest, CreateSnapshotResponse> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportCreateSnapshotAction(TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, SnapshotsService snapshotsService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(CreateSnapshotAction.NAME, transportService, clusterService, threadPool, actionFilters,
              CreateSnapshotRequest::new, indexNameExpressionResolver);
        this.snapshotsService = snapshotsService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateSnapshotResponse read(StreamInput in) throws IOException {
        return new CreateSnapshotResponse(in);
    }

    /**
     * 创建快照的 block级别是 METADATA_READ
     * @param request
     * @param state
     * @return
     */
    @Override
    protected ClusterBlockException checkBlock(CreateSnapshotRequest request, ClusterState state) {
        // We only check metadata block, as we want to snapshot closed indices (which have a read block)
        ClusterBlockException clusterBlockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        if (clusterBlockException != null) {
            return clusterBlockException;
        }
        return null;
    }

    /**
     * 这里就是创建了一个 snapshot.Entry 存储到 clusterState中 并且根据当下所有shard的情况 设置对应的state的状态   但是并没有看到快照真正执行的逻辑 应该是有某个地方在检测
     * 并等待直到所有shard state都处于 started 后才真正开始快照任务吧
     * @param task
     * @param request
     * @param state
     * @param listener
     */
    @Override
    protected void masterOperation(Task task, final CreateSnapshotRequest request, ClusterState state,
                                   final ActionListener<CreateSnapshotResponse> listener) {
        // 本次请求只会在处理完毕时才收到响应结果
        if (request.waitForCompletion()) {
            snapshotsService.executeSnapshot(request, ActionListener.map(listener, CreateSnapshotResponse::new));
        } else {
            // 当成功创建快照任务后 就会触发回调
            snapshotsService.createSnapshot(request, ActionListener.map(listener, snapshot -> new CreateSnapshotResponse()));
        }
    }
}
