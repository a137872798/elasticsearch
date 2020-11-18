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
package org.elasticsearch.action.admin.cluster.repositories.cleanup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Repository cleanup action for repository implementations based on {@link BlobStoreRepository}.
 * <p>
 * The steps taken by the repository cleanup operation are as follows:
 * <ol>
 * <li>Check that there are no running repository cleanup, snapshot create, or snapshot delete actions
 * and add an entry for the repository that is to be cleaned up to {@link RepositoryCleanupInProgress}</li>
 * <li>Run cleanup actions on the repository. Note, these are executed exclusively on the master node.
 * For the precise operations execute see {@link BlobStoreRepository#cleanup}</li>
 * <li>Remove the entry in {@link RepositoryCleanupInProgress} in the first step.</li>
 * </ol>
 * <p>
 * On master failover during the cleanup operation it is simply removed from the cluster state. This is safe because the logic in
 * {@link BlobStoreRepository#cleanup} ensures that the repository state id has not changed between creation of the cluster state entry
 * and any delete/write operations. TODO: This will not work if we also want to clean up at the shard level as those will involve writes
 * as well as deletes.
 * 清理仓库的任务只能在leader节点执行,为什么??? 还有在Engine中并没有看到 有关仓库的存储动作
 */
public final class TransportCleanupRepositoryAction extends TransportMasterNodeAction<CleanupRepositoryRequest,
    CleanupRepositoryResponse> {

    private static final Logger logger = LogManager.getLogger(TransportCleanupRepositoryAction.class);

    private final RepositoriesService repositoriesService;

    private final SnapshotsService snapshotsService;

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Inject
    public TransportCleanupRepositoryAction(TransportService transportService, ClusterService clusterService,
                                            RepositoriesService repositoriesService, SnapshotsService snapshotsService,
                                            ThreadPool threadPool, ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(CleanupRepositoryAction.NAME, transportService, clusterService, threadPool, actionFilters,
            CleanupRepositoryRequest::new, indexNameExpressionResolver);
        this.repositoriesService = repositoriesService;
        this.snapshotsService = snapshotsService;
        // We add a state applier that will remove any dangling repository cleanup actions on master failover.
        // This is safe to do since cleanups will increment the repository state id before executing any operations to prevent concurrent
        // operations from corrupting the repository. This is the same safety mechanism used by snapshot deletes.
        // 当集群状态发生变化时 要进行处理
        clusterService.addStateApplier(event -> {
            // 因为本对象在所有节点上都会创建   当某个节点升级/降级 都会对执行任务造成影响
            // 当本节点晋升成leader后触发相关逻辑
            if (event.localNodeMaster() && event.previousState().nodes().isLocalNodeElectedMaster() == false) {
                // 存储属于额外的模块 它的相关信息是存储在 state.custom 内的
                final RepositoryCleanupInProgress repositoryCleanupInProgress = event.state().custom(RepositoryCleanupInProgress.TYPE);
                // 当这个节点变成leader节点时 检测是否有正在执行的清理工作 如果没有 那么就不需要特殊处理了
                // 否则节点角色的变换会影响到之前已经在执行的清理任务
                if (repositoryCleanupInProgress == null || repositoryCleanupInProgress.hasCleanupInProgress() == false) {
                    return;
                }
                clusterService.submitStateUpdateTask("clean up repository cleanup task after master failover",
                    new ClusterStateUpdateTask() {

                        /**
                         * 停止之前的清理工作
                         * @param currentState
                         * @return
                         */
                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            return removeInProgressCleanup(currentState);
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            logger.debug("Removed repository cleanup task [{}] from cluster state", repositoryCleanupInProgress);
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            logger.warn(
                                "Failed to remove repository cleanup task [{}] from cluster state", repositoryCleanupInProgress);
                        }
                    });
            }
        });
    }

    /**
     * 当集群中leader节点发生变化的时候
     * @param currentState
     * @return
     */
    private static ClusterState removeInProgressCleanup(final ClusterState currentState) {
        RepositoryCleanupInProgress cleanupInProgress = currentState.custom(RepositoryCleanupInProgress.TYPE);
        if (cleanupInProgress != null) {
            boolean changed = false;
            // 如果不存在描述cleanup的对象 不做处理 否则将内部信息重置
            if (cleanupInProgress.hasCleanupInProgress()) {
                cleanupInProgress = new RepositoryCleanupInProgress();
                changed = true;
            }
            if (changed) {
                return ClusterState.builder(currentState).putCustom(
                    RepositoryCleanupInProgress.TYPE, cleanupInProgress).build();
            }
        }
        return currentState;
    }

    @Override
    protected CleanupRepositoryResponse read(StreamInput in) throws IOException {
        return new CleanupRepositoryResponse(in);
    }

    /**
     * 在确保当前节点是leader节点后触发该方法
     * @param task
     * @param request
     * @param state
     * @param listener
     */
    @Override
    protected void masterOperation(Task task, CleanupRepositoryRequest request, ClusterState state,
                                   ActionListener<CleanupRepositoryResponse> listener) {
        cleanupRepo(request.name(), ActionListener.map(listener, CleanupRepositoryResponse::new));
    }

    /**
     * 所属的级别是  metadata_read 级别
     * @param request
     * @param state
     * @return
     */
    @Override
    protected ClusterBlockException checkBlock(CleanupRepositoryRequest request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    /**
     * Runs cleanup operations on the given repository.
     * 开始执行清理操作
     *
     * @param repositoryName Repository to clean up
     * @param listener       Listener for cleanup result
     */
    private void cleanupRepo(String repositoryName, ActionListener<RepositoryCleanupResult> listener) {

        // 获取指定要清理的仓库对象
        final Repository repository = repositoriesService.repository(repositoryName);
        if (repository instanceof BlobStoreRepository == false) {
            listener.onFailure(new IllegalArgumentException("Repository [" + repositoryName + "] does not support repository cleanup"));
            return;
        }
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
        // 获取此时最新的数据 并触发监听器   在repository中也有多个文件 也有gen的概念 这里是取出最后一个index-gen文件对应的数据 并转换成实体对象
        repository.getRepositoryData(repositoryDataListener);
        repositoryDataListener.whenComplete(repositoryData -> {
            final long repositoryStateId = repositoryData.getGenId();
            logger.info("Running cleanup operations on repository [{}][{}]", repositoryName, repositoryStateId);

            // 注意此时本节点是leader节点
            clusterService.submitStateUpdateTask("cleanup repository [" + repositoryName + "][" + repositoryStateId + ']',
                new ClusterStateUpdateTask() {

                    private boolean startedCleanup = false;

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        final RepositoryCleanupInProgress repositoryCleanupInProgress =
                            currentState.custom(RepositoryCleanupInProgress.TYPE);
                        // 禁止重复执行清理工作
                        if (repositoryCleanupInProgress != null && repositoryCleanupInProgress.hasCleanupInProgress()) {
                            throw new IllegalStateException(
                                "Cannot cleanup [" + repositoryName + "] - a repository cleanup is already in-progress in ["
                                    + repositoryCleanupInProgress + "]");
                        }
                        // 也不允许存在执行中的快照删除任务
                        SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);
                        if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                            throw new IllegalStateException("Cannot cleanup [" + repositoryName
                                + "] - a snapshot is currently being deleted in [" + deletionsInProgress + "]");
                        }

                        // 快照任务也不允许执行
                        SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                        if (snapshots != null && !snapshots.entries().isEmpty()) {
                            throw new IllegalStateException(
                                "Cannot cleanup [" + repositoryName + "] - a snapshot is currently running in [" + snapshots + "]");
                        }
                        // 此时在 clusterState上开启了一个新的 repositoryCleanup进程
                        return ClusterState.builder(currentState).putCustom(RepositoryCleanupInProgress.TYPE,
                            new RepositoryCleanupInProgress(
                                RepositoryCleanupInProgress.startedEntry(repositoryName, repositoryStateId))).build();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        after(e, null);
                    }

                    /**
                     * 应该是代表本次更新成功提交到超过半数的节点
                     * @param source
                     * @param oldState
                     * @param newState
                     */
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        startedCleanup = true;
                        logger.debug("Initialized repository cleanup in cluster state for [{}][{}]", repositoryName, repositoryStateId);
                        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(listener,
                            // cleanup() 在这里会更新存储数据 写入index-n 文件  已经将最新的数据通知到集群其他节点  并且已经删除了不再被引用的数据
                            l -> blobStoreRepository.cleanup(
                                repositoryStateId,
                                snapshotsService.minCompatibleVersion(
                                    newState.nodes().getMinNodeVersion(), repositoryName, repositoryData, null),
                                ActionListener.wrap(result -> after(null, result), e -> after(e, null)))
                        ));
                    }

                    /**
                     * 当处理结束后触发该方法
                     * @param failure
                     * @param result
                     */
                    private void after(@Nullable Exception failure, @Nullable RepositoryCleanupResult result) {
                        if (failure == null) {
                            logger.debug("Finished repository cleanup operations on [{}][{}]", repositoryName, repositoryStateId);
                        } else {
                            logger.debug(() -> new ParameterizedMessage(
                                "Failed to finish repository cleanup operations on [{}][{}]", repositoryName, repositoryStateId), failure);
                        }
                        assert failure != null || result != null;
                        // 代表清理失败了
                        if (startedCleanup == false) {
                            logger.debug("No cleanup task to remove from cluster state because we failed to start one", failure);
                            listener.onFailure(failure);
                            return;
                        }

                        // 清理成功后还要通知集群其他节点   从清理状态解除    清理状态会产生block吗 ???
                        clusterService.submitStateUpdateTask(
                            "remove repository cleanup task [" + repositoryName + "][" + repositoryStateId + ']',
                            new ClusterStateUpdateTask() {
                                @Override
                                public ClusterState execute(ClusterState currentState) {
                                    return removeInProgressCleanup(currentState);
                                }

                                @Override
                                public void onFailure(String source, Exception e) {
                                    if (failure != null) {
                                        e.addSuppressed(failure);
                                    }
                                    logger.warn(() ->
                                        new ParameterizedMessage("[{}] failed to remove repository cleanup task", repositoryName), e);
                                    listener.onFailure(e);
                                }

                                @Override
                                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                                    if (failure == null) {
                                        logger.info("Done with repository cleanup on [{}][{}] with result [{}]",
                                            repositoryName, repositoryStateId, result);
                                        listener.onResponse(result);
                                    } else {
                                        logger.warn(() -> new ParameterizedMessage(
                                            "Failed to run repository cleanup operations on [{}][{}]",
                                            repositoryName, repositoryStateId), failure);
                                        listener.onFailure(failure);
                                    }
                                }
                            });
                    }
                });
        }, listener::onFailure);
    }
}
