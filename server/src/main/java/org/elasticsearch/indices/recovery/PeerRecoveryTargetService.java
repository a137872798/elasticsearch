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

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.indices.recovery.RecoveriesCollection.RecoveryRef;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * The recovery target handles recoveries of peer shards of the shard+node to recover to.
 * <p>
 * Note, it can be safely assumed that there will only be a single recovery per shard (index+id) and
 * not several of them (since we don't allocate several shard replicas to the same node).
 * 当需要从其他节点拉取数据以恢复本地数据时 会使用到该服务
 */
public class PeerRecoveryTargetService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(PeerRecoveryTargetService.class);

    /**
     * 拉取一些描述信息的指令
     */
    public static class Actions {
        public static final String FILES_INFO = "internal:index/shard/recovery/filesInfo";
        public static final String FILE_CHUNK = "internal:index/shard/recovery/file_chunk";
        public static final String CLEAN_FILES = "internal:index/shard/recovery/clean_files";
        public static final String TRANSLOG_OPS = "internal:index/shard/recovery/translog_ops";
        public static final String PREPARE_TRANSLOG = "internal:index/shard/recovery/prepare_translog";
        public static final String FINALIZE = "internal:index/shard/recovery/finalize";
        public static final String HANDOFF_PRIMARY_CONTEXT = "internal:index/shard/recovery/handoff_primary_context";
    }

    private final ThreadPool threadPool;

    /**
     * 可以与远端节点通信
     */
    private final TransportService transportService;

    private final RecoverySettings recoverySettings;
    /**
     * 可以提交集群状态变化的任务 会通知到其他节点
     */
    private final ClusterService clusterService;

    /**
     * 管理所有的target对象
     */
    private final RecoveriesCollection onGoingRecoveries;

    /**
     * 在Node中被初始化  维护每个需要从primary拉取数据的 replica Shard 的恢复情况
     * @param threadPool
     * @param transportService
     * @param recoverySettings
     * @param clusterService
     */
    public PeerRecoveryTargetService(ThreadPool threadPool, TransportService transportService,
            RecoverySettings recoverySettings, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
        this.onGoingRecoveries = new RecoveriesCollection(logger, threadPool);

        // 主分片会存在一个通知副本，本次要恢复哪些文件的动作
        transportService.registerRequestHandler(Actions.FILES_INFO, ThreadPool.Names.GENERIC, RecoveryFilesInfoRequest::new,
            new FilesInfoRequestHandler());
        // 这一步操作是接收 primaryShard发送的数据片段
        transportService.registerRequestHandler(Actions.FILE_CHUNK, ThreadPool.Names.GENERIC, RecoveryFileChunkRequest::new,
            new FileChunkTransportRequestHandler());
        // 当文件传输完成 并且生成了该副本对应的续约对象 并发布到分片所有节点上后
        // 会将除了最新快照外的所有文件删除  包括事务文件 之后会创建一个空的事务文件
        transportService.registerRequestHandler(Actions.CLEAN_FILES, ThreadPool.Names.GENERIC,
            RecoveryCleanFilesRequest::new, new CleanFilesRequestHandler());

        // 在快照文件准备完毕后  在副本开启engine (最接近globalCheckpoint的索引文件)
        transportService.registerRequestHandler(Actions.PREPARE_TRANSLOG, ThreadPool.Names.GENERIC,
                RecoveryPrepareForTranslogOperationsRequest::new, new PrepareForTranslogOperationsRequestHandler());
        // 接收从primary传输过来的事务日志
        transportService.registerRequestHandler(Actions.TRANSLOG_OPS, ThreadPool.Names.GENERIC, RecoveryTranslogOperationsRequest::new,
            new TranslogOperationsRequestHandler());
        transportService.registerRequestHandler(Actions.FINALIZE, ThreadPool.Names.GENERIC, RecoveryFinalizeRecoveryRequest::new,
            new FinalizeRecoveryRequestHandler());
        transportService.registerRequestHandler(
                Actions.HANDOFF_PRIMARY_CONTEXT,
                ThreadPool.Names.GENERIC,
                RecoveryHandoffPrimaryContextRequest::new,
                new HandoffPrimaryContextRequestHandler());
    }

    /**
     * 当某个分片要被关闭时 同时停止所有的相关恢复操作
     * @param shardId
     * @param indexShard The index shard
     * @param indexSettings
     */
    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            onGoingRecoveries.cancelRecoveriesForShard(shardId, "shard closed");
        }
    }

    /**
     * 当 recoverySource 为 PEER 时 会通过该对象进行数据恢复
     * @param indexShard
     * @param sourceNode
     * @param listener
     */
    public void startRecovery(final IndexShard indexShard, final DiscoveryNode sourceNode, final RecoveryListener listener) {
        // create a new recovery status, and process...
        // 每个分片会的数据恢复 会分配一个 恢复任务  并且关联一个recoveryId
        final long recoveryId = onGoingRecoveries.startRecovery(indexShard, sourceNode, listener, recoverySettings.activityTimeout());
        // we fork off quickly here and go async but this is called from the cluster state applier thread too and that can cause
        // assertions to trip if we executed it on the same thread hence we fork off to the generic threadpool.
        // 以上只是创建任务  这里才是开始执行任务
        threadPool.generic().execute(new RecoveryRunner(recoveryId));
    }

    /**
     *
     * @param recoveryId
     * @param reason
     * @param retryAfter  在延迟多长时间后重试
     * @param activityTimeout
     */
    protected void retryRecovery(final long recoveryId, final Throwable reason, TimeValue retryAfter, TimeValue activityTimeout) {
        logger.trace(() -> new ParameterizedMessage(
                "will retry recovery with id [{}] in [{}]", recoveryId, retryAfter), reason);
        retryRecovery(recoveryId, retryAfter, activityTimeout);
    }

    protected void retryRecovery(final long recoveryId, final String reason, TimeValue retryAfter, TimeValue activityTimeout) {
        logger.trace("will retry recovery with id [{}] in [{}] (reason [{}])", recoveryId, retryAfter, reason);
        retryRecovery(recoveryId, retryAfter, activityTimeout);
    }

    private void retryRecovery(final long recoveryId, final TimeValue retryAfter, final TimeValue activityTimeout) {
        RecoveryTarget newTarget = onGoingRecoveries.resetRecovery(recoveryId, activityTimeout);
        if (newTarget != null) {
            threadPool.schedule(new RecoveryRunner(newTarget.recoveryId()), retryAfter, ThreadPool.Names.GENERIC);
        }
    }

    /**
     * 执行恢复任务
     * @param recoveryId
     */
    private void doRecovery(final long recoveryId) {
        final StartRecoveryRequest request;
        final RecoveryState.Timer timer;
        CancellableThreads cancellableThreads;
        // 先找到recoveryId 对应的任务对象
        try (RecoveryRef recoveryRef = onGoingRecoveries.getRecovery(recoveryId)) {
            if (recoveryRef == null) {
                logger.trace("not running recovery with id [{}] - can not find it (probably finished)", recoveryId);
                return;
            }
            final RecoveryTarget recoveryTarget = recoveryRef.target();
            timer = recoveryTarget.state().getTimer();
            // 该对象负责管理一组线程
            cancellableThreads = recoveryTarget.cancellableThreads();
            try {
                // 本次将被恢复数据的分片
                final IndexShard indexShard = recoveryTarget.indexShard();
                // 触发前置钩子
                indexShard.preRecovery();
                assert recoveryTarget.sourceNode() != null : "can not do a recovery without a source node";
                logger.trace("{} preparing shard for peer recovery", recoveryTarget.shardId());
                indexShard.prepareForIndexRecovery();

                // 全局检查点就代表所有分片的数据都已经至少同步到这个位置 那么只有之后的数据 需要从primary上拉取
                // 之前的数据 还是可以从本地恢复
                final long startingSeqNo = indexShard.recoverLocallyUpToGlobalCheckpoint();
                assert startingSeqNo == UNASSIGNED_SEQ_NO || recoveryTarget.state().getStage() == RecoveryState.Stage.TRANSLOG :
                    "unexpected recovery stage [" + recoveryTarget.state().getStage() + "] starting seqno [ " + startingSeqNo + "]";

                // 生成开始恢复数据的请求 并通过传输层发送到master节点上
                request = getStartRecoveryRequest(logger, clusterService.localNode(), recoveryTarget, startingSeqNo);
            } catch (final Exception e) {
                // this will be logged as warning later on...
                logger.trace("unexpected error while preparing shard for peer recovery, failing recovery", e);
                onGoingRecoveries.failRecovery(recoveryId,
                    new RecoveryFailedException(recoveryTarget.state(), "failed to prepare shard for recovery", e), true);
                return;
            }
        }

        // 这个是异常处理器
        Consumer<Exception> handleException = e -> {
            if (logger.isTraceEnabled()) {
                logger.trace(() -> new ParameterizedMessage(
                    "[{}][{}] Got exception on recovery", request.shardId().getIndex().getName(),
                    request.shardId().id()), e);
            }
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof CancellableThreads.ExecutionCancelledException) {
                // this can also come from the source wrapped in a RemoteTransportException
                onGoingRecoveries.failRecovery(recoveryId, new RecoveryFailedException(request,
                    "source has canceled the recovery", cause), false);
                return;
            }
            if (cause instanceof RecoveryEngineException) {
                // unwrap an exception that was thrown as part of the recovery
                cause = cause.getCause();
            }
            // do it twice, in case we have double transport exception
            cause = ExceptionsHelper.unwrapCause(cause);
            if (cause instanceof RecoveryEngineException) {
                // unwrap an exception that was thrown as part of the recovery
                cause = cause.getCause();
            }

            // here, we would add checks against exception that need to be retried (and not removeAndClean in this case)
            // 某些异常是允许发起重试的
            if (cause instanceof IllegalIndexShardStateException || cause instanceof IndexNotFoundException ||
                cause instanceof ShardNotFoundException) {
                // if the target is not ready yet, retry
                retryRecovery(
                    recoveryId,
                    "remote shard not ready",
                    recoverySettings.retryDelayStateSync(),
                    recoverySettings.activityTimeout());
                return;
            }

            // 应该代表此时处于繁忙状态之类的 在等待一段时间后进行重试
            if (cause instanceof DelayRecoveryException) {
                retryRecovery(recoveryId, cause, recoverySettings.retryDelayStateSync(),
                    recoverySettings.activityTimeout());
                return;
            }

            if (cause instanceof ConnectTransportException) {
                logger.debug("delaying recovery of {} for [{}] due to networking error [{}]", request.shardId(),
                    recoverySettings.retryDelayNetwork(), cause.getMessage());
                retryRecovery(recoveryId, cause.getMessage(), recoverySettings.retryDelayNetwork(),
                    recoverySettings.activityTimeout());
                return;
            }

            if (cause instanceof AlreadyClosedException) {
                onGoingRecoveries.failRecovery(recoveryId,
                    new RecoveryFailedException(request, "source shard is closed", cause), false);
                return;
            }

            // 会发送 分片启动失败的信息到 leader
            onGoingRecoveries.failRecovery(recoveryId, new RecoveryFailedException(request, e), true);
        };

        try {
            logger.trace("{} starting recovery from {}", request.shardId(), request.sourceNode());
            // 通过这个统一的线程管理对象执行任务
            // cancellableThreads对外暴露了一个方法 能够关闭内部所有正在维护的线程
            cancellableThreads.executeIO(() ->
                // we still execute under cancelableThreads here to ensure we interrupt any blocking call to the network if any
                // on the underlying transport. It's unclear if we need this here at all after moving to async execution but
                // the issues that a missing call to this could cause are sneaky and hard to debug. If we don't need it on this
                // call we can potentially remove it altogether which we should do it in a major release only with enough
                // time to test. This shoudl be done for 7.0 if possible

                // 请求从 primaryShard拉取数据 恢复replica的action为 START_RECOVERY   目标节点就是 primaryShard所在节点
                transportService.sendRequest(request.sourceNode(), PeerRecoverySourceService.Actions.START_RECOVERY, request,
                    // 处理recoveryRes
                    new TransportResponseHandler<RecoveryResponse>() {
                        @Override
                        public void handleResponse(RecoveryResponse recoveryResponse) {
                            final TimeValue recoveryTime = new TimeValue(timer.time());
                            // do this through ongoing recoveries to remove it from the collection
                            // 触发recovery后置钩子
                            onGoingRecoveries.markRecoveryAsDone(recoveryId);
                            if (logger.isTraceEnabled()) {
                                StringBuilder sb = new StringBuilder();
                                sb.append('[').append(request.shardId().getIndex().getName()).append(']')
                                    .append('[').append(request.shardId().id()).append("] ");
                                sb.append("recovery completed from ").append(request.sourceNode()).append(", took[").append(recoveryTime)
                                    .append("]\n");
                                sb.append("   phase1: recovered_files [").append(recoveryResponse.phase1FileNames.size()).append("]")
                                    .append(" with total_size of [").append(new ByteSizeValue(recoveryResponse.phase1TotalSize)).append("]")
                                    .append(", took [").append(timeValueMillis(recoveryResponse.phase1Time)).append("], throttling_wait [")
                                    .append(timeValueMillis(recoveryResponse.phase1ThrottlingWaitTime)).append(']').append("\n");
                                sb.append("         : reusing_files   [").append(recoveryResponse.phase1ExistingFileNames.size())
                                    .append("] with total_size of [").append(new ByteSizeValue(recoveryResponse.phase1ExistingTotalSize))
                                    .append("]\n");
                                sb.append("   phase2: start took [").append(timeValueMillis(recoveryResponse.startTime)).append("]\n");
                                sb.append("         : recovered [").append(recoveryResponse.phase2Operations).append("]")
                                    .append(" transaction log operations")
                                    .append(", took [").append(timeValueMillis(recoveryResponse.phase2Time)).append("]")
                                    .append("\n");
                                logger.trace("{}", sb);
                            } else {
                                logger.debug("{} recovery done from [{}], took [{}]", request.shardId(), request.sourceNode(),
                                    recoveryTime);
                            }
                        }

                        @Override
                        public void handleException(TransportException e) {
                            handleException.accept(e);
                        }

                        @Override
                        public String executor() {
                            // we do some heavy work like refreshes in the response so fork off to the generic threadpool
                            return ThreadPool.Names.GENERIC;
                        }

                        @Override
                        public RecoveryResponse read(StreamInput in) throws IOException {
                            return new RecoveryResponse(in);
                        }
                    })
            );
        } catch (CancellableThreads.ExecutionCancelledException e) {
            logger.trace("recovery cancelled", e);
        } catch (Exception e) {
            // 当产生异常时 通过handle对象进行处理
            handleException.accept(e);
        }
    }

    /**
     * Prepare the start recovery request.
     *
     * @param logger         the logger
     * @param localNode      the local node of the recovery target
     * @param recoveryTarget the target of the recovery
     * @param startingSeqNo  a sequence number that an operation-based peer recovery can start with.
     *                       This is the first operation after the local checkpoint of the safe commit if exists.
     *                       从该seqNo 开始拉取数据  一般就是globalCheckpoint+1
     * @return a start recovery request
     * 向primaryShard 所在的节点发送拉取数据的请求
     */
    public static StartRecoveryRequest getStartRecoveryRequest(Logger logger, DiscoveryNode localNode,
                                                               RecoveryTarget recoveryTarget, long startingSeqNo) {
        final StartRecoveryRequest request;
        logger.trace("{} collecting local files for [{}]", recoveryTarget.shardId(), recoveryTarget.sourceNode());

        Store.MetadataSnapshot metadataSnapshot;
        try {
            // 获取本地最新的 commitInfo 对应的元数据信息
            metadataSnapshot = recoveryTarget.indexShard().snapshotStoreMetadata();
            // Make sure that the current translog is consistent with the Lucene index; otherwise, we have to throw away the Lucene index.
            try {

                // 获取最新的事务日志文件记录的 globalCheckpoint
                final String expectedTranslogUUID = metadataSnapshot.getCommitUserData().get(Translog.TRANSLOG_UUID_KEY);
                // expectedTranslogUUID 主要是校验检查的元数据的文件是否一致
                final long globalCheckpoint = Translog.readGlobalCheckpoint(recoveryTarget.translogLocation(), expectedTranslogUUID);
                assert globalCheckpoint + 1 >= startingSeqNo : "invalid startingSeqNo " + startingSeqNo + " >= " + globalCheckpoint;
            } catch (IOException | TranslogCorruptedException e) {
                logger.warn(new ParameterizedMessage("error while reading global checkpoint from translog, " +
                    "resetting the starting sequence number from {} to unassigned and recovering as if there are none", startingSeqNo), e);
                metadataSnapshot = Store.MetadataSnapshot.EMPTY;
                startingSeqNo = UNASSIGNED_SEQ_NO;
            }
        } catch (final org.apache.lucene.index.IndexNotFoundException e) {
            // happens on an empty folder. no need to log
            assert startingSeqNo == UNASSIGNED_SEQ_NO : startingSeqNo;
            logger.trace("{} shard folder empty, recovering all files", recoveryTarget);
            metadataSnapshot = Store.MetadataSnapshot.EMPTY;
        } catch (final IOException e) {
            if (startingSeqNo != UNASSIGNED_SEQ_NO) {
                logger.warn(new ParameterizedMessage("error while listing local files, resetting the starting sequence number from {} " +
                    "to unassigned and recovering as if there are none", startingSeqNo), e);
                startingSeqNo = UNASSIGNED_SEQ_NO;
            } else {
                logger.warn("error while listing local files, recovering as if there are none", e);
            }
            metadataSnapshot = Store.MetadataSnapshot.EMPTY;
        }
        logger.trace("{} local file count [{}]", recoveryTarget.shardId(), metadataSnapshot.size());

        // 通过相关信息生成req对象
        request = new StartRecoveryRequest(
            recoveryTarget.shardId(),
            recoveryTarget.indexShard().routingEntry().allocationId().getId(),
            recoveryTarget.sourceNode(),
            localNode,
            metadataSnapshot,
            recoveryTarget.state().getPrimary(),
            recoveryTarget.recoveryId(),
            startingSeqNo);
        return request;
    }

    public interface RecoveryListener {
        void onRecoveryDone(RecoveryState state);

        void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure);
    }

    /**
     * 处理 PrepareForTranslog 请求
     */
    class PrepareForTranslogOperationsRequestHandler implements TransportRequestHandler<RecoveryPrepareForTranslogOperationsRequest> {

        @Override
        public void messageReceived(RecoveryPrepareForTranslogOperationsRequest request, TransportChannel channel, Task task) {
            // 确保恢复任务还在执行中   每次调用该方法都会更新 RecoveryTarget.lastAccessTime 这样就不会被自动清理了
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final ActionListener<TransportResponse> listener = new ChannelActionListener<>(channel, Actions.PREPARE_TRANSLOG, request);
                recoveryRef.target().prepareForTranslogOperations(request.totalTranslogOps(),
                    ActionListener.map(listener, nullVal -> TransportResponse.Empty.INSTANCE));
            }
        }
    }

    /**
     * 当整个恢复流程完成后触发
     */
    class FinalizeRecoveryRequestHandler implements TransportRequestHandler<RecoveryFinalizeRecoveryRequest> {

        @Override
        public void messageReceived(RecoveryFinalizeRecoveryRequest request, TransportChannel channel, Task task) throws Exception {
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final ActionListener<TransportResponse> listener = new ChannelActionListener<>(channel, Actions.FINALIZE, request);
                recoveryRef.target().finalizeRecovery(request.globalCheckpoint(), request.trimAboveSeqNo(),
                    ActionListener.map(listener, nullVal -> TransportResponse.Empty.INSTANCE));
            }
        }
    }

    class HandoffPrimaryContextRequestHandler implements TransportRequestHandler<RecoveryHandoffPrimaryContextRequest> {

        @Override
        public void messageReceived(final RecoveryHandoffPrimaryContextRequest request, final TransportChannel channel,
                                    Task task) throws Exception {
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                recoveryRef.target().handoffPrimaryContext(request.primaryContext());
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

    }

    /**
     * 接收primary发送来的事务日志
     */
    class TranslogOperationsRequestHandler implements TransportRequestHandler<RecoveryTranslogOperationsRequest> {

        @Override
        public void messageReceived(final RecoveryTranslogOperationsRequest request, final TransportChannel channel,
                                    Task task) throws IOException {
            try (RecoveryRef recoveryRef =
                     onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {

                // 生成一个集群状态观测对象
                final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
                final RecoveryTarget recoveryTarget = recoveryRef.target();
                final ActionListener<RecoveryTranslogOperationsResponse> listener =
                    new ChannelActionListener<>(channel, Actions.TRANSLOG_OPS, request);
                // 定义了如何处理异常
                final Consumer<Exception> retryOnMappingException = exception -> {
                    // in very rare cases a translog replay from primary is processed before a mapping update on this node
                    // which causes local mapping changes since the mapping (clusterstate) might not have arrived on this node.
                    logger.debug("delaying recovery due to missing mapping changes", exception);
                    // we do not need to use a timeout here since the entire recovery mechanism has an inactivity protection (it will be
                    // canceled)
                    observer.waitForNextChange(new ClusterStateObserver.Listener() {
                        @Override
                        public void onNewClusterState(ClusterState state) {
                            try {
                                messageReceived(request, channel, task);
                            } catch (Exception e) {
                                listener.onFailure(e);
                            }
                        }

                        @Override
                        public void onClusterServiceClose() {
                            listener.onFailure(new ElasticsearchException(
                                "cluster service was closed while waiting for mapping updates"));
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            // note that we do not use a timeout (see comment above)
                            listener.onFailure(new ElasticsearchTimeoutException("timed out waiting for mapping updates " +
                                "(timeout [" + timeout + "])"));
                        }
                    });
                };

                // 获取shard对应索引的元数据信息
                final IndexMetadata indexMetadata = clusterService.state().metadata().index(request.shardId().getIndex());
                final long mappingVersionOnTarget = indexMetadata != null ? indexMetadata.getMappingVersion() : 0L;
                recoveryTarget.indexTranslogOperations(
                        request.operations(),
                        request.totalTranslogOps(),
                        request.maxSeenAutoIdTimestampOnPrimary(),
                        request.maxSeqNoOfUpdatesOrDeletesOnPrimary(),
                        request.retentionLeases(),
                        request.mappingVersionOnPrimary(),
                        ActionListener.wrap(
                                checkpoint -> listener.onResponse(new RecoveryTranslogOperationsResponse(checkpoint)),
                                e -> {
                                    // do not retry if the mapping on replica is at least as recent as the mapping
                                    // that the primary used to index the operations in the request.
                                    if (mappingVersionOnTarget < request.mappingVersionOnPrimary() && e instanceof MapperException) {
                                        retryOnMappingException.accept(e);
                                    } else {
                                        listener.onFailure(e);
                                    }
                                })
                );
            }
        }
    }

    /**
     * 进入到recovery.phase1时 source节点会先获取safeCommit对应的文件信息  并与目标分片此时最新的commit文件进行对比
     * 在生成请求后会通知到目标节点
     */
    class FilesInfoRequestHandler implements TransportRequestHandler<RecoveryFilesInfoRequest> {

        @Override
        public void messageReceived(RecoveryFilesInfoRequest request, TransportChannel channel, Task task) throws Exception {
            // 确保此时该恢复请求还在处理中
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final ActionListener<TransportResponse> listener = new ChannelActionListener<>(channel, Actions.FILES_INFO, request);

                // 实际上就是在target.RecoveryState.File 中插入文件
                recoveryRef.target().receiveFileInfo(
                    request.phase1FileNames, request.phase1FileSizes, request.phase1ExistingFileNames, request.phase1ExistingFileSizes,
                    request.totalTranslogOps, ActionListener.map(listener, nullVal -> TransportResponse.Empty.INSTANCE));
            }
        }
    }

    /**
     * 仅保留本次恢复的索引文件   并且会删除之前所有事务文件 创建一个空的
     */
    class CleanFilesRequestHandler implements TransportRequestHandler<RecoveryCleanFilesRequest> {

        @Override
        public void messageReceived(RecoveryCleanFilesRequest request, TransportChannel channel, Task task) throws Exception {
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final ActionListener<TransportResponse> listener = new ChannelActionListener<>(channel, Actions.CLEAN_FILES, request);
                recoveryRef.target().cleanFiles(request.totalTranslogOps(), request.getGlobalCheckpoint(), request.sourceMetaSnapshot(),
                    ActionListener.map(listener, nullVal -> TransportResponse.Empty.INSTANCE));
            }
        }
    }

    /**
     * 接收primaryShard 发送过来的用于恢复文件的数据片段
     */
    class FileChunkTransportRequestHandler implements TransportRequestHandler<RecoveryFileChunkRequest> {

        // How many bytes we've copied since we last called RateLimiter.pause
        final AtomicLong bytesSinceLastPause = new AtomicLong();

        @Override
        public void messageReceived(final RecoveryFileChunkRequest request, TransportChannel channel, Task task) throws Exception {
            // 确保recoveryRef对象还没有被移除
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final RecoveryTarget recoveryTarget = recoveryRef.target();
                final RecoveryState.Index indexState = recoveryTarget.state().getIndex();
                // 因为 通过网络传输文件流是一个比较耗资源的操作  对端如果被限流了 会将限流时间带过来
                if (request.sourceThrottleTimeInNanos() != RecoveryState.Index.UNKNOWN) {
                    indexState.addSourceThrottling(request.sourceThrottleTimeInNanos());
                }

                // 这里马上会触发大量的写入操作 也需要做限流控制
                RateLimiter rateLimiter = recoverySettings.rateLimiter();
                if (rateLimiter != null) {
                    long bytes = bytesSinceLastPause.addAndGet(request.content().length());
                    if (bytes > rateLimiter.getMinPauseCheckBytes()) {
                        // Time to pause
                        bytesSinceLastPause.addAndGet(-bytes);
                        long throttleTimeInNanos = rateLimiter.pause(bytes);
                        indexState.addTargetThrottling(throttleTimeInNanos);
                        recoveryTarget.indexShard().recoveryStats().addThrottleTime(throttleTimeInNanos);
                    }
                }
                final ActionListener<TransportResponse> listener = new ChannelActionListener<>(channel, Actions.FILE_CHUNK, request);
                // 将数据写入到本地
                recoveryTarget.writeFileChunk(request.metadata(), request.position(), request.content(), request.lastChunk(),
                    request.totalTranslogOps(), ActionListener.map(listener, nullVal -> TransportResponse.Empty.INSTANCE));
            }
        }
    }

    /**
     * 执行恢复任务
     */
    class RecoveryRunner extends AbstractRunnable {

        final long recoveryId;

        RecoveryRunner(long recoveryId) {
            this.recoveryId = recoveryId;
        }

        @Override
        public void onFailure(Exception e) {
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecovery(recoveryId)) {
                if (recoveryRef != null) {
                    logger.error(() -> new ParameterizedMessage("unexpected error during recovery [{}], failing shard", recoveryId), e);
                    onGoingRecoveries.failRecovery(recoveryId,
                            new RecoveryFailedException(recoveryRef.target().state(), "unexpected error", e),
                            true // be safe
                    );
                } else {
                    logger.debug(() -> new ParameterizedMessage(
                            "unexpected error during recovery, but recovery id [{}] is finished", recoveryId), e);
                }
            }
        }

        @Override
        public void doRun() {
            doRecovery(recoveryId);
        }
    }
}
