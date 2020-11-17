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
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportFuture;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 定义如何从远端节点拉取数据 用于恢复某个节点的处理器
 */
public class RemoteRecoveryTargetHandler implements RecoveryTargetHandler {

    private static final Logger logger = LogManager.getLogger(RemoteRecoveryTargetHandler.class);

    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final long recoveryId;
    private final ShardId shardId;
    private final BigArrays bigArrays;
    private final DiscoveryNode targetNode;
    private final RecoverySettings recoverySettings;
    /**
     * 代表一个可重试的指令
     */
    private final Map<Object, RetryableAction<?>> onGoingRetryableActions = ConcurrentCollections.newConcurrentMap();

    // 将2种操作区分开 这样他们在传输层能分配到的channel数就会不同
    private final TransportRequestOptions translogOpsRequestOptions;
    private final TransportRequestOptions fileChunkRequestOptions;

    private final AtomicLong bytesSinceLastPause = new AtomicLong();

    private final Consumer<Long> onSourceThrottle;
    private volatile boolean isCancelled = false;

    public RemoteRecoveryTargetHandler(long recoveryId, ShardId shardId, TransportService transportService, BigArrays bigArrays,
                                       DiscoveryNode targetNode, RecoverySettings recoverySettings, Consumer<Long> onSourceThrottle) {
        this.transportService = transportService;
        this.threadPool = transportService.getThreadPool();
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.bigArrays = bigArrays;
        this.targetNode = targetNode;
        this.recoverySettings = recoverySettings;
        this.onSourceThrottle = onSourceThrottle;

        // 看来这2个Options是一样的
        this.translogOpsRequestOptions = TransportRequestOptions.builder()
                .withType(TransportRequestOptions.Type.RECOVERY)
                .withTimeout(recoverySettings.internalActionLongTimeout())
                .build();
        this.fileChunkRequestOptions = TransportRequestOptions.builder()
                .withType(TransportRequestOptions.Type.RECOVERY)
                .withTimeout(recoverySettings.internalActionTimeout())
                .build();
    }

    /**
     * 在将本地数据传输到远端节点的过程中 分为几个阶段 其中一个阶段就是准备好数据
     * @param totalTranslogOps  total translog operations expected to be sent  预计会发送多少个operation的数据
     * @param listener 当相关操作完成时触发监听器
     */
    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
        final String action = PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG;
        final RecoveryPrepareForTranslogOperationsRequest request =
            new RecoveryPrepareForTranslogOperationsRequest(recoveryId, shardId, totalTranslogOps);
        final TransportRequestOptions options =
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build();
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        // 桥接监听器
        final ActionListener<TransportResponse.Empty> responseListener = ActionListener.map(listener, r -> null);
        executeRetryableAction(action, request, options, responseListener, reader);
    }

    /**
     * 当整个恢复流程结束后 通知到对端
     * @param globalCheckpoint the global checkpoint on the recovery source
     * @param trimAboveSeqNo   The recovery target should erase its existing translog above this sequence number
     *                         from the previous primary terms.
     * @param listener         the listener which will be notified when this method is completed
     */
    @Override
    public void finalizeRecovery(final long globalCheckpoint, final long trimAboveSeqNo, final ActionListener<Void> listener) {
        final String action = PeerRecoveryTargetService.Actions.FINALIZE;
        final RecoveryFinalizeRecoveryRequest request =
            new RecoveryFinalizeRecoveryRequest(recoveryId, shardId, globalCheckpoint, trimAboveSeqNo);
        final TransportRequestOptions options =
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionLongTimeout()).build();
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = ActionListener.map(listener, r -> null);
        executeRetryableAction(action, request, options, responseListener, reader);
    }

    @Override
    public void handoffPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) {
        TransportFuture<TransportResponse.Empty> handler = new TransportFuture<>(EmptyTransportResponseHandler.INSTANCE_SAME);
        transportService.sendRequest(
            targetNode, PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT,
            new RecoveryHandoffPrimaryContextRequest(recoveryId, shardId, primaryContext),
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(), handler);
        handler.txGet();
    }

    /**
     * 在发送完 lucene文件后    事务日志文件也需要同步
     * @param operations                          operations to index
     * @param totalTranslogOps                    current number of total operations expected to be indexed
     * @param maxSeenAutoIdTimestampOnPrimary     the maximum auto_id_timestamp of all append-only requests processed by the primary shard
     * @param maxSeqNoOfDeletesOrUpdatesOnPrimary
     * @param retentionLeases                     the retention leases on the primary
     * @param mappingVersionOnPrimary             the mapping version which is at least as up to date as the mapping version that the
     *                                            primary used to index translog {@code operations} in this request.
     *                                            If the mapping version on the replica is not older this version, we should not retry on
     *                                            {@link org.elasticsearch.index.mapper.MapperException}; otherwise we should wait for a
     *                                            new mapping then retry.
     * @param listener                            a listener which will be notified with the local checkpoint on the target
     */
    @Override
    public void indexTranslogOperations(
            final List<Translog.Operation> operations,  // 该值可能为空列表
            final int totalTranslogOps,
            final long maxSeenAutoIdTimestampOnPrimary,
            final long maxSeqNoOfDeletesOrUpdatesOnPrimary,
            final RetentionLeases retentionLeases,
            final long mappingVersionOnPrimary,
            final ActionListener<Long> listener) {
        final String action = PeerRecoveryTargetService.Actions.TRANSLOG_OPS;
        final RecoveryTranslogOperationsRequest request = new RecoveryTranslogOperationsRequest(
                recoveryId,
                shardId,
                operations,
                totalTranslogOps,
                maxSeenAutoIdTimestampOnPrimary,
                maxSeqNoOfDeletesOrUpdatesOnPrimary,
                retentionLeases,
                mappingVersionOnPrimary);
        final Writeable.Reader<RecoveryTranslogOperationsResponse> reader = RecoveryTranslogOperationsResponse::new;
        final ActionListener<RecoveryTranslogOperationsResponse> responseListener = ActionListener.map(listener, r -> r.localCheckpoint);
        executeRetryableAction(action, request, translogOpsRequestOptions, responseListener, reader);
    }

    /**
     * 将相关信息包装成req对象 并发送到target节点
     * @param phase1FileNames
     * @param phase1FileSizes
     * @param phase1ExistingFileNames
     * @param phase1ExistingFileSizes
     * @param totalTranslogOps
     * @param listener
     */
    @Override
    public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames,
                                List<Long> phase1ExistingFileSizes, int totalTranslogOps, ActionListener<Void> listener) {
        final String action = PeerRecoveryTargetService.Actions.FILES_INFO;
        RecoveryFilesInfoRequest request = new RecoveryFilesInfoRequest(recoveryId, shardId, phase1FileNames, phase1FileSizes,
            phase1ExistingFileNames, phase1ExistingFileSizes, totalTranslogOps);
        final TransportRequestOptions options =
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build();
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = ActionListener.map(listener, r -> null);
        executeRetryableAction(action, request, options, responseListener, reader);
    }

    /**
     * 将primary的全局检查点发送到副本节点上 副本节点根据这个偏移量检测哪些事务文件可以删除
     * @param totalTranslogOps an update number of translog operations that will be replayed later on
     * @param globalCheckpoint the global checkpoint on the primary
     * @param sourceMetadata   meta data of the source store
     * @param listener
     */
    @Override
    public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetadata,
                           ActionListener<Void> listener) {
        final String action = PeerRecoveryTargetService.Actions.CLEAN_FILES;
        final RecoveryCleanFilesRequest request =
            new RecoveryCleanFilesRequest(recoveryId, shardId, sourceMetadata, totalTranslogOps, globalCheckpoint);
        final TransportRequestOptions options =
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build();
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = ActionListener.map(listener, r -> null);
        executeRetryableAction(action, request, options, responseListener, reader);
    }

    /**
     * 将数据流发送到target节点  用于数据同步
     * @param fileMetadata
     * @param position
     * @param content
     * @param lastChunk
     * @param totalTranslogOps
     * @param listener
     */
    @Override
    public void writeFileChunk(StoreFileMetadata fileMetadata, long position, BytesReference content,
                               boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener) {
        // Pause using the rate limiter, if desired, to throttle the recovery
        final long throttleTimeInNanos;
        // always fetch the ratelimiter - it might be updated in real-time on the recovery settings
        // 向传输数据流这种比较耗资源的操作 都是通过 rateLimiter进行限流的
        final RateLimiter rl = recoverySettings.rateLimiter();
        if (rl != null) {
            long bytes = bytesSinceLastPause.addAndGet(content.length());
            if (bytes > rl.getMinPauseCheckBytes()) {
                // Time to pause
                // 代表超标了本次请求需要通过 RateLimiter做限流
                bytesSinceLastPause.addAndGet(-bytes);
                try {
                    throttleTimeInNanos = rl.pause(bytes);
                    onSourceThrottle.accept(throttleTimeInNanos);
                } catch (IOException e) {
                    throw new ElasticsearchException("failed to pause recovery", e);
                }
            } else {
                throttleTimeInNanos = 0;
            }
        } else {
            throttleTimeInNanos = 0;
        }

        final String action = PeerRecoveryTargetService.Actions.FILE_CHUNK;
        final ReleasableBytesStreamOutput output = new ReleasableBytesStreamOutput(content.length(), bigArrays);
        boolean actionStarted = false;
        try {
            content.writeTo(output);
            /* we send estimateTotalOperations with every request since we collect stats on the target and that way we can
             * see how many translog ops we accumulate while copying files across the network. A future optimization
             * would be in to restart file copy again (new deltas) if we have too many translog ops are piling up.
             * 在发送请求的时候 将本次限流时间也传过去了
             */
            final RecoveryFileChunkRequest request = new RecoveryFileChunkRequest(recoveryId, shardId, fileMetadata,
                position, output.bytes(), lastChunk, totalTranslogOps, throttleTimeInNanos);
            final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
            final ActionListener<TransportResponse.Empty> responseListener = ActionListener.map(listener, r -> null);
            final ActionListener<TransportResponse.Empty> releaseListener = ActionListener.runBefore(responseListener, output::close);
            executeRetryableAction(action, request, fileChunkRequestOptions, releaseListener, reader);
            actionStarted = true;
        } catch (IOException e) {
            // Since the content data is buffer in memory, we should never get an exception.
            throw new AssertionError(e);
        } finally {
            if (actionStarted == false) {
                output.close();
            }
        }
    }

    @Override
    public void cancel() {
        isCancelled = true;
        if (onGoingRetryableActions.isEmpty()) {
            return;
        }
        final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("recovery was cancelled");
        // Dispatch to generic as cancellation calls can come on the cluster state applier thread
        threadPool.generic().execute(() -> {
            for (RetryableAction<?> action : onGoingRetryableActions.values()) {
                action.cancel(exception);
            }
            onGoingRetryableActions.clear();
        });
    }

    /**
     * 执行一个在满足某些条件下可以重试的action
     * @param action
     * @param request
     * @param options
     * @param actionListener
     * @param reader
     * @param <T>
     */
    private <T extends TransportResponse> void executeRetryableAction(String action, TransportRequest request,
                                                                      TransportRequestOptions options, ActionListener<T> actionListener,
                                                                      Writeable.Reader<T> reader) {
        final Object key = new Object();
        // 成功触发监听器时 将这个action从retry列表中移除
        final ActionListener<T> removeListener = ActionListener.runBefore(actionListener, () -> onGoingRetryableActions.remove(key));
        final TimeValue initialDelay = TimeValue.timeValueMillis(200);
        final TimeValue timeout = recoverySettings.internalActionRetryTimeout();
        // 定义重试的逻辑
        final RetryableAction<T> retryableAction = new RetryableAction<>(logger, threadPool, initialDelay, timeout, removeListener) {

            /**
             * 向发起恢复数据的节点发起一个 PREPARE_TRANSLOG action
             * @param listener
             */
            @Override
            public void tryAction(ActionListener<T> listener) {
                transportService.sendRequest(targetNode, action, request, options,
                    new ActionListenerResponseHandler<>(listener, reader, ThreadPool.Names.GENERIC));
            }

            /**
             * 异常是否支持重试
             * @param e
             * @return
             */
            @Override
            public boolean shouldRetry(Exception e) {
                return retryableException(e);
            }
        };
        onGoingRetryableActions.put(key, retryableAction);
        retryableAction.run();
        if (isCancelled) {
            retryableAction.cancel(new CancellableThreads.ExecutionCancelledException("recovery was cancelled"));
        }
    }

    private static boolean retryableException(Exception e) {
        if (e instanceof RemoteTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof CircuitBreakingException ||
                cause instanceof EsRejectedExecutionException;
        }
        return false;
    }
}
