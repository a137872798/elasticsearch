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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The source recovery accepts recovery requests from other peer shards and start the recovery process from this
 * source shard to the target shard.
 * 该节点接收 replicaShard的请求 并从primaryShard拉取数据返回
 */
public class PeerRecoverySourceService extends AbstractLifecycleComponent implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(PeerRecoverySourceService.class);

    public static class Actions {
        public static final String START_RECOVERY = "internal:index/shard/recovery/start_recovery";
    }

    /**
     * 通过传输层服务来发起请求 和返回结果
     * ES内部交互基于TCP 而对用户又暴露了基于HTTP的入口
     */
    private final TransportService transportService;
    private final IndicesService indicesService;
    private final RecoverySettings recoverySettings;
    private final BigArrays bigArrays;

    /**
     * 记录此时正在恢复的分片
     */
    final OngoingRecoveries ongoingRecoveries = new OngoingRecoveries();

    @Inject
    public PeerRecoverySourceService(TransportService transportService, IndicesService indicesService,
                                     RecoverySettings recoverySettings, BigArrays bigArrays) {
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        this.bigArrays = bigArrays;
        transportService.registerRequestHandler(Actions.START_RECOVERY, ThreadPool.Names.GENERIC, StartRecoveryRequest::new,
            new StartRecoveryTransportRequestHandler());
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
        // 在关闭该对象时 会阻塞直到所有req都被处理完
        ongoingRecoveries.awaitEmpty();
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                       Settings indexSettings) {
        if (indexShard != null) {
            ongoingRecoveries.cancel(indexShard, "shard is closed");
        }
    }

    /**
     * 通过req中描述的相关信息  开始传输数据给副本 完成副本的恢复流程
     * @param request
     * @param listener
     */
    private void recover(StartRecoveryRequest request, ActionListener<RecoveryResponse> listener) {
        // 通过指定的index 获取到对应的分片
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard shard = indexService.getShard(request.shardId().id());

        final ShardRouting routingEntry = shard.routingEntry();

        // 首先确保该分片是主分片 并且已经完成了自身的数据恢复
        if (routingEntry.primary() == false || routingEntry.active() == false) {
            throw new DelayRecoveryException("source shard [" + routingEntry + "] is not an active primary");
        }

        // TODO
        if (request.isPrimaryRelocation() && (routingEntry.relocating() == false ||
            routingEntry.relocatingNodeId().equals(request.targetNode().getId()) == false)) {
            logger.debug("delaying recovery of {} as source shard is not marked yet as relocating to {}",
                request.shardId(), request.targetNode());
            throw new DelayRecoveryException("source shard is not marked yet as relocating to [" + request.targetNode() + "]");
        }

        // 在PeerRecoveryTargetService中要记录 哪些副本(重定向后的primary) 正在恢复数据
        // 而在PeerRecoverySourceService中也需要记录 收到了哪些恢复请求
        RecoverySourceHandler handler = ongoingRecoveries.addNewRecovery(request, shard);
        logger.trace("[{}][{}] starting recovery to {}", request.shardId().getIndex().getName(), request.shardId().id(),
            request.targetNode());
        // 开始执行恢复逻辑  当恢复完成时 从相关容器中移除
        handler.recoverToTarget(ActionListener.runAfter(listener, () -> ongoingRecoveries.remove(shard, handler)));
    }

    /**
     * 某个副本请求从主分片拉取数据
     */
    class StartRecoveryTransportRequestHandler implements TransportRequestHandler<StartRecoveryRequest> {
        @Override
        public void messageReceived(final StartRecoveryRequest request, final TransportChannel channel, Task task) throws Exception {
            // res的处理就是标记某个 peerRecovery已经完成 同时打印日志信息
            recover(request, new ChannelActionListener<>(channel, Actions.START_RECOVERY, request));
        }
    }

    // exposed for testing
    final int numberOfOngoingRecoveries() {
        return ongoingRecoveries.ongoingRecoveries.size();
    }

    /**
     * 代表此时正在处理的恢复操作   当前是主分片才会维护该对象
     */
    final class OngoingRecoveries {

        /**
         * 以分片为单位 存储了相关的上下文对象
         */
        private final Map<IndexShard, ShardRecoveryContext> ongoingRecoveries = new HashMap<>();

        @Nullable
        private List<ActionListener<Void>> emptyListeners;

        /**
         * 增加一个正在处理的 恢复操作
         * @param request   本次恢复操作的请求信息
         * @param shard    针对哪个分片发起的恢复操作
         * @return
         */
        synchronized RecoverySourceHandler addNewRecovery(StartRecoveryRequest request, IndexShard shard) {
            assert lifecycle.started();
            // 因为同一个分片 可以有多个副本发起请求 所以这里用列表
            final ShardRecoveryContext shardContext = ongoingRecoveries.computeIfAbsent(shard, s -> new ShardRecoveryContext());
            RecoverySourceHandler handler = shardContext.addNewRecovery(request, shard);
            shard.recoveryStats().incCurrentAsSource();
            return handler;
        }

        synchronized void remove(IndexShard shard, RecoverySourceHandler handler) {
            final ShardRecoveryContext shardRecoveryContext = ongoingRecoveries.get(shard);
            assert shardRecoveryContext != null : "Shard was not registered [" + shard + "]";
            boolean remove = shardRecoveryContext.recoveryHandlers.remove(handler);
            assert remove : "Handler was not registered [" + handler + "]";
            if (remove) {
                shard.recoveryStats().decCurrentAsSource();
            }
            if (shardRecoveryContext.recoveryHandlers.isEmpty()) {
                ongoingRecoveries.remove(shard);
            }
            if (ongoingRecoveries.isEmpty()) {
                if (emptyListeners != null) {
                    final List<ActionListener<Void>> onEmptyListeners = emptyListeners;
                    emptyListeners = null;
                    ActionListener.onResponse(onEmptyListeners, null);
                }
            }
        }

        synchronized void cancel(IndexShard shard, String reason) {
            final ShardRecoveryContext shardRecoveryContext = ongoingRecoveries.get(shard);
            if (shardRecoveryContext != null) {
                final List<Exception> failures = new ArrayList<>();
                for (RecoverySourceHandler handlers : shardRecoveryContext.recoveryHandlers) {
                    try {
                        handlers.cancel(reason);
                    } catch (Exception ex) {
                        failures.add(ex);
                    } finally {
                        shard.recoveryStats().decCurrentAsSource();
                    }
                }
                ExceptionsHelper.maybeThrowRuntimeAndSuppress(failures);
            }
        }

        void awaitEmpty() {
            assert lifecycle.stoppedOrClosed();
            final PlainActionFuture<Void> future;
            synchronized (this) {
                if (ongoingRecoveries.isEmpty()) {
                    return;
                }
                future = new PlainActionFuture<>();
                if (emptyListeners == null) {
                    emptyListeners = new ArrayList<>();
                }
                emptyListeners.add(future);
            }
            FutureUtils.get(future);
        }

        /**
         * 以分片为单位记录所有的恢复请求
         */
        private final class ShardRecoveryContext {

            /**
             * 相同shard下 针对同一source 每个target对应一个handler对象
             * 所以这组映射关系是一个三元组
             */
            final Set<RecoverySourceHandler> recoveryHandlers = new HashSet<>();

            /**
             * Adds recovery source handler.
             * @param request 可以同时处理多个副本(重定向主分片)的请求
             */
            synchronized RecoverySourceHandler addNewRecovery(StartRecoveryRequest request, IndexShard shard) {
                // 每个allocationId 可以理解区分每个分片的唯一id  这里代表不能在同一时间 不应该收到同一个副本的多个请求
                for (RecoverySourceHandler existingHandler : recoveryHandlers) {
                    if (existingHandler.getRequest().targetAllocationId().equals(request.targetAllocationId())) {
                        throw new DelayRecoveryException("recovery with same target already registered, waiting for " +
                            "previous recovery attempt to be cancelled or completed");
                    }
                }
                RecoverySourceHandler handler = createRecoverySourceHandler(request, shard);
                recoveryHandlers.add(handler);
                return handler;
            }

            /**
             * 每个副本的恢复操作都由对应的handler来完成
             * @param request
             * @param shard
             * @return
             */
            private RecoverySourceHandler createRecoverySourceHandler(StartRecoveryRequest request, IndexShard shard) {
                RecoverySourceHandler handler;
                // 负责传输数据
                final RemoteRecoveryTargetHandler recoveryTarget =
                    new RemoteRecoveryTargetHandler(request.recoveryId(), request.shardId(), transportService, bigArrays,
                        request.targetNode(), recoverySettings, throttleTime -> shard.recoveryStats().addThrottleTime(throttleTime));

                // 负责从primary抽取数据
                handler = new RecoverySourceHandler(shard, recoveryTarget, shard.getThreadPool(), request,
                    Math.toIntExact(recoverySettings.getChunkSize().getBytes()), recoverySettings.getMaxConcurrentFileChunks());
                return handler;
            }
        }
    }
}
