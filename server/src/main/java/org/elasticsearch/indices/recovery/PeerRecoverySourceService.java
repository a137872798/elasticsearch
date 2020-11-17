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
 * 该对象负责接收 某个节点的恢复请求 (START_RECOVERY)
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
     * 执行恢复操作
     * @param request
     * @param listener
     */
    private void recover(StartRecoveryRequest request, ActionListener<RecoveryResponse> listener) {
        // 通过指定的index 获取到对应的分片
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard shard = indexService.getShard(request.shardId().id());

        // 获取这个分片的路由信息 记录了这个shard相关的信息分散在哪些节点上 并且在哪个节点上是primary分片 在哪些节点上是副本
        final ShardRouting routingEntry = shard.routingEntry();

        // 作为恢复源的 shard 必须是主分片 因为主分片上的数据才可以确保是最新的 最可靠的
        if (routingEntry.primary() == false || routingEntry.active() == false) {
            throw new DelayRecoveryException("source shard [" + routingEntry + "] is not an active primary");
        }

        // 表明 target成为了该分片重定向后的节点     当检测到路由信息落后时 触发一个延迟方法 等待routing信息同步
        if (request.isPrimaryRelocation() && (routingEntry.relocating() == false ||
            routingEntry.relocatingNodeId().equals(request.targetNode().getId()) == false)) {
            logger.debug("delaying recovery of {} as source shard is not marked yet as relocating to {}",
                request.shardId(), request.targetNode());
            throw new DelayRecoveryException("source shard is not marked yet as relocating to [" + request.targetNode() + "]");
        }

        // 返回针对某个分片进行数据恢复的处理器
        RecoverySourceHandler handler = ongoingRecoveries.addNewRecovery(request, shard);
        logger.trace("[{}][{}] starting recovery to {}", request.shardId().getIndex().getName(), request.shardId().id(),
            request.targetNode());
        // 开始执行恢复逻辑  当恢复完成时 从相关容器中移除
        handler.recoverToTarget(ActionListener.runAfter(listener, () -> ongoingRecoveries.remove(shard, handler)));
    }

    /**
     * 当收到某个节点的 恢复请求时 通过该对象进行处理
     */
    class StartRecoveryTransportRequestHandler implements TransportRequestHandler<StartRecoveryRequest> {
        @Override
        public void messageReceived(final StartRecoveryRequest request, final TransportChannel channel, Task task) throws Exception {
            // 本次 recoveryRes 将会通过channel 发送到target
            recover(request, new ChannelActionListener<>(channel, Actions.START_RECOVERY, request));
        }
    }

    // exposed for testing
    final int numberOfOngoingRecoveries() {
        return ongoingRecoveries.ongoingRecoveries.size();
    }

    /**
     * 代表此时正在处理的恢复操作
     */
    final class OngoingRecoveries {

        /**
         * 以分片为单位 存储了相关的上下文对象
         */
        private final Map<IndexShard, ShardRecoveryContext> ongoingRecoveries = new HashMap<>();

        @Nullable
        private List<ActionListener<Void>> emptyListeners;

        synchronized RecoverySourceHandler addNewRecovery(StartRecoveryRequest request, IndexShard shard) {
            assert lifecycle.started();
            // 在source端要记录此时正在处理哪些shard的恢复
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
         * 在处理数据恢复过程中的上下文对象
         */
        private final class ShardRecoveryContext {

            /**
             * 相同shard下 针对同一source 每个target对应一个handler对象
             * 所以这组映射关系是一个三元组
             */
            final Set<RecoverySourceHandler> recoveryHandlers = new HashSet<>();

            /**
             * Adds recovery source handler.
             * 可能会有多个节点像 shard.primary所在的节点发起请求 申请数据恢复   针对每个节点都会维护对应的handler对象
             */
            synchronized RecoverySourceHandler addNewRecovery(StartRecoveryRequest request, IndexShard shard) {
                // 每个shard分配在某个节点上都有一个唯一的allocationId  代表一个 shard -> targetNode 的映射关系
                // 每当集群状态发生变化时 如果有涉及到集群节点上shard分配关系的变化 那么应该会生成新的allocationId 用于区分
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
             * 针对 source -> target -> shard  的映射关系 生成一个handler对象
             * @param request
             * @param shard
             * @return
             */
            private RecoverySourceHandler createRecoverySourceHandler(StartRecoveryRequest request, IndexShard shard) {
                RecoverySourceHandler handler;
                // 该对象定义了一组接口的实现 而调用时机是由 recoverySourceHandler决定的
                final RemoteRecoveryTargetHandler recoveryTarget =
                    new RemoteRecoveryTargetHandler(request.recoveryId(), request.shardId(), transportService, bigArrays,
                        request.targetNode(), recoverySettings, throttleTime -> shard.recoveryStats().addThrottleTime(throttleTime));
                handler = new RecoverySourceHandler(shard, recoveryTarget, shard.getThreadPool(), request,
                    Math.toIntExact(recoverySettings.getChunkSize().getBytes()), recoverySettings.getMaxConcurrentFileChunks());
                return handler;
            }
        }
    }
}
