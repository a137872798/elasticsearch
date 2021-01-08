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

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.PriorityComparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;

/**
 * This service manages the node allocation of a cluster. For this reason the
 * {@link AllocationService} keeps {@link AllocationDeciders} to choose nodes
 * for shard allocation. This class also manages new nodes joining the cluster
 * and rerouting of shards.
 * 分配服务 该对象决定了分片该分配到哪个节点 并且在合适的时机会将集群内的分片进行重分配 确保每个节点的负载能力尽可能接近
 */
public class AllocationService {

    private static final Logger logger = LogManager.getLogger(AllocationService.class);

    /**
     * 内部包含了一组分配对象 能够判别某个分片是否应该分配在某个node上  基本都会借助到 RoutingAllocation 该对象描述了整个集群下分片的分配位置
     */
    private final AllocationDeciders allocationDeciders;
    private Map<String, ExistingShardsAllocator> existingShardsAllocators;

    /**
     * 该对象定义了分配的逻辑 配合 AllocationDeciders 使用
     */
    private final ShardsAllocator shardsAllocator;
    /**
     * 获取当前集群状态的服务
     */
    private final ClusterInfoService clusterInfoService;

    // only for tests that use the GatewayAllocator as the unique ExistingShardsAllocator
    public AllocationService(AllocationDeciders allocationDeciders, GatewayAllocator gatewayAllocator,
                             ShardsAllocator shardsAllocator, ClusterInfoService clusterInfoService) {
        this(allocationDeciders, shardsAllocator, clusterInfoService);
        setExistingShardsAllocators(Collections.singletonMap(GatewayAllocator.ALLOCATOR_NAME, gatewayAllocator));
    }

    public AllocationService(AllocationDeciders allocationDeciders, ShardsAllocator shardsAllocator,
                             ClusterInfoService clusterInfoService) {
        this.allocationDeciders = allocationDeciders;
        this.shardsAllocator = shardsAllocator;
        this.clusterInfoService = clusterInfoService;
    }

    /**
     * Inject the {@link ExistingShardsAllocator}s to use. May only be called once.
     * 在Node初始化阶段  完成IOC容器的构建后 会将此时已经存在的分片分配器设置到 AllocationService中
     */
    public void setExistingShardsAllocators(Map<String, ExistingShardsAllocator> existingShardsAllocators) {
        assert this.existingShardsAllocators == null : "cannot set allocators " + existingShardsAllocators + " twice";
        assert existingShardsAllocators.isEmpty() == false : "must add at least one ExistingShardsAllocator";
        this.existingShardsAllocators = Collections.unmodifiableMap(existingShardsAllocators);
    }

    /**
     * Applies the started shards. Note, only initializing ShardRouting instances that exist in the routing table should be
     * provided as parameter and no duplicates should be contained.
     * <p>
     * If the same instance of the {@link ClusterState} is returned, then no change has been made.</p>
     * @param clusterState
     * @param startedShards 这组分片会被切换成start状态
     */
    public ClusterState applyStartedShards(ClusterState clusterState, List<ShardRouting> startedShards) {
        assert assertInitialized();
        if (startedShards.isEmpty()) {
            return clusterState;
        }
        // 将ClusterState中的 routingTable对象 转换成以node为单位存储分片的 RoutingNodes对象
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();

        // 该对象描述了此时所有分片的分配信息
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState,
            clusterInfoService.getClusterInfo(), currentNanoTime());
        // as starting a primary relocation target can reinitialize replica shards, start replicas first
        startedShards = new ArrayList<>(startedShards);
        // 将本次修改成启动状态的所有分片中的主分片排到前面
        startedShards.sort(Comparator.comparing(ShardRouting::primary));

        // 将所有分片修改成启动状态  同时触发 observer等逻辑
        applyStartedShards(allocation, startedShards);

        for (final ExistingShardsAllocator allocator : existingShardsAllocators.values()) {
            allocator.applyStartedShards(startedShards, allocation);
        }
        assert RoutingNodes.assertShardStats(allocation.routingNodes());

        // 生成格式化字符串
        String startedShardsAsString
            = firstListElementsToCommaDelimitedString(startedShards, s -> s.shardId().toString(), logger.isDebugEnabled());
        // 根据此时的 RoutingAllocation 更新ClusterState
        return buildResultAndLogHealthChange(clusterState, allocation, "shards started [" + startedShardsAsString + "]");
    }

    /**
     * 根据 RoutingAllocation的最新数据 更新ClusterState
     * @param oldState   之前的集群状态
     * @param allocation   如果某些shard发生了状态的变化是可以通过allocation 观测到的
     * @param reason
     * @return
     */
    protected ClusterState buildResultAndLogHealthChange(ClusterState oldState, RoutingAllocation allocation, String reason) {
        ClusterState newState = buildResult(oldState, allocation);

        // 打印日志
        logClusterHealthStateChange(
            new ClusterStateHealth(oldState),
            new ClusterStateHealth(newState),
            reason
        );

        return newState;
    }

    /**
     * 通过allocation内的信息修改集群状态    因为某个分片的状态变化会在allocation中反映出来
     * @param oldState
     * @param allocation
     * @return
     */
    private ClusterState buildResult(ClusterState oldState, RoutingAllocation allocation) {
        final RoutingTable oldRoutingTable = oldState.routingTable();
        final RoutingNodes newRoutingNodes = allocation.routingNodes();

        // routingNodes中记录了分片最新的状态  通过它来还原 routingTable
        final RoutingTable newRoutingTable = new RoutingTable.Builder().updateNodes(oldRoutingTable.version(), newRoutingNodes).build();

        // 使用新的路由表 更新metadata
        // 同时在之前reroute使得一些分片发生变化时 allocation对象内部有一个监听器会记录这些变化的信息 这时用那些更新信息来更新 metadata 主要就是更新primaryVersion 以及 in-sync
        final Metadata newMetadata = allocation.updateMetadataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetadata); // validates the routing table is coherent with the cluster state metadata

        // 更新集群状态对象
        final ClusterState.Builder newStateBuilder = ClusterState.builder(oldState)
            .routingTable(newRoutingTable)
            .metadata(newMetadata);

        // TODO restore 只针对 recoverySource 为snapshot的
        final RestoreInProgress restoreInProgress = allocation.custom(RestoreInProgress.TYPE);
        if (restoreInProgress != null) {
            // 更新 restoreInProgress
            RestoreInProgress updatedRestoreInProgress = allocation.updateRestoreInfoWithRoutingChanges(restoreInProgress);
            if (updatedRestoreInProgress != restoreInProgress) {
                ImmutableOpenMap.Builder<String, ClusterState.Custom> customsBuilder = ImmutableOpenMap.builder(allocation.getCustoms());
                customsBuilder.put(RestoreInProgress.TYPE, updatedRestoreInProgress);
                newStateBuilder.customs(customsBuilder.build());
            }
        }
        return newStateBuilder.build();
    }

    // Used for testing
    public ClusterState applyFailedShard(ClusterState clusterState, ShardRouting failedShard, boolean markAsStale) {
        return applyFailedShards(clusterState, singletonList(new FailedShard(failedShard, null, null, markAsStale)), emptyList());
    }

    // Used for testing
    public ClusterState applyFailedShards(ClusterState clusterState, List<FailedShard> failedShards) {
        return applyFailedShards(clusterState, failedShards, emptyList());
    }

    /**
     * Applies the failed shards. Note, only assigned ShardRouting instances that exist in the routing table should be
     * provided as parameter. Also applies a list of allocation ids to remove from the in-sync set for shard copies for which there
     * are no routing entries in the routing table.
     *
     * <p>
     * If the same instance of ClusterState is returned, then no change has been made.</p>
     * @param clusterState   当前集群状态
     * @param failedShards   某些处理失败的分片会上报给leader节点  这些 failedShard对象就是处理失败的分片
     * @param staleShards    某些处理失败的分片已经无法从集群路由表中找到 但是依然能够在 indexMetadata.inSync容器中找到 那么就会标记成过期分片
     * 处理失败或者过期的分片
     */
    public ClusterState applyFailedShards(final ClusterState clusterState, final List<FailedShard> failedShards,
                                          final List<StaleShard> staleShards) {
        assert assertInitialized();
        // 没有需要处理的分片
        if (staleShards.isEmpty() && failedShards.isEmpty()) {
            return clusterState;
        }

        // 处理 staleShards 的数据  也就是从 indexMetadata中移除 inSync
        ClusterState tmpState = IndexMetadataUpdater.removeStaleIdsWithoutRoutings(clusterState, staleShards, logger);

        RoutingNodes routingNodes = getMutableRoutingNodes(tmpState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        long currentNanoTime = currentNanoTime();

        // 该对象包含了分配需要的全部信息 相当于一个context
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, tmpState,
            clusterInfoService.getClusterInfo(), currentNanoTime);

        // 这里在遍历所有失败的分片
        for (FailedShard failedShardEntry : failedShards) {

            // 获取该失败的分片的路由信息
            ShardRouting shardToFail = failedShardEntry.getRoutingEntry();
            // 获取分片相关的索引元数据
            IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardToFail.shardId().getIndex());

            // 标记这个分片无法分配到这个node上  每次进行reroute时 这些node会被跳过
            allocation.addIgnoreShardForNode(shardToFail.shardId(), shardToFail.currentNodeId());
            // failing a primary also fails initializing replica shards, re-resolve ShardRouting

            ShardRouting failedShard = routingNodes.getByAllocationId(shardToFail.shardId(), shardToFail.allocationId().getId());
            if (failedShard != null) {
                // 先忽略这种情况吧  allocationId 应该是唯一的不会出现这种情况
                if (failedShard != shardToFail) {
                    logger.trace("{} shard routing modified in an earlier iteration (previous: {}, current: {})",
                        shardToFail.shardId(), shardToFail, failedShard);
                }

                // unassignedInfo 中会记录分片全部的失败记录
                int failedAllocations = failedShard.unassignedInfo() != null ? failedShard.unassignedInfo().getNumFailedAllocations() : 0;

                // 通过unassignedInfo 甚至可以追溯之前分配失败的所有node
                final Set<String> failedNodeIds;
                if (failedShard.unassignedInfo() != null) {
                    failedNodeIds = new HashSet<>(failedShard.unassignedInfo().getFailedNodeIds().size() + 1);
                    failedNodeIds.addAll(failedShard.unassignedInfo().getFailedNodeIds());
                    failedNodeIds.add(failedShard.currentNodeId());
                } else {
                    failedNodeIds = Collections.emptySet();
                }
                String message = "failed shard on node [" + shardToFail.currentNodeId() + "]: " + failedShardEntry.getMessage();

                // 更新 unassignedInfo
                UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, message,
                    failedShardEntry.getFailure(), failedAllocations + 1, currentNanoTime, System.currentTimeMillis(), false,
                    AllocationStatus.NO_ATTEMPT, failedNodeIds);

                // 本分片是否需要同时从 inSync 中移除
                if (failedShardEntry.markAsStale()) {
                    allocation.removeAllocationId(failedShard);
                }
                logger.warn(new ParameterizedMessage("failing shard [{}]", failedShardEntry), failedShardEntry.getFailure());
                // 将该分片标记成失败 在内部会做一些状态的转换   变化会记录到 changes中  在最后需要生成 clusterState时 会借助内部的信息
                // 内部可能会出现副本升级成主分片 主分片降级   以及 重分配分片取消  原分片删除等等特殊情况
                routingNodes.failShard(logger, failedShard, unassignedInfo, indexMetadata, allocation.changes());
            } else {
                logger.trace("{} shard routing failed in an earlier iteration (routing: {})", shardToFail.shardId(), shardToFail);
            }
        }

        // 先使用增强的分配器进行处理  这里主要是之前拉取到的节点数据不再可靠 进行清理 并在之后reroute时尝试重新拉取
        for (final ExistingShardsAllocator allocator : existingShardsAllocators.values()) {
            allocator.applyFailedShards(failedShards, allocation);
        }

        // 对unassigned分片进行分配
        reroute(allocation);
        // 生成描述信息
        String failedShardsAsString
            = firstListElementsToCommaDelimitedString(failedShards, s -> s.getRoutingEntry().shardId().toString(), logger.isDebugEnabled());
        return buildResultAndLogHealthChange(clusterState, allocation, "shards failed [" + failedShardsAsString + "]");
    }

    /**
     * unassigned an shards that are associated with nodes that are no longer part of the cluster, potentially promoting replicas
     * if needed.
     * 分离已死的节点
     */
    public ClusterState disassociateDeadNodes(ClusterState clusterState, boolean reroute, String reason) {
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState,
            clusterInfoService.getClusterInfo(), currentNanoTime());

        // first, clear from the shards any node id they used to belong to that is now dead
        // 某些节点会发现已经不存在于 DiscoveryNodes的 dataNodes中   触发 shardFailed
        disassociateDeadNodes(allocation);

        // 只要有分片发生了变化 那么就更新集群state对象
        if (allocation.routingNodesChanged()) {
            clusterState = buildResult(clusterState, allocation);
        }

        // 如果需要对剩余的分片进行重路由
        if (reroute) {
            return reroute(clusterState, reason);
        } else {
            return clusterState;
        }
    }

    /**
     * Checks if the are replicas with the auto-expand feature that need to be adapted.
     * Returns an updated cluster state if changes were necessary, or the identical cluster if no changes were required.
     * 根据当前集群状态 判断需要创建多少副本
     */
    public ClusterState adaptAutoExpandReplicas(ClusterState clusterState) {
        // 该对象描述了当前集群所有分片的分配情况
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState,
            clusterInfoService.getClusterInfo(), currentNanoTime());


        // 推算每个索引合适的副本数量   key对应副本数量 value对应使用该副本数量的所有索引
        final Map<Integer, List<String>> autoExpandReplicaChanges =
            AutoExpandReplicas.getAutoExpandReplicaChanges(clusterState.metadata(), allocation);
        // 代表自适应机制被关闭 不会自动创建副本   不对clusterState进行修改
        if (autoExpandReplicaChanges.isEmpty()) {
            return clusterState;
        } else {
            // 更新 ClusterState信息
            final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.routingTable());
            final Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
            for (Map.Entry<Integer, List<String>> entry : autoExpandReplicaChanges.entrySet()) {
                final int numberOfReplicas = entry.getKey();
                final String[] indices = entry.getValue().toArray(new String[entry.getValue().size()]);
                // we do *not* update the in sync allocation ids as they will be removed upon the first index
                // operation which make these copies stale
                // 更新每个索引此时的副本数
                routingTableBuilder.updateNumberOfReplicas(numberOfReplicas, indices);
                // 在元数据内更新相关索引的副本数量
                metadataBuilder.updateNumberOfReplicas(numberOfReplicas, indices);
                // update settings version for each index
                // 只要index级别的元数据发生了变化 就要更新版本号
                for (final String index : indices) {
                    final IndexMetadata indexMetadata = metadataBuilder.get(index);
                    final IndexMetadata.Builder indexMetadataBuilder =
                            new IndexMetadata.Builder(indexMetadata).settingsVersion(1 + indexMetadata.getSettingsVersion());
                    metadataBuilder.put(indexMetadataBuilder);
                }
                logger.info("updating number_of_replicas to [{}] for indices {}", numberOfReplicas, indices);
            }
            final ClusterState fixedState = ClusterState.builder(clusterState).routingTable(routingTableBuilder.build())
                .metadata(metadataBuilder).build();
            assert AutoExpandReplicas.getAutoExpandReplicaChanges(fixedState.metadata(), allocation).isEmpty();
            return fixedState;
        }
    }

    /**
     * Removes delay markers from unassigned shards based on current time stamp.
     * 尝试将达到时限的分片的延时标记移除
     */
    private void removeDelayMarkers(RoutingAllocation allocation) {
        // 迭代所有unassigned 分片  每个自适应创建的副本一开始都处于 unassigned状态
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = allocation.routingNodes().unassigned().iterator();
        final Metadata metadata = allocation.metadata();
        while (unassignedIterator.hasNext()) {
            ShardRouting shardRouting = unassignedIterator.next();
            UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
            // 只针对设置了延时标识的分片
            if (unassignedInfo.isDelayed()) {
                // 计算还有多少剩余时间
                final long newComputedLeftDelayNanos = unassignedInfo.getRemainingDelay(allocation.getCurrentNanoTime(),
                    metadata.getIndexSafe(shardRouting.index()).getSettings());
                // 代表已满足延时条件
                if (newComputedLeftDelayNanos == 0) {
                    // 移除延时标识
                    unassignedIterator.updateUnassigned(new UnassignedInfo(unassignedInfo.getReason(), unassignedInfo.getMessage(),
                        unassignedInfo.getFailure(), unassignedInfo.getNumFailedAllocations(), unassignedInfo.getUnassignedTimeInNanos(),
                        unassignedInfo.getUnassignedTimeInMillis(), false, unassignedInfo.getLastAllocationStatus(),
                        unassignedInfo.getFailedNodeIds()), shardRouting.recoverySource(), allocation.changes());
                }
            }
        }
    }

    /**
     * Reset failed allocation counter for unassigned shards
     * 将每个未分配分片的 unassignedInfo的 failed数量置零
     */
    private void resetFailedAllocationCounter(RoutingAllocation allocation) {
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = allocation.routingNodes().unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            ShardRouting shardRouting = unassignedIterator.next();
            UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
            unassignedIterator.updateUnassigned(new UnassignedInfo(unassignedInfo.getNumFailedAllocations() > 0 ?
                UnassignedInfo.Reason.MANUAL_ALLOCATION : unassignedInfo.getReason(), unassignedInfo.getMessage(),
                unassignedInfo.getFailure(), 0, unassignedInfo.getUnassignedTimeInNanos(),
                unassignedInfo.getUnassignedTimeInMillis(), unassignedInfo.isDelayed(),
                unassignedInfo.getLastAllocationStatus(), Collections.emptySet()), shardRouting.recoverySource(), allocation.changes());
        }
    }

    /**
     * Internal helper to cap the number of elements in a potentially long list for logging.
     *
     * @param elements  The elements to log. May be any non-null list. Must not be null.
     * @param formatter A function that can convert list elements to a String. Must not be null.
     * @param <T>       The list element type.
     * @return A comma-separated string of the first few elements.
     */
    public static <T> String firstListElementsToCommaDelimitedString(List<T> elements, Function<T, String> formatter,
                                                                     boolean isDebugEnabled) {
        final int maxNumberOfElements = 10;
        if (isDebugEnabled || elements.size() <= maxNumberOfElements) {
            return elements.stream().map(formatter).collect(Collectors.joining(", "));
        } else {
            return elements.stream().limit(maxNumberOfElements).map(formatter).collect(Collectors.joining(", "))
                + ", ... [" + elements.size() + " items in total]";
        }
    }

    /**
     * 根据一组分配相关的命令 对当前所有分片进行重路由
     * @param clusterState
     * @param commands
     * @param explain
     * @param retryFailed
     * @return
     */
    public CommandsResult reroute(final ClusterState clusterState, AllocationCommands commands, boolean explain, boolean retryFailed) {
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // we don't shuffle the unassigned shards here, to try and get as close as possible to
        // a consistent result of the effect the commands have on the routing
        // this allows systems to dry run the commands, see the resulting cluster state, and act on it
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState,
            clusterInfoService.getClusterInfo(), currentNanoTime());
        // don't short circuit deciders, we want a full explanation
        allocation.debugDecision(true);
        // we ignore disable allocation, because commands are explicit
        allocation.ignoreDisable(true);

        // 将失败次数清零
        if (retryFailed) {
            resetFailedAllocationCounter(allocation);
        }

        // TODO 挨个执行命令 并将返回的 Explanation 组合在一起并返回
        RoutingExplanations explanations = commands.execute(allocation, explain);
        // we revert the ignore disable flag, since when rerouting, we want the original setting to take place
        allocation.ignoreDisable(false);
        // the assumption is that commands will move / act on shards (or fail through exceptions)
        // so, there will always be shard "movements", so no need to check on reroute
        reroute(allocation);
        return new CommandsResult(explanations, buildResultAndLogHealthChange(clusterState, allocation, "reroute commands"));
    }

    /**
     * Reroutes the routing table based on the live nodes.
     * <p>
     * If the same instance of ClusterState is returned, then no change has been made.
     * @param clusterState
     * 根据当前最新的集群状态 进行重路由 包括了自动创建副本以及为副本分配位置的逻辑
     */
    public ClusterState reroute(ClusterState clusterState, String reason) {
        // 自适应条件当前的副本数量  一个隐含的好处就是只需要创建主分片 并启动完成后  通过调用reroute 就可以自动创建副本
        ClusterState fixedClusterState = adaptAutoExpandReplicas(clusterState);

        // 新创建的副本此时都处于 unassigned状态
        RoutingNodes routingNodes = getMutableRoutingNodes(fixedClusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        // 打乱内部未分配的分片副本
        routingNodes.unassigned().shuffle();

        // 将执行分配相关的参数包装成该对象
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, fixedClusterState,
            clusterInfoService.getClusterInfo(), currentNanoTime());

        // 针对 unassigned的分片进行分配  同时为那些处于start状态的分片选择更合适的节点(relocate)
        reroute(allocation);

        // 没有任何分片发生变化 直接返回
        if (fixedClusterState == clusterState && allocation.routingNodesChanged() == false) {
            return clusterState;
        }
        // 代表集群中分片的分配状态发生了变化
        return buildResultAndLogHealthChange(clusterState, allocation, reason);
    }

    private void logClusterHealthStateChange(ClusterStateHealth previousStateHealth, ClusterStateHealth newStateHealth, String reason) {
        ClusterHealthStatus previousHealth = previousStateHealth.getStatus();
        ClusterHealthStatus currentHealth = newStateHealth.getStatus();
        if (!previousHealth.equals(currentHealth)) {
            logger.info(new ESLogMessage("Cluster health status changed from [{}] to [{}] (reason: [{}]).")
                                    .argAndField("previous.health", previousHealth)
                                    .argAndField("current.health", currentHealth)
                                    .argAndField("reason", reason));

        }
    }

    private boolean hasDeadNodes(RoutingAllocation allocation) {
        for (RoutingNode routingNode : allocation.routingNodes()) {
            if (allocation.nodes().getDataNodes().containsKey(routingNode.nodeId()) == false) {
                return true;
            }
        }
        return false;
    }

    /**
     * 对处于unassigned的分片进行分配   以及尝试对一些处于运作状态的分片进行relocate
     * @param allocation
     */
    private void reroute(RoutingAllocation allocation) {
        assert hasDeadNodes(allocation) == false : "dead nodes should be explicitly cleaned up. See disassociateDeadNodes";
        assert AutoExpandReplicas.getAutoExpandReplicaChanges(allocation.metadata(), allocation).isEmpty() :
            "auto-expand replicas out of sync with number of nodes in the cluster";
        assert assertInitialized();

        // 某些处于 unassigned的分片 可以设置延时标识 未超时的分片在本轮中将不会被分配
        removeDelayMarkers(allocation);

        // 这里可以自定义前置分配器  默认使用 gatewayAllocator进行分配 而在本轮无法处理的分片就会交由  shardsAllocator进行兜底处理
        allocateExistingUnassignedShards(allocation);  // try to allocate existing shard copies first
        // 兜底分配策略  除了对一些unassigned进行分配外 一些已经分配好的节点会根据集群中节点的负载情况 进行relocate
        shardsAllocator.allocate(allocation);
        assert RoutingNodes.assertShardStats(allocation.routingNodes());
    }

    /**
     * 处理所有未分配的分片
     * @param allocation
     */
    private void allocateExistingUnassignedShards(RoutingAllocation allocation) {
        // 所有分片会按照 index的优先级进行排序
        allocation.routingNodes().unassigned().sort(PriorityComparator.getAllocationComparator(allocation)); // sort for priority ordering

        // 这是分配器 这里允许在处理前增加一些逻辑
        for (final ExistingShardsAllocator existingShardsAllocator : existingShardsAllocators.values()) {
            // 针对ES 内置的分配器  GatewayAllocator来说 做的是清理缓存的工作
            existingShardsAllocator.beforeAllocation(allocation);
        }

        // 优先对主分片进行分配
        final RoutingNodes.UnassignedShards.UnassignedIterator primaryIterator = allocation.routingNodes().unassigned().iterator();
        while (primaryIterator.hasNext()) {
            final ShardRouting shardRouting = primaryIterator.next();
            if (shardRouting.primary()) {
                // 通过 index信息找到对应的 Allocator 并进行分配
                getAllocatorForShard(shardRouting, allocation).allocateUnassigned(shardRouting, allocation, primaryIterator);
            }
        }

        // 在完成了primary的分配后  开始replicate的分配前 执行的钩子
        // 这里是寻找某些shard是否被分配在了不合适的node (考核的标准就是在该node上与 primary所在的node数据同步率是否太小， 如果是的话会取消本次的恢复操作)
        // 当然本操作也是异步的
        for (final ExistingShardsAllocator existingShardsAllocator : existingShardsAllocators.values()) {
            existingShardsAllocator.afterPrimariesBeforeReplicas(allocation);
        }

        // 开始为副本进行分配  逻辑与 主分片不同
        final RoutingNodes.UnassignedShards.UnassignedIterator replicaIterator = allocation.routingNodes().unassigned().iterator();
        while (replicaIterator.hasNext()) {
            final ShardRouting shardRouting = replicaIterator.next();
            if (shardRouting.primary() == false) {
                getAllocatorForShard(shardRouting, allocation).allocateUnassigned(shardRouting, allocation, replicaIterator);
            }
        }
    }

    /**
     * 分离已死的节点
     * @param allocation
     */
    private void disassociateDeadNodes(RoutingAllocation allocation) {
        for (Iterator<RoutingNode> it = allocation.routingNodes().mutableIterator(); it.hasNext(); ) {
            RoutingNode node = it.next();
            // 当某个节点已经不存在于 dataNodes列表时  这个节点被认为是已死的节点
            if (allocation.nodes().getDataNodes().containsKey(node.nodeId())) {
                // its a live node, continue
                continue;
            }
            // now, go over all the shards routing on the node, and fail them
            // 生成某个节点的所有分片
            for (ShardRouting shardRouting : node.copyShards()) {
                final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
                // 获取超时时间 如果存在 就生成一个延迟对象
                boolean delayed = INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(indexMetadata.getSettings()).nanos() > 0;
                UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, "node_left [" + node.nodeId() + "]",
                    null, 0, allocation.getCurrentNanoTime(), System.currentTimeMillis(), delayed, AllocationStatus.NO_ATTEMPT,
                    Collections.emptySet());
                // 将它的所有分片都标记成失败
                allocation.routingNodes().failShard(logger, shardRouting, unassignedInfo, indexMetadata, allocation.changes());
            }
            // its a dead node, remove it, note, its important to remove it *after* we apply failed shard
            // since it relies on the fact that the RoutingNode exists in the list of nodes
            it.remove();
        }
    }

    /**
     * 将目标分片转换成启动状态 同时触发监听器
     * @param routingAllocation  包含了为分片分配时需要的所有信息
     * @param startedShardEntries   本次需要转换成start的所有分片
     */
    private void applyStartedShards(RoutingAllocation routingAllocation, List<ShardRouting> startedShardEntries) {
        assert startedShardEntries.isEmpty() == false : "non-empty list of started shard entries expected";
        RoutingNodes routingNodes = routingAllocation.routingNodes();
        for (ShardRouting startedShard : startedShardEntries) {
            assert startedShard.initializing() : "only initializing shards can be started";
            assert routingAllocation.metadata().index(startedShard.shardId().getIndex()) != null :
                "shard started for unknown index (shard entry: " + startedShard + ")";
            assert startedShard == routingNodes.getByAllocationId(startedShard.shardId(), startedShard.allocationId().getId()) :
                "shard routing to start does not exist in routing table, expected: " + startedShard + " but was: " +
                    routingNodes.getByAllocationId(startedShard.shardId(), startedShard.allocationId().getId());

            routingNodes.startShard(logger, startedShard, routingAllocation.changes());
        }
    }

    /**
     * Create a mutable {@link RoutingNodes}. This is a costly operation so this must only be called once!
     * 根据当前集群状态生成一个新的 RoutingNodes  该对象是将分片以node为单位进行划分的
     */
    private RoutingNodes getMutableRoutingNodes(ClusterState clusterState) {
        return new RoutingNodes(clusterState, false);
    }

    /** override this to control time based decisions during allocation */
    protected long currentNanoTime() {
        return System.nanoTime();
    }

    /**
     * TODO  existingShardsAllocators 相关的先忽略
     */
    public void cleanCaches() {
        assert assertInitialized();
        existingShardsAllocators.values().forEach(ExistingShardsAllocator::cleanCaches);
    }

    public int getNumberOfInFlightFetches() {
        assert assertInitialized();
        return existingShardsAllocators.values().stream().mapToInt(ExistingShardsAllocator::getNumberOfInFlightFetches).sum();
    }

    /**
     * 生成描述信息
     * @param shardRouting
     * @param allocation
     * @return
     */
    public ShardAllocationDecision explainShardAllocation(ShardRouting shardRouting, RoutingAllocation allocation) {
        assert allocation.debugDecision();
        AllocateUnassignedDecision allocateDecision
            = shardRouting.unassigned() ? explainUnassignedShardAllocation(shardRouting, allocation) : AllocateUnassignedDecision.NOT_TAKEN;
        if (allocateDecision.isDecisionTaken()) {
            return new ShardAllocationDecision(allocateDecision, MoveDecision.NOT_TAKEN);
        } else {
            return shardsAllocator.decideShardAllocation(shardRouting, allocation);
        }
    }

    /**
     * 代表未分配的决策信息
     * @param shardRouting
     * @param routingAllocation
     * @return
     */
    private AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting shardRouting, RoutingAllocation routingAllocation) {
        assert shardRouting.unassigned();
        assert routingAllocation.debugDecision();
        assert assertInitialized();
        // 通过相关配置项 获取目标分片的index对应的分配者
        final ExistingShardsAllocator existingShardsAllocator = getAllocatorForShard(shardRouting, routingAllocation);
        // 通过分配者 获取决策信息
        final AllocateUnassignedDecision decision
            = existingShardsAllocator.explainUnassignedShardAllocation(shardRouting, routingAllocation);
        if (decision.isDecisionTaken()) {
            return decision;
        }
        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    /**
     * 以index为单位 相关的配置中可以指定使用的 allocator
     * 默认使用 GatewayAllocator
     * @param shardRouting   与某个分片对应
     * @param routingAllocation 这个对象可以获取到所有分片的分配信息
     * @return
     */
    private ExistingShardsAllocator getAllocatorForShard(ShardRouting shardRouting, RoutingAllocation routingAllocation) {
        assert assertInitialized();
        // 获取分配对象名
        final String allocatorName = ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.get(
            routingAllocation.metadata().getIndexSafe(shardRouting.index()).getSettings());

        // 查找allocator
        final ExistingShardsAllocator existingShardsAllocator = existingShardsAllocators.get(allocatorName);
        return existingShardsAllocator != null ? existingShardsAllocator : new NotFoundAllocator(allocatorName);
    }

    private boolean assertInitialized() {
        assert existingShardsAllocators != null: "must have set allocators first";
        return true;
    }

    private static class NotFoundAllocator implements ExistingShardsAllocator {
        private final String allocatorName;

        private NotFoundAllocator(String allocatorName) {
            this.allocatorName = allocatorName;
        }

        @Override
        public void beforeAllocation(RoutingAllocation allocation) {
        }

        @Override
        public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {
        }

        @Override
        public void allocateUnassigned(ShardRouting shardRouting, RoutingAllocation allocation,
                                       UnassignedAllocationHandler unassignedAllocationHandler) {
            unassignedAllocationHandler.removeAndIgnore(AllocationStatus.NO_VALID_SHARD_COPY, allocation.changes());
        }

        @Override
        public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation allocation) {
            assert unassignedShard.unassigned();
            assert allocation.debugDecision();
            final List<NodeAllocationResult> nodeAllocationResults = new ArrayList<>(allocation.nodes().getSize());
            for (DiscoveryNode discoveryNode : allocation.nodes()) {
                nodeAllocationResults.add(new NodeAllocationResult(discoveryNode, null, allocation.decision(Decision.NO,
                    "allocator_plugin", "finding the previous copies of this shard requires an allocator called [%s] but " +
                        "that allocator was not found; perhaps the corresponding plugin is not installed",
                    allocatorName)));
            }
            return AllocateUnassignedDecision.no(AllocationStatus.NO_VALID_SHARD_COPY, nodeAllocationResults);
        }

        @Override
        public void cleanCaches() {
        }

        @Override
        public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {
        }

        @Override
        public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {
        }

        @Override
        public int getNumberOfInFlightFetches() {
            return 0;
        }
    }

    /**
     * this class is used to describe results of applying a set of
     * {@link org.elasticsearch.cluster.routing.allocation.command.AllocationCommand}
     */
    public static class CommandsResult {

        private final RoutingExplanations explanations;

        private final ClusterState clusterState;

        /**
         * Creates a new {@link CommandsResult}
         * @param explanations Explanation for the reroute actions
         * @param clusterState Resulting cluster state
         */
        private CommandsResult(RoutingExplanations explanations, ClusterState clusterState) {
            this.clusterState = clusterState;
            this.explanations = explanations;
        }

        /**
         * Get the explanation of this result
         */
        public RoutingExplanations explanations() {
            return explanations;
        }

        /**
         * the resulting cluster state, after the commands were applied
         */
        public ClusterState getClusterState() {
            return clusterState;
        }
    }
}
