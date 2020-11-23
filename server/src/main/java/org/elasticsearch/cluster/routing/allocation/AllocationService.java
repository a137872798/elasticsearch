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
     * 设置某种分片分配器
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
     * @param startedShards
     * 将某组分片从初始状态修改成start状态
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

        // 通过相关信息生成 RoutingAllocation对象
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState,
            clusterInfoService.getClusterInfo(), currentNanoTime());
        // as starting a primary relocation target can reinitialize replica shards, start replicas first
        startedShards = new ArrayList<>(startedShards);
        startedShards.sort(Comparator.comparing(ShardRouting::primary));

        // 将所有分片修改成启动状态
        applyStartedShards(allocation, startedShards);

        // TODO 先忽略这种增强逻辑
        for (final ExistingShardsAllocator allocator : existingShardsAllocators.values()) {
            allocator.applyStartedShards(startedShards, allocation);
        }
        assert RoutingNodes.assertShardStats(allocation.routingNodes());

        // 生成格式化字符串
        String startedShardsAsString
            = firstListElementsToCommaDelimitedString(startedShards, s -> s.shardId().toString(), logger.isDebugEnabled());
        return buildResultAndLogHealthChange(clusterState, allocation, "shards started [" + startedShardsAsString + "]");
    }

    /**
     * 根据 RoutingAllocation的最新数据 更新ClusterState
     * @param oldState
     * @param allocation
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
     * 更新此时的集群对象
     * @param oldState
     * @param allocation
     * @return
     */
    private ClusterState buildResult(ClusterState oldState, RoutingAllocation allocation) {
        final RoutingTable oldRoutingTable = oldState.routingTable();
        final RoutingNodes newRoutingNodes = allocation.routingNodes();
        // 通过此时最新的 RoutingNodes 反向生成最新的路由表
        final RoutingTable newRoutingTable = new RoutingTable.Builder().updateNodes(oldRoutingTable.version(), newRoutingNodes).build();

        // allocation 本身会记录所有分片的变化  这时根据这些变化的分片 去更新metadata
        final Metadata newMetadata = allocation.updateMetadataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetadata); // validates the routing table is coherent with the cluster state metadata

        // 更新集群状态对象
        final ClusterState.Builder newStateBuilder = ClusterState.builder(oldState)
            .routingTable(newRoutingTable)
            .metadata(newMetadata);

        // 使用 restoreInProgressUpdater 处理之前监听到的各种分片变化  TODO 先忽略
        final RestoreInProgress restoreInProgress = allocation.custom(RestoreInProgress.TYPE);
        if (restoreInProgress != null) {
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
     * 处理失败或者过期的分片
     */
    public ClusterState applyFailedShards(final ClusterState clusterState, final List<FailedShard> failedShards,
                                          final List<StaleShard> staleShards) {
        assert assertInitialized();
        // 代表不需要处理
        if (staleShards.isEmpty() && failedShards.isEmpty()) {
            return clusterState;
        }
        // 移除掉过期分片  同时更新索引元数据  并更新 ClusterState中的metadata
        ClusterState tmpState = IndexMetadataUpdater.removeStaleIdsWithoutRoutings(clusterState, staleShards, logger);

        // 此时的clusterState还不是最终结果  还需要剔除failed数据
        RoutingNodes routingNodes = getMutableRoutingNodes(tmpState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        long currentNanoTime = currentNanoTime();
        // 生成最新的 RoutingAllocation
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, tmpState,
            clusterInfoService.getClusterInfo(), currentNanoTime);

        for (FailedShard failedShardEntry : failedShards) {
            ShardRouting shardToFail = failedShardEntry.getRoutingEntry();
            // 获取分片相关的索引元数据
            IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardToFail.shardId().getIndex());

            // 因为尝试将分片分配到这个node失败了 所以设置一个 ignore的映射关系到容器中
            allocation.addIgnoreShardForNode(shardToFail.shardId(), shardToFail.currentNodeId());
            // failing a primary also fails initializing replica shards, re-resolve ShardRouting
            // 定位到失败的分片对象
            ShardRouting failedShard = routingNodes.getByAllocationId(shardToFail.shardId(), shardToFail.allocationId().getId());
            if (failedShard != null) {
                if (failedShard != shardToFail) {
                    logger.trace("{} shard routing modified in an earlier iteration (previous: {}, current: {})",
                        shardToFail.shardId(), shardToFail, failedShard);
                }
                // 获取失败次数
                int failedAllocations = failedShard.unassignedInfo() != null ? failedShard.unassignedInfo().getNumFailedAllocations() : 0;
                final Set<String> failedNodeIds;
                if (failedShard.unassignedInfo() != null) {
                    failedNodeIds = new HashSet<>(failedShard.unassignedInfo().getFailedNodeIds().size() + 1);
                    failedNodeIds.addAll(failedShard.unassignedInfo().getFailedNodeIds());
                    failedNodeIds.add(failedShard.currentNodeId());
                } else {
                    failedNodeIds = Collections.emptySet();
                }
                String message = "failed shard on node [" + shardToFail.currentNodeId() + "]: " + failedShardEntry.getMessage();
                UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, message,
                    failedShardEntry.getFailure(), failedAllocations + 1, currentNanoTime, System.currentTimeMillis(), false,
                    AllocationStatus.NO_ATTEMPT, failedNodeIds);

                // 如果失败的分片要标记成丢弃 就添加到 update对象中
                if (failedShardEntry.markAsStale()) {
                    allocation.removeAllocationId(failedShard);
                }
                logger.warn(new ParameterizedMessage("failing shard [{}]", failedShardEntry), failedShardEntry.getFailure());
                // 涉及到一大堆状态的转换 卧槽
                routingNodes.failShard(logger, failedShard, unassignedInfo, indexMetadata, allocation.changes());
            } else {
                logger.trace("{} shard routing failed in an earlier iteration (routing: {})", shardToFail.shardId(), shardToFail);
            }
        }

        // TODO 先忽略
        for (final ExistingShardsAllocator allocator : existingShardsAllocators.values()) {
            allocator.applyFailedShards(failedShards, allocation);
        }

        // 核心就是使用 ShardsAllocator 对所有分片进行分配 (涉及init relocation rebalance)
        reroute(allocation);
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
     */
    public ClusterState adaptAutoExpandReplicas(ClusterState clusterState) {
        // 该对象描述了当前集群所有分片的分配情况
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState,
            clusterInfoService.getClusterInfo(), currentNanoTime());

        // key 对应合适的副本数量 value 对应有哪些索引应该存在这么多数量的副本
        final Map<Integer, List<String>> autoExpandReplicaChanges =
            AutoExpandReplicas.getAutoExpandReplicaChanges(clusterState.metadata(), allocation);
        // 索引副本数没有发生变化
        if (autoExpandReplicaChanges.isEmpty()) {
            return clusterState;
        } else {
            // RoutingTable 中记录了所有分片的分配情况  这里使用原数据快速填充builder
            // autoExpandReplicaChanges 只包含了本次变化的副本
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
     * 移除延时标识
     */
    private void removeDelayMarkers(RoutingAllocation allocation) {
        // 迭代所有unassigned 分片
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = allocation.routingNodes().unassigned().iterator();
        // 该对象内部存储了各种各样的元数据信息
        final Metadata metadata = allocation.metadata();
        while (unassignedIterator.hasNext()) {
            ShardRouting shardRouting = unassignedIterator.next();
            UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
            // 如果这个未分配信息包含延迟标识
            if (unassignedInfo.isDelayed()) {
                // 计算还有多少剩余时间
                final long newComputedLeftDelayNanos = unassignedInfo.getRemainingDelay(allocation.getCurrentNanoTime(),
                    metadata.getIndexSafe(shardRouting.index()).getSettings());
                // 代表已经达到时限
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
     * @param clusterState 此时最新的集群状态
     * 根据此时最新的集群状态(主要是内部的配置) 进行重路由
     */
    public ClusterState reroute(ClusterState clusterState, String reason) {
        // 自适应调整此时的副本数量 并更新到集群状态中  减少的话不需要做处理 但是增加的话 就会有新的副本处于未分配的状态  TODO 那么如何确定副本应该被设置到哪个node上呢 应该是通过那个decision对象
        ClusterState fixedClusterState = adaptAutoExpandReplicas(clusterState);

        // 将此时所有分片的分配情况按照node来划分
        RoutingNodes routingNodes = getMutableRoutingNodes(fixedClusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        // 打乱内部未分配的分片副本
        routingNodes.unassigned().shuffle();
        // 该对象描述的是整个集群下所有primary replicate的分配情况
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, fixedClusterState,
            clusterInfoService.getClusterInfo(), currentNanoTime());
        reroute(allocation);
        // 代表没有副本发生变化
        if (fixedClusterState == clusterState && allocation.routingNodesChanged() == false) {
            return clusterState;
        }
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
     * 根据现有的所有副本的分配情况 进行重路由
     * @param allocation  该对象内部还记录了哪些节点处于 unassigned状态
     */
    private void reroute(RoutingAllocation allocation) {
        assert hasDeadNodes(allocation) == false : "dead nodes should be explicitly cleaned up. See disassociateDeadNodes";
        assert AutoExpandReplicas.getAutoExpandReplicaChanges(allocation.metadata(), allocation).isEmpty() :
            "auto-expand replicas out of sync with number of nodes in the cluster";
        assert assertInitialized();

        // 如果某些unassigned 超过了延时时间 就将延时标记去除
        removeDelayMarkers(allocation);

        // 针对这些未分配的副本进行重分配
        allocateExistingUnassignedShards(allocation);  // try to allocate existing shard copies first
        // 这里为所有分片进行重分配
        shardsAllocator.allocate(allocation);
        assert RoutingNodes.assertShardStats(allocation.routingNodes());
    }

    /**
     * 处理所有未分配的 分片
     * @param allocation
     */
    private void allocateExistingUnassignedShards(RoutingAllocation allocation) {
        allocation.routingNodes().unassigned().sort(PriorityComparator.getAllocationComparator(allocation)); // sort for priority ordering

        // 这是分配器 这里允许在处理前增加一些逻辑
        for (final ExistingShardsAllocator existingShardsAllocator : existingShardsAllocators.values()) {
            existingShardsAllocator.beforeAllocation(allocation);
        }

        final RoutingNodes.UnassignedShards.UnassignedIterator primaryIterator = allocation.routingNodes().unassigned().iterator();
        while (primaryIterator.hasNext()) {
            final ShardRouting shardRouting = primaryIterator.next();
            // 优先为主分片进行分配
            if (shardRouting.primary()) {
                getAllocatorForShard(shardRouting, allocation).allocateUnassigned(shardRouting, allocation, primaryIterator);
            }
        }

        // TODO 先忽略
        for (final ExistingShardsAllocator existingShardsAllocator : existingShardsAllocators.values()) {
            existingShardsAllocator.afterPrimariesBeforeReplicas(allocation);
        }

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
     * @param routingAllocation
     * @param startedShardEntries
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
     * 将此时所有分片(副本) 的分配信息按照node为维度进行划分
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
