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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult.ShardStoreInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.AsyncShardFetch.FetchResult;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The primary shard allocator allocates unassigned primary shards to nodes that hold
 * valid copies of the unassigned primaries.  It does this by iterating over all unassigned
 * primary shards in the routing table and fetching shard metadata from each node in the cluster
 * that holds a copy of the shard.  The shard metadata from each node is compared against the
 * set of valid allocation IDs and for all valid shard copies (if any), the primary shard allocator
 * executes the allocation deciders to chose a copy to assign the primary shard to.
 *
 * Note that the PrimaryShardAllocator does *not* allocate primaries on index creation
 * (see {@link org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator}),
 * nor does it allocate primaries when a primary shard failed and there is a valid replica
 * copy that can immediately be promoted to primary, as this takes place in {@link RoutingNodes#failShard}.
 */
public abstract class PrimaryShardAllocator extends BaseGatewayShardAllocator {
    /**
     * Is the allocator responsible for allocating the given {@link ShardRouting}?
     * 本对象能否为该分片进行分配
     */
    private static boolean isResponsibleFor(final ShardRouting shard) {
        return shard.primary() // must be primary
                && shard.unassigned() // must be unassigned
                // only handle either an existing store or a snapshot recovery
                && (shard.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE     // 对应从本地事务日志进行恢复
                    || shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT);
    }

    /**
     * 决定某个主分片应该分片在哪个节点上   那么判断的依据是什么呢 应该是根据目标节点该分片数据此时已持久化的数据量
     * 应该是包含最新数据的才能做主分片  当执行 bulk任务时 每个req会携带一个id 通过id 或者 routing信息 并经过operationRouting处理后会发往同一个shardId
     * @param unassignedShard  the unassigned shard to allocate
     * @param allocation       the current routing state
     * @param logger           the logger
     * @return
     */
    @Override
    public AllocateUnassignedDecision makeAllocationDecision(final ShardRouting unassignedShard,
                                                             final RoutingAllocation allocation,
                                                             final Logger logger) {
        // 首先判断该分片是否允许进行分配
        if (isResponsibleFor(unassignedShard) == false) {
            // this allocator is not responsible for allocating this shard
            // 该分片不需要分配时 返回该标识
            return AllocateUnassignedDecision.NOT_TAKEN;
        }

        final boolean explain = allocation.debugDecision();

        // 需要每个节点此时shard的元数据 用于作为判断该分片最合适的分配节点的依据
        final FetchResult<NodeGatewayStartedShards> shardState = fetchData(unassignedShard, allocation);
        // 此时还处于等待元数据返回的阶段
        if (shardState.hasData() == false) {
            // 设置一个异步等待的标识
            allocation.setHasPendingAsyncFetch();
            List<NodeAllocationResult> nodeDecisions = null;
            // TODO 忽略debug
            if (explain) {
                nodeDecisions = buildDecisionsForAllNodes(unassignedShard, allocation);
            }
            // 此时无法得出分配结果
            return AllocateUnassignedDecision.no(AllocationStatus.FETCHING_SHARD_DATA, nodeDecisions);
        }

        // don't create a new IndexSetting object for every shard as this could cause a lot of garbage
        // on cluster restart if we allocate a boat load of shards
        final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(unassignedShard.index());

        // 获取此时元数据中维护的所有启动的 start分片    这些分片下发到主分片会生成 ReplicationTracker
        // 也就是在IndexMetadata中的 in-sync 与 数据传输中的 in-sync 的概念是不一样的
        final Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(unassignedShard.id());
        // TODO 先忽略通过快照恢复数据的情况   从目前来看 副本必然是通过PEER进行数据恢复的   在createIndex处理中 可以看到 主分片默认恢复源是Empty 如果此时in-sync队列中有元素 就选择基于本地事务日志进行恢复
        final boolean snapshotRestore = unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT;

        assert inSyncAllocationIds.isEmpty() == false;
        // use in-sync allocation ids to select nodes

        // 为当前主分片选择合适的节点  如果此时没有分片处于 in-sync 那么该result为空
        final NodeShardsResult nodeShardsResult = buildNodeShardsResult(unassignedShard, snapshotRestore,
            allocation.getIgnoreNodes(unassignedShard.shardId()), inSyncAllocationIds, shardState, logger);

        // 如果in-sync 为空  那么该标识为false
        final boolean enoughAllocationsFound = nodeShardsResult.orderedAllocationCandidates.size() > 0;
        logger.debug("[{}][{}]: found {} allocation candidates of {} based on allocation ids: [{}]", unassignedShard.index(),
            unassignedShard.id(), nodeShardsResult.orderedAllocationCandidates.size(), unassignedShard, inSyncAllocationIds);

        // 由于in-sync中没有分片 无法进行分配
        if (enoughAllocationsFound == false) {
            // TODO 先忽略快照恢复
            if (snapshotRestore) {
                // let BalancedShardsAllocator take care of allocating this shard
                logger.debug("[{}][{}]: missing local data, will restore from [{}]",
                             unassignedShard.index(), unassignedShard.id(), unassignedShard.recoverySource());
                return AllocateUnassignedDecision.NOT_TAKEN;
            } else {
                // We have a shard that was previously allocated, but we could not find a valid shard copy to allocate the primary.
                // We could just be waiting for the node that holds the primary to start back up, in which case the allocation for
                // this shard will be picked up when the node joins and we do another allocation reroute
                logger.debug("[{}][{}]: not allocating, number_of_allocated_shards_found [{}]",
                             unassignedShard.index(), unassignedShard.id(), nodeShardsResult.allocationsFound);
                // 注意这里返回的结果是  无分片数据可拷贝
                return AllocateUnassignedDecision.no(AllocationStatus.NO_VALID_SHARD_COPY,
                    explain ? buildNodeDecisions(null, shardState, inSyncAllocationIds) : null);
            }
        }

        // 以上无法找到分片此时在集群下的分配信息 只能返回空结果

        // 此时通过决策对象 检测了当前最适合存储该shard数据的node 以及不合适的node 不合适的node可能就会替换  所以才需要一开始访问所有节点啊 就是为了纵观全局
        NodesToAllocate nodesToAllocate = buildNodesToAllocate(
            allocation, nodeShardsResult.orderedAllocationCandidates, unassignedShard, false
        );
        DiscoveryNode node = null;
        String allocationId = null;
        boolean throttled = false;

        // 某些node适合继续存储该shard的数据
        if (nodesToAllocate.yesNodeShards.isEmpty() == false) {
            DecidedNode decidedNode = nodesToAllocate.yesNodeShards.get(0);
            logger.debug("[{}][{}]: allocating [{}] to [{}] on primary allocation",
                         unassignedShard.index(), unassignedShard.id(), unassignedShard, decidedNode.nodeShardState.getNode());
            node = decidedNode.nodeShardState.getNode();
            allocationId = decidedNode.nodeShardState.allocationId();


            // 限制级的node为空 且  某些node不再合适存储该shard的数据 (隐含条件 yes.isEmpty)
        } else if (nodesToAllocate.throttleNodeShards.isEmpty() && !nodesToAllocate.noNodeShards.isEmpty()) {
            // The deciders returned a NO decision for all nodes with shard copies, so we check if primary shard
            // can be force-allocated to one of the nodes.

            // 开始采取强制手段
            nodesToAllocate = buildNodesToAllocate(allocation, nodeShardsResult.orderedAllocationCandidates, unassignedShard, true);

            // 在强制手段下得到了yes的node 选择这个node
            if (nodesToAllocate.yesNodeShards.isEmpty() == false) {
                final DecidedNode decidedNode = nodesToAllocate.yesNodeShards.get(0);
                final NodeGatewayStartedShards nodeShardState = decidedNode.nodeShardState;
                logger.debug("[{}][{}]: allocating [{}] to [{}] on forced primary allocation",
                             unassignedShard.index(), unassignedShard.id(), unassignedShard, nodeShardState.getNode());
                node = nodeShardState.getNode();
                allocationId = nodeShardState.allocationId();
                // 当存在受限的node时 只能选择这个node
            } else if (nodesToAllocate.throttleNodeShards.isEmpty() == false) {
                logger.debug("[{}][{}]: throttling allocation [{}] to [{}] on forced primary allocation",
                             unassignedShard.index(), unassignedShard.id(), unassignedShard, nodesToAllocate.throttleNodeShards);
                throttled = true;
            } else {
                // 没有可用的node
                logger.debug("[{}][{}]: forced primary allocation denied [{}]",
                             unassignedShard.index(), unassignedShard.id(), unassignedShard);
            }
        } else {
            // 在非强制的情况下 有些node处于限制级
            // we are throttling this, since we are allowed to allocate to this node but there are enough allocations
            // taking place on the node currently, ignore it for now
            logger.debug("[{}][{}]: throttling allocation [{}] to [{}] on primary allocation",
                         unassignedShard.index(), unassignedShard.id(), unassignedShard, nodesToAllocate.throttleNodeShards);
            throttled = true;
        }

        List<NodeAllocationResult> nodeResults = null;
        if (explain) {
            nodeResults = buildNodeDecisions(nodesToAllocate, shardState, inSyncAllocationIds);
        }

        // 这种情况代表首先是异步调用 打上异步的标记 之后在回调中 递归调用reroute 又进入了这里 发现了之前的异步标记  还是返回空结果
        if (allocation.hasPendingAsyncFetch()) {
            return AllocateUnassignedDecision.no(AllocationStatus.FETCHING_SHARD_DATA, nodeResults);
            // 当node不为空时 代表这个就是本次的结果了
        } else if (node != null) {
            return AllocateUnassignedDecision.yes(node, allocationId, nodeResults, false);
        } else if (throttled) {
            return AllocateUnassignedDecision.throttle(nodeResults);
        } else {
            return AllocateUnassignedDecision.no(AllocationStatus.DECIDERS_NO, nodeResults, true);
        }
    }

    /**
     * Builds a map of nodes to the corresponding allocation decisions for those nodes.
     */
    private static List<NodeAllocationResult> buildNodeDecisions(NodesToAllocate nodesToAllocate,
                                                                 FetchResult<NodeGatewayStartedShards> fetchedShardData,
                                                                 Set<String> inSyncAllocationIds) {
        List<NodeAllocationResult> nodeResults = new ArrayList<>();
        Collection<NodeGatewayStartedShards> ineligibleShards;
        if (nodesToAllocate != null) {
            final Set<DiscoveryNode> discoNodes = new HashSet<>();
            nodeResults.addAll(Stream.of(nodesToAllocate.yesNodeShards, nodesToAllocate.throttleNodeShards, nodesToAllocate.noNodeShards)
                                .flatMap(Collection::stream)
                                .map(dnode -> {
                                    discoNodes.add(dnode.nodeShardState.getNode());
                                    return new NodeAllocationResult(dnode.nodeShardState.getNode(),
                                                                       shardStoreInfo(dnode.nodeShardState, inSyncAllocationIds),
                                                                       dnode.decision);
                                }).collect(Collectors.toList()));
            ineligibleShards = fetchedShardData.getData().values().stream().filter(shardData ->
                discoNodes.contains(shardData.getNode()) == false
            ).collect(Collectors.toList());
        } else {
            // there were no shard copies that were eligible for being assigned the allocation,
            // so all fetched shard data are ineligible shards
            ineligibleShards = fetchedShardData.getData().values();
        }

        nodeResults.addAll(ineligibleShards.stream().map(shardData ->
            new NodeAllocationResult(shardData.getNode(), shardStoreInfo(shardData, inSyncAllocationIds), null)
        ).collect(Collectors.toList()));

        return nodeResults;
    }

    private static ShardStoreInfo shardStoreInfo(NodeGatewayStartedShards nodeShardState, Set<String> inSyncAllocationIds) {
        final Exception storeErr = nodeShardState.storeException();
        final boolean inSync = nodeShardState.allocationId() != null && inSyncAllocationIds.contains(nodeShardState.allocationId());
        return new ShardStoreInfo(nodeShardState.allocationId(), inSync, storeErr);
    }

    private static final Comparator<NodeGatewayStartedShards> NO_STORE_EXCEPTION_FIRST_COMPARATOR =
        Comparator.comparing((NodeGatewayStartedShards state) -> state.storeException() == null).reversed();
    private static final Comparator<NodeGatewayStartedShards> PRIMARY_FIRST_COMPARATOR =
        Comparator.comparing(NodeGatewayStartedShards::primary).reversed();

    /**
     * Builds a list of nodes. If matchAnyShard is set to false, only nodes that have an allocation id matching
     * inSyncAllocationIds are added to the list. Otherwise, any node that has a shard is added to the list, but
     * entries with matching allocation id are always at the front of the list.
     * @param shard       本次需要分配的主分片
     * @param matchAnyShard     当恢复源为 Snapshot时 该标识为true
     * @param ignoreNodes     本次分配不会选择这些节点
     * @param inSyncAllocationIds      TODO 如果有些分片已经启动了 还有分配主分片的必要吗  这些分片是副本还是???
     * @param shardState    用于决策分片分配节点的相关数据
     */
    protected static NodeShardsResult buildNodeShardsResult(ShardRouting shard, boolean matchAnyShard,
                                                            Set<String> ignoreNodes, Set<String> inSyncAllocationIds,
                                                            FetchResult<NodeGatewayStartedShards> shardState,
                                                            Logger logger) {
        List<NodeGatewayStartedShards> nodeShardStates = new ArrayList<>();
        int numberOfAllocationsFound = 0;

        for (NodeGatewayStartedShards nodeShardState : shardState.getData().values()) {
            DiscoveryNode node = nodeShardState.getNode();
            String allocationId = nodeShardState.allocationId();

            // 因为每次拉取元数据是都是针对所有数据  如果有已经被排除的节点 就跳过
            if (ignoreNodes.contains(node.getId())) {
                continue;
            }

            if (nodeShardState.storeException() == null) {
                if (allocationId == null) {
                    logger.trace("[{}] on node [{}] has no shard state information", shard, nodeShardState.getNode());
                } else {
                    logger.trace("[{}] on node [{}] has allocation id [{}]", shard, nodeShardState.getNode(), allocationId);
                }
            } else {
                // 代表该shardId 对应的数据在目标节点存储过程中出现了异常  那么不应该选择该节点
                final String finalAllocationId = allocationId;
                // 只是获取锁失败的话 还是允许选择的
                if (nodeShardState.storeException() instanceof ShardLockObtainFailedException) {
                    logger.trace(() -> new ParameterizedMessage("[{}] on node [{}] has allocation id [{}] but the store can not be " +
                        "opened as it's locked, treating as valid shard", shard, nodeShardState.getNode(), finalAllocationId),
                        nodeShardState.storeException());
                } else {
                    logger.trace(() -> new ParameterizedMessage("[{}] on node [{}] has allocation id [{}] but the store can not be " +
                        "opened, treating as no allocation id", shard, nodeShardState.getNode(), finalAllocationId),
                        nodeShardState.storeException());
                    allocationId = null;
                }
            }

            if (allocationId != null) {
                assert nodeShardState.storeException() == null ||
                    nodeShardState.storeException() instanceof ShardLockObtainFailedException :
                    "only allow store that can be opened or that throws a ShardLockObtainFailedException while being opened but got a " +
                        "store throwing " + nodeShardState.storeException();
                // 代表找到了一个允许分配的候选节点
                numberOfAllocationsFound++;

                // TODO 先忽略通过快照进行数据恢复   什么时候in-sync 内有分片
                if (matchAnyShard || inSyncAllocationIds.contains(nodeShardState.allocationId())) {
                    nodeShardStates.add(nodeShardState);
                }
            }
        }

        final Comparator<NodeGatewayStartedShards> comparator; // allocation preference
        // TODO
        if (matchAnyShard) {
            // prefer shards with matching allocation ids
            Comparator<NodeGatewayStartedShards> matchingAllocationsFirst = Comparator.comparing(
                (NodeGatewayStartedShards state) -> inSyncAllocationIds.contains(state.allocationId())).reversed();
            comparator = matchingAllocationsFirst.thenComparing(NO_STORE_EXCEPTION_FIRST_COMPARATOR)
                .thenComparing(PRIMARY_FIRST_COMPARATOR);
        } else {
            // 优先将 primary为true的放在前面  其次是storeException为null的   primary为true就代表该节点之前就是作为主分片节点在存储数据
            // 那么本地的数据必然是最新的  有利于主分片的数据恢复
            comparator = NO_STORE_EXCEPTION_FIRST_COMPARATOR.thenComparing(PRIMARY_FIRST_COMPARATOR);
        }

        nodeShardStates.sort(comparator);

        if (logger.isTraceEnabled()) {
            logger.trace("{} candidates for allocation: {}", shard, nodeShardStates.stream().map(s -> s.getNode().getName())
                .collect(Collectors.joining(", ")));
        }
        return new NodeShardsResult(nodeShardStates, numberOfAllocationsFound);
    }

    /**
     * Split the list of node shard states into groups yes/no/throttle based on allocation deciders
     * @param allocation 记录了此时集群内所有分片的分配情况
     * @param nodeShardStates  本次访问集群后获取到的所有有效的结果  (此时shard.primary replicate 都已经存在于哪些节点上了) 这些节点不应该在被选为分配的目标
     * @param shardRouting 本次待分配的分片
     * @param forceAllocate 是否采用强制分配
     * 根据当前的参数信息 为shardRouting 生成分配结果
     */
    private static NodesToAllocate buildNodesToAllocate(RoutingAllocation allocation,
                                                        List<NodeGatewayStartedShards> nodeShardStates,
                                                        ShardRouting shardRouting,
                                                        boolean forceAllocate) {
        List<DecidedNode> yesNodeShards = new ArrayList<>();
        List<DecidedNode> throttledNodeShards = new ArrayList<>();
        List<DecidedNode> noNodeShards = new ArrayList<>();
        for (NodeGatewayStartedShards nodeShardState : nodeShardStates) {

            // 找到该node上所有分片此时的分配情况
            RoutingNode node = allocation.routingNodes().node(nodeShardState.getNode().getId());
            if (node == null) {
                continue;
            }

            // 判断是否允许分配  TODO 具体决策如何实现的先不看
            Decision decision = forceAllocate ? allocation.deciders().canForceAllocatePrimary(shardRouting, node, allocation) :
                                                allocation.deciders().canAllocate(shardRouting, node, allocation);

            // 将节点与 该分片相关的决策结果合并  并按照不同的结果划分到不同的容器中
            DecidedNode decidedNode = new DecidedNode(nodeShardState, decision);
            if (decision.type() == Type.THROTTLE) {
                throttledNodeShards.add(decidedNode);
            } else if (decision.type() == Type.NO) {
                noNodeShards.add(decidedNode);
            } else {
                yesNodeShards.add(decidedNode);
            }
        }
        return new NodesToAllocate(Collections.unmodifiableList(yesNodeShards), Collections.unmodifiableList(throttledNodeShards),
                                      Collections.unmodifiableList(noNodeShards));
    }

    protected abstract FetchResult<NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation);

    private static class NodeShardsResult {

        final List<NodeGatewayStartedShards> orderedAllocationCandidates;

        /**
         * 总计找到了几个 allocationId 与 orderedAllocationCandidates 的数量可能不符
         */
        final int allocationsFound;

        NodeShardsResult(List<NodeGatewayStartedShards> orderedAllocationCandidates, int allocationsFound) {
            this.orderedAllocationCandidates = orderedAllocationCandidates;
            this.allocationsFound = allocationsFound;
        }
    }

    /**
     * 表示一个分配结果 某个分片在相关node上分配的合适程度
     */
    static class NodesToAllocate {
        final List<DecidedNode> yesNodeShards;
        final List<DecidedNode> throttleNodeShards;
        final List<DecidedNode> noNodeShards;

        NodesToAllocate(List<DecidedNode> yesNodeShards, List<DecidedNode> throttleNodeShards, List<DecidedNode> noNodeShards) {
            this.yesNodeShards = yesNodeShards;
            this.throttleNodeShards = throttleNodeShards;
            this.noNodeShards = noNodeShards;
        }
    }

    /**
     * This class encapsulates the shard state retrieved from a node and the decision that was made
     * by the allocator for allocating to the node that holds the shard copy.
     */
    private static class DecidedNode {
        final NodeGatewayStartedShards nodeShardState;
        final Decision decision;

        private DecidedNode(NodeGatewayStartedShards nodeShardState, Decision decision) {
            this.nodeShardState = nodeShardState;
            this.decision = decision;
        }
    }
}
