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
                // 该网关对象 仅支持处理恢复源为快照 或者 EXISTING_STORE的  新建的index对应的恢复源为 empty
                && (shard.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE
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
        // 该分片是否需要支持被本对象分配
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

        // 获取此时元数据中维护的所有启动的start分片    其中主分片分配到的节点会生成 ReplicationTracker
        // 也就是在IndexMetadata中的 in-sync 与 数据传输中的 in-sync 的概念是不一样的
        final Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(unassignedShard.id());
        // 该对象本身就是针对 以快照作为恢复源的分片进行处理的  在生成新的indexMetadata时 恢复源对应的indexMetadata的insync容器也会传递过来
        // 这样新的索引 就可以从原index上对应的shardId 获取数据了
        // 如果目标索引本身就存在 那么就是强制为这些分片重写数据  通过fetchData 可以快速得知这些分片现在分配在哪些node上
        // 只要就不需要为这些分片指定新的node了 直接在当前node进行数据恢复即可
        final boolean snapshotRestore = unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT;

        assert inSyncAllocationIds.isEmpty() == false;
        // use in-sync allocation ids to select nodes

        // 如果restore的索引 之前就存在与集群中 那么这时就可以直接在当前节点上进行数据恢复
        // 如果创建了一个新的索引 那么之前肯定没有分配到存在的节点上 这个应该就是empty
        final NodeShardsResult nodeShardsResult = buildNodeShardsResult(unassignedShard, snapshotRestore,
            allocation.getIgnoreNodes(unassignedShard.shardId()), inSyncAllocationIds, shardState, logger);

        // 代表有相同 indexName/shardId  的分片已经存在于集群的某个节点上了
        final boolean enoughAllocationsFound = nodeShardsResult.orderedAllocationCandidates.size() > 0;
        logger.debug("[{}][{}]: found {} allocation candidates of {} based on allocation ids: [{}]", unassignedShard.index(),
            unassignedShard.id(), nodeShardsResult.orderedAllocationCandidates.size(), unassignedShard, inSyncAllocationIds);

        // 代表本次是一个新建的索引 所有分片都需要重新分配
        if (enoughAllocationsFound == false) {
            // 无法利用特性进行快速分配  (网关其实起的就是拦截作用 发现是基于快照恢复的 就尝试快速找到合适的节点)
            // 那么交由兜底的分配器进行分配 balanceAllocator
            if (snapshotRestore) {
                // let BalancedShardsAllocator take care of allocating this shard
                logger.debug("[{}][{}]: missing local data, will restore from [{}]",
                             unassignedShard.index(), unassignedShard.id(), unassignedShard.recoverySource());
                return AllocateUnassignedDecision.NOT_TAKEN;
            } else {
                // TODO

                // We have a shard that was previously allocated, but we could not find a valid shard copy to allocate the primary.
                // We could just be waiting for the node that holds the primary to start back up, in which case the allocation for
                // this shard will be picked up when the node joins and we do another allocation reroute
                logger.debug("[{}][{}]: not allocating, number_of_allocated_shards_found [{}]",
                             unassignedShard.index(), unassignedShard.id(), nodeShardsResult.allocationsFound);
                // 注意这里返回的结果是  无分片数据可拷贝 本次reroute会放弃对该分片的分配  兜底的分配器也不会处理
                return AllocateUnassignedDecision.no(AllocationStatus.NO_VALID_SHARD_COPY,
                    explain ? buildNodeDecisions(null, shardState, inSyncAllocationIds) : null);
            }
        }

        // 因为此时有一组合适的节点 这里还要进行筛选  只要该分片曾经分配到某个节点上(成功过) 那么之后失败的概率就会小些
        NodesToAllocate nodesToAllocate = buildNodesToAllocate(
            allocation, nodeShardsResult.orderedAllocationCandidates, unassignedShard, false
        );
        DiscoveryNode node = null;
        String allocationId = null;
        boolean throttled = false;

        // 存在一些负载比较小的节点  直接使用就好
        if (nodesToAllocate.yesNodeShards.isEmpty() == false) {
            DecidedNode decidedNode = nodesToAllocate.yesNodeShards.get(0);
            logger.debug("[{}][{}]: allocating [{}] to [{}] on primary allocation",
                         unassignedShard.index(), unassignedShard.id(), unassignedShard, decidedNode.nodeShardState.getNode());
            node = decidedNode.nodeShardState.getNode();
            allocationId = decidedNode.nodeShardState.allocationId();

            // 此时没有可用的节点
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

        // 在本次处理前 可能又发现了新的节点 那么等待本次数据拉取结束后 再执行分配是最合适的
        if (allocation.hasPendingAsyncFetch()) {
            return AllocateUnassignedDecision.no(AllocationStatus.FETCHING_SHARD_DATA, nodeResults);
            // 本次产生了结果
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
     * @param shard
     * @param matchAnyShard
     * @param ignoreNodes
     * @param inSyncAllocationIds
     * @param shardState
     */
    protected static NodeShardsResult buildNodeShardsResult(ShardRouting shard,
                                                            boolean matchAnyShard, // 代表本分片的recoverySource 是否是快照
                                                            Set<String> ignoreNodes, // 之前在某节点上尝试恢复该分片数据失败了 所以该节点被认为不适合存储该分片数据
                                                            Set<String> inSyncAllocationIds, // 作为快照源的index此时所有启动的shardId
                                                            FetchResult<NodeGatewayStartedShards> shardState, // 该分片在所有节点上的分配状况 比如在a节点上是否存在该shardId 是否是主分片等
                                                            Logger logger) {
        List<NodeGatewayStartedShards> nodeShardStates = new ArrayList<>();
        int numberOfAllocationsFound = 0;

        // 遍历在每个节点上的处理结果   这个探测结果是针对 recover 并renamed后的index
        for (NodeGatewayStartedShards nodeShardState : shardState.getData().values()) {
            DiscoveryNode node = nodeShardState.getNode();
            String allocationId = nodeShardState.allocationId();

            // 这种情况 代表该节点之前启动该分片已经失败了  因为节点上报了 failure信息 而触发的reroute
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

                    // 代表索引目录无法正常使用  将allocationId置空 这样下面就不会选择该节点了
                    logger.trace(() -> new ParameterizedMessage("[{}] on node [{}] has allocation id [{}] but the store can not be " +
                        "opened, treating as no allocation id", shard, nodeShardState.getNode(), finalAllocationId),
                        nodeShardState.storeException());
                    allocationId = null;
                }
            }

            // 代表该分片曾经分配到该节点上过  针对 recover#renamed后的index 与之前的某个index同名的情况 这样直接修改原index上的分片数据就可以了
            if (allocationId != null) {
                assert nodeShardState.storeException() == null ||
                    nodeShardState.storeException() instanceof ShardLockObtainFailedException :
                    "only allow store that can be opened or that throws a ShardLockObtainFailedException while being opened but got a " +
                        "store throwing " + nodeShardState.storeException();
                // 代表找到了一个曾经分配过的信息
                numberOfAllocationsFound++;

                // 如果本次是快照恢复
                if (matchAnyShard || inSyncAllocationIds.contains(nodeShardState.allocationId())) {
                    nodeShardStates.add(nodeShardState);
                }
            }
        }

        final Comparator<NodeGatewayStartedShards> comparator; // allocation preference

        // 处理快照恢复的情况
        if (matchAnyShard) {
            // prefer shards with matching allocation ids
            // restore 有3种使用方式
            // 第一种 创建一个新的索引 并以 生成快照时的 indexMetadata作为参照 尝试恢复数据
            // 第二种 将生成快照时的 indexMetadata  映射到一个已经存在的index上 这时 快照index.inSyncAllocationIds 与新的index的 allocationId是不会出现相同的情况的
            // 第三种 将某个之前生成过快照的index还原到之前的状态 这时 原index.inSyncAllocationIds 与 新的index的 allocationId 是有可能相同的 也有可能不同
            Comparator<NodeGatewayStartedShards> matchingAllocationsFirst = Comparator.comparing(
                // 可以看到这里将 allocationId相同的 排在了最前面
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
     * @param allocation
     * @param nodeShardStates
     * @param shardRouting
     * @param forceAllocate
     * 根据该分片之前在集群其他节点上分配的情况 对节点进行分级 以便选出一个最合适的节点
     */
    private static NodesToAllocate buildNodesToAllocate(RoutingAllocation allocation,
                                                        List<NodeGatewayStartedShards> nodeShardStates,
                                                        ShardRouting shardRouting,
                                                        boolean forceAllocate) {
        List<DecidedNode> yesNodeShards = new ArrayList<>();
        List<DecidedNode> throttledNodeShards = new ArrayList<>();
        List<DecidedNode> noNodeShards = new ArrayList<>();
        for (NodeGatewayStartedShards nodeShardState : nodeShardStates) {

            // 如果当前节点已经下线 忽略处理 因为第一次的 fetch 和第二次的处理存在时间差 可能该节点下线了
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
