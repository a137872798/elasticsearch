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

import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult.ShardStoreInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ReplicaShardAllocator extends BaseGatewayShardAllocator {
    /**
     * Process existing recoveries of replicas and see if we need to cancel them if we find a better
     * match. Today, a better match is one that can perform a no-op recovery while the previous recovery
     * has to copy segment files.
     *
     * @param allocation
     * 检测之前已经分配出去 处于init状态的副本  是否分配到了一个合理的节点上   就是通过匹配副本节点与主分片lucene数据的同步率
     */
    public void processExistingRecoveries(RoutingAllocation allocation) {
        // 获取各种元数据的总集对象
        Metadata metadata = allocation.metadata();

        // 以node 为单位维护下面相关分片的状况
        RoutingNodes routingNodes = allocation.routingNodes();
        List<Runnable> shardCancellationActions = new ArrayList<>();
        for (RoutingNode routingNode : routingNodes) {
            // 遍历每个分片
            for (ShardRouting shard : routingNode) {

                // 忽略主分片
                if (shard.primary()) {
                    continue;
                }
                // 未分配或者已经恢复完数据的跳过
                if (shard.initializing() == false) {
                    continue;
                }
                // 处于重定向状态的跳过
                if (shard.relocatingNodeId() != null) {
                    continue;
                }

                // if we are allocating a replica because of index creation, no need to go and find a copy, there isn't one...
                // TODO 目前看到副本的创建只有  REPLICA_ADDED 先忽略这种情况
                if (shard.unassignedInfo() != null && shard.unassignedInfo().getReason() == UnassignedInfo.Reason.INDEX_CREATED) {
                    continue;
                }

                // 只要有副本处于恢复阶段  就检测所有分片与主分片数据的同步率  太低的就尝试切换到更合适的节点
                AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> shardStores = fetchData(shard, allocation);
                // 针对所有shardId 都发起请求后 退出for循环
                if (shardStores.hasData() == false) {
                    logger.trace("{}: fetching new stores for initializing shard", shard);
                    continue; // still fetching
                }

                ShardRouting primaryShard = allocation.routingNodes().activePrimary(shard.shardId());
                assert primaryShard != null : "the replica shard can be allocated on at least one node, so there must be an active primary";
                assert primaryShard.currentNodeId() != null;
                // 找到主分片所在的节点
                final DiscoveryNode primaryNode = allocation.nodes().get(primaryShard.currentNodeId());
                // 找到对应节点的索引文件元数据
                final TransportNodesListShardStoreMetadata.StoreFilesMetadata primaryStore = findStore(primaryNode, shardStores);
                // 没有找到主分片相关的store信息
                if (primaryStore == null) {
                    // if we can't find the primary data, it is probably because the primary shard is corrupted (and listing failed)
                    // just let the recovery find it out, no need to do anything about it for the initializing shard
                    logger.trace("{}: no primary shard store found or allocated, letting actual allocation figure it out", shard);
                    continue;
                }

                // TODO
                // 找到这个副本合适的一系列node  其中还有一个最接近primary的node
                MatchingNodes matchingNodes = findMatchingNodes(shard, allocation, true, primaryNode, primaryStore, shardStores, false);
                if (matchingNodes.getNodeWithHighestMatch() != null) {
                    DiscoveryNode currentNode = allocation.nodes().get(shard.currentNodeId());
                    DiscoveryNode nodeWithHighestMatch = matchingNodes.getNodeWithHighestMatch();
                    // current node will not be in matchingNodes as it is filtered away by SameShardAllocationDecider
                    // 当前分片不是同步率最高的replica
                    if (currentNode.equals(nodeWithHighestMatch) == false
                        // 且最高同步率的节点不需要恢复数据(也就是与primary完全同步)
                        && matchingNodes.canPerformNoopRecovery(nodeWithHighestMatch)
                        // 且当前节点之前没有同步过任何数据
                        && canPerformOperationBasedRecovery(primaryStore, shardStores, currentNode) == false) {
                        // we found a better match that can perform noop recovery, cancel the existing allocation.
                        logger.debug("cancelling allocation of replica on [{}], can perform a noop recovery on node [{}]",
                            currentNode, nodeWithHighestMatch);
                        final Set<String> failedNodeIds =
                            shard.unassignedInfo() == null ? Collections.emptySet() : shard.unassignedInfo().getFailedNodeIds();
                        // 这种情况选择将副本重新定位  这样就可以减少无意义的数据拷贝了
                        UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.REALLOCATED_REPLICA,
                            "existing allocation of replica to [" + currentNode + "] cancelled, can perform a noop recovery on [" +
                                nodeWithHighestMatch + "]",
                            null, 0, allocation.getCurrentNanoTime(), System.currentTimeMillis(), false,
                            UnassignedInfo.AllocationStatus.NO_ATTEMPT, failedNodeIds);
                        // don't cancel shard in the loop as it will cause a ConcurrentModificationException
                        // 这里是关闭某个节点下的某个分片  TODO
                        shardCancellationActions.add(() -> routingNodes.failShard(logger, shard, unassignedInfo,
                            metadata.getIndexSafe(shard.index()), allocation.changes()));
                    }
                }
            }
        }
        for (Runnable action : shardCancellationActions) {
            action.run();
        }
    }

    /**
     * Is the allocator responsible for allocating the given {@link ShardRouting}?
     * 检测该副本分片是否需要分配
     */
    private static boolean isResponsibleFor(final ShardRouting shard) {
        return shard.primary() == false // must be a replica
            && shard.unassigned() // must be unassigned
            // if we are allocating a replica because of index creation, no need to go and find a copy, there isn't one...
            && shard.unassignedInfo().getReason() != UnassignedInfo.Reason.INDEX_CREATED;
    }

    /**
     * 为副本分片选择合适的节点
     *
     * @param unassignedShard the unassigned shard to allocate
     * @param allocation      the current routing state
     * @param logger          the logger
     * @return
     */
    @Override
    public AllocateUnassignedDecision makeAllocationDecision(final ShardRouting unassignedShard,
                                                             final RoutingAllocation allocation,
                                                             final Logger logger) {
        // 检测该分片是否需要分配
        if (isResponsibleFor(unassignedShard) == false) {
            // this allocator is not responsible for deciding on this shard
            return AllocateUnassignedDecision.NOT_TAKEN;
        }

        final RoutingNodes routingNodes = allocation.routingNodes();
        final boolean explain = allocation.debugDecision();
        // pre-check if it can be allocated to any node that currently exists, so we won't list the store for it for nothing
        // 确保该分片至少能分配到某个node上  如果某个节点上已经存在该shardId的分片了 就不允许分配
        Tuple<Decision, Map<String, NodeAllocationResult>> result = canBeAllocatedToAtLeastOneNode(unassignedShard, allocation);
        Decision allocateDecision = result.v1();

        // 当前分片没有合适的节点分配   直接返回失败结果   如果此时正在拉取该分片的元数据 那么先进入下一步判断 情况可能会发生改变
        if (allocateDecision.type() != Decision.Type.YES
            && (explain == false || hasInitiatedFetching(unassignedShard) == false)) {
            // only return early if we are not in explain mode, or we are in explain mode but we have not
            // yet attempted to fetch any shard data
            logger.trace("{}: ignoring allocation, can't be allocated on any node", unassignedShard);
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.fromDecision(allocateDecision.type()),
                result.v2() != null ? new ArrayList<>(result.v2().values()) : null);
        }

        // 副本分片拉取的数据与primary的不同
        // 发送范围也是集群内所有节点
        AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> shardStores = fetchData(unassignedShard, allocation);
        // 代表本次通过异步调用 之后会在回调中重新触发 reroute
        if (shardStores.hasData() == false) {
            logger.trace("{}: ignoring allocation, still fetching shard stores", unassignedShard);
            // 标记此时处于一个异步状态
            allocation.setHasPendingAsyncFetch();
            List<NodeAllocationResult> nodeDecisions = null;
            if (explain) {
                nodeDecisions = buildDecisionsForAllNodes(unassignedShard, allocation);
            }
            return AllocateUnassignedDecision.no(AllocationStatus.FETCHING_SHARD_DATA, nodeDecisions);
        }

        // 该shard关联的主分片
        ShardRouting primaryShard = routingNodes.activePrimary(unassignedShard.shardId());
        if (primaryShard == null) {
            assert explain : "primary should only be null here if we are in explain mode, so we didn't " +
                "exit early when canBeAllocatedToAtLeastOneNode didn't return a YES decision";
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.fromDecision(allocateDecision.type()),
                new ArrayList<>(result.v2().values()));
        }
        assert primaryShard.currentNodeId() != null;
        final DiscoveryNode primaryNode = allocation.nodes().get(primaryShard.currentNodeId());
        // 获取主分片所在节点关联的元数据信息
        final TransportNodesListShardStoreMetadata.StoreFilesMetadata primaryStore = findStore(primaryNode, shardStores);
        if (primaryStore == null) {
            // if we can't find the primary data, it is probably because the primary shard is corrupted (and listing failed)
            // we want to let the replica be allocated in order to expose the actual problem with the primary that the replica
            // will try and recover from
            // Note, this is the existing behavior, as exposed in running CorruptFileTest#testNoPrimaryData
            logger.trace("{}: no primary shard store found or allocated, letting actual allocation figure it out", unassignedShard);
            return AllocateUnassignedDecision.NOT_TAKEN;
        }

        // 找到当前未分配的这个shard 匹配的一组node
        MatchingNodes matchingNodes = findMatchingNodes(
            unassignedShard, allocation, false, primaryNode, primaryStore, shardStores, explain);
        assert explain == false || matchingNodes.nodeDecisions != null : "in explain mode, we must have individual node decisions";

        // 将 NodeAllocationResult 整合
        List<NodeAllocationResult> nodeDecisions = augmentExplanationsWithStoreInfo(result.v2(), matchingNodes.nodeDecisions);

        // 代表此时在集群中找不到该shard可以使用的node了
        if (allocateDecision.type() != Decision.Type.YES) {
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.fromDecision(allocateDecision.type()), nodeDecisions);

        // 此时已经通过decides判断出一个可以分配的node了   但是还需要检测当存在匹配度最高的node时
        } else if (matchingNodes.getNodeWithHighestMatch() != null) {
            RoutingNode nodeWithHighestMatch = allocation.routingNodes().node(matchingNodes.getNodeWithHighestMatch().getId());
            // we only check on THROTTLE since we checked before before on NO

            // 检测能否将分片分配到这个同步率最高的节点上  也就是在节点上有分片的数据 却不代表此时有分片分配在节点上???  有数据只能代表着之前可能已经同步过部分数据 这样需要恢复的数据量会小些
            Decision decision = allocation.deciders().canAllocate(unassignedShard, nodeWithHighestMatch, allocation);

            // 如果最优的节点此时处于限制状态 就不允许分配
            if (decision.type() == Decision.Type.THROTTLE) {
                logger.debug("[{}][{}]: throttling allocation [{}] to [{}] in order to reuse its unallocated persistent store",
                    unassignedShard.index(), unassignedShard.id(), unassignedShard, nodeWithHighestMatch.node());
                // we are throttling this, as we have enough other shards to allocate to this node, so ignore it for now
                return AllocateUnassignedDecision.throttle(nodeDecisions);
            } else {

                // TODO 这里不会出现NO吗???
                logger.debug("[{}][{}]: allocating [{}] to [{}] in order to reuse its unallocated persistent store",
                    unassignedShard.index(), unassignedShard.id(), unassignedShard, nodeWithHighestMatch.node());
                // we found a match
                return AllocateUnassignedDecision.yes(nodeWithHighestMatch.node(), null, nodeDecisions, true);
            }

            // 首先allocateDecision.type()是YES 其次没有最匹配的节点 才会进入这个分支
            // TODO 此时没有任何可分配的节点  那首先allocateDecision.type() == YES 是怎么满足的???
            // 此时采用了延迟模式
        } else if (matchingNodes.hasAnyData() == false && unassignedShard.unassignedInfo().isDelayed()) {
            // if we didn't manage to find *any* data (regardless of matching sizes), and the replica is
            // unassigned due to a node leaving, so we delay allocation of this replica to see if the
            // node with the shard copy will rejoin so we can re-use the copy it has
            logger.debug("{}: allocation of [{}] is delayed", unassignedShard.shardId(), unassignedShard);
            long remainingDelayMillis = 0L;
            long totalDelayMillis = 0L;
            if (explain) {
                UnassignedInfo unassignedInfo = unassignedShard.unassignedInfo();
                Metadata metadata = allocation.metadata();
                IndexMetadata indexMetadata = metadata.index(unassignedShard.index());
                totalDelayMillis = INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(indexMetadata.getSettings()).getMillis();
                long remainingDelayNanos = unassignedInfo.getRemainingDelay(System.nanoTime(), indexMetadata.getSettings());
                remainingDelayMillis = TimeValue.timeValueNanos(remainingDelayNanos).millis();
            }
            // 返回延迟对象
            return AllocateUnassignedDecision.delayed(remainingDelayMillis, totalDelayMillis, nodeDecisions);
        }

        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    /**
     * Determines if the shard can be allocated on at least one node based on the allocation deciders.
     * <p>
     * Returns the best allocation decision for allocating the shard on any node (i.e. YES if at least one
     * node decided YES, THROTTLE if at least one node decided THROTTLE, and NO if none of the nodes decided
     * YES or THROTTLE).  If in explain mode, also returns the node-level explanations as the second element
     * in the returned tuple.
     * 检测是否还有节点可以分配该副本分片
     */
    private static Tuple<Decision, Map<String, NodeAllocationResult>> canBeAllocatedToAtLeastOneNode(ShardRouting shard,
                                                                                                     RoutingAllocation allocation) {
        Decision madeDecision = Decision.NO;
        final boolean explain = allocation.debugDecision();
        Map<String, NodeAllocationResult> nodeDecisions = explain ? new HashMap<>() : null;
        for (ObjectCursor<DiscoveryNode> cursor : allocation.nodes().getDataNodes().values()) {
            RoutingNode node = allocation.routingNodes().node(cursor.value.getId());
            if (node == null) {
                continue;
            }
            // if we can't allocate it on a node, ignore it, for example, this handles
            // cases for only allocating a replica after a primary
            // 首先检测该分片能否分配到该node上
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            // 代表首次设置
            if (decision.type() == Decision.Type.YES && madeDecision.type() != Decision.Type.YES) {
                if (explain) {
                    madeDecision = decision;
                } else {
                    return Tuple.tuple(decision, null);
                }
            } else if (madeDecision.type() == Decision.Type.NO && decision.type() == Decision.Type.THROTTLE) {
                madeDecision = decision;
            }
            // 先忽略 debug信息
            if (explain) {
                nodeDecisions.put(node.nodeId(), new NodeAllocationResult(node.node(), null, decision));
            }
        }
        // v2 对应的是描述信息 先忽略
        return Tuple.tuple(madeDecision, nodeDecisions);
    }

    /**
     * Takes the store info for nodes that have a shard store and adds them to the node decisions,
     * leaving the node explanations untouched for those nodes that do not have any store information.
     * 将 NodeAllocationResult结合
     */
    private static List<NodeAllocationResult> augmentExplanationsWithStoreInfo(Map<String, NodeAllocationResult> nodeDecisions,
                                                                               Map<String, NodeAllocationResult> withShardStores) {
        if (nodeDecisions == null || withShardStores == null) {
            return null;
        }
        List<NodeAllocationResult> augmented = new ArrayList<>();
        for (Map.Entry<String, NodeAllocationResult> entry : nodeDecisions.entrySet()) {
            if (withShardStores.containsKey(entry.getKey())) {
                augmented.add(withShardStores.get(entry.getKey()));
            } else {
                augmented.add(entry.getValue());
            }
        }
        return augmented;
    }

    /**
     * Finds the store for the assigned shard in the fetched data, returns null if none is found.
     *
     * @param node 主分片所在的节点
     * @param data 针对某个shard 索引文件数据 在集群范围内拉取到的元数据
     */
    private static TransportNodesListShardStoreMetadata.StoreFilesMetadata findStore(DiscoveryNode node,
                                                                                     AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> data) {
        NodeStoreFilesMetadata nodeFilesStore = data.getData().get(node);
        if (nodeFilesStore == null) {
            return null;
        }
        return nodeFilesStore.storeFilesMetadata();
    }

    /**
     * 找到与条件匹配的node组
     *
     * @param shard              本次待处理的分片  此时是副本分片
     * @param allocation         该对象内部包含了集群中所有shard的分配状态
     * @param noMatchFailedNodes 代表还需要与 unassigned内部的 failedNodeIds信息做匹配
     * @param primaryNode        主分片所在的节点
     * @param primaryStore       主分片对应的store信息
     * @param data               该shard在所有节点上的store信息
     * @param explain
     * @return
     */
    private MatchingNodes findMatchingNodes(ShardRouting shard, RoutingAllocation allocation, boolean noMatchFailedNodes,
                                            DiscoveryNode primaryNode, TransportNodesListShardStoreMetadata.StoreFilesMetadata primaryStore,
                                            AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> data,
                                            boolean explain) {
        Map<DiscoveryNode, MatchingNode> matchingNodes = new HashMap<>();
        // 先忽略debug信息
        Map<String, NodeAllocationResult> nodeDecisions = explain ? new HashMap<>() : null;
        for (Map.Entry<DiscoveryNode, NodeStoreFilesMetadata> nodeStoreEntry : data.getData().entrySet()) {
            DiscoveryNode discoNode = nodeStoreEntry.getKey();
            // 如果需要匹配failedNodes 并且匹配成功了 那么跳过这个节点
            if (noMatchFailedNodes && shard.unassignedInfo() != null &&
                shard.unassignedInfo().getFailedNodeIds().contains(discoNode.getId())) {
                continue;
            }

            // 如果在这个节点上没有元数据信息 那么跳过
            TransportNodesListShardStoreMetadata.StoreFilesMetadata storeFilesMetadata = nodeStoreEntry.getValue().storeFilesMetadata();
            // we don't have any files at all, it is an empty index
            if (storeFilesMetadata.isEmpty()) {
                continue;
            }

            RoutingNode node = allocation.routingNodes().node(discoNode.getId());
            if (node == null) {
                continue;
            }

            // check if we can allocate on that node...
            // we only check for NO, since if this node is THROTTLING and it has enough "same data"
            // then we will try and assign it next time
            // 判断分片能否分配到该节点上      会在这里过滤掉 primary 所在的节点么  因为replica应该是不能出现在primary的节点上的
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            MatchingNode matchingNode = null;
            // TODO
            if (explain) {
                matchingNode = computeMatchingNode(primaryNode, primaryStore, discoNode, storeFilesMetadata);
                ShardStoreInfo shardStoreInfo = new ShardStoreInfo(matchingNode.matchingBytes);
                nodeDecisions.put(node.nodeId(), new NodeAllocationResult(discoNode, shardStoreInfo, decision));
            }

            // 跳过无法分配该shard副本的节点
            if (decision.type() == Decision.Type.NO) {
                continue;
            }

            // 通过相关信息生成 MatchingNode对象
            if (matchingNode == null) {
                matchingNode = computeMatchingNode(primaryNode, primaryStore, discoNode, storeFilesMetadata);
            }
            matchingNodes.put(discoNode, matchingNode);
            if (logger.isTraceEnabled()) {
                if (matchingNode.isNoopRecovery) {
                    logger.trace("{}: node [{}] can perform a noop recovery", shard, discoNode.getName());
                } else if (matchingNode.retainingSeqNo >= 0) {
                    logger.trace("{}: node [{}] can perform operation-based recovery with retaining sequence number [{}]",
                        shard, discoNode.getName(), matchingNode.retainingSeqNo);
                } else {
                    logger.trace("{}: node [{}] has [{}/{}] bytes of re-usable data",
                        shard, discoNode.getName(), new ByteSizeValue(matchingNode.matchingBytes), matchingNode.matchingBytes);
                }
            }
        }

        // 将所有允许该副本分配的node 合并成一个 MatchingNodes对象  内部还会判断哪个node的匹配度是最高的
        return new MatchingNodes(matchingNodes, nodeDecisions);
    }

    /**
     * 检测主仓库与目标仓库内 完全一致的文件总计有多少byte
     *
     * @param primaryStore
     * @param storeFilesMetadata
     * @return
     */
    private static long computeMatchingBytes(TransportNodesListShardStoreMetadata.StoreFilesMetadata primaryStore,
                                             TransportNodesListShardStoreMetadata.StoreFilesMetadata storeFilesMetadata) {
        long sizeMatched = 0;
        // 遍历内部每个索引文件的元数据
        for (StoreFileMetadata storeFileMetadata : storeFilesMetadata) {
            String metadataFileName = storeFileMetadata.name();
            if (primaryStore.fileExists(metadataFileName) && primaryStore.file(metadataFileName).isSame(storeFileMetadata)) {
                sizeMatched += storeFileMetadata.length();
            }
        }
        return sizeMatched;
    }

    /**
     * 当 syncId 一致时 就代表2个分片上的数据是一致的 不需要做恢复操作
     *
     * @param primaryStore
     * @param replicaStore
     * @return
     */
    private static boolean hasMatchingSyncId(TransportNodesListShardStoreMetadata.StoreFilesMetadata primaryStore,
                                             TransportNodesListShardStoreMetadata.StoreFilesMetadata replicaStore) {
        String primarySyncId = primaryStore.syncId();
        return primarySyncId != null && primarySyncId.equals(replicaStore.syncId());
    }

    /**
     * 将相关信息包装成 MatchingNode
     *
     * @param primaryNode  主分片所在的node
     * @param primaryStore 主分片对应的store内元数据
     * @param replicaNode  本次考虑的一个目标节点   它在decides检测中判断副本允许设置在该node上
     * @param replicaStore replicateNode对应的store
     * @return
     */
    private static MatchingNode computeMatchingNode(
        DiscoveryNode primaryNode, TransportNodesListShardStoreMetadata.StoreFilesMetadata primaryStore,
        DiscoveryNode replicaNode, TransportNodesListShardStoreMetadata.StoreFilesMetadata replicaStore) {
        // 主分片所在仓库 内部有一个 replicaTracker 负责进行追踪每个副本的数据恢复情况  内部记录了相关的seqNo
        final long retainingSeqNoForPrimary = primaryStore.getPeerRecoveryRetentionLeaseRetainingSeqNo(primaryNode);
        final long retainingSeqNoForReplica = primaryStore.getPeerRecoveryRetentionLeaseRetainingSeqNo(replicaNode);

        // 判断此时是否需要恢复数据 如果副本与 primary的偏移量是一样的 就不需要恢复
        final boolean isNoopRecovery = (retainingSeqNoForReplica >= retainingSeqNoForPrimary && retainingSeqNoForPrimary >= 0)
            || hasMatchingSyncId(primaryStore, replicaStore);

        // 检测匹配的索引文件总计有多少bytes  TODO 同步过程中 还可以往primary中写入数据么???
        final long matchingBytes = computeMatchingBytes(primaryStore, replicaStore);
        return new MatchingNode(matchingBytes, retainingSeqNoForReplica, isNoopRecovery);
    }


    /**
     * 代表该节点上至少已经同步了部分数据  也可能完全匹配好了数据
     *
     * @param primaryStore 主分片对应的store信息
     * @param shardStores  集群中每个节点有关这个shard的store信息
     * @param targetNode   本次将要被处理的node
     * @return
     */
    private static boolean canPerformOperationBasedRecovery(TransportNodesListShardStoreMetadata.StoreFilesMetadata primaryStore,
                                                            AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> shardStores,
                                                            DiscoveryNode targetNode) {
        // 获取该node相关的store元数据信息  此时就不支持恢复数据了
        final NodeStoreFilesMetadata targetNodeStore = shardStores.getData().get(targetNode);
        if (targetNodeStore == null || targetNodeStore.storeFilesMetadata().isEmpty()) {
            return false;
        }
        if (hasMatchingSyncId(primaryStore, targetNodeStore.storeFilesMetadata())) {
            return true;
        }
        return primaryStore.getPeerRecoveryRetentionLeaseRetainingSeqNo(targetNode) >= 0;
    }

    protected abstract AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> fetchData(ShardRouting shard, RoutingAllocation allocation);

    /**
     * Returns a boolean indicating whether fetching shard data has been triggered at any point for the given shard.
     */
    protected abstract boolean hasInitiatedFetching(ShardRouting shard);


    /**
     * 当某个shard 找到了一个可以分配的node时 会包装一个该对象
     */
    private static class MatchingNode {
        static final Comparator<MatchingNode> COMPARATOR = Comparator.<MatchingNode, Boolean>comparing(m -> m.isNoopRecovery)
            .thenComparing(m -> m.retainingSeqNo).thenComparing(m -> m.matchingBytes);

        final long matchingBytes;
        /**
         * 这个不为空就代表至少写入了部分数据 也就是有部分匹配
         */
        final long retainingSeqNo;
        final boolean isNoopRecovery;

        /**
         * @param matchingBytes  所有匹配的文件的总大小
         * @param retainingSeqNo 当前replica 在primary.replicaTacker记录的seqNo
         * @param isNoopRecovery 代表是否还需要恢复数据
         */
        MatchingNode(long matchingBytes, long retainingSeqNo, boolean isNoopRecovery) {
            this.matchingBytes = matchingBytes;
            this.retainingSeqNo = retainingSeqNo;
            this.isNoopRecovery = isNoopRecovery;
        }

        /**
         * 至少有某个东西匹配成功了
         *
         * @return
         */
        boolean anyMatch() {
            return isNoopRecovery || retainingSeqNo >= 0 || matchingBytes > 0;
        }
    }

    /**
     * 一组 MatchingNode 对象
     */
    static class MatchingNodes {

        /**
         * 某个副本支持在该node上被分配 以及与primary之间的同步状态
         */
        private final Map<DiscoveryNode, MatchingNode> matchingNodes;
        /**
         * 在所有可用的副本节点中找到匹配度最高的节点
         */
        private final DiscoveryNode nodeWithHighestMatch;

        /**
         * DEBUG 相关的 先忽略
         */
        @Nullable
        private final Map<String, NodeAllocationResult> nodeDecisions;

        /**
         * @param matchingNodes
         * @param nodeDecisions 当没有使用debug模式时  该字段为null
         */
        MatchingNodes(Map<DiscoveryNode, MatchingNode> matchingNodes, @Nullable Map<String, NodeAllocationResult> nodeDecisions) {
            this.matchingNodes = matchingNodes;
            this.nodeDecisions = nodeDecisions;

            // 这里找到目前匹配度最高的节点
            this.nodeWithHighestMatch = matchingNodes.entrySet().stream()
                .filter(e -> e.getValue().anyMatch())
                .max(Comparator.comparing(Map.Entry::getValue, MatchingNode.COMPARATOR))
                .map(Map.Entry::getKey).orElse(null);
        }

        /**
         * Returns the node with the highest "non zero byte" match compared to
         * the primary.
         */
        @Nullable
        public DiscoveryNode getNodeWithHighestMatch() {
            return this.nodeWithHighestMatch;
        }

        boolean canPerformNoopRecovery(DiscoveryNode node) {
            final MatchingNode matchingNode = matchingNodes.get(node);
            return matchingNode.isNoopRecovery;
        }

        /**
         * Did we manage to find any data, regardless how well they matched or not.
         */
        public boolean hasAnyData() {
            return matchingNodes.isEmpty() == false;
        }
    }
}
