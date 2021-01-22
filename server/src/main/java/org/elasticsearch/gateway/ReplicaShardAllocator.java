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
                // 当通过indexCreate 请求创建索引时 会根据配置项中的 主副本数量创建副本  同时reason就是 INDEX_CREATED
                // 如果是自适应创建的副本就是 REPLICA_ADDED
                // TODO 这里新创建索引时生成的分片 就不需要重分配了
                if (shard.unassignedInfo() != null && shard.unassignedInfo().getReason() == UnassignedInfo.Reason.INDEX_CREATED) {
                    continue;
                }

                // 实际上 peer的恢复方式 就是直接从primary 传输索引文件数据 并使用事务日志进行数据恢复  那么之前的数据同步率应该是没有意义的啊
                AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> shardStores = fetchData(shard, allocation);
                // 针对所有shardId 都发起请求后 退出for循环
                if (shardStores.hasData() == false) {
                    logger.trace("{}: fetching new stores for initializing shard", shard);
                    continue; // still fetching
                }

                // 能够进入到这里 代表上次的数据已经拉取完毕
                ShardRouting primaryShard = allocation.routingNodes().activePrimary(shard.shardId());
                assert primaryShard != null : "the replica shard can be allocated on at least one node, so there must be an active primary";
                assert primaryShard.currentNodeId() != null;
                // 找到主分片所在的节点
                final DiscoveryNode primaryNode = allocation.nodes().get(primaryShard.currentNodeId());
                // 找到主节点上该分片此时的索引文件元数据  比如文件大小/校验和等
                final TransportNodesListShardStoreMetadata.StoreFilesMetadata primaryStore = findStore(primaryNode, shardStores);
                // 如果此时没有找到主分片数据 那么此时主分片 应该已经出现问题了 等待它自己通知leader 失败
                // 此时如果存在可用副本 那么副本会升级成主分片 如果不存在可用副本 副本会重新变成 unassigned 主分片也会变成unassigned
                if (primaryStore == null) {
                    // if we can't find the primary data, it is probably because the primary shard is corrupted (and listing failed)
                    // just let the recovery find it out, no need to do anything about it for the initializing shard
                    logger.trace("{}: no primary shard store found or allocated, letting actual allocation figure it out", shard);
                    continue;
                }

                // 通过该分片在所有节点上的索引文件信息 找到一个恢复最快 (所需要恢复的数据最少的节点)
                MatchingNodes matchingNodes = findMatchingNodes(shard, allocation, true, primaryNode, primaryStore, shardStores, false);

                // 至少有某个匹配部分数据的节点
                if (matchingNodes.getNodeWithHighestMatch() != null) {
                    DiscoveryNode currentNode = allocation.nodes().get(shard.currentNodeId());
                    DiscoveryNode nodeWithHighestMatch = matchingNodes.getNodeWithHighestMatch();
                    // current node will not be in matchingNodes as it is filtered away by SameShardAllocationDecider
                    // 代表当前副本所在的节点 不是匹配度最高的那个节点
                    if (currentNode.equals(nodeWithHighestMatch) == false
                        // 且最高同步率的节点不需要恢复数据(也就是与primary完全同步)  TODO 但是事务日志还是要继续恢复的吧
                        && matchingNodes.canPerformNoopRecovery(nodeWithHighestMatch)
                        // 且当前节点之前没有同步过任何数据  也就是索引文件还没有生成 且还没有上传任何续约信息
                        && canPerformOperationBasedRecovery(primaryStore, shardStores, currentNode) == false) {
                        // we found a better match that can perform noop recovery, cancel the existing allocation.
                        logger.debug("cancelling allocation of replica on [{}], can perform a noop recovery on node [{}]",
                            currentNode, nodeWithHighestMatch);
                        final Set<String> failedNodeIds =
                            shard.unassignedInfo() == null ? Collections.emptySet() : shard.unassignedInfo().getFailedNodeIds();
                        // 这种情况选择将副本重新定位  这样就可以减少无意义的数据拷贝了   看来使用网关副本分配器时 能够确保分配到一个匹配度最高的节点
                        UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.REALLOCATED_REPLICA,
                            "existing allocation of replica to [" + currentNode + "] cancelled, can perform a noop recovery on [" +
                                nodeWithHighestMatch + "]",
                            null, 0, allocation.getCurrentNanoTime(), System.currentTimeMillis(), false,
                            UnassignedInfo.AllocationStatus.NO_ATTEMPT, failedNodeIds);
                        // don't cancel shard in the loop as it will cause a ConcurrentModificationException
                        // routingNodes.failShard 会将分片重置为 unassigned
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
     * 该分片是否支持被本对象处理  不是所有处于 assigned的分片都能被处理的   比如创建原因是 INDEX_CREATED 也就是自然创建的副本实际上不会受到该对象的影响
     */
    private static boolean isResponsibleFor(final ShardRouting shard) {
        return shard.primary() == false // must be a replica
            && shard.unassigned() // must be unassigned
            // if we are allocating a replica because of index creation, no need to go and find a copy, there isn't one...  如果索引是首次创建 副本自然找不到什么更合适的节点
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
        // 确保该分片至少能分配到某个node上
        Tuple<Decision, Map<String, NodeAllocationResult>> result = canBeAllocatedToAtLeastOneNode(unassignedShard, allocation);
        Decision allocateDecision = result.v1();

        // 当前分片没有合适的节点分配   直接返回失败结果
        // 如果 explain为true 那么走下一步 获取更多的描述信息
        // 如果已经针对某个shardId 拉取到节点的数据 那么先进入下一步判断 情况可能会发生改变
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
        // 代表本次通过异步调用 无法直接返回结果
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

        // 如果此时没有活跃状态的主分片是无法对副本进行分配的
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

        // 当主分片还没有产生数据时 使用兜底的分配策略 因为此时肯定不会有什么索引文件相匹配的情况 无法优化恢复流程
        if (primaryStore == null) {
            // if we can't find the primary data, it is probably because the primary shard is corrupted (and listing failed)
            // we want to let the replica be allocated in order to expose the actual problem with the primary that the replica
            // will try and recover from
            // Note, this is the existing behavior, as exposed in running CorruptFileTest#testNoPrimaryData
            logger.trace("{}: no primary shard store found or allocated, letting actual allocation figure it out", unassignedShard);
            return AllocateUnassignedDecision.NOT_TAKEN;
        }

        // 根据该分片在每个节点上残留的索引文件数据 选择最优的节点
        MatchingNodes matchingNodes = findMatchingNodes(
            unassignedShard, allocation, false, primaryNode, primaryStore, shardStores, explain);
        assert explain == false || matchingNodes.nodeDecisions != null : "in explain mode, we must have individual node decisions";

        // 追加描述信息 先忽略
        List<NodeAllocationResult> nodeDecisions = augmentExplanationsWithStoreInfo(result.v2(), matchingNodes.nodeDecisions);

        // 代表此时在集群中找不到该shard可以使用的node了
        if (allocateDecision.type() != Decision.Type.YES) {
            return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.fromDecision(allocateDecision.type()), nodeDecisions);

            // 本轮找到了匹配度高的节点
        } else if (matchingNodes.getNodeWithHighestMatch() != null) {
            RoutingNode nodeWithHighestMatch = allocation.routingNodes().node(matchingNodes.getNodeWithHighestMatch().getId());
            // we only check on THROTTLE since we checked before before on NO

            // 检测此时该分片是否可用
            Decision decision = allocation.deciders().canAllocate(unassignedShard, nodeWithHighestMatch, allocation);

            // 本次最合适的节点此时处于限流状态 等待下次合适的时机进行reroute
            if (decision.type() == Decision.Type.THROTTLE) {
                logger.debug("[{}][{}]: throttling allocation [{}] to [{}] in order to reuse its unallocated persistent store",
                    unassignedShard.index(), unassignedShard.id(), unassignedShard, nodeWithHighestMatch.node());
                // we are throttling this, as we have enough other shards to allocate to this node, so ignore it for now
                return AllocateUnassignedDecision.throttle(nodeDecisions);
            } else {

                // 在findMatchingNodes 中已经过滤掉了 NO 的节点了
                logger.debug("[{}][{}]: allocating [{}] to [{}] in order to reuse its unallocated persistent store",
                    unassignedShard.index(), unassignedShard.id(), unassignedShard, nodeWithHighestMatch.node());
                // we found a match
                return AllocateUnassignedDecision.yes(nodeWithHighestMatch.node(), null, nodeDecisions, true);
            }

            // 虽然此时无可复用节点  但是没有要求在此刻立即分配的话 可以不交给兜底分配策略
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

        // 因为找不到已经存在过数据的节点 无法快速定位 所以交由兜底的分配策略进行分配
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
     *
     * @param shard
     * @param allocation
     * @param noMatchFailedNodes
     * @param primaryNode
     * @param primaryStore
     * @param data
     * @param explain
     * @return
     * 针对 处于init状态的副本 尝试找到一个更合适的节点  不过副本都是使用peer 并且数据都是全量拷贝 这一步的意义是什么 ???
     */
    private MatchingNodes findMatchingNodes(ShardRouting shard, // 本次要分配的分片
                                            RoutingAllocation allocation,  // 描述本次分配的相关信息
                                            boolean noMatchFailedNodes,  // 是否要匹配失败的node
                                            DiscoveryNode primaryNode,
                                            TransportNodesListShardStoreMetadata.StoreFilesMetadata primaryStore,  // 主分片此时的数据
                                            AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> data,  // 每个节点上有关该分片的数据
                                            boolean explain) {
        Map<DiscoveryNode, MatchingNode> matchingNodes = new HashMap<>();
        // 先忽略debug信息
        Map<String, NodeAllocationResult> nodeDecisions = explain ? new HashMap<>() : null;
        for (Map.Entry<DiscoveryNode, NodeStoreFilesMetadata> nodeStoreEntry : data.getData().entrySet()) {
            DiscoveryNode discoNode = nodeStoreEntry.getKey();

            // 代表不匹配失败的节点  并且该分片曾经在该节点上分配失败过
            if (noMatchFailedNodes && shard.unassignedInfo() != null &&
                shard.unassignedInfo().getFailedNodeIds().contains(discoNode.getId())) {
                continue;
            }

            // 代表该节点之前都没有该分片的有关信息
            TransportNodesListShardStoreMetadata.StoreFilesMetadata storeFilesMetadata = nodeStoreEntry.getValue().storeFilesMetadata();
            // we don't have any files at all, it is an empty index
            if (storeFilesMetadata.isEmpty()) {
                continue;
            }

            // 如果当前节点已经从集群中移除 也忽略
            RoutingNode node = allocation.routingNodes().node(discoNode.getId());
            if (node == null) {
                continue;
            }

            // check if we can allocate on that node...
            // we only check for NO, since if this node is THROTTLING and it has enough "same data"
            // then we will try and assign it next time
            // 这里已经排除掉primary 所在的节点了
            Decision decision = allocation.deciders().canAllocate(shard, node, allocation);
            MatchingNode matchingNode = null;
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

        // 检测此分片在所有节点上 索引文件的匹配度 一样的文件越多 数据恢复就越快 同时做的无用功也就越少
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
     * 通过某个分片之前在某个节点上的索引数据信息  以及主分片的索引数据信息的对比 计算匹配度
     *
     * @param primaryNode
     * @param primaryStore
     * @param replicaNode
     * @param replicaStore
     * @return
     */
    private static MatchingNode computeMatchingNode(
        DiscoveryNode primaryNode, // 此时正在运行状态的主分片所在的节点
        TransportNodesListShardStoreMetadata.StoreFilesMetadata primaryStore,
        DiscoveryNode replicaNode, // 假设副本此时分配在该节点
        TransportNodesListShardStoreMetadata.StoreFilesMetadata replicaStore) {

        // 找到该节点的续约信息
        final long retainingSeqNoForPrimary = primaryStore.getPeerRecoveryRetentionLeaseRetainingSeqNo(primaryNode);
        final long retainingSeqNoForReplica = primaryStore.getPeerRecoveryRetentionLeaseRetainingSeqNo(replicaNode);

        // 如果续约的seq是一样的  或者 2个lucene.userData.syncId 完全一致 代表该分片在某节点上已经与主分片数据完全同步
        final boolean isNoopRecovery = (retainingSeqNoForReplica >= retainingSeqNoForPrimary && retainingSeqNoForPrimary >= 0)
            || hasMatchingSyncId(primaryStore, replicaStore);

        // 检测此时有多少索引文件与主分片的完全一致 这些文件都不需要恢复了
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
         * 代表某分片数据在该节点至少有某些数据与主分片匹配了
         * @return
         */
        boolean anyMatch() {
            return isNoopRecovery // 代表数据完全匹配
                || retainingSeqNo >= 0   // 只要该节点已经开始恢复数据 并上报续约信息
                || matchingBytes > 0;  // 代表至少有一个索引文件完全匹配
        }
    }

    /**
     * 该分片在所有节点上的索引数据 与主分片索引数据的匹配度
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
