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
package org.elasticsearch.cluster.coordination;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * 当某个节点晋升成leader后 本次支持者对应的join请求都会被包装成task对象 并被该handle处理  包含自身
 */
public class JoinTaskExecutor implements ClusterStateTaskExecutor<JoinTaskExecutor.Task> {

    /**
     * 该对象负责分片的分配 以及分片的平衡任务
     */
    private final AllocationService allocationService;

    private final Logger logger;

    private final RerouteService rerouteService;

    /**
     * 每个发起join请求的node 会被包装成task 对象
     */
    public static class Task {

        /**
         * 发起join请求的节点
         */
        private final DiscoveryNode node;
        private final String reason;

        public Task(DiscoveryNode node, String reason) {
            this.node = node;
            this.reason = reason;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String reason() {
            return reason;
        }

        @Override
        public String toString() {
            return node != null ? node + " " + reason : reason;
        }

        /**
         * 当某个节点从candidate->leader时 会设置一个晋升的任务
         *
         * @return
         */
        public boolean isBecomeMasterTask() {
            return reason.equals(BECOME_MASTER_TASK_REASON);
        }

        /**
         * 表明某次选举结束了
         *
         * @return
         */
        public boolean isFinishElectionTask() {
            return reason.equals(FINISH_ELECTION_TASK_REASON);
        }

        private static final String BECOME_MASTER_TASK_REASON = "_BECOME_MASTER_TASK_";
        private static final String FINISH_ELECTION_TASK_REASON = "_FINISH_ELECTION_";
    }

    public JoinTaskExecutor(AllocationService allocationService, Logger logger, RerouteService rerouteService) {
        this.allocationService = allocationService;
        this.logger = logger;
        this.rerouteService = rerouteService;
    }

    /**
     * 处理join任务
     *
     * @param currentState
     * @param joiningNodes 本次支持的所有节点   极端情况下只有 略大于1/2的节点
     * @return ClusterTasksResult 中包含每个任务的结果
     * @throws Exception
     */
    @Override
    public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> joiningNodes) throws Exception {
        final ClusterTasksResult.Builder<Task> results = ClusterTasksResult.builder();

        final DiscoveryNodes currentNodes = currentState.nodes();
        boolean nodesChanged = false;
        ClusterState.Builder newState;

        // TODO 如果一个join就能结束选举工作 那应该是单节点环境 忽略这条逻辑 可以看到它既不需要更新clusterState 也不需要publish到其他节点
        if (joiningNodes.size() == 1 && joiningNodes.get(0).isFinishElectionTask()) {
            return results.successes(joiningNodes).build(currentState);
            // 代表本次首次选举成功
        } else if (currentNodes.getMasterNode() == null && joiningNodes.stream().anyMatch(Task::isBecomeMasterTask)) {

            // 如果本次task中 出现了 becomeMaster 那么同时还必须出现 finishElection
            assert joiningNodes.stream().anyMatch(Task::isFinishElectionTask)
                : "becoming a master but election is not finished " + joiningNodes;
            // use these joins to try and become the master.
            // Note that we don't have to do any validation of the amount of joining nodes - the commit
            // during the cluster state publishing guarantees that we have enough

            // 移除冲突的节点  但是之前集群中观测到此时还没有选择本节点的其他节点相关的分片信息也不会发生改变  只有明确发生冲突的才会变化
            newState = becomeMasterAndTrimConflictingNodes(currentState, joiningNodes);
            nodesChanged = true;

            // 不是leader节点 不允许走下面的分支   也就是只有leader节点 能通过batch处理 之前选举时收到的join请求
        } else if (currentNodes.isLocalNodeElectedMaster() == false) {
            logger.trace("processing node joins, but we are not the master. current master: {}", currentNodes.getMasterNode());
            throw new NotMasterException("Node [" + currentNodes.getLocalNode() + "] not master for join request");
        } else {
            // 当前节点已经成为leader的情况下 收到新的join请求 会更新clusterState
            newState = ClusterState.builder(currentState);
        }

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());

        assert nodesBuilder.isLocalNodeElectedMaster();

        // 分别获取集群中version 最大/小的节点
        Version minClusterNodeVersion = newState.nodes().getMinNodeVersion();
        Version maxClusterNodeVersion = newState.nodes().getMaxNodeVersion();
        // we only enforce major version transitions on a fully formed clusters
        final boolean enforceMajorVersion = currentState.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false;
        // processing any joins
        Map<String, String> joiniedNodeNameIds = new HashMap<>();

        for (final Task joinTask : joiningNodes) {
            if (joinTask.isBecomeMasterTask() || joinTask.isFinishElectionTask()) {
                // noop
                // 该节点已经记录在clusterState中了 不需要更新集群状态
            } else if (currentNodes.nodeExists(joinTask.node())) {
                logger.debug("received a join request for an existing node [{}]", joinTask.node());
            } else {
                final DiscoveryNode node = joinTask.node();
                try {
                    // TODO 有关兼容性的处理先忽略
                    if (enforceMajorVersion) {
                        ensureMajorVersionBarrier(node.getVersion(), minClusterNodeVersion);
                    }
                    ensureNodesCompatibility(node.getVersion(), minClusterNodeVersion, maxClusterNodeVersion);
                    // we do this validation quite late to prevent race conditions between nodes joining and importing dangling indices
                    // we have to reject nodes that don't support all indices we have in this cluster
                    ensureIndexCompatibility(node.getVersion(), currentState.getMetadata());

                    // 通过校验后代表本节点可以加入到最新的集群中
                    nodesBuilder.add(node);
                    nodesChanged = true;
                    minClusterNodeVersion = Version.min(minClusterNodeVersion, node.getVersion());
                    maxClusterNodeVersion = Version.max(maxClusterNodeVersion, node.getVersion());
                    // 该节点属于可以参与选举的节点   startJoin阶段是将请求发往 finder对象观测到的node  而finder对象应该是只能观测到masterNode的
                    // 应该是除了 masterNode外的其他节点 在发现leader后 会自动发起join请求 而不是通过startJoin
                    if (node.isMasterNode()) {
                        joiniedNodeNameIds.put(node.getName(), node.getId());
                    }
                } catch (IllegalArgumentException | IllegalStateException e) {
                    // 代表某个节点处理失败了   一开始join的时候直接检测不就好了
                    results.failure(joinTask, e);
                    continue;
                }
            }
            results.success(joinTask);
        }


        // 代表集群节点发生了变化 或者某个节点晋升成leader
        if (nodesChanged) {
            // 此时可能某些node与原节点产生了冲突 那些节点下的分片需要重新分配 这里就提交一个重分配的任务
            rerouteService.reroute("post-join reroute", Priority.HIGH, ActionListener.wrap(
                r -> logger.trace("post-join reroute completed"),
                e -> logger.debug("post-join reroute failed", e)));


            // 当本次发起join请求的节点中 包含 masterNode
            if (joiniedNodeNameIds.isEmpty() == false) {


                // 之前配置的不会选举成功的node 可能没有id信息 这时从joiniedNodeNameIds中找到id 并填充到 Exclusion中
                Set<CoordinationMetadata.VotingConfigExclusion> currentVotingConfigExclusions = currentState.getVotingConfigExclusions();
                Set<CoordinationMetadata.VotingConfigExclusion> newVotingConfigExclusions = currentVotingConfigExclusions.stream()
                    .map(e -> {
                        // Update nodeId in VotingConfigExclusion when a new node with excluded node name joins
                        // 代表不通过id 而是name 进行匹配么
                        if (CoordinationMetadata.VotingConfigExclusion.MISSING_VALUE_MARKER.equals(e.getNodeId()) &&
                            joiniedNodeNameIds.containsKey(e.getNodeName())) {
                            return new CoordinationMetadata.VotingConfigExclusion(joiniedNodeNameIds.get(e.getNodeName()), e.getNodeName());
                        } else {
                            return e;
                        }
                    }).collect(Collectors.toSet());

                // 代表某些node 完善了信息
                if (newVotingConfigExclusions.equals(currentVotingConfigExclusions) == false) {
                    CoordinationMetadata.Builder coordMetadataBuilder = CoordinationMetadata.builder(currentState.coordinationMetadata())
                        .clearVotingConfigExclusions();
                    newVotingConfigExclusions.forEach(coordMetadataBuilder::addVotingConfigExclusion);
                    Metadata newMetadata = Metadata.builder(currentState.metadata())
                        .coordinationMetadata(coordMetadataBuilder.build()).build();
                    // 更新clusterState 并返回结果  这里还包含了 自适应的副本拓展逻辑
                    return results.build(allocationService.adaptAutoExpandReplicas(newState.nodes(nodesBuilder)
                        .metadata(newMetadata).build()));
                }
            }

            // 代表有关 exclusion的配置信息没有变化 只需要处理自适应拓展的副本逻辑即可
            return results.build(allocationService.adaptAutoExpandReplicas(newState.nodes(nodesBuilder).build()));
        } else {
            // we must return a new cluster state instance to force publishing. This is important
            // for the joining node to finalize its join and set us as a master
            return results.build(newState.build());
        }
    }

    /**
     * 在clusterState中将本节点设置为leader节点 并且将发生变化的node从clusterState中移除
     *
     * @param currentState
     * @param joiningNodes
     * @return
     */
    protected ClusterState.Builder becomeMasterAndTrimConflictingNodes(ClusterState currentState, List<Task> joiningNodes) {
        assert currentState.nodes().getMasterNodeId() == null : currentState;
        DiscoveryNodes currentNodes = currentState.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);
        // 将本地节点修改成leader节点
        nodesBuilder.masterNodeId(currentState.nodes().getLocalNodeId());

        for (final Task joinTask : joiningNodes) {
            // 相当于2个哨兵任务  可以忽略
            if (joinTask.isBecomeMasterTask() || joinTask.isFinishElectionTask()) {
                // noop
            } else {
                // 本次支持者的节点
                final DiscoveryNode joiningNode = joinTask.node();
                final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());

                // 尝试用node.id/address 进行匹配 当匹配成功时 比较发现node数据发生变化 移除旧节点
                if (nodeWithSameId != null && nodeWithSameId.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameId, joiningNode);
                    nodesBuilder.remove(nodeWithSameId.getId());
                }
                final DiscoveryNode nodeWithSameAddress = currentNodes.findByAddress(joiningNode.getAddress());
                if (nodeWithSameAddress != null && nodeWithSameAddress.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameAddress,
                        joiningNode);
                    nodesBuilder.remove(nodeWithSameAddress.getId());
                }
            }
        }


        // now trim any left over dead nodes - either left there when the previous master stepped down
        // or removed by us above
        // 因为此时已经变成master节点了 所以将之前的 NoMasterBlock对象移除
        ClusterState tmpState = ClusterState.builder(currentState).nodes(nodesBuilder).blocks(
            ClusterBlocks.builder()
                .blocks(currentState.blocks())
                .removeGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_ID))
            .build();
        logger.trace("becomeMasterAndTrimConflictingNodes: {}", tmpState.nodes());

        // 有一些额外的分片分配增强组件 会将其他节点的数据信息缓存到本地 这里是进行清理 只有leader才能进行分配 所以只要清理主分片的就可以
        // 对选举流程本身没有影响
        allocationService.cleanCaches();
        // PersistentTasksCustomMetadata 是某种增强功能相关的元数据  可以忽略
        tmpState = PersistentTasksCustomMetadata.disassociateDeadNodes(tmpState);
        // 将冲突的节点 相关的分片信息从路由表中移除 这时之前节点下的分片某些会变成 unassigned 状态 TODO 这时不需要进行重分配
        return ClusterState.builder(allocationService.disassociateDeadNodes(tmpState, false, "removed dead nodes on election"));
    }

    /**
     * 非leader节点也可以处理 join请求
     *
     * @return
     */
    @Override
    public boolean runOnlyOnMaster() {
        // we validate that we are allowed to change the cluster state during cluster state processing
        return false;
    }

    public static Task newBecomeMasterTask() {
        return new Task(null, Task.BECOME_MASTER_TASK_REASON);
    }

    /**
     * a task that is used to signal the election is stopped and we should process pending joins.
     * it may be used in combination with {@link JoinTaskExecutor#newBecomeMasterTask()}
     */
    public static Task newFinishElectionTask() {
        return new Task(null, Task.FINISH_ELECTION_TASK_REASON);
    }

    /**
     * Ensures that all indices are compatible with the given node version. This will ensure that all indices in the given metadata
     * will not be created with a newer version of elasticsearch as well as that all indices are newer or equal to the minimum index
     * compatibility version.
     *
     * @throws IllegalStateException if any index is incompatible with the given version
     * @see Version#minimumIndexCompatibilityVersion()
     * 检测索引是否兼容   主要就是看lucene的版本
     */
    public static void ensureIndexCompatibility(final Version nodeVersion, Metadata metadata) {
        Version supportedIndexVersion = nodeVersion.minimumIndexCompatibilityVersion();
        // we ensure that all indices in the cluster we join are compatible with us no matter if they are
        // closed or not we can't read mappings of these indices so we need to reject the join...
        for (IndexMetadata idxMetadata : metadata) {
            if (idxMetadata.getCreationVersion().after(nodeVersion)) {
                throw new IllegalStateException("index " + idxMetadata.getIndex() + " version not supported: "
                    + idxMetadata.getCreationVersion() + " the node version is: " + nodeVersion);
            }
            if (idxMetadata.getCreationVersion().before(supportedIndexVersion)) {
                throw new IllegalStateException("index " + idxMetadata.getIndex() + " version not supported: "
                    + idxMetadata.getCreationVersion() + " minimum compatible index version is: " + supportedIndexVersion);
            }
        }
    }

    /**
     * ensures that the joining node has a version that's compatible with all current nodes
     * 主要是确保join的节点与当前节点的版本兼容
     */
    public static void ensureNodesCompatibility(final Version joiningNodeVersion, DiscoveryNodes currentNodes) {
        final Version minNodeVersion = currentNodes.getMinNodeVersion();
        final Version maxNodeVersion = currentNodes.getMaxNodeVersion();
        ensureNodesCompatibility(joiningNodeVersion, minNodeVersion, maxNodeVersion);
    }

    /**
     * ensures that the joining node has a version that's compatible with a given version range
     */
    public static void ensureNodesCompatibility(Version joiningNodeVersion, Version minClusterNodeVersion, Version maxClusterNodeVersion) {
        assert minClusterNodeVersion.onOrBefore(maxClusterNodeVersion) : minClusterNodeVersion + " > " + maxClusterNodeVersion;
        if (joiningNodeVersion.isCompatible(maxClusterNodeVersion) == false) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported. " +
                "The cluster contains nodes with version [" + maxClusterNodeVersion + "], which is incompatible.");
        }
        if (joiningNodeVersion.isCompatible(minClusterNodeVersion) == false) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported." +
                "The cluster contains nodes with version [" + minClusterNodeVersion + "], which is incompatible.");
        }
    }

    /**
     * ensures that the joining node's major version is equal or higher to the minClusterNodeVersion. This is needed
     * to ensure that if the master is already fully operating under the new major version, it doesn't go back to mixed
     * version mode
     **/
    public static void ensureMajorVersionBarrier(Version joiningNodeVersion, Version minClusterNodeVersion) {
        final byte clusterMajor = minClusterNodeVersion.major;
        if (joiningNodeVersion.major < clusterMajor) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported. " +
                "All nodes in the cluster are of a higher major [" + clusterMajor + "].");
        }
    }

    /**
     * 在基础上追加2个校验器
     *
     * @param onJoinValidators
     * @return
     */
    public static Collection<BiConsumer<DiscoveryNode, ClusterState>> addBuiltInJoinValidators(
        Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators) {
        final Collection<BiConsumer<DiscoveryNode, ClusterState>> validators = new ArrayList<>();
        validators.add((node, state) -> {
            ensureNodesCompatibility(node.getVersion(), state.getNodes());
            ensureIndexCompatibility(node.getVersion(), state.getMetadata());
        });
        validators.addAll(onJoinValidators);
        return Collections.unmodifiableCollection(validators);
    }
}
