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

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

/**
 * 该对象定义了 如果处理 由joinRequest包装起来的 join对象
 */
public class JoinTaskExecutor implements ClusterStateTaskExecutor<JoinTaskExecutor.Task> {

    /**
     * 该对象提供了分配分片的api
     */
    private final AllocationService allocationService;

    private final Logger logger;

    private final RerouteService rerouteService;

    /**
     * 每个task 对应一个 join 请求
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
         * 代表该任务是尝试将目标节点变成master节点
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
     * @param joiningNodes  可能旧的clusterState中不包含这个节点  那么在本次处理中会node加入到clusterState中 并将最新的clusterState发布到其他节点上
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
        //  代表本次任务 当前节点刚刚晋升成leader
        //  本节点竞选leader成功时 会将集群中的leader修改为当前节点
        } else if (currentNodes.getMasterNode() == null && joiningNodes.stream().anyMatch(Task::isBecomeMasterTask)) {

            // 如果本次task中 出现了 becomeMaster 那么同时还必须出现 finishElection
            assert joiningNodes.stream().anyMatch(Task::isFinishElectionTask)
                : "becoming a master but election is not finished " + joiningNodes;
            // use these joins to try and become the master.
            // Note that we don't have to do any validation of the amount of joining nodes - the commit
            // during the cluster state publishing guarantees that we have enough
            // 移除与本次join冲突的node   同时将自身修改为 master节点  因为同一个node 可能它的版本或者啥的发生了变化 那么之前记录的信息就需要被移除掉
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
        // TODO
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
                    // 确认兼容性
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
                    // 该节点属于可以参与选举的节点
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


        // 本次集群状态发生了变化 (收到了一个之前不存在与集群中的node的join请求 或者是本次晋升成了leader) 都要通知到集群中所有节点
        // 当节点的兼容性没有通过时 会返回失败信息给发送端
        if (nodesChanged) {
            // TODO 因为master节点发生了变化 所以要进行重路由
            rerouteService.reroute("post-join reroute", Priority.HIGH, ActionListener.wrap(
                r -> logger.trace("post-join reroute completed"),
                e -> logger.debug("post-join reroute failed", e)));


            if (joiniedNodeNameIds.isEmpty() == false) {
                // 找到此时不参与选举的配置信息
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

                // 因为某些 exclusion需要通过name来匹配  这时新的节点名字匹配上了 就要追加到原来的配置中
                // if VotingConfigExclusions did get updated
                if (newVotingConfigExclusions.equals(currentVotingConfigExclusions) == false) {
                    CoordinationMetadata.Builder coordMetadataBuilder = CoordinationMetadata.builder(currentState.coordinationMetadata())
                        .clearVotingConfigExclusions();
                    newVotingConfigExclusions.forEach(coordMetadataBuilder::addVotingConfigExclusion);
                    Metadata newMetadata = Metadata.builder(currentState.metadata())
                        .coordinationMetadata(coordMetadataBuilder.build()).build();
                    // 更新clusterState 并返回结果
                    return results.build(allocationService.adaptAutoExpandReplicas(newState.nodes(nodesBuilder)
                        .metadata(newMetadata).build()));
                }
            }

            return results.build(allocationService.adaptAutoExpandReplicas(newState.nodes(nodesBuilder).build()));
        } else {
            // we must return a new cluster state instance to force publishing. This is important
            // for the joining node to finalize its join and set us as a master
            return results.build(newState.build());
        }
    }

    /**
     * 本次发起join请求的node是最新的 如果它与之前的clusterState中某些node冲突 那么移除掉
     * 以及解除 NoMasterBlock 限制
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
            // 忽略2个选举相关的任务
            if (joinTask.isBecomeMasterTask() || joinTask.isFinishElectionTask()) {
                // noop
            } else {
                // 找到对应节点
                final DiscoveryNode joiningNode = joinTask.node();
                final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());
                // 代表节点id相同 但是ephemeralId 不同   应该是代表节点重启过吧  但是此时收到该节点的join 就代表认同此时的信息
                if (nodeWithSameId != null && nodeWithSameId.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameId, joiningNode);
                    nodesBuilder.remove(nodeWithSameId.getId());
                }
                final DiscoveryNode nodeWithSameAddress = currentNodes.findByAddress(joiningNode.getAddress());
                // 通过节点地址也排查一遍冲突的node
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

        // TODO  下面3个跟选举没有直接关系 先忽略
        allocationService.cleanCaches();
        // 更新持久化任务的元数据
        tmpState = PersistentTasksCustomMetadata.disassociateDeadNodes(tmpState);
        // 去除无效节点
        return ClusterState.builder(allocationService.disassociateDeadNodes(tmpState, false, "removed dead nodes on election"));
    }

    /**
     * 非leader节点 可能处理 join回调
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
