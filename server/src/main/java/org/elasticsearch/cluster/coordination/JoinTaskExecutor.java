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
     * 批量执行一组任务 并返回结果
     *
     * @param currentState
     * @param joiningNodes  每个join请求都会被封装成这个对象
     * @return ClusterTasksResult 中包含每个任务的结果
     * @throws Exception
     */
    @Override
    public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> joiningNodes) throws Exception {
        final ClusterTasksResult.Builder<Task> results = ClusterTasksResult.builder();

        final DiscoveryNodes currentNodes = currentState.nodes();
        boolean nodesChanged = false;
        ClusterState.Builder newState;

        // 如果此时只有一个任务  且任务代表某个选举结束了  直接生成成功结果
        if (joiningNodes.size() == 1 && joiningNodes.get(0).isFinishElectionTask()) {
            return results.successes(joiningNodes).build(currentState);
        //  代表此时集群中还没有旧的leader节点  并且 本次joinTask中有表明当前节点晋升成master节点的任务
        } else if (currentNodes.getMasterNode() == null && joiningNodes.stream().anyMatch(Task::isBecomeMasterTask)) {

            // 如果本次task中 出现了 becomeMaster 那么同时还必须出现 finishElection
            assert joiningNodes.stream().anyMatch(Task::isFinishElectionTask)
                : "becoming a master but election is not finished " + joiningNodes;
            // use these joins to try and become the master.
            // Note that we don't have to do any validation of the amount of joining nodes - the commit
            // during the cluster state publishing guarantees that we have enough
            // 去除已经无效的节点  同时将自身修改为 master节点
            newState = becomeMasterAndTrimConflictingNodes(currentState, joiningNodes);
            nodesChanged = true;

            // TODO
        } else if (currentNodes.isLocalNodeElectedMaster() == false) {
            logger.trace("processing node joins, but we are not the master. current master: {}", currentNodes.getMasterNode());
            throw new NotMasterException("Node [" + currentNodes.getLocalNode() + "] not master for join request");
        } else {
            // TODO
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

        // 当检测完兼容性时 task已经处理完了
        for (final Task joinTask : joiningNodes) {
            if (joinTask.isBecomeMasterTask() || joinTask.isFinishElectionTask()) {
                // noop
                // 如果这个任务对应的node 已经不存在于集群中了 忽略
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
                    nodesBuilder.add(node);
                    nodesChanged = true;
                    minClusterNodeVersion = Version.min(minClusterNodeVersion, node.getVersion());
                    maxClusterNodeVersion = Version.max(maxClusterNodeVersion, node.getVersion());
                    if (node.isMasterNode()) {
                        joiniedNodeNameIds.put(node.getName(), node.getId());
                    }
                } catch (IllegalArgumentException | IllegalStateException e) {
                    results.failure(joinTask, e);
                    continue;
                }
            }
            results.success(joinTask);
        }

        if (nodesChanged) {
            // 因为master节点发生了变化 所以要进行重路由
            rerouteService.reroute("post-join reroute", Priority.HIGH, ActionListener.wrap(
                r -> logger.trace("post-join reroute completed"),
                e -> logger.debug("post-join reroute failed", e)));

            // TODO 如果投选本次节点的某个节点 是上一任期的 master节点
            if (joiniedNodeNameIds.isEmpty() == false) {
                // 找到一组不参与选举的对象
                Set<CoordinationMetadata.VotingConfigExclusion> currentVotingConfigExclusions = currentState.getVotingConfigExclusions();
                Set<CoordinationMetadata.VotingConfigExclusion> newVotingConfigExclusions = currentVotingConfigExclusions.stream()
                    .map(e -> {
                        // Update nodeId in VotingConfigExclusion when a new node with excluded node name joins
                        if (CoordinationMetadata.VotingConfigExclusion.MISSING_VALUE_MARKER.equals(e.getNodeId()) &&
                            joiniedNodeNameIds.containsKey(e.getNodeName())) {
                            return new CoordinationMetadata.VotingConfigExclusion(joiniedNodeNameIds.get(e.getNodeName()), e.getNodeName());
                        } else {
                            return e;
                        }
                    }).collect(Collectors.toSet());

                // if VotingConfigExclusions did get updated
                // 代表需要更新 VotingConfigExclusion配置
                if (newVotingConfigExclusions.equals(currentVotingConfigExclusions) == false) {
                    CoordinationMetadata.Builder coordMetadataBuilder = CoordinationMetadata.builder(currentState.coordinationMetadata())
                        .clearVotingConfigExclusions();
                    newVotingConfigExclusions.forEach(coordMetadataBuilder::addVotingConfigExclusion);
                    Metadata newMetadata = Metadata.builder(currentState.metadata())
                        .coordinationMetadata(coordMetadataBuilder.build()).build();
                    // 根据新的配置 触发自适应副本分配
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
     * 做一些无效节点检测 以及解除 NoMasterBlock 限制
     *
     * @param currentState
     * @param joiningNodes
     * @return
     */
    protected ClusterState.Builder becomeMasterAndTrimConflictingNodes(ClusterState currentState, List<Task> joiningNodes) {
        assert currentState.nodes().getMasterNodeId() == null : currentState;
        DiscoveryNodes currentNodes = currentState.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);
        // 将本地节点修改成master节点  果然master节点就代表leader节点
        nodesBuilder.masterNodeId(currentState.nodes().getLocalNodeId());

        for (final Task joinTask : joiningNodes) {
            // 忽略2个选举相关的任务
            if (joinTask.isBecomeMasterTask() || joinTask.isFinishElectionTask()) {
                // noop
            } else {
                // 该任务是由哪个节点发出的
                final DiscoveryNode joiningNode = joinTask.node();
                final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());
                // 代表节点id相同 但是ephemeralId 不同  这样的节点会被移除
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
        // TODO 清理缓存
        allocationService.cleanCaches();
        // 更新持久化任务的元数据
        tmpState = PersistentTasksCustomMetadata.disassociateDeadNodes(tmpState);
        // 去除无效节点
        return ClusterState.builder(allocationService.disassociateDeadNodes(tmpState, false, "removed dead nodes on election"));
    }

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
