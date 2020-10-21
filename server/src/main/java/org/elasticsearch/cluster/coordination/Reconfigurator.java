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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Computes the optimal configuration of voting nodes in the cluster.
 */
public class Reconfigurator {

    private static final Logger logger = LogManager.getLogger(Reconfigurator.class);

    /**
     * The cluster usually requires a vote from at least half of the master nodes in order to commit a cluster state update, and to achieve
     * the best resilience it makes automatic adjustments to the voting configuration as master nodes join or leave the cluster. Adjustments
     * that fix or increase the size of the voting configuration are always a good idea, but the wisdom of reducing the voting configuration
     * size is less clear. For instance, automatically reducing the voting configuration down to a single node means the cluster requires
     * this node to operate, which is not resilient: if it broke we could restore every other master-eligible node in the cluster to health
     * and still the cluster would be unavailable. However not reducing the voting configuration size can also hamper resilience: in a
     * five-node cluster we could lose two nodes and by reducing the voting configuration to the remaining three nodes we could tolerate the
     * loss of a further node before failing.
     *
     * We offer two options: either we auto-shrink the voting configuration as long as it contains more than three nodes, or we don't and we
     * require the user to control the voting configuration manually using the retirement API. The former, default, option, guarantees that
     * as long as there have been at least three master-eligible nodes in the cluster and no more than one of them is currently unavailable,
     * then the cluster will still operate, which is what almost everyone wants. Manual control is for users who want different guarantees.
     */
    public static final Setting<Boolean> CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION =
        Setting.boolSetting("cluster.auto_shrink_voting_configuration", true, Property.NodeScope, Property.Dynamic);

    private volatile boolean autoShrinkVotingConfiguration;

    public Reconfigurator(Settings settings, ClusterSettings clusterSettings) {
        autoShrinkVotingConfiguration = CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION, this::setAutoShrinkVotingConfiguration);
    }

    public void setAutoShrinkVotingConfiguration(boolean autoShrinkVotingConfiguration) {
        this.autoShrinkVotingConfiguration = autoShrinkVotingConfiguration;
    }

    private static int roundDownToOdd(int size) {
        return size - (size % 2 == 0 ? 1 : 0);
    }

    @Override
    public String toString() {
        return "Reconfigurator{" +
            "autoShrinkVotingConfiguration=" + autoShrinkVotingConfiguration +
            '}';
    }

    /**
     * Compute an optimal configuration for the cluster.
     *
     * @param liveNodes      The live nodes in the cluster. The optimal configuration prefers live nodes over non-live nodes as far as
     *                       possible.                                                                                                当前存活的所有节点
     * @param retiredNodeIds Nodes that are leaving the cluster and which should not appear in the configuration if possible. Nodes that are
     *                       retired and not in the current configuration will never appear in the resulting configuration; this is useful
     *                       for shifting the vote in a 2-node cluster so one of the nodes can be restarted without harming availability.   这些节点将不会存在于VotingConfiguration中
     * @param currentMaster  The current master. Unless retired, we prefer to keep the current master in the config.                   当前master节点
     * @param currentConfig  The current configuration. As far as possible, we prefer to keep the current config as-is.                  最近一次持久化的所有可参与选举的节点
     * @return An optimal configuration, or leave the current configuration unchanged if the optimal configuration has no live quorum.
     * 基于当前信息生成一个新的选举配置对象
     */
    public VotingConfiguration reconfigure(Set<DiscoveryNode> liveNodes, Set<String> retiredNodeIds, DiscoveryNode currentMaster,
                                           VotingConfiguration currentConfig) {
        assert liveNodes.contains(currentMaster) : "liveNodes = " + liveNodes + " master = " + currentMaster;
        logger.trace("{} reconfiguring {} based on liveNodes={}, retiredNodeIds={}, currentMaster={}",
            this, currentConfig, liveNodes, retiredNodeIds, currentMaster);

        final Set<String> liveNodeIds = liveNodes.stream()
            .filter(DiscoveryNode::isMasterNode).map(DiscoveryNode::getId).collect(Collectors.toSet());
        final Set<String> currentConfigNodeIds = currentConfig.getNodeIds();

        // 将此时所有存活的候选节点按照特殊顺序排序
        final Set<VotingConfigNode> orderedCandidateNodes = new TreeSet<>();

        liveNodes.stream()
            .filter(DiscoveryNode::isMasterNode)
            // 过滤掉在选举阶段被排除的节点
            .filter(n -> retiredNodeIds.contains(n.getId()) == false)
            .forEach(n -> orderedCandidateNodes.add(new VotingConfigNode(n.getId(), true,
                n.getId().equals(currentMaster.getId()), currentConfigNodeIds.contains(n.getId()))));

        // 将之前配置中的配置 按照当前是否被排除 是否失活的情况 生成live = false 的VotingConfigNode对象 并加入到列表中
        currentConfigNodeIds.stream()
            .filter(nid -> liveNodeIds.contains(nid) == false)
            .filter(nid -> retiredNodeIds.contains(nid) == false)
            .forEach(nid -> orderedCandidateNodes.add(new VotingConfigNode(nid, false, false, true)));

        /*
         * Now we work out how many nodes should be in the configuration:
         * 计算此时所有已写入的节点的数量
         */
        final int nonRetiredConfigSize = Math.toIntExact(orderedCandidateNodes.stream().filter(n -> n.inCurrentConfig).count());
        // 是否开启了自动缩减功能  也就是如果检测到某个节点失活 是否自动从集群中剔除
        // 这个是最少要保证的节点数量
        final int minimumConfigEnforcedSize = autoShrinkVotingConfiguration ? (nonRetiredConfigSize < 3 ? 1 : 3) : nonRetiredConfigSize;
        // 找到所有存活节点数量
        final int nonRetiredLiveNodeCount = Math.toIntExact(orderedCandidateNodes.stream().filter(n -> n.live).count());
        // roundDownToOdd(nonRetiredLiveNodeCount) 将nonRetiredLiveNodeCount 向下变成奇数
        final int targetSize = Math.max(roundDownToOdd(nonRetiredLiveNodeCount), minimumConfigEnforcedSize);

        // 仅将有效的节点数更新到投票配置中  因为在排序过程中live 肯定排在 noLive前面 且targetSize <= live 数量 所以可以确保存储的都是存活节点
        final VotingConfiguration newConfig = new VotingConfiguration(
            orderedCandidateNodes.stream()
                .limit(targetSize)
                .map(n -> n.id)
                .collect(Collectors.toSet()));

        // new configuration should have a quorum
        // 正常情况下肯定满足该条件  但是当过多有效节点刚好被排除时 可能此时有效的节点数不足所有存活节点的一半 这时不进行改动
        if (newConfig.hasQuorum(liveNodeIds)) {
            return newConfig;
        } else {
            // If there are not enough live nodes to form a quorum in the newly-proposed configuration, it's better to do nothing.
            return currentConfig;
        }
    }

    static class VotingConfigNode implements Comparable<VotingConfigNode> {

        final String id;
        /**
         * 该节点此时是否已经确定存活
         */
        final boolean live;
        /**
         * 该节点是否是leader节点
         */
        final boolean currentMaster;
        /**
         * 该节点可以作为选举节点的信息是否已经持久化
         */
        final boolean inCurrentConfig;

        VotingConfigNode(String id, boolean live, boolean currentMaster, boolean inCurrentConfig) {
            this.id = id;
            this.live = live;
            this.currentMaster = currentMaster;
            this.inCurrentConfig = inCurrentConfig;
        }


        @Override
        public int compareTo(VotingConfigNode other) {
            // prefer current master
            // 如果leader不相同 按照不同的leader节点排序  这条规则基本可以忽略
            final int currentMasterComp = Boolean.compare(other.currentMaster, currentMaster);
            if (currentMasterComp != 0) {
                return currentMasterComp;
            }
            // prefer nodes that are live
            final int liveComp = Boolean.compare(other.live, live);
            if (liveComp != 0) {
                return liveComp;
            }
            // prefer nodes that are in current config for stability
            final int inCurrentConfigComp = Boolean.compare(other.inCurrentConfig, inCurrentConfig);
            if (inCurrentConfigComp != 0) {
                return inCurrentConfigComp;
            }
            // tiebreak by node id to have stable ordering
            return id.compareTo(other.id);
        }

        @Override
        public String toString() {
            return "VotingConfigNode{" +
                "id='" + id + '\'' +
                ", live=" + live +
                ", currentMaster=" + currentMaster +
                ", inCurrentConfig=" + inCurrentConfig +
                '}';
        }
    }
}
