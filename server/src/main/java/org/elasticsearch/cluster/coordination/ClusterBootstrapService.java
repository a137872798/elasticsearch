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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;

/**
 * 某些刚启动的节点可能没有设置voteConfiguration  这样是无法正常进行选举的 所以需要确保在一定延时后必须初始化选举配置 否则抛出异常
 */
public class ClusterBootstrapService {

    /**
     * 除了通过配置seed地址外 也可以选择通过配置文件设置初始的masterNode
     */
    public static final Setting<List<String>> INITIAL_MASTER_NODES_SETTING =
        Setting.listSetting("cluster.initial_master_nodes", emptyList(), Function.identity(), Property.NodeScope);

    public static final Setting<TimeValue> UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.unconfigured_bootstrap_timeout",
            TimeValue.timeValueSeconds(3), TimeValue.timeValueMillis(1), Property.NodeScope);

    static final String BOOTSTRAP_PLACEHOLDER_PREFIX = "{bootstrap-placeholder}-";

    private static final Logger logger = LogManager.getLogger(ClusterBootstrapService.class);

    /**
     * 要求哪些节点必须存在
     */
    private final Set<String> bootstrapRequirements;
    @Nullable // null if discoveryIsConfigured()
    private final TimeValue unconfiguredBootstrapTimeout;
    private final TransportService transportService;
    private final Supplier<Iterable<DiscoveryNode>> discoveredNodesSupplier;
    /**
     * 对应 getStateForMasterService().getLastAcceptedConfiguration().isEmpty() == false 就是之前持久化了相关数据
     */
    private final BooleanSupplier isBootstrappedSupplier;
    private final Consumer<VotingConfiguration> votingConfigurationConsumer;
    private final AtomicBoolean bootstrappingPermitted = new AtomicBoolean(true);


    /**
     *
     * @param settings
     * @param transportService
     * @param discoveredNodesSupplier 对应 Coordinator::getFoundPeers   可以获取到 finder对象此时连接到的所有masterNode
     * @param isBootstrappedSupplier
     * @param votingConfigurationConsumer  在一定延时后 会将观测到的集群内节点设置到 coordinator上
     */
    public ClusterBootstrapService(Settings settings, TransportService transportService,
                                   Supplier<Iterable<DiscoveryNode>> discoveredNodesSupplier, BooleanSupplier isBootstrappedSupplier,
                                   Consumer<VotingConfiguration> votingConfigurationConsumer) {
        // TODO 如果当前集群仅包含一个node
        if (DiscoveryModule.isSingleNodeDiscovery(settings)) {
            if (INITIAL_MASTER_NODES_SETTING.exists(settings)) {
                throw new IllegalArgumentException("setting [" + INITIAL_MASTER_NODES_SETTING.getKey() +
                    "] is not allowed when [" + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey() + "] is set to [" +
                    DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE + "]");
            }
            if (DiscoveryNode.isMasterNode(settings) == false) {
                throw new IllegalArgumentException("node with [" + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey() + "] set to [" +
                    DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE +  "] must be master-eligible");
            }
            bootstrapRequirements = Collections.singleton(Node.NODE_NAME_SETTING.get(settings));
            unconfiguredBootstrapTimeout = null;
        } else {
            // 打个比方说 一开始配置了3个节点 那么初始化该节点后 必须要通过finder对象感知到这3个节点
            final List<String> initialMasterNodes = INITIAL_MASTER_NODES_SETTING.get(settings);
            bootstrapRequirements = unmodifiableSet(new LinkedHashSet<>(initialMasterNodes));
            if (bootstrapRequirements.size() != initialMasterNodes.size()) {
                throw new IllegalArgumentException(
                    "setting [" + INITIAL_MASTER_NODES_SETTING.getKey() + "] contains duplicates: " + initialMasterNodes);
            }
            // 代表此时配置中没有设置任何一种 发现集群节点的方式
            unconfiguredBootstrapTimeout = discoveryIsConfigured(settings) ? null : UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING.get(settings);
        }

        this.transportService = transportService;
        this.discoveredNodesSupplier = discoveredNodesSupplier;
        this.isBootstrappedSupplier = isBootstrappedSupplier;
        this.votingConfigurationConsumer = votingConfigurationConsumer;
    }

    public static boolean discoveryIsConfigured(Settings settings) {
        return Stream.of(DISCOVERY_SEED_PROVIDERS_SETTING, DISCOVERY_SEED_HOSTS_SETTING,
            INITIAL_MASTER_NODES_SETTING).anyMatch(s -> s.exists(settings));
    }

    /**
     * 每当集群中能感知到的节点发生变化时触发  在启动探测和关闭探测时也会触发
     */
    void onFoundPeersUpdated() {
        // 就是本地节点 + 通过finder感应到的所有节点
        final Set<DiscoveryNode> nodes = getDiscoveredNodes();

        // 当初始的voteConfiguration 未配置时 无法进行正常的选举  这里尝试进行初始化
        if (bootstrappingPermitted.get() && transportService.getLocalNode().isMasterNode() && bootstrapRequirements.isEmpty() == false
            && isBootstrappedSupplier.getAsBoolean() == false) {

            final Tuple<Set<DiscoveryNode>,List<String>> requirementMatchingResult;
            try {
                requirementMatchingResult = checkRequirements(nodes);
            } catch (IllegalStateException e) {
                logger.warn("bootstrapping cancelled", e);
                bootstrappingPermitted.set(false);
                return;
            }

            // v1 代表此时匹配上的node v2 代表暂时未匹配上的node
            final Set<DiscoveryNode> nodesMatchingRequirements = requirementMatchingResult.v1();
            final List<String> unsatisfiedRequirements = requirementMatchingResult.v2();
            logger.trace("nodesMatchingRequirements={}, unsatisfiedRequirements={}, bootstrapRequirements={}",
                nodesMatchingRequirements, unsatisfiedRequirements, bootstrapRequirements);

            // TODO 必然会包含自己 先忽略
            if (nodesMatchingRequirements.contains(transportService.getLocalNode()) == false) {
                logger.info("skipping cluster bootstrapping as local node does not match bootstrap requirements: {}",
                    bootstrapRequirements);
                bootstrappingPermitted.set(false);
                return;
            }

            // 至少在集群中要明确发现 1/2 以上的node 才能初始化投票箱
            if (nodesMatchingRequirements.size() * 2 > bootstrapRequirements.size()) {
                startBootstrap(nodesMatchingRequirements, unsatisfiedRequirements);
            }
        }
    }

    /**
     * 在调用 coordinator.startInitialJoin 后会触发该方法 检测在一定延时后是否发现了集群中的节点 如果始终无法发现节点 那么本次启动算是失败的
     */
    void scheduleUnconfiguredBootstrap() {
        // 代表配置了某些发现集群节点的方式 就不需要开启定时检测了
        if (unconfiguredBootstrapTimeout == null) {
            return;
        }

        // 如果本节点本身不参与选举 也不需要处理
        if (transportService.getLocalNode().isMasterNode() == false) {
            return;
        }

        logger.info("no discovery configuration found, will perform best-effort cluster bootstrapping after [{}] " +
            "unless existing master is discovered", unconfiguredBootstrapTimeout);

        // 这里有一个触发时间 可以理解为最终只等待这么长时间 在这时间之后就认为此时检测到的所有节点就是集群中的所有节点
        transportService.getThreadPool().scheduleUnlessShuttingDown(unconfiguredBootstrapTimeout, Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                // 对应 peerFinder.getFoundPeers()  也就是要求在一定延时后必须通过finder对象连接到某个节点上
                final Set<DiscoveryNode> discoveredNodes = getDiscoveredNodes();
                logger.debug("performing best-effort cluster bootstrapping with {}", discoveredNodes);
                startBootstrap(discoveredNodes, emptyList());
            }

            @Override
            public String toString() {
                return "unconfigured-discovery delayed bootstrap";
            }
        });
    }

    private Set<DiscoveryNode> getDiscoveredNodes() {
        return Stream.concat(Stream.of(transportService.getLocalNode()),
            StreamSupport.stream(discoveredNodesSupplier.get().spliterator(), false)).collect(Collectors.toSet());
    }


    /**
     * @param discoveryNodes  通过finder感知到的所有节点
     * @param unsatisfiedRequirements
     */
    private void startBootstrap(Set<DiscoveryNode> discoveryNodes, List<String> unsatisfiedRequirements) {
        assert discoveryNodes.stream().allMatch(DiscoveryNode::isMasterNode) : discoveryNodes;
        assert unsatisfiedRequirements.size() < discoveryNodes.size() : discoveryNodes + " smaller than " + unsatisfiedRequirements;
        if (bootstrappingPermitted.compareAndSet(true, false)) {
            doBootstrap(new VotingConfiguration(Stream.concat(discoveryNodes.stream().map(DiscoveryNode::getId),
                unsatisfiedRequirements.stream().map(s -> BOOTSTRAP_PLACEHOLDER_PREFIX + s))
                .collect(Collectors.toSet())));
        }
    }

    public static boolean isBootstrapPlaceholder(String nodeId) {
        return nodeId.startsWith(BOOTSTRAP_PLACEHOLDER_PREFIX);
    }


    /**
     * @param votingConfiguration
     */
    private void doBootstrap(VotingConfiguration votingConfiguration) {
        assert transportService.getLocalNode().isMasterNode();

        try {
            votingConfigurationConsumer.accept(votingConfiguration);
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("exception when bootstrapping with {}, rescheduling", votingConfiguration), e);
            // 因为finder会不断探测节点 直到满足生成voteConfig的最小条件时即可
            transportService.getThreadPool().scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(10), Names.GENERIC,
                new Runnable() {
                    @Override
                    public void run() {
                        doBootstrap(votingConfiguration);
                    }

                    @Override
                    public String toString() {
                        return "retry of failed bootstrapping with " + votingConfiguration;
                    }
                }
            );
        }
    }

    private static boolean matchesRequirement(DiscoveryNode discoveryNode, String requirement) {
        return discoveryNode.getName().equals(requirement)
            || discoveryNode.getAddress().toString().equals(requirement)
            || discoveryNode.getAddress().getAddress().equals(requirement);
    }

    /**
     * @param nodes  此时感知到的所有节点
     * @return
     */
    private Tuple<Set<DiscoveryNode>,List<String>> checkRequirements(Set<DiscoveryNode> nodes) {

        // 匹配的节点会设置到这个容器中
        final Set<DiscoveryNode> selectedNodes = new HashSet<>();
        // 此时某些要求的节点还未被发现
        final List<String> unmatchedRequirements = new ArrayList<>();
        // 这些节点必须存在
        for (final String bootstrapRequirement : bootstrapRequirements) {
            final Set<DiscoveryNode> matchingNodes
                = nodes.stream().filter(n -> matchesRequirement(n, bootstrapRequirement)).collect(Collectors.toSet());

            if (matchingNodes.size() == 0) {
                unmatchedRequirements.add(bootstrapRequirement);
            }

            if (matchingNodes.size() > 1) {
                throw new IllegalStateException("requirement [" + bootstrapRequirement + "] matches multiple nodes: " + matchingNodes);
            }

            for (final DiscoveryNode matchingNode : matchingNodes) {
                if (selectedNodes.add(matchingNode) == false) {
                    throw new IllegalStateException("node [" + matchingNode + "] matches multiple requirements: " +
                        bootstrapRequirements.stream().filter(r -> matchesRequirement(matchingNode, r)).collect(Collectors.toList()));
                }
            }
        }

        return Tuple.tuple(selectedNodes, unmatchedRequirements);
    }
}
