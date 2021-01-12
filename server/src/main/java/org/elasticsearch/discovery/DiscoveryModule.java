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

package org.elasticsearch.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.transport.TransportService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

/**
 * A module for loading classes for node discovery.
 * 集群中注册发现模块
 */
public class DiscoveryModule {
    private static final Logger logger = LogManager.getLogger(DiscoveryModule.class);

    public static final String ZEN2_DISCOVERY_TYPE = "zen";

    public static final String SINGLE_NODE_DISCOVERY_TYPE = "single-node";

    public static final Setting<String> DISCOVERY_TYPE_SETTING =
        new Setting<>("discovery.type", ZEN2_DISCOVERY_TYPE, Function.identity(), Property.NodeScope);
    public static final Setting<List<String>> DISCOVERY_SEED_PROVIDERS_SETTING =
        Setting.listSetting("discovery.seed_providers", Collections.emptyList(), Function.identity(),
            Property.NodeScope);

    public static final String DEFAULT_ELECTION_STRATEGY = "default";

    public static final Setting<String> ELECTION_STRATEGY_SETTING =
        new Setting<>("cluster.election.strategy", DEFAULT_ELECTION_STRATEGY, Function.identity(), Property.NodeScope);

    /**
     * 注册中心
     */
    private final Discovery discovery;


    /**
     * 注册发现模块
     * @param settings
     * @param transportService
     * @param namedWriteableRegistry
     * @param networkService
     * @param masterService
     * @param clusterApplier
     * @param clusterSettings
     * @param plugins
     * @param allocationService
     * @param configFile
     * @param gatewayMetaState
     * @param rerouteService
     */
    public DiscoveryModule(Settings settings, TransportService transportService,
                           NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService, MasterService masterService,
                           ClusterApplier clusterApplier, ClusterSettings clusterSettings, List<DiscoveryPlugin> plugins,
                           AllocationService allocationService, Path configFile, GatewayMetaState gatewayMetaState,
                           RerouteService rerouteService) {
        final Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators = new ArrayList<>();
        // 种子地址就是获取最基础地址信息的服务 比如节点刚启动时 完全不知道整个集群的情况  这时就会先从种子服务器上获取基础信息
        final Map<String, Supplier<SeedHostsProvider>> hostProviders = new HashMap<>();

        // 一种是基于文件获取种子信息 还有一种是基于配置项获取种子信息
        hostProviders.put("settings", () -> new SettingsBasedSeedHostsProvider(settings, transportService));
        hostProviders.put("file", () -> new FileBasedSeedHostsProvider(configFile));

        // 选举策略 应该是决定当集群中节点达到多少数量才选举成功吧 默认就是超过1/2
        final Map<String, ElectionStrategy> electionStrategies = new HashMap<>();
        electionStrategies.put(DEFAULT_ELECTION_STRATEGY, ElectionStrategy.DEFAULT_INSTANCE);

        // TODO 先忽略插件
        for (DiscoveryPlugin plugin : plugins) {
            plugin.getSeedHostProviders(transportService, networkService).forEach((key, value) -> {
                if (hostProviders.put(key, value) != null) {
                    throw new IllegalArgumentException("Cannot register seed provider [" + key + "] twice");
                }
            });
            BiConsumer<DiscoveryNode, ClusterState> joinValidator = plugin.getJoinValidator();
            if (joinValidator != null) {
                joinValidators.add(joinValidator);
            }
            plugin.getElectionStrategies().forEach((key, value) -> {
                if (electionStrategies.put(key, value) != null) {
                    throw new IllegalArgumentException("Cannot register election strategy [" + key + "] twice");
                }
            });
        }

        // 用于选举任务时 发现集群初始列表的对象  至少会追加基于配置文件的发现对象
        List<String> seedProviderNames = DISCOVERY_SEED_PROVIDERS_SETTING.get(settings);
        // for bwc purposes, add settings provider even if not explicitly specified
        if (seedProviderNames.contains("settings") == false) {
            List<String> extendedSeedProviderNames = new ArrayList<>();
            extendedSeedProviderNames.add("settings");
            extendedSeedProviderNames.addAll(seedProviderNames);
            seedProviderNames = extendedSeedProviderNames;
        }

        final Set<String> missingProviderNames = new HashSet<>(seedProviderNames);
        missingProviderNames.removeAll(hostProviders.keySet());
        if (missingProviderNames.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown seed providers " + missingProviderNames);
        }

        // 通过这些名字找到对应的 seedProvider
        List<SeedHostsProvider> filteredSeedProviders = seedProviderNames.stream()
            .map(hostProviders::get).map(Supplier::get).collect(Collectors.toList());

        String discoveryType = DISCOVERY_TYPE_SETTING.get(settings);

        // 将List<SeedHostsProvider> 合并成一个 SeedHostsProvider
        final SeedHostsProvider seedHostsProvider = hostsResolver -> {
            final List<TransportAddress> addresses = new ArrayList<>();
            for (SeedHostsProvider provider : filteredSeedProviders) {
                addresses.addAll(provider.getSeedAddresses(hostsResolver));
            }
            return Collections.unmodifiableList(addresses);
        };

        final ElectionStrategy electionStrategy = electionStrategies.get(ELECTION_STRATEGY_SETTING.get(settings));
        if (electionStrategy == null) {
            throw new IllegalArgumentException("Unknown election strategy " + ELECTION_STRATEGY_SETTING.get(settings));
        }

        if (ZEN2_DISCOVERY_TYPE.equals(discoveryType) || SINGLE_NODE_DISCOVERY_TYPE.equals(discoveryType)) {
            discovery = new Coordinator(NODE_NAME_SETTING.get(settings),
                settings, clusterSettings,
                transportService, namedWriteableRegistry, allocationService, masterService, gatewayMetaState::getPersistedState,
                seedHostsProvider, clusterApplier, joinValidators, new Random(Randomness.get().nextLong()), rerouteService,
                electionStrategy);
        } else {
            throw new IllegalArgumentException("Unknown discovery type [" + discoveryType + "]");
        }

        logger.info("using discovery type [{}] and seed hosts providers {}", discoveryType, seedProviderNames);
    }

    public static boolean isSingleNodeDiscovery(Settings settings) {
        return SINGLE_NODE_DISCOVERY_TYPE.equals(DISCOVERY_TYPE_SETTING.get(settings));
    }

    public Discovery getDiscovery() {
        return discovery;
    }
}
