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

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.ComponentTemplateMetadata;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateV2Metadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexAliasesService;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetadataMappingService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.routing.DelayedAllocationService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.RebalanceOnlyWhenActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ResizeAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.RestoreInProgressAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksNodeService;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.script.ScriptMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskResultsService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Configures classes and services that affect the entire cluster.
 * 各个服务会被包装成模块对象 之后模块会被包装成注入点(Injector)
 */
public class ClusterModule extends AbstractModule {

    public static final String BALANCED_ALLOCATOR = "balanced"; // default
    public static final Setting<String> SHARDS_ALLOCATOR_TYPE_SETTING =
        new Setting<>("cluster.routing.allocation.type", BALANCED_ALLOCATOR, Function.identity(), Property.NodeScope);

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final AllocationDeciders allocationDeciders;
    private final AllocationService allocationService;
    private final List<ClusterPlugin> clusterPlugins;
    // pkg private for tests
    final Collection<AllocationDecider> deciderList;
    final ShardsAllocator shardsAllocator;


    /**
     *
     * @param settings
     * @param clusterService   集群服务对象
     * @param clusterPlugins    集群相关插件
     * @param clusterInfoService   集群信息服务对象
     */
    public ClusterModule(Settings settings, ClusterService clusterService, List<ClusterPlugin> clusterPlugins,
                         ClusterInfoService clusterInfoService) {
        this.clusterPlugins = clusterPlugins;
        this.deciderList = createAllocationDeciders(settings, clusterService.getClusterSettings(), clusterPlugins);
        this.allocationDeciders = new AllocationDeciders(deciderList);
        this.shardsAllocator = createShardsAllocator(settings, clusterService.getClusterSettings(), clusterPlugins);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver();
        this.allocationService = new AllocationService(allocationDeciders, shardsAllocator, clusterInfoService);
    }

    public static List<Entry> getNamedWriteables() {
        List<Entry> entries = new ArrayList<>();
        // Cluster State
        registerClusterCustom(entries, SnapshotsInProgress.TYPE, SnapshotsInProgress::new, SnapshotsInProgress::readDiffFrom);
        registerClusterCustom(entries, RestoreInProgress.TYPE, RestoreInProgress::new, RestoreInProgress::readDiffFrom);
        registerClusterCustom(entries, SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress::new,
            SnapshotDeletionsInProgress::readDiffFrom);
        registerClusterCustom(entries, RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress::new,
            RepositoryCleanupInProgress::readDiffFrom);
        // Metadata
        registerMetadataCustom(entries, RepositoriesMetadata.TYPE, RepositoriesMetadata::new, RepositoriesMetadata::readDiffFrom);
        registerMetadataCustom(entries, IngestMetadata.TYPE, IngestMetadata::new, IngestMetadata::readDiffFrom);
        registerMetadataCustom(entries, ScriptMetadata.TYPE, ScriptMetadata::new, ScriptMetadata::readDiffFrom);
        registerMetadataCustom(entries, IndexGraveyard.TYPE, IndexGraveyard::new, IndexGraveyard::readDiffFrom);
        registerMetadataCustom(entries, PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata::new,
            PersistentTasksCustomMetadata::readDiffFrom);
        registerMetadataCustom(entries, ComponentTemplateMetadata.TYPE, ComponentTemplateMetadata::new,
            ComponentTemplateMetadata::readDiffFrom);
        registerMetadataCustom(entries, IndexTemplateV2Metadata.TYPE, IndexTemplateV2Metadata::new,
            IndexTemplateV2Metadata::readDiffFrom);
        registerMetadataCustom(entries, DataStreamMetadata.TYPE, DataStreamMetadata::new, DataStreamMetadata::readDiffFrom);
        // Task Status (not Diffable)
        entries.add(new Entry(Task.Status.class, PersistentTasksNodeService.Status.NAME, PersistentTasksNodeService.Status::new));
        return entries;
    }

    public static List<NamedXContentRegistry.Entry> getNamedXWriteables() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        // Metadata
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(RepositoriesMetadata.TYPE),
            RepositoriesMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IngestMetadata.TYPE),
            IngestMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(ScriptMetadata.TYPE),
            ScriptMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IndexGraveyard.TYPE),
            IndexGraveyard::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(PersistentTasksCustomMetadata.TYPE),
            PersistentTasksCustomMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(ComponentTemplateMetadata.TYPE),
            ComponentTemplateMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IndexTemplateV2Metadata.TYPE),
            IndexTemplateV2Metadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(DataStreamMetadata.TYPE),
            DataStreamMetadata::fromXContent));
        return entries;
    }

    private static <T extends ClusterState.Custom> void registerClusterCustom(List<Entry> entries, String name, Reader<? extends T> reader,
                                                                       Reader<NamedDiff> diffReader) {
        registerCustom(entries, ClusterState.Custom.class, name, reader, diffReader);
    }

    private static <T extends Metadata.Custom> void registerMetadataCustom(List<Entry> entries, String name, Reader<? extends T> reader,
                                                                           Reader<NamedDiff> diffReader) {
        registerCustom(entries, Metadata.Custom.class, name, reader, diffReader);
    }

    private static <T extends NamedWriteable> void registerCustom(List<Entry> entries, Class<T> category, String name,
                                                                  Reader<? extends T> reader, Reader<NamedDiff> diffReader) {
        entries.add(new Entry(category, name, reader));
        entries.add(new Entry(NamedDiff.class, name, diffReader));
    }

    public IndexNameExpressionResolver getIndexNameExpressionResolver() {
        return indexNameExpressionResolver;
    }

    // TODO: this is public so allocation benchmark can access the default deciders...can we do that in another way?
    /**
     * Return a new {@link AllocationDecider} instance with builtin deciders as well as those from plugins.
     * @param clusterPlugins 支持通过插件系统拓展decider  TODO 目前只看ES内置的决策对象
     * 这里添加了一系列的  decider  这些对象决定了 某个shard应该分配到哪个节点  是否允许进行balance等
     */
    public static Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings,
                                                                         List<ClusterPlugin> clusterPlugins) {
        // collect deciders by class so that we can detect duplicates
        Map<Class, AllocationDecider> deciders = new LinkedHashMap<>();
        // 通过UnassignedInfo内部的失败次数 限定重试次数
        addAllocationDecider(deciders, new MaxRetryAllocationDecider());
        // TODO 跟RESIZE 索引有关 虽然不知道是干啥的
        addAllocationDecider(deciders, new ResizeAllocationDecider());
        // 在检测副本能否正常分配前 必须确保primary 处于active状态
        addAllocationDecider(deciders, new ReplicaAfterPrimaryActiveAllocationDecider());
        // 在进行rebalance前 必须确保该shardId 下所有primary replica 都处于active状态
        addAllocationDecider(deciders, new RebalanceOnlyWhenActiveAllocationDecider());
        // 要不全部primary 处于active 状态 要不所有shard(primary replica) 都处于active状态
        addAllocationDecider(deciders, new ClusterRebalanceAllocationDecider(settings, clusterSettings));
        // 允许有部分分片处于 relocation的状态 也可以进行rebalance
        addAllocationDecider(deciders, new ConcurrentRebalanceAllocationDecider(settings, clusterSettings));
        // 通过配置项决定能否进行 allocation/rebalance
        addAllocationDecider(deciders, new EnableAllocationDecider(settings, clusterSettings));
        // 通过版本号来决定是否允许 allocation/rebalance
        addAllocationDecider(deciders, new NodeVersionAllocationDecider());
        // 检测相关节点上匹配的索引是否正在执行快照  是的话返回throttle
        addAllocationDecider(deciders, new SnapshotInProgressAllocationDecider());
        // 检测是否处于恢复状态 并进行限制    快照和 restore好像是匹配的 也就是基于快照进行恢复
        addAllocationDecider(deciders, new RestoreInProgressAllocationDecider());
        // 通过一组自定义的过滤器进行处理
        addAllocationDecider(deciders, new FilterAllocationDecider(settings, clusterSettings));
        // 应该就时通过这个对象来进行去重了 因为在一个node上 同一个shardId 对应的shard只允许出现一次
        addAllocationDecider(deciders, new SameShardAllocationDecider(settings, clusterSettings));
        // 基于当前节点的磁盘负载情况  TODO 磁盘使用率的先不细看了 对架构本身没有侵入性
        addAllocationDecider(deciders, new DiskThresholdDecider(settings, clusterSettings));
        // 该对象会判断是否满足限流条件
        addAllocationDecider(deciders, new ThrottlingAllocationDecider(settings, clusterSettings));
        // 有一个限制值 当node/index 级别的分片超过时 返回NO 否则返回YES
        addAllocationDecider(deciders, new ShardsLimitAllocationDecider(settings, clusterSettings));
        // 通过一些自定义配置进行处理 TODO 不是很重要 先忽略
        addAllocationDecider(deciders, new AwarenessAllocationDecider(settings, clusterSettings));

        clusterPlugins.stream()
            .flatMap(p -> p.createAllocationDeciders(settings, clusterSettings).stream())
            .forEach(d -> addAllocationDecider(deciders, d));

        return deciders.values();
    }

    /** Add the given allocation decider to the given deciders collection, erroring if the class name is already used. */
    private static void addAllocationDecider(Map<Class, AllocationDecider> deciders, AllocationDecider decider) {
        if (deciders.put(decider.getClass(), decider) != null) {
            throw new IllegalArgumentException("Cannot specify allocation decider [" + decider.getClass().getName() + "] twice");
        }
    }

    private static ShardsAllocator createShardsAllocator(Settings settings, ClusterSettings clusterSettings,
                                                         List<ClusterPlugin> clusterPlugins) {
        Map<String, Supplier<ShardsAllocator>> allocators = new HashMap<>();
        allocators.put(BALANCED_ALLOCATOR, () -> new BalancedShardsAllocator(settings, clusterSettings));

        for (ClusterPlugin plugin : clusterPlugins) {
            plugin.getShardsAllocators(settings, clusterSettings).forEach((k, v) -> {
                if (allocators.put(k, v) != null) {
                    throw new IllegalArgumentException("ShardsAllocator [" + k + "] already defined");
                }
            });
        }
        String allocatorName = SHARDS_ALLOCATOR_TYPE_SETTING.get(settings);
        Supplier<ShardsAllocator> allocatorSupplier = allocators.get(allocatorName);
        if (allocatorSupplier == null) {
            throw new IllegalArgumentException("Unknown ShardsAllocator [" + allocatorName + "]");
        }
        return Objects.requireNonNull(allocatorSupplier.get(),
            "ShardsAllocator factory for [" + allocatorName + "] returned null");
    }

    public AllocationService getAllocationService() {
        return allocationService;
    }

    @Override
    protected void configure() {
        bind(GatewayAllocator.class).asEagerSingleton();
        bind(AllocationService.class).toInstance(allocationService);
        bind(ClusterService.class).toInstance(clusterService);
        bind(NodeConnectionsService.class).asEagerSingleton();
        bind(MetadataDeleteIndexService.class).asEagerSingleton();
        bind(MetadataIndexStateService.class).asEagerSingleton();
        bind(MetadataMappingService.class).asEagerSingleton();
        bind(MetadataIndexAliasesService.class).asEagerSingleton();
        bind(MetadataUpdateSettingsService.class).asEagerSingleton();
        bind(MetadataIndexTemplateService.class).asEagerSingleton();
        bind(IndexNameExpressionResolver.class).toInstance(indexNameExpressionResolver);
        bind(DelayedAllocationService.class).asEagerSingleton();
        bind(ShardStateAction.class).asEagerSingleton();
        bind(NodeMappingRefreshAction.class).asEagerSingleton();
        bind(MappingUpdatedAction.class).asEagerSingleton();
        bind(TaskResultsService.class).asEagerSingleton();
        bind(AllocationDeciders.class).toInstance(allocationDeciders);
        bind(ShardsAllocator.class).toInstance(shardsAllocator);
    }

    public void setExistingShardsAllocators(GatewayAllocator gatewayAllocator) {
        final Map<String, ExistingShardsAllocator> existingShardsAllocators = new HashMap<>();
        existingShardsAllocators.put(GatewayAllocator.ALLOCATOR_NAME, gatewayAllocator);

        for (ClusterPlugin clusterPlugin : clusterPlugins) {
            for (Map.Entry<String, ExistingShardsAllocator> existingShardsAllocatorEntry
                : clusterPlugin.getExistingShardsAllocators().entrySet()) {
                final String allocatorName = existingShardsAllocatorEntry.getKey();
                if (existingShardsAllocators.put(allocatorName, existingShardsAllocatorEntry.getValue()) != null) {
                    throw new IllegalArgumentException("ExistingShardsAllocator [" + allocatorName + "] from [" +
                        clusterPlugin.getClass().getName() + "] was already defined");
                }
            }
        }
        allocationService.setExistingShardsAllocators(existingShardsAllocators);
    }

}
