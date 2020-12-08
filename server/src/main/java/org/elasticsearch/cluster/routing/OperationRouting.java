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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.ResponseCollectorService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 该对象提供了寻找路由表的api
 */
public class OperationRouting {

    public static final Setting<Boolean> USE_ADAPTIVE_REPLICA_SELECTION_SETTING =
            Setting.boolSetting("cluster.routing.use_adaptive_replica_selection", true,
                    Setting.Property.Dynamic, Setting.Property.NodeScope);

    /**
     * 什么叫自适应的复制选择
     */
    private boolean useAdaptiveReplicaSelection;

    /**
     *
     * @param settings   从配置文件中抽取出来的配置
     * @param clusterSettings   集群相关配置
     */
    public OperationRouting(Settings settings, ClusterSettings clusterSettings) {
        this.useAdaptiveReplicaSelection = USE_ADAPTIVE_REPLICA_SELECTION_SETTING.get(settings);
        // 设置一个配置变化处理器  当master节点修改集群配置时 会通知到其他节点 这时根据 settingsUpdateConsumer 处理更新的配置
        clusterSettings.addSettingsUpdateConsumer(USE_ADAPTIVE_REPLICA_SELECTION_SETTING, this::setUseAdaptiveReplicaSelection);
    }

    void setUseAdaptiveReplicaSelection(boolean useAdaptiveReplicaSelection) {
        this.useAdaptiveReplicaSelection = useAdaptiveReplicaSelection;
    }

    /**
     * 获取index 下某个 shardId 对应的所有分片
     * @param clusterState
     * @param index
     * @param id  req.id
     * @param routing
     * @return
     */
    public ShardIterator indexShards(ClusterState clusterState, String index, String id, @Nullable String routing) {
        return shards(clusterState, index, id, routing).shardsIt();
    }

    /**
     *
     * @param clusterState
     * @param index
     * @param id
     * @param routing
     * @param preference  偏好返回什么样的分片
     * @return
     */
    public ShardIterator getShards(ClusterState clusterState, String index, String id, @Nullable String routing,
                                   @Nullable String preference) {
        return preferenceActiveShardIterator(shards(clusterState, index, id, routing), clusterState.nodes().getLocalNodeId(),
            clusterState.nodes(), preference, null, null);
    }

    public ShardIterator getShards(ClusterState clusterState, String index, int shardId, @Nullable String preference) {
        final IndexShardRoutingTable indexShard = clusterState.getRoutingTable().shardRoutingTable(index, shardId);
        return preferenceActiveShardIterator(indexShard, clusterState.nodes().getLocalNodeId(), clusterState.nodes(),
            preference, null, null);
    }

    /**
     * 寻找一组分片
     * @param clusterState
     * @param concreteIndices  传入多个索引 每个索引都有对应的一些分片
     * @param routing  这是一个查询范围  key对应索引 value对应索引所在的路由的范围
     * @param preference  偏向于使用哪个结果
     * @return
     */
    public GroupShardsIterator<ShardIterator> searchShards(ClusterState clusterState,
                                                           String[] concreteIndices,
                                                           @Nullable Map<String, Set<String>> routing,
                                                           @Nullable String preference) {
        return searchShards(clusterState, concreteIndices, routing, preference, null, null);
    }


    /**
     * 根据指定的一组索引 返回分片数据    GroupShardsIterator 就是多个迭代器的组合对象
     * @param clusterState
     * @param concreteIndices 相关的所有索引名 不包含别名
     * @param routing  这是查询范围
     * @param preference
     * @param collectorService
     * @param nodeCounts
     * @return
     */
    public GroupShardsIterator<ShardIterator> searchShards(ClusterState clusterState,
                                                           String[] concreteIndices,
                                                           @Nullable Map<String, Set<String>> routing,
                                                           @Nullable String preference,
                                                           @Nullable ResponseCollectorService collectorService,
                                                           @Nullable Map<String, Long> nodeCounts) {
        // 通过传入的routingMap信息 计算shardId 并将相关的路由表取出来 如果没有条件限制 就是返回所有的路由表信息
        final Set<IndexShardRoutingTable> shards = computeTargetedShards(clusterState, concreteIndices, routing);
        final Set<ShardIterator> set = new HashSet<>(shards.size());
        for (IndexShardRoutingTable shard : shards) {
            // 通过偏向的分片进一步做过滤
            ShardIterator iterator = preferenceActiveShardIterator(shard,
                    clusterState.nodes().getLocalNodeId(), clusterState.nodes(), preference, collectorService, nodeCounts);
            if (iterator != null) {
                set.add(iterator);
            }
        }
        return GroupShardsIterator.sortAndCreate(new ArrayList<>(set));
    }

    private static final Map<String, Set<String>> EMPTY_ROUTING = Collections.emptyMap();

    /**
     * 返回concreteIndices 相关的所有索引的分片  每个分片会分配到一个node上 换言之就知道了该index在整个集群的分布情况
     * @param clusterState  当前集群状态
     * @param concreteIndices   相关的所有索引
     * @param routing  以index为key 存储了一组路由信息 作为查询条件
     * @return
     */
    private Set<IndexShardRoutingTable> computeTargetedShards(ClusterState clusterState, String[] concreteIndices,
                                                              @Nullable Map<String, Set<String>> routing) {
        routing = routing == null ? EMPTY_ROUTING : routing; // just use an empty map
        final Set<IndexShardRoutingTable> set = new HashSet<>();
        // we use set here and not list since we might get duplicates
        for (String index : concreteIndices) {
            // 从元数据中获取索引相关的路由表  记录了这个索引的所有分片在哪些节点上
            final IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
            final IndexMetadata indexMetadata = indexMetadata(clusterState, index);

            // 路由信息可以转换成分片id  每个分片id 对应一个路由表和多个分片
            final Set<String> effectiveRouting = routing.get(index);
            if (effectiveRouting != null) {
                for (String r : effectiveRouting) {
                    final int routingPartitionSize = indexMetadata.getRoutingPartitionSize();
                    // 从路由表中找到与 r 匹配的路由信息 并设置到set中
                    for (int partitionOffset = 0; partitionOffset < routingPartitionSize; partitionOffset++) {
                        // calculateScaledShardId(indexMetadata, r, partitionOffset) 计算shardId
                        set.add(RoutingTable.shardRoutingTable(indexRouting, calculateScaledShardId(indexMetadata, r, partitionOffset)));
                    }
                }
            } else {
                // 当没有指定查询范围时 全部范围
                for (IndexShardRoutingTable indexShard : indexRouting) {
                    set.add(indexShard);
                }
            }
        }
        return set;
    }

    /**
     * @param indexShard   某个分片
     * @param localNodeId    当前进程对应的节点
     * @param nodes      当前集群内所有的节点
     * @param preference    偏向于选择哪些分片
     * @param collectorService   采集响应信息的
     * @param nodeCounts
     * @return
     */
    private ShardIterator preferenceActiveShardIterator(IndexShardRoutingTable indexShard, String localNodeId,
                                                        DiscoveryNodes nodes, @Nullable String preference,
                                                        @Nullable ResponseCollectorService collectorService,
                                                        @Nullable Map<String, Long> nodeCounts) {
        // 当没有指定偏好信息时 将有效的分片包装成一个迭代器
        if (preference == null || preference.isEmpty()) {
            return shardRoutings(indexShard, nodes, collectorService, nodeCounts);
        }
        // 要求 preference 的首个字符必须是 _ 才处理
        if (preference.charAt(0) == '_') {
            // 解析偏好信息
            Preference preferenceType = Preference.parse(preference);
            if (preferenceType == Preference.SHARDS) {
                // starts with _shards, so execute on specific ones
                int index = preference.indexOf('|');

                // 截取 _shards 到 | 的部分
                String shards;
                if (index == -1) {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1);
                } else {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1, index);
                }
                // 代表指定了要获取哪些分片id 对应的数据
                String[] ids = Strings.splitStringByCommaToArray(shards);
                boolean found = false;
                // 本次分片是否在偏好的分片内
                for (String id : ids) {
                    if (Integer.parseInt(id) == indexShard.shardId().id()) {
                        found = true;
                        break;
                    }
                }
                // 没有找到合适的分片 直接返回
                if (!found) {
                    return null;
                }
                // no more preference
                // 代表只有 SHARDS 一种限制 那么此时可以返回
                if (index == -1 || index == preference.length() - 1) {
                    return shardRoutings(indexShard, nodes, collectorService, nodeCounts);
                } else {
                    // update the preference and continue
                    // 满足了第一个条件 之后匹配其他条件
                    preference = preference.substring(index + 1);
                }
            }

            // 解析非 Preference.SHARDS
            preferenceType = Preference.parse(preference);
            switch (preferenceType) {
                // 代表指定了某些节点
                case PREFER_NODES:
                    final Set<String> nodesIds =
                            Arrays.stream(
                                    preference.substring(Preference.PREFER_NODES.type().length() + 1).split(",")
                            ).collect(Collectors.toSet());
                    // 这里不会过滤掉未命中nodeId的数据 只是会将命中的排在前面
                    return indexShard.preferNodeActiveInitializingShardsIt(nodesIds);
                case LOCAL:
                    return indexShard.preferNodeActiveInitializingShardsIt(Collections.singleton(localNodeId));
                // 仅返回命中nodeId的分片
                case ONLY_LOCAL:
                    return indexShard.onlyNodeActiveInitializingShardsIt(localNodeId);
                case ONLY_NODES:
                    String nodeAttributes = preference.substring(Preference.ONLY_NODES.type().length() + 1);
                    return indexShard.onlyNodeSelectorActiveInitializingShardsIt(nodeAttributes.split(","), nodes);
                default:
                    throw new IllegalArgumentException("unknown preference [" + preferenceType + "]");
            }
        }
        // if not, then use it as the index
        int routingHash = 31 * Murmur3HashFunction.hash(preference) + indexShard.shardId.hashCode();
        // 使用hash 打乱分片顺序
        return indexShard.activeInitializingShardsIt(routingHash);
    }

    /**
     * 将相关的路由信息包装成迭代器
     * @param indexShard
     * @param nodes
     * @param collectorService  可空
     * @param nodeCounts  可空
     * @return
     */
    private ShardIterator shardRoutings(IndexShardRoutingTable indexShard, DiscoveryNodes nodes,
            @Nullable ResponseCollectorService collectorService, @Nullable Map<String, Long> nodeCounts) {
        // 默认为true  通过collectorService做一些数据统计 便于之后自适应的调整
        if (useAdaptiveReplicaSelection) {
            return indexShard.activeInitializingShardsRankedIt(collectorService, nodeCounts);
        } else {
            // 返回活跃状态 以及初始状态的分片
            return indexShard.activeInitializingShardsRandomIt();
        }
    }

    /**
     * 从集群状态中获取某个索引的路由表  路由表一开始就存储在元数据中
     * @param clusterState
     * @param index
     * @return
     */
    protected IndexRoutingTable indexRoutingTable(ClusterState clusterState, String index) {
        IndexRoutingTable indexRouting = clusterState.routingTable().index(index);
        if (indexRouting == null) {
            throw new IndexNotFoundException(index);
        }
        return indexRouting;
    }

    /**
     * 获取clusterState中 有关某个索引的元数据信息
     * @param clusterState
     * @param index
     * @return
     */
    protected IndexMetadata indexMetadata(ClusterState clusterState, String index) {
        IndexMetadata indexMetadata = clusterState.metadata().index(index);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(index);
        }
        return indexMetadata;
    }

    /**
     * 获取目标索引相关的所有迭代器
     * @param clusterState 此时的集群状态 内部包含了 metadata 记录此时分片信息
     * @param index  索引名
     * @param id
     * @param routing
     * @return
     */
    protected IndexShardRoutingTable shards(ClusterState clusterState, String index, String id, String routing) {
        // generateShardId 就是通过一种接近随机的方式获取分片id
        int shardId = generateShardId(indexMetadata(clusterState, index), id, routing);
        // 通过路由表对象找到 目标索引下有关某个分片的路由表
        return clusterState.getRoutingTable().shardRoutingTable(index, shardId);
    }

    /**
     * 将相关信息包装成分片id
     * @param clusterState
     * @param index
     * @param id
     * @param routing
     * @return
     */
    public ShardId shardId(ClusterState clusterState, String index, String id, @Nullable String routing) {
        IndexMetadata indexMetadata = indexMetadata(clusterState, index);
        return new ShardId(indexMetadata.getIndex(), generateShardId(indexMetadata, id, routing));
    }

    /**
     * 生成分片id
     * @param indexMetadata  索引元数据信息
     * @param id   一个uuid  当处理一个 bulkReq时 会为它生成一个随机id
     * @param routing 路由信息
     * @return
     */
    public static int generateShardId(IndexMetadata indexMetadata, @Nullable String id, @Nullable String routing) {
        final String effectiveRouting;
        final int partitionOffset;

        if (routing == null) {
            assert(indexMetadata.isRoutingPartitionedIndex() == false) : "A routing value is required for gets from a partitioned index";
            effectiveRouting = id;
        } else {
            effectiveRouting = routing;
        }

        // 代表这个索引还有多个分区  TODO 先忽略分区的概念
        if (indexMetadata.isRoutingPartitionedIndex()) {
            // 通过hash计算后 获得一个分区的offset
            partitionOffset = Math.floorMod(Murmur3HashFunction.hash(id), indexMetadata.getRoutingPartitionSize());
        } else {
            // we would have still got 0 above but this check just saves us an unnecessary hash calculation
            partitionOffset = 0;
        }

        return calculateScaledShardId(indexMetadata, effectiveRouting, partitionOffset);
    }

    /**
     *
     * @param indexMetadata  索引元数据
     * @param effectiveRouting   有效的路由信息
     * @param partitionOffset   索引所在分区的偏移量 某个索引可能存在多个分区  先假设始终为0
     * @return
     */
    private static int calculateScaledShardId(IndexMetadata indexMetadata, String effectiveRouting, int partitionOffset) {
        final int hash = Murmur3HashFunction.hash(effectiveRouting) + partitionOffset;

        // we don't use IMD#getNumberOfShards since the index might have been shrunk such that we need to use the size
        // of original index to hash documents
        return Math.floorMod(hash, indexMetadata.getRoutingNumShards()) / indexMetadata.getRoutingFactor();
    }

}
