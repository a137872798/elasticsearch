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
package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.isIndexVerifiedBeforeClosed;

/**
 * This class acts as a functional wrapper around the {@code index.auto_expand_replicas} setting.
 * This setting or rather it's value is expanded into a min and max value which requires special handling
 * based on the number of datanodes in the cluster. This class handles all the parsing and streamlines the access to these values.
 * 基于相关信息自动计算应当创建多少副本
 */
public final class AutoExpandReplicas {
    // the value we recognize in the "max" position to mean all the nodes
    private static final String ALL_NODES_VALUE = "all";

    /**
     * 默认情况  某个索引的副本数量为0
     */
    private static final AutoExpandReplicas FALSE_INSTANCE = new AutoExpandReplicas(0, 0, false);

    public static final Setting<AutoExpandReplicas> SETTING = new Setting<>(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "false",
        AutoExpandReplicas::parse, Property.Dynamic, Property.IndexScope);

    /**
     * 将str 转换成描述自动拓展分片数量的相关实体
     * @param value
     * @return
     */
    private static AutoExpandReplicas parse(String value) {
        final int min;
        final int max;
        // 如果是 "false"代表不进行分配
        if (Booleans.isFalse(value)) {
            return FALSE_INSTANCE;
        }
        // 推测是这种格式  min-max  一个范围的数字
        final int dash = value.indexOf('-');
        if (-1 == dash) {
            throw new IllegalArgumentException("failed to parse [" + IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS +
                "] from value: [" + value + "] at index " + dash);
        }
        final String sMin = value.substring(0, dash);
        try {
            min = Integer.parseInt(sMin);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("failed to parse [" + IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS +
                "] from value: [" + value + "] at index "  + dash, e);
        }
        String sMax = value.substring(dash + 1);
        // 代表副本数没有上限
        if (sMax.equals(ALL_NODES_VALUE)) {
            max = Integer.MAX_VALUE;
        } else {
            try {
                max = Integer.parseInt(sMax);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("failed to parse [" + IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS +
                    "] from value: [" + value + "] at index "  + dash, e);
            }
        }
        return new AutoExpandReplicas(min, max, true);
    }

    /**
     * 最少创建多少分片
     */
    private final int minReplicas;
    /**
     * 最多创建多少分片
     */
    private final int maxReplicas;
    /**
     * 该对象是否处于可用状态
     */
    private final boolean enabled;

    private AutoExpandReplicas(int minReplicas, int maxReplicas, boolean enabled) {
        if (minReplicas > maxReplicas) {
            throw new IllegalArgumentException("[" + IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS +
                "] minReplicas must be =< maxReplicas but wasn't " + minReplicas + " > "  + maxReplicas);
        }
        this.minReplicas = minReplicas;
        this.maxReplicas = maxReplicas;
        this.enabled = enabled;
    }

    int getMinReplicas() {
        return minReplicas;
    }

    int getMaxReplicas(int numDataNodes) {
        return Math.min(maxReplicas, numDataNodes-1);
    }

    /**
     * 计算出一个最合适的分片数量  实际上就是通过deciders对象检测该index允许分配在哪些节点上
     * @param indexMetadata  本次要预估的索引的元数据信息
     * @param allocation  此时集群内所有分片的分配情况
     * @return
     */
    private OptionalInt getDesiredNumberOfReplicas(IndexMetadata indexMetadata, RoutingAllocation allocation) {
        // 首先确保此时该对象处于可用状态
        if (enabled) {
            int numMatchingDataNodes = 0;
            // Only start using new logic once all nodes are migrated to 7.6.0, avoiding disruption during an upgrade
            if (allocation.nodes().getMinNodeVersion().onOrAfter(Version.V_7_6_0)) {
                for (ObjectCursor<DiscoveryNode> cursor : allocation.nodes().getDataNodes().values()) {
                    // 检测某个节点是否应该存在该索引的分片  将集群中允许存在该分片的所有节点数加起来
                    // 从 ES内置的 Decision来看
                    Decision decision = allocation.deciders().shouldAutoExpandToNode(indexMetadata, cursor.value, allocation);
                    if (decision.type() != Decision.Type.NO) {
                        numMatchingDataNodes ++;
                    }
                }
            } else {
                // TODO 忽略旧代码
                numMatchingDataNodes = allocation.nodes().getDataNodes().size();
            }

            final int min = getMinReplicas();
            // 生成副本的数量 最大为 numMatchingDataNodes -1  最小为 maxReplicas
            final int max = getMaxReplicas(numMatchingDataNodes);
            int numberOfReplicas = numMatchingDataNodes - 1;
            if (numberOfReplicas < min) {
                numberOfReplicas = min;
            } else if (numberOfReplicas > max) {
                numberOfReplicas = max;
            }

            // 确保此时分片数在合理范围内
            if (numberOfReplicas >= min && numberOfReplicas <= max) {
                return OptionalInt.of(numberOfReplicas);
            }
        }
        return OptionalInt.empty();
    }

    @Override
    public String toString() {
        return enabled ? minReplicas + "-" + maxReplicas : "false";
    }

    /**
     * Checks if the are replicas with the auto-expand feature that need to be adapted.
     * Returns a map of updates, which maps the indices to be updated to the desired number of replicas.
     * The map has the desired number of replicas as key and the indices to update as value, as this allows the result
     * of this method to be directly applied to RoutingTable.Builder#updateNumberOfReplicas.
     * 根据当前所有分片的分配情况  为每个索引计算最合适的分片数量
     */
    public static Map<Integer, List<String>> getAutoExpandReplicaChanges(Metadata metadata, RoutingAllocation allocation) {
        // 存储所有索引合适的副本数  因为多个index可能对应相同的副本数 所以副本数作为key
        Map<Integer, List<String>> nrReplicasChanged = new HashMap<>();

        for (final IndexMetadata indexMetadata : metadata) {
            // 首先确保此时index处于打开状态  或者开启了即使处于Close状态也要先分配的配置
            if (indexMetadata.getState() == IndexMetadata.State.OPEN || isIndexVerifiedBeforeClosed(indexMetadata)) {
                // 这个对象描述了某个index应当包含的最小and最大副本数
                AutoExpandReplicas autoExpandReplicas = SETTING.get(indexMetadata.getSettings());
                // 计算一个最合适的副本数
                autoExpandReplicas.getDesiredNumberOfReplicas(indexMetadata, allocation).ifPresent(numberOfReplicas -> {
                    if (numberOfReplicas != indexMetadata.getNumberOfReplicas()) {
                        nrReplicasChanged.computeIfAbsent(numberOfReplicas, ArrayList::new).add(indexMetadata.getIndex().getName());
                    }
                });
            }
        }
        return nrReplicasChanged;
    }
}


