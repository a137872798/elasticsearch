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

package org.elasticsearch.action.admin.cluster.settings;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

import static org.elasticsearch.cluster.ClusterState.builder;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;

/**
 * Updates transient and persistent cluster state settings if there are any changes
 * due to the update.
 * 描述更新了什么样的配置
 */
final class SettingsUpdater {

    /**
     * 更新时使用到的 builder对象
     */
    final Settings.Builder transientUpdates = Settings.builder();
    final Settings.Builder persistentUpdates = Settings.builder();

    /**
     * 当前集群配置
     */
    private final ClusterSettings clusterSettings;

    SettingsUpdater(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    synchronized Settings getTransientUpdates() {
        return transientUpdates.build();
    }

    synchronized Settings getPersistentUpdate() {
        return persistentUpdates.build();
    }

    /**
     * 将一组更新的配置作用在当前集群上
     * @param currentState
     * @param transientToApply
     * @param persistentToApply
     * @param logger
     * @return
     */
    synchronized ClusterState updateSettings(
            final ClusterState currentState, final Settings transientToApply, final Settings persistentToApply, final Logger logger) {
        boolean changed = false;

        /*
         * Our cluster state could have unknown or invalid settings that are known and valid in a previous version of Elasticsearch. We can
         * end up in this situation during a rolling upgrade where the previous version will infect the current version of Elasticsearch
         * with settings that the current version either no longer knows about or now considers to have invalid values. When the current
         * version of Elasticsearch becomes infected with a cluster state containing such settings, we need to skip validating such settings
         * and instead archive them. Consequently, for the current transient and persistent settings in the cluster state we do the
         * following:
         *  - split existing settings instance into two with the known and valid settings in one, and the unknown or invalid in another
         *    (note that existing archived settings are included in the known and valid settings)
         *  - validate the incoming settings update combined with the existing known and valid settings
         *  - merge in the archived unknown or invalid settings
         */
        // 对当前的瞬时配置进行校验
        final Tuple<Settings, Settings> partitionedTransientSettings =
                partitionKnownAndValidSettings(currentState.metadata().transientSettings(), "transient", logger);

        // 当前已知的通过校验的配置 内部包含了 archived配置 和 非archived配置
        final Settings knownAndValidTransientSettings = partitionedTransientSettings.v1();
        final Settings unknownOrInvalidTransientSettings = partitionedTransientSettings.v2();
        // 将此时有效的瞬时配置取出来生成builder
        final Settings.Builder transientSettings = Settings.builder().put(knownAndValidTransientSettings);
        // 根据本次传入的动态配置进行更新   最终结果体现在transientSettings  而涉及到的所有更新配置体现在transientUpdates (该配置与一开始传入的toApply不同 因为有些配置可能是用来删除的)
        changed |= clusterSettings.updateDynamicSettings(transientToApply, transientSettings, transientUpdates, "transient");

        // 针对持久化配置再执行一次
        final Tuple<Settings, Settings> partitionedPersistentSettings =
                partitionKnownAndValidSettings(currentState.metadata().persistentSettings(), "persistent", logger);
        final Settings knownAndValidPersistentSettings = partitionedPersistentSettings.v1();
        final Settings unknownOrInvalidPersistentSettings = partitionedPersistentSettings.v2();
        final Settings.Builder persistentSettings = Settings.builder().put(knownAndValidPersistentSettings);
        changed |= clusterSettings.updateDynamicSettings(persistentToApply, persistentSettings, persistentUpdates, "persistent");

        final ClusterState clusterState;
        if (changed) {
            Settings transientFinalSettings = transientSettings.build();
            Settings persistentFinalSettings = persistentSettings.build();
            // both transient and persistent settings must be consistent by itself we can't allow dependencies to be
            // in either of them otherwise a full cluster restart will break the settings validation
            // 因为之前移除了一些配置 所以现在检测配置的依赖关系是否被破坏
            clusterSettings.validate(transientFinalSettings, true);
            clusterSettings.validate(persistentFinalSettings, true);

            // 更新此时的元数据信息
            Metadata.Builder metadata = Metadata.builder(currentState.metadata())
                    .transientSettings(Settings.builder().put(transientFinalSettings).put(unknownOrInvalidTransientSettings).build())
                    .persistentSettings(Settings.builder().put(persistentFinalSettings).put(unknownOrInvalidPersistentSettings).build());

            ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
            boolean updatedReadOnly = Metadata.SETTING_READ_ONLY_SETTING.get(metadata.persistentSettings())
                    || Metadata.SETTING_READ_ONLY_SETTING.get(metadata.transientSettings());
            // 这里尝试更新block配置
            if (updatedReadOnly) {
                blocks.addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK);
            } else {
                blocks.removeGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK);
            }
            boolean updatedReadOnlyAllowDelete = Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.get(metadata.persistentSettings())
                    || Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.get(metadata.transientSettings());
            if (updatedReadOnlyAllowDelete) {
                blocks.addGlobalBlock(Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
            } else {
                blocks.removeGlobalBlock(Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
            }
            // 根据最新的元数据信息 生成集群配置对象
            clusterState = builder(currentState).metadata(metadata).blocks(blocks).build();
        } else {
            clusterState = currentState;
        }

        /*
         * Now we try to apply things and if they are invalid we fail. This dry run will validate, parse settings, and trigger deprecation
         * logging, but will not actually apply them.
         */
        final Settings settings = clusterState.metadata().settings();
        // 配置发生了变化  触发监听的钩子
        clusterSettings.validateUpdate(settings);

        return clusterState;
    }

    /**
     * Partitions the settings into those that are known and valid versus those that are unknown or invalid. The resulting tuple contains
     * the known and valid settings in the first component and the unknown or invalid settings in the second component. Note that archived
     * settings contained in the settings to partition are included in the first component.
     *
     * @param settings     the settings to partition
     * @param settingsType a string to identify the settings (for logging)
     * @param logger       a logger to sending warnings to
     * @return the partitioned settings
     * 校验已知的配置
     */
    private Tuple<Settings, Settings> partitionKnownAndValidSettings(
            final Settings settings, final String settingsType, final Logger logger) {
        // 如果配置包含了  archived 前缀  代表是存档配置
        final Settings existingArchivedSettings = settings.filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX));
        // 这里是非存档配置
        final Settings settingsExcludingExistingArchivedSettings =
                settings.filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX) == false);
        final Settings settingsWithUnknownOrInvalidArchived = clusterSettings.archiveUnknownOrInvalidSettings(
                settingsExcludingExistingArchivedSettings,
                e -> logUnknownSetting(settingsType, e, logger),
                (e, ex) -> logInvalidSetting(settingsType, e, ex, logger));
        return Tuple.tuple(
                Settings.builder()
                    // 同时插入非存档配置 和存档配置
                        .put(settingsWithUnknownOrInvalidArchived.filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX) == false))
                        .put(existingArchivedSettings)
                        .build(),
                // 在archiveUnknownOrInvalidSettings 中 遇到未知的或者无效的会将key的前缀修改成ARCHIVED 所以这里过滤得到的就是一些异常的settings
                settingsWithUnknownOrInvalidArchived.filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX)));
    }

    private void logUnknownSetting(final String settingType, final Map.Entry<String, String> e, final Logger logger) {
        logger.warn("ignoring existing unknown {} setting: [{}] with value [{}]; archiving", settingType, e.getKey(), e.getValue());
    }

    private void logInvalidSetting(
            final String settingType, final Map.Entry<String, String> e, final IllegalArgumentException ex, final Logger logger) {
        logger.warn(
                (Supplier<?>)
                        () -> new ParameterizedMessage("ignoring existing invalid {} setting: [{}] with value [{}]; archiving",
                                settingType,
                                e.getKey(),
                                e.getValue()),
                ex);
    }

}
