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

package org.elasticsearch.common.settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A module that binds the provided settings to the {@link Settings} interface.
 * 该模块管理所有的 settings
 */
public class SettingsModule implements Module {
    private static final Logger logger = LogManager.getLogger(SettingsModule.class);

    private final Settings settings;
    private final Set<String> settingsFilterPattern = new HashSet<>();
    private final Map<String, Setting<?>> nodeSettings = new HashMap<>();
    private final Map<String, Setting<?>> indexSettings = new HashMap<>();
    private final Set<Setting<?>> consistentSettings = new HashSet<>();
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;

    public SettingsModule(Settings settings, Setting<?>... additionalSettings) {
        this(settings, Arrays.asList(additionalSettings), Collections.emptyList(), Collections.emptySet());
    }

    /**
     *
     * @param settings   从es环境/配置文件 中解析出来的配置
     * @param additionalSettings   后面加入的额外配置 比如插件的/线程池的
     * @param settingsFilter      插件服务会有一组配置过滤器
     * @param settingUpgraders    插件服务中支持动态更新的配置项
     */
    public SettingsModule(
            Settings settings,
            List<Setting<?>> additionalSettings,
            List<String> settingsFilter,
            Set<SettingUpgrader<?>> settingUpgraders) {
        this(
            settings,
            additionalSettings,
            settingsFilter,
            settingUpgraders,
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
    }


    /**
     *
     * @param settings      从配置文件中获取的配置
     * @param additionalSettings     插件定义的额外配置
     * @param settingsFilter        对应 pluginsService.getPluginSettingsFilter()
     * @param settingUpgraders      插件升级相关的配置
     * @param registeredClusterSettings   集群相关的配置
     * @param registeredIndexSettings    索引相关的配置
     */
    SettingsModule(
        final Settings settings,
        final List<Setting<?>> additionalSettings,
        final List<String> settingsFilter,
        final Set<SettingUpgrader<?>> settingUpgraders,
        final Set<Setting<?>> registeredClusterSettings,
        final Set<Setting<?>> registeredIndexSettings) {
        this.settings = settings;
        for (Setting<?> setting : registeredClusterSettings) {
            registerSetting(setting);
        }
        for (Setting<?> setting : registeredIndexSettings) {
            registerSetting(setting);
        }

        for (Setting<?> setting : additionalSettings) {
            registerSetting(setting);
        }
        for (String filter : settingsFilter) {
            registerSettingsFilter(filter);
        }
        // 除了传入的 集群升级配置外 还添加了内置的集群升级配置
        final Set<SettingUpgrader<?>> clusterSettingUpgraders = new HashSet<>();
        for (final SettingUpgrader<?> settingUpgrader : ClusterSettings.BUILT_IN_SETTING_UPGRADERS) {
            assert settingUpgrader.getSetting().hasNodeScope() : settingUpgrader.getSetting().getKey();
            final boolean added = clusterSettingUpgraders.add(settingUpgrader);
            assert added : settingUpgrader.getSetting().getKey();
        }
        for (final SettingUpgrader<?> settingUpgrader : settingUpgraders) {
            assert settingUpgrader.getSetting().hasNodeScope() : settingUpgrader.getSetting().getKey();
            final boolean added = clusterSettingUpgraders.add(settingUpgrader);
            assert added : settingUpgrader.getSetting().getKey();
        }
        // 分别生成 index/cluster相关的配置对象
        this.indexScopedSettings = new IndexScopedSettings(settings, new HashSet<>(this.indexSettings.values()));
        this.clusterSettings = new ClusterSettings(settings, new HashSet<>(this.nodeSettings.values()), clusterSettingUpgraders);
        Settings indexSettings = settings.filter((s) -> s.startsWith("index.") && clusterSettings.get(s) == null);
        if (indexSettings.isEmpty() == false) {
            try {
                String separator = IntStream.range(0, 85).mapToObj(s -> "*").collect(Collectors.joining("")).trim();
                StringBuilder builder = new StringBuilder();
                builder.append(System.lineSeparator());
                builder.append(separator);
                builder.append(System.lineSeparator());
                builder.append("Found index level settings on node level configuration.");
                builder.append(System.lineSeparator());
                builder.append(System.lineSeparator());
                int count = 0;
                for (String word : ("Since elasticsearch 5.x index level settings can NOT be set on the nodes configuration like " +
                    "the elasticsearch.yaml, in system properties or command line arguments." +
                    "In order to upgrade all indices the settings must be updated via the /${index}/_settings API. " +
                    "Unless all settings are dynamic all indices must be closed in order to apply the upgrade" +
                    "Indices created in the future should use index templates to set default values."
                ).split(" ")) {
                    if (count + word.length() > 85) {
                        builder.append(System.lineSeparator());
                        count = 0;
                    }
                    count += word.length() + 1;
                    builder.append(word).append(" ");
                }

                builder.append(System.lineSeparator());
                builder.append(System.lineSeparator());
                builder.append("Please ensure all required values are updated on all indices by executing: ");
                builder.append(System.lineSeparator());
                builder.append(System.lineSeparator());
                builder.append("curl -XPUT 'http://localhost:9200/_all/_settings?preserve_existing=true' -d '");
                try (XContentBuilder xContentBuilder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    xContentBuilder.prettyPrint();
                    xContentBuilder.startObject();
                    indexSettings.toXContent(xContentBuilder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
                    xContentBuilder.endObject();
                    builder.append(Strings.toString(xContentBuilder));
                }
                builder.append("'");
                builder.append(System.lineSeparator());
                builder.append(separator);
                builder.append(System.lineSeparator());

                logger.warn(builder.toString());
                throw new IllegalArgumentException("node settings must not contain any index level settings");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // by now we are fully configured, lets check node level settings for unregistered index settings
        clusterSettings.validate(settings, true);
        // 将所有需要过滤的settings 包装成一个 settingsFilter对象
        this.settingsFilter = new SettingsFilter(settingsFilterPattern);
    }

    /**
     * 在使用guice进行注入时 绑定了 Settings  SettingsFilter  ClusterSettings  IndexScopedSettings 的实例
     * @param binder
     */
    @Override
    public void configure(Binder binder) {
        binder.bind(Settings.class).toInstance(settings);
        binder.bind(SettingsFilter.class).toInstance(settingsFilter);
        binder.bind(ClusterSettings.class).toInstance(clusterSettings);
        binder.bind(IndexScopedSettings.class).toInstance(indexScopedSettings);
    }

    /**
     * Registers a new setting. This method should be used by plugins in order to expose any custom settings the plugin defines.
     * Unless a setting is registered the setting is unusable. If a setting is never the less specified the node will reject
     * the setting during startup.
     * 注册某个配置项
     */
    private void registerSetting(Setting<?> setting) {
        if (setting.getKey().contains(".") == false) {
            throw new IllegalArgumentException("setting [" + setting.getKey() + "] is not in any namespace, its name must contain a dot");
        }
        // 代表该配置需要被过滤一下
        if (setting.isFiltered()) {
            if (settingsFilterPattern.contains(setting.getKey()) == false) {
                registerSettingsFilter(setting.getKey());
            }
        }
        // 如果该配置是针对 node 或者针对 index的
        // 下面的逻辑就是将settings 按照 prop进行分类  存储到不同的容器中
        if (setting.hasNodeScope() || setting.hasIndexScope()) {
            if (setting.hasNodeScope()) {
                Setting<?> existingSetting = nodeSettings.get(setting.getKey());
                if (existingSetting != null) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                // 设置一致性相关的配置
                if (setting.isConsistent()) {
                    if (setting instanceof Setting.AffixSetting<?>) {
                        if (((Setting.AffixSetting<?>)setting).getConcreteSettingForNamespace("_na_") instanceof SecureSetting<?>) {
                            consistentSettings.add(setting);
                        } else {
                            throw new IllegalArgumentException("Invalid consistent secure setting [" + setting.getKey() + "]");
                        }
                    } else if (setting instanceof SecureSetting<?>) {
                        consistentSettings.add(setting);
                    } else {
                        throw new IllegalArgumentException("Invalid consistent secure setting [" + setting.getKey() + "]");
                    }
                }
                nodeSettings.put(setting.getKey(), setting);
            }
            if (setting.hasIndexScope()) {
                Setting<?> existingSetting = indexSettings.get(setting.getKey());
                if (existingSetting != null) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                if (setting.isConsistent()) {
                    throw new IllegalStateException("Consistent setting [" + setting.getKey() + "] cannot be index scoped");
                }
                indexSettings.put(setting.getKey(), setting);
            }
        } else {
            // 其余情况选择抛出异常
            throw new IllegalArgumentException("No scope found for setting [" + setting.getKey() + "]");
        }
    }

    /**
     * Registers a settings filter pattern that allows to filter out certain settings that for instance contain sensitive information
     * or if a setting is for internal purposes only. The given pattern must either be a valid settings key or a simple regexp pattern.
     */
    private void registerSettingsFilter(String filter) {
        // 进行格式校验
        if (SettingsFilter.isValidPattern(filter) == false) {
            throw new IllegalArgumentException("filter [" + filter +"] is invalid must be either a key or a regex pattern");
        }
        // 拒绝重复加入
        if (settingsFilterPattern.contains(filter)) {
            throw new IllegalArgumentException("filter [" + filter + "] has already been registered");
        }
        settingsFilterPattern.add(filter);
    }

    public Settings getSettings() {
        return settings;
    }

    public IndexScopedSettings getIndexScopedSettings() {
        return indexScopedSettings;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    public Set<Setting<?>> getConsistentSettings() {
        return consistentSettings;
    }

    public SettingsFilter getSettingsFilter() {
        return settingsFilter;
    }

}
