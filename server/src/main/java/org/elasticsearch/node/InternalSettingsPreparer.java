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

package org.elasticsearch.node;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class InternalSettingsPreparer {

    // TODO: refactor this method out, it used to exist for the transport client
    public static Settings prepareSettings(Settings input) {
        Settings.Builder output = Settings.builder();
        initializeSettings(output, input, Collections.emptyMap());
        finalizeSettings(output, () -> null);
        return output.build();
    }

    /**
     * Prepares the settings by gathering all elasticsearch system properties, optionally loading the configuration settings.
     *
     * @param input      the custom settings to use; these are not overwritten by settings in the configuration file   基础配置信息
     * @param properties map of properties key/value pairs (usually from the command-line)    从cli中解析出的配置信息
     * @param configPath path to config directory; (use null to indicate the default)   配置文件地址
     * @param defaultNodeName supplier for the default node.name if the setting isn't defined  获取当前应用所在的hostName 并会作为nodeName
     * @return the {@link Environment}
     */
    public static Environment prepareEnvironment(Settings input, Map<String, String> properties,
            Path configPath, Supplier<String> defaultNodeName) {
        // just create enough settings to build the environment, to get the config dir
        // 创建一个生成 settings的builder对象
        Settings.Builder output = Settings.builder();
        // 将基础配置 以及从cli中解析出来的配置合并
        initializeSettings(output, input, properties);
        Environment environment = new Environment(output.build(), configPath);

        if (Files.exists(environment.configFile().resolve("elasticsearch.yaml"))) {
            throw new SettingsException("elasticsearch.yaml was deprecated in 5.5.0 and must be renamed to elasticsearch.yml");
        }

        if (Files.exists(environment.configFile().resolve("elasticsearch.json"))) {
            throw new SettingsException("elasticsearch.json was deprecated in 5.5.0 and must be converted to elasticsearch.yml");
        }

        output = Settings.builder(); // start with a fresh output
        Path path = environment.configFile().resolve("elasticsearch.yml");
        if (Files.exists(path)) {
            try {
                output.loadFromPath(path);
            } catch (IOException e) {
                throw new SettingsException("Failed to load settings from " + path.toString(), e);
            }
        }

        // re-initialize settings now that the config file has been loaded
        initializeSettings(output, input, properties);
        finalizeSettings(output, defaultNodeName);

        return new Environment(output.build(), configPath);
    }

    /**
     * Initializes the builder with the given input settings, and applies settings from the specified map (these settings typically come
     * from the command line).
     *
     * @param output the settings builder to apply the input and default settings to   用于整合2个配置
     * @param input the input settings         基础配置
     * @param esSettings a map from which to apply settings        从cli中解析出来的配置
     */
    static void initializeSettings(final Settings.Builder output, final Settings input, final Map<String, String> esSettings) {
        output.put(input);
        // 将map中的数据填充到 settings中
        output.putProperties(esSettings, Function.identity());
        // 如果配置项中存在占位符 将占位符替换成实际的参数值
        output.replacePropertyPlaceholders();
    }

    /**
     * Finish preparing settings by replacing forced settings and any defaults that need to be added.
     */
    private static void finalizeSettings(Settings.Builder output, Supplier<String> defaultNodeName) {
        // allow to force set properties based on configuration of the settings provided
        List<String> forcedSettings = new ArrayList<>();
        for (String setting : output.keys()) {
            if (setting.startsWith("force.")) {
                forcedSettings.add(setting);
            }
        }
        for (String forcedSetting : forcedSettings) {
            String value = output.remove(forcedSetting);
            output.put(forcedSetting.substring("force.".length()), value);
        }
        output.replacePropertyPlaceholders();

        // put the cluster and node name if they aren't set
        if (output.get(ClusterName.CLUSTER_NAME_SETTING.getKey()) == null) {
            output.put(ClusterName.CLUSTER_NAME_SETTING.getKey(), ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY).value());
        }
        if (output.get(Node.NODE_NAME_SETTING.getKey()) == null) {
            output.put(Node.NODE_NAME_SETTING.getKey(), defaultNodeName.get());
        }
    }
}
