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

import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;

import java.util.EnumSet;

/**
 * 由于不是master节点 导致某些操作被阻塞
 */
public class NoMasterBlockService {

    /**
     * 非master节点的blockId 都是2
     */
    public static final int NO_MASTER_BLOCK_ID = 2;

    // 代表2种阻塞类型
    public static final ClusterBlock NO_MASTER_BLOCK_WRITES = new ClusterBlock(NO_MASTER_BLOCK_ID, "no master", true, false, false,
        RestStatus.SERVICE_UNAVAILABLE, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));
    public static final ClusterBlock NO_MASTER_BLOCK_ALL = new ClusterBlock(NO_MASTER_BLOCK_ID, "no master", true, true, false,
        RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);

    public static final Setting<ClusterBlock> NO_MASTER_BLOCK_SETTING =
        new Setting<>("cluster.no_master_block", "write", NoMasterBlockService::parseNoMasterBlock,
            Property.Dynamic, Property.NodeScope);


    /**
     * 当前节点不是master时 会阻塞哪些操作
     */
    private volatile ClusterBlock noMasterBlock;

    public NoMasterBlockService(Settings settings, ClusterSettings clusterSettings) {
        this.noMasterBlock = NO_MASTER_BLOCK_SETTING.get(settings);
        // 监听配置的变化
        clusterSettings.addSettingsUpdateConsumer(NO_MASTER_BLOCK_SETTING, this::setNoMasterBlock);
    }

    private static ClusterBlock parseNoMasterBlock(String value) {
        switch (value) {
            case "all":
                return NO_MASTER_BLOCK_ALL;
            case "write":
                return NO_MASTER_BLOCK_WRITES;
            default:
                throw new IllegalArgumentException("invalid no-master block [" + value + "], must be one of [all, write]");
        }
    }

    public ClusterBlock getNoMasterBlock() {
        return noMasterBlock;
    }

    private void setNoMasterBlock(ClusterBlock noMasterBlock) {
        this.noMasterBlock = noMasterBlock;
    }
}
