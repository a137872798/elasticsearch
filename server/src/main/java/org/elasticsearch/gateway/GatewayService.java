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

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * ES 是自带网关的 起到限流作用
 */
public class GatewayService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(GatewayService.class);

    public static final Setting<Integer> EXPECTED_NODES_SETTING =
        Setting.intSetting("gateway.expected_nodes", -1, -1, Property.NodeScope, Property.Deprecated);
    public static final Setting<Integer> EXPECTED_DATA_NODES_SETTING =
        Setting.intSetting("gateway.expected_data_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> EXPECTED_MASTER_NODES_SETTING =
        Setting.intSetting("gateway.expected_master_nodes", -1, -1, Property.NodeScope, Property.Deprecated);
    public static final Setting<TimeValue> RECOVER_AFTER_TIME_SETTING =
        Setting.positiveTimeSetting("gateway.recover_after_time", TimeValue.timeValueMillis(0), Property.NodeScope);
    public static final Setting<Integer> RECOVER_AFTER_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_nodes", -1, -1, Property.NodeScope, Property.Deprecated);
    public static final Setting<Integer> RECOVER_AFTER_DATA_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_data_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> RECOVER_AFTER_MASTER_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_master_nodes", 0, 0, Property.NodeScope, Property.Deprecated);

    public static final ClusterBlock STATE_NOT_RECOVERED_BLOCK = new ClusterBlock(1, "state not recovered / initialized", true, true,
        false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);

    static final TimeValue DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET = TimeValue.timeValueMinutes(5);

    private final ThreadPool threadPool;

    /**
     * 该对象负责对一些未分配的分片决定合适的节点  以及对一些已分配的分片进行平衡 (找到更合适的节点)
     */
    private final AllocationService allocationService;

    private final ClusterService clusterService;

    private final TimeValue recoverAfterTime;
    private final int recoverAfterNodes;
    private final int expectedNodes;
    private final int recoverAfterDataNodes;
    private final int expectedDataNodes;
    private final int recoverAfterMasterNodes;
    private final int expectedMasterNodes;

    private final Runnable recoveryRunnable;

    private final AtomicBoolean recoveryInProgress = new AtomicBoolean();
    private final AtomicBoolean scheduledRecovery = new AtomicBoolean();

    @Inject
    public GatewayService(final Settings settings, final AllocationService allocationService, final ClusterService clusterService,
                          final ThreadPool threadPool, final Discovery discovery, final NodeClient client) {
        this.allocationService = allocationService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // allow to control a delay of when indices will get created
        // 一开始 预期的节点数/dataNode节点数/masterNode节点数  都没有特定要求
        this.expectedNodes = EXPECTED_NODES_SETTING.get(settings);
        this.expectedDataNodes = EXPECTED_DATA_NODES_SETTING.get(settings);
        this.expectedMasterNodes = EXPECTED_MASTER_NODES_SETTING.get(settings);

        // 当设置了预期的节点数时 要设置 recoverAfterTime
        if (RECOVER_AFTER_TIME_SETTING.exists(settings)) {
            recoverAfterTime = RECOVER_AFTER_TIME_SETTING.get(settings);
        } else if (expectedNodes >= 0 || expectedDataNodes >= 0 || expectedMasterNodes >= 0) {
            recoverAfterTime = DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET;
        } else {
            recoverAfterTime = null;
        }


        this.recoverAfterNodes = RECOVER_AFTER_NODES_SETTING.get(settings);
        this.recoverAfterDataNodes = RECOVER_AFTER_DATA_NODES_SETTING.get(settings);
        // default the recover after master nodes to the minimum master nodes in the discovery
        if (RECOVER_AFTER_MASTER_NODES_SETTING.exists(settings)) {
            recoverAfterMasterNodes = RECOVER_AFTER_MASTER_NODES_SETTING.get(settings);
        } else {
            recoverAfterMasterNodes = -1;
        }

        // 默认情况下 注册中心就是 coordinator
        if (discovery instanceof Coordinator) {
            recoveryRunnable = () ->
                    clusterService.submitStateUpdateTask("local-gateway-elected-state", new RecoverStateUpdateTask());
        } else {
            final Gateway gateway = new Gateway(clusterService, client);
            recoveryRunnable = () ->
                    gateway.performStateRecovery(new GatewayRecoveryListener());
        }
    }

    @Override
    protected void doStart() {
        // use post applied so that the state will be visible to the background recovery thread we spawn in performStateRecovery
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {
    }

    /**
     * 当本对象监听到 clusterState发生变化时 触发
     * @param event
     */
    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }

        final ClusterState state = event.state();

        // 非leader节点就不需要处理了
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            // not our job to recover
            return;
        }
        // 当所有分片都已经完成数据恢复 就不需要处理了
        if (state.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
            // already recovered
            return;
        }


        // 这里尝试移除 recoveryBlock
        final DiscoveryNodes nodes = state.nodes();
        // 如果有强制要求节点数 那么无法自动恢复
        if (state.nodes().getMasterNodeId() == null) {
            logger.debug("not recovering from gateway, no master elected yet");
        } else if (recoverAfterNodes != -1 && (nodes.getMasterAndDataNodes().size()) < recoverAfterNodes) {
            logger.debug("not recovering from gateway, nodes_size (data+master) [{}] < recover_after_nodes [{}]",
                nodes.getMasterAndDataNodes().size(), recoverAfterNodes);
        } else if (recoverAfterDataNodes != -1 && nodes.getDataNodes().size() < recoverAfterDataNodes) {
            logger.debug("not recovering from gateway, nodes_size (data) [{}] < recover_after_data_nodes [{}]",
                nodes.getDataNodes().size(), recoverAfterDataNodes);
        } else if (recoverAfterMasterNodes != -1 && nodes.getMasterNodes().size() < recoverAfterMasterNodes) {
            logger.debug("not recovering from gateway, nodes_size (master) [{}] < recover_after_master_nodes [{}]",
                nodes.getMasterNodes().size(), recoverAfterMasterNodes);
        } else {
            // 如果设置的是 expectedXXX 那么配合 recoverAfterTime 可以自动移除 recoveryBlock

            boolean enforceRecoverAfterTime;
            String reason;
            if (expectedNodes == -1 && expectedMasterNodes == -1 && expectedDataNodes == -1) {
                // no expected is set, honor the setting if they are there
                enforceRecoverAfterTime = true;
                reason = "recover_after_time was set to [" + recoverAfterTime + "]";
            } else {
                // one of the expected is set, see if all of them meet the need, and ignore the timeout in this case
                enforceRecoverAfterTime = false;
                reason = "";
                if (expectedNodes != -1 && (nodes.getMasterAndDataNodes().size() < expectedNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedNodes + "] nodes, but only have [" + nodes.getMasterAndDataNodes().size() + "]";
                } else if (expectedDataNodes != -1 && (nodes.getDataNodes().size() < expectedDataNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedDataNodes + "] data nodes, but only have [" + nodes.getDataNodes().size() + "]";
                } else if (expectedMasterNodes != -1 && (nodes.getMasterNodes().size() < expectedMasterNodes)) {
                    // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedMasterNodes + "] master nodes, but only have [" + nodes.getMasterNodes().size() + "]";
                }
            }
            performStateRecovery(enforceRecoverAfterTime, reason);
        }
    }

    /**
     * @param enforceRecoverAfterTime  如果此时已经达到预期的节点数了 不需要强制恢复 直接移除recoverBlock 即可
     *                                 如果未达到节点数 且设置了超时时间 那么在等待一定时间后移除 recoverBlock
     * @param reason
     */
    private void performStateRecovery(final boolean enforceRecoverAfterTime, final String reason) {
        if (enforceRecoverAfterTime && recoverAfterTime != null) {
            if (scheduledRecovery.compareAndSet(false, true)) {
                logger.info("delaying initial state recovery for [{}]. {}", recoverAfterTime, reason);
                threadPool.schedule(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("delayed state recovery failed", e);
                        resetRecoveredFlags();
                    }

                    @Override
                    protected void doRun() {
                        if (recoveryInProgress.compareAndSet(false, true)) {
                            logger.info("recover_after_time [{}] elapsed. performing state recovery...", recoverAfterTime);
                            recoveryRunnable.run();
                        }
                    }
                }, recoverAfterTime, ThreadPool.Names.GENERIC);
            }
        } else {
            if (recoveryInProgress.compareAndSet(false, true)) {
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(final Exception e) {
                        logger.warn("state recovery failed", e);
                        resetRecoveredFlags();
                    }

                    @Override
                    protected void doRun() {
                        logger.debug("performing state recovery...");
                        recoveryRunnable.run();
                    }
                });
            }
        }
    }

    /**
     * 当移除 recoveryBlock的clusterState发布到集群后触发
     */
    private void resetRecoveredFlags() {
        recoveryInProgress.set(false);
        scheduledRecovery.set(false);
    }

    /**
     * 该任务会在合适的时机被触发
     */
    class RecoverStateUpdateTask extends ClusterStateUpdateTask {

        @Override
        public ClusterState execute(final ClusterState currentState) {
            // 已经移除了 notRecover标记 就不需要重复处理了
            if (currentState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                logger.debug("cluster is already recovered");
                return currentState;
            }

            // 移除 处于恢复中的block标记
            final ClusterState newState = Function.<ClusterState>identity()
                    .andThen(ClusterStateUpdaters::updateRoutingTable)
                    .andThen(ClusterStateUpdaters::removeStateNotRecoveredBlock)
                    .apply(currentState);

            return allocationService.reroute(newState, "state recovered");
        }

        @Override
        public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
            logger.info("recovered [{}] indices into cluster_state", newState.metadata().indices().size());
            // reset flag even though state recovery completed, to ensure that if we subsequently become leader again based on a
            // not-recovered state, that we again do another state recovery.
            resetRecoveredFlags();
        }

        @Override
        public void onNoLongerMaster(String source) {
            logger.debug("stepped down as master before recovering state [{}]", source);
            resetRecoveredFlags();
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            logger.info(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
            resetRecoveredFlags();
        }
    }

    /**
     * TODO 当使用的是 ES内置的注册中心时 不需要使用该对象
     */
    class GatewayRecoveryListener implements Gateway.GatewayStateRecoveredListener {

        @Override
        public void onSuccess(final ClusterState recoveredState) {
            logger.trace("successful state recovery, importing cluster state...");
            clusterService.submitStateUpdateTask("local-gateway-elected-state", new RecoverStateUpdateTask() {
                @Override
                public ClusterState execute(final ClusterState currentState) {
                    final ClusterState updatedState = ClusterStateUpdaters.mixCurrentStateAndRecoveredState(currentState, recoveredState);
                    return super.execute(ClusterStateUpdaters.recoverClusterBlocks(updatedState));
                }
            });
        }

        @Override
        public void onFailure(final String msg) {
            logger.info("state recovery failed: {}", msg);
            resetRecoveredFlags();
        }

    }

    // used for testing
    TimeValue recoverAfterTime() {
        return recoverAfterTime;
    }

}
