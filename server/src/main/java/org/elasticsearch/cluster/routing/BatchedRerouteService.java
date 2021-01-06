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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * A {@link BatchedRerouteService} is a {@link RerouteService} that batches together reroute requests to avoid unnecessary extra reroutes.
 * This component only does meaningful work on the elected master node. Reroute requests will fail with a {@link NotMasterException} on
 * other nodes.
 * ES内置的重路由服务对象
 * 原本是因为某个shard 而发起的重路由 实际上并没有局限于仅处理一个
 * 这个batched的意思是将多次reroute请求合并成一次执行
 */
public class BatchedRerouteService implements RerouteService {
    private static final Logger logger = LogManager.getLogger(BatchedRerouteService.class);

    private static final String CLUSTER_UPDATE_TASK_SOURCE = "cluster_reroute";

    /**
     * 集群服务对象
     */
    private final ClusterService clusterService;

    /**
     * 该对象定义了 reroute的逻辑
     */
    private final BiFunction<ClusterState, String, ClusterState> reroute;

    private final Object mutex = new Object();
    @Nullable // null if no reroute is currently pending   如果当前有囤积的reroute任务 该值不为null 否则该值为null
    private List<ActionListener<ClusterState>> pendingRerouteListeners;

    /**
     * 当前囤积的所有任务中 最高的优先级
     */
    private Priority pendingTaskPriority = Priority.LANGUID;

    /**
     * @param reroute Function that computes the updated cluster state after it has been rerouted.
     *                重路由函数 对应allocationService::reroute   在重路由过程中会更新 CS
     */
    public BatchedRerouteService(ClusterService clusterService, BiFunction<ClusterState, String, ClusterState> reroute) {
        this.clusterService = clusterService;
        this.reroute = reroute;
    }

    /**
     * Initiates a reroute.
     * 开始执行重路由服务   就是用来决定某个shard的 primary replicate 应该怎样分配在集群中
     */
    @Override
    public final void reroute(String reason, Priority priority, ActionListener<ClusterState> listener) {
        final List<ActionListener<ClusterState>> currentListeners;
        synchronized (mutex) {
            // 代表有囤积的reroute任务
            if (pendingRerouteListeners != null) {
                // 如果优先级还比当前囤积的任务低 那么直接追加到list就好   注意这里没有重复发起reroute请求
                if (priority.sameOrAfter(pendingTaskPriority)) {
                    logger.trace("already has pending reroute at priority [{}], adding [{}] with priority [{}] to batch",
                        pendingTaskPriority, reason, priority);
                    pendingRerouteListeners.add(listener);
                    return;
                } else {
                    // 将任务设置到了最前面 同时更新最大的优先级
                    logger.trace("already has pending reroute at priority [{}], promoting batch to [{}] and adding [{}]",
                        pendingTaskPriority, priority, reason);
                    currentListeners = new ArrayList<>(1 + pendingRerouteListeners.size());
                    currentListeners.add(listener);
                    currentListeners.addAll(pendingRerouteListeners);
                    pendingRerouteListeners.clear();
                    pendingRerouteListeners = currentListeners;
                    pendingTaskPriority = priority;
                }
            } else {
                // 如果首次添加任务 那么设置到list中
                logger.trace("no pending reroute, scheduling reroute [{}] at priority [{}]", reason, priority);
                currentListeners = new ArrayList<>(1);
                currentListeners.add(listener);
                pendingRerouteListeners = currentListeners;
                pendingTaskPriority = priority;
            }
        }
        try {
            // 通过集群服务来批量提交任务  该任务完成时 还会发布到集群所有节点上
            // 进行allocate 和 reroute的应该都是leader节点
            clusterService.submitStateUpdateTask(CLUSTER_UPDATE_TASK_SOURCE + "(" + reason + ")",
                new ClusterStateUpdateTask(priority) {

                    /**
                     * 重路由的逻辑
                     * @param currentState
                     * @return
                     */
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        final boolean currentListenersArePending;
                        synchronized (mutex) {
                            assert currentListeners.isEmpty() == (pendingRerouteListeners != currentListeners)
                                : "currentListeners=" + currentListeners + ", pendingRerouteListeners=" + pendingRerouteListeners;
                            // 代表期间没有新的reroute请求 本次reroute执行完后 就不需要重复执行了
                            currentListenersArePending = pendingRerouteListeners == currentListeners;
                            if (currentListenersArePending) {
                                pendingRerouteListeners = null;
                            }
                        }
                        if (currentListenersArePending) {
                            logger.trace("performing batched reroute [{}]", reason);
                            return reroute.apply(currentState, reason);
                        } else {
                            // 既然追加了新的reroute请求 那么本次就不需要执行了
                            logger.trace("batched reroute [{}] was promoted", reason);
                            return currentState;
                        }
                    }

                    /**
                     * 在执行任务的过程中发现当前节点不再是master节点时触发
                     * @param source
                     */
                    @Override
                    public void onNoLongerMaster(String source) {
                        synchronized (mutex) {
                            if (pendingRerouteListeners == currentListeners) {
                                pendingRerouteListeners = null;
                            }
                        }
                        // 那么当前囤积的任务就无法继续执行了  以异常形式触发所有监听器
                        ActionListener.onFailure(currentListeners, new NotMasterException("delayed reroute [" + reason + "] cancelled"));
                        // no big deal, the new master will reroute again
                    }

                    /**
                     * 当某个任务执行失败时
                     * @param source
                     * @param e
                     */
                    @Override
                    public void onFailure(String source, Exception e) {
                        synchronized (mutex) {
                            if (pendingRerouteListeners == currentListeners) {
                                pendingRerouteListeners = null;
                            }
                        }
                        final ClusterState state = clusterService.state();
                        if (logger.isTraceEnabled()) {
                            logger.error(() -> new ParameterizedMessage("unexpected failure during [{}], current state:\n{}",
                                source, state), e);
                        } else {
                            logger.error(() -> new ParameterizedMessage("unexpected failure during [{}], current state version [{}]",
                                source, state.version()), e);
                        }
                        ActionListener.onFailure(currentListeners,
                            new ElasticsearchException("delayed reroute [" + reason + "] failed", e));
                    }

                    /**
                     * 当处理成功时 使用最新的state触发所有监听器
                     * @param source
                     * @param oldState
                     * @param newState
                     */
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        ActionListener.onResponse(currentListeners, newState);
                    }
                });
        } catch (Exception e) {
            synchronized (mutex) {
                assert currentListeners.isEmpty() == (pendingRerouteListeners != currentListeners);
                if (pendingRerouteListeners == currentListeners) {
                    pendingRerouteListeners = null;
                }
            }
            ClusterState state = clusterService.state();
            logger.warn(() -> new ParameterizedMessage("failed to reroute routing table, current state:\n{}", state), e);
            ActionListener.onFailure(currentListeners,
                new ElasticsearchException("delayed reroute [" + reason + "] could not be submitted", e));
        }
    }
}
