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

package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.ClusterState;

import java.util.function.Supplier;

/**
 * 用于控制集群状态的接口
 */
public interface ClusterApplier {
    /**
     * Sets the initial state for this applier. Should only be called once.
     * @param initialState the initial state to set
     */
    void setInitialState(ClusterState initialState);

    /**
     * Method to invoke when a new cluster state is available to be applied
     *
     * @param source information where the cluster state came from
     * @param clusterStateSupplier the cluster state supplier which provides the latest cluster state to apply
     * @param listener callback that is invoked after cluster state is applied
     */
    void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier, ClusterApplyListener listener);

    /**
     * Listener for results of cluster state application
     * 处理集群状态的结果
     */
    interface ClusterApplyListener {
        /**
         * Called on successful cluster state application
         * @param source information where the cluster state came from
         *               成功处理
         */
        default void onSuccess(String source) {
        }

        /**
         * Called on failure during cluster state application
         * @param source information where the cluster state came from
         * @param e exception that occurred
         *          失败 比如任务超时
         */
        void onFailure(String source, Exception e);
    }
}
