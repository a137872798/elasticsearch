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

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * Collects statistics about queue size, response time, and service time of
 * tasks executed on each node, making the EWMA of the values available to the
 * coordinating node.
 * TODO 统计相关的先忽略 不影响主流程
 */
public final class ResponseCollectorService implements ClusterStateListener {

    /**
     * 用于计算平均数据相关的权重值
     */
    private static final double ALPHA = 0.3;

    /**
     * 维护每个节点对应的统计信息
     */
    private final ConcurrentMap<String, NodeStatistics> nodeIdToStats = ConcurrentCollections.newConcurrentMap();

    /**
     * 该对象在初始化时 会将自身设置到集群服务中 以便监听集群状态的变化
     * @param clusterService
     */
    public ResponseCollectorService(ClusterService clusterService) {
        clusterService.addListener(this);
    }

    /**
     * 当感应到集群状态发生变化时 触发该方法
     * @param event
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // 当有节点被移除时 移除相关的统计信息
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                removeNode(removedNode.getId());
            }
        }
    }

    void removeNode(String nodeId) {
        nodeIdToStats.remove(nodeId);
    }

    /**
     * 为某个节点增加一个统计数据
     * @param nodeId
     * @param queueSize
     * @param responseTimeNanos
     * @param avgServiceTimeNanos
     */
    public void addNodeStatistics(String nodeId, int queueSize, long responseTimeNanos, long avgServiceTimeNanos) {
        // 首次添加 进行初始化 之后添加 采用累加的方式
        nodeIdToStats.compute(nodeId, (id, ns) -> {
            if (ns == null) {
                ExponentiallyWeightedMovingAverage queueEWMA = new ExponentiallyWeightedMovingAverage(ALPHA, queueSize);
                ExponentiallyWeightedMovingAverage responseEWMA = new ExponentiallyWeightedMovingAverage(ALPHA, responseTimeNanos);
                return new NodeStatistics(nodeId, queueEWMA, responseEWMA, avgServiceTimeNanos);
            } else {
                ns.queueSize.addValue((double) queueSize);
                ns.responseTime.addValue((double) responseTimeNanos);
                ns.serviceTime = avgServiceTimeNanos;
                return ns;
            }
        });
    }

    /**
     * @return
     */
    public Map<String, ComputedNodeStats> getAllNodeStatistics() {
        final int clientNum = nodeIdToStats.size();
        // Transform the mutable object internally used for accounting into the computed version
        // 将NodeStatistics 转换成了ComputedNodeStats
        Map<String, ComputedNodeStats> nodeStats = new HashMap<>(nodeIdToStats.size());
        nodeIdToStats.forEach((k, v) -> {
            nodeStats.put(k, new ComputedNodeStats(clientNum, v));
        });
        return nodeStats;
    }

    public AdaptiveSelectionStats getAdaptiveStats(Map<String, Long> clientSearchConnections) {
        return new AdaptiveSelectionStats(clientSearchConnections, getAllNodeStatistics());
    }

    /**
     * Optionally return a {@code NodeStatistics} for the given nodeid, if
     * response information exists for the given node. Returns an empty
     * {@code Optional} if the node was not found.
     * 通过指定节点id 寻找对应的统计数据
     */
    public Optional<ComputedNodeStats> getNodeStatistics(final String nodeId) {
        final int clientNum = nodeIdToStats.size();
        return Optional.ofNullable(nodeIdToStats.get(nodeId)).map(ns -> new ComputedNodeStats(clientNum, ns));
    }

    /**
     * Struct-like class encapsulating a point-in-time snapshot of a particular
     * node's statistics. This includes the EWMA of queue size, response time,
     * and service time.
     * 已经计算过的统计数据
     * NodeStatistics 相当于是原始数据
     */
    public static class ComputedNodeStats implements Writeable {
        // We store timestamps with nanosecond precision, however, the
        // formula specifies milliseconds, therefore we need to convert
        // the values so the times don't unduely weight the formula
        private final double FACTOR = 1000000.0;
        private final int clientNum;

        /**
         * 缓存的 rank值 如果该值为0 需要重新计算
         */
        private double cachedRank = 0;

        public final String nodeId;
        public final int queueSize;
        public final double responseTime;
        public final double serviceTime;

        /**
         *
         * @param nodeId  当前统计的节点id
         * @param clientNum  一共会与多少个node进行通信
         * @param queueSize   这队列啥意思
         * @param responseTime
         * @param serviceTime
         */
        public ComputedNodeStats(String nodeId, int clientNum, int queueSize, double responseTime, double serviceTime) {
            this.nodeId = nodeId;
            this.clientNum = clientNum;
            this.queueSize = queueSize;
            this.responseTime = responseTime;
            this.serviceTime = serviceTime;
        }

        ComputedNodeStats(int clientNum, NodeStatistics nodeStats) {
            this(nodeStats.nodeId, clientNum,
                    (int) nodeStats.queueSize.getAverage(), nodeStats.responseTime.getAverage(), nodeStats.serviceTime);
        }

        ComputedNodeStats(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.clientNum = in.readInt();
            this.queueSize = in.readInt();
            this.responseTime = in.readDouble();
            this.serviceTime = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.nodeId);
            out.writeInt(this.clientNum);
            out.writeInt(this.queueSize);
            out.writeDouble(this.responseTime);
            out.writeDouble(this.serviceTime);
        }

        /**
         * Rank this copy of the data, according to the adaptive replica selection formula from the C3 paper
         * https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf
         * @param outstandingRequests 根据对外的连接数 计算一个rank值
         *                            TODO 先不看这个在算什么
         */
        private double innerRank(long outstandingRequests) {
            // the concurrency compensation is defined as the number of
            // outstanding requests from the client to the node times the number
            // of clients in the system
            // 推测此时所有节点连接数总和
            double concurrencyCompensation = outstandingRequests * clientNum;

            // Cubic queue adjustment factor. The paper chose 3 though we could
            // potentially make this configurable if desired.
            int queueAdjustmentFactor = 3;

            // EWMA of queue size
            double qBar = queueSize;
            double qHatS = 1 + concurrencyCompensation + qBar;

            // EWMA of response time
            double rS = responseTime / FACTOR;
            // EWMA of service time
            double muBarS = serviceTime / FACTOR;

            // The final formula
            double rank = rS - (1.0 / muBarS) + (Math.pow(qHatS, queueAdjustmentFactor) / muBarS);
            return rank;
        }

        /**
         * 根据当前节点对外暴露的连接数 计算一个rank值
         * @param outstandingRequests
         * @return
         */
        public double rank(long outstandingRequests) {
            if (cachedRank == 0) {
                cachedRank = innerRank(outstandingRequests);
            }
            return cachedRank;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("ComputedNodeStats[");
            sb.append(nodeId).append("](");
            sb.append("nodes: ").append(clientNum);
            sb.append(", queue: ").append(queueSize);
            sb.append(", response time: ").append(String.format(Locale.ROOT, "%.1f", responseTime));
            sb.append(", service time: ").append(String.format(Locale.ROOT, "%.1f", serviceTime));
            sb.append(", rank: ").append(String.format(Locale.ROOT, "%.1f", rank(1)));
            sb.append(")");
            return sb.toString();
        }
    }

    /**
     * Class encapsulating a node's exponentially weighted queue size, response
     * time, and service time, however, this class is private and intended only
     * to be used for the internal accounting of {@code ResponseCollectorService}.
     * 指某个节点相关的统计数据
     */
    private static class NodeStatistics {
        /**
         * 当前节点的id
         */
        final String nodeId;
        // ExponentiallyWeightedMovingAverage  本身是反应某个指标的平均水平
        // 反应了队列长度的平均长度
        final ExponentiallyWeightedMovingAverage queueSize;
        // 反应了平均响应时长
        final ExponentiallyWeightedMovingAverage responseTime;

        /**
         * 服务时间是啥
         */
        double serviceTime;

        NodeStatistics(String nodeId,
                       ExponentiallyWeightedMovingAverage queueSizeEWMA,
                       ExponentiallyWeightedMovingAverage responseTimeEWMA,
                       double serviceTimeEWMA) {
            this.nodeId = nodeId;
            this.queueSize = queueSizeEWMA;
            this.responseTime = responseTimeEWMA;
            this.serviceTime = serviceTimeEWMA;
        }
    }
}
