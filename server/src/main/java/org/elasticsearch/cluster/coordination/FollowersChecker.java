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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportRequestOptions.Type;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 * The FollowersChecker is responsible for allowing a leader to check that its followers are still connected and healthy. On deciding that a
 * follower has failed the leader will remove it from the cluster. We are fairly lenient, possibly allowing multiple checks to fail before
 * considering a follower to be faulty, to allow for a brief network partition or a long GC cycle to occur without triggering the removal of
 * a node and the consequent shard reallocation.
 * 作为leader节点 检测它的所有跟随者是否正常  (只有作为leader节点时才能正常工作)
 * 比如某个节点的离线也可以感知到  并且更新clusterState
 *
 */
public class FollowersChecker {

    private static final Logger logger = LogManager.getLogger(FollowersChecker.class);

    public static final String FOLLOWER_CHECK_ACTION_NAME = "internal:coordination/fault_detection/follower_check";

    // the time between checks sent to each node
    public static final Setting<TimeValue> FOLLOWER_CHECK_INTERVAL_SETTING =
        Setting.timeSetting("cluster.fault_detection.follower_check.interval",
            TimeValue.timeValueMillis(1000), TimeValue.timeValueMillis(100), Setting.Property.NodeScope);

    // the timeout for each check sent to each node
    public static final Setting<TimeValue> FOLLOWER_CHECK_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.fault_detection.follower_check.timeout",
            TimeValue.timeValueMillis(10000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    // the number of failed checks that must happen before the follower is considered to have failed.
    public static final Setting<Integer> FOLLOWER_CHECK_RETRY_COUNT_SETTING =
        Setting.intSetting("cluster.fault_detection.follower_check.retry_count", 3, 1, Setting.Property.NodeScope);

    private final Settings settings;

    private final TimeValue followerCheckInterval;
    private final TimeValue followerCheckTimeout;
    private final int followerCheckRetryCount;
    private final BiConsumer<DiscoveryNode, String> onNodeFailure;

    /**
     * 该函数会处理检测 follower的请求 如果没有抛出异常就代表此时节点是follower
     */
    private final Consumer<FollowerCheckRequest> handleRequestAndUpdateState;

    private final Object mutex = new Object(); // protects writes to this state; read access does not need sync

    /**
     * 针对集群中的不同节点 都有对应的检测器
     */
    private final Map<DiscoveryNode, FollowerChecker> followerCheckers = newConcurrentMap();

    /**
     * 因为长时间连接不上而被从集群中移除的节点会存储在这里
     */
    private final Set<DiscoveryNode> faultyNodes = new HashSet<>();

    private final TransportService transportService;

    /**
     * 如果该对象的任期与请求的任期一致 可以直接参考该对象的 mode  这样就可以跳过handleRequestAndUpdateState
     */
    private volatile FastResponseState fastResponseState;

    /**
     *
     * @param settings
     * @param transportService
     * @param handleRequestAndUpdateState
     * @param onNodeFailure
     */
    public FollowersChecker(Settings settings, TransportService transportService,
                            Consumer<FollowerCheckRequest> handleRequestAndUpdateState,
                            BiConsumer<DiscoveryNode, String> onNodeFailure) {
        this.settings = settings;
        this.transportService = transportService;
        this.handleRequestAndUpdateState = handleRequestAndUpdateState;
        this.onNodeFailure = onNodeFailure;

        followerCheckInterval = FOLLOWER_CHECK_INTERVAL_SETTING.get(settings);
        followerCheckTimeout = FOLLOWER_CHECK_TIMEOUT_SETTING.get(settings);
        followerCheckRetryCount = FOLLOWER_CHECK_RETRY_COUNT_SETTING.get(settings);

        // 该对象在初始阶段时 使用一个默认的响应结果  每当探测其他节点 或者自身的角色发生变化时 会将最新的状态信息更新到该属性上
        updateFastResponseState(0, Mode.CANDIDATE);
        transportService.registerRequestHandler(FOLLOWER_CHECK_ACTION_NAME, Names.SAME, false, false, FollowerCheckRequest::new,
            (request, transportChannel, task) -> handleFollowerCheck(request, transportChannel));
        transportService.addConnectionListener(new TransportConnectionListener() {
            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                handleDisconnectedNode(node);
            }
        });
    }

    /**
     * Update the set of known nodes, starting to check any new ones and stopping checking any previously-known-but-now-unknown ones.
     * 当触发了该方法时 本对象才开始工作
     */
    public void setCurrentNodes(DiscoveryNodes discoveryNodes) {
        synchronized (mutex) {
            final Predicate<DiscoveryNode> isUnknownNode = n -> discoveryNodes.nodeExists(n) == false;

            // 某些节点此时已经不属于这个集群了  需要移除
            followerCheckers.keySet().removeIf(isUnknownNode);
            faultyNodes.removeIf(isUnknownNode);

            // 这些节点中可能包含 非masterNode  将它们排序后 使得masterNode排在前面 并挨个进行探测
            // 可以看到 非masterNode节点 也会收到 follower请求 并转换成follower节点   不直接参与选举
            discoveryNodes.mastersFirstStream().forEach(discoveryNode -> {
                // 跳过本节点  以及已经在处理中的节点  以及失败的节点
                if (discoveryNode.equals(discoveryNodes.getLocalNode()) == false
                    && followerCheckers.containsKey(discoveryNode) == false
                    && faultyNodes.contains(discoveryNode) == false) {

                    final FollowerChecker followerChecker = new FollowerChecker(discoveryNode);
                    followerCheckers.put(discoveryNode, followerChecker);
                    followerChecker.start();
                }
            });
        }
    }

    /**
     * Clear the set of known nodes, stopping all checks.
     */
    public void clearCurrentNodes() {
        setCurrentNodes(DiscoveryNodes.EMPTY_NODES);
    }

    /**
     * The system is normally in a state in which every follower remains a follower of a stable leader in a single term for an extended
     * period of time, and therefore our response to every follower check is the same. We handle this case with a single volatile read
     * entirely on the network thread, and only if the fast path fails do we perform some work in the background, by notifying the
     * FollowersChecker whenever our term or mode changes here.
     */
    public void updateFastResponseState(final long term, final Mode mode) {
        fastResponseState = new FastResponseState(term, mode);
    }

    /**
     * 当集群中产生了新的leader时 会发起请求将其他节点降级成follower
     * @param request
     * @param transportChannel
     * @throws IOException
     */
    private void handleFollowerCheck(FollowerCheckRequest request, TransportChannel transportChannel) throws IOException {

        // 该对象中包含了此时该节点的任期 以及当前角色
        FastResponseState responder = this.fastResponseState;

        // 当前节点此时确实是 follower时 且term匹配时 直接返回ack信息
        if (responder.mode == Mode.FOLLOWER && responder.term == request.term) {
            logger.trace("responding to {} on fast path", request);
            transportChannel.sendResponse(Empty.INSTANCE);
            return;
        }

        // 忽略旧的 leader的请求
        if (request.term < responder.term) {
            throw new CoordinationStateRejectedException("rejecting " + request + " since local state is " + this);
        }

        transportService.getThreadPool().generic().execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws IOException {
                logger.trace("responding to {} on slow path", request);
                try {
                    // 根据请求中的信息 更新当前节点的状态
                    handleRequestAndUpdateState.accept(request);
                } catch (Exception e) {
                    transportChannel.sendResponse(e);
                    return;
                }
                transportChannel.sendResponse(Empty.INSTANCE);
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(new ParameterizedMessage("exception while responding to {}", request), e);
            }

            @Override
            public String toString() {
                return "slow path response to " + request;
            }
        });
    }

    /**
     * @return nodes in the current cluster state which have failed their follower checks.
     */
    public Set<DiscoveryNode> getFaultyNodes() {
        synchronized (mutex) {
            return new HashSet<>(this.faultyNodes);
        }
    }

    @Override
    public String toString() {
        return "FollowersChecker{" +
            "followerCheckInterval=" + followerCheckInterval +
            ", followerCheckTimeout=" + followerCheckTimeout +
            ", followerCheckRetryCount=" + followerCheckRetryCount +
            ", followerCheckers=" + followerCheckers +
            ", faultyNodes=" + faultyNodes +
            ", fastResponseState=" + fastResponseState +
            '}';
    }

    // For assertions
    FastResponseState getFastResponseState() {
        return fastResponseState;
    }

    // For assertions
    Set<DiscoveryNode> getKnownFollowers() {
        synchronized (mutex) {
            final Set<DiscoveryNode> knownFollowers = new HashSet<>(faultyNodes);
            knownFollowers.addAll(followerCheckers.keySet());
            return knownFollowers;
        }
    }

    private void handleDisconnectedNode(DiscoveryNode discoveryNode) {
        FollowerChecker followerChecker = followerCheckers.get(discoveryNode);
        if (followerChecker != null) {
            followerChecker.failNode("disconnected");
        }
    }

    /**
     * 因为CP中某个节点状态一旦确认下来 任期 mode都不会变化   所以采用单例模式
     */
    static class FastResponseState {
        final long term;

        /**
         * 代表节点的角色   候选人  follower leader
         */
        final Mode mode;

        FastResponseState(final long term, final Mode mode) {
            this.term = term;
            this.mode = mode;
        }

        @Override
        public String toString() {
            return "FastResponseState{" +
                "term=" + term +
                ", mode=" + mode +
                '}';
        }
    }

    /**
     * A checker for an individual follower.
     * 当本节点变成leader节点后 需要通知其他节点变成follower 节点
     */
    private class FollowerChecker {

        /**
         * 需要探测的节点
         */
        private final DiscoveryNode discoveryNode;
        private int failureCountSinceLastSuccess;

        FollowerChecker(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        /**
         * 如果这个对象已经从容器中被替换 就代表这个对象此时已经失效
         * @return
         */
        private boolean running() {
            return this == followerCheckers.get(discoveryNode);
        }

        void start() {
            assert running();
            handleWakeUp();
        }

        /**
         * 尝试与目标节点建立联系  通知其转换成follower节点   以及之后要定期检测 这样本节点能够快速感知到自己已经落后 并自动降级成candidate
         */
        private void handleWakeUp() {
            if (running() == false) {
                logger.trace("handleWakeUp: not running");
                return;
            }

            // 当前leader节点以及对应的任期
            final FollowerCheckRequest request = new FollowerCheckRequest(fastResponseState.term, transportService.getLocalNode());
            logger.trace("handleWakeUp: checking {} with {}", discoveryNode, request);

            transportService.sendRequest(discoveryNode, FOLLOWER_CHECK_ACTION_NAME, request,
                TransportRequestOptions.builder().withTimeout(followerCheckTimeout).withType(Type.PING).build(),
                new TransportResponseHandler<Empty>() {
                    @Override
                    public Empty read(StreamInput in) {
                        return Empty.INSTANCE;
                    }

                    @Override
                    public void handleResponse(Empty response) {
                        if (running() == false) {
                            logger.trace("{} no longer running", FollowerChecker.this);
                            return;
                        }

                        // 当对端正常处理完请求后 重置失败次数
                        failureCountSinceLastSuccess = 0;
                        logger.trace("{} check successful", FollowerChecker.this);
                        // 递归执行检测任务
                        scheduleNextWakeUp();
                    }

                    /**
                     * 当收到失败信息时 在一定容错次数内 不做处理   当超过限制时 加入到失败容器中
                     * @param exp
                     */
                    @Override
                    public void handleException(TransportException exp) {
                        if (running() == false) {
                            logger.debug(new ParameterizedMessage("{} no longer running", FollowerChecker.this), exp);
                            return;
                        }

                        failureCountSinceLastSuccess++;

                        final String reason;
                        if (failureCountSinceLastSuccess >= followerCheckRetryCount) {
                            logger.debug(() -> new ParameterizedMessage("{} failed too many times", FollowerChecker.this), exp);
                            reason = "followers check retry count exceeded";
                        } else if (exp instanceof ConnectTransportException
                            || exp.getCause() instanceof ConnectTransportException) {
                            logger.debug(() -> new ParameterizedMessage("{} disconnected", FollowerChecker.this), exp);
                            reason = "disconnected";
                        } else {
                            logger.debug(() -> new ParameterizedMessage("{} failed, retrying", FollowerChecker.this), exp);
                            scheduleNextWakeUp();
                            return;
                        }

                        failNode(reason);
                    }


                    @Override
                    public String executor() {
                        return Names.SAME;
                    }
                });
        }

        /**
         * 认为这个节点已经脱离了集群 不再进行无意义的检测  同时会尝试将该节点从clusterState中移除
         * @param reason
         */
        void failNode(String reason) {
            transportService.getThreadPool().generic().execute(new Runnable() {
                @Override
                public void run() {
                    synchronized (mutex) {
                        if (running() == false) {
                            logger.trace("{} no longer running, not marking faulty", FollowerChecker.this);
                            return;
                        }
                        logger.debug("{} marking node as faulty", FollowerChecker.this);
                        faultyNodes.add(discoveryNode);
                        followerCheckers.remove(discoveryNode);
                    }
                    onNodeFailure.accept(discoveryNode, reason);
                }

                @Override
                public String toString() {
                    return "detected failure of " + discoveryNode;
                }
            });
        }

        private void scheduleNextWakeUp() {
            transportService.getThreadPool().schedule(new Runnable() {
                @Override
                public void run() {
                    handleWakeUp();
                }

                @Override
                public String toString() {
                    return FollowerChecker.this + "::handleWakeUp";
                }
            }, followerCheckInterval, Names.SAME);
        }

        @Override
        public String toString() {
            return "FollowerChecker{" +
                "discoveryNode=" + discoveryNode +
                ", failureCountSinceLastSuccess=" + failureCountSinceLastSuccess +
                ", [" + FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey() + "]=" + followerCheckRetryCount +
                '}';
        }
    }

    public static class FollowerCheckRequest extends TransportRequest {

        private final long term;

        private final DiscoveryNode sender;

        public long getTerm() {
            return term;
        }

        public DiscoveryNode getSender() {
            return sender;
        }

        public FollowerCheckRequest(final long term, final DiscoveryNode sender) {
            this.term = term;
            this.sender = sender;
        }

        public FollowerCheckRequest(final StreamInput in) throws IOException {
            super(in);
            term = in.readLong();
            sender = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(term);
            sender.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FollowerCheckRequest that = (FollowerCheckRequest) o;
            return term == that.term &&
                Objects.equals(sender, that.sender);
        }

        @Override
        public String toString() {
            return "FollowerCheckRequest{" +
                "term=" + term +
                ", sender=" + sender +
                '}';
        }

        @Override
        public int hashCode() {
            return Objects.hash(term, sender);
        }
    }
}
