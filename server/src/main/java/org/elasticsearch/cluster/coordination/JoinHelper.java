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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;


public class JoinHelper {

    private static final Logger logger = LogManager.getLogger(JoinHelper.class);

    public static final String JOIN_ACTION_NAME = "internal:cluster/coordination/join";
    public static final String VALIDATE_JOIN_ACTION_NAME = "internal:cluster/coordination/join/validate";
    public static final String START_JOIN_ACTION_NAME = "internal:cluster/coordination/start_join";

    // the timeout for each join attempt
    public static final Setting<TimeValue> JOIN_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.join.timeout",
            TimeValue.timeValueMillis(60000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    /**
     * 该对象作为CP中的master节点 对外提供更新集群信息的api 同时会将最新信息发布到集群其他节点上
     */
    private final MasterService masterService;
    /**
     * 负责节点间的通信工作
     */
    private final TransportService transportService;

    /**
     * 该对象可以一次性执行多个task 对象
     */
    private final JoinTaskExecutor joinTaskExecutor;

    @Nullable // if using single-node discovery    一轮选举的时间
    private final TimeValue joinTimeout;

    /**
     * 命令池  等待响应结果
     */
    private final Set<Tuple<DiscoveryNode, JoinRequest>> pendingOutgoingJoins = Collections.synchronizedSet(new HashSet<>());

    /**
     * 记录最近一次失败的join
     * FailedJoinAttempt 中记录了失败的异常
     * 如果某次join成功 会将该值置null
     */
    private AtomicReference<FailedJoinAttempt> lastFailedJoinAttempt = new AtomicReference<>();

    /**
     *
     * @param settings
     * @param allocationService
     * @param masterService
     * @param transportService
     * @param currentTermSupplier
     * @param currentStateSupplier
     * @param joinHandler  该对象处理join请求
     * @param joinLeaderInTerm  该函数可以根据 startJoin对象 生成 join  (join对象描述了某次join请求的target/source 节点)
     * @param joinValidators  一组join请求的校验器  非法请求将被拒绝
     * @param rerouteService
     */
    JoinHelper(Settings settings, AllocationService allocationService, MasterService masterService,
               TransportService transportService, LongSupplier currentTermSupplier, Supplier<ClusterState> currentStateSupplier,
               BiConsumer<JoinRequest, JoinCallback> joinHandler, Function<StartJoinRequest, Join> joinLeaderInTerm,
               Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators, RerouteService rerouteService) {
        this.masterService = masterService;
        this.transportService = transportService;
        // 一轮选举的等待时间
        this.joinTimeout = DiscoveryModule.isSingleNodeDiscovery(settings) ? null : JOIN_TIMEOUT_SETTING.get(settings);

        // 该对象现在会通过 currentTermSupplier获取最新的任期
        this.joinTaskExecutor = new JoinTaskExecutor(allocationService, logger, rerouteService) {

            /**
             * 在之前的基础上还需要判断任期是否合法
             * @param currentState
             * @param joiningTasks
             * @return
             * @throws Exception
             */
            @Override
            public ClusterTasksResult<JoinTaskExecutor.Task> execute(ClusterState currentState, List<JoinTaskExecutor.Task> joiningTasks)
                throws Exception {
                // This is called when preparing the next cluster state for publication. There is no guarantee that the term we see here is
                // the term under which this state will eventually be published: the current term may be increased after this check due to
                // some other activity. That the term is correct is, however, checked properly during publication, so it is sufficient to
                // check it here on a best-effort basis. This is fine because a concurrent change indicates the existence of another leader
                // in a higher term which will cause this node to stand down.

                // 每次通过该函数获取任期 同时更新元数据
                final long currentTerm = currentTermSupplier.getAsLong();
                if (currentState.term() != currentTerm) {
                    final CoordinationMetadata coordinationMetadata =
                            CoordinationMetadata.builder(currentState.coordinationMetadata()).term(currentTerm).build();
                    final Metadata metadata = Metadata.builder(currentState.metadata()).coordinationMetadata(coordinationMetadata).build();
                    currentState = ClusterState.builder(currentState).metadata(metadata).build();
                }
                return super.execute(currentState, joiningTasks);
            }

        };

        // 在传输层注册请求处理器   这个join的ack信息不是立即返回的 而是要等到 选举结束
        transportService.registerRequestHandler(JOIN_ACTION_NAME, ThreadPool.Names.GENERIC, false, false, JoinRequest::new,
            (request, channel, task) -> joinHandler.accept(request, transportJoinCallback(request, channel)));

        // 注册处理 start_join 请求的处理器
        transportService.registerRequestHandler(START_JOIN_ACTION_NAME, Names.GENERIC, false, false,
            StartJoinRequest::new,
            (request, channel, task) -> {
                final DiscoveryNode destination = request.getSourceNode();
                // 收到start_join 请求后 回复一个join请求
                sendJoinRequest(destination, currentTermSupplier.getAsLong(), Optional.of(joinLeaderInTerm.apply(request)));
                // 这个res 代表成功收到了startJoin请求
                channel.sendResponse(Empty.INSTANCE);
            });

        // 校验join请求
        transportService.registerRequestHandler(VALIDATE_JOIN_ACTION_NAME,
            ThreadPool.Names.GENERIC, ValidateJoinRequest::new,
            (request, channel, task) -> {
                final ClusterState localState = currentStateSupplier.get();
                // 这里尝试匹配本地clusterState的 uuid  与请求携带的uuid  不匹配的情况 拒绝请求
                if (localState.metadata().clusterUUIDCommitted() &&
                    localState.metadata().clusterUUID().equals(request.getState().metadata().clusterUUID()) == false) {
                    throw new CoordinationStateRejectedException("join validation on cluster state" +
                        " with a different cluster uuid " + request.getState().metadata().clusterUUID() +
                        " than local cluster uuid " + localState.metadata().clusterUUID() + ", rejecting");
                }
                joinValidators.forEach(action -> action.accept(transportService.getLocalNode(), request.getState()));
                // 校验之后 发送一个ack信息
                channel.sendResponse(Empty.INSTANCE);
            });
    }

    /**
     * 处理join的回调函数
     * @param request
     * @param channel
     * @return
     */
    private JoinCallback transportJoinCallback(TransportRequest request, TransportChannel channel) {
        return new JoinCallback() {

            @Override
            public void onSuccess() {
                try {
                    channel.sendResponse(Empty.INSTANCE);
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.warn("failed to send back failure on join request", inner);
                }
            }

            @Override
            public String toString() {
                return "JoinCallback{request=" + request + "}";
            }
        };
    }

    boolean isJoinPending() {
        return pendingOutgoingJoins.isEmpty() == false;
    }

    /**
     * 描述某次join失败的原因 等信息
     */
    static class FailedJoinAttempt {
        private final DiscoveryNode destination;
        private final JoinRequest joinRequest;
        private final TransportException exception;
        private final long timestamp;

        FailedJoinAttempt(DiscoveryNode destination, JoinRequest joinRequest, TransportException exception) {
            this.destination = destination;
            this.joinRequest = joinRequest;
            this.exception = exception;
            this.timestamp = System.nanoTime();
        }

        void logNow() {
            logger.log(getLogLevel(exception),
                    () -> new ParameterizedMessage("failed to join {} with {}", destination, joinRequest),
                    exception);
        }

        static Level getLogLevel(TransportException e) {
            Throwable cause = e.unwrapCause();
            if (cause instanceof CoordinationStateRejectedException ||
                cause instanceof FailedToCommitClusterStateException ||
                cause instanceof NotMasterException) {
                return Level.DEBUG;
            }
            return Level.INFO;
        }

        void logWarnWithTimestamp() {
            logger.info(() -> new ParameterizedMessage("last failed join attempt was {} ago, failed to join {} with {}",
                            TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - timestamp)),
                            destination,
                            joinRequest),
                    exception);
        }
    }


    void logLastFailedJoinAttempt() {
        FailedJoinAttempt attempt = lastFailedJoinAttempt.get();
        if (attempt != null) {
            attempt.logWarnWithTimestamp();
            lastFailedJoinAttempt.compareAndSet(attempt, null);
        }
    }

    /**
     * 往目标节点发送一个 join的请求
     * 流程是这样 首先某个通过预投票的节点 会向所有节点发送一个startJoin请求 之后 每个节点会返回一个join请求
     * source节点通过了预投票阶段  那么此时就不需要做任何检测 无条件信任目标节点 并直接返回join请求
     * @param destination  通过预投票的节点
     * @param term  本地任期
     * @param optionalJoin   目前看来该值一定会被设置
     */
    public void sendJoinRequest(DiscoveryNode destination, long term, Optional<Join> optionalJoin) {
        assert destination.isMasterNode() : "trying to join master-ineligible " + destination;

        // 代表当前节点尝试加入到目标节点所在的集群
        final JoinRequest joinRequest = new JoinRequest(transportService.getLocalNode(), term, optionalJoin);
        final Tuple<DiscoveryNode, JoinRequest> dedupKey = Tuple.tuple(destination, joinRequest);

        // 加入成功代表首次往该节点上发送join请求  加入失败代表在这轮join中已经为该节点投过一票了
        if (pendingOutgoingJoins.add(dedupKey)) {
            logger.debug("attempting to join {} with {}", destination, joinRequest);
            transportService.sendRequest(destination, JOIN_ACTION_NAME, joinRequest,
                TransportRequestOptions.builder().withTimeout(joinTimeout).build(),

                new TransportResponseHandler<Empty>() {
                    @Override
                    public Empty read(StreamInput in) {
                        return Empty.INSTANCE;
                    }

                    /**
                     * 只有当本轮选举结束
                     * 确认leader 并且将最新集群的信息发布到超过半数节点 并将clusterState通过clusterApplier作用到leader后才会返回ack
                     * @param response
                     */
                    @Override
                    public void handleResponse(Empty response) {
                        // 操作后使得可以继续往同一节点发送请求
                        pendingOutgoingJoins.remove(dedupKey);
                        logger.debug("successfully joined {} with {}", destination, joinRequest);
                        lastFailedJoinAttempt.set(null);
                    }

                    /**
                     * 当支持的节点选举失败时 会以异常方式触发该方法
                     * 当支持的节点选举成功 但是校验失败时 还是会以异常方式触发 先忽略这种异常吧 校验失败的节点本身就不应该参与选举啊
                     * @param exp
                     */
                    @Override
                    public void handleException(TransportException exp) {
                        pendingOutgoingJoins.remove(dedupKey);
                        // 当往某个节点发送的join失败时 会设置lastFailedJoinAttempt
                        FailedJoinAttempt attempt = new FailedJoinAttempt(destination, joinRequest, exp);
                        attempt.logNow();
                        lastFailedJoinAttempt.set(attempt);
                    }

                    @Override
                    public String executor() {
                        return Names.SAME;
                    }
                });
        } else {
            logger.debug("already attempting to join {} with request {}, not sending request", destination, joinRequest);
        }
    }

    /**
     * 某个候选者通过了预投票阶段后 会往此时集群中已知的所有节点发起startJoin请求
     * @param startJoinRequest
     * @param destination
     */
    void sendStartJoinRequest(final StartJoinRequest startJoinRequest, final DiscoveryNode destination) {
        assert startJoinRequest.getSourceNode().isMasterNode()
            : "sending start-join request for master-ineligible " + startJoinRequest.getSourceNode();
        transportService.sendRequest(destination, START_JOIN_ACTION_NAME,
            startJoinRequest,
            // startJoin 本身的响应结果不重要  因为如果对端成接收到数据 会立即返回一个join请求
            new TransportResponseHandler<Empty>() {
                @Override
                public Empty read(StreamInput in) {
                    return Empty.INSTANCE;
                }

                @Override
                public void handleResponse(Empty response) {
                    logger.debug("successful response to {} from {}", startJoinRequest, destination);
                }

                /**
                 * 当某个节点本轮已经选择了一个node后 继续往该节点发送就会触发该方法
                 * @param exp
                 */
                @Override
                public void handleException(TransportException exp) {
                    logger.debug(new ParameterizedMessage("failure in response to {} from {}", startJoinRequest, destination), exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
    }

    /**
     * 校验join的请求
     * @param node
     * @param state
     * @param listener
     */
    void sendValidateJoinRequest(DiscoveryNode node, ClusterState state, ActionListener<TransportResponse.Empty> listener) {
        transportService.sendRequest(node, VALIDATE_JOIN_ACTION_NAME,
            new ValidateJoinRequest(state),
            TransportRequestOptions.builder().withTimeout(joinTimeout).build(),
            new ActionListenerResponseHandler<>(listener, i -> Empty.INSTANCE, ThreadPool.Names.GENERIC));
    }

    public interface JoinCallback {
        void onSuccess();

        void onFailure(Exception e);
    }

    /**
     * 处理join任务相关的监听器
     * 这里包装了callback 以解耦
     */
    static class JoinTaskListener implements ClusterStateTaskListener {

        /**
         * 该对象内部包含了发送join请求的节点
         */
        private final JoinTaskExecutor.Task task;

        /**
         * 调用该函数时 就是将选举的结果通知到发出join请求的节点
         */
        private final JoinCallback joinCallback;

        JoinTaskListener(JoinTaskExecutor.Task task, JoinCallback joinCallback) {
            this.task = task;
            this.joinCallback = joinCallback;
        }

        @Override
        public void onFailure(String source, Exception e) {
            joinCallback.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            joinCallback.onSuccess();
        }

        @Override
        public String toString() {
            return "JoinTaskListener{task=" + task + "}";
        }
    }

    /**
     * 累加器接口定义了主节点收到join请求时 如何处理他们累加的逻辑
     */
    interface JoinAccumulator {
        void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback);

        default void close(Mode newMode) {
        }
    }


    /**
     * 当前节点角色转变成 leader后 内部的累加器也会更改
     */
    class LeaderJoinAccumulator implements JoinAccumulator {

        /**
         * 当本节点已经变成leader节点时 继续收到join请求 会直接进行处理 这套逻辑与 candidate的close是一样的
         * @param sender
         * @param joinCallback
         */
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            // 采用批处理最大的原因是 每个通过join的节点都会加入到最新的 clusterState中 并且会触发publish方法 该方法的处理比较复杂 通过批处理可以将一批更新任务一起执行
            // 尽可能只发布一个最新的clusterState
            final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(sender, "join existing leader");
            masterService.submitStateUpdateTask("node-join", task, ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor, new JoinTaskListener(task, joinCallback));
        }

        @Override
        public String toString() {
            return "LeaderJoinAccumulator";
        }
    }

    static class InitialJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            assert false : "unexpected join from " + sender + " during initialisation";
            joinCallback.onFailure(new CoordinationStateRejectedException("join target is not initialised yet"));
        }

        @Override
        public String toString() {
            return "InitialJoinAccumulator";
        }
    }

    static class FollowerJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            joinCallback.onFailure(new CoordinationStateRejectedException("join target is a follower"));
        }

        @Override
        public String toString() {
            return "FollowerJoinAccumulator";
        }
    }


    /**
     * 在当前节点转变成候选人时  JoinAccumulator 也会变成对应的实现类
     */
    class CandidateJoinAccumulator implements JoinAccumulator {

        /**
         * 该对象存储了在 候选阶段收到的所有join请求
         * 当本次变更leader的集群状态发布到超过半数的节点后 并且成功commit到 leader本地时 才会返回join的成功信息
         */
        private final Map<DiscoveryNode, JoinCallback> joinRequestAccumulator = new HashMap<>();
        boolean closed;

        /**
         * 当某个参与选举的节点
         * @param sender
         * @param joinCallback
         */
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            assert closed == false : "CandidateJoinAccumulator closed";
            JoinCallback prev = joinRequestAccumulator.put(sender, joinCallback);
            if (prev != null) {
                prev.onFailure(new CoordinationStateRejectedException("received a newer join from " + sender));
            }
        }

        /**
         * 可以通过 callback对象将结果返回给发送 join请求的节点了
         * @param newMode  本次参与选举的节点最后决定的角色
         */
        @Override
        public void close(Mode newMode) {
            assert closed == false : "CandidateJoinAccumulator closed";
            closed = true;
            // 代表这个节点采集了足够的票数 并成功晋升成leader
            if (newMode == Mode.LEADER) {
                final Map<JoinTaskExecutor.Task, ClusterStateTaskListener> pendingAsTasks = new LinkedHashMap<>();

                // 将本次选举时 该节点的所有支持者抽取出来生成task 对象
                joinRequestAccumulator.forEach((key, value) -> {
                    final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(key, "elect leader");
                    // 这个value 就是callback 适配成了 JoinTaskListener
                    pendingAsTasks.put(task, new JoinTaskListener(task, value));
                });

                final String stateUpdateSource = "elected-as-master ([" + pendingAsTasks.size() + "] nodes joined)";

                // 本节点变成leader时 需要插入2个特殊的任务
                pendingAsTasks.put(JoinTaskExecutor.newBecomeMasterTask(), (source, e) -> {
                });
                pendingAsTasks.put(JoinTaskExecutor.newFinishElectionTask(), (source, e) -> {
                });
                // 在处理过程中 未通过校验的node 会发送失败信息回给join节点 通过校验的节点会正常返回ack信息 并且如果集群状态发生了变化 会触发钩子
                masterService.submitStateUpdateTasks(stateUpdateSource, pendingAsTasks, ClusterStateTaskConfig.build(Priority.URGENT),
                    joinTaskExecutor);
            } else {
                // 该节点变成了follower时 返回异常信息
                assert newMode == Mode.FOLLOWER : newMode;
                joinRequestAccumulator.values().forEach(joinCallback -> joinCallback.onFailure(
                    new CoordinationStateRejectedException("became follower")));
            }

            // CandidateJoinAccumulator is only closed when becoming leader or follower, otherwise it accumulates all joins received
            // regardless of term.
        }

        @Override
        public String toString() {
            return "CandidateJoinAccumulator{" + joinRequestAccumulator.keySet() +
                ", closed=" + closed + '}';
        }
    }
}
