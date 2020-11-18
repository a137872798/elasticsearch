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

package org.elasticsearch.action.support.master;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.MasterNodeChangePredicate;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * A base class for operations that needs to be performed on the master node.
 * action在本节点通过 RestAction进行映射  当发送到其他节点时 通过transportService 进行匹配
 */
public abstract class TransportMasterNodeAction<Request extends MasterNodeRequest<Request>, Response extends ActionResponse>
    extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportMasterNodeAction.class);

    protected final ThreadPool threadPool;
    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * 线程池类型
     */
    private final String executor;

    /**
     *
     * @param actionName
     * @param transportService
     * @param clusterService
     * @param threadPool
     * @param actionFilters  在初始化某个action时 往往指定了相关的filter
     * @param request
     * @param indexNameExpressionResolver
     */
    protected TransportMasterNodeAction(String actionName, TransportService transportService,
                                        ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                        Writeable.Reader<Request> request, IndexNameExpressionResolver indexNameExpressionResolver) {
        this(actionName, true, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    /**
     *
     * @param actionName
     * @param canTripCircuitBreaker  检测处理该action是否可能触发熔断
     * @param transportService
     * @param clusterService
     * @param threadPool
     * @param actionFilters
     * @param request
     * @param indexNameExpressionResolver
     */
    protected TransportMasterNodeAction(String actionName, boolean canTripCircuitBreaker,
                                        TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                        ActionFilters actionFilters, Writeable.Reader<Request> request,
                                        IndexNameExpressionResolver indexNameExpressionResolver) {
        super(actionName, canTripCircuitBreaker, transportService, actionFilters, request);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = executor();
    }

    protected abstract String executor();

    protected abstract Response read(StreamInput in) throws IOException;

    /**
     * 对应在leader节点上执行操作   有关元数据变化的操作肯定只能在leader节点上  因为分布式一致性算法采用了CP的实现
     * @param task
     * @param request
     * @param state
     * @param listener
     * @throws Exception
     */
    protected abstract void masterOperation(Task task, Request request, ClusterState state,
                                            ActionListener<Response> listener) throws Exception;

    protected boolean localExecute(Request request) {
        return false;
    }

    /**
     * 判断本次请求是否应当被阻塞
     * @param request
     * @param state
     * @return
     */
    protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);

    /**
     * 当通过一系列的过滤器后 最终调用该方法  这里需要定义处理action的真正逻辑
     * @param task
     * @param request
     * @param listener
     */
    @Override
    protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(task, request, listener).start();
    }

    /**
     * 这里就是线程的切换点  也就是默认情况下在拦截器中都还是IO线程 而到了最后一步 切换到了线程池中的其他线程
     * 当然用户可以提前切换线程
     */
    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final Request request;

        private volatile ClusterStateObserver observer;
        private final Task task;

        /**
         *
         * @param task  在RestController接收请求后 会先注册到TaskManager上 并包装成task对象
         * @param request   请求体中的数据
         * @param listener   当完成任务时触发的监听器
         */
        AsyncSingleAction(Task task, Request request, ActionListener<Response> listener) {
            this.task = task;
            this.request = request;
            if (task != null) {
                // 为 req设置task
                request.setParentTask(clusterService.localNode().getId(), task.getId());
            }
            this.listener = listener;
        }

        public void start() {
            // 获取此时的集群状态
            ClusterState state = clusterService.state();
            logger.trace("starting processing request [{}] with cluster state version [{}]", request, state.version());
            this.observer
                = new ClusterStateObserver(state, clusterService, request.masterNodeTimeout(), logger, threadPool.getThreadContext());
            doStart(state);
        }

        /**
         * 处理action
         * @param clusterState
         */
        protected void doStart(ClusterState clusterState) {
            try {
                // 一个检测state新旧的函数
                final Predicate<ClusterState> masterChangePredicate = MasterNodeChangePredicate.build(clusterState);
                final DiscoveryNodes nodes = clusterState.nodes();
                // 检测当前节点是否是 leader节点  或者该req将在当前节点执行
                if (nodes.isLocalNodeElectedMaster() || localExecute(request)) {
                    // check for block, if blocked, retry, else, execute locally
                    final ClusterBlockException blockException = checkBlock(request, clusterState);
                    // 发现本次操作被阻塞
                    if (blockException != null) {
                        // 本次任务无法重试 以失败方式触发监听器
                        if (!blockException.retryable()) {
                            logger.trace("can't execute due to a non-retryable cluster block", blockException);
                            listener.onFailure(blockException);
                        } else {
                            logger.debug("can't execute due to a cluster block, retrying", blockException);
                            retry(blockException, newState -> {
                                try {
                                    ClusterBlockException newException = checkBlock(request, newState);
                                    return (newException == null || !newException.retryable());
                                } catch (Exception e) {
                                    // accept state as block will be rechecked by doStart() and listener.onFailure() then called
                                    logger.debug("exception occurred during cluster block checking, accepting state", e);
                                    return true;
                                }
                            });
                        }
                        // 代表本次操作不会被阻塞
                    } else {
                        // 对监听器进行包装
                        ActionListener<Response> delegate = ActionListener.delegateResponse(listener,
                            // 当出现异常时 检测异常是否可重试
                            (delegatedListener, t) -> {
                            if (t instanceof FailedToCommitClusterStateException || t instanceof NotMasterException) {
                                logger.debug(() -> new ParameterizedMessage("master could not publish cluster state or " +
                                    "stepped down before publishing action [{}], scheduling a retry", actionName), t);
                                retry(t, masterChangePredicate);
                            } else {
                                logger.debug("unexpected exception during publication", t);
                                delegatedListener.onFailure(t);
                            }
                        });

                        // 在这里进行了线程的切换
                        threadPool.executor(executor)
                            .execute(ActionRunnable.wrap(delegate, l -> masterOperation(task, request, clusterState, l)));
                    }
                } else {

                    // 当前不在master节点上执行任务   此时还未选举出leader节点 整个集群处于不可用状态  监听集群状态变化
                    if (nodes.getMasterNode() == null) {
                        logger.debug("no known master node, scheduling a retry");
                        retry(null, masterChangePredicate);
                    } else {
                        DiscoveryNode masterNode = nodes.getMasterNode();
                        final String actionName = getMasterActionName(masterNode);
                        logger.trace("forwarding request [{}] to master [{}]", actionName, masterNode);

                        // 因为这个 MasterNodeAction 本身已经要求 相关的action 必须在leader节点上执行 所以将请求发送到leader节点
                        transportService.sendRequest(masterNode, actionName, request,
                            // 默认情况下 execute是 SAME 代表回调逻辑直接在当前线程执行
                            new ActionListenerResponseHandler<Response>(listener, TransportMasterNodeAction.this::read) {
                                @Override
                                public void handleException(final TransportException exp) {
                                    Throwable cause = exp.unwrapCause();
                                    if (cause instanceof ConnectTransportException ||
                                        (exp instanceof RemoteTransportException && cause instanceof NodeClosedException)) {
                                        // we want to retry here a bit to see if a new master is elected
                                        logger.debug("connection exception while trying to forward request with action name [{}] to " +
                                                "master node [{}], scheduling a retry. Error: [{}]",
                                            actionName, nodes.getMasterNode(), exp.getDetailedMessage());
                                        // 允许重试
                                        retry(cause, masterChangePredicate);
                                    } else {
                                        logger.trace(new ParameterizedMessage("failure when forwarding request [{}] to master [{}]",
                                            actionName, masterNode), exp);
                                        listener.onFailure(exp);
                                    }
                                }
                        });
                    }
                }
            } catch (Exception e) {
                logger.trace("top-level failure", e);
                listener.onFailure(e);
            }
        }

        /**
         * 等待集群状态的变化 之后重试任务
         * @param failure
         * @param statePredicate 检测新的集群状态是否满足条件
         */
        private void retry(final Throwable failure, final Predicate<ClusterState> statePredicate) {
            // 等待集群变化后 可能就会清除掉某些block
            observer.waitForNextChange(
                new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        logger.trace("retrying with cluster state version [{}]", state.version());
                        doStart(state);
                    }

                    // 当因异常原因导致重试失败时  触发action.listener.onFailure

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        logger.debug(() -> new ParameterizedMessage("timed out while retrying [{}] after failure (timeout [{}])",
                            actionName, timeout), failure);
                        listener.onFailure(new MasterNotDiscoveredException(failure));
                    }
                }, statePredicate);
        }
    }

    /**
     * Allows to conditionally return a different master node action name in the case an action gets renamed.
     * This mainly for backwards compatibility should be used rarely
     */
    protected String getMasterActionName(DiscoveryNode node) {
        return actionName;
    }
}
