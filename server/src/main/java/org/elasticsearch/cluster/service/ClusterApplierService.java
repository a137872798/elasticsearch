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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * 有关集群状态变化的逻辑处理都交由该对象处理  ClusterService 则充当一个中枢对象
 */
public class ClusterApplierService extends AbstractLifecycleComponent implements ClusterApplier {
    private static final Logger logger = LogManager.getLogger(ClusterApplierService.class);

    /**
     * 同样是有关定期打印日志的周期
     */
    public static final Setting<TimeValue> CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING =
        Setting.positiveTimeSetting("cluster.service.slow_task_logging_threshold", TimeValue.timeValueSeconds(30),
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    /**
     * 指定对应线程池创建的线程名
     */
    public static final String CLUSTER_UPDATE_THREAD_NAME = "clusterApplierService#updateTask";

    /**
     * 集群相关配置
     */
    private final ClusterSettings clusterSettings;
    protected final ThreadPool threadPool;

    private volatile TimeValue slowTaskLoggingThreshold;

    /**
     * 该线程池允许添加包含优先级的任务
     */
    private volatile PrioritizedEsThreadPoolExecutor threadPoolExecutor;

    /**
     * Those 3 state listeners are changing infrequently - CopyOnWriteArrayList is just fine
     * ClusterStateApplier 处理集群状态的变化  以下是分别存储3种优先级的容器 这样就可以配合 PrioritizedEsThreadPoolExecutor
     */
    private final Collection<ClusterStateApplier> highPriorityStateAppliers = new CopyOnWriteArrayList<>();
    private final Collection<ClusterStateApplier> normalPriorityStateAppliers = new CopyOnWriteArrayList<>();
    private final Collection<ClusterStateApplier> lowPriorityStateAppliers = new CopyOnWriteArrayList<>();

    private final Iterable<ClusterStateApplier> clusterStateAppliers = Iterables.concat(highPriorityStateAppliers,
        normalPriorityStateAppliers, lowPriorityStateAppliers);

    /**
     * 这个接口的定义与 ClusterStateApplier 类似 都是接受一个集群状态变化的事件作为参数
     */
    private final Collection<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<>();

    /**
     * 存储一组延时任务
     */
    private final Collection<TimeoutClusterStateListener> timeoutClusterStateListeners =
        Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * 该对象维护了 所有监听集群变化的监听器 (onMaster offMaster)  该对象也会设置到 clusterStateListeners中
     */
    private final LocalNodeMasterListeners localNodeMasterListeners;

    /**
     * NotifyTimeout 用于定义超时任务 同时还包含关闭超时任务的功能
     */
    private final Queue<NotifyTimeout> onGoingTimeouts = ConcurrentCollections.newQueue();

    /**
     * 维护最新的状态信息
     */
    private final AtomicReference<ClusterState> state; // last applied state

    private final String nodeName;

    /**
     * 该服务管理节点间的连接 或者说创建连接   以及处理req/res
     */
    private NodeConnectionsService nodeConnectionsService;

    /**
     *
     * @param nodeName  当前节点名
     * @param settings   从配置文件中解析的配置
     * @param clusterSettings   集群相关配置
     * @param threadPool   线程池
     */
    public ClusterApplierService(String nodeName, Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.clusterSettings = clusterSettings;
        this.threadPool = threadPool;
        this.state = new AtomicReference<>();
        this.localNodeMasterListeners = new LocalNodeMasterListeners(threadPool);
        this.nodeName = nodeName;

        this.slowTaskLoggingThreshold = CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);
        // 设置一个动态配置  master节点对该配置进行修改时 会通知到普通节点
        this.clusterSettings.addSettingsUpdateConsumer(CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
            this::setSlowTaskLoggingThreshold);
    }

    private void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService) {
        assert this.nodeConnectionsService == null : "nodeConnectionsService is already set";
        this.nodeConnectionsService = nodeConnectionsService;
    }

    @Override
    public void setInitialState(ClusterState initialState) {
        if (lifecycle.started()) {
            throw new IllegalStateException("can't set initial state when started");
        }
        assert state.get() == null : "state is already set";
        state.set(initialState);
    }

    @Override
    protected synchronized void doStart() {
        Objects.requireNonNull(nodeConnectionsService, "please set the node connection service before starting");
        Objects.requireNonNull(state.get(), "please set initial state before starting");
        addListener(localNodeMasterListeners);
        threadPoolExecutor = createThreadPoolExecutor();
    }

    protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
        return EsExecutors.newSinglePrioritizing(
            nodeName + "/" + CLUSTER_UPDATE_THREAD_NAME,
            daemonThreadFactory(nodeName, CLUSTER_UPDATE_THREAD_NAME),
            threadPool.getThreadContext(),
            threadPool.scheduler());
    }

    /**
     * SourcePrioritizedRunnable 具备一个优先级属性
     */
    class UpdateTask extends SourcePrioritizedRunnable implements Function<ClusterState, ClusterState> {
        final ClusterApplyListener listener;
        /**
         * 集群状态的更新函数
         */
        final Function<ClusterState, ClusterState> updateFunction;

        UpdateTask(Priority priority, String source, ClusterApplyListener listener,
                   Function<ClusterState, ClusterState> updateFunction) {
            super(priority, source);
            this.listener = listener;
            this.updateFunction = updateFunction;
        }

        @Override
        public ClusterState apply(ClusterState clusterState) {
            return updateFunction.apply(clusterState);
        }

        @Override
        public void run() {
            runTask(this);
        }
    }

    @Override
    protected synchronized void doStop() {
        for (NotifyTimeout onGoingTimeout : onGoingTimeouts) {
            // 强制关闭所有超时任务 并触发监听器
            onGoingTimeout.cancel();
            try {
                onGoingTimeout.cancel();
                onGoingTimeout.listener.onClose();
            } catch (Exception ex) {
                logger.debug("failed to notify listeners on shutdown", ex);
            }
        }
        ThreadPool.terminate(threadPoolExecutor, 10, TimeUnit.SECONDS);
        // close timeout listeners that did not have an ongoing timeout
        // 关闭所有超时监听器
        timeoutClusterStateListeners.forEach(TimeoutClusterStateListener::onClose);
        // 将监听本地节点是否为master节点的监听器移除
        removeListener(localNodeMasterListeners);
    }

    @Override
    protected synchronized void doClose() {
    }

    /**
     * The current cluster state.
     * Should be renamed to appliedClusterState
     */
    public ClusterState state() {
        assert assertNotCalledFromClusterStateApplier("the applied cluster state is not yet available");
        ClusterState clusterState = this.state.get();
        assert clusterState != null : "initial cluster state not set yet";
        return clusterState;
    }

    /**
     * Adds a high priority applier of updated cluster states.
     */
    public void addHighPriorityApplier(ClusterStateApplier applier) {
        highPriorityStateAppliers.add(applier);
    }

    /**
     * Adds an applier which will be called after all high priority and normal appliers have been called.
     */
    public void addLowPriorityApplier(ClusterStateApplier applier) {
        lowPriorityStateAppliers.add(applier);
    }

    /**
     * Adds a applier of updated cluster states.
     */
    public void addStateApplier(ClusterStateApplier applier) {
        normalPriorityStateAppliers.add(applier);
    }

    /**
     * Removes an applier of updated cluster states.
     */
    public void removeApplier(ClusterStateApplier applier) {
        normalPriorityStateAppliers.remove(applier);
        highPriorityStateAppliers.remove(applier);
        lowPriorityStateAppliers.remove(applier);
    }

    /**
     * Add a listener for updated cluster states
     */
    public void addListener(ClusterStateListener listener) {
        clusterStateListeners.add(listener);
    }

    /**
     * Removes a listener for updated cluster states.
     */
    public void removeListener(ClusterStateListener listener) {
        clusterStateListeners.remove(listener);
    }

    /**
     * Removes a timeout listener for updated cluster states.
     */
    public void removeTimeoutListener(TimeoutClusterStateListener listener) {
        timeoutClusterStateListeners.remove(listener);
        for (Iterator<NotifyTimeout> it = onGoingTimeouts.iterator(); it.hasNext(); ) {
            NotifyTimeout timeout = it.next();
            if (timeout.listener.equals(listener)) {
                timeout.cancel();
                it.remove();
            }
        }
    }

    /**
     * Add a listener for on/off local node master events
     */
    public void addLocalNodeMasterListener(LocalNodeMasterListener listener) {
        localNodeMasterListeners.add(listener);
    }

    /**
     * Adds a cluster state listener that is expected to be removed during a short period of time.
     * If provided, the listener will be notified once a specific time has elapsed.
     *
     * NOTE: the listener is not removed on timeout. This is the responsibility of the caller.
     * 插入的超时监听器会直接执行
     */
    public void addTimeoutListener(@Nullable final TimeValue timeout, final TimeoutClusterStateListener listener) {
        if (lifecycle.stoppedOrClosed()) {
            listener.onClose();
            return;
        }
        // call the post added notification on the same event thread
        try {
            threadPoolExecutor.execute(new SourcePrioritizedRunnable(Priority.HIGH, "_add_listener_") {
                @Override
                public void run() {
                    if (timeout != null) {
                        NotifyTimeout notifyTimeout = new NotifyTimeout(listener, timeout);
                        notifyTimeout.cancellable = threadPool.schedule(notifyTimeout, timeout, ThreadPool.Names.GENERIC);
                        onGoingTimeouts.add(notifyTimeout);
                    }
                    timeoutClusterStateListeners.add(listener);
                    // 当任务成功添加时 会触发 postAdded方法
                    listener.postAdded();
                }
            });
        } catch (EsRejectedExecutionException e) {
            if (lifecycle.stoppedOrClosed()) {
                listener.onClose();
            } else {
                throw e;
            }
        }
    }

    /**
     * @param source
     * @param clusterStateConsumer   处理集群状态的函数
     * @param listener
     * @param priority
     */
    public void runOnApplierThread(final String source, Consumer<ClusterState> clusterStateConsumer,
                                   final ClusterApplyListener listener, Priority priority) {
        submitStateUpdateTask(source, ClusterStateTaskConfig.build(priority),
            // 使用consumer更新集群状态
            (clusterState) -> {
                clusterStateConsumer.accept(clusterState);
                return clusterState;
            },
            listener);
    }

    public void runOnApplierThread(final String source, Consumer<ClusterState> clusterStateConsumer,
                                   final ClusterApplyListener listener) {
        runOnApplierThread(source, clusterStateConsumer, listener, Priority.HIGH);
    }

    public ThreadPool threadPool() {
        return threadPool;
    }

    /**
     * 这个是本对象监听到集群变化 此时不会直接处理 而是将函数转到线程池中调用
     * @param source information where the cluster state came from
     * @param clusterStateSupplier the cluster state supplier which provides the latest cluster state to apply
     * @param listener callback that is invoked after cluster state is applied
     */
    @Override
    public void onNewClusterState(final String source, final Supplier<ClusterState> clusterStateSupplier,
                                  final ClusterApplyListener listener) {
        Function<ClusterState, ClusterState> applyFunction = currentState -> {
            ClusterState nextState = clusterStateSupplier.get();
            if (nextState != null) {
                return nextState;
            } else {
                return currentState;
            }
        };
        submitStateUpdateTask(source, ClusterStateTaskConfig.build(Priority.HIGH), applyFunction, listener);
    }

    /**
     * 提交一个监听集群状态的任务
     * @param source
     * @param config   使用的配置信息 比如 任务的优先级，超时时间
     * @param executor   如何将state更新
     * @param listener  监听处理结果
     */
    private void submitStateUpdateTask(final String source, final ClusterStateTaskConfig config,
                                       final Function<ClusterState, ClusterState> executor,
                                       final ClusterApplyListener listener) {
        if (!lifecycle.started()) {
            return;
        }
        try {
            // 将处理函数包装成一个 updateTask 对象
            UpdateTask updateTask = new UpdateTask(config.priority(), source, new SafeClusterApplyListener(listener, logger), executor);
            if (config.timeout() != null) {
                threadPoolExecutor.execute(updateTask, config.timeout(),
                    // 超时时全部统一由 generic线程池执行任务
                    () -> threadPool.generic().execute(
                        () -> listener.onFailure(source, new ProcessClusterEventTimeoutException(config.timeout(), source))));
            } else {
                threadPoolExecutor.execute(updateTask);
            }
        } catch (EsRejectedExecutionException e) {
            // ignore cases where we are shutting down..., there is really nothing interesting
            // to be done here...
            if (!lifecycle.stoppedOrClosed()) {
                throw e;
            }
        }
    }

    /** asserts that the current thread is <b>NOT</b> the cluster state update thread */
    public static boolean assertNotClusterStateUpdateThread(String reason) {
        assert Thread.currentThread().getName().contains(CLUSTER_UPDATE_THREAD_NAME) == false :
            "Expected current thread [" + Thread.currentThread() + "] to not be the cluster state update thread. Reason: [" + reason + "]";
        return true;
    }

    /** asserts that the current stack trace does <b>NOT</b> involve a cluster state applier */
    private static boolean assertNotCalledFromClusterStateApplier(String reason) {
        if (Thread.currentThread().getName().contains(CLUSTER_UPDATE_THREAD_NAME)) {
            for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
                final String className = element.getClassName();
                final String methodName = element.getMethodName();
                if (className.equals(ClusterStateObserver.class.getName())) {
                    // people may start an observer from an applier
                    return true;
                } else if (className.equals(ClusterApplierService.class.getName())
                    && methodName.equals("callClusterStateAppliers")) {
                    throw new AssertionError("should not be called by a cluster state applier. reason [" + reason + "]");
                }
            }
        }
        return true;
    }

    /**
     * 执行更新集群的任务
     * @param task
     */
    private void runTask(UpdateTask task) {
        if (!lifecycle.started()) {
            logger.debug("processing [{}]: ignoring, cluster applier service not started", task.source);
            return;
        }

        logger.debug("processing [{}]: execute", task.source);
        final ClusterState previousClusterState = state.get();

        long startTimeMS = currentTimeInMillis();
        final StopWatch stopWatch = new StopWatch();
        final ClusterState newClusterState;
        try {
            try (Releasable ignored = stopWatch.timing("running task [" + task.source + ']')) {
                newClusterState = task.apply(previousClusterState);
            }
        } catch (Exception e) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, currentTimeInMillis() - startTimeMS));
            logger.trace(() -> new ParameterizedMessage(
                "failed to execute cluster state applier in [{}], state:\nversion [{}], source [{}]\n{}",
                executionTime, previousClusterState.version(), task.source, previousClusterState), e);
            warnAboutSlowTaskIfNeeded(executionTime, task.source, stopWatch);
            task.listener.onFailure(task.source, e);
            return;
        }

        // 代表集群状态前后没有变化  不需要调用 applyChanges  也就是不需要通知相关监听器
        if (previousClusterState == newClusterState) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, currentTimeInMillis() - startTimeMS));
            logger.debug("processing [{}]: took [{}] no change in cluster state", task.source, executionTime);
            warnAboutSlowTaskIfNeeded(executionTime, task.source, stopWatch);
            task.listener.onSuccess(task.source);
        } else {
            if (logger.isTraceEnabled()) {
                logger.debug("cluster state updated, version [{}], source [{}]\n{}", newClusterState.version(), task.source,
                    newClusterState);
            } else {
                logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), task.source);
            }
            try {
                applyChanges(task, previousClusterState, newClusterState, stopWatch);
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, currentTimeInMillis() - startTimeMS));
                logger.debug("processing [{}]: took [{}] done applying updated cluster state (version: {}, uuid: {})", task.source,
                    executionTime, newClusterState.version(),
                    newClusterState.stateUUID());
                warnAboutSlowTaskIfNeeded(executionTime, task.source, stopWatch);
                task.listener.onSuccess(task.source);
            } catch (Exception e) {
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, currentTimeInMillis() - startTimeMS));
                if (logger.isTraceEnabled()) {
                    logger.warn(new ParameterizedMessage(
                            "failed to apply updated cluster state in [{}]:\nversion [{}], uuid [{}], source [{}]\n{}",
                            executionTime, newClusterState.version(), newClusterState.stateUUID(), task.source, newClusterState), e);
                } else {
                    logger.warn(new ParameterizedMessage(
                            "failed to apply updated cluster state in [{}]:\nversion [{}], uuid [{}], source [{}]",
                            executionTime, newClusterState.version(), newClusterState.stateUUID(), task.source), e);
                }
                // failing to apply a cluster state with an exception indicates a bug in validation or in one of the appliers; if we
                // continue we will retry with the same cluster state but that might not help.
                assert applicationMayFail();
                task.listener.onFailure(task.source, e);
            }
        }
    }

    /**
     * 通知所有监听器
     * @param task
     * @param previousClusterState
     * @param newClusterState
     * @param stopWatch
     */
    private void applyChanges(UpdateTask task, ClusterState previousClusterState, ClusterState newClusterState, StopWatch stopWatch) {
        ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(task.source, newClusterState, previousClusterState);
        // new cluster state, notify all listeners
        // 通过前后状态 生成节点的变化对象
        final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
        if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
            String summary = nodesDelta.shortSummary();
            if (summary.length() > 0) {
                logger.info("{}, term: {}, version: {}, reason: {}",
                    summary, newClusterState.term(), newClusterState.version(), task.source);
            }
        }

        logger.trace("connecting to nodes of cluster state with version {}", newClusterState.version());
        try (Releasable ignored = stopWatch.timing("connecting to new nodes")) {
            // 当前节点会立即与集群间其他节点建立连接  TODO 应该是只要master节点与其他节点建立连接就可以了啊 并且连接之后是想要做什么呢  应该只需要与共享分片的节点连接 用于拷贝数据
            connectToNodesAndWait(newClusterState);
        }

        // nothing to do until we actually recover from the gateway or any other block indicates we need to disable persistency
        // TODO  gateway 和 block是什么 ???
        // 代表状态开启了持久化 并且元数据发生了变化
        if (clusterChangedEvent.state().blocks().disableStatePersistence() == false && clusterChangedEvent.metadataChanged()) {
            logger.debug("applying settings from cluster state with version {}", newClusterState.version());
            final Settings incomingSettings = clusterChangedEvent.state().metadata().settings();
            try (Releasable ignored = stopWatch.timing("applying settings")) {
                // 更新本地配置
                clusterSettings.applySettings(incomingSettings);
            }
        }

        logger.debug("apply cluster state with version {}", newClusterState.version());
        // 通知监听器
        callClusterStateAppliers(clusterChangedEvent, stopWatch);

        // 这里又断开连接了???
        nodeConnectionsService.disconnectFromNodesExcept(newClusterState.nodes());

        logger.debug("set locally applied cluster state to version {}", newClusterState.version());
        state.set(newClusterState);

        // 触发另一套监听器
        callClusterStateListeners(clusterChangedEvent, stopWatch);
    }

    /**
     * 与集群中其他节点建立连接
     * @param newClusterState
     */
    protected void connectToNodesAndWait(ClusterState newClusterState) {
        // can't wait for an ActionFuture on the cluster applier thread, but we do want to block the thread here, so use a CountDownLatch.
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        nodeConnectionsService.connectToNodes(newClusterState.nodes(), countDownLatch::countDown);
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.debug("interrupted while connecting to nodes, continuing", e);
            Thread.currentThread().interrupt();
        }
    }

    private void callClusterStateAppliers(ClusterChangedEvent clusterChangedEvent, StopWatch stopWatch) {
        clusterStateAppliers.forEach(applier -> {
            logger.trace("calling [{}] with change to version [{}]", applier, clusterChangedEvent.state().version());
            try (Releasable ignored = stopWatch.timing("running applier [" + applier + "]")) {
                applier.applyClusterState(clusterChangedEvent);
            }
        });
    }

    private void callClusterStateListeners(ClusterChangedEvent clusterChangedEvent, StopWatch stopWatch) {
        Stream.concat(clusterStateListeners.stream(), timeoutClusterStateListeners.stream()).forEach(listener -> {
            try {
                logger.trace("calling [{}] with change to version [{}]", listener, clusterChangedEvent.state().version());
                try (Releasable ignored = stopWatch.timing("notifying listener [" + listener + "]")) {
                    listener.clusterChanged(clusterChangedEvent);
                }
            } catch (Exception ex) {
                logger.warn("failed to notify ClusterStateListener", ex);
            }
        });
    }

    /**
     * 包装原本的监听器 捕获处理异常并打印日志
     */
    private static class SafeClusterApplyListener implements ClusterApplyListener {
        private final ClusterApplyListener listener;
        private final Logger logger;

        SafeClusterApplyListener(ClusterApplyListener listener, Logger logger) {
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Exception e) {
            try {
                listener.onFailure(source, e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error(new ParameterizedMessage(
                        "exception thrown by listener notifying of failure from [{}]", source), inner);
            }
        }

        @Override
        public void onSuccess(String source) {
            try {
                listener.onSuccess(source);
            } catch (Exception e) {
                logger.error(new ParameterizedMessage(
                    "exception thrown by listener while notifying of cluster state processed from [{}]", source), e);
            }
        }
    }

    private void warnAboutSlowTaskIfNeeded(TimeValue executionTime, String source, StopWatch stopWatch) {
        if (executionTime.getMillis() > slowTaskLoggingThreshold.getMillis()) {
            logger.warn("cluster state applier task [{}] took [{}] which is above the warn threshold of [{}]: {}", source, executionTime,
                slowTaskLoggingThreshold, Arrays.stream(stopWatch.taskInfo())
                    .map(ti -> '[' + ti.getTaskName() + "] took [" + ti.getTime().millis() + "ms]").collect(Collectors.joining(", ")));
        }
    }


    /**
     * 该对象会在一定延时后触发
     */
    class NotifyTimeout implements Runnable {
        /**
         * 定义了处理逻辑的模板
         */
        final TimeoutClusterStateListener listener;
        /**
         * 在多久后触发
         */
        final TimeValue timeout;
        /**
         * 这个是用来关闭超时任务的
         */
        volatile Scheduler.Cancellable cancellable;

        NotifyTimeout(TimeoutClusterStateListener listener, TimeValue timeout) {
            this.listener = listener;
            this.timeout = timeout;
        }

        public void cancel() {
            if (cancellable != null) {
                cancellable.cancel();
            }
        }

        /**
         *
         */
        @Override
        public void run() {
            if (cancellable != null && cancellable.isCancelled()) {
                return;
            }
            // 检测ClusterApplierService 生命周期是否已经结束 是的话触发 onClose 否则触发超时
            if (lifecycle.stoppedOrClosed()) {
                listener.onClose();
            } else {
                listener.onTimeout(this.timeout);
            }
            // note, we rely on the listener to remove itself in case of timeout if needed
        }
    }

    /**
     * 检测当前节点是否变更为master节点
     */
    private static class LocalNodeMasterListeners implements ClusterStateListener {

        /**
         * 本地节点变成master节点 或者降级为普通节点时 会触发不同的逻辑
         */
        private final List<LocalNodeMasterListener> listeners = new CopyOnWriteArrayList<>();

        /**
         * LocalNodeMasterListener 处理逻辑一般是交给线程池做的
         */
        private final ThreadPool threadPool;

        /**
         * 当前节点是否是 master节点
         */
        private volatile boolean master = false;

        private LocalNodeMasterListeners(ThreadPool threadPool) {
            this.threadPool = threadPool;
        }

        /**
         * @param event
         */
        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            // 代表本节点此时晋升为master节点  使用线程池执行对应逻辑
            if (!master && event.localNodeMaster()) {
                master = true;

                for (LocalNodeMasterListener listener : listeners) {
                    // 根据线程池名字 从线程池总控对象中获取对应的线程池
                    java.util.concurrent.Executor executor = threadPool.executor(listener.executorName());
                    executor.execute(new OnMasterRunnable(listener));
                }
                return;
            }

            if (master && !event.localNodeMaster()) {
                master = false;
                for (LocalNodeMasterListener listener : listeners) {
                    java.util.concurrent.Executor executor = threadPool.executor(listener.executorName());
                    executor.execute(new OffMasterRunnable(listener));
                }
            }
        }

        private void add(LocalNodeMasterListener listener) {
            listeners.add(listener);
        }

    }


    /**
     * 代表当前节点晋升成master节点
     */
    private static class OnMasterRunnable implements Runnable {

        private final LocalNodeMasterListener listener;

        private OnMasterRunnable(LocalNodeMasterListener listener) {
            this.listener = listener;
        }

        @Override
        public void run() {
            listener.onMaster();
        }
    }

    /**
     * 当前节点降级为普通节点
     */
    private static class OffMasterRunnable implements Runnable {

        private final LocalNodeMasterListener listener;

        private OffMasterRunnable(LocalNodeMasterListener listener) {
            this.listener = listener;
        }

        @Override
        public void run() {
            listener.offMaster();
        }
    }

    // this one is overridden in tests so we can control time
    protected long currentTimeInMillis() {
        return threadPool.relativeTimeInMillis();
    }

    // overridden by tests that need to check behaviour in the event of an application failure
    protected boolean applicationMayFail() {
        return false;
    }
}
