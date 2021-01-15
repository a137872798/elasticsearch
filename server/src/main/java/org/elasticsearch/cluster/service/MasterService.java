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
import org.elasticsearch.Assertions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * 执行更新集群状态变化的任务
 */
public class MasterService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(MasterService.class);

    public static final Setting<TimeValue> MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING =
        Setting.positiveTimeSetting("cluster.service.slow_master_task_logging_threshold", TimeValue.timeValueSeconds(10),
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    static final String MASTER_UPDATE_THREAD_NAME = "masterService#updateTask";

    /**
     * 集群状态发布对象
     * 实际上就是 Coordinator  负责通知到集群的其他节点
     */
    ClusterStatePublisher clusterStatePublisher;

    /**
     * 当前节点的名字
     */
    private final String nodeName;

    /**
     * 用于获取当前集群状态的对象
     * 对应 Coordinator::getStateForMasterService
     */
    private java.util.function.Supplier<ClusterState> clusterStateSupplier;

    /**
     * 代表每隔多少时间更新一次集群相关的配置
     */
    private volatile TimeValue slowTaskLoggingThreshold;

    protected final ThreadPool threadPool;

    /**
     * 该线程池使用优先队列作为任务队列 所以可以为任务安排优先级
     */
    private volatile PrioritizedEsThreadPoolExecutor threadPoolExecutor;

    /**
     * 提交批量任务的对象
     */
    private volatile Batcher taskBatcher;


    /**
     *
     * @param settings   所有解析出来的配置
     * @param clusterSettings    仅集群相关的配置
     * @param threadPool     管理所有线程池
     */
    public MasterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.nodeName = Objects.requireNonNull(Node.NODE_NAME_SETTING.get(settings));

        this.slowTaskLoggingThreshold = MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);
        // 注册当配置发生变化时的处理逻辑
        clusterSettings.addSettingsUpdateConsumer(MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING, this::setSlowTaskLoggingThreshold);

        this.threadPool = threadPool;
    }

    private void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setClusterStatePublisher(ClusterStatePublisher publisher) {
        clusterStatePublisher = publisher;
    }

    public synchronized void setClusterStateSupplier(java.util.function.Supplier<ClusterState> clusterStateSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
    }

    /**
     * 做初始化工作  也就是初始化优先级线程池 以及使用它来初始化 batcher对象
     */
    @Override
    protected synchronized void doStart() {
        Objects.requireNonNull(clusterStatePublisher, "please set a cluster state publisher before starting");
        Objects.requireNonNull(clusterStateSupplier, "please set a cluster state supplier before starting");
        threadPoolExecutor = createThreadPoolExecutor();
        taskBatcher = new Batcher(logger, threadPoolExecutor);
    }

    /**
     * 创建 PrioritizedEsThreadPoolExecutor
     * @return
     */
    protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
        return EsExecutors.newSinglePrioritizing(
                nodeName + "/" + MASTER_UPDATE_THREAD_NAME,
                daemonThreadFactory(nodeName, MASTER_UPDATE_THREAD_NAME),
                threadPool.getThreadContext(),
                threadPool.scheduler());
    }

    /**
     * 处理批任务的一个模板
     */
    @SuppressWarnings("unchecked")
    class Batcher extends TaskBatcher {

        Batcher(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor) {
            super(logger, threadExecutor);
        }

        /**
         * 当任务超时时触发
         * @param tasks   当前待处理的任务
         * @param timeout
         */
        @Override
        protected void onTimeout(List<? extends BatchedTask> tasks, TimeValue timeout) {
            // 获取名为 GENERIC 的线程池
            // 以失败方式触发监听器
            threadPool.generic().execute(
                () -> tasks.forEach(
                    // 这里相当于是做适配  由子类来定义超时处理逻辑
                    task -> ((UpdateTask) task).listener.onFailure(task.source,
                        new ProcessClusterEventTimeoutException(timeout, task.source))));
        }

        /**
         * 执行批任务
         * @param batchingKey 一般就是 executor
         * @param tasks  本次所有任务对象
         * @param tasksSummary 本批任务的描述信息
         */
        @Override
        protected void run(Object batchingKey, List<? extends BatchedTask> tasks, String tasksSummary) {
            // 这里要求标注一批任务的 key 是 taskExecutor对象 用于处理updateTask
            ClusterStateTaskExecutor<Object> taskExecutor = (ClusterStateTaskExecutor<Object>) batchingKey;
            List<UpdateTask> updateTasks = (List<UpdateTask>) tasks;
            // 将多个任务合并成一个 input对象
            runTasks(new TaskInputs(taskExecutor, updateTasks, tasksSummary));
        }

        /**
         * 每个能被批量执行的任务必须继承自 BatchedTask
         * 这里是更新任务
         */
        class UpdateTask extends BatchedTask {

            /**
             * 该对象负责处理超时逻辑
             */
            final ClusterStateTaskListener listener;

            UpdateTask(Priority priority, String source, Object task, ClusterStateTaskListener listener,
                       ClusterStateTaskExecutor<?> executor) {
                super(priority, source, executor, task);
                this.listener = listener;
            }

            /**
             * 用于生成任务描述
             * @param tasks
             * @return
             */
            @Override
            public String describeTasks(List<? extends BatchedTask> tasks) {
                return ((ClusterStateTaskExecutor<Object>) batchingKey).describeTasks(
                    tasks.stream().map(BatchedTask::getTask).collect(Collectors.toList()));
            }
        }
    }

    /**
     * 关闭线程池对象
     */
    @Override
    protected synchronized void doStop() {
        ThreadPool.terminate(threadPoolExecutor, 10, TimeUnit.SECONDS);
    }

    @Override
    protected synchronized void doClose() {
    }

    /**
     * The current cluster state exposed by the discovery layer. Package-visible for tests.
     * 通过注册中心获取集群最新状态
     */
    ClusterState state() {
        return clusterStateSupplier.get();
    }

    /**
     * 检测当前线程是否线程池中的线程  因为使用的线程池线程数固定为1  且名称中携带了MASTER_UPDATE_THREAD_NAME
     * @return
     */
    private static boolean isMasterUpdateThread() {
        return Thread.currentThread().getName().contains(MASTER_UPDATE_THREAD_NAME);
    }

    public static boolean assertNotMasterUpdateThread(String reason) {
        assert isMasterUpdateThread() == false :
            "Expected current thread [" + Thread.currentThread() + "] to not be the master service thread. Reason: [" + reason + "]";
        return true;
    }

    /**
     * 执行一组任务 更新clusterState 并发布到集群中
     * @param taskInputs 将一组UpdateTask 和一个 taskExecutor合并
     */
    private void runTasks(TaskInputs taskInputs) {
        final String summary = taskInputs.summary;
        if (!lifecycle.started()) {
            logger.debug("processing [{}]: ignoring, master service not started", summary);
            return;
        }

        logger.debug("executing cluster state update for [{}]", summary);

        // 在更新集群状态前 获取之前的集群信息
        final ClusterState previousClusterState = state();

        // 当本节点不是leader节点时 且该任务要求必须在leader上执行 触发noLongerMaster后返回
        if (!previousClusterState.nodes().isLocalNodeElectedMaster() && taskInputs.runOnlyWhenMaster()) {
            logger.debug("failing [{}]: local node is no longer master", summary);
            taskInputs.onNoLongerMaster();
            return;
        }

        final long computationStartTime = threadPool.relativeTimeInMillis();
        // 处理任务并输出一个结果对象
        final TaskOutputs taskOutputs = calculateTaskOutputs(taskInputs, previousClusterState);
        // 处理失败的任务
        taskOutputs.notifyFailedTasks();
        // 打印任务执行时长
        final TimeValue computationTime = getTimeSince(computationStartTime);
        logExecutionTime(computationTime, "compute cluster state update", summary);

        // 代表本次更新任务 并没有改变集群状态  触发对应钩子
        if (taskOutputs.clusterStateUnchanged()) {
            final long notificationStartTime = threadPool.relativeTimeInMillis();
            // 因为集群状态没有发生变化  直接触发完成钩子
            taskOutputs.notifySuccessfulTasksOnUnchangedClusterState();
            final TimeValue executionTime = getTimeSince(notificationStartTime);
            logExecutionTime(executionTime, "notify listeners on unchanged cluster state", summary);
        } else {

            final ClusterState newClusterState = taskOutputs.newClusterState;
            if (logger.isTraceEnabled()) {
                logger.trace("cluster state updated, source [{}]\n{}", summary, newClusterState);
            } else {
                logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), summary);
            }
            final long publicationStartTime = threadPool.relativeTimeInMillis();
            try {
                // 将集群变化事件通知到所有节点
                ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(summary, newClusterState, previousClusterState);
                // new cluster state, notify all listeners
                final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
                // 集群内节点数量发生变化 打印日志
                if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                    String nodesDeltaSummary = nodesDelta.shortSummary();
                    if (nodesDeltaSummary.length() > 0) {
                        logger.info("{}, term: {}, version: {}, delta: {}",
                            summary, newClusterState.term(), newClusterState.version(), nodesDeltaSummary);
                    }
                }

                logger.debug("publishing cluster state version [{}]", newClusterState.version());

                // 将最新状态发布到集群中
                publish(clusterChangedEvent, taskOutputs, publicationStartTime);
            } catch (Exception e) {
                handleException(summary, publicationStartTime, newClusterState, e);
            }
        }
    }

    /**
     * threadPool.relativeTimeInMillis() 相当于是一个近似的当前时间  这里返回的是2个时间差
     * 为了避免频繁调用 System.current的开销  使用一个定时线程池更新当前时间
     * @param startTimeMillis
     * @return
     */
    private TimeValue getTimeSince(long startTimeMillis) {
        return TimeValue.timeValueMillis(Math.max(0, threadPool.relativeTimeInMillis() - startTimeMillis));
    }

    /**
     * 将集群状态变化通知到所有节点
     * @param clusterChangedEvent
     * @param taskOutputs
     * @param startTimeMillis
     */
    protected void publish(ClusterChangedEvent clusterChangedEvent, TaskOutputs taskOutputs, long startTimeMillis) {
        final PlainActionFuture<Void> fut = new PlainActionFuture<Void>() {
            @Override
            protected boolean blockingAllowed() {
                return isMasterUpdateThread() || super.blockingAllowed();
            }
        };

        // 当发布成功后会设置future的结果
        // ack监听器要求请求发送到所有节点上 才会触发
        clusterStatePublisher.publish(clusterChangedEvent, fut, taskOutputs.createAckListener(threadPool, clusterChangedEvent.state()));

        // indefinitely wait for publication to complete
        try {
            // 阻塞直到pub完成
            FutureUtils.get(fut);
            // 任务执行成功 触发监听器
            onPublicationSuccess(clusterChangedEvent, taskOutputs);
        } catch (Exception e) {
            // 针对失败的情况 调用future.get 会抛出异常
            onPublicationFailed(clusterChangedEvent, taskOutputs, startTimeMillis, e);
        }
    }

    /**
     * 触发发布成功时的钩子
     * @param clusterChangedEvent
     * @param taskOutputs
     */
    void onPublicationSuccess(ClusterChangedEvent clusterChangedEvent, TaskOutputs taskOutputs) {
        final long notificationStartTime = threadPool.relativeTimeInMillis();
        // 当发布成功后才触发 stateProcess钩子
        taskOutputs.processedDifferentClusterState(clusterChangedEvent.previousState(), clusterChangedEvent.state());

        try {
            taskOutputs.clusterStatePublished(clusterChangedEvent);
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage(
                "exception thrown while notifying executor of new cluster state publication [{}]",
                clusterChangedEvent.source()), e);
        }
        final TimeValue executionTime = getTimeSince(notificationStartTime);
        logExecutionTime(executionTime, "notify listeners on successful publication of cluster state (version: "
            + clusterChangedEvent.state().version() + ", uuid: " + clusterChangedEvent.state().stateUUID() + ')',
            clusterChangedEvent.source());
    }

    /**
     * 当发布任务失败时
     * @param clusterChangedEvent
     * @param taskOutputs
     * @param startTimeMillis
     * @param exception
     */
    void onPublicationFailed(ClusterChangedEvent clusterChangedEvent, TaskOutputs taskOutputs, long startTimeMillis, Exception exception) {
        // 当此时正在执行发布任务  或者本节点不再是leader 或者term不匹配的情况会抛出该异常 一般要配合重试机制
        if (exception instanceof FailedToCommitClusterStateException) {
            final long version = clusterChangedEvent.state().version();
            logger.warn(() -> new ParameterizedMessage(
                "failing [{}]: failed to commit cluster state version [{}]", clusterChangedEvent.source(), version), exception);
            // 触发所有之前执行成功的批任务的 onFailure方法  也就是join请求都会失败
            taskOutputs.publishingFailed((FailedToCommitClusterStateException) exception);
        } else {
            handleException(clusterChangedEvent.source(), startTimeMillis, clusterChangedEvent.state(), exception);
        }
    }

    private void handleException(String summary, long startTimeMillis, ClusterState newClusterState, Exception e) {
        final TimeValue executionTime = getTimeSince(startTimeMillis);
        final long version = newClusterState.version();
        final String stateUUID = newClusterState.stateUUID();
        final String fullState = newClusterState.toString();
        logger.warn(new ParameterizedMessage(
                "took [{}] and then failed to publish updated cluster state (version: {}, uuid: {}) for [{}]:\n{}",
                executionTime,
                version,
                stateUUID,
                summary,
                fullState),
            e);
        // TODO: do we want to call updateTask.onFailure here?
    }

    /**
     * 处理一组任务对象 并将结果合并输出
     * @param taskInputs
     * @param previousClusterState
     * @return
     */
    private TaskOutputs calculateTaskOutputs(TaskInputs taskInputs, ClusterState previousClusterState) {
        // 这里主要是调用 executor处理批任务并返回结果
        ClusterTasksResult<Object> clusterTasksResult = executeTasks(taskInputs, previousClusterState);
        // 当路由表 或者 metadata 发生变化时 增加版本号
        ClusterState newClusterState = patchVersions(previousClusterState, clusterTasksResult);
        // 将本次执行任务的所有信息合并成一个 output对象
        return new TaskOutputs(taskInputs, previousClusterState, newClusterState, getNonFailedTasks(taskInputs, clusterTasksResult),
            clusterTasksResult.executionResults);
    }

    /**
     * 更新版本号
     * @param previousClusterState
     * @param executionResult
     * @return
     */
    private ClusterState patchVersions(ClusterState previousClusterState, ClusterTasksResult<?> executionResult) {
        // 本次执行更新后得到的集群状态
        ClusterState newClusterState = executionResult.resultingState;

        // 当发现集群状态发生变化时
        if (previousClusterState != newClusterState) {
            // only the master controls the version numbers
            Builder builder = incrementVersion(newClusterState);
            // 什么变化就更新相关的版本号
            if (previousClusterState.routingTable() != newClusterState.routingTable()) {
                builder.routingTable(RoutingTable.builder(newClusterState.routingTable())
                    .version(newClusterState.routingTable().version() + 1).build());
            }
            if (previousClusterState.metadata() != newClusterState.metadata()) {
                builder.metadata(Metadata.builder(newClusterState.metadata()).version(newClusterState.metadata().version() + 1));
            }

            newClusterState = builder.build();
        }

        return newClusterState;
    }

    public Builder incrementVersion(ClusterState clusterState) {
        return ClusterState.builder(clusterState).incrementVersion();
    }

    /**
     * Submits a cluster state update task; unlike {@link #submitStateUpdateTask(String, Object, ClusterStateTaskConfig,
     * ClusterStateTaskExecutor, ClusterStateTaskListener)}, submitted updates will not be batched.
     *
     * @param source     the source of the cluster state update task
     * @param updateTask the full context for the cluster state update
     *                   task
     *                  提交某个更新任务
     */
    public <T extends ClusterStateTaskConfig & ClusterStateTaskExecutor<T> & ClusterStateTaskListener>
        void submitStateUpdateTask(
        String source, T updateTask) {
        submitStateUpdateTask(source, updateTask, updateTask, updateTask, updateTask);
    }

    /**
     * Submits a cluster state update task; submitted updates will be
     * batched across the same instance of executor. The exact batching
     * semantics depend on the underlying implementation but a rough
     * guideline is that if the update task is submitted while there
     * are pending update tasks for the same executor, these update
     * tasks will all be executed on the executor in a single batch
     *
     * @param source   the source of the cluster state update task
     * @param task     the state needed for the cluster state update task
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param listener callback after the cluster state update task
     *                 completes
     * @param <T>      the type of the cluster state update task state
     *                 提交某个更新任务 同时还有处理该任务使用 executor 以及监听器
     */
    public <T> void submitStateUpdateTask(String source, T task,
                                          ClusterStateTaskConfig config,
                                          ClusterStateTaskExecutor<T> executor,
                                          ClusterStateTaskListener listener) {
        submitStateUpdateTasks(source, Collections.singletonMap(task, listener), config, executor);
    }

    /**
     * Output created by executing a set of tasks provided as TaskInputs
     * 代表处理某个批任务对应的结果
     */
    class TaskOutputs {
        /**
         * 之前处理的批任务
         */
        final TaskInputs taskInputs;
        // 前后集群状态
        final ClusterState previousClusterState;
        final ClusterState newClusterState;
        /**
         * 仅包含处理成功的task
         */
        final List<Batcher.UpdateTask> nonFailedTasks;

        /**
         * 每个任务对应的结果
         */
        final Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults;

        TaskOutputs(TaskInputs taskInputs, ClusterState previousClusterState,
                           ClusterState newClusterState,
                           List<Batcher.UpdateTask> nonFailedTasks,
                           Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults) {
            this.taskInputs = taskInputs;
            this.previousClusterState = previousClusterState;
            this.newClusterState = newClusterState;
            this.nonFailedTasks = nonFailedTasks;
            this.executionResults = executionResults;
        }

        void publishingFailed(FailedToCommitClusterStateException t) {
            nonFailedTasks.forEach(task -> task.listener.onFailure(task.source(), t));
        }

        /**
         * 触发 stateProcessed钩子  必须要确保发布成功
         * @param previousClusterState
         * @param newClusterState
         */
        void processedDifferentClusterState(ClusterState previousClusterState, ClusterState newClusterState) {
            nonFailedTasks.forEach(task -> task.listener.clusterStateProcessed(task.source(), previousClusterState, newClusterState));
        }

        /**
         * 每个executor还可以设置钩子
         * @param clusterChangedEvent
         */
        void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
            taskInputs.executor.clusterStatePublished(clusterChangedEvent);
        }

        /**
         * ack监听器 要求本次请求通知到所有节点上
         * @param threadPool
         * @param newClusterState  此时最新的集群状态
         * @return
         */
        Discovery.AckListener createAckListener(ThreadPool threadPool, ClusterState newClusterState) {
            // 排除掉提前失败的任务
            return new DelegatingAckListener(nonFailedTasks.stream()
                .filter(task -> task.listener instanceof AckedClusterStateTaskListener)
                // 看来要通知到所有的node 才能真正触发监听器
                .map(task -> new AckCountDownListener((AckedClusterStateTaskListener) task.listener, newClusterState.version(),
                    newClusterState.nodes(), threadPool))
                .collect(Collectors.toList()));
        }

        boolean clusterStateUnchanged() {
            return previousClusterState == newClusterState;
        }

        /**
         * 某批任务中可能有部分执行失败了
         */
        void notifyFailedTasks() {
            // fail all tasks that have failed
            for (Batcher.UpdateTask updateTask : taskInputs.updateTasks) {
                assert executionResults.containsKey(updateTask.task) : "missing " + updateTask;
                final ClusterStateTaskExecutor.TaskResult taskResult = executionResults.get(updateTask.task);
                // 任务失败 执行失败钩子
                if (taskResult.isSuccess() == false) {
                    updateTask.listener.onFailure(updateTask.source(), taskResult.getFailure());
                }
            }
        }

        /**
         * 代表处理任务前后集群状态没有发生变化
         */
        void notifySuccessfulTasksOnUnchangedClusterState() {
            nonFailedTasks.forEach(task -> {
                // 如果任务需要确认ack信息 因为本身CS没有变化 所以可以直接触发
                if (task.listener instanceof AckedClusterStateTaskListener) {
                    //no need to wait for ack if nothing changed, the update can be counted as acknowledged
                    ((AckedClusterStateTaskListener) task.listener).onAllNodesAcked(null);
                }
                task.listener.clusterStateProcessed(task.source(), newClusterState, newClusterState);
            });
        }
    }

    /**
     * Returns the tasks that are pending.
     * 获取当前所有等待中的任务
     */
    public List<PendingClusterTask> pendingTasks() {
        return Arrays.stream(threadPoolExecutor.getPending()).map(pending -> {
            assert pending.task instanceof SourcePrioritizedRunnable :
                "thread pool executor should only use SourcePrioritizedRunnable instances but found: " + pending.task.getClass().getName();
            SourcePrioritizedRunnable task = (SourcePrioritizedRunnable) pending.task;
            return new PendingClusterTask(pending.insertionOrder, pending.priority, new Text(task.source()),
                task.getAgeInMillis(), pending.executing);
        }).collect(Collectors.toList());
    }

    /**
     * Returns the number of currently pending tasks.
     */
    public int numberOfPendingTasks() {
        return threadPoolExecutor.getNumberOfPendingTasks();
    }

    /**
     * Returns the maximum wait time for tasks in the queue
     *
     * @return A zero time value if the queue is empty, otherwise the time value oldest task waiting in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        return threadPoolExecutor.getMaxTaskWaitTime();
    }

    /**
     * 确保在监听器中不会抛出异常 保护线程池线程不被打断
     * @param listener
     * @param contextSupplier
     * @return
     */
    private SafeClusterStateTaskListener safe(ClusterStateTaskListener listener, Supplier<ThreadContext.StoredContext> contextSupplier) {
        if (listener instanceof AckedClusterStateTaskListener) {
            return new SafeAckedClusterStateTaskListener((AckedClusterStateTaskListener) listener, contextSupplier, logger);
        } else {
            return new SafeClusterStateTaskListener(listener, contextSupplier, logger);
        }
    }

    /**
     * 在执行回调时 会将线程上下文切换成 context中暂存的
     */
    private static class SafeClusterStateTaskListener implements ClusterStateTaskListener {
        private final ClusterStateTaskListener listener;
        protected final Supplier<ThreadContext.StoredContext> context;
        private final Logger logger;

        SafeClusterStateTaskListener(ClusterStateTaskListener listener, Supplier<ThreadContext.StoredContext> context, Logger logger) {
            this.listener = listener;
            this.context = context;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Exception e) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onFailure(source, e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error(() -> new ParameterizedMessage(
                        "exception thrown by listener notifying of failure from [{}]", source), inner);
            }
        }

        @Override
        public void onNoLongerMaster(String source) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onNoLongerMaster(source);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage(
                        "exception thrown by listener while notifying no longer master from [{}]", source), e);
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.clusterStateProcessed(source, oldState, newState);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage(
                        "exception thrown by listener while notifying of cluster state processed from [{}], old cluster state:\n" +
                            "{}\nnew cluster state:\n{}", source, oldState, newState), e);
            }
        }
    }

    private static class SafeAckedClusterStateTaskListener extends SafeClusterStateTaskListener implements AckedClusterStateTaskListener {
        private final AckedClusterStateTaskListener listener;
        private final Logger logger;

        SafeAckedClusterStateTaskListener(AckedClusterStateTaskListener listener, Supplier<ThreadContext.StoredContext> context,
                                          Logger logger) {
            super(listener, context, logger);
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return listener.mustAck(discoveryNode);
        }

        @Override
        public void onAllNodesAcked(@Nullable Exception e) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onAllNodesAcked(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error("exception thrown by listener while notifying on all nodes acked", inner);
            }
        }

        @Override
        public void onAckTimeout() {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onAckTimeout();
            } catch (Exception e) {
                logger.error("exception thrown by listener while notifying on ack timeout", e);
            }
        }

        @Override
        public TimeValue ackTimeout() {
            return listener.ackTimeout();
        }
    }

    private void logExecutionTime(TimeValue executionTime, String activity, String summary) {
        if (executionTime.getMillis() > slowTaskLoggingThreshold.getMillis()) {
            logger.warn("took [{}], which is over [{}], to {} for [{}]", executionTime, slowTaskLoggingThreshold, activity, summary);
        } else {
            logger.debug("took [{}] to {} for [{}]", executionTime, activity, summary);
        }
    }

    /**
     * 统一处理一组监听器
     */
    private static class DelegatingAckListener implements Discovery.AckListener {

        private final List<Discovery.AckListener> listeners;

        private DelegatingAckListener(List<Discovery.AckListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void onCommit(TimeValue commitTime) {
            for (Discovery.AckListener listener : listeners) {
                listener.onCommit(commitTime);
            }
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            for (Discovery.AckListener listener : listeners) {
                listener.onNodeAck(node, e);
            }
        }
    }


    /**
     * 必须通知到 集群中所有 mustAck返回true的节点 才可以正常触发监听器
     */
    private static class AckCountDownListener implements Discovery.AckListener {

        private static final Logger logger = LogManager.getLogger(AckCountDownListener.class);

        /**
         * 在ClusterStateTaskListener上增加了几个钩子
         */
        private final AckedClusterStateTaskListener ackedTaskListener;

        /**
         * 当通知到多少个节点后 可以正常触发监听器
         */
        private final CountDown countDown;
        private final DiscoveryNode masterNode;
        private final ThreadPool threadPool;
        private final long clusterStateVersion;
        private volatile Scheduler.Cancellable ackTimeoutCallback;
        private Exception lastFailure;

        /**
         *
         * @param ackedTaskListener
         * @param clusterStateVersion
         * @param nodes     当前clusterState下所有node
         * @param threadPool
         */
        AckCountDownListener(AckedClusterStateTaskListener ackedTaskListener, long clusterStateVersion, DiscoveryNodes nodes,
                             ThreadPool threadPool) {
            this.ackedTaskListener = ackedTaskListener;
            this.clusterStateVersion = clusterStateVersion;
            this.threadPool = threadPool;
            this.masterNode = nodes.getMasterNode();
            int countDown = 0;
            // 检查集群内所有节点 只有满足条件的才需要确认ack  默认情况下mustAck为true 也就是要求集群内所有节点都收到
            for (DiscoveryNode node : nodes) {
                //we always wait for at least the master node
                // leader节点 和 mustAck == true的节点必须确认
                if (node.equals(masterNode) || ackedTaskListener.mustAck(node)) {
                    countDown++;
                }
            }
            logger.trace("expecting {} acknowledgements for cluster_state update (version: {})", countDown, clusterStateVersion);
            this.countDown = new CountDown(countDown + 1); // we also wait for onCommit to be called
        }


        /**
         * 代表此时 pub达到1/2以上的masterNode
         * @param commitTime the time it took to commit the cluster state
         */
        @Override
        public void onCommit(TimeValue commitTime) {
            // 获取pub超时时间
            TimeValue ackTimeout = ackedTaskListener.ackTimeout();
            if (ackTimeout == null) {
                ackTimeout = TimeValue.ZERO;
            }
            final TimeValue timeLeft = TimeValue.timeValueNanos(Math.max(0, ackTimeout.nanos() - commitTime.nanos()));
            // 发生超时
            if (timeLeft.nanos() == 0L) {
                onTimeout();
            // 如果所有node都发布成功 触发该方法
            } else if (countDown.countDown()) {
                finish();
            } else {
                this.ackTimeoutCallback = threadPool.schedule(this::onTimeout, timeLeft, ThreadPool.Names.GENERIC);
                // re-check if onNodeAck has not completed while we were scheduling the timeout
                if (countDown.isCountedDown()) {
                    ackTimeoutCallback.cancel();
                }
            }
        }

        /**
         * 每当某个节点进行确认时 减少一次计数值
         * @param node the node
         * @param e the optional exception
         */
        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            // 不满足ack条件 忽略
            if (node.equals(masterNode) == false && ackedTaskListener.mustAck(node) == false) {
                return;
            }
            if (e == null) {
                logger.trace("ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion);
            } else {
                this.lastFailure = e;
                logger.debug(() -> new ParameterizedMessage(
                        "ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion), e);
            }

            // 减少计数值
            if (countDown.countDown()) {
                finish();
            }
        }

        /**
         * 当所有node 都提交成功时触发该方法
         */
        private void finish() {
            logger.trace("all expected nodes acknowledged cluster_state update (version: {})", clusterStateVersion);
            if (ackTimeoutCallback != null) {
                ackTimeoutCallback.cancel();
            }
            ackedTaskListener.onAllNodesAcked(lastFailure);
        }

        public void onTimeout() {
            if (countDown.fastForward()) {
                logger.trace("timeout waiting for acknowledgement for cluster_state update (version: {})", clusterStateVersion);
                ackedTaskListener.onAckTimeout();
            }
        }
    }

    /**
     * 处理批任务
     * @param taskInputs
     * @param previousClusterState 执行更新任务前的集群状态
     * @return
     */
    private ClusterTasksResult<Object> executeTasks(TaskInputs taskInputs, ClusterState previousClusterState) {
        ClusterTasksResult<Object> clusterTasksResult;
        try {
            // 取出一组task
            List<Object> inputs = taskInputs.updateTasks.stream().map(tUpdateTask -> tUpdateTask.task).collect(Collectors.toList());

            // 每个批任务会绑定一个执行器对象 该对象内部已经定义了任务的处理逻辑
            clusterTasksResult = taskInputs.executor.execute(previousClusterState, inputs);

            // 集群的状态变化不允许将当前节点从leader修改为普通节点
            if (previousClusterState != clusterTasksResult.resultingState &&
                previousClusterState.nodes().isLocalNodeElectedMaster() &&
                (clusterTasksResult.resultingState.nodes().isLocalNodeElectedMaster() == false)) {
                throw new AssertionError("update task submitted to MasterService cannot remove master");
            }
        } catch (Exception e) {
            logger.trace(() -> new ParameterizedMessage(
                    "failed to execute cluster state update (on version: [{}], uuid: [{}]) for [{}]\n{}{}{}",
                    previousClusterState.version(),
                    previousClusterState.stateUUID(),
                    taskInputs.summary,
                    previousClusterState.nodes(),
                    previousClusterState.routingTable(),
                    previousClusterState.getRoutingNodes()), // may be expensive => construct message lazily
                e);
            clusterTasksResult = ClusterTasksResult.builder()
                .failures(taskInputs.updateTasks.stream().map(updateTask -> updateTask.task)::iterator, e)
                .build(previousClusterState);
        }

        assert clusterTasksResult.executionResults != null;
        assert clusterTasksResult.executionResults.size() == taskInputs.updateTasks.size()
            : String.format(Locale.ROOT, "expected [%d] task result%s but was [%d]", taskInputs.updateTasks.size(),
            taskInputs.updateTasks.size() == 1 ? "" : "s", clusterTasksResult.executionResults.size());
        if (Assertions.ENABLED) {
            ClusterTasksResult<Object> finalClusterTasksResult = clusterTasksResult;
            taskInputs.updateTasks.forEach(updateTask -> {
                assert finalClusterTasksResult.executionResults.containsKey(updateTask.task) :
                    "missing task result for " + updateTask;
            });
        }

        return clusterTasksResult;
    }

    /**
     * 从批任务中 只找到成功执行的
     * @param taskInputs
     * @param clusterTasksResult
     * @return
     */
    private List<Batcher.UpdateTask> getNonFailedTasks(TaskInputs taskInputs,
                                                      ClusterTasksResult<Object> clusterTasksResult) {
        return taskInputs.updateTasks.stream().filter(updateTask -> {
            assert clusterTasksResult.executionResults.containsKey(updateTask.task) : "missing " + updateTask;
            final ClusterStateTaskExecutor.TaskResult taskResult =
                clusterTasksResult.executionResults.get(updateTask.task);
            return taskResult.isSuccess();
        }).collect(Collectors.toList());
    }

    /**
     * Represents a set of tasks to be processed together with their executor
     * 将一组要执行的任务包装成一个对象
     */
    private class TaskInputs {
        final String summary;
        final List<Batcher.UpdateTask> updateTasks;
        final ClusterStateTaskExecutor<Object> executor;

        TaskInputs(ClusterStateTaskExecutor<Object> executor, List<Batcher.UpdateTask> updateTasks, String summary) {
            this.summary = summary;
            this.executor = executor;
            this.updateTasks = updateTasks;
        }

        boolean runOnlyWhenMaster() {
            return executor.runOnlyOnMaster();
        }

        /**
         * 此时本节点不是leader节点 触发钩子
         */
        void onNoLongerMaster() {
            updateTasks.forEach(task -> task.listener.onNoLongerMaster(task.source()));
        }
    }

    /**
     * Submits a batch of cluster state update tasks; submitted updates are guaranteed to be processed together,
     * potentially with more tasks of the same executor.
     *
     * @param source   the source of the cluster state update task
     * @param tasks    a map of update tasks and their corresponding listeners
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param <T>      the type of the cluster state update task state
     *                 处理一组更新任务
     */
    public <T> void submitStateUpdateTasks(final String source,
                                           final Map<T, ClusterStateTaskListener> tasks,  // key 代表任务本身
                                           final ClusterStateTaskConfig config,
                                           final ClusterStateTaskExecutor<T> executor) {
        if (!lifecycle.started()) {
            return;
        }
        final ThreadContext threadContext = threadPool.getThreadContext();
        // 将当前线程上下文暂存到这个函数中
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(true);
        // threadContext.stashContext 开启一个新的上下文
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.markAsSystemContext();

            List<Batcher.UpdateTask> safeTasks = tasks.entrySet().stream()
                // 传入supplier是为了用户可以根据自己的需要随时恢复线程上下文
                .map(e -> taskBatcher.new UpdateTask(config.priority(), source, e.getKey(), safe(e.getValue(), supplier), executor))
                .collect(Collectors.toList());
            taskBatcher.submitTasks(safeTasks, config.timeout());
        } catch (EsRejectedExecutionException e) {
            // ignore cases where we are shutting down..., there is really nothing interesting
            // to be done here...
            if (!lifecycle.stoppedOrClosed()) {
                throw e;
            }
        }
    }

}
