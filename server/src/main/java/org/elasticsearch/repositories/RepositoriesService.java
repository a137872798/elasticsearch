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

package org.elasticsearch.repositories;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service responsible for maintaining and providing access to snapshot repositories on nodes.
 * 该对象负责存储相关的功能  同时可以监听集群的变化事件
 */
public class RepositoriesService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(RepositoriesService.class);

    /**
     * 可以key 可以找到不同的存储工厂
     */
    private final Map<String, Repository.Factory> typesRegistry;
    /**
     * 某些插件中可能会有内置的工厂  在默认的基于FS的实现 没有填充该工厂
     */
    private final Map<String, Repository.Factory> internalTypesRegistry;

    /**
     * 集群服务  本对象会监听集群状态的变化
     */
    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    /**
     * 该对象负责进行认证相关的工作
     */
    private final VerifyNodeRepositoryAction verifyAction;

    // 通过工厂生成的对象会保存到这个容器中
    private final Map<String, Repository> internalRepositories = ConcurrentCollections.newConcurrentMap();
    private volatile Map<String, Repository> repositories = Collections.emptyMap();

    /**
     *
     * @param settings
     * @param clusterService
     * @param transportService
     * @param typesRegistry    生成每个存储对象的工厂     默认实现中只要关注基于Fs的工厂就可以 并且基于文件的实现不需要 internalTypeRegistry
     * @param internalTypesRegistry   每个存储插件还可能包含了内部工厂
     * @param threadPool
     */
    public RepositoriesService(Settings settings, ClusterService clusterService, TransportService transportService,
                               Map<String, Repository.Factory> typesRegistry, Map<String, Repository.Factory> internalTypesRegistry,
                               ThreadPool threadPool) {
        this.typesRegistry = typesRegistry;
        this.internalTypesRegistry = internalTypesRegistry;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // Doesn't make sense to maintain repositories on non-master and non-data nodes
        // Nothing happens there anyway
        // 如果当前节点是数据节点 或者是master节点(参与选举的节点)  那么该对象将会监听集群的变化
        if (DiscoveryNode.isDataNode(settings) || DiscoveryNode.isMasterNode(settings)) {
            clusterService.addStateApplier(this);
        }
        // 初始化认证器对象
        this.verifyAction = new VerifyNodeRepositoryAction(transportService, clusterService, this);
    }

    /**
     * Registers new repository in the cluster
     * <p>
     * This method can be only called on the master node. It tries to create a new repository on the master
     * and if it was successful it adds new repository to cluster metadata.
     *
     * @param request  register repository request
     * @param listener register repository listener    该监听器包含了处理本次通知的结果
     *
     *                 注册一个新的存储层实现  该方法只有 master节点可以调用 当创建成功时该存储实例会暴露到集群中
     */
    public void registerRepository(final PutRepositoryRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        assert lifecycle.started() : "Trying to register new repository but service is in state [" + lifecycle.state() + "]";

        // 将相关信息包装成 metadata 并设置到clusterState中
        final RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(request.name(), request.type(), request.settings());
        validate(request.name());

        // 当新的集群状态发布到其他节点后
        final ActionListener<ClusterStateUpdateResponse> registrationListener;
        if (request.verify()) {
            // 当失败时 直接触发第一个监听器 成功时触发第二个参数 第二个参数内部包含了第一个参数
            registrationListener = ActionListener.delegateFailure(listener, (delegatedListener, clusterStateUpdateResponse) -> {
                // 代表本次通知到了所有的节点
                if (clusterStateUpdateResponse.isAcknowledged()) {
                    // The response was acknowledged - all nodes should know about the new repository, let's verify them
                    // 这里执行一个认证工作 当认证完成时触发监听器
                    verifyRepository(request.name(), ActionListener.delegateFailure(delegatedListener,
                        // 当成功时 触发以下方法  也就是将处理逻辑桥接到 listener上
                        (innerDelegatedListener, discoveryNodes) -> innerDelegatedListener.onResponse(clusterStateUpdateResponse)));
                } else {
                    // 当 ack = false时 直接触发监听器
                    delegatedListener.onResponse(clusterStateUpdateResponse);
                }
            });
        } else {
            registrationListener = listener;
        }

        // Trying to create the new repository on master to make sure it works
        try {
            // 创建失败提前结束处理   这里只是确认是否可以工作 所以在使用完后立即就close了
            closeRepository(createRepository(newRepositoryMetadata, typesRegistry));
        } catch (Exception e) {
            registrationListener.onFailure(e);
            return;
        }

        // submitStateUpdateTask 该方法会将集群状态改变后 并发布到集群中  只有当成功写入到超过半数的 候选节点后才算发布成功
        clusterService.submitStateUpdateTask("put_repository [" + request.name() + "]",
            // 这里的ack监听器 可以先放一下
            new AckedClusterStateUpdateTask<>(request, registrationListener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    // 返回一个代表通知结果的 res   内部的ack为true 代表通知到所有节点 如果出现了异常情况 内部ack=false
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                /**
                 * 更新集群状态
                 * @param currentState
                 * @return
                 */
                @Override
                public ClusterState execute(ClusterState currentState) {
                    // 确保该存储实例此时还没有进程在使用 TODO 为什么需要这层保证 ???
                    ensureRepositoryNotInUse(currentState, request.name());
                    // 该对象内部维护了各种元数据  自定义的元数据将会存储在 .custom() 中
                    Metadata metadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                    // 获取存储层元数据
                    RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
                    if (repositories == null) {
                        logger.info("put repository [{}]", request.name());
                        // 将本次申请的存储层实例封装成一个 metadata 并追加到 RepositoriesMetadata 中
                        repositories = new RepositoriesMetadata(
                            Collections.singletonList(new RepositoryMetadata(request.name(), request.type(), request.settings())));
                    } else {
                        boolean found = false;
                        // 额外预留一个位置  用于存储本次申请的存储层
                        List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size() + 1);

                        for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                            // 代表本次申请的存储对象 之前已经发布到clusterState中了
                            if (repositoryMetadata.name().equals(newRepositoryMetadata.name())) {
                                // 当本次申请的settings 都完全一致时 返回原 state 此时不会触发 publish
                                if (newRepositoryMetadata.equalsIgnoreGenerations(repositoryMetadata)) {
                                    // Previous version is the same as this one no update is needed.
                                    return currentState;
                                }
                                // 使用新的metadata 覆盖之前的数据
                                found = true;
                                repositoriesMetadata.add(newRepositoryMetadata);
                            } else {
                                repositoriesMetadata.add(repositoryMetadata);
                            }
                        }
                        // 本次存储实例是首次插入
                        if (!found) {
                            logger.info("put repository [{}]", request.name());
                            repositoriesMetadata.add(new RepositoryMetadata(request.name(), request.type(), request.settings()));
                        } else {
                            // 仅打印日志 提示被更新过
                            logger.info("update repository [{}]", request.name());
                        }
                        repositories = new RepositoriesMetadata(repositoriesMetadata);
                    }
                    mdBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
                    // 返回此时最新的clusterState 之后会触发 publish 发布到所有节点上
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(() -> new ParameterizedMessage("failed to create repository [{}]", request.name()), e);
                    super.onFailure(source, e);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    // repository is created on both master and data nodes
                    return discoveryNode.isMasterNode() || discoveryNode.isDataNode();
                }
            });
    }
    /**
     * Unregisters repository in the cluster
     * <p>
     * This method can be only called on the master node. It removes repository information from cluster metadata.
     *
     * @param request  unregister repository request
     * @param listener unregister repository listener
     *                 注销某个存储实例
     */
    public void unregisterRepository(final DeleteRepositoryRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("delete_repository [" + request.name() + "]",
            new AckedClusterStateUpdateTask<>(request, listener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                /**
                 * 这里流程跟注册时大体类似 区别就是省去了认证操作
                 * @param currentState
                 * @return
                 */
                @Override
                public ClusterState execute(ClusterState currentState) {
                    ensureRepositoryNotInUse(currentState, request.name());
                    Metadata metadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                    RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
                    if (repositories != null && repositories.repositories().size() > 0) {
                        List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size());
                        boolean changed = false;
                        for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                            // 命中的存储实例 不会加入到新容器中
                            if (Regex.simpleMatch(request.name(), repositoryMetadata.name())) {
                                logger.info("delete repository [{}]", repositoryMetadata.name());
                                changed = true;
                            } else {
                                repositoriesMetadata.add(repositoryMetadata);
                            }
                        }
                        if (changed) {
                            repositories = new RepositoriesMetadata(repositoriesMetadata);
                            mdBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
                            return ClusterState.builder(currentState).metadata(mdBuilder).build();
                        }
                    }
                    // 如果传入的是* 不进行处理
                    if (Regex.isMatchAllPattern(request.name())) { // we use a wildcard so we don't barf if it's not present.
                        return currentState;
                    }
                    // 如果向注销的存储并没有存在于  clusterState中 抛出异常
                    throw new RepositoryMissingException(request.name());
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    // repository was created on both master and data nodes
                    return discoveryNode.isMasterNode() || discoveryNode.isDataNode();
                }
            });
    }

    /**
     * 当putRepository 对应的res.ack 为true时 触发该方法
     * @param repositoryName
     * @param listener
     */
    public void verifyRepository(final String repositoryName, final ActionListener<List<DiscoveryNode>> listener) {
        // 找到此时存储在容器中的实例对象  也就是实例要提前设置到容器中么???   注意该对象api的使用顺序
        final Repository repository = repository(repositoryName);
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                // 默认情况下认证就是使用存储实例 存储一个基础数据 只要没有抛出异常就是成功处理了
                final String verificationToken = repository.startVerification();
                if (verificationToken != null) {
                    try {
                        // TODO 忽略认证
                        verifyAction.verify(repositoryName, verificationToken, ActionListener.delegateFailure(listener,
                            (delegatedListener, verifyResponse) -> threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                                try {
                                    // 执行成功的时候 清理之前为认证做的准备工作
                                    repository.endVerification(verificationToken);
                                } catch (Exception e) {
                                    logger.warn(() -> new ParameterizedMessage(
                                        "[{}] failed to finish repository verification", repositoryName), e);
                                    delegatedListener.onFailure(e);
                                    return;
                                }
                                delegatedListener.onResponse(verifyResponse);
                            })));
                    } catch (Exception e) {
                        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                            try {
                                repository.endVerification(verificationToken);
                            } catch (Exception inner) {
                                inner.addSuppressed(e);
                                logger.warn(() -> new ParameterizedMessage(
                                    "[{}] failed to finish repository verification", repositoryName), inner);
                            }
                            listener.onFailure(e);
                        });
                    }
                } else {
                    listener.onResponse(Collections.emptyList());
                }
            }
        });
    }


    /**
     * Checks if new repositories appeared in or disappeared from cluster metadata and updates current list of
     * repositories accordingly.
     *
     * @param event cluster changed event
     *              当集群状态发生变化时触发   实际上registerRepository/unRegisterRepository 就是依靠该方法起作用
     */
    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            final ClusterState state = event.state();
            RepositoriesMetadata oldMetadata = event.previousState().getMetadata().custom(RepositoriesMetadata.TYPE);
            RepositoriesMetadata newMetadata = state.getMetadata().custom(RepositoriesMetadata.TYPE);

            // Check if repositories got changed
            // 代表集群状态中有关存储层元数据的部分发生了变化  各个存储实例根据自己的需要进行更新
            if ((oldMetadata == null && newMetadata == null) || (oldMetadata != null && oldMetadata.equalsIgnoreGenerations(newMetadata))) {
                for (Repository repo : repositories.values()) {
                    repo.updateState(state);
                }
                return;
            }

            logger.trace("processing new index repositories for state version [{}]", event.state().version());

            Map<String, Repository> survivors = new HashMap<>();
            // First, remove repositories that are no longer there
            for (Map.Entry<String, Repository> entry : repositories.entrySet()) {
                // 元数据中已经找不到某个存储服务了 关闭他们
                if (newMetadata == null || newMetadata.repository(entry.getKey()) == null) {
                    logger.debug("unregistering repository [{}]", entry.getKey());
                    closeRepository(entry.getValue());
                } else {
                    survivors.put(entry.getKey(), entry.getValue());
                }
            }

            // 从元数据中找到新增的 存储实例
            Map<String, Repository> builder = new HashMap<>();
            if (newMetadata != null) {
                // Now go through all repositories and update existing or create missing
                for (RepositoryMetadata repositoryMetadata : newMetadata.repositories()) {
                    Repository repository = survivors.get(repositoryMetadata.name());
                    if (repository != null) {
                        // Found previous version of this repository
                        // 检测元数据是否发生了变化
                        RepositoryMetadata previousMetadata = repository.getMetadata();
                        if (previousMetadata.type().equals(repositoryMetadata.type()) == false
                            || previousMetadata.settings().equals(repositoryMetadata.settings()) == false) {
                            // Previous version is different from the version in settings
                            logger.debug("updating repository [{}]", repositoryMetadata.name());
                            closeRepository(repository);
                            repository = null;
                            try {
                                // 基于新配置创建一个新对象
                                repository = createRepository(repositoryMetadata, typesRegistry);
                            } catch (RepositoryException ex) {
                                // TODO: this catch is bogus, it means the old repo is already closed,
                                // but we have nothing to replace it
                                logger.warn(() -> new ParameterizedMessage("failed to change repository [{}]",
                                    repositoryMetadata.name()), ex);
                            }
                        }
                    } else {
                        try {
                            // 生成新对象
                            repository = createRepository(repositoryMetadata, typesRegistry);
                        } catch (RepositoryException ex) {
                            logger.warn(() -> new ParameterizedMessage("failed to create repository [{}]", repositoryMetadata.name()), ex);
                        }
                    }
                    if (repository != null) {
                        logger.debug("registering repository [{}]", repositoryMetadata.name());
                        // 如果更新了存储实例 那么在这里就会进行覆盖
                        builder.put(repositoryMetadata.name(), repository);
                    }
                }
            }

            // 每个存储实例根据自身需要接收最新的clusterState数据 并更新内部属性
            for (Repository repo : builder.values()) {
                repo.updateState(state);
            }
            // 这里做了赋值操作
            repositories = Collections.unmodifiableMap(builder);
        } catch (Exception ex) {
            logger.warn("failure updating cluster state ", ex);
        }
    }

    /**
     * Gets the {@link RepositoryData} for the given repository.
     *
     * @param repositoryName repository name
     * @param listener       listener to pass {@link RepositoryData} to
     *                       从指定的存储实例中获取 repositoryData 并触发监听器
     */
    public void getRepositoryData(final String repositoryName, final ActionListener<RepositoryData> listener) {
        try {
            Repository repository = repository(repositoryName);
            assert repository != null; // should only be called once we've validated the repository exists
            repository.getRepositoryData(listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Returns registered repository
     * <p>
     * This method is called only on the master node
     *
     * @param repositoryName repository name
     * @return registered repository
     * @throws RepositoryMissingException if repository with such name isn't registered
     */
    public Repository repository(String repositoryName) {
        Repository repository = repositories.get(repositoryName);
        if (repository != null) {
            return repository;
        }
        repository = internalRepositories.get(repositoryName);
        if (repository != null) {
            return repository;
        }
        throw new RepositoryMissingException(repositoryName);
    }

    /**
     * 注册一个internalRepositories 对象  利用internalRepository工厂
     * @param name
     * @param type
     */
    public void registerInternalRepository(String name, String type) {
        RepositoryMetadata metadata = new RepositoryMetadata(name, type, Settings.EMPTY);
        Repository repository = internalRepositories.computeIfAbsent(name, (n) -> {
            logger.debug("put internal repository [{}][{}]", name, type);
            return createRepository(metadata, internalTypesRegistry);
        });
        if (type.equals(repository.getMetadata().type()) == false) {
            logger.warn(new ParameterizedMessage("internal repository [{}][{}] already registered. this prevented the registration of " +
                "internal repository [{}][{}].", name, repository.getMetadata().type(), name, type));
        } else if (repositories.containsKey(name)) {
            logger.warn(new ParameterizedMessage("non-internal repository [{}] already registered. this repository will block the " +
                "usage of internal repository [{}][{}].", name, metadata.type(), name));
        }
    }

    public void unregisterInternalRepository(String name) {
        Repository repository = internalRepositories.remove(name);
        if (repository != null) {
            RepositoryMetadata metadata = repository.getMetadata();
            logger.debug(() -> new ParameterizedMessage("delete internal repository [{}][{}].", metadata.type(), name));
            closeRepository(repository);
        }
    }

    /** Closes the given repository. */
    private void closeRepository(Repository repository) {
        logger.debug("closing repository [{}][{}]", repository.getMetadata().type(), repository.getMetadata().name());
        repository.close();
    }

    /**
     * Creates repository holder. This method starts the repository
     * 通过描述的待初始化的存储实例 以及之前在初始化阶段加载的插件中包含的工厂 创建存储实例
     */
    private Repository createRepository(RepositoryMetadata repositoryMetadata, Map<String, Repository.Factory> factories) {
        logger.debug("creating repository [{}][{}]", repositoryMetadata.type(), repositoryMetadata.name());
        Repository.Factory factory = factories.get(repositoryMetadata.type());

        // 代表对应的实现 在node初始化阶段并没有通过插件系统添加到容器中
        if (factory == null) {
            throw new RepositoryException(repositoryMetadata.name(),
                "repository type [" + repositoryMetadata.type() + "] does not exist");
        }
        Repository repository = null;
        try {
            repository = factory.create(repositoryMetadata, factories::get);
            // 目前只关注基于 FS的实现
            repository.start();
            return repository;
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(repository);
            logger.warn(new ParameterizedMessage("failed to create repository [{}][{}]",
                repositoryMetadata.type(), repositoryMetadata.name()), e);
            throw new RepositoryException(repositoryMetadata.name(), "failed to create repository", e);
        }
    }

    /**
     * 校验存储类名称是否合法
     * @param repositoryName
     */
    private static void validate(final String repositoryName) {
        if (Strings.hasLength(repositoryName) == false) {
            throw new RepositoryException(repositoryName, "cannot be empty");
        }
        if (repositoryName.contains("#")) {
            throw new RepositoryException(repositoryName, "must not contain '#'");
        }
        if (Strings.validFileName(repositoryName) == false) {
            throw new RepositoryException(repositoryName,
                "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
    }

    private static void ensureRepositoryNotInUse(ClusterState clusterState, String repository) {
        if (isRepositoryInUse(clusterState, repository)) {
            throw new IllegalStateException("trying to modify or unregister repository that is currently used ");
        }
    }

    /**
     * Checks if a repository is currently in use by one of the snapshots
     *
     * @param clusterState cluster state
     * @param repository   repository id
     * @return true if repository is currently in use by one of the running snapshots
     * 检测某个存储实例此时是否已经被快照使用了
     */
    private static boolean isRepositoryInUse(ClusterState clusterState, String repository) {
        // 快照进程维护当前节点下所有快照行为  每个快照行为的状态可能不同 以及他们使用的存储类也可能不同
        SnapshotsInProgress snapshots = clusterState.custom(SnapshotsInProgress.TYPE);
        if (snapshots != null) {
            for (SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                if (repository.equals(snapshot.snapshot().getRepository())) {
                    return true;
                }
            }
        }

        // 遍历所有正在运行中的删除进程
        SnapshotDeletionsInProgress deletionsInProgress = clusterState.custom(SnapshotDeletionsInProgress.TYPE);
        if (deletionsInProgress != null) {
            for (SnapshotDeletionsInProgress.Entry entry : deletionsInProgress.getEntries()) {
                if (entry.repository().equals(repository)) {
                    return true;
                }
            }
        }

        // 针对每个正在运行的RepositoryCleanup 任务 也会存储到一个 inProgress中
        final RepositoryCleanupInProgress repositoryCleanupInProgress = clusterState.custom(RepositoryCleanupInProgress.TYPE);
        if (repositoryCleanupInProgress != null) {
            for (RepositoryCleanupInProgress.Entry entry : repositoryCleanupInProgress.entries()) {
                if (entry.repository().equals(repository)) {
                    return true;
                }
            }
        }

        // 恢复也会涉及到一组存储实例
        RestoreInProgress restoreInProgress = clusterState.custom(RestoreInProgress.TYPE);
        if (restoreInProgress != null) {
            for (RestoreInProgress.Entry entry: restoreInProgress) {
                if (repository.equals(entry.snapshot().getRepository())) {
                    return true;
                }
            }
        }
        return false;
    }


    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {
        clusterService.removeApplier(this);
        final Collection<Repository> repos = new ArrayList<>();
        repos.addAll(internalRepositories.values());
        repos.addAll(repositories.values());
        IOUtils.close(repos);
    }
}
