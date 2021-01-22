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
 * 一个RepositoriesService 中包含了多个 Repository 实例   默认情况下ES 内置了基于FS系统的存储实例
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
     * 存储层服务本身同时维护多个 repository实现  基于不同的工厂
     * @param settings
     * @param clusterService
     * @param transportService
     * @param typesRegistry
     * @param internalTypesRegistry    在Node初始化时 会加载存储插件 插件内部对应着Repository.Factory ES会内置一个基于FS的存储仓库  在初始化时就会携带进来了
     *                                 虽然ES本身对外开放了增加存储层实例的接口 但是一般用不上
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
        // 某些操作需要先进行认证 这里注册处理认证请求的handler
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
     *                 用户可以向集群提交一个插入仓库的请求 这里会生成仓库元数据对象，并设置到clusterState中 之后发布到集群中
     *                 ES在选举过程中 可能会产生2个leader 但是在更新leader的clusterState发布到集群中时 要求必须所有节点都认同 否则会重新发起选举
     *                 也就是首次发布最终会确保集群中只存在一个leader 所有节点都认同它  那么之后关于其他操作导致的clusterState的更新就不会丢失或者覆盖了 只是还是存在失败的情况 比如脑裂
     */
    public void registerRepository(final PutRepositoryRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        assert lifecycle.started() : "Trying to register new repository but service is in state [" + lifecycle.state() + "]";

        // 将相关信息包装成 metadata 并设置到clusterState中
        final RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(request.name(), request.type(), request.settings());
        validate(request.name());

        // 当新的集群状态发布到其他节点后
        final ActionListener<ClusterStateUpdateResponse> registrationListener;

        // 这里只是在包装监听器
        // 代表需要检测请求
        if (request.verify()) {
            registrationListener = ActionListener.delegateFailure(listener, (delegatedListener, clusterStateUpdateResponse) -> {
                // 代表本次通知到了所有的节点
                if (clusterStateUpdateResponse.isAcknowledged()) {
                    // The response was acknowledged - all nodes should know about the new repository, let's verify them
                    // 当创建仓库成功时 需要验证能否正常工作  当验证失败时会通知用户
                    verifyRepository(request.name(), ActionListener.delegateFailure(delegatedListener,
                        // 当成功时 触发以下方法  也就是将处理逻辑桥接到 listener上
                        (innerDelegatedListener, discoveryNodes) -> innerDelegatedListener.onResponse(clusterStateUpdateResponse)));
                } else {
                    delegatedListener.onResponse(clusterStateUpdateResponse);
                }
            });
        } else {
            registrationListener = listener;
        }

        // Trying to create the new repository on master to make sure it works
        try {
            // 检测仓库能否正常创建  无法创建直接返回失败信息
            closeRepository(createRepository(newRepositoryMetadata, typesRegistry));
        } catch (Exception e) {
            registrationListener.onFailure(e);
            return;
        }

        // 到了这里才开始处理  也就是将添加了 repositoryMetadata 的 ClusterState 发布到集群中
        clusterService.submitStateUpdateTask("put_repository [" + request.name() + "]",
            // 如果是基于 clusterStateProcessed的监听器 实际上clusterState更新成功后还是有被覆盖的可能 所以需要配合类似eureka的续约机制
            // 至少每次都要将 核心数据与集群新下发的 clusterState做比较 并为核心数据进行续约
            // 而基于ACK的监听器 可以要求pub必须在所有节点上执行成功 这样就确保存储层数据不会丢失
            new AckedClusterStateUpdateTask<>(request, registrationListener) {

                /**
                 * @param acknowledged 代表在某个节点上执行失败了
                 * @return
                 */
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
                    // 当前仓库对象正在使用中  不宜进行注册  这会引发更新操作
                    ensureRepositoryNotInUse(currentState, request.name());
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
                                // 直接添加新的元数据 而不是存储旧的元数据
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

                /**
                 * 当发布失败时 会在外层进行重试 因为一般抛出的都是这个异常 FailedToCommitClusterStateException
                 * @param source
                 * @param e
                 */
                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(() -> new ParameterizedMessage("failed to create repository [{}]", request.name()), e);
                    super.onFailure(source, e);
                }

                /**
                 * 只需要确认 masterNode/dataNode
                 * @param discoveryNode a node
                 * @return
                 */
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
     *                 将某个repository 从clusterState中移除
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

                        // 代表需要保留的repository
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
                    // 代表没有找到准备删除的 repository  如果本次没有明确指定 那么不做处理
                    if (Regex.isMatchAllPattern(request.name())) { // we use a wildcard so we don't barf if it's not present.
                        return currentState;
                    }
                    // 代表指定的repository没有存在于clusterState中 抛出异常
                    throw new RepositoryMissingException(request.name());
                }

                /**
                 * 只有 data/master 节点需要响应结果
                 * @param discoveryNode a node
                 * @return
                 */
                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    // repository was created on both master and data nodes
                    return discoveryNode.isMasterNode() || discoveryNode.isDataNode();
                }
            });
    }

    /**
     * 校验仓库能否正常运行   必须要求本次创建仓库的请求在所有节点上都成功后才触发
     * @param repositoryName
     * @param listener
     */
    public void verifyRepository(final String repositoryName, final ActionListener<List<DiscoveryNode>> listener) {
        // 在注册仓库实例的过程中 本节点会先触发  applyClusterState 会生成存储层实例
        final Repository repository = repository(repositoryName);

        // 当获取到存储层实例后 开始校验
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                // 默认情况下认证就是使用存储实例 存储一个基础数据 只要没有抛出异常就是成功处理了
                final String verificationToken = repository.startVerification();
                if (verificationToken != null) {
                    try {
                        // 在集群范围内 统一向所有节点发送认证数据
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
     *              在执行快照任务时  当整个快照流程结束后 会更新repositoryData.gen 并发布到集群中  该对象会监听仓库数据的变化
     */
    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {

            // 检测有关存储层的元数据信息前后是否发生了变化
            final ClusterState state = event.state();
            RepositoriesMetadata oldMetadata = event.previousState().getMetadata().custom(RepositoriesMetadata.TYPE);
            RepositoriesMetadata newMetadata = state.getMetadata().custom(RepositoriesMetadata.TYPE);

            // Check if repositories got changed
            // 代表元数据前后没有发生变化  只需要处理state的变化就可以  这里主要是更新repository观察到的最大的 gen
            if ((oldMetadata == null && newMetadata == null) || (oldMetadata != null && oldMetadata.equalsIgnoreGenerations(newMetadata))) {
                for (Repository repo : repositories.values()) {
                    repo.updateState(state);
                }
                return;
            }

            logger.trace("processing new index repositories for state version [{}]", event.state().version());

            // 代表仓库元数据本身发生了变化 需要更新仓库 以及更新gen
            // 该容器保留未被删除的实例
            Map<String, Repository> survivors = new HashMap<>();
            // First, remove repositories that are no longer there
            for (Map.Entry<String, Repository> entry : repositories.entrySet()) {
                // 元数据中已经找不到某个存储服务了 关闭他们
                if (newMetadata == null || newMetadata.repository(entry.getKey()) == null) {
                    logger.debug("unregistering repository [{}]", entry.getKey());
                    // 针对ES内置的 默认实现   close是 noop
                    closeRepository(entry.getValue());
                } else {
                    survivors.put(entry.getKey(), entry.getValue());
                }
            }

            // 用户传入新的存储层元数据 配合该对象初始化时存储的 Factory 创建/更新存储层实例
            Map<String, Repository> builder = new HashMap<>();
            if (newMetadata != null) {
                // Now go through all repositories and update existing or create missing
                for (RepositoryMetadata repositoryMetadata : newMetadata.repositories()) {
                    Repository repository = survivors.get(repositoryMetadata.name());
                    // 代表是更新操作
                    if (repository != null) {
                        // Found previous version of this repository
                        // 检测元数据是否发生了变化
                        RepositoryMetadata previousMetadata = repository.getMetadata();
                        if (previousMetadata.type().equals(repositoryMetadata.type()) == false
                            || previousMetadata.settings().equals(repositoryMetadata.settings()) == false) {
                            // Previous version is different from the version in settings
                            logger.debug("updating repository [{}]", repositoryMetadata.name());

                            // 关闭旧仓库 并创建新仓库  默认的FS仓库是NOOP
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
     * 推测仓库应该是多个节点连接到同一个地方 这样就不需要额外的数据同步机制了
     * 因为从ES的实现上来看 每个节点在将数据写入到repository时 并不会对其他节点的仓库做数据同步 所以拉取仓库数据时应该是节点无关的
     *
     * 比如某个新的分片此时分配的节点与写入快照数据的节点不一致 如果repository不是互通的 那么无法恢复数据
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
     * 通过初始化时设置的 factory 配合某个仓库的元数据信息 生成新的仓库对象
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
     * 检测某个仓库此时是否在使用中
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
