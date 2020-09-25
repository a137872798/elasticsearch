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

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 代表连接到远端集群的一种策略
 * 该对象同时实现了连接监听器 能够感知连接的创建等 并作出相应处理
 */
public abstract class RemoteConnectionStrategy implements TransportConnectionListener, Closeable {

    enum ConnectionStrategy {
        SNIFF(SniffConnectionStrategy.CHANNELS_PER_CONNECTION, SniffConnectionStrategy::enablementSettings,
            SniffConnectionStrategy::infoReader) {
            @Override
            public String toString() {
                return "sniff";
            }
        },
        PROXY(ProxyConnectionStrategy.CHANNELS_PER_CONNECTION, ProxyConnectionStrategy::enablementSettings,
            ProxyConnectionStrategy::infoReader) {
            @Override
            public String toString() {
                return "proxy";
            }
        };

        private final int numberOfChannels;
        private final Supplier<Stream<Setting.AffixSetting<?>>> enablementSettings;
        private final Supplier<Writeable.Reader<RemoteConnectionInfo.ModeInfo>> reader;

        ConnectionStrategy(int numberOfChannels, Supplier<Stream<Setting.AffixSetting<?>>> enablementSettings,
                           Supplier<Writeable.Reader<RemoteConnectionInfo.ModeInfo>> reader) {
            this.numberOfChannels = numberOfChannels;
            this.enablementSettings = enablementSettings;
            this.reader = reader;
        }

        public int getNumberOfChannels() {
            return numberOfChannels;
        }

        public Supplier<Stream<Setting.AffixSetting<?>>> getEnablementSettings() {
            return enablementSettings;
        }

        public Writeable.Reader<RemoteConnectionInfo.ModeInfo> getReader() {
            return reader.get();
        }
    }

    public static final Setting.AffixSetting<ConnectionStrategy> REMOTE_CONNECTION_MODE = Setting.affixKeySetting(
        "cluster.remote.", "mode", key -> new Setting<>(
            key,
            ConnectionStrategy.SNIFF.name(),
            value -> ConnectionStrategy.valueOf(value.toUpperCase(Locale.ROOT)),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic));

    /**
     * 默认情况下每个连接只支持维护 1000个监听器
     */
    public static final Setting<Integer> REMOTE_MAX_PENDING_CONNECTION_LISTENERS =
        Setting.intSetting("cluster.remote.max_pending_connection_listeners", 1000, Setting.Property.NodeScope);

    private final int maxPendingConnectionListeners;

    private static final Logger logger = LogManager.getLogger(RemoteConnectionStrategy.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object mutex = new Object();
    private List<ActionListener<Void>> listeners = new ArrayList<>();

    /**
     * 创建连接的操作委托给该对象
     */
    protected final TransportService transportService;
    /**
     * 连接管理器 该对象仅服务于 远端连接
     */
    protected final RemoteConnectionManager connectionManager;

    /**
     * 目标集群名
     */
    protected final String clusterAlias;

    /**
     *
     * @param clusterAlias
     * @param transportService
     * @param connectionManager
     * @param settings
     */
    RemoteConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                             Settings settings) {
        this.clusterAlias = clusterAlias;
        this.transportService = transportService;
        this.connectionManager = connectionManager;
        this.maxPendingConnectionListeners = REMOTE_MAX_PENDING_CONNECTION_LISTENERS.get(settings);
        // 将自身注册到 连接管理器上 监听新连接
        connectionManager.addListener(this);
    }

    static ConnectionProfile buildConnectionProfile(String clusterAlias, Settings settings) {
        // 通过远端的集群前缀 定位到具体策略模式
        ConnectionStrategy mode = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).get(settings);
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder()
            .setConnectTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
            .setHandshakeTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
            .setCompressionEnabled(RemoteClusterService.REMOTE_CLUSTER_COMPRESS.getConcreteSettingForNamespace(clusterAlias).get(settings))
            .setPingInterval(RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE.getConcreteSettingForNamespace(clusterAlias).get(settings))
            // 为不同类型的连接设置 channel数量
            .addConnections(0, TransportRequestOptions.Type.BULK, TransportRequestOptions.Type.STATE,
                TransportRequestOptions.Type.RECOVERY)
            // TODO: Evaluate if we actually need PING channels?
            .addConnections(mode.numberOfChannels, TransportRequestOptions.Type.REG, TransportRequestOptions.Type.PING);
        return builder.build();
    }

    /**
     * 根据相关信息生成 远端连接策略
     * @param clusterAlias
     * @param transportService
     * @param connectionManager
     * @param settings
     * @return
     */
    static RemoteConnectionStrategy buildStrategy(String clusterAlias, TransportService transportService,
                                                  RemoteConnectionManager connectionManager, Settings settings) {
        ConnectionStrategy mode = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).get(settings);
        switch (mode) {
            case SNIFF:
                return new SniffConnectionStrategy(clusterAlias, transportService, connectionManager, settings);
            case PROXY:
                return new ProxyConnectionStrategy(clusterAlias, transportService, connectionManager, settings);
            default:
                throw new AssertionError("Invalid connection strategy" + mode);
        }
    }

    /**
     * 获取所有远端的集群名
     * @param settings
     * @return
     */
    static Set<String> getRemoteClusters(Settings settings) {
        final Stream<Setting.AffixSetting<?>> enablementSettings = Arrays.stream(ConnectionStrategy.values())
            .flatMap(strategy -> strategy.getEnablementSettings().get());
        return enablementSettings.flatMap(s -> getClusterAlias(settings, s)).collect(Collectors.toSet());
    }

    public static boolean isConnectionEnabled(String clusterAlias, Settings settings) {
        ConnectionStrategy mode = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).get(settings);
        if (mode.equals(ConnectionStrategy.SNIFF)) {
            List<String> seeds = SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias).get(settings);
            return seeds.isEmpty() == false;
        } else {
            String address = ProxyConnectionStrategy.PROXY_ADDRESS.getConcreteSettingForNamespace(clusterAlias).get(settings);
            return Strings.isEmpty(address) == false;
        }
    }

    @SuppressWarnings("unchecked")
    public static boolean isConnectionEnabled(String clusterAlias, Map<Setting<?>, Object> settings) {
        ConnectionStrategy mode = (ConnectionStrategy) settings.get(REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias));
        if (mode.equals(ConnectionStrategy.SNIFF)) {
            List<String> seeds = (List<String>) settings.get(SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS
                .getConcreteSettingForNamespace(clusterAlias));
            return seeds.isEmpty() == false;
        } else {
            String address = (String) settings.get(ProxyConnectionStrategy.PROXY_ADDRESS
                .getConcreteSettingForNamespace(clusterAlias));
            return Strings.isEmpty(address) == false;
        }
    }

    /**
     * 返回命名空间  也就是集群名
     * @param settings
     * @param affixSetting
     * @param <T>
     * @return
     */
    private static <T> Stream<String> getClusterAlias(Settings settings, Setting.AffixSetting<T> affixSetting) {
        Stream<Setting<T>> allConcreteSettings = affixSetting.getAllConcreteSettings(settings);
        return allConcreteSettings.map(affixSetting::getNamespace);
    }

    /**
     * 将拼接的字符串转换成一个 address 对象
     * @param configuredAddress
     * @return
     */
    static InetSocketAddress parseConfiguredAddress(String configuredAddress) {
        final String host = parseHost(configuredAddress);
        final int port = parsePort(configuredAddress);
        InetAddress hostAddress;
        try {
            hostAddress = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("unknown host [" + host + "]", e);
        }
        return new InetSocketAddress(hostAddress, port);
    }

    static String parseHost(final String configuredAddress) {
        return configuredAddress.substring(0, indexOfPortSeparator(configuredAddress));
    }

    static int parsePort(String remoteHost) {
        try {
            int port = Integer.valueOf(remoteHost.substring(indexOfPortSeparator(remoteHost) + 1));
            if (port <= 0) {
                throw new IllegalArgumentException("port number must be > 0 but was: [" + port + "]");
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("failed to parse port", e);
        }
    }

    private static int indexOfPortSeparator(String remoteHost) {
        int portSeparator = remoteHost.lastIndexOf(':'); // in case we have a IPv6 address ie. [::1]:9300
        if (portSeparator == -1 || portSeparator == remoteHost.length()) {
            throw new IllegalArgumentException("remote hosts need to be configured as [host:port], found [" + remoteHost + "] instead");
        }
        return portSeparator;
    }

    /**
     * Triggers a connect round unless there is one running already. If there is a connect round running, the listener will either
     * be queued or rejected and failed.
     * 连接到远端集群
     */
    void connect(ActionListener<Void> connectListener) {
        boolean runConnect = false;
        // TODO 先理解场景后再回看这段代码
        final ActionListener<Void> listener =
            ContextPreservingActionListener.wrapPreservingContext(connectListener, transportService.getThreadPool().getThreadContext());
        boolean closed;
        synchronized (mutex) {
            closed = this.closed.get();
            if (closed) {
                assert listeners.isEmpty();
            } else {
                if (listeners.size() >= maxPendingConnectionListeners) {
                    assert listeners.size() == maxPendingConnectionListeners;
                    listener.onFailure(new EsRejectedExecutionException("connect listener queue is full"));
                    return;
                } else {
                    listeners.add(listener);
                }
                // 只有首次添加监听器时 才会执行连接 否则不会执行连接逻辑
                runConnect = listeners.size() == 1;
            }
        }
        if (closed) {
            connectListener.onFailure(new AlreadyClosedException("connect handler is already closed"));
            return;
        }
        if (runConnect) {
            ExecutorService executor = transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT);
            // 当任务完成时 同时此时已经存在的所有监听器 因为在连接完成前 可能后面又增加了一些监听器 那么也会触发
            executor.submit(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    // 将此时能获取到的所有监听器的对应钩子触发 同时清理监听器
                    ActionListener.onFailure(getAndClearListeners(), e);
                }

                @Override
                protected void doRun() {
                    // 使用的监听器内整合了多个监听器
                    connectImpl(new ActionListener<>() {
                        @Override
                        public void onResponse(Void aVoid) {
                            ActionListener.onResponse(getAndClearListeners(), aVoid);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            ActionListener.onFailure(getAndClearListeners(), e);
                        }
                    });
                }
            });
        }
    }

    /**
     * 是否需要重连
     * @param newSettings
     * @return
     */
    boolean shouldRebuildConnection(Settings newSettings) {
        // 检测最新的配置中连接策略是否发生了变化
        ConnectionStrategy newMode = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        if (newMode.equals(strategyType()) == false) {
            return true;
        } else {
            // 此时检测 profile信息是否发生了变化
            Boolean compressionEnabled = RemoteClusterService.REMOTE_CLUSTER_COMPRESS
                .getConcreteSettingForNamespace(clusterAlias)
                .get(newSettings);
            TimeValue pingSchedule = RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE
                .getConcreteSettingForNamespace(clusterAlias)
                .get(newSettings);

            ConnectionProfile oldProfile = connectionManager.getConnectionProfile();
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder(oldProfile);
            builder.setCompressionEnabled(compressionEnabled);
            builder.setPingInterval(pingSchedule);
            ConnectionProfile newProfile = builder.build();
            // 当profile信息发生变化也代表需要重建连接
            return connectionProfileChanged(oldProfile, newProfile) || strategyMustBeRebuilt(newSettings);
        }
    }

    /**
     * 子类可以根据最新的配置决定是否需要重建连接
     * @param newSettings
     * @return
     */
    protected abstract boolean strategyMustBeRebuilt(Settings newSettings);

    protected abstract ConnectionStrategy strategyType();

    /**
     * 当检测到与某个节点的连接建立时
     * @param node
     * @param connection
     */
    @Override
    public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
        // 检测是否需要打开更多的连接
        if (shouldOpenMoreConnections()) {
            // try to reconnect and fill up the slot of the disconnected node
            // 继续创建连接
            connect(ActionListener.wrap(
                ignore -> logger.trace("successfully connected after disconnect of {}", node),
                e -> logger.trace(() -> new ParameterizedMessage("failed to connect after disconnect of {}", node), e)));
        }
    }

    @Override
    public void close() {
        final List<ActionListener<Void>> toNotify;
        synchronized (mutex) {
            if (closed.compareAndSet(false, true)) {
                connectionManager.removeListener(this);
                toNotify = listeners;
                listeners = Collections.emptyList();
            } else {
                toNotify = Collections.emptyList();
            }
        }
        // 以失败方式触发此时等待连接的监听器
        ActionListener.onFailure(toNotify, new AlreadyClosedException("connect handler is already closed"));
    }

    public boolean isClosed() {
        return closed.get();
    }

    // for testing only
    boolean assertNoRunningConnections() {
        synchronized (mutex) {
            assert listeners.isEmpty();
        }
        return true;
    }

    protected abstract boolean shouldOpenMoreConnections();

    protected abstract void connectImpl(ActionListener<Void> listener);

    protected abstract RemoteConnectionInfo.ModeInfo getModeInfo();

    /**
     * 获取当前所有监听器 以及清空
     * @return
     */
    private List<ActionListener<Void>> getAndClearListeners() {
        final List<ActionListener<Void>> result;
        synchronized (mutex) {
            if (listeners.isEmpty()) {
                result = Collections.emptyList();
            } else {
                result = listeners;
                listeners = new ArrayList<>();
            }
        }
        return result;
    }

    private boolean connectionProfileChanged(ConnectionProfile oldProfile, ConnectionProfile newProfile) {
        return Objects.equals(oldProfile.getCompressionEnabled(), newProfile.getCompressionEnabled()) == false
            || Objects.equals(oldProfile.getPingInterval(), newProfile.getPingInterval()) == false;
    }

    /**
     * 在读取策略配置时 需要进行校验
     * @param <T>
     */
    static class StrategyValidator<T> implements Setting.Validator<T> {

        private final String key;
        private final ConnectionStrategy expectedStrategy;
        private final String namespace;
        private final Consumer<T> valueChecker;

        StrategyValidator(String namespace, String key, ConnectionStrategy expectedStrategy) {
            this(namespace, key, expectedStrategy, (v) -> {});
        }

        StrategyValidator(String namespace, String key, ConnectionStrategy expectedStrategy, Consumer<T> valueChecker) {
            this.namespace = namespace;
            this.key = key;
            this.expectedStrategy = expectedStrategy;
            this.valueChecker = valueChecker;
        }

        @Override
        public void validate(T value) {
            valueChecker.accept(value);
        }

        @Override
        public void validate(T value, Map<Setting<?>, Object> settings, boolean isPresent) {
            Setting<ConnectionStrategy> concrete = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(namespace);
            ConnectionStrategy modeType = (ConnectionStrategy) settings.get(concrete);
            if (isPresent && modeType.equals(expectedStrategy) == false) {
                throw new IllegalArgumentException("Setting \"" + key + "\" cannot be used with the configured \"" + concrete.getKey()
                    + "\" [required=" + expectedStrategy.name() + ", configured=" + modeType.name() + "]");
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            Setting<ConnectionStrategy> concrete = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(namespace);
            Stream<Setting<?>> settingStream = Stream.of(concrete);
            return settingStream.iterator();
        }
    }
}
