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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.intSetting;

/**
 * 代理连接策略
 */
public class ProxyConnectionStrategy extends RemoteConnectionStrategy {

    /**
     * The remote address for the proxy. The connections will be opened to the configured address.
     */
    public static final Setting.AffixSetting<String> PROXY_ADDRESS = Setting.affixKeySetting(
        "cluster.remote.",
        "proxy_address",
        (ns, key) -> Setting.simpleString(key, new StrategyValidator<>(ns, key, ConnectionStrategy.PROXY, s -> {
                if (Strings.hasLength(s)) {
                    parsePort(s);
                }
            }), Setting.Property.Dynamic, Setting.Property.NodeScope));

    /**
     * The maximum number of socket connections that will be established to a remote cluster. The default is 18.
     */
    public static final Setting.AffixSetting<Integer> REMOTE_SOCKET_CONNECTIONS = Setting.affixKeySetting(
        "cluster.remote.",
        "proxy_socket_connections",
        (ns, key) -> intSetting(key, 18, 1, new StrategyValidator<>(ns, key, ConnectionStrategy.PROXY),
            Setting.Property.Dynamic, Setting.Property.NodeScope));

    /**
     * A configurable server_name attribute
     */
    public static final Setting.AffixSetting<String> SERVER_NAME = Setting.affixKeySetting(
        "cluster.remote.",
        "server_name",
        (ns, key) -> Setting.simpleString(key, new StrategyValidator<>(ns, key, ConnectionStrategy.PROXY),
            Setting.Property.Dynamic, Setting.Property.NodeScope));

    static final int CHANNELS_PER_CONNECTION = 1;

    /**
     * 在一次创建连接的操作中多给出几次机会 用于在失败时继续尝试
     */
    private static final int MAX_CONNECT_ATTEMPTS_PER_RUN = 3;
    private static final Logger logger = LogManager.getLogger(ProxyConnectionStrategy.class);

    /**
     * 代表该策略支持的最大连接数
     */
    private final int maxNumConnections;
    private final String configuredAddress;
    private final String configuredServerName;
    private final Supplier<TransportAddress> address;
    private final AtomicReference<ClusterName> remoteClusterName = new AtomicReference<>();
    private final ConnectionManager.ConnectionValidator clusterNameValidator;

    /**
     * 代理连接策略
     * @param clusterAlias
     * @param transportService
     * @param connectionManager
     * @param settings
     */
    ProxyConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                            Settings settings) {
        this(
            clusterAlias,
            transportService,
            connectionManager,
            settings,
            REMOTE_SOCKET_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(settings),
            PROXY_ADDRESS.getConcreteSettingForNamespace(clusterAlias).get(settings),
            SERVER_NAME.getConcreteSettingForNamespace(clusterAlias).get(settings));
    }

    ProxyConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                            Settings settings, int maxNumConnections, String configuredAddress) {
        this(clusterAlias, transportService, connectionManager, settings, maxNumConnections, configuredAddress,
            () -> resolveAddress(configuredAddress), null);
    }

    ProxyConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                            Settings settings, int maxNumConnections, String configuredAddress, String configuredServerName) {
        this(clusterAlias, transportService, connectionManager, settings, maxNumConnections, configuredAddress,
            () -> resolveAddress(configuredAddress), configuredServerName);
    }

    /**
     * 代理连接策略
     * @param clusterAlias  目标集群的名字
     * @param transportService    该对象实现连接功能
     * @param connectionManager  管理所有创建的连接
     * @param settings
     * @param maxNumConnections     最大连接数
     * @param configuredAddress     从配置中获取的目标地址
     * @param address               通过特殊处理后 获取实际可用的连接对象
     * @param configuredServerName
     */
    ProxyConnectionStrategy(String clusterAlias, TransportService transportService, RemoteConnectionManager connectionManager,
                            Settings settings, int maxNumConnections, String configuredAddress, Supplier<TransportAddress> address,
                            String configuredServerName) {
        super(clusterAlias, transportService, connectionManager, settings);
        this.maxNumConnections = maxNumConnections;
        this.configuredAddress = configuredAddress;
        this.configuredServerName = configuredServerName;
        assert Strings.isEmpty(configuredAddress) == false : "Cannot use proxy connection strategy with no configured addresses";
        this.address = address;
        // 校验集群名称时 发起一个握手请求 并从响应结果中获取集群名称  通过比较集群名称进行校验
        this.clusterNameValidator = (newConnection, actualProfile, listener) ->
            transportService.handshake(newConnection, actualProfile.getHandshakeTimeout().millis(), cn -> true,
                ActionListener.map(listener, resp -> {
                    ClusterName remote = resp.getClusterName();
                    if (remoteClusterName.compareAndSet(null, remote)) {
                        return null;
                    } else {
                        if (remoteClusterName.get().equals(remote) == false) {
                            DiscoveryNode node = newConnection.getNode();
                            throw new ConnectTransportException(node, "handshake failed. unexpected remote cluster name " + remote);
                        }
                        return null;
                    }
                }));
    }

    /**
     * 找到集群有关代理地址的配置
     * @return
     */
    static Stream<Setting.AffixSetting<?>> enablementSettings() {
        return Stream.of(ProxyConnectionStrategy.PROXY_ADDRESS);
    }

    /**
     * 从输入流中还原代理模式信息
     * @return
     */
    static Writeable.Reader<RemoteConnectionInfo.ModeInfo> infoReader() {
        return ProxyModeInfo::new;
    }

    /**
     * 只要没有满足最大连接数 还会继续申请创建连接
     * @return
     */
    @Override
    protected boolean shouldOpenMoreConnections() {
        return connectionManager.size() < maxNumConnections;
    }

    /**
     * 根据最新配置检测是否需要重新创建连接
     * @param newSettings
     * @return
     */
    @Override
    protected boolean strategyMustBeRebuilt(Settings newSettings) {
        String address = PROXY_ADDRESS.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        int numOfSockets = REMOTE_SOCKET_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        String serverName = SERVER_NAME.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        return numOfSockets != maxNumConnections || configuredAddress.equals(address) == false ||
            Objects.equals(serverName, configuredServerName) == false;
    }

    @Override
    protected ConnectionStrategy strategyType() {
        return ConnectionStrategy.PROXY;
    }

    @Override
    protected void connectImpl(ActionListener<Void> listener) {
        performProxyConnectionProcess(listener);
    }

    @Override
    public RemoteConnectionInfo.ModeInfo getModeInfo() {
        return new ProxyModeInfo(configuredAddress, configuredServerName, maxNumConnections, connectionManager.size());
    }

    private void performProxyConnectionProcess(ActionListener<Void> listener) {
        openConnections(listener, 1);
    }

    /**
     * 真正创建连接的地方  这里好像会额外触发很多次连接操作 但是目的就是在最初的一次openConnections 调用后 直接申请创建max数量的连接
     * @param finished
     * @param attemptNumber
     */
    private void openConnections(ActionListener<Void> finished, int attemptNumber) {
        if (attemptNumber <= MAX_CONNECT_ATTEMPTS_PER_RUN) {
            TransportAddress resolved = address.get();

            // 代表还允许创建多少连接
            int remaining = maxNumConnections - connectionManager.size();
            ActionListener<Void> compositeListener = new ActionListener<>() {

                private final AtomicInteger successfulConnections = new AtomicInteger(0);
                private final CountDown countDown = new CountDown(remaining);

                /**
                 * 当连接建立完成时 会递归继续创建连接
                 * @param v
                 */
                @Override
                public void onResponse(Void v) {
                    successfulConnections.incrementAndGet();
                    if (countDown.countDown()) {
                        if (shouldOpenMoreConnections()) {
                            openConnections(finished, attemptNumber + 1);
                        } else {
                            finished.onResponse(v);
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (countDown.countDown()) {
                        openConnections(finished, attemptNumber + 1);
                    }
                }
            };

            // 这里会尝试一次性将连接数建立满
            for (int i = 0; i < remaining; ++i) {
                String id = clusterAlias + "#" + resolved;
                Map<String, String> attributes;
                if (Strings.isNullOrEmpty(configuredServerName)) {
                    attributes = Collections.emptyMap();
                } else {
                    attributes = Collections.singletonMap("server_name", configuredServerName);
                }
                // 生成对端节点
                DiscoveryNode node = new DiscoveryNode(id, resolved, attributes, DiscoveryNodeRole.BUILT_IN_ROLES,
                    Version.CURRENT.minimumCompatibilityVersion());

                // 通过manager对象创建连接  每次连接调用还有2次额外机会
                connectionManager.connectToNode(node, null, clusterNameValidator, new ActionListener<>() {
                    @Override
                    public void onResponse(Void v) {
                        compositeListener.onResponse(v);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(new ParameterizedMessage("failed to open remote connection [remote cluster: {}, address: {}]",
                            clusterAlias, resolved), e);
                        compositeListener.onFailure(e);
                    }
                });
            }
        } else {
            // 代表递归次数达到单次请求上限
            int openConnections = connectionManager.size();
            // 此时没有任何一条连接被成功创建
            if (openConnections == 0) {
                finished.onFailure(new IllegalStateException("Unable to open any proxy connections to remote cluster [" + clusterAlias
                    + "]"));
            } else {
                // 在使用完机会后还是没达到最大连接数 如果达到的话会触发 compositeListener.finished
                logger.debug("unable to open maximum number of connections [remote cluster: {}, opened: {}, maximum: {}]", clusterAlias,
                    openConnections, maxNumConnections);
                finished.onResponse(null);
            }
        }
    }

    /**
     * 将某种格式化的地址字符串转换为地址对象
     * @param address
     * @return
     */
    private static TransportAddress resolveAddress(String address) {
        return new TransportAddress(parseConfiguredAddress(address));
    }

    /**
     * 描述代理连接模式的信息
     */
    public static class ProxyModeInfo implements RemoteConnectionInfo.ModeInfo {

        private final String address;
        private final String serverName;
        private final int maxSocketConnections;
        private final int numSocketsConnected;

        public ProxyModeInfo(String address, String serverName, int maxSocketConnections, int numSocketsConnected) {
            this.address = address;
            this.serverName = serverName;
            this.maxSocketConnections = maxSocketConnections;
            this.numSocketsConnected = numSocketsConnected;
        }

        private ProxyModeInfo(StreamInput input) throws IOException {
            address = input.readString();
            if (input.getVersion().onOrAfter(Version.V_7_7_0)) {
                serverName = input.readString();
            } else {
                serverName = null;
            }
            maxSocketConnections = input.readVInt();
            numSocketsConnected = input.readVInt();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("proxy_address", address);
            builder.field("server_name", serverName);
            builder.field("num_proxy_sockets_connected", numSocketsConnected);
            builder.field("max_proxy_socket_connections", maxSocketConnections);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(address);
            if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
                out.writeString(serverName);
            }
            out.writeVInt(maxSocketConnections);
            out.writeVInt(numSocketsConnected);
        }

        @Override
        public boolean isConnected() {
            return numSocketsConnected > 0;
        }

        @Override
        public String modeName() {
            return "proxy";
        }

        public String getAddress() {
            return address;
        }

        public String getServerName() {
            return serverName;
        }

        public int getMaxSocketConnections() {
            return maxSocketConnections;
        }

        public int getNumSocketsConnected() {
            return numSocketsConnected;
        }

        @Override
        public RemoteConnectionStrategy.ConnectionStrategy modeType() {
            return RemoteConnectionStrategy.ConnectionStrategy.PROXY;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProxyModeInfo otherProxy = (ProxyModeInfo) o;
            return maxSocketConnections == otherProxy.maxSocketConnections &&
                numSocketsConnected == otherProxy.numSocketsConnected &&
                Objects.equals(address, otherProxy.address) &&
                Objects.equals(serverName, otherProxy.serverName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, serverName, maxSocketConnections, numSocketsConnected);
        }
    }
}
