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

package org.elasticsearch.transport.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.nio.BytesChannelContext;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.Config;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 * 使用原生的 NIO 实现传输层
 */
public class NioTransport extends TcpTransport {

    private static final Logger logger = LogManager.getLogger(NioTransport.class);

    /**
     * 该对象会按需分配内存
     */
    protected final PageAllocator pageAllocator;
    private final ConcurrentMap<String, TcpChannelFactory> profileToChannelFactory = newConcurrentMap();
    private final NioGroupFactory groupFactory;
    private volatile NioGroup nioGroup;

    /**
     * 根据传入的node 生成对应的channel
     */
    private volatile Function<DiscoveryNode, TcpChannelFactory> clientChannelFactory;


    /**
     * 初始化基于原生NIO 的传输层对象
     * @param settings
     * @param version
     * @param threadPool
     * @param networkService
     * @param pageCacheRecycler
     * @param namedWriteableRegistry
     * @param circuitBreakerService
     * @param groupFactory
     */
    protected NioTransport(Settings settings, Version version, ThreadPool threadPool, NetworkService networkService,
                           PageCacheRecycler pageCacheRecycler, NamedWriteableRegistry namedWriteableRegistry,
                           CircuitBreakerService circuitBreakerService, NioGroupFactory groupFactory) {
        super(settings, version, threadPool, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, networkService);
        this.pageAllocator = new PageAllocator(pageCacheRecycler);
        this.groupFactory = groupFactory;
    }

    /**
     * 从不同profile信息对应的channel工厂中生成服务端channel 并绑定到目标端口上
     * @param name    the profile name
     * @param address the address to bind to
     * @return
     * @throws IOException
     */
    @Override
    protected NioTcpServerChannel bind(String name, InetSocketAddress address) throws IOException {
        TcpChannelFactory channelFactory = this.profileToChannelFactory.get(name);
        // 生成channel 并注册到事件循环组
        // 因为这里是非事件循环线程执行的绑定 绑定动作会异步化 这里需要阻塞监听结果
        NioTcpServerChannel serverChannel = nioGroup.bindServerChannel(address, channelFactory);
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        // 当绑定完成时 触发监听器
        serverChannel.addBindListener(ActionListener.toBiConsumer(future));
        // 阻塞直到绑定完成
        future.actionGet();
        return serverChannel;
    }

    /**
     * 将目标节点变成一个  socketChannel
     * @param node for the initiated connection
     * @return
     * @throws IOException
     */
    @Override
    protected NioTcpChannel initiateChannel(DiscoveryNode node) throws IOException {
        InetSocketAddress address = node.getAddress().address();
        return nioGroup.openChannel(address, clientChannelFactory.apply(node));
    }

    /**
     * 当传输层对象启动时 首先触发该方法
     */
    @Override
    protected void doStart() {
        boolean success = false;
        try {
            // 生成一个事件循环组
            // 对应NettyTransport的  eventLoopGroup = new NioEventLoopGroup(workerCount, threadFactory);
            // 这个时候已经启动事件循环组了
            nioGroup = groupFactory.getTransportGroup();

            ProfileSettings clientProfileSettings = new ProfileSettings(settings, TransportSettings.DEFAULT_PROFILE);

            // 生成客户端工厂 ES中哪个节点都同时作为client/server存在 一旦通过ClusterApplierService感知到其他节点就会通过transportService进行连接
            clientChannelFactory = clientChannelFactoryFunction(clientProfileSettings);

            // 如果当前节点作为服务端 那么根据配置创建各种不同的channel装配工厂
            if (NetworkService.NETWORK_SERVER.get(settings)) {
                // loop through all profiles and start them up, special handling for default one
                // 针对服务器配置允许使用多套 profile信息
                for (ProfileSettings profileSettings : profileSettings) {
                    String profileName = profileSettings.profileName;
                    TcpChannelFactory factory = serverChannelFactory(profileSettings);
                    profileToChannelFactory.putIfAbsent(profileName, factory);
                    // 这里已经将serverChannel 绑定到本地了
                    bindServer(profileSettings);
                }
            }

            super.doStart();
            success = true;
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        } finally {
            if (success == false) {
                doStop();
            }
        }
    }

    @Override
    protected void stopInternal() {
        try {
            nioGroup.close();
        } catch (Exception e) {
            logger.warn("unexpected exception while stopping nio group", e);
        }
        profileToChannelFactory.clear();
    }

    protected void acceptChannel(NioSocketChannel channel) {
        serverAcceptedChannel((NioTcpChannel) channel);
    }

    protected TcpChannelFactory serverChannelFactory(ProfileSettings profileSettings) {
        return new TcpChannelFactoryImpl(profileSettings, false);
    }

    /**
     * @param profileSettings
     * @return
     */
    protected Function<DiscoveryNode, TcpChannelFactory> clientChannelFactoryFunction(ProfileSettings profileSettings) {
        return (n) -> new TcpChannelFactoryImpl(profileSettings, true);
    }

    protected abstract static class TcpChannelFactory extends ChannelFactory<NioTcpServerChannel, NioTcpChannel> {

        protected TcpChannelFactory(ProfileSettings profileSettings) {
            super(profileSettings.tcpNoDelay, profileSettings.tcpKeepAlive, profileSettings.tcpKeepIdle, profileSettings.tcpKeepInterval,
                profileSettings.tcpKeepCount, profileSettings.reuseAddress, Math.toIntExact(profileSettings.sendBufferSize.getBytes()),
                Math.toIntExact(profileSettings.receiveBufferSize.getBytes()));
        }
    }

    /**
     * jdk底层的channel 会被封装成es的channel
     */
    private class TcpChannelFactoryImpl extends TcpChannelFactory {

        /**
         * 代表创建的是 服务端channel 还是客户端channel
         */
        private final boolean isClient;
        private final String profileName;

        /**
         *
         * @param profileSettings  该配置对象中的各种属性会被抽取出来 用于组装原生的channel
         * @param isClient
         */
        private TcpChannelFactoryImpl(ProfileSettings profileSettings, boolean isClient) {
            super(profileSettings);
            this.isClient = isClient;
            this.profileName = profileSettings.profileName;
        }

        /**
         * 将普通的channel 封装成ESchannel
         * @param selector     the channel will be registered with channel会绑定在哪个选择器上
         * @param channel      the raw channel  JAVA原生channel
         * @param socketConfig the socket config  该channel相关的配置 还包含了remoteAddress(目标服务器地址)
         * @return
         */
        @Override
        public NioTcpChannel createChannel(NioSelector selector, SocketChannel channel, Config.Socket socketConfig) {
            NioTcpChannel nioChannel = new NioTcpChannel(isClient == false, profileName, channel);
            Consumer<Exception> exceptionHandler = (e) -> onException(nioChannel, e);
            // 专门负责往该channel中写数据的writerHandler 对象
            TcpReadWriteHandler handler = new TcpReadWriteHandler(nioChannel, pageCacheRecycler, NioTransport.this);
            // 将相关对象包装成context  设置到channel后返回channel
            BytesChannelContext context = new BytesChannelContext(nioChannel, selector, socketConfig, exceptionHandler, handler,
                new InboundChannelBuffer(pageAllocator));
            nioChannel.setContext(context);
            return nioChannel;
        }

        /**
         * 将绑定的事件循环对象  java服务端通道  和配置项信息包装成ESServerChannel
         * @param selector the channel will be registered with
         * @param channel  the raw channel
         * @param socketConfig the socket config
         * @return
         */
        @Override
        public NioTcpServerChannel createServerChannel(NioSelector selector, ServerSocketChannel channel,
                                                       Config.ServerSocket socketConfig) {
            NioTcpServerChannel nioChannel = new NioTcpServerChannel(channel);
            Consumer<Exception> exceptionHandler = (e) -> onServerException(nioChannel, e);
            // 当接收到一条新的连接时 触发acceptChannel  当前逻辑仅是打印一条日志
            Consumer<NioSocketChannel> acceptor = NioTransport.this::acceptChannel;
            // 将相关信息包装成上下文后 绑定到channel上
            ServerChannelContext context = new ServerChannelContext(nioChannel, this, selector, socketConfig, acceptor, exceptionHandler);
            nioChannel.setContext(context);
            return nioChannel;
        }
    }
}
