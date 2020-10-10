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

package org.elasticsearch.transport.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.core.internal.net.NetUtils;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NettyAllocator;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.Setting.byteSizeSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * There are 4 types of connections per node, low/med/high/ping. Low if for batch oriented APIs (like recovery or
 * batch) with high payload that will cause regular request. (like search or single index) to take
 * longer. Med is for the typical search / single doc index. And High for things like cluster state. Ping is reserved for
 * sending out ping requests to other nodes.
 * TcpTransport 实际的连接操作会交由子类实现
 * 通过 Bootstrap/ServerBootstrap 组装channel 并注册到事件循环中 之后通过事件循环监听准备好的事件 并通过handler处理读写请求
 */
public class Netty4Transport extends TcpTransport {
    private static final Logger logger = LogManager.getLogger(Netty4Transport.class);

    public static final Setting<Integer> WORKER_COUNT =
        new Setting<>("transport.netty.worker_count",
            (s) -> Integer.toString(EsExecutors.allocatedProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "transport.netty.worker_count"), Property.NodeScope);

    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_SIZE = Setting.byteSizeSetting(
        "transport.netty.receive_predictor_size", new ByteSizeValue(64, ByteSizeUnit.KB), Property.NodeScope);
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MIN =
        byteSizeSetting("transport.netty.receive_predictor_min", NETTY_RECEIVE_PREDICTOR_SIZE, Property.NodeScope);
    public static final Setting<ByteSizeValue> NETTY_RECEIVE_PREDICTOR_MAX =
        byteSizeSetting("transport.netty.receive_predictor_max", NETTY_RECEIVE_PREDICTOR_SIZE, Property.NodeScope);
    public static final Setting<Integer> NETTY_BOSS_COUNT =
        intSetting("transport.netty.boss_count", 1, 1, Property.NodeScope);

    /**
     * 分配接收消息的缓冲区
     */
    private final RecvByteBufAllocator recvByteBufAllocator;
    private final int workerCount;
    /**
     * 描述应当分配的最小空间 以及最大空间
     */
    private final ByteSizeValue receivePredictorMin;
    private final ByteSizeValue receivePredictorMax;
    /**
     * 每个profile对应一个引导程序
     */
    private final Map<String, ServerBootstrap> serverBootstraps = newConcurrentMap();

    /**
     * 该对象负责快速使用 SocketChannel 连接到某个地址
     */
    private volatile Bootstrap clientBootstrap;
    /**
     * 每个对象都包含一个事件循环线程
     */
    private volatile NioEventLoopGroup eventLoopGroup;

    public Netty4Transport(Settings settings, Version version, ThreadPool threadPool, NetworkService networkService,
                           PageCacheRecycler pageCacheRecycler, NamedWriteableRegistry namedWriteableRegistry,
                           CircuitBreakerService circuitBreakerService) {
        super(settings, version, threadPool, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, networkService);
        // 设置进程数 核心线程数
        Netty4Utils.setAvailableProcessors(EsExecutors.NODE_PROCESSORS_SETTING.get(settings));
        this.workerCount = WORKER_COUNT.get(settings);

        // See AdaptiveReceiveBufferSizePredictor#DEFAULT_XXX for default values in netty..., we can use higher ones for us, even fixed one
        this.receivePredictorMin = NETTY_RECEIVE_PREDICTOR_MIN.get(settings);
        this.receivePredictorMax = NETTY_RECEIVE_PREDICTOR_MAX.get(settings);
        if (receivePredictorMax.getBytes() == receivePredictorMin.getBytes()) {
            recvByteBufAllocator = new FixedRecvByteBufAllocator((int) receivePredictorMax.getBytes());
        } else {
            // 基于当前情况自动分配合适大小的byteBuf缓冲区  不细看
            recvByteBufAllocator = new AdaptiveRecvByteBufAllocator((int) receivePredictorMin.getBytes(),
                (int) receivePredictorMin.getBytes(), (int) receivePredictorMax.getBytes());
        }
    }

    /**
     * 当传输层对象被启动时 绑定本地端口开启监听
     */
    @Override
    protected void doStart() {
        boolean success = false;
        try {
            ThreadFactory threadFactory = daemonThreadFactory(settings, TRANSPORT_WORKER_THREAD_NAME_PREFIX);
            // 每条线程都会进行事件循环
            eventLoopGroup = new NioEventLoopGroup(workerCount, threadFactory);
            // 创建一个新的bootstrap 并进行装配 这样channel 会注册到 某个eventLoop的 selector上
            clientBootstrap = createClientBootstrap(eventLoopGroup);
            // 如果当前节点在集群中以服务器的角色存在  创建对应数量的服务器引导程序
            if (NetworkService.NETWORK_SERVER.get(settings)) {
                for (ProfileSettings profileSettings : profileSettings) {
                    createServerBootstrap(profileSettings, eventLoopGroup);
                    // 在这里将会根据指定的地址数量 创建等量 ServerSocketChannel   使用多个ServerBootstrap是因为每个profile配置值不同  注意 ServerBootstrap本身就仅作为一个引导程序
                    bindServer(profileSettings);
                }
            }
            super.doStart();
            success = true;
        } finally {
            if (success == false) {
                doStop();
            }
        }
    }

    /**
     * 这里完成了对 Bootstrap的装配
     * @param eventLoopGroup 每个对象都会检测各种io事件 以及在空闲时间处理队列中的任务
     * @return
     */
    private Bootstrap createClientBootstrap(NioEventLoopGroup eventLoopGroup) {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);

        // NettyAllocator will return the channel type designed to work with the configured allocator
        bootstrap.channel(NettyAllocator.getChannelType());
        bootstrap.option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator());

        bootstrap.option(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings));
        bootstrap.option(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));
        if (TransportSettings.TCP_KEEP_ALIVE.get(settings)) {
            // Netty logs a warning if it can't set the option, so try this only on supported platforms
            if (IOUtils.LINUX || IOUtils.MAC_OS_X) {
                if (TransportSettings.TCP_KEEP_IDLE.get(settings) >= 0) {
                    final SocketOption<Integer> keepIdleOption = NetUtils.getTcpKeepIdleSocketOptionOrNull();
                    if (keepIdleOption != null) {
                        bootstrap.option(NioChannelOption.of(keepIdleOption), TransportSettings.TCP_KEEP_IDLE.get(settings));
                    }
                }
                if (TransportSettings.TCP_KEEP_INTERVAL.get(settings) >= 0) {
                    final SocketOption<Integer> keepIntervalOption = NetUtils.getTcpKeepIntervalSocketOptionOrNull();
                    if (keepIntervalOption != null) {
                        bootstrap.option(NioChannelOption.of(keepIntervalOption), TransportSettings.TCP_KEEP_INTERVAL.get(settings));
                    }
                }
                if (TransportSettings.TCP_KEEP_COUNT.get(settings) >= 0) {
                    final SocketOption<Integer> keepCountOption = NetUtils.getTcpKeepCountSocketOptionOrNull();
                    if (keepCountOption != null) {
                        bootstrap.option(NioChannelOption.of(keepCountOption), TransportSettings.TCP_KEEP_COUNT.get(settings));
                    }
                }
            }
        }

        final ByteSizeValue tcpSendBufferSize = TransportSettings.TCP_SEND_BUFFER_SIZE.get(settings);
        if (tcpSendBufferSize.getBytes() > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, Math.toIntExact(tcpSendBufferSize.getBytes()));
        }

        final ByteSizeValue tcpReceiveBufferSize = TransportSettings.TCP_RECEIVE_BUFFER_SIZE.get(settings);
        if (tcpReceiveBufferSize.getBytes() > 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, Math.toIntExact(tcpReceiveBufferSize.getBytes()));
        }

        bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);

        final boolean reuseAddress = TransportSettings.TCP_REUSE_ADDRESS.get(settings);
        bootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);

        return bootstrap;
    }

    private void createServerBootstrap(ProfileSettings profileSettings, NioEventLoopGroup eventLoopGroup) {
        String name = profileSettings.profileName;
        if (logger.isDebugEnabled()) {
            logger.debug("using profile[{}], worker_count[{}], port[{}], bind_host[{}], publish_host[{}], receive_predictor[{}->{}]",
                name, workerCount, profileSettings.portOrRange, profileSettings.bindHosts, profileSettings.publishHosts,
                receivePredictorMin, receivePredictorMax);
        }

        final ServerBootstrap serverBootstrap = new ServerBootstrap();

        serverBootstrap.group(eventLoopGroup);

        // NettyAllocator will return the channel type designed to work with the configuredAllocator
        serverBootstrap.channel(NettyAllocator.getServerChannelType());

        // Set the allocators for both the server channel and the child channels created
        serverBootstrap.option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator());
        serverBootstrap.childOption(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator());

        // 当监听到连接时 会转给子对象处理 借此实现reactor模型
        serverBootstrap.childHandler(getServerChannelInitializer(name));
        serverBootstrap.handler(new ServerChannelExceptionHandler());

        serverBootstrap.childOption(ChannelOption.TCP_NODELAY, profileSettings.tcpNoDelay);
        serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, profileSettings.tcpKeepAlive);
        if (profileSettings.tcpKeepAlive) {
            // Netty logs a warning if it can't set the option, so try this only on supported platforms
            if (IOUtils.LINUX || IOUtils.MAC_OS_X) {
                if (profileSettings.tcpKeepIdle >= 0) {
                    final SocketOption<Integer> keepIdleOption = NetUtils.getTcpKeepIdleSocketOptionOrNull();
                    if (keepIdleOption != null) {
                        serverBootstrap.childOption(NioChannelOption.of(keepIdleOption), profileSettings.tcpKeepIdle);
                    }
                }
                if (profileSettings.tcpKeepInterval >= 0) {
                    final SocketOption<Integer> keepIntervalOption = NetUtils.getTcpKeepIntervalSocketOptionOrNull();
                    if (keepIntervalOption != null) {
                        serverBootstrap.childOption(NioChannelOption.of(keepIntervalOption), profileSettings.tcpKeepInterval);
                    }

                }
                if (profileSettings.tcpKeepCount >= 0) {
                    final SocketOption<Integer> keepCountOption = NetUtils.getTcpKeepCountSocketOptionOrNull();
                    if (keepCountOption != null) {
                        serverBootstrap.childOption(NioChannelOption.of(keepCountOption), profileSettings.tcpKeepCount);
                    }
                }
            }
        }

        if (profileSettings.sendBufferSize.getBytes() != -1) {
            serverBootstrap.childOption(ChannelOption.SO_SNDBUF, Math.toIntExact(profileSettings.sendBufferSize.getBytes()));
        }

        if (profileSettings.receiveBufferSize.getBytes() != -1) {
            serverBootstrap.childOption(ChannelOption.SO_RCVBUF, Math.toIntExact(profileSettings.receiveBufferSize.bytesAsInt()));
        }

        serverBootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);
        serverBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);

        serverBootstrap.option(ChannelOption.SO_REUSEADDR, profileSettings.reuseAddress);
        serverBootstrap.childOption(ChannelOption.SO_REUSEADDR, profileSettings.reuseAddress);
        serverBootstrap.validate();

        serverBootstraps.put(name, serverBootstrap);
    }

    protected ChannelHandler getServerChannelInitializer(String name) {
        return new ServerChannelInitializer(name);
    }

    protected ChannelHandler getClientChannelInitializer(DiscoveryNode node) {
        return new ClientChannelInitializer();
    }

    static final AttributeKey<Netty4TcpChannel> CHANNEL_KEY = AttributeKey.newInstance("es-channel");
    static final AttributeKey<Netty4TcpServerChannel> SERVER_CHANNEL_KEY = AttributeKey.newInstance("es-server-channel");

    /**
     * 生成通往目标地址的channel
     * @param node for the initiated connection
     * @return
     * @throws IOException
     */
    @Override
    protected Netty4TcpChannel initiateChannel(DiscoveryNode node) throws IOException {
        InetSocketAddress address = node.getAddress().address();
        Bootstrap bootstrapWithHandler = clientBootstrap.clone();
        bootstrapWithHandler.handler(getClientChannelInitializer(node));
        bootstrapWithHandler.remoteAddress(address);
        // 生成channel 并连接到目标地址
        ChannelFuture connectFuture = bootstrapWithHandler.connect();

        Channel channel = connectFuture.channel();
        if (channel == null) {
            ExceptionsHelper.maybeDieOnAnotherThread(connectFuture.cause());
            throw new IOException(connectFuture.cause());
        }
        addClosedExceptionLogger(channel);

        Netty4TcpChannel nettyChannel = new Netty4TcpChannel(channel, false, "default", connectFuture);
        channel.attr(CHANNEL_KEY).set(nettyChannel);

        return nettyChannel;
    }

    /**
     * 生成合适的地址后 绑定仅返回生成的channel
     * @param name    the profile name
     * @param address the address to bind to
     * @return
     */
    @Override
    protected Netty4TcpServerChannel bind(String name, InetSocketAddress address) {
        // 先创建一个serverChannel 并注册到事件循环上 之后为该对象设置accept事件 这样就可以在事件循环中接收新的连接了
        Channel channel = serverBootstraps.get(name).bind(address).syncUninterruptibly().channel();
        Netty4TcpServerChannel esChannel = new Netty4TcpServerChannel(channel);
        channel.attr(SERVER_CHANNEL_KEY).set(esChannel);
        return esChannel;
    }

    @Override
    @SuppressForbidden(reason = "debug")
    protected void stopInternal() {
        Releasables.close(() -> {
            Future<?> shutdownFuture = eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
            shutdownFuture.awaitUninterruptibly();
            if (shutdownFuture.isSuccess() == false) {
                logger.warn("Error closing netty event loop group", shutdownFuture.cause());
            }

            serverBootstraps.clear();
            clientBootstrap = null;
        });
    }

    protected class ClientChannelInitializer extends ChannelInitializer<Channel> {

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast("logging", new ESLoggingHandler());
            // using a dot as a prefix means this cannot come from any settings parsed
            ch.pipeline().addLast("dispatcher", new Netty4MessageChannelHandler(pageCacheRecycler, Netty4Transport.this));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            super.exceptionCaught(ctx, cause);
        }
    }

    /**
     * 装配管道
     */
    protected class ServerChannelInitializer extends ChannelInitializer<Channel> {

        protected final String name;

        protected ServerChannelInitializer(String name) {
            this.name = name;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            addClosedExceptionLogger(ch);
            // 将es增强的管道绑定在原生的netty管道上 这样就可以随时获取该对象
            Netty4TcpChannel nettyTcpChannel = new Netty4TcpChannel(ch, true, name, ch.newSucceededFuture());
            ch.attr(CHANNEL_KEY).set(nettyTcpChannel);
            // 该对象会在触发每一环时打印相关日志
            ch.pipeline().addLast("logging", new ESLoggingHandler());
            // 该对象定义了消息流的编解码 解压 粘包 以及处理   WriteOp中的数据流应该是已经编码后的
            ch.pipeline().addLast("dispatcher", new Netty4MessageChannelHandler(pageCacheRecycler, Netty4Transport.this));
            // 因为接入了一个新的channel 打印日志
            serverAcceptedChannel(nettyTcpChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            super.exceptionCaught(ctx, cause);
        }
    }

    private void addClosedExceptionLogger(Channel channel) {
        channel.closeFuture().addListener(f -> {
            if (f.isSuccess() == false) {
                logger.debug(() -> new ParameterizedMessage("exception while closing channel: {}", channel), f.cause());
            }
        });
    }

    @ChannelHandler.Sharable
    private class ServerChannelExceptionHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            Netty4TcpServerChannel serverChannel = ctx.channel().attr(SERVER_CHANNEL_KEY).get();
            if (cause instanceof Error) {
                onServerException(serverChannel, new Exception(cause));
            } else {
                onServerException(serverChannel, (Exception) cause);
            }
        }
    }
}
