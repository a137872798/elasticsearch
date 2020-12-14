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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.CancelledKeyException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isCloseConnectionException;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isConnectException;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 * 基于TCP协议实现传输层功能
 */
public abstract class TcpTransport extends AbstractLifecycleComponent implements Transport {
    private static final Logger logger = LogManager.getLogger(TcpTransport.class);

    public static final String TRANSPORT_WORKER_THREAD_NAME_PREFIX = "transport_worker";

    // This is the number of bytes necessary to read the message size
    // 消息头长度
    private static final int BYTES_NEEDED_FOR_MESSAGE_SIZE = TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;
    /**
     * 30% 的 jvm堆大小
     */
    private static final long THIRTY_PER_HEAP_SIZE = (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.3);

    /**
     * 统计相关信息的对象
     */
    final StatsTracker statsTracker = new StatsTracker();

    // this limit is per-address
    private static final int LIMIT_LOCAL_PORTS_COUNT = 6;

    /**
     * 初始化传输对象需要的参数从这里获取
     */
    protected final Settings settings;
    /**
     * 描述ES的版本 主要是做兼容性判断
     */
    private final Version version;
    /**
     * 维护所有线程池对象
     */
    protected final ThreadPool threadPool;

    /**
     * 该对象内部存储了各种 数组
     */
    protected final PageCacheRecycler pageCacheRecycler;

    /**
     * 该对象内置了一组地址解析器  可以对传入的地址进行解析 同时还包含了一系列网络需要的配置
     */
    protected final NetworkService networkService;
    /**
     * 这个对象本身可以使用一组配置
     */
    protected final Set<ProfileSettings> profileSettings;

    /**
     * 提供熔断功能
     */
    private final CircuitBreakerService circuitBreakerService;

    /**
     * key profileName  value 一组绑定的地址 以及一个对外暴露的地址
     */
    private final ConcurrentMap<String, BoundTransportAddress> profileBoundAddresses = newConcurrentMap();
    /**
     * 维护所有连接的channel  key对应profile的名字  每个profile应该是类似于一种应用名
     */
    private final Map<String, List<TcpServerChannel>> serverChannels = newConcurrentMap();
    private final Set<TcpChannel> acceptedChannels = ConcurrentCollections.newConcurrentSet();

    // this lock is here to make sure we close this transport and disconnect all the client nodes
    // connections while no connect operations is going on
    private final ReadWriteLock closeLock = new ReentrantReadWriteLock();
    private volatile BoundTransportAddress boundAddress;

    /**
     * 发送探测请求 返回对端节点的版本号 用于检测兼容性
     */
    private final TransportHandshaker handshaker;

    /**
     * 该对象专门负责维护心跳
     */
    private final TransportKeepAlive keepAlive;
    /**
     * 该对象可以包装需要发送的数据 并通过channel发送
     */
    private final OutboundHandler outboundHandler;
    /**
     * 将InboundMessage 交由 resHandler/reqHandler处理
     */
    private final InboundHandler inboundHandler;
    /**
     * 在发送一个请求时  会同时将reqId 以及resHandler 存储到一个 命令池中   当对端将res返回时 会使用resHandler进行处理
     */
    private final ResponseHandlers responseHandlers = new ResponseHandlers();
    /**
     * 存储处理req的对象
     */
    private final RequestHandlers requestHandlers = new RequestHandlers();

    /**
     * 创建基于tcp协议实现的传输层
     * @param settings
     * @param version
     * @param threadPool
     * @param pageCacheRecycler  该对象内部存储了各种数组
     * @param circuitBreakerService  该对象负责管理熔断器
     * @param namedWriteableRegistry  这里定义了一系列class 以及如何读取读取的reader对象
     * @param networkService  该对象主要是负责地址解析的
     */
    public TcpTransport(Settings settings, Version version, ThreadPool threadPool, PageCacheRecycler pageCacheRecycler,
                        CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry,
                        NetworkService networkService) {
        this.settings = settings;
        this.profileSettings = getProfileSettings(settings);
        this.version = version;
        this.threadPool = threadPool;
        this.pageCacheRecycler = pageCacheRecycler;
        this.circuitBreakerService = circuitBreakerService;
        this.networkService = networkService;
        String nodeName = Node.NODE_NAME_SETTING.get(settings);
        // 传输数据时管理使用的内存对象
        BigArrays bigArrays = new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.IN_FLIGHT_REQUESTS);

        // 该对象主要就是向外发送数据
        this.outboundHandler = new OutboundHandler(nodeName, version, statsTracker, threadPool, bigArrays);

        this.handshaker = new TransportHandshaker(version, threadPool,
            // 定义了如何发送握手请求  实际上也就是委托 outboundHandler发送请求
            (node, channel, requestId, v) -> outboundHandler.sendRequest(node, channel, requestId,
                TransportHandshaker.HANDSHAKE_ACTION_NAME, new TransportHandshaker.HandshakeRequest(version),
                TransportRequestOptions.EMPTY, v, false, true));

        // 该对象会定期为当前维护的所有channel 发送心跳包
        this.keepAlive = new TransportKeepAlive(threadPool, this.outboundHandler::sendBytes);
        // 定义接收消息 以及分配给不同handler进行处理的逻辑  只处理已经拼装好的消息  因为在传输层需要解决拆包/粘包问题
        this.inboundHandler = new InboundHandler(threadPool, outboundHandler, namedWriteableRegistry, handshaker, keepAlive,
            requestHandlers, responseHandlers);
    }

    public Version getVersion() {
        return version;
    }

    public StatsTracker getStatsTracker() {
        return statsTracker;
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    /**
     * 针对不同的业务会有不同的熔断器 比如传输层对象需要的就是 IN_FLIGHT_REQUESTS 对应的熔断器
     * @return
     */
    public Supplier<CircuitBreaker> getInflightBreaker() {
        return () -> circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
    }

    @Override
    protected void doStart() {
    }

    /**
     * 消息监听器会拦截接收到的 req res
     * @param listener
     */
    @Override
    public synchronized void setMessageListener(TransportMessageListener listener) {
        outboundHandler.setMessageListener(listener);
        inboundHandler.setMessageListener(listener);
    }


    /**
     * 维护了通往某个node的所有channel
     */
    public final class NodeChannels extends CloseableConnection {
        private final Map<TransportRequestOptions.Type, ConnectionProfile.ConnectionTypeHandle> typeMapping;
        /**
         * 与该node的所有连接
         */
        private final List<TcpChannel> channels;
        /**
         * 目标节点信息
         */
        private final DiscoveryNode node;
        private final Version version;
        private final boolean compress;
        private final AtomicBoolean isClosing = new AtomicBoolean(false);

        NodeChannels(DiscoveryNode node, List<TcpChannel> channels, ConnectionProfile connectionProfile, Version handshakeVersion) {
            this.node = node;
            this.channels = Collections.unmodifiableList(channels);
            assert channels.size() == connectionProfile.getNumConnections() : "expected channels size to be == "
                + connectionProfile.getNumConnections() + " but was: [" + channels.size() + "]";
            typeMapping = new EnumMap<>(TransportRequestOptions.Type.class);
            // 每种类型对应不同数量的handle 并且每个handle有不同数量的channel
            for (ConnectionProfile.ConnectionTypeHandle handle : connectionProfile.getHandles()) {
                for (TransportRequestOptions.Type type : handle.getTypes())
                    typeMapping.put(type, handle);
            }
            version = handshakeVersion;
            compress = connectionProfile.getCompressionEnabled();
        }

        @Override
        public Version getVersion() {
            return version;
        }

        public List<TcpChannel> getChannels() {
            return channels;
        }

        /**
         * 根据执行的类型 获取由多条channel组成的 handle对象
         * @param type
         * @return
         */
        public TcpChannel channel(TransportRequestOptions.Type type) {
            ConnectionProfile.ConnectionTypeHandle connectionTypeHandle = typeMapping.get(type);
            if (connectionTypeHandle == null) {
                throw new IllegalArgumentException("no type channel for [" + type + "]");
            }
            // 每次从范围内随机返回一个channel
            return connectionTypeHandle.getChannel(channels);
        }

        @Override
        public void close() {
            if (isClosing.compareAndSet(false, true)) {
                try {
                    // 如果不是网络线程 那么需要阻塞等待关闭
                    boolean block = lifecycle.stopped() && Transports.isTransportThread(Thread.currentThread()) == false;
                    CloseableChannel.closeChannels(channels, block);
                } finally {
                    // Call the super method to trigger listeners
                    super.close();
                }
            }
        }

        @Override
        public DiscoveryNode getNode() {
            return this.node;
        }

        /**
         * 当需要往某个node 发送请求时 通过channel组 随机选择一个channel 并发送数据
         * @param requestId see {@link ResponseHandlers#add(ResponseContext)} for details   请求的id
         * @param action the action to execute     请求的api
         * @param request the request to send    请求体
         * @param options request options to apply   描述请求体的特殊信息
         * @throws IOException
         * @throws TransportException
         */
        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            if (isClosing.get()) {
                throw new NodeNotConnectedException(node, "connection already closed");
            }
            // 本次请求的类型会使用多少条channel  并从中选择一条
            TcpChannel channel = channel(options.type());
            outboundHandler.sendRequest(node, channel, requestId, action, request, options, getVersion(), compress, false);
        }
    }

    // This allows transport implementations to potentially override specific connection profiles. This
    // primarily exists for the test implementations.
    // 很像lucene中的各种重写方法
    protected ConnectionProfile maybeOverrideConnectionProfile(ConnectionProfile connectionProfile) {
        return connectionProfile;
    }

    /**
     * 连接到目标node
     * @param node
     * @param profile 描述连接的详情
     * @param listener  定义了处理Connection 对象的逻辑
     */
    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Transport.Connection> listener) {

        Objects.requireNonNull(profile, "connection profile cannot be null");
        if (node == null) {
            throw new ConnectTransportException(null, "can't open connection to a null node");
        }
        // 该对象会对connection 进行配置
        ConnectionProfile finalProfile = maybeOverrideConnectionProfile(profile);
        closeLock.readLock().lock(); // ensure we don't open connections while we are closing
        try {
            ensureOpen();
            initiateConnection(node, finalProfile, listener);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /**
     * 初始化连接对象
     * @param node
     * @param connectionProfile
     * @param listener   这个监听器要处理无法正常连接到某个node的逻辑 包含了因为版本号不兼容导致的失败
     * @return
     */
    private List<TcpChannel> initiateConnection(DiscoveryNode node, ConnectionProfile connectionProfile,
                                                ActionListener<Transport.Connection> listener) {
        // 代表要针对某个node建立多条channel 能够提高性能么
        int numConnections = connectionProfile.getNumConnections();
        assert numConnections > 0 : "A connection profile must be configured with at least one connection";

        final List<TcpChannel> channels = new ArrayList<>(numConnections);

        for (int i = 0; i < numConnections; ++i) {
            try {
                // 生成连接到目标节点的channel   因为使用了事件循环模型 所以这里不会阻塞
                TcpChannel channel = initiateChannel(node);
                logger.trace(() -> new ParameterizedMessage("Tcp transport channel opened: {}", channel));
                channels.add(channel);
            } catch (ConnectTransportException e) {
                CloseableChannel.closeChannels(channels, false);
                listener.onFailure(e);
                return channels;
            } catch (Exception e) {
                CloseableChannel.closeChannels(channels, false);
                listener.onFailure(new ConnectTransportException(node, "general node connection failure", e));
                return channels;
            }
        }

        // 此时已经为目标节点创建了多条channel了  但是连接不一定完成
        ChannelsConnectedListener channelsConnectedListener = new ChannelsConnectedListener(node, connectionProfile, channels,
            new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.GENERIC, listener, false));

        // 当连接完成时触发监听器
        for (TcpChannel channel : channels) {
            channel.addConnectListener(channelsConnectedListener);
        }

        // 设置一个超时任务 以异常形式触发listener
        TimeValue connectTimeout = connectionProfile.getConnectTimeout();
        threadPool.schedule(channelsConnectedListener::onTimeout, connectTimeout, ThreadPool.Names.GENERIC);
        return channels;
    }

    /**
     * 当前节点绑定的地址  BoundTransportAddress 对象内部包含了一组地址 TODO 为什么 怎么使用
     * @return
     */
    @Override
    public BoundTransportAddress boundAddress() {
        return this.boundAddress;
    }

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return unmodifiableMap(new HashMap<>(profileBoundAddresses));
    }

    @Override
    public List<String> getDefaultSeedAddresses() {
        List<String> local = new ArrayList<>();
        local.add("127.0.0.1");
        // check if v6 is supported, if so, v4 will also work via mapped addresses.
        if (NetworkUtils.SUPPORTS_V6) {
            local.add("[::1]"); // may get ports appended!
        }
        return local.stream()
            .flatMap(
                address -> Arrays.stream(defaultPortRange())
                    .limit(LIMIT_LOCAL_PORTS_COUNT)
                    .mapToObj(port -> address + ":" + port)
            )
            .collect(Collectors.toList());
    }

    /**
     * @param profileSettings
     */
    protected void bindServer(ProfileSettings profileSettings) {
        // Bind and start to accept incoming connections.
        InetAddress[] hostAddresses;
        // 获取允许绑定的一组地址
        List<String> profileBindHosts = profileSettings.bindHosts;
        try {
            // 将地址解析成 InetAddress
            hostAddresses = networkService.resolveBindHostAddresses(profileBindHosts.toArray(Strings.EMPTY_ARRAY));
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve host " + profileBindHosts, e);
        }
        if (logger.isDebugEnabled()) {
            String[] addresses = new String[hostAddresses.length];
            for (int i = 0; i < hostAddresses.length; i++) {
                addresses[i] = NetworkAddress.format(hostAddresses[i]);
            }
            logger.debug("binding server bootstrap to: {}", (Object) addresses);
        }

        assert hostAddresses.length > 0;

        List<InetSocketAddress> boundAddresses = new ArrayList<>();
        for (InetAddress hostAddress : hostAddresses) {
            // 这里绑定的端口是随机的  将结果设置到boundAddresses中
            boundAddresses.add(bindToPort(profileSettings.profileName, hostAddress, profileSettings.portOrRange));
        }

        // 将地址合并成 boundTransportAddress
        final BoundTransportAddress boundTransportAddress = createBoundTransportAddress(profileSettings, boundAddresses);

        if (profileSettings.isDefaultProfile) {
            this.boundAddress = boundTransportAddress;
        } else {
            profileBoundAddresses.put(profileSettings.profileName, boundTransportAddress);
        }
    }

    /**
     * 从给定的port范围中进行绑定 返回首个绑定成功的地址
     * @param name
     * @param hostAddress
     * @param port
     * @return
     */
    private InetSocketAddress bindToPort(final String name, final InetAddress hostAddress, String port) {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        closeLock.writeLock().lock();
        try {
            // No need for locking here since Lifecycle objects can't move from STARTED to INITIALIZED
            if (lifecycle.initialized() == false && lifecycle.started() == false) {
                throw new IllegalStateException("transport has been stopped");
            }
            boolean success = portsRange.iterate(portNumber -> {
                try {
                    TcpServerChannel channel = bind(name, new InetSocketAddress(hostAddress, portNumber));
                    serverChannels.computeIfAbsent(name, k -> new ArrayList<>()).add(channel);
                    boundSocket.set(channel.getLocalAddress());
                } catch (Exception e) {
                    // 重复绑定应该会在这里抛出异常
                    lastException.set(e);
                    return false;
                }
                return true;
            });
            if (!success) {
                throw new BindTransportException(
                    "Failed to bind to " + NetworkAddress.format(hostAddress, portsRange),
                    lastException.get()
                );
            }
        } finally {
            closeLock.writeLock().unlock();
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Bound profile [{}] to address {{}}", name, NetworkAddress.format(boundSocket.get()));
        }

        return boundSocket.get();
    }

    /**
     *
     * @param profileSettings    一些描述tcp配置的信息
     * @param boundAddresses   本地被绑定的地址
     * @return
     */
    private BoundTransportAddress createBoundTransportAddress(ProfileSettings profileSettings,
                                                              List<InetSocketAddress> boundAddresses) {
        String[] boundAddressesHostStrings = new String[boundAddresses.size()];
        TransportAddress[] transportBoundAddresses = new TransportAddress[boundAddresses.size()];
        for (int i = 0; i < boundAddresses.size(); i++) {
            InetSocketAddress boundAddress = boundAddresses.get(i);
            boundAddressesHostStrings[i] = boundAddress.getHostString();
            transportBoundAddresses[i] = new TransportAddress(boundAddress);
        }

        List<String> publishHosts = profileSettings.publishHosts;
        if (profileSettings.isDefaultProfile == false && publishHosts.isEmpty()) {
            publishHosts = Arrays.asList(boundAddressesHostStrings);
        }
        if (publishHosts.isEmpty()) {
            publishHosts = NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings);
        }

        final InetAddress publishInetAddress;
        try {
            // 选择其中某个地址对外暴露  默认情况下就是选择第一个地址 这里先不细看
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts.toArray(Strings.EMPTY_ARRAY));
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }

        // 生成对外暴露的端口
        final int publishPort = resolvePublishPort(profileSettings, boundAddresses, publishInetAddress);
        final TransportAddress publishAddress = new TransportAddress(new InetSocketAddress(publishInetAddress, publishPort));
        return new BoundTransportAddress(transportBoundAddresses, publishAddress);
    }

    /**
     * 获取绑定的端口
     * @param profileSettings
     * @param boundAddresses   当前绑定的地址
     * @param publishInetAddress  打算对外暴露的地址
     * @return
     */
    static int resolvePublishPort(ProfileSettings profileSettings, List<InetSocketAddress> boundAddresses,
                                  InetAddress publishInetAddress) {
        int publishPort = profileSettings.publishPort;

        // if port not explicitly provided, search for port of address in boundAddresses that matches publishInetAddress
        // 从匹配的 boundInetAddress 上找到端口
        if (publishPort < 0) {
            for (InetSocketAddress boundAddress : boundAddresses) {
                InetAddress boundInetAddress = boundAddress.getAddress();
                if (boundInetAddress.isAnyLocalAddress() || boundInetAddress.equals(publishInetAddress)) {
                    publishPort = boundAddress.getPort();
                    break;
                }
            }
        }

        // if no matching boundAddress found, check if there is a unique port for all bound addresses
        // 如果地址都不匹配 则返回一个所有总和值 这样能确保端口不重复
        if (publishPort < 0) {
            final IntSet ports = new IntHashSet();
            for (InetSocketAddress boundAddress : boundAddresses) {
                ports.add(boundAddress.getPort());
            }
            if (ports.size() == 1) {
                publishPort = ports.iterator().next().value;
            }
        }

        if (publishPort < 0) {
            String profileExplanation = profileSettings.isDefaultProfile ? "" : " for profile " + profileSettings.profileName;
            throw new BindTransportException("Failed to auto-resolve publish port" + profileExplanation + ", multiple bound addresses " +
                boundAddresses + " with distinct ports and none of them matched the publish address (" + publishInetAddress + "). " +
                "Please specify a unique port by setting " + TransportSettings.PORT.getKey() + " or " +
                TransportSettings.PUBLISH_PORT.getKey());
        }
        return publishPort;
    }

    @Override
    public TransportAddress[] addressesFromString(String address) throws UnknownHostException {
        return parse(address, defaultPortRange()[0]);
    }

    private int[] defaultPortRange() {
        return new PortsRange(
            settings.get(
                TransportSettings.PORT_PROFILE.getConcreteSettingForNamespace(TransportSettings.DEFAULT_PROFILE).getKey(),
                TransportSettings.PORT.get(settings)
            )
        ).ports();
    }

    // this code is a take on guava's HostAndPort, like a HostAndPortRange

    // pattern for validating ipv6 bracket addresses.
    // not perfect, but PortsRange should take care of any port range validation, not a regex
    private static final Pattern BRACKET_PATTERN = Pattern.compile("^\\[(.*:.*)\\](?::([\\d\\-]*))?$");

    /**
     * parse a hostname+port spec into its equivalent addresses
     * 生成一组地址
     */
    static TransportAddress[] parse(String hostPortString, int defaultPort) throws UnknownHostException {
        Objects.requireNonNull(hostPortString);
        String host;
        String portString = null;

        if (hostPortString.startsWith("[")) {
            // Parse a bracketed host, typically an IPv6 literal.
            Matcher matcher = BRACKET_PATTERN.matcher(hostPortString);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid bracketed host/port range: " + hostPortString);
            }
            host = matcher.group(1);
            portString = matcher.group(2);  // could be null
        } else {
            int colonPos = hostPortString.indexOf(':');
            if (colonPos >= 0 && hostPortString.indexOf(':', colonPos + 1) == -1) {
                // Exactly 1 colon.  Split into host:port.
                host = hostPortString.substring(0, colonPos);
                portString = hostPortString.substring(colonPos + 1);
            } else {
                // 0 or 2+ colons.  Bare hostname or IPv6 literal.
                host = hostPortString;
                // 2+ colons and not bracketed: exception
                if (colonPos >= 0) {
                    throw new IllegalArgumentException("IPv6 addresses must be bracketed: " + hostPortString);
                }
            }
        }

        int port;
        // if port isn't specified, fill with the default
        if (portString == null || portString.isEmpty()) {
            port = defaultPort;
        } else {
            port = Integer.parseInt(portString);
        }

        return Arrays.stream(InetAddress.getAllByName(host))
            .distinct()
            .map(address -> new TransportAddress(address, port))
            .toArray(TransportAddress[]::new);
    }

    @Override
    protected final void doClose() {
    }

    /**
     * 当传输层对象终止时触发
     */
    @Override
    protected final void doStop() {
        final CountDownLatch latch = new CountDownLatch(1);
        // make sure we run it on another thread than a possible IO handler thread
        assert threadPool.generic().isShutdown() == false : "Must stop transport before terminating underlying threadpool";
        threadPool.generic().execute(() -> {
            closeLock.writeLock().lock();
            try {
                keepAlive.close();

                // first stop to accept any incoming connections so nobody can connect to this transport
                for (Map.Entry<String, List<TcpServerChannel>> entry : serverChannels.entrySet()) {
                    String profile = entry.getKey();
                    List<TcpServerChannel> channels = entry.getValue();
                    ActionListener<Void> closeFailLogger = ActionListener.wrap(c -> {
                        },
                        e -> logger.warn(() -> new ParameterizedMessage("Error closing serverChannel for profile [{}]", profile), e));
                    channels.forEach(c -> c.addCloseListener(closeFailLogger));
                    CloseableChannel.closeChannels(channels, true);
                }
                serverChannels.clear();

                // close all of the incoming channels. The closeChannels method takes a list so we must convert the set.
                CloseableChannel.closeChannels(new ArrayList<>(acceptedChannels), true);
                acceptedChannels.clear();

                stopInternal();
            } finally {
                closeLock.writeLock().unlock();
                latch.countDown();
            }
        });

        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // ignore
        }
    }

    public void onException(TcpChannel channel, Exception e) {
        handleException(channel, e, lifecycle, outboundHandler);
    }

    // exposed for tests
    static void handleException(TcpChannel channel, Exception e, Lifecycle lifecycle, OutboundHandler outboundHandler) {
        if (!lifecycle.started()) {
            // just close and ignore - we are already stopped and just need to make sure we release all resources
            CloseableChannel.closeChannel(channel);
            return;
        }

        if (isCloseConnectionException(e)) {
            logger.debug(() -> new ParameterizedMessage(
                "close connection exception caught on transport layer [{}], disconnecting from relevant node", channel), e);
            // close the channel, which will cause a node to be disconnected if relevant
            CloseableChannel.closeChannel(channel);
        } else if (isConnectException(e)) {
            logger.debug(() -> new ParameterizedMessage("connect exception caught on transport layer [{}]", channel), e);
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            CloseableChannel.closeChannel(channel);
        } else if (e instanceof BindException) {
            logger.debug(() -> new ParameterizedMessage("bind exception caught on transport layer [{}]", channel), e);
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            CloseableChannel.closeChannel(channel);
        } else if (e instanceof CancelledKeyException) {
            logger.debug(() -> new ParameterizedMessage(
                "cancelled key exception caught on transport layer [{}], disconnecting from relevant node", channel), e);
            // close the channel as safe measure, which will cause a node to be disconnected if relevant
            CloseableChannel.closeChannel(channel);
        } else if (e instanceof HttpRequestOnTransportException) {
            // in case we are able to return data, serialize the exception content and sent it back to the client
            if (channel.isOpen()) {
                BytesArray message = new BytesArray(e.getMessage().getBytes(StandardCharsets.UTF_8));
                outboundHandler.sendBytes(channel, message, ActionListener.wrap(() -> CloseableChannel.closeChannel(channel)));
            }
        } else if (e instanceof StreamCorruptedException) {
            logger.warn(() -> new ParameterizedMessage("{}, [{}], closing connection", e.getMessage(), channel));
            CloseableChannel.closeChannel(channel);
        } else {
            logger.warn(() -> new ParameterizedMessage("exception caught on transport layer [{}], closing connection", channel), e);
            // close the channel, which will cause a node to be disconnected if relevant
            CloseableChannel.closeChannel(channel);
        }
    }

    protected void onServerException(TcpServerChannel channel, Exception e) {
        if (e instanceof BindException) {
            logger.debug(() -> new ParameterizedMessage("bind exception from server channel caught on transport layer [{}]", channel), e);
        } else {
            logger.error(new ParameterizedMessage("exception from server channel caught on transport layer [{}]", channel), e);
        }
    }

    /**
     * 代表获取到一个新的连接到本机的客户端管道
     * @param channel
     */
    protected void serverAcceptedChannel(TcpChannel channel) {
        boolean addedOnThisCall = acceptedChannels.add(channel);
        assert addedOnThisCall : "Channel should only be added to accepted channel set once";
        // Mark the channel init time
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        channel.addCloseListener(ActionListener.wrap(() -> acceptedChannels.remove(channel)));
        logger.trace(() -> new ParameterizedMessage("Tcp transport channel accepted: {}", channel));
    }

    /**
     * Binds to the given {@link InetSocketAddress}
     *
     * @param name    the profile name
     * @param address the address to bind to
     */
    protected abstract TcpServerChannel bind(String name, InetSocketAddress address) throws IOException;

    /**
     * Initiate a single tcp socket channel.
     *
     * @param node for the initiated connection
     * @return the pending connection
     * @throws IOException if an I/O exception occurs while opening the channel
     */
    protected abstract TcpChannel initiateChannel(DiscoveryNode node) throws IOException;

    /**
     * Called to tear down internal resources
     */
    protected abstract void stopInternal();

    /**
     * Handles inbound message that has been decoded.
     *
     * @param channel the channel the message is from
     * @param message the message
     *                处理一条新接收的消息 这条消息已经做过解压和粘包处理了
     */
    public void inboundMessage(TcpChannel channel, InboundMessage message) {
        try {
            inboundHandler.inboundMessage(channel, message);
        } catch (Exception e) {
            onException(channel, e);
        }
    }

    /**
     * Validates the first 6 bytes of the message header and returns the length of the message. If 6 bytes
     * are not available, it returns -1.
     *
     * @param networkBytes the will be read
     * @return the length of the message
     * @throws StreamCorruptedException              if the message header format is not recognized
     * @throws HttpRequestOnTransportException       if the message header appears to be an HTTP message
     * @throws IllegalArgumentException              if the message length is greater that the maximum allowed frame size.
     *                                               This is dependent on the available memory.
     */
    public static int readMessageLength(BytesReference networkBytes) throws IOException {
        if (networkBytes.length() < BYTES_NEEDED_FOR_MESSAGE_SIZE) {
            return -1;
        } else {
            return readHeaderBuffer(networkBytes);
        }
    }

    private static int readHeaderBuffer(BytesReference headerBuffer) throws IOException {
        // 代表不是基于ES内部消息协议   可能是基于http协议
        if (headerBuffer.get(0) != 'E' || headerBuffer.get(1) != 'S') {
            // 不允许出现Http协议要求的请求报文格式
            if (appearsToBeHTTPRequest(headerBuffer)) {
                throw new HttpRequestOnTransportException("This is not an HTTP port");
            }

            if (appearsToBeHTTPResponse(headerBuffer)) {
                throw new StreamCorruptedException("received HTTP response on transport port, ensure that transport port (not " +
                        "HTTP port) of a remote node is specified in the configuration");
            }

            String firstBytes = "("
                    + Integer.toHexString(headerBuffer.get(0) & 0xFF) + ","
                    + Integer.toHexString(headerBuffer.get(1) & 0xFF) + ","
                    + Integer.toHexString(headerBuffer.get(2) & 0xFF) + ","
                    + Integer.toHexString(headerBuffer.get(3) & 0xFF) + ")";

            // 忽略加密报文
            if (appearsToBeTLS(headerBuffer)) {
                throw new StreamCorruptedException("SSL/TLS request received but SSL/TLS is not enabled on this node, got " + firstBytes);
            }

            throw new StreamCorruptedException("invalid internal transport message format, got " + firstBytes);
        }
        // 获取消息长度
        final int messageLength = headerBuffer.getInt(TcpHeader.MARKER_BYTES_SIZE);

        // 代表本次是一个心跳包
        if (messageLength == TransportKeepAlive.PING_DATA_SIZE) {
            // This is a ping
            return 0;
        }

        if (messageLength <= 0) {
            throw new StreamCorruptedException("invalid data length: " + messageLength);
        }

        if (messageLength > THIRTY_PER_HEAP_SIZE) {
            throw new IllegalArgumentException("transport content length received [" + new ByteSizeValue(messageLength) + "] exceeded ["
                + new ByteSizeValue(THIRTY_PER_HEAP_SIZE) + "]");
        }

        return messageLength;
    }

    private static boolean appearsToBeHTTPRequest(BytesReference headerBuffer) {
        return bufferStartsWith(headerBuffer, "GET")
            || bufferStartsWith(headerBuffer, "POST")
            || bufferStartsWith(headerBuffer, "PUT")
            || bufferStartsWith(headerBuffer, "HEAD")
            || bufferStartsWith(headerBuffer, "DELETE")
            // Actually 'OPTIONS'. But we are only guaranteed to have read six bytes at this point.
            || bufferStartsWith(headerBuffer, "OPTION")
            || bufferStartsWith(headerBuffer, "PATCH")
            || bufferStartsWith(headerBuffer, "TRACE");
    }

    private static boolean appearsToBeHTTPResponse(BytesReference headerBuffer) {
        return bufferStartsWith(headerBuffer, "HTTP");
    }

    private static boolean appearsToBeTLS(BytesReference headerBuffer) {
        return headerBuffer.get(0) == 0x16 && headerBuffer.get(1) == 0x03;
    }

    private static boolean bufferStartsWith(BytesReference buffer, String method) {
        char[] chars = method.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (buffer.get(i) != chars[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * A helper exception to mark an incoming connection as potentially being HTTP
     * so an appropriate error code can be returned
     */
    public static class HttpRequestOnTransportException extends ElasticsearchException {

        HttpRequestOnTransportException(String msg) {
            super(msg);
        }

        @Override
        public RestStatus status() {
            return RestStatus.BAD_REQUEST;
        }

        public HttpRequestOnTransportException(StreamInput in) throws IOException {
            super(in);
        }
    }

    /**
     * 发送一个探测请求
     * @param node
     * @param channel
     * @param profile
     * @param listener
     */
    public void executeHandshake(DiscoveryNode node, TcpChannel channel, ConnectionProfile profile, ActionListener<Version> listener) {
        // 为本次请求生成一个唯一id
        long requestId = responseHandlers.newRequestId();
        // 发送一个探测请求  并使用listener 处理结果
        handshaker.sendHandshake(requestId, node, channel, profile.getHandshakeTimeout(), listener);
    }

    final TransportKeepAlive getKeepAlive() {
        return keepAlive;
    }

    final int getNumPendingHandshakes() {
        return handshaker.getNumPendingHandshakes();
    }

    final long getNumHandshakes() {
        return handshaker.getNumHandshakes();
    }

    final Set<TcpChannel> getAcceptedChannels() {
        return Collections.unmodifiableSet(acceptedChannels);
    }

    /**
     * Ensures this transport is still started / open
     *
     * @throws IllegalStateException if the transport is not started / open
     */
    private void ensureOpen() {
        if (lifecycle.started() == false) {
            throw new IllegalStateException("transport has been stopped");
        }
    }

    /**
     * 获取当前传输层的使用状态
     * @return
     */
    @Override
    public final TransportStats getStats() {
        final MeanMetric writeBytesMetric = statsTracker.getWriteBytes();
        final long bytesWritten = statsTracker.getBytesWritten();
        final long messagesSent = statsTracker.getMessagesSent();
        final long messagesReceived = statsTracker.getMessagesReceived();
        final long bytesRead = statsTracker.getBytesRead();
        return new TransportStats(acceptedChannels.size(), messagesReceived, bytesRead, messagesSent, bytesWritten);
    }

    /**
     * Returns all profile settings for the given settings object
     * 从配置对象中抽取有关传输层描述信息的配置
     */
    public static Set<ProfileSettings> getProfileSettings(Settings settings) {
        HashSet<ProfileSettings> profiles = new HashSet<>();
        boolean isDefaultSet = false;
        for (String profile : settings.getGroups("transport.profiles.", true).keySet()) {
            profiles.add(new ProfileSettings(settings, profile));
            if (TransportSettings.DEFAULT_PROFILE.equals(profile)) {
                isDefaultSet = true;
            }
        }
        if (isDefaultSet == false) {
            profiles.add(new ProfileSettings(settings, TransportSettings.DEFAULT_PROFILE));
        }
        return Collections.unmodifiableSet(profiles);
    }

    /**
     * Representation of a transport profile settings for a {@code transport.profiles.$profilename.*}
     * 描述传输层配置的对象
     */
    public static final class ProfileSettings {
        public final String profileName;
        public final boolean tcpNoDelay;
        public final boolean tcpKeepAlive;
        public final int tcpKeepIdle;
        public final int tcpKeepInterval;
        public final int tcpKeepCount;
        public final boolean reuseAddress;
        public final ByteSizeValue sendBufferSize;
        public final ByteSizeValue receiveBufferSize;
        public final List<String> bindHosts;
        public final List<String> publishHosts;
        public final String portOrRange;
        public final int publishPort;
        public final boolean isDefaultProfile;

        public ProfileSettings(Settings settings, String profileName) {
            this.profileName = profileName;
            isDefaultProfile = TransportSettings.DEFAULT_PROFILE.equals(profileName);
            tcpKeepAlive = TransportSettings.TCP_KEEP_ALIVE_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            tcpKeepIdle = TransportSettings.TCP_KEEP_IDLE_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            tcpKeepInterval = TransportSettings.TCP_KEEP_INTERVAL_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            tcpKeepCount = TransportSettings.TCP_KEEP_COUNT_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            tcpNoDelay = TransportSettings.TCP_NO_DELAY_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            reuseAddress = TransportSettings.TCP_REUSE_ADDRESS_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            sendBufferSize = TransportSettings.TCP_SEND_BUFFER_SIZE_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            receiveBufferSize = TransportSettings.TCP_RECEIVE_BUFFER_SIZE_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            List<String> profileBindHosts = TransportSettings.BIND_HOST_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            bindHosts = (profileBindHosts.isEmpty() ? NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.get(settings) : profileBindHosts);
            publishHosts = TransportSettings.PUBLISH_HOST_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            Setting<String> concretePort = TransportSettings.PORT_PROFILE.getConcreteSettingForNamespace(profileName);
            if (concretePort.exists(settings) == false && isDefaultProfile == false) {
                throw new IllegalStateException("profile [" + profileName + "] has no port configured");
            }
            portOrRange = TransportSettings.PORT_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
            publishPort = isDefaultProfile ? TransportSettings.PUBLISH_PORT.get(settings) :
                TransportSettings.PUBLISH_PORT_PROFILE.getConcreteSettingForNamespace(profileName).get(settings);
        }
    }

    @Override
    public final ResponseHandlers getResponseHandlers() {
        return responseHandlers;
    }

    @Override
    public final RequestHandlers getRequestHandlers() {
        return requestHandlers;
    }

    /**
     * 当channel完成连接时触发 因为调用 serverChannel.connect 并不代表连接已经建立
     */
    private final class ChannelsConnectedListener implements ActionListener<Void> {

        private final DiscoveryNode node;
        private final ConnectionProfile connectionProfile;
        private final List<TcpChannel> channels;
        /**
         * 实际上监听器的逻辑包含在该对象中
         */
        private final ActionListener<Transport.Connection> listener;
        private final CountDown countDown;

        private ChannelsConnectedListener(DiscoveryNode node, ConnectionProfile connectionProfile, List<TcpChannel> channels,
                                          ActionListener<Transport.Connection> listener) {
            this.node = node;
            this.connectionProfile = connectionProfile;
            this.channels = channels;
            this.listener = listener;
            this.countDown = new CountDown(channels.size());
        }

        /**
         * 每当某个channel 成功连接时触发
         * @param v
         */
        @Override
        public void onResponse(Void v) {
            // Returns true if all connections have completed successfully
            // 返回ture 代表本次使得闭锁被打开  之后继续调用该方法都是返回false    也就是仅有所有达到要求的channel数量连接完成时才发起握手请求 检测节点间的兼容性
            if (countDown.countDown()) {
                final TcpChannel handshakeChannel = channels.get(0);
                try {
                    // 发送探测信息 检测2个节点间能否兼容  避免某些请求无法处理的情况
                    executeHandshake(node, handshakeChannel, connectionProfile,
                        // 定义了如何处理对端的版本号   当不兼容时会以失败形式触发
                        ActionListener.wrap(version -> {
                            // 将相关信息包装成 NodeChannels
                        NodeChannels nodeChannels = new NodeChannels(node, channels, connectionProfile, version);
                        long relativeMillisTime = threadPool.relativeTimeInMillis();
                        nodeChannels.channels.forEach(ch -> {
                            // Mark the channel init time
                            ch.getChannelStats().markAccessed(relativeMillisTime);
                            ch.addCloseListener(ActionListener.wrap(nodeChannels::close));
                        });
                        keepAlive.registerNodeConnection(nodeChannels.channels, connectionProfile);
                        listener.onResponse(nodeChannels);
                        // 代表版本号不兼容  关闭所有连接
                    }, e -> closeAndFail(e instanceof ConnectTransportException ?
                        e : new ConnectTransportException(node, "general node connection failure", e))));
                } catch (Exception ex) {
                    closeAndFail(ex);
                }
            }
        }

        @Override
        public void onFailure(Exception ex) {
            if (countDown.fastForward()) {
                closeAndFail(new ConnectTransportException(node, "connect_exception", ex));
            }
        }

        /**
         * 代表本次连接超时
         */
        public void onTimeout() {
            // 代表闭锁是由 大于0的值直接变成0的 也就是某些channel的连接还没有完成
            if (countDown.fastForward()) {
                closeAndFail(new ConnectTransportException(node, "connect_timeout[" + connectionProfile.getConnectTimeout() + "]"));
            }
        }

        private void closeAndFail(Exception e) {
            try {
                // 关闭所有channel 并以失败形式触发监听器
                CloseableChannel.closeChannels(channels, false);
            } catch (Exception ex) {
                e.addSuppressed(ex);
            } finally {
                listener.onFailure(e);
            }
        }
    }
}
