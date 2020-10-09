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

package org.elasticsearch.nio;

import org.elasticsearch.common.concurrent.CompletableContext;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 对应服务端channel的上下文对象 每当新生成一个连接 就会产生一个新的 ChannelContext (客户端channel)
 */
public class ServerChannelContext extends ChannelContext<ServerSocketChannel> {

    /**
     * 该上下文绑定的服务端通道
     */
    private final NioServerSocketChannel channel;
    /**
     * 该对象定义了是事件循环的逻辑
     */
    private final NioSelector selector;
    /**
     * 配置信息
     */
    private final Config.ServerSocket config;
    /**
     * 定义了当接受到新的通道时该如何处理
     */
    private final Consumer<NioSocketChannel> acceptor;
    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final ChannelFactory<?, ?> channelFactory;
    /**
     * 监听绑定动作是否完成
     */
    private final CompletableContext<Void> bindContext = new CompletableContext<>();

    public ServerChannelContext(NioServerSocketChannel channel, ChannelFactory<?, ?> channelFactory, NioSelector selector,
                                Config.ServerSocket config, Consumer<NioSocketChannel> acceptor,
                                Consumer<Exception> exceptionHandler) {
        super(channel.getRawChannel(), exceptionHandler);
        this.channel = channel;
        this.channelFactory = channelFactory;
        this.selector = selector;
        this.config = config;
        this.acceptor = acceptor;
    }

    public void acceptChannels(Supplier<NioSelector> selectorSupplier) throws IOException {
        SocketChannel acceptedChannel;
        // 这里不断循环 直到接收到新的客户端
        while ((acceptedChannel = accept(rawChannel)) != null) {
            // 封装成ESchannel
            NioSocketChannel nioChannel = channelFactory.acceptNioChannel(acceptedChannel, selectorSupplier);
            acceptor.accept(nioChannel);
        }
    }

    public void addBindListener(BiConsumer<Void, Exception> listener) {
        bindContext.addListener(listener);
    }

    @Override
    protected void register() throws IOException {
        super.register();

        configureSocket(rawChannel.socket());

        InetSocketAddress localAddress = config.getLocalAddress();
        try {
            rawChannel.bind(localAddress);
            bindContext.complete(null);
        } catch (IOException e) {
            BindException exception = new BindException("Failed to bind server socket channel {localAddress=" + localAddress + "}.");
            exception.initCause(e);
            bindContext.completeExceptionally(exception);
            throw exception;
        }
    }

    @Override
    public void closeChannel() {
        if (isClosing.compareAndSet(false, true)) {
            getSelector().queueChannelClose(channel);
        }
    }

    @Override
    public NioSelector getSelector() {
        return selector;
    }

    @Override
    public NioServerSocketChannel getChannel() {
        return channel;
    }

    private void configureSocket(ServerSocket socket) throws IOException {
        socket.setReuseAddress(config.tcpReuseAddress());
    }

    /**
     * 处于非阻塞模式 调用accept 并立即返回
     * @param serverSocketChannel
     * @return
     * @throws IOException
     */
    protected static SocketChannel accept(ServerSocketChannel serverSocketChannel) throws IOException {
        try {
            assert serverSocketChannel.isBlocking() == false;
            SocketChannel channel = AccessController.doPrivileged((PrivilegedExceptionAction<SocketChannel>) serverSocketChannel::accept);
            assert serverSocketChannel.isBlocking() == false;
            return channel;
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }
}
