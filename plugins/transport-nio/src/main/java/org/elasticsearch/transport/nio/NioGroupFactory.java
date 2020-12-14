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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.EventHandler;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.nio.NioSelectorGroup;
import org.elasticsearch.nio.NioServerSocketChannel;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.transport.TcpTransport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * Creates and returns {@link NioSelectorGroup} instances. It will return a shared group for
 * both {@link #getHttpGroup()} and {@link #getTransportGroup()} if
 * {@link NioTransportPlugin#NIO_HTTP_WORKER_COUNT} is configured to be 0. If that setting is not 0, then it
 * will return a different group in the {@link #getHttpGroup()} call.
 * 生成 循环组的工厂
 */
public final class NioGroupFactory {

    private final Logger logger;
    private final Settings settings;
    /**
     * worker数量 (事件循环线程数量)
     */
    private final int httpWorkerCount;

    private RefCountedNioGroup refCountedGroup;

    /**
     *
     * @param settings 内部包含相关配置
     * @param logger
     */
    public NioGroupFactory(Settings settings, Logger logger) {
        this.logger = logger;
        this.settings = settings;
        this.httpWorkerCount = NioTransportPlugin.NIO_HTTP_WORKER_COUNT.get(settings);
    }

    public Settings getSettings() {
        return settings;
    }

    public synchronized NioGroup getTransportGroup() throws IOException {
        return getGenericGroup();
    }

    public synchronized NioGroup getHttpGroup() throws IOException {
        if (httpWorkerCount == 0) {
            return getGenericGroup();
        } else {
            return new NioSelectorGroup(daemonThreadFactory(this.settings, HttpServerTransport.HTTP_SERVER_WORKER_THREAD_NAME_PREFIX),
                httpWorkerCount, (s) -> new EventHandler(this::onException, s));
        }
    }

    /**
     * 生成事件循环组
     * @return
     * @throws IOException
     */
    private NioGroup getGenericGroup() throws IOException {
        if (refCountedGroup == null) {
            // 获取线程工程  该工厂创建的线程都是守护线程
            ThreadFactory threadFactory = daemonThreadFactory(this.settings, TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX);
            NioSelectorGroup nioGroup = new NioSelectorGroup(threadFactory, NioTransportPlugin.NIO_WORKER_COUNT.get(settings),
                // 当selector感知到事件后 交由该对象进行处理
                (s) -> new EventHandler(this::onException, s));
            this.refCountedGroup = new RefCountedNioGroup(nioGroup);
            return new WrappedNioGroup(refCountedGroup);
        } else {
            // 当重复调用该方法时 不会创建更多的线程组 而是复用之前的 因为受到底层IO的限制 所以提高线程数并不能提高性能
            refCountedGroup.incRef();
            return new WrappedNioGroup(refCountedGroup);
        }
    }

    private void onException(Exception exception) {
        logger.warn(new ParameterizedMessage("exception caught on transport layer [thread={}]", Thread.currentThread().getName()),
            exception);
    }

    /**
     * 有关group的引用计数 当归0时关闭对象
     */
    private static class RefCountedNioGroup extends AbstractRefCounted implements NioGroup {

        public static final String NAME = "ref-counted-nio-group";

        /**
         * 内部是事件循环组  模仿netty的 EventLoopGroup 每个线程都处理任务队列中的任务 同时在空闲事件select准备好的事件
         */
        private final NioSelectorGroup nioGroup;

        private RefCountedNioGroup(NioSelectorGroup nioGroup) {
            super(NAME);
            this.nioGroup = nioGroup;
        }

        @Override
        protected void closeInternal() {
            try {
                nioGroup.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public <S extends NioServerSocketChannel> S bindServerChannel(InetSocketAddress address, ChannelFactory<S, ?> factory)
            throws IOException {
            return nioGroup.bindServerChannel(address, factory);
        }

        @Override
        public <S extends NioSocketChannel> S openChannel(InetSocketAddress address, ChannelFactory<?, S> factory) throws IOException {
            return nioGroup.openChannel(address, factory);
        }

        @Override
        public void close() throws IOException {
            throw new UnsupportedOperationException("Should not close. Instead use decRef call.");
        }
    }

    /**
     * Wraps the {@link RefCountedNioGroup}. Calls {@link RefCountedNioGroup#decRef()} on close. After close,
     * this wrapped instance can no longer be used.
     */
    private static class WrappedNioGroup implements NioGroup {

        private final RefCountedNioGroup refCountedNioGroup;

        private final AtomicBoolean isOpen = new AtomicBoolean(true);

        private WrappedNioGroup(RefCountedNioGroup refCountedNioGroup) {
            this.refCountedNioGroup = refCountedNioGroup;
        }

        public <S extends NioServerSocketChannel> S bindServerChannel(InetSocketAddress address, ChannelFactory<S, ?> factory)
            throws IOException {
            ensureOpen();
            return refCountedNioGroup.bindServerChannel(address, factory);
        }

        public <S extends NioSocketChannel> S openChannel(InetSocketAddress address, ChannelFactory<?, S> factory) throws IOException {
            ensureOpen();
            return refCountedNioGroup.openChannel(address, factory);
        }

        @Override
        public void close() {
            if (isOpen.compareAndSet(true, false)) {
                refCountedNioGroup.decRef();
            }
        }

        private void ensureOpen() {
            if (isOpen.get() == false) {
                throw new IllegalStateException("NioGroup is closed.");
            }
        }
    }
}
