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

import org.elasticsearch.nio.utils.ExceptionsHelper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The NioSelectorGroup is a group of selectors for interfacing with java nio. When it is started it will create the
 * configured number of selectors. Each selector will be running in a dedicated thread. Server connections
 * can be bound using the {@link #bindServerChannel(InetSocketAddress, ChannelFactory)} method. Client
 * connections can be opened using the {@link #openChannel(InetSocketAddress, ChannelFactory)} method.
 * <p>
 * The logic specific to a particular channel is provided by the {@link ChannelFactory} passed to the method
 * when the channel is created. This is what allows an NioSelectorGroup to support different channel types.
 * 模仿netty的事件循环组
 */
public class NioSelectorGroup implements NioGroup {

    /**
     * 专门负责接受外部连接的选择器   通过将连接与读写解耦 确保读写紧张时不会影响新建连接 也就是reactor模型
     */
    private final List<NioSelector> dedicatedAcceptors;
    /**
     * 该对象以轮询的方式每次随机获取一个选择器  netty中也有类似的接口 基于特殊的策略返回一个事件循环对象
     */
    private final RoundRobinSupplier<NioSelector> acceptorSupplier;

    // 只负责处理读写请求的选择器
    private final List<NioSelector> selectors;

    /**
     * 内部是一个 Selector[] 但是调用get()每次只会返回一个selector
     */
    private final RoundRobinSupplier<NioSelector> selectorSupplier;

    private final AtomicBoolean isOpen = new AtomicBoolean(true);

    /**
     * This will create an NioSelectorGroup with no dedicated acceptors. All server channels will be handled by the
     * same selectors that are handling child channels.
     *
     * @param threadFactory factory to create selector threads
     * @param selectorCount the number of selectors to be created  事件循环组将会有多少条线程
     * @param eventHandlerFunction function for creating event handlers  事件处理器
     * @throws IOException occurs if there is a problem while opening a java.nio.Selector
     */
    public NioSelectorGroup(ThreadFactory threadFactory, int selectorCount,
                            Function<Supplier<NioSelector>, EventHandler> eventHandlerFunction) throws IOException {
        this(null, 0, threadFactory, selectorCount, eventHandlerFunction);
    }

    /**
     * This will create an NioSelectorGroup with dedicated acceptors. All server channels will be handled by a group
     * of selectors dedicated to accepting channels. These accepted channels will be handed off the
     * non-server selectors.
     *
     * @param acceptorThreadFactory factory to create acceptor selector threads  默认情况下不设置
     * @param dedicatedAcceptorCount the number of dedicated acceptor selectors to be created  默认情况下不设置
     * @param selectorThreadFactory factory to create non-acceptor selector threads
     * @param selectorCount the number of non-acceptor selectors to be created
     * @param eventHandlerFunction function for creating event handlers
     * @throws IOException occurs if there is a problem while opening a java.nio.Selector
     */
    public NioSelectorGroup(ThreadFactory acceptorThreadFactory, int dedicatedAcceptorCount, ThreadFactory selectorThreadFactory,
                            int selectorCount, Function<Supplier<NioSelector>, EventHandler> eventHandlerFunction) throws IOException {

        // 分别生成 处理连接请求/处理读写请求的选择器组
        dedicatedAcceptors = new ArrayList<>(dedicatedAcceptorCount);
        selectors = new ArrayList<>(selectorCount);

        try {
            // 这容器是干嘛用的???
            List<RoundRobinSupplier<NioSelector>> suppliersToSet = new ArrayList<>(selectorCount);
            for (int i = 0; i < selectorCount; ++i) {
                RoundRobinSupplier<NioSelector> supplier = new RoundRobinSupplier<>();
                suppliersToSet.add(supplier);
                // 通过 eventHandler对象初始化 selector   事件处理器定义了如何处理选择器检测到的IO事件
                NioSelector selector = new NioSelector(eventHandlerFunction.apply(supplier));
                selectors.add(selector);
            }
            // 内部每个RoundRobinSupplier 使用的 selector[] 都是一样的
            for (RoundRobinSupplier<NioSelector> supplierToSet : suppliersToSet) {
                supplierToSet.setSelectors(selectors.toArray(new NioSelector[0]));
                assert supplierToSet.count() == selectors.size() : "Supplier should have same count as selector list.";
            }

            // TODO 忽略 accept相关的
            for (int i = 0; i < dedicatedAcceptorCount; ++i) {
                RoundRobinSupplier<NioSelector> supplier = new RoundRobinSupplier<>(selectors.toArray(new NioSelector[0]));
                NioSelector acceptor = new NioSelector(eventHandlerFunction.apply(supplier));
                dedicatedAcceptors.add(acceptor);
            }

            if (dedicatedAcceptorCount != 0) {
                acceptorSupplier = new RoundRobinSupplier<>(dedicatedAcceptors.toArray(new NioSelector[0]));
            } else {
                // 当没有指定 acceptor线程组时与普通selector共用
                acceptorSupplier = new RoundRobinSupplier<>(selectors.toArray(new NioSelector[0]));
            }
            selectorSupplier = new RoundRobinSupplier<>(selectors.toArray(new NioSelector[0]));
            assert selectorCount == selectors.size() : "We need to have created all the selectors at this point.";
            assert dedicatedAcceptorCount == dedicatedAcceptors.size() : "We need to have created all the acceptors at this point.";

            startSelectors(selectors, selectorThreadFactory);
            startSelectors(dedicatedAcceptors, acceptorThreadFactory);
        } catch (Exception e) {
            try {
                close();
            } catch (Exception e1) {
                e.addSuppressed(e1);
            }
            throw e;
        }
    }

    @Override
    public <S extends NioServerSocketChannel> S bindServerChannel(InetSocketAddress address, ChannelFactory<S, ?> factory)
        throws IOException {
        ensureOpen();
        // 将 serverChannel 与selector合并
        return factory.openNioServerSocketChannel(address, acceptorSupplier);
    }

    /**
     * 如果创建的是客户端channel 是直接绑定到 selectorSupplier上的 与acceptor做隔离
     * @param address
     * @param factory
     * @param <S>
     * @return
     * @throws IOException
     */
    @Override
    public <S extends NioSocketChannel> S openChannel(InetSocketAddress address, ChannelFactory<?, S> factory) throws IOException {
        ensureOpen();
        return factory.openNioChannel(address, selectorSupplier);
    }

    @Override
    public void close() throws IOException {
        if (isOpen.compareAndSet(true, false)) {
            List<NioSelector> toClose = Stream.concat(dedicatedAcceptors.stream(), selectors.stream()).collect(Collectors.toList());
            List<IOException> closingExceptions = new ArrayList<>();
            for (NioSelector selector : toClose) {
                try {
                    selector.close();
                } catch (IOException e) {
                    closingExceptions.add(e);
                }
            }
            ExceptionsHelper.rethrowAndSuppress(closingExceptions);
        }
    }

    /**
     * 启动所有的选择器
     * @param selectors
     * @param threadFactory
     */
    private static void startSelectors(Iterable<NioSelector> selectors, ThreadFactory threadFactory) {
        for (NioSelector selector : selectors) {
            if (selector.isRunning() == false) {
                // 每个事件循环 用专门的IO线程去执行
                threadFactory.newThread(selector::runLoop).start();
                try {
                    selector.isRunningFuture().get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for selector to start.", e);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof RuntimeException) {
                        throw (RuntimeException) e.getCause();
                    } else {
                        throw new RuntimeException("Exception during selector start.", e);
                    }
                }
            }
        }
    }

    private void ensureOpen() {
        if (isOpen.get() == false) {
            throw new IllegalStateException("NioGroup is closed.");
        }
    }
}
