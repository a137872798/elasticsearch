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
import java.nio.channels.NetworkChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Implements the logic related to interacting with a java.nio channel. For example: registering with a
 * selector, managing the selection key, closing, etc is implemented by this class or its subclasses.
 *
 * @param <S> the type of channel
 *           有关本次通道绑定的上下文信息
 */
public abstract class ChannelContext<S extends SelectableChannel & NetworkChannel> {

    /**
     * 通道对象本身
     */
    protected final S rawChannel;
    /**
     * 该对象负责处理异常
     */
    private final Consumer<Exception> exceptionHandler;
    /**
     * 该对象就是在future对象上绑定一个监听器 当future对象生成结果时触发
     */
    private final CompletableContext<Void> closeContext = new CompletableContext<>();

    /**
     * nio的选择键
     */
    private volatile SelectionKey selectionKey;

    ChannelContext(S rawChannel, Consumer<Exception> exceptionHandler) {
        this.rawChannel = rawChannel;
        this.exceptionHandler = exceptionHandler;
    }

    protected void register() throws IOException {
        doSelectorRegister();
    }

    /**
     * 当channel被激活时触发
     * @throws IOException
     */
    protected void channelActive() throws IOException {}

    // Package private for testing
    void doSelectorRegister() throws IOException {
        // 这里做了一层适配  通过rawSelector 从es封装的selector中剥离出最基础的选择器  同时将它注册到未封装的channel上
        setSelectionKey(rawChannel.register(getSelector().rawSelector(), 0, this));
    }

    protected SelectionKey getSelectionKey() {
        return selectionKey;
    }

    // public for tests
    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    /**
     * This method cleans up any context resources that need to be released when a channel is closed. It
     * should only be called by the selector thread.
     *
     * @throws IOException during channel / context close
     * 通过选择器关闭该上下文 同时为future对象设置结果 这样还会触发context上绑定的监听器
     */
    public void closeFromSelector() throws IOException {
        if (isOpen()) {
            try {
                rawChannel.close();
                closeContext.complete(null);
            } catch (Exception e) {
                closeContext.completeExceptionally(e);
                throw e;
            }
        }
    }

    /**
     * Add a listener that will be called when the channel is closed.
     *
     * @param listener to be called
     */
    public void addCloseListener(BiConsumer<Void, Exception> listener) {
        closeContext.addListener(listener);
    }

    public boolean isOpen() {
        return closeContext.isDone() == false;
    }

    protected void handleException(Exception e) {
        exceptionHandler.accept(e);
    }

    /**
     * Schedules a channel to be closed by the selector event loop with which it is registered.
     *
     * If the channel is open and the state can be transitioned to closed, the close operation will
     * be scheduled with the event loop.
     *
     * Depending on the underlying protocol of the channel, a close operation might simply close the socket
     * channel or may involve reading and writing messages.
     */
    public abstract void closeChannel();

    public abstract NioSelector getSelector();

    public abstract NioChannel getChannel();

}
