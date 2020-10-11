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
import org.elasticsearch.core.internal.net.NetUtils;
import org.elasticsearch.nio.utils.ByteBufferUtils;
import org.elasticsearch.nio.utils.ExceptionsHelper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This context should implement the specific logic for a channel. When a channel receives a notification
 * that it is ready to perform certain operations (read, write, etc) the {@link SocketChannelContext} will
 * be called. This context will need to implement all protocol related logic. Additionally, if any special
 * close behavior is required, it should be implemented in this context.
 *
 * The only methods of the context that should ever be called from a non-selector thread are
 * {@link #closeChannel()} and {@link #sendMessage(Object, BiConsumer)}.
 * 父类只是定义了一套模板 子类实现了有关发送消息等的操作
 * 该对象好像一开始就组装了需要的所有参数 同时在使用前会调用register
 */
public abstract class SocketChannelContext extends ChannelContext<SocketChannel> {

    /**
     * ES 适配后的channel
     */
    protected final NioSocketChannel channel;
    /**
     * 该对象可以理解为一个对象池 内部存储了多个page(byteBuffer)
     */
    protected final InboundChannelBuffer channelBuffer;
    protected final AtomicBoolean isClosing = new AtomicBoolean(false);
    /**
     * 该对象负责处理收到的数据
     */
    private final NioChannelHandler channelHandler;
    /**
     * 该对象相关的channel 绑定在哪个选择器上
     */
    private final NioSelector selector;
    /**
     * 有关套接字的配置信息
     */
    private final Config.Socket socketConfig;
    /**
     * 当连接完成时 会设置该future的结果
     */
    private final CompletableContext<Void> connectContext = new CompletableContext<>();
    /**
     * 待处理的刷盘操作
     */
    private final LinkedList<FlushOperation> pendingFlushes = new LinkedList<>();
    private boolean closeNow;
    /**
     * 绑定的channel是否已经装配过socket
     */
    private boolean socketOptionsSet;
    private Exception connectException;

    protected SocketChannelContext(NioSocketChannel channel, NioSelector selector, Config.Socket socketConfig,
                                   Consumer<Exception> exceptionHandler, NioChannelHandler channelHandler,
                                   InboundChannelBuffer channelBuffer) {
        super(channel.getRawChannel(), exceptionHandler);
        this.selector = selector;
        this.channel = channel;
        this.socketConfig = socketConfig;
        this.channelHandler = channelHandler;
        this.channelBuffer = channelBuffer;
    }

    @Override
    public NioSelector getSelector() {
        return selector;
    }

    @Override
    public NioSocketChannel getChannel() {
        return channel;
    }

    @Override
    protected void register() throws IOException {
        // 完成nio的selector/channel注册
        super.register();

        // 将socket相关配置注册到 Socket对象上
        configureSocket(rawChannel.socket(), false);

        if (socketConfig.isAccepted() == false) {
            InetSocketAddress remoteAddress = socketConfig.getRemoteAddress();
            try {
                // 连接到目标地址
                connect(rawChannel, remoteAddress);
            } catch (IOException e) {
                throw new IOException("Failed to initiate socket channel connection {remoteAddress=" + remoteAddress + "}.", e);
            }
        }
    }

    public void addConnectListener(BiConsumer<Void, Exception> listener) {
        connectContext.addListener(listener);
    }

    public boolean isConnectComplete() {
        return connectContext.isDone() && connectContext.isCompletedExceptionally() == false;
    }

    /**
     * This method will attempt to complete the connection process for this channel. It should be called for
     * new channels or for a channel that has produced a OP_CONNECT event. If this method returns true then
     * the connection is complete and the channel is ready for reads and writes. If it returns false, the
     * channel is not yet connected and this method should be called again when a OP_CONNECT event is
     * received.
     *
     * @return true if the connection process is complete
     * @throws IOException if an I/O error occurs
     */
    public boolean connect() throws IOException {
        // 连接成功 忽略
        if (isConnectComplete()) {
            return true;
        // 连接出现异常时 抛出异常
        } else if (connectContext.isCompletedExceptionally()) {
            Exception exception = connectException;
            if (exception == null) {
                throw new AssertionError("Should have received connection exception");
            } else if (exception instanceof IOException) {
                throw (IOException) exception;
            } else {
                throw (RuntimeException) exception;
            }
        }

        boolean isConnected = rawChannel.isConnected();
        if (isConnected == false) {
            try {
                // 该方法会阻塞直到channel完成连接  在非阻塞模式下 如果连接还未完成会返回false
                isConnected = rawChannel.finishConnect();
            } catch (IOException | RuntimeException e) {
                connectException = e;
                connectContext.completeExceptionally(e);
                throw e;
            }
        }
        if (isConnected) {
            connectContext.complete(null);
            configureSocket(rawChannel.socket(), true);
        }
        return isConnected;
    }


    /**
     * 这里尽可能泛化了 发送消息的参数  这样更加灵活
     * @param message
     * @param listener
     */
    public void sendMessage(Object message, BiConsumer<Void, Exception> listener) {
        if (isClosing.get()) {
            listener.accept(null, new ClosedChannelException());
            return;
        }

        // 将消息体封装成一个Op对象 赋予业务意义
        WriteOperation writeOperation = channelHandler.createWriteOperation(this, message, listener);

        // 如果是在业务线程 那么会存储到选择器的队列中 如果是IO线程 会直接处理写操作
        getSelector().queueWrite(writeOperation);
    }

    /**
     * 将某个WriteOperation 对象先写入到handler中 并返回一个flush任务 添加到刷盘队列
     * @param writeOperation
     */
    public void queueWriteOperation(WriteOperation writeOperation) {
        getSelector().assertOnSelectorThread();
        pendingFlushes.addAll(channelHandler.writeToBytes(writeOperation));
    }

    public abstract int read() throws IOException;

    public abstract void flushChannel() throws IOException;

    protected void currentFlushOperationFailed(IOException e) {
        FlushOperation flushOperation = pendingFlushes.pollFirst();
        getSelector().executeFailedListener(flushOperation.getListener(), e);
    }

    protected void currentFlushOperationComplete() {
        FlushOperation flushOperation = pendingFlushes.pollFirst();
        getSelector().executeListener(flushOperation.getListener(), null);
    }

    protected FlushOperation getPendingFlush() {
        return pendingFlushes.peekFirst();
    }

    /**
     * 当连接被激活时 由handler进行处理
     * @throws IOException
     */
    @Override
    protected void channelActive() throws IOException {
        channelHandler.channelActive();
    }

    /**
     * 代表通过selector 关闭context
     * @throws IOException
     */
    @Override
    public void closeFromSelector() throws IOException {
        // 因为确保在单线程环境执行 所以不需要考虑并发问题
        getSelector().assertOnSelectorThread();
        if (isOpen()) {
            ArrayList<IOException> closingExceptions = new ArrayList<>(3);
            try {
                super.closeFromSelector();
            } catch (IOException e) {
                closingExceptions.add(e);
            }
            // Set to true in order to reject new writes before queuing with selector
            isClosing.set(true);

            // Poll for new flush operations to close
            // 以关闭异常触发所有监听器
            pendingFlushes.addAll(channelHandler.pollFlushOperations());
            FlushOperation flushOperation;
            while ((flushOperation = pendingFlushes.pollFirst()) != null) {
                selector.executeFailedListener(flushOperation.getListener(), new ClosedChannelException());
            }

            try {
                channelHandler.close();
            } catch (IOException e) {
                closingExceptions.add(e);
            }
            channelBuffer.close();

            if (closingExceptions.isEmpty() == false) {
                ExceptionsHelper.rethrowAndSuppress(closingExceptions);
            }
        }
    }

    /**
     * 处理读取到的数据
     * @throws IOException
     */
    protected void handleReadBytes() throws IOException {
        int bytesConsumed = Integer.MAX_VALUE;
        while (isOpen() && bytesConsumed > 0 && channelBuffer.getIndex() > 0) {
            bytesConsumed = channelHandler.consumeReads(channelBuffer);
            channelBuffer.release(bytesConsumed);
        }

        // Some protocols might produce messages to flush during a read operation.
        // 通过 channelHandler.consumeReads 处理读取到的数据 并在内部转换成flushOp 对象 之后在这里取出数据并追加到pendingFlushes
        pendingFlushes.addAll(channelHandler.pollFlushOperations());
    }

    /**
     * 此时是否有待处理的刷盘任务
     * @return
     */
    public boolean readyForFlush() {
        getSelector().assertOnSelectorThread();
        return pendingFlushes.isEmpty() == false;
    }

    /**
     * This method indicates if a selector should close this channel.
     *
     * @return a boolean indicating if the selector should close
     */
    public abstract boolean selectorShouldClose();

    protected boolean closeNow() {
        return closeNow || channelHandler.closeNow();
    }

    protected void setCloseNow() {
        closeNow = true;
    }

    // When you read or write to a nio socket in java, the heap memory passed down must be copied to/from
    // direct memory. The JVM internally does some buffering of the direct memory, however we can save space
    // by reusing a thread-local direct buffer (provided by the NioSelector).
    //
    // Each network event loop is given a 64kb DirectByteBuffer. When we read we use this buffer and copy the
    // data after the read. When we go to write, we copy the data to the direct memory before calling write.
    // The choice of 64KB is rather arbitrary. We can explore different sizes in the future. However, any
    // data that is copied to the buffer for a write, but not successfully flushed immediately, must be
    // copied again on the next call.
    // 从channel中读取数据 并存储到channelBuffer
    protected int readFromChannel(InboundChannelBuffer channelBuffer) throws IOException {
        ByteBuffer ioBuffer = getSelector().getIoBuffer();
        int bytesRead;
        try {
            bytesRead = rawChannel.read(ioBuffer);
        } catch (IOException e) {
            closeNow = true;
            throw e;
        }
        if (bytesRead < 0) {
            closeNow = true;
            return 0;
        } else {
            ioBuffer.flip();
            // 确保此时有足够的空间
            channelBuffer.ensureCapacity(channelBuffer.getIndex() + ioBuffer.remaining());
            // 这里获取到的就是 ioBuffer.remaining() 对应的空容器大小
            ByteBuffer[] buffers = channelBuffer.sliceBuffersFrom(channelBuffer.getIndex());
            int j = 0;
            while (j < buffers.length && ioBuffer.remaining() > 0) {
                ByteBuffer buffer = buffers[j++];
                ByteBufferUtils.copyBytes(ioBuffer, buffer);
            }
            // 增加已经使用的数据对应的下标
            channelBuffer.incrementIndex(bytesRead);
            return bytesRead;
        }
    }

    // Currently we limit to 64KB. This is a trade-off which means more syscalls, in exchange for less
    // copying.
    private static final int WRITE_LIMIT = 1 << 16;

    /**
     * 将flushOp 内部的所有数据写入到channel中
     * @param flushOperation
     * @return
     * @throws IOException
     */
    protected int flushToChannel(FlushOperation flushOperation) throws IOException {
        ByteBuffer ioBuffer = getSelector().getIoBuffer();

        // 代表该flushOp内部的数据还没有写完
        boolean continueFlush = flushOperation.isFullyFlushed() == false;
        // 记录本次总计写入了多少数据
        int totalBytesFlushed = 0;
        while (continueFlush) {
            ioBuffer.clear();
            ioBuffer.limit(Math.min(WRITE_LIMIT, ioBuffer.limit()));
            // 从未写入的 位置开始读取目标数量的buffer
            ByteBuffer[] buffers = flushOperation.getBuffersToWrite(WRITE_LIMIT);
            // 将数据转移到ioBuffer中后 写入到channel
            ByteBufferUtils.copyBytes(buffers, ioBuffer);
            ioBuffer.flip();
            int bytesFlushed;
            try {
                bytesFlushed = rawChannel.write(ioBuffer);
            } catch (IOException e) {
                closeNow = true;
                throw e;
            }
            flushOperation.incrementIndex(bytesFlushed);
            totalBytesFlushed += bytesFlushed;
            continueFlush = ioBuffer.hasRemaining() == false && flushOperation.isFullyFlushed() == false;
        }
        return totalBytesFlushed;
    }

    private void configureSocket(Socket socket, boolean isConnectComplete) throws IOException {
        if (socketOptionsSet) {
            return;
        }

        try {
            // Set reuse address first as it must be set before a bind call. Some implementations throw
            // exceptions on other socket options if the channel is not connected. But setting reuse first,
            // we ensure that it is properly set before any bind attempt.
            socket.setReuseAddress(socketConfig.tcpReuseAddress());
            socket.setKeepAlive(socketConfig.tcpKeepAlive());
            if (socketConfig.tcpKeepAlive()) {
                final Set<SocketOption<?>> supportedOptions = socket.supportedOptions();
                if (socketConfig.tcpKeepIdle() >= 0) {
                    final SocketOption<Integer> keepIdleOption = NetUtils.getTcpKeepIdleSocketOptionOrNull();
                    if (keepIdleOption != null && supportedOptions.contains(keepIdleOption)) {
                        socket.setOption(keepIdleOption, socketConfig.tcpKeepIdle());
                    }
                }
                if (socketConfig.tcpKeepInterval() >= 0) {
                    final SocketOption<Integer> keepIntervalOption = NetUtils.getTcpKeepIntervalSocketOptionOrNull();
                    if (keepIntervalOption != null && supportedOptions.contains(keepIntervalOption)) {
                        socket.setOption(keepIntervalOption, socketConfig.tcpKeepInterval());
                    }
                }
                if (socketConfig.tcpKeepCount() >= 0) {
                    final SocketOption<Integer> keepCountOption = NetUtils.getTcpKeepCountSocketOptionOrNull();
                    if (keepCountOption != null && supportedOptions.contains(keepCountOption)) {
                        socket.setOption(keepCountOption, socketConfig.tcpKeepCount());
                    }
                }
            }
            socket.setTcpNoDelay(socketConfig.tcpNoDelay());
            int tcpSendBufferSize = socketConfig.tcpSendBufferSize();
            if (tcpSendBufferSize > 0) {
                socket.setSendBufferSize(tcpSendBufferSize);
            }
            int tcpReceiveBufferSize = socketConfig.tcpReceiveBufferSize();
            if (tcpReceiveBufferSize > 0) {
                socket.setReceiveBufferSize(tcpReceiveBufferSize);
            }
            socketOptionsSet = true;
        } catch (IOException e) {
            if (isConnectComplete) {
                throw e;
            }
            // Ignore if not connect complete. Some implementations fail on setting socket options if the
            // socket is not connected. We will try again after connection.
        }
    }

    private static void connect(SocketChannel socketChannel, InetSocketAddress remoteAddress) throws IOException {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Boolean>) () -> socketChannel.connect(remoteAddress));
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }
}
