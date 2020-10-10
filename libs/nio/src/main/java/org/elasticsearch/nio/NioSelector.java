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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * This is a nio selector implementation. This selector wraps a raw nio {@link Selector}. When you call
 * {@link #runLoop()}, the selector will run until {@link #close()} is called. This instance handles closing
 * of channels. Users should call {@link #queueChannelClose(NioChannel)} to schedule a channel for close by
 * this selector.
 * ES 封装的选择器  定义了事件循环的处理逻辑
 */
public class NioSelector implements Closeable {

    /**
     * 存储业务线程生成的写入操作
     */
    private final ConcurrentLinkedQueue<WriteOperation> queuedWrites = new ConcurrentLinkedQueue<>();

    private final ConcurrentLinkedQueue<ChannelContext<?>> channelsToClose = new ConcurrentLinkedQueue<>();
    /**
     * 应该是这样 每当某个channel被注册时 相关的context 就会存入到这个队列  而当channel被关闭时 就会设置到 close 队列
     * 而在es封装的选择器中 并没有直接维护channel 而是通过一个context对象实现解耦
     */
    private final ConcurrentLinkedQueue<ChannelContext<?>> channelsToRegister = new ConcurrentLinkedQueue<>();
    /**
     * 定义处理选择器相关事件的逻辑
     */
    private final EventHandler eventHandler;
    private final Selector selector;

    /**
     * 这个对象应该就是在IO线程(事件循环线程)中使用 因为它没有做线程安全
     */
    private final ByteBuffer ioBuffer;

    /**
     * 简易的定时队列 单线程环境下使用
     */
    private final TaskScheduler taskScheduler = new TaskScheduler();
    private final ReentrantLock runLock = new ReentrantLock();
    private final CountDownLatch exitedLoop = new CountDownLatch(1);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final CompletableFuture<Void> isRunningFuture = new CompletableFuture<>();

    /**
     * 事件循环线程
     */
    private final AtomicReference<Thread> thread = new AtomicReference<>(null);
    /**
     * 代表下次应该调用 selectNow
     */
    private final AtomicBoolean wokenUp = new AtomicBoolean(false);

    public NioSelector(EventHandler eventHandler) throws IOException {
        this(eventHandler, Selector.open());
    }

    /**
     *
     * @param eventHandler
     * @param selector  通过分配合理的时间监听io密集型/cpu密集型
     */
    public NioSelector(EventHandler eventHandler, Selector selector) {
        this.selector = selector;
        this.eventHandler = eventHandler;
        this.ioBuffer = ByteBuffer.allocateDirect(1 << 18);
    }

    /**
     * Returns a cached direct byte buffer for network operations. It is cleared on every get call.
     *
     * @return the byte buffer
     */
    public ByteBuffer getIoBuffer() {
        assertOnSelectorThread();
        ioBuffer.clear();
        return ioBuffer;
    }

    public TaskScheduler getTaskScheduler() {
        return taskScheduler;
    }

    public Selector rawSelector() {
        return selector;
    }

    public boolean isOpen() {
        return isClosed.get() == false;
    }

    public boolean isRunning() {
        return runLock.isLocked();
    }

    Future<Void> isRunningFuture() {
        return isRunningFuture;
    }

    void setThread() {
        boolean wasSet = thread.compareAndSet(null, Thread.currentThread());
        assert wasSet : "Failed to set thread as it was already set. Should only set once.";
    }

    public boolean isOnCurrentThread() {
        return Thread.currentThread() == thread.get();
    }

    public void assertOnSelectorThread() {
        assert isOnCurrentThread() : "Must be on selector thread [" + thread.get().getName() + "} to perform this operation. " +
            "Currently on thread [" + Thread.currentThread().getName() + "].";
    }

    /**
     * Starts this selector. The selector will run until {@link #close()} is called.
     * 每个选择器被创建后 会通过专门的线程执行该函数
     */
    public void runLoop() {
        if (runLock.tryLock()) {
            isRunningFuture.complete(null);
            try {
                // 设置IO线程 之后便于判断触发某些函数是在IO线程还是业务线程
                setThread();
                while (isOpen()) {
                    singleLoop();
                }
            } finally {
                try {
                    cleanupAndCloseChannels();
                } finally {
                    try {
                        selector.close();
                    } catch (IOException e) {
                        eventHandler.selectorException(e);
                    } finally {
                        runLock.unlock();
                        // 当事件循环结束时 唤醒闭锁
                        exitedLoop.countDown();
                    }
                }
            }
        } else {
            throw new IllegalStateException("selector is already running");
        }
    }

    /**
     * 代表单次完整循环
     * 核心逻辑就是有可以处理的任务就尽可能立即处理 等到无任务时选择阻塞监听准备好的事件  这样可以提高线程的利用率
     */
    void singleLoop() {
        try {
            // 检测绑定在该选择器上所有关闭的channel 并进行处理     当外部线程添加一个close任务时 就会唤醒选择器 这样事件循环线程就可以立即醒来 并处理事件 提高了事件循环线程的利用率 也使得外部线程更快返回 因为close/register等任务本身就可以以异步方式实现
            closePendingChannels();
            // 做一些准备工作
            preSelect();
            long nanosUntilNextTask = taskScheduler.nanosUntilNextTask(System.nanoTime());
            int ready;
            // 代表在阻塞过程中 wokenUp被设置过true    或者此时有待执行的任务 那么立即触发一次select 如果此时马上获取到准备好的事件就可以处理 否则留到下次
            if (wokenUp.getAndSet(false) || nanosUntilNextTask == 0) {
                ready = selector.selectNow();
            } else {
                // 检测taskScheduler 下次任务触发时间 并根据该时间阻塞调用select
                long millisUntilNextTask = TimeUnit.NANOSECONDS.toMillis(nanosUntilNextTask);
                // Only select until the next task needs to be run. Do not select with a value of 0 because
                // that blocks without a timeout.
                ready = selector.select(Math.min(300, Math.max(millisUntilNextTask, 1)));
            }

            // 处理准备好的事件
            if (ready > 0) {
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectionKeys.iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey sk = keyIterator.next();
                    keyIterator.remove();
                    if (sk.isValid()) {
                        try {
                            processKey(sk);
                        } catch (CancelledKeyException cke) {
                            eventHandler.genericChannelException((ChannelContext<?>) sk.attachment(),  cke);
                        }
                    } else {
                        eventHandler.genericChannelException((ChannelContext<?>) sk.attachment(),  new CancelledKeyException());
                    }
                }
            }

            // 处理定时队列中的任务
            handleScheduledTasks(System.nanoTime());
        } catch (ClosedSelectorException e) {
            if (isOpen()) {
                throw e;
            }
        } catch (IOException e) {
            eventHandler.selectorException(e);
        } catch (Exception e) {
            eventHandler.uncaughtException(e);
        }
    }

    /**
     * 终止所有待处理的注册 写入事件
     */
    void cleanupAndCloseChannels() {
        cleanupPendingWrites();
        channelsToClose.addAll(channelsToRegister);
        channelsToRegister.clear();
        channelsToClose.addAll(selector.keys().stream()
            .map(sk -> (ChannelContext<?>) sk.attachment()).filter(Objects::nonNull).collect(Collectors.toList()));
        closePendingChannels();
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            wakeup();
            if (isRunning()) {
                try {
                    // 等待事件循环结束
                    exitedLoop.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Thread was interrupted while waiting for selector to close", e);
                }
            } else if (selector.isOpen()) {
                selector.close();
            }
        }
    }

    /**
     * 处理准备好的事件
     * @param selectionKey
     */
    void processKey(SelectionKey selectionKey) {
        ChannelContext<?> context = (ChannelContext<?>) selectionKey.attachment();
        // 代表接收到了新的连接
        if (selectionKey.isAcceptable()) {
            assert context instanceof ServerChannelContext : "Only server channels can receive accept events";
            ServerChannelContext serverChannelContext = (ServerChannelContext) context;
            try {
                eventHandler.acceptChannel(serverChannelContext);
            } catch (IOException e) {
                eventHandler.acceptException(serverChannelContext, e);
            }
        } else {
            assert context instanceof SocketChannelContext : "Only sockets channels can receive non-accept events";
            SocketChannelContext channelContext = (SocketChannelContext) context;
            int ops = selectionKey.readyOps();
            // 检测连接是否完成
            if ((ops & SelectionKey.OP_CONNECT) != 0) {
                attemptConnect(channelContext, true);
            }

            // 当连接完成时才能处理读写事件
            if (channelContext.isConnectComplete()) {
                if (channelContext.selectorShouldClose() == false) {
                    if ((ops & SelectionKey.OP_WRITE) != 0) {
                        handleWrite(channelContext);
                    }
                    if (channelContext.selectorShouldClose() == false && (ops & SelectionKey.OP_READ) != 0) {
                        handleRead(channelContext);
                    }
                }
            }
            // 主要是检测是否还需要注册 write事件
            eventHandler.postHandling(channelContext);
        }

    }

    /**
     * Called immediately prior to a raw {@link Selector#select()} call. Should be used to implement
     * channel registration, handling queued writes, and other work that is not specifically processing
     * a selection key.
     */
    void preSelect() {
        // 每次select一段时间后 会囤积一些新的连接 这里进行注册
        setUpNewChannels();
        // 处理队列中所有 待写入任务
        handleQueuedWrites();
    }

    private void handleScheduledTasks(long nanoTime) {
        Runnable task;
        while ((task = taskScheduler.pollTask(nanoTime)) != null) {
            handleTask(task);
        }
    }

    private void handleTask(Runnable task) {
        try {
            eventHandler.handleTask(task);
        } catch (Exception e) {
            eventHandler.taskException(e);
        }
    }

    /**
     * Queues a write operation to be handled by the event loop. This can be called by any thread and is the
     * api available for non-selector threads to schedule writes. When invoked from the selector thread the write will be executed
     * right away.
     *
     * @param writeOperation to be queued
     *                       这里模拟了netty的事件循环机制   每当某个channel 想要写入数据时都是将数据封装成一个 WriteOperation 并存入到关联selector的队列中
     *                       flushOp 需要准备好write事件  而 writeOp 不需要准备 所以这里可以提前唤醒选择器
     */
    public void queueWrite(WriteOperation writeOperation) {
        // 如果当前是io线程 直接写入到channel中
        if (isOnCurrentThread()) {
            writeToChannel(writeOperation);
        } else {
            // 当前属于任务线程 存储到队列中
            queuedWrites.offer(writeOperation);
            if (isOpen() == false) {
                boolean wasRemoved = queuedWrites.remove(writeOperation);
                if (wasRemoved) {
                    writeOperation.getListener().accept(null, new ClosedSelectorException());
                }
            } else {
                // 提前唤醒选择器
                wakeup();
            }
        }
    }

    public void queueChannelClose(NioChannel channel) {
        ChannelContext<?> context = channel.getContext();
        assert context.getSelector() == this : "Must schedule a channel for closure with its selector";
        // 在业务线程中调用 (或者说外部线程 那么将任务包装后设置到队列中)   这样的分配是为了提高事件循环线程本身的利用率
        if (isOnCurrentThread() == false) {
            channelsToClose.offer(context);
            ensureSelectorOpenForEnqueuing(channelsToClose, context);
            // 唤醒选择器
            wakeup();
        } else {
            // 在IO 线程内直接关闭某个context关联的channel
            closeChannel(context);
        }
    }

    /**
     * Schedules a NioChannel to be registered with this selector. The channel will by queued and
     * eventually registered next time through the event loop.
     *
     * @param channel to register
     *                处理某个channel的注册操作 套路跟其他是一致的
     */
    public void scheduleForRegistration(NioChannel channel) {
        ChannelContext<?> context = channel.getContext();
        if (isOnCurrentThread() == false) {
            // 非事件循环线程 添加任务后唤醒选择器
            channelsToRegister.add(context);
            ensureSelectorOpenForEnqueuing(channelsToRegister, context);
            wakeup();
        } else {
            // 在事件循环线程中 直接执行注册
            registerChannel(context);
        }
    }

    /**
     * Queues a write operation directly in a channel's buffer. If this channel does not have pending writes
     * already, the channel will be flushed. Channel buffers are only safe to be accessed by the selector
     * thread. As a result, this method should only be called by the selector thread. If this channel does
     * not have pending writes already, the channel will be flushed.
     *
     * @param writeOperation to be queued in a channel's buffer
     *                       将某个待写入数据通过channel发送
     */
    private void writeToChannel(WriteOperation writeOperation) {
        // 确保当前线程是  该选择器关联的事件循环线程
        assertOnSelectorThread();
        // 找到本次生成该操作的 channel
        SocketChannelContext context = writeOperation.getChannel();

        if (context.isOpen() == false) {
            // 以失败方式触发监听器
            executeFailedListener(writeOperation.getListener(), new ClosedChannelException());
            // 代表还没有触发register方法  此时应抛出异常
        } else if (context.getSelectionKey() == null) {
            // This should very rarely happen. The only times a channel is exposed outside the event loop,
            // but might not registered is through the exception handler and channel accepted callbacks.
            executeFailedListener(writeOperation.getListener(), new IllegalStateException("Channel not registered"));
        } else {
            // If the channel does not currently have anything that is ready to flush, we should flush after
            // the write operation is queued.
            boolean shouldFlushAfterQueuing = context.readyForFlush() == false;
            try {
                // 使用 handler处理写入操作后 并存储到刷盘队列
                context.queueWriteOperation(writeOperation);
            } catch (Exception e) {
                shouldFlushAfterQueuing = false;
                executeFailedListener(writeOperation.getListener(), e);
            }

            // 之前没有囤积的flush 代表此时可以直接将数据写入到缓冲区   如果有囤积任务 代表之前缓冲区就满了 必须等待write事件准备完成
            if (shouldFlushAfterQueuing) {
                // We only attempt the write if the connect process is complete and the context is not
                // signalling that it should be closed.
                // 如果连接已经完成 且 未关闭
                if (context.isConnectComplete() && context.selectorShouldClose() == false) {
                    // 使用eventHandler 处理context内所有待刷盘任务
                    handleWrite(context);
                }
                // 触发后置钩子 检测此时是否有数据无法写入到缓冲区 有的话 选择注册Write事件
                eventHandler.postHandling(context);
            }
        }
    }

    /**
     * Executes a success listener with consistent exception handling. This can only be called from current
     * selector thread.
     *
     * @param listener to be executed
     * @param value    to provide to listener
     */
    public <V> void executeListener(BiConsumer<V, Exception> listener, V value) {
        assertOnSelectorThread();
        handleTask(() -> listener.accept(value, null));
    }

    /**
     * Executes a failed listener with consistent exception handling. This can only be called from current
     * selector thread.
     *
     * @param listener  to be executed
     * @param exception to provide to listener
     */
    public <V> void executeFailedListener(BiConsumer<V, Exception> listener, Exception exception) {
        assertOnSelectorThread();
        handleTask(() -> listener.accept(null, exception));
    }

    private void cleanupPendingWrites() {
        WriteOperation op;
        while ((op = queuedWrites.poll()) != null) {
            executeFailedListener(op.getListener(), new ClosedSelectorException());
        }
    }

    private void wakeup() {
        assert isOnCurrentThread() == false;
        if (wokenUp.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    /**
     * 处理写事件
     * @param context
     */
    private void handleWrite(SocketChannelContext context) {
        try {
            eventHandler.handleWrite(context);
        } catch (Exception e) {
            eventHandler.writeException(context, e);
        }
    }

    private void handleRead(SocketChannelContext context) {
        try {
            eventHandler.handleRead(context);
        } catch (Exception e) {
            eventHandler.readException(context, e);
        }
    }

    /**
     * 检测连接是否完成
     * @param context
     * @param connectEvent  是否准备好连接事件
     */
    private void attemptConnect(SocketChannelContext context, boolean connectEvent) {
        try {
            eventHandler.handleConnect(context);
            if (connectEvent && context.isConnectComplete() == false) {
                eventHandler.connectException(context, new IOException("Received OP_CONNECT but connect failed"));
            }
        } catch (Exception e) {
            eventHandler.connectException(context, e);
        }
    }

    private void setUpNewChannels() {
        ChannelContext<?> newChannel;
        while ((newChannel = this.channelsToRegister.poll()) != null) {
            registerChannel(newChannel);
        }
    }

    /**
     * 处理一些囤积的注册请求
     * @param newChannel
     */
    private void registerChannel(ChannelContext<?> newChannel) {
        assert newChannel.getSelector() == this : "The channel must be registered with the selector with which it was created";
        try {
            if (newChannel.isOpen()) {
                // 在这里将channel注册到了 selector上
                eventHandler.handleRegistration(newChannel);
                // 触发钩子函数
                channelActive(newChannel);
                if (newChannel instanceof SocketChannelContext) {
                    // 检测连接是否完成， 是的话移除 connect事件
                    attemptConnect((SocketChannelContext) newChannel, false);
                }
            } else {
                eventHandler.registrationException(newChannel, new ClosedChannelException());
                closeChannel(newChannel);
            }
        } catch (Exception e) {
            eventHandler.registrationException(newChannel, e);
            closeChannel(newChannel);
        }
    }

    /**
     * 在基于原生nio开发的 连接框架中 当channel注册到选择器上时 认为channel处于活跃状态
     * @param newChannel
     */
    private void channelActive(ChannelContext<?> newChannel) {
        try {
            // 当channel被激活时监听对应的事件 比如 ServerSocketChannel 就要监听 accept事件  如果是 SocketChannel 就是监听read事件
            eventHandler.handleActive(newChannel);
        } catch (IOException e) {
            eventHandler.activeException(newChannel, e);
        }
    }

    /**
     * 处理关闭的channel
     */
    private void closePendingChannels() {
        ChannelContext<?> channelContext;
        while ((channelContext = channelsToClose.poll()) != null) {
            closeChannel(channelContext);
        }
    }

    /**
     * 委托给eventHandler关闭channel
     * @param channelContext
     */
    private void closeChannel(final ChannelContext<?> channelContext) {
        try {
            eventHandler.handleClose(channelContext);
        } catch (Exception e) {
            eventHandler.closeException(channelContext, e);
        }
    }

    private void handleQueuedWrites() {
        WriteOperation writeOperation;
        while ((writeOperation = queuedWrites.poll()) != null) {
            writeToChannel(writeOperation);
        }
    }

    /**
     * This is a convenience method to be called after some object (normally channels) are enqueued with this
     * selector. This method will check if the selector is still open. If it is open, normal operation can
     * proceed.
     *
     * If the selector is closed, then we attempt to remove the object from the queue. If the removal
     * succeeds then we throw an {@link IllegalStateException} indicating that normal operation failed. If
     * the object cannot be removed from the queue, then the object has already been handled by the selector
     * and operation can proceed normally.
     *
     * If this method is called from the selector thread, we will not allow the queuing to occur as the
     * selector thread can manipulate its queues internally even if it is no longer open.
     *
     * @param queue the queue to which the object was added
     * @param objectAdded the objected added
     * @param <O> the object type
     */
    private <O> void ensureSelectorOpenForEnqueuing(ConcurrentLinkedQueue<O> queue, O objectAdded) {
        if (isOpen() == false) {
            if (queue.remove(objectAdded)) {
                throw new IllegalStateException("selector is already closed");
            }
        }
    }
}
