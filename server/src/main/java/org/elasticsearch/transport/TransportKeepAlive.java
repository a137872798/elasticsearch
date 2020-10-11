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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.AsyncBiFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractLifecycleRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements the scheduling and sending of keep alive pings. Client channels send keep alive pings to the
 * server and server channels respond. Pings are only sent at the scheduled time if the channel did not send
 * and receive a message since the last ping.
 * 定期发送心跳包  在ES的架构中 心跳包是由客户端发往服务器的
 * 每当传输层建立了与某个node的连接时 当前节点就会作为客户端往节点发送心跳包
 */
final class TransportKeepAlive implements Closeable {

    static final int PING_DATA_SIZE = -1;

    private final Logger logger = LogManager.getLogger(TransportKeepAlive.class);

    /**
     * 记录总计成功了多少次 ping
     */
    private final CounterMetric successfulPings = new CounterMetric();
    /**
     * 记录失败的ping
     */
    private final CounterMetric failedPings = new CounterMetric();
    private final ConcurrentMap<TimeValue, ScheduledPing> pingIntervals = ConcurrentCollections.newConcurrentMap();

    /**
     * 描述当前对象的生命周期状态  ping任务在执行时会检测当前生命周期  在不合适的时候会停止发送
     */
    private final Lifecycle lifecycle = new Lifecycle();
    private final ThreadPool threadPool;

    /**
     * 该对象定义了将数据包发往哪个channel 同时使用结果触发监听器的逻辑
     */
    private final AsyncBiFunction<TcpChannel, BytesReference, Void> pingSender;
    /**
     * 心跳包本身是单例模式
     */
    private final BytesReference pingMessage;

    /**
     *
     * @param threadPool
     * @param pingSender  对应 outboundHandler::sendBytes 也就是将消息体通过channel发送
     */
    TransportKeepAlive(ThreadPool threadPool, AsyncBiFunction<TcpChannel, BytesReference, Void> pingSender) {
        this.threadPool = threadPool;
        this.pingSender = pingSender;

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // ES的自定义协议 以 “ES” 作为开头
            out.writeByte((byte) 'E');
            out.writeByte((byte) 'S');
            out.writeInt(PING_DATA_SIZE);
            pingMessage = out.bytes();
        } catch (IOException e) {
            throw new AssertionError(e.getMessage(), e); // won't happen
        }

        this.lifecycle.moveToStarted();
    }

    /**
     * 针对所有需要检测的channel 生成一个任务对象
     * @param nodeChannels
     * @param connectionProfile  该对象维护了心跳检测间隔
     */
    void registerNodeConnection(List<TcpChannel> nodeChannels, ConnectionProfile connectionProfile) {
        TimeValue pingInterval = connectionProfile.getPingInterval();
        if (pingInterval.millis() < 0) {
            return;
        }

        final ScheduledPing scheduledPing = pingIntervals.computeIfAbsent(pingInterval, ScheduledPing::new);
        scheduledPing.ensureStarted();

        for (TcpChannel channel : nodeChannels) {
            scheduledPing.addChannel(channel);
            // 当某个channel 被关闭时 自动从ping任务中移除
            channel.addCloseListener(ActionListener.wrap(() -> scheduledPing.removeChannel(channel)));
        }
    }

    /**
     * Called when a keep alive ping is received. If the channel that received the keep alive ping is a
     * server channel, a ping is sent back. If the channel that received the keep alive is a client channel,
     * this method does nothing as the client initiated the ping in the first place.
     *
     * @param channel that received the keep alive ping
     *                当某个服务端无法正常返回心跳包时会怎么样呢
     */
    void receiveKeepAlive(TcpChannel channel) {
        // The client-side initiates pings and the server-side responds. So if this is a client channel, this
        // method is a no-op.
        // 当前服务端收到某个client的心跳请求时 返回响应信息
        if (channel.isServerChannel()) {
            sendPing(channel);
        }
    }

    long successfulPingCount() {
        return successfulPings.count();
    }

    long failedPingCount() {
        return failedPings.count();
    }

    /**
     * 往某个channel 发送心跳包
     * @param channel
     */
    private void sendPing(TcpChannel channel) {
        pingSender.apply(channel, pingMessage, new ActionListener<Void>() {

            @Override
            public void onResponse(Void v) {
                successfulPings.inc();
            }

            @Override
            public void onFailure(Exception e) {
                if (channel.isOpen()) {
                    logger.debug(() -> new ParameterizedMessage("[{}] failed to send transport ping", channel), e);
                    failedPings.inc();
                } else {
                    logger.trace(() -> new ParameterizedMessage("[{}] failed to send transport ping (channel closed)", channel), e);
                }
            }
        });
    }

    @Override
    public void close() {
        synchronized (lifecycle) {
            lifecycle.moveToStopped();
            lifecycle.moveToClosed();
        }
    }

    /**
     * 该任务有生命周期的概念 同时在一定时间间隔后会向所有目标节点发送心跳包
     * 以 Connection为单位
     */
    private class ScheduledPing extends AbstractLifecycleRunnable {

        /**
         * 对应发送心跳包的时间点
         */
        private final TimeValue pingInterval;

        /**
         * 将会发往哪些channel
         */
        private final Set<TcpChannel> channels = ConcurrentCollections.newConcurrentSet();

        private final AtomicBoolean isStarted = new AtomicBoolean(false);
        private volatile long lastPingRelativeMillis;

        private ScheduledPing(TimeValue pingInterval) {
            // 当本对象无效时 ping任务也不需要继续处理了
            super(lifecycle, logger);
            this.pingInterval = pingInterval;
            this.lastPingRelativeMillis = threadPool.relativeTimeInMillis();
        }

        /**
         * 启动任务
         */
        void ensureStarted() {
            if (isStarted.get() == false && isStarted.compareAndSet(false, true)) {
                threadPool.schedule(this, pingInterval, ThreadPool.Names.GENERIC);
            }
        }

        void addChannel(TcpChannel channel) {
            channels.add(channel);
        }

        void removeChannel(TcpChannel channel) {
            channels.remove(channel);
        }

        @Override
        protected void doRunInLifecycle() {
            for (TcpChannel channel : channels) {
                // In the future it is possible that we may want to kill a channel if we have not read from
                // the channel since the last ping. However, this will need to be backwards compatible with
                // pre-6.6 nodes that DO NOT respond to pings
                // 只找到需要检测的channel  并发送心跳包
                if (needsKeepAlivePing(channel)) {
                    sendPing(channel);
                }
            }
            this.lastPingRelativeMillis = threadPool.relativeTimeInMillis();
        }

        /**
         * 当心跳包发送完成时的后置钩子  就是定时执行下次任务
         */
        @Override
        protected void onAfterInLifecycle() {
            threadPool.scheduleUnlessShuttingDown(pingInterval, ThreadPool.Names.GENERIC, this);
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("failed to send ping transport message", e);
        }

        /**
         * 判断某个channel是否有检测的必要   比如刚从某个channel接收到数据 那么就不需要检测了
         * @param channel
         * @return
         */
        private boolean needsKeepAlivePing(TcpChannel channel) {
            TcpChannel.ChannelStats stats = channel.getChannelStats();
            // 代表在该对象生成之后 接收到了该channel发送的数据 那么该channel  本轮不需要检测
            long accessedDelta = stats.lastAccessedTime() - lastPingRelativeMillis;
            return accessedDelta <= 0;
        }
    }
}
