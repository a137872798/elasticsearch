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

import org.elasticsearch.Version;
import org.elasticsearch.common.lease.Releasable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 贴近业务层的 channel对象   每当处理某个req对象时 就会生成一个对应的channel
 * 该对象只负责发送 res
 */
public final class TcpTransportChannel implements TransportChannel {

    private final AtomicBoolean released = new AtomicBoolean();
    /**
     * 该对象定义了通过channel发送消息的逻辑
     */
    private final OutboundHandler outboundHandler;
    /**
     * 底层channel对象 包含发送message的api
     */
    private final TcpChannel channel;
    private final String action;
    private final long requestId;
    private final Version version;
    private final boolean compressResponse;
    private final boolean isHandshake;

    /**
     * 在该对象处理完毕时 减少熔断器的负载 避免触发熔断
     */
    private final Releasable breakerRelease;

    TcpTransportChannel(OutboundHandler outboundHandler, TcpChannel channel, String action, long requestId, Version version,
                        boolean compressResponse, boolean isHandshake, Releasable breakerRelease) {
        this.version = version;
        this.channel = channel;
        this.outboundHandler = outboundHandler;
        this.action = action;
        this.requestId = requestId;
        this.compressResponse = compressResponse;
        this.isHandshake = isHandshake;
        this.breakerRelease = breakerRelease;
    }

    @Override
    public String getProfileName() {
        return channel.getProfile();
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        try {
            outboundHandler.sendResponse(version, channel, requestId, action, response, compressResponse, isHandshake);
        } finally {
            release(false);
        }
    }

    /**
     * 当接收方处理抛出异常时 异常会以数据流的形式返回
     * @param exception
     * @throws IOException
     */
    @Override
    public void sendResponse(Exception exception) throws IOException {
        try {
            outboundHandler.sendErrorResponse(version, channel, requestId, action, exception);
        } finally {
            release(true);
        }
    }

    private Exception releaseBy;

    private void release(boolean isExceptionResponse) {
        if (released.compareAndSet(false, true)) {
            assert (releaseBy = new Exception()) != null; // easier to debug if it's already closed
            breakerRelease.close();
        } else if (isExceptionResponse == false) {
            // only fail if we are not sending an error - we might send the error triggered by the previous
            // sendResponse call
            throw new IllegalStateException("reserved bytes are already released", releaseBy);
        }
    }

    @Override
    public String getChannelType() {
        return "transport";
    }

    @Override
    public Version getVersion() {
        return version;
    }

    public TcpChannel getChannel() {
        return channel;
    }
}

