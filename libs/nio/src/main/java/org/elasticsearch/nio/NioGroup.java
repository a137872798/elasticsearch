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
import java.net.InetSocketAddress;

/**
 * An class for interfacing with java.nio. Implementations implement the underlying logic for opening
 * channels and registering them with the OS.
 * 事件循环组
 */
public interface NioGroup extends Closeable {

    /**
     * Opens and binds a server channel to accept incoming connections.
     * 绑定本地端口
     */
    <S extends NioServerSocketChannel> S bindServerChannel(InetSocketAddress address, ChannelFactory<S, ?> factory) throws IOException;

    /**
     * Opens a outgoing client channel.
     * 连接到服务器
     */
    <S extends NioSocketChannel> S openChannel(InetSocketAddress address, ChannelFactory<?, S> factory) throws IOException;

    @Override
    void close() throws IOException;
}
