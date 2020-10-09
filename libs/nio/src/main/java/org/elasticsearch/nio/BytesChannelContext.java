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

import java.io.IOException;
import java.util.function.Consumer;

/**
 * 先忽略SSL对象 该对象基于明文数据使用
 */
public class BytesChannelContext extends SocketChannelContext {

    public BytesChannelContext(NioSocketChannel channel, NioSelector selector, Config.Socket socketConfig,
                               Consumer<Exception> exceptionHandler, NioChannelHandler handler, InboundChannelBuffer channelBuffer) {
        super(channel, selector, socketConfig, exceptionHandler, handler, channelBuffer);
    }

    @Override
    public int read() throws IOException {
        // 使用 selector.ioBuffer 从该context相关的channel上读取数据 之后将数据转移到channelBuffer上
        int bytesRead = readFromChannel(channelBuffer);

        if (bytesRead == 0) {
            return 0;
        }

        // 消费读取到的数据 并将结果 包装成flushOp
        handleReadBytes();

        return bytesRead;
    }

    /**
     * 处理所有待刷盘任务
     * @throws IOException
     */
    @Override
    public void flushChannel() throws IOException {
        getSelector().assertOnSelectorThread();
        boolean lastOpCompleted = true;
        FlushOperation flushOperation;
        while (lastOpCompleted && (flushOperation = getPendingFlush()) != null) {
            try {
                if (singleFlush(flushOperation)) {
                    currentFlushOperationComplete();
                } else {
                    lastOpCompleted = false;
                }
            } catch (IOException e) {
                currentFlushOperationFailed(e);
                throw e;
            }
        }
    }

    @Override
    public void closeChannel() {
        if (isClosing.compareAndSet(false, true)) {
            getSelector().queueChannelClose(channel);
        }
    }

    @Override
    public boolean selectorShouldClose() {
        return closeNow() || isClosing.get();
    }

    /**
     * Returns a boolean indicating if the operation was fully flushed.
     */
    private boolean singleFlush(FlushOperation flushOperation) throws IOException {
        // 将flushOp内部的数据通过channel写到对端
        flushToChannel(flushOperation);
        return flushOperation.isFullyFlushed();
    }
}
