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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * 输入消息解码器
 */
public class InboundDecoder implements Releasable {

    static final Object PING = new Object();
    static final Object END_CONTENT = new Object();

    private final Version version;
    /**
     * 对象池
     */
    private final PageCacheRecycler recycler;

    /**
     * 基于某种特殊算法进行压缩  就不细看了
     */
    private TransportDecompressor decompressor;
    /**
     * 当前解析消息的总长度
     */
    private int totalNetworkSize = -1;
    private int bytesConsumed = 0;
    private boolean isClosed = false;

    public InboundDecoder(Version version, PageCacheRecycler recycler) {
        this.version = version;
        this.recycler = recycler;
    }

    public int decode(ReleasableBytesReference reference, Consumer<Object> fragmentConsumer) throws IOException {
        ensureOpen();
        try {
            return internalDecode(reference, fragmentConsumer);
        } catch (Exception e) {
            cleanDecodeState();
            throw e;
        }
    }

    /**
     * 感觉上这个方法在解析一条消息时会被连续调用
     * @param reference   对该数据流进行解压
     * @param fragmentConsumer 处理结果
     * @return
     * @throws IOException
     */
    public int internalDecode(ReleasableBytesReference reference, Consumer<Object> fragmentConsumer) throws IOException {
        // 代表首次解码 需要从头部开始
        if (isOnHeader()) {
            // 获取有效长度  也就是忽略BYTES_REQUIRED_FOR_MESSAGE_SIZE的部分
            int messageLength = TcpTransport.readMessageLength(reference);
            // 异常情况 忽略
            if (messageLength == -1) {
                return 0;
            // 代表是心跳包
            } else if (messageLength == 0) {
                fragmentConsumer.accept(PING);
                return 6;
            } else {
                // 获取消息头的总长度
                int headerBytesToRead = headerBytesToRead(reference);
                if (headerBytesToRead == 0) {
                    return 0;
                } else {
                    // 这个是总长度
                    totalNetworkSize = messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE;

                    // 反序列化生成header对象
                    Header header = readHeader(messageLength, reference);
                    bytesConsumed += headerBytesToRead;
                    if (header.isCompressed()) {
                        decompressor = new TransportDecompressor(recycler);
                    }
                    fragmentConsumer.accept(header);

                    // 如果此时已经处理完所有待解析消息 触发 finishMessage
                    if (isDone()) {
                        finishMessage(fragmentConsumer);
                    }
                    return headerBytesToRead;
                }
            }
        } else {
            // There are a minimum number of bytes required to start decompression
            // 在解压时有一个最小长度要求 当不满足该要求时 无法进行解压
            // 如果 hasReadHeader 为true代表已经解压过一部分了 那么剩余的数据无论长度多少 都必须继续解压 (因为可能发生拆包粘包的问题)
            if (decompressor != null && decompressor.canDecompress(reference.length()) == false) {
                return 0;
            }
            // 剩余需要解压的长度
            int bytesToConsume = Math.min(reference.length(), totalNetworkSize - bytesConsumed);
            bytesConsumed += bytesToConsume;
            ReleasableBytesReference retainedContent;
            if (isDone()) {
                retainedContent = reference.retainedSlice(0, bytesToConsume);
            } else {
                retainedContent = reference.retain();
            }
            if (decompressor != null) {
                decompress(retainedContent);
                ReleasableBytesReference decompressed;
                while ((decompressed = decompressor.pollDecompressedPage()) != null) {
                    fragmentConsumer.accept(decompressed);
                }
            } else {
                // 如果消息体本身不需要解压 直接处理就好
                fragmentConsumer.accept(retainedContent);
            }
            if (isDone()) {
                finishMessage(fragmentConsumer);
            }

            return bytesToConsume;
        }
    }

    @Override
    public void close() {
        isClosed = true;
        cleanDecodeState();
    }

    private void finishMessage(Consumer<Object> fragmentConsumer) {
        cleanDecodeState();
        // 代表本次解析完成的标识 在 pipeline中会对该标识进行检测
        fragmentConsumer.accept(END_CONTENT);
    }

    /**
     * 每当某个消息解析完毕后 重置相关属性
     */
    private void cleanDecodeState() {
        IOUtils.closeWhileHandlingException(decompressor);
        decompressor = null;
        totalNetworkSize = -1;
        bytesConsumed = 0;
    }

    private void decompress(ReleasableBytesReference content) throws IOException {
        try (content) {
            int consumed = decompressor.decompress(content);
            assert consumed == content.length();
        }
    }

    private boolean isDone() {
        return bytesConsumed == totalNetworkSize;
    }

    /**
     * 获取消息头的长度
     * @param reference
     * @return
     */
    private int headerBytesToRead(BytesReference reference) {
        if (reference.length() < TcpHeader.BYTES_REQUIRED_FOR_VERSION) {
            return 0;
        }

        // 跳跃到下标为15的位置 读取4个长度 作为版本号
        Version remoteVersion = Version.fromId(reference.getInt(TcpHeader.VERSION_POSITION));
        // 不同的版本消息头长度不同
        int fixedHeaderSize = TcpHeader.headerSize(remoteVersion);
        if (fixedHeaderSize > reference.length()) {
            return 0;
        } else if (remoteVersion.before(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
            return fixedHeaderSize;
        } else {
            // 在之后的版本中追加了变长部分
            int variableHeaderSize = reference.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);
            int totalHeaderSize = fixedHeaderSize + variableHeaderSize;
            if (totalHeaderSize > reference.length()) {
                return 0;
            } else {
                return totalHeaderSize;
            }
        }
    }

    /**
     * 读取部分数据发序列化成消息头
     * @param networkMessageSize  有效的消息体长度 也就是跳过 BYTES_REQUIRED_FOR_MESSAGE_SIZE 的部分
     * @param bytesReference
     * @return
     * @throws IOException
     */
    private Header readHeader(int networkMessageSize, BytesReference bytesReference) throws IOException {
        try (StreamInput streamInput = bytesReference.streamInput()) {
            streamInput.skip(TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
            long requestId = streamInput.readLong();
            byte status = streamInput.readByte();
            Version remoteVersion = Version.fromId(streamInput.readInt());
            Header header = new Header(networkMessageSize, requestId, status, remoteVersion);
            // TODO 忽略兼容性检测
            final IllegalStateException invalidVersion = ensureVersionCompatibility(remoteVersion, version, header.isHandshake());
            if (invalidVersion != null) {
                throw invalidVersion;
            } else {
                // 读取actionName
                if (remoteVersion.onOrAfter(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
                    // Skip since we already have ensured enough data available
                    streamInput.readInt();
                    header.finishParsingHeader(streamInput);
                }
            }
            return header;
        }
    }

    private boolean isOnHeader() {
        return totalNetworkSize == -1;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Decoder is already closed");
        }
    }

    static IllegalStateException ensureVersionCompatibility(Version remoteVersion, Version currentVersion, boolean isHandshake) {
        // for handshakes we are compatible with N-2 since otherwise we can't figure out our initial version
        // since we are compatible with N-1 and N+1 so we always send our minCompatVersion as the initial version in the
        // handshake. This looks odd but it's required to establish the connection correctly we check for real compatibility
        // once the connection is established
        final Version compatibilityVersion = isHandshake ? currentVersion.minimumCompatibilityVersion() : currentVersion;
        if (remoteVersion.isCompatible(compatibilityVersion) == false) {
            final Version minCompatibilityVersion = isHandshake ? compatibilityVersion : compatibilityVersion.minimumCompatibilityVersion();
            String msg = "Received " + (isHandshake ? "handshake " : "") + "message from unsupported version: [";
            return new IllegalStateException(msg + remoteVersion + "] minimal compatible version is: [" + minCompatibilityVersion + "]");
        }
        return null;
    }
}
