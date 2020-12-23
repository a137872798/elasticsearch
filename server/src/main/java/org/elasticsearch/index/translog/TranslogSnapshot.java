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
package org.elasticsearch.index.translog;

import org.elasticsearch.common.io.Channels;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 代表某一时刻 事务日志的信息快照
 * 怎么体现快照 实际上数据还是从 file中获取 而没有做数据拷贝
 */
final class TranslogSnapshot extends BaseTranslogReader {

    /**
     * 在生成快照时 当前事务日志已经记录了多少操作
     */
    private final int totalOperations;
    /**
     * 某个 TranslogReader对应的checkpoint对象
     */
    private final Checkpoint checkpoint;
    /**
     * 此刻事务文件的大小
     */
    protected final long length;

    /**
     * 临时容器 存储单个operation
     */
    private final ByteBuffer reusableBuffer;
    /**
     * 记录此时读取的operation在事务文件的偏移量
     * 初始值为首个操作信息的起始偏移量
     * 每读取一个新的operation 就会增加该值
     */
    private long position;

    private int skippedOperations;

    /**
     * 每个快照有自己的下标 记录此时读取到内部的第几个operation
     */
    private int readOperations;
    private BufferedChecksumStreamInput reuse;

    /**
     * Create a snapshot of translog file channel.
     * @param reader 快照的生成需要借助某个文件的数据
     * @param length  事务文件总大小
     */
    TranslogSnapshot(final BaseTranslogReader reader, final long length) {
        super(reader.generation, reader.channel, reader.path, reader.header);
        this.length = length;
        this.totalOperations = reader.totalOperations();
        this.checkpoint = reader.getCheckpoint();
        this.reusableBuffer = ByteBuffer.allocate(1024);
        this.readOperations = 0;
        this.position = reader.getFirstOperationOffset();
        this.reuse = null;
    }

    @Override
    public int totalOperations() {
        return totalOperations;
    }

    int skippedOperations(){
        return skippedOperations;
    }

    @Override
    Checkpoint getCheckpoint() {
        return checkpoint;
    }


    /**
     * 获取该快照内的下一个 operation 对象 每个快照下包含了一组操作
     * @return
     * @throws IOException
     */
    public Translog.Operation next() throws IOException {
        while (readOperations < totalOperations) {
            final Translog.Operation operation = readOperation();
            // TODO 是啥
            if (operation.seqNo() <= checkpoint.trimmedAboveSeqNo || checkpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                return operation;
            }
            skippedOperations++;
        }
        return null;
    }

    /**
     * 读取下一个operation 对象 并增加readOperation
     * @return
     * @throws IOException
     */
    private Translog.Operation readOperation() throws IOException {
        // 先读取 operation 的长度
        final int opSize = readSize(reusableBuffer, position);
        // 读取数据流
        reuse = checksummedStream(reusableBuffer, position, opSize, reuse);
        // 将数据流反序列化成operation
        Translog.Operation op = read(reuse);
        position += opSize;
        readOperations++;
        return op;
    }

    public long sizeInBytes() {
        return length;
    }

    /**
     * reads an operation at the given position into the given buffer.
     * 定义了从文件中读取数据的逻辑
     */
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        try {
            // 就是通过这个长度限制来做到数据隔离的吗 使得不同快照之间不会相互影响
            if (position >= length) {
                throw new EOFException("read requested past EOF. pos [" + position + "] end: [" + length + "], generation: [" +
                    getGeneration() + "], path: [" + path + "]");
            }
            if (position < getFirstOperationOffset()) {
                throw new IOException("read requested before position of first ops. pos [" + position + "] first op on: [" +
                    getFirstOperationOffset() + "], generation: [" + getGeneration() + "], path: [" + path + "]");
            }
            // 就是一些 fileChannel的操作
            Channels.readFromFileChannelWithEofException(channel, position, buffer);
        } catch (EOFException e) {
            throw new TranslogCorruptedException(path.toString(), "translog truncated", e);
        }
    }

    @Override
    public String toString() {
        return "TranslogSnapshot{" +
                "readOperations=" + readOperations +
                ", position=" + position +
                ", estimateTotalOperations=" + totalOperations +
                ", length=" + length +
                ", generation=" + generation +
                ", reusableBuffer=" + reusableBuffer +
                '}';
    }
}
