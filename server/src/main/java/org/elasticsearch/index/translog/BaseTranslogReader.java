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

import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A base class for all classes that allows reading ops from translog files
 * 读取事务日志的骨架类
 */
public abstract class BaseTranslogReader implements Comparable<BaseTranslogReader> {

    /**
     * 这个gen 指代的是lucene的 segment_N 么 ???
     */
    protected final long generation;
    /**
     * 对应某个事务日志的文件channel
     */
    protected final FileChannel channel;
    protected final Path path;

    /**
     * 该对象负责写入 读取文件头
     */
    protected final TranslogHeader header;

    public BaseTranslogReader(long generation, FileChannel channel, Path path, TranslogHeader header) {
        assert Translog.parseIdFromFileName(path) == generation : "generation mismatch. Path: " +
            Translog.parseIdFromFileName(path) + " but generation: " + generation;

        this.generation = generation;
        this.path = path;
        this.channel = channel;
        this.header = header;
    }

    public long getGeneration() {
        return this.generation;
    }

    /**
     * 获取事务文件的总大小
     * @return
     */
    public abstract long sizeInBytes();

    /**
     * 事务日志中 总计发起了多少次操作  每次操作都会以一个单元的形式存储在事务文件中
     * @return
     */
    public abstract int totalOperations();

    /**
     * 获取事务日志此时的检查点
     * @return
     */
    abstract Checkpoint getCheckpoint();

    /**
     * 找到第一个操作记录 对应的偏移量 就是在 header的后面
     * @return
     */
    public final long getFirstOperationOffset() {
        return header.sizeInBytes();
    }

    /**
     * Returns the primary term associated with this translog reader.
     * term类似于版本号
     */
    public final long getPrimaryTerm() {
        return header.getPrimaryTerm();
    }

    /**
     * read the size of the op (i.e., number of bytes, including the op size) written at the given position
     * @param reusableBuffer 临时buffer
     * @param position 实际上就是定位到即将读取长度的位置
     */
    protected final int readSize(ByteBuffer reusableBuffer, long position) throws IOException {
        // read op size from disk
        assert reusableBuffer.capacity() >= 4 : "reusable buffer must have capacity >=4 when reading opSize. got [" +
            reusableBuffer.capacity() + "]";
        // 先清空临时容器
        reusableBuffer.clear();
        reusableBuffer.limit(4);
        // 从事务文件指定的位置 开始读取一个int值
        readBytes(reusableBuffer, position);
        reusableBuffer.flip();
        // Add an extra 4 to account for the operation size integer itself
        // 描述长度的size本身在之后还要被读取一次 所以要加上
        final int size = reusableBuffer.getInt() + 4;
        // 数据部分必然要小于获取到的size
        final long maxSize = sizeInBytes() - position;
        if (size < 0 || size > maxSize) {
            throw new TranslogCorruptedException(
                    path.toString(),
                    "operation size is corrupted must be [0.." + maxSize + "] but was: " + size);
        }
        return size;
    }

    /**
     * 根据当前reader信息 生成一个快照对象
     * 记录了此时的事务文件 操作总数 检查点 等等信息
     * @return
     */
    public TranslogSnapshot newSnapshot() {
        return new TranslogSnapshot(this, sizeInBytes());
    }

    /**
     * reads an operation at the given position and returns it. The buffer length is equal to the number
     * of bytes reads.
     * @param opSize 推测是记录一个operate的数据大小
     * @return 追加有关校验和的功能 先忽略校验和
     */
    protected final BufferedChecksumStreamInput checksummedStream(ByteBuffer reusableBuffer, long position, int opSize,
                                                                        BufferedChecksumStreamInput reuse) throws IOException {
        final ByteBuffer buffer;
        if (reusableBuffer.capacity() >= opSize) {
            buffer = reusableBuffer;
        } else {
            // 没有足够的空间时 重新申请buffer
            buffer = ByteBuffer.allocate(opSize);
        }
        buffer.clear();
        buffer.limit(opSize);
        // 从指定的位置读取数据
        readBytes(buffer, position);
        buffer.flip();
        // ByteBufferStreamInput 代表将BB 作为一个输入流 并从内部读取数据
        return new BufferedChecksumStreamInput(new ByteBufferStreamInput(buffer), path.toString(), reuse);
    }

    protected Translog.Operation read(BufferedChecksumStreamInput inStream) throws IOException {
        // 从输入流中读取数据 并反序列化成一个操作对象
        final Translog.Operation op = Translog.readOperation(inStream);
        // 不允许操作的term 比当前对象内存储的term 大  除非当前事务文件还没有分配
        if (op.primaryTerm() > getPrimaryTerm() && getPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            throw new TranslogCorruptedException(
                    path.toString(),
                    "operation's term is newer than translog header term; " +
                    "operation term[" + op.primaryTerm() + "], translog header term [" + getPrimaryTerm() + "]");
        }
        return op;
    }

    /**
     * reads bytes at position into the given buffer, filling it.
     */
    protected abstract void readBytes(ByteBuffer buffer, long position) throws IOException;

    @Override
    public String toString() {
        return "translog [" + generation + "][" + path + "]";
    }

    @Override
    public int compareTo(BaseTranslogReader o) {
        return Long.compare(getGeneration(), o.getGeneration());
    }


    public Path path() {
        return path;
    }

    /**
     * 获取文件最近的修改时间
     * @return
     * @throws IOException
     */
    public long getLastModifiedTime() throws IOException {
        return Files.getLastModifiedTime(path).toMillis();
    }

    /**
     * Reads a single operation from the given location.
     * @param location 抽象出来在事务文件中定位 operation
     */
    Translog.Operation read(Translog.Location location) throws IOException {
        assert location.generation == this.generation : "generation mismatch expected: " + generation + " got: " + location.generation;
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        return read(checksummedStream(buffer, location.translogLocation, location.size, null));
    }
}
