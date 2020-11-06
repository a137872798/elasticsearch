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

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.translog.Translog.getCommitCheckpointFileName;

/**
 * an immutable translog filereader
 * 该对象负责读取事务日志
 */
public class TranslogReader extends BaseTranslogReader implements Closeable {

    /**
     * 文件大小
     */
    protected final long length;
    /**
     * 记录事务文件下总计存储了多少 operation
     */
    private final int totalOperations;
    /**
     * 当前检查点信息  这个是初始化时从外部设置进来的  怎么用
     */
    private final Checkpoint checkpoint;
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a translog writer against the specified translog file channel.
     *
     * @param checkpoint the translog checkpoint
     * @param channel    the translog file channel to open a translog reader against
     * @param path       the path to the translog
     * @param header     the header of the translog file  从事务文件中读取出来的文件头
     */
    TranslogReader(final Checkpoint checkpoint, final FileChannel channel, final Path path, final TranslogHeader header) {
        super(checkpoint.generation, channel, path, header);
        this.length = checkpoint.offset;
        this.totalOperations = checkpoint.numOps;
        this.checkpoint = checkpoint;
    }

    /**
     * Given a file channel, opens a {@link TranslogReader}, taking care of checking and validating the file header.
     *
     * @param channel the translog file channel
     * @param path the path to the translog     事务文件对应的路径
     * @param checkpoint the translog checkpoint
     * @param translogUUID the tranlog UUID    为每个事务文件分配的uuid
     * @return a new TranslogReader
     * @throws IOException if any of the file operations resulted in an I/O exception
     * 一般是通过该方法初始化 事务文件读取对象的
     */
    public static TranslogReader open(
            final FileChannel channel, final Path path, final Checkpoint checkpoint, final String translogUUID) throws IOException {
        // 当读取一个之前存在的事务时  先读取事务头
        final TranslogHeader header = TranslogHeader.read(translogUUID, path, channel);
        return new TranslogReader(checkpoint, channel, path, header);
    }

    /**
     * Closes current reader and creates new one with new checkoint and same file channel
     * 使用同一个fileChannel 创建一个新reader对象
     */
    TranslogReader closeIntoTrimmedReader(long aboveSeqNo, ChannelFactory channelFactory) throws IOException {
        // 设置关闭标识
        if (closed.compareAndSet(false, true)) {
            Closeable toCloseOnFailure = channel;
            final TranslogReader newReader;
            try {
                // 也就是说每次调用该方法 且使用同一个检查点时 要求裁剪点必须比 trimmedAboveSeqNo 大
                if (aboveSeqNo < checkpoint.trimmedAboveSeqNo
                    // 代表首次裁剪
                    || aboveSeqNo < checkpoint.maxSeqNo && checkpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    // 每当发生一次裁剪时 更新检查点文件的数据 如果是第一次应该就是新建文件  translog-gen.ckp
                    final Path checkpointFile = path.getParent().resolve(getCommitCheckpointFileName(checkpoint.generation));
                    final Checkpoint newCheckpoint = new Checkpoint(checkpoint.offset, checkpoint.numOps,
                        checkpoint.generation, checkpoint.minSeqNo, checkpoint.maxSeqNo,
                        checkpoint.globalCheckpoint, checkpoint.minTranslogGeneration, aboveSeqNo);

                    // 将最新的检查点写入到文件中
                    Checkpoint.write(channelFactory, checkpointFile, newCheckpoint, StandardOpenOption.WRITE);

                    // TODO 不懂  不影响流程
                    IOUtils.fsync(checkpointFile, false);
                    IOUtils.fsync(checkpointFile.getParent(), true);

                    newReader = new TranslogReader(newCheckpoint, channel, path, header);
                } else {
                    // 仅返回一个新对象 实际上没有任何变化
                    newReader = new TranslogReader(checkpoint, channel, path, header);
                }
                toCloseOnFailure = null;
                return newReader;
            } finally {
                // 关了channel 之后的reader怎么用啊???
                IOUtils.close(toCloseOnFailure);
            }
        } else {
            throw new AlreadyClosedException(toString() + " is already closed");
        }
    }

    public long sizeInBytes() {
        return length;
    }

    public int totalOperations() {
        return totalOperations;
    }

    @Override
    final Checkpoint getCheckpoint() {
        return checkpoint;
    }

    /**
     * reads an operation at the given position into the given buffer.
     */
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        if (position >= length) {
            throw new EOFException("read requested past EOF. pos [" + position + "] end: [" + length + "]");
        }
        if (position < getFirstOperationOffset()) {
            throw new IOException("read requested before position of first ops. pos [" + position + "] first op on: [" +
                getFirstOperationOffset() + "]");
        }
        // 内部就是针对fileChannel的操作
        Channels.readFromFileChannelWithEofException(channel, position, buffer);
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            channel.close();
        }
    }

    protected final boolean isClosed() {
        return closed.get();
    }

    protected void ensureOpen() {
        if (isClosed()) {
            throw new AlreadyClosedException(toString() + " is already closed");
        }
    }
}
