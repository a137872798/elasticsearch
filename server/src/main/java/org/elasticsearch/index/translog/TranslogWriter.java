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

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.procedures.LongProcedure;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.Assertions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * 负责往事务日志中写入数据
 */
public class TranslogWriter extends BaseTranslogReader implements Closeable {
    private final ShardId shardId;
    private final ChannelFactory channelFactory;
    // the last checkpoint that was written when the translog was last synced
    // 最近一次提交的检查点 因为提交不代表会关闭该对象
    private volatile Checkpoint lastSyncedCheckpoint;
    /*
    *the number of translog operations written to this file
    * 记录该writer写入的operation总数
    */
    private volatile int operationCounter;
    /* if we hit an exception that we can't recover from we assign it to this var and ship it with every AlreadyClosedException we throw */
    private final TragicExceptionHolder tragedy;
    /*
     * A buffered outputstream what writes to the writers channel
     * 刷盘时就是将该输出流内的数据写入到文件中
     */
    private final OutputStream outputStream;
    /*
    *the total offset of this file including the bytes written to the file as well as into the buffer
    * 记录该writer写入的文件总长度
    */
    private volatile long totalOffset;

    // seq 对应的是每个操作 这里分别记录一个事务文件下的最大值/最小值
    private volatile long minSeqNo;
    private volatile long maxSeqNo;

    private final LongSupplier globalCheckpointSupplier;
    private final LongSupplier minTranslogGenerationSupplier;

    // callback that's called whenever an operation with a given sequence number is successfully persisted.
    // 该函数是定义如何处理那些未提交的数据的
    private final LongConsumer persistedSequenceNumberConsumer;

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    // lock order synchronized(syncLock) -> synchronized(this)
    private final Object syncLock = new Object();

    /**
     * 每次写入新的operation都会存储它的seqNo 在每次sync时才交给函数去处理
     */
    private LongArrayList nonFsyncedSequenceNumbers;

    private final Map<Long, Tuple<BytesReference, Exception>> seenSequenceNumbers;

    /**
     * 这边基本就是赋值操作  TranslogWriter都是通过静态方法创建的
     *
     * @param channelFactory
     * @param shardId
     * @param initialCheckpoint
     * @param channel    事务文件的channel
     * @param path                            对应 translog-gen.tlg 文件 也就是事务文件
     * @param bufferSize
     * @param globalCheckpointSupplier
     * @param minTranslogGenerationSupplier
     * @param header
     * @param tragedy
     * @param persistedSequenceNumberConsumer
     * @throws IOException
     */
    private TranslogWriter(
        final ChannelFactory channelFactory,
        final ShardId shardId,
        final Checkpoint initialCheckpoint,
        final FileChannel channel,
        final Path path,
        final ByteSizeValue bufferSize,
        final LongSupplier globalCheckpointSupplier, LongSupplier minTranslogGenerationSupplier, TranslogHeader header,
        TragicExceptionHolder tragedy,
        final LongConsumer persistedSequenceNumberConsumer)
        throws
        IOException {
        super(initialCheckpoint.generation, channel, path, header);
        assert initialCheckpoint.offset == channel.position() :
            "initial checkpoint offset [" + initialCheckpoint.offset + "] is different than current channel position ["
                + channel.position() + "]";
        this.shardId = shardId;
        this.channelFactory = channelFactory;
        this.minTranslogGenerationSupplier = minTranslogGenerationSupplier;
        this.outputStream = new BufferedChannelOutputStream(java.nio.channels.Channels.newOutputStream(channel), bufferSize.bytesAsInt());
        // 使用的是全局检查点
        this.lastSyncedCheckpoint = initialCheckpoint;
        this.totalOffset = initialCheckpoint.offset;
        assert initialCheckpoint.minSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.minSeqNo;
        this.minSeqNo = initialCheckpoint.minSeqNo;
        assert initialCheckpoint.maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.maxSeqNo;
        this.maxSeqNo = initialCheckpoint.maxSeqNo;
        assert initialCheckpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : initialCheckpoint.trimmedAboveSeqNo;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.nonFsyncedSequenceNumbers = new LongArrayList(64);
        this.persistedSequenceNumberConsumer = persistedSequenceNumberConsumer;
        this.seenSequenceNumbers = Assertions.ENABLED ? new HashMap<>() : null;
        this.tragedy = tragedy;
    }

    /**
     * 通过静态方法创建writer对象
     *
     * @param shardId                         每个writer对应一个translog 对应一个shard
     * @param translogUUID                    每次初始化translog时会分配一个uuid
     * @param fileGeneration                  该对象要写入的文件对应的gen 会作为文件名的一部分
     * @param file                            对应事务文件路径 (translog-gen.tlg文件)
     * @param channelFactory                  用于生成fileChannel
     * @param bufferSize                      内存缓冲区大小  数据会先存储在缓冲区中 并一次性写入到磁盘
     * @param initialMinTranslogGen           此时存活的所有事务文件中最小的gen  仅对应生成该对象时的状态
     * @param initialGlobalCheckpoint         此时的全局检查点   当分片处于恢复阶段时 会使用本地检查点作为全局检查点
     * @param globalCheckpointSupplier        globalCheckpointSupplier
     * @param minTranslogGenerationSupplier   对应函数 getMinFileGeneration() 动态获取此时所有事务文件中最小的gen
     * @param primaryTerm                     生成该分片对应的shardRouting时的 term信息
     * @param tragedy                         TragicExceptionHolder  就是可以存储一个异常对象
     * @param persistedSequenceNumberConsumer
     * @return
     * @throws IOException
     */
    public static TranslogWriter create(ShardId shardId, String translogUUID, long fileGeneration, Path file, ChannelFactory channelFactory,
                                        ByteSizeValue bufferSize, final long initialMinTranslogGen, long initialGlobalCheckpoint,
                                        final LongSupplier globalCheckpointSupplier, final LongSupplier minTranslogGenerationSupplier,
                                        final long primaryTerm, TragicExceptionHolder tragedy, LongConsumer persistedSequenceNumberConsumer)
        throws IOException {
        // 这里生成最新的事务文件
        final FileChannel channel = channelFactory.open(file);
        try {
            // 因为事务头的格式是固定的 所以可以预估header大小
            final TranslogHeader header = new TranslogHeader(translogUUID, primaryTerm);
            header.write(channel);

            // 生成空的检查点对象
            final Checkpoint checkpoint = Checkpoint.emptyTranslogCheckpoint(header.sizeInBytes(), fileGeneration,
                initialGlobalCheckpoint, initialMinTranslogGen);

            // 覆盖之前的全局检查点文件  文件生成顺序是 translog-gen.tlg translog.ckp translog-gen.ckp translog-gen+1.tlg ...
            writeCheckpoint(channelFactory, file.getParent(), checkpoint);
            final LongSupplier writerGlobalCheckpointSupplier;
            if (Assertions.ENABLED) {
                writerGlobalCheckpointSupplier = () -> {
                    long gcp = globalCheckpointSupplier.getAsLong();
                    assert gcp >= initialGlobalCheckpoint :
                        "global checkpoint [" + gcp + "] lower than initial gcp [" + initialGlobalCheckpoint + "]";
                    return gcp;
                };
            } else {
                writerGlobalCheckpointSupplier = globalCheckpointSupplier;
            }
            // 使用相关信息生成 事务写入对象
            return new TranslogWriter(channelFactory, shardId, checkpoint, channel, file, bufferSize,
                writerGlobalCheckpointSupplier, minTranslogGenerationSupplier, header, tragedy, persistedSequenceNumberConsumer);
        } catch (Exception exception) {
            // if we fail to bake the file-generation into the checkpoint we stick with the file and once we recover and that
            // file exists we remove it. We only apply this logic to the checkpoint.generation+1 any other file with a higher generation
            // is an error condition
            IOUtils.closeWhileHandlingException(channel);
            throw exception;
        }
    }

    private synchronized void closeWithTragicEvent(final Exception ex) {
        tragedy.setTragicException(ex);
        try {
            close();
        } catch (final IOException | RuntimeException e) {
            ex.addSuppressed(e);
        }
    }

    /**
     * add the given bytes to the translog and return the location they were written at
     */

    /**
     * Add the given bytes to the translog with the specified sequence number; returns the location the bytes were written to.
     *
     * @param data  the bytes to write    operation序列化后的数据流
     * @param seqNo the sequence number associated with the operation  每个operation都有一个全局序列号
     * @return the location the bytes were written to
     * @throws IOException if writing to the translog resulted in an I/O exception
     * 往事务日志中记录一个新的operation
     */
    public synchronized Translog.Location add(final BytesReference data, final long seqNo) throws IOException {
        ensureOpen();
        final long offset = totalOffset;
        try {
            data.writeTo(outputStream);
        } catch (final Exception ex) {
            closeWithTragicEvent(ex);
            throw ex;
        }

        totalOffset += data.length();

        if (minSeqNo == SequenceNumbers.NO_OPS_PERFORMED) {
            assert operationCounter == 0;
        }
        if (maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED) {
            assert operationCounter == 0;
        }

        minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
        maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);

        nonFsyncedSequenceNumbers.add(seqNo);

        operationCounter++;

        assert assertNoSeqNumberConflict(seqNo, data);

        return new Translog.Location(generation, offset, data.length());
    }

    private synchronized boolean assertNoSeqNumberConflict(long seqNo, BytesReference data) throws IOException {
        if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            // nothing to do
        } else if (seenSequenceNumbers.containsKey(seqNo)) {
            final Tuple<BytesReference, Exception> previous = seenSequenceNumbers.get(seqNo);
            if (previous.v1().equals(data) == false) {
                Translog.Operation newOp = Translog.readOperation(
                    new BufferedChecksumStreamInput(data.streamInput(), "assertion"));
                Translog.Operation prvOp = Translog.readOperation(
                    new BufferedChecksumStreamInput(previous.v1().streamInput(), "assertion"));
                // TODO: We haven't had timestamp for Index operations in Lucene yet, we need to loosen this check without timestamp.
                final boolean sameOp;
                if (newOp instanceof Translog.Index && prvOp instanceof Translog.Index) {
                    final Translog.Index o1 = (Translog.Index) prvOp;
                    final Translog.Index o2 = (Translog.Index) newOp;
                    sameOp = Objects.equals(o1.id(), o2.id())
                        && Objects.equals(o1.source(), o2.source()) && Objects.equals(o1.routing(), o2.routing())
                        && o1.primaryTerm() == o2.primaryTerm() && o1.seqNo() == o2.seqNo()
                        && o1.version() == o2.version();
                } else if (newOp instanceof Translog.Delete && prvOp instanceof Translog.Delete) {
                    final Translog.Delete o1 = (Translog.Delete) newOp;
                    final Translog.Delete o2 = (Translog.Delete) prvOp;
                    sameOp = Objects.equals(o1.id(), o2.id())
                        && o1.primaryTerm() == o2.primaryTerm() && o1.seqNo() == o2.seqNo() && o1.version() == o2.version();
                } else {
                    sameOp = false;
                }
                if (sameOp == false) {
                    throw new AssertionError(
                        "seqNo [" + seqNo + "] was processed twice in generation [" + generation + "], with different data. " +
                            "prvOp [" + prvOp + "], newOp [" + newOp + "]", previous.v2());
                }
            }
        } else {
            seenSequenceNumbers.put(seqNo,
                new Tuple<>(new BytesArray(data.toBytesRef(), true), new RuntimeException("stack capture previous op")));
        }
        return true;
    }

    synchronized boolean assertNoSeqAbove(long belowTerm, long aboveSeqNo) {
        seenSequenceNumbers.entrySet().stream().filter(e -> e.getKey().longValue() > aboveSeqNo)
            .forEach(e -> {
                final Translog.Operation op;
                try {
                    op = Translog.readOperation(
                        new BufferedChecksumStreamInput(e.getValue().v1().streamInput(), "assertion"));
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                long seqNo = op.seqNo();
                long primaryTerm = op.primaryTerm();
                if (primaryTerm < belowTerm) {
                    throw new AssertionError("current should not have any operations with seq#:primaryTerm ["
                        + seqNo + ":" + primaryTerm + "] > " + aboveSeqNo + ":" + belowTerm);
                }
            });
        return true;
    }

    /**
     * write all buffered ops to disk and fsync file.
     * <p>
     * Note: any exception during the sync process will be interpreted as a tragic exception and the writer will be closed before
     * raising the exception.
     */
    public void sync() throws IOException {
        syncUpTo(Long.MAX_VALUE);
    }

    /**
     * Returns <code>true</code> if there are buffered operations that have not been flushed and fsynced to disk or if the latest global
     * checkpoint has not yet been fsynced
     * 只有数据发生了变化才有刷盘的必要
     */
    public boolean syncNeeded() {
        return totalOffset != lastSyncedCheckpoint.offset ||
            globalCheckpointSupplier.getAsLong() != lastSyncedCheckpoint.globalCheckpoint ||
            minTranslogGenerationSupplier.getAsLong() != lastSyncedCheckpoint.minTranslogGeneration;
    }

    @Override
    public int totalOperations() {
        return operationCounter;
    }

    @Override
    synchronized Checkpoint getCheckpoint() {
        return new Checkpoint(totalOffset, operationCounter, generation, minSeqNo, maxSeqNo,
            globalCheckpointSupplier.getAsLong(), minTranslogGenerationSupplier.getAsLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO);
    }

    @Override
    public long sizeInBytes() {
        return totalOffset;
    }

    /**
     * Closes this writer and transfers its underlying file channel to a new immutable {@link TranslogReader}
     *
     * @return a new {@link TranslogReader}
     * @throws IOException if any of the file operations resulted in an I/O exception
     * 关闭当前写入对象并转换成一个reader对象 当然针对的都是同一事务文件
     */
    public TranslogReader closeIntoReader() throws IOException {
        // make sure to acquire the sync lock first, to prevent dead locks with threads calling
        // syncUpTo() , where the sync lock is acquired first, following by the synchronize(this)
        //
        // Note: While this is not strictly needed as this method is called while blocking all ops on the translog,
        //       we do this to for correctness and preventing future issues.
        synchronized (syncLock) {
            synchronized (this) {
                try {
                    sync(); // sync before we close..
                } catch (final Exception ex) {
                    closeWithTragicEvent(ex);
                    throw ex;
                }
                if (closed.compareAndSet(false, true)) {
                    return new TranslogReader(getLastSyncedCheckpoint(), channel, path, header);
                } else {
                    throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed (path [" + path + "]",
                        tragedy.get());
                }
            }
        }
    }


    /**
     * writer生成快照分为2步
     * 第一步将最新数据刷盘
     * 第二步基于之前读取到的数据生成快照对象
     * @return
     */
    @Override
    public TranslogSnapshot newSnapshot() {
        // make sure to acquire the sync lock first, to prevent dead locks with threads calling
        // syncUpTo() , where the sync lock is acquired first, following by the synchronize(this)
        synchronized (syncLock) {
            synchronized (this) {
                ensureOpen();
                try {
                    sync();
                } catch (IOException e) {
                    throw new TranslogException(shardId, "exception while syncing before creating a snapshot", e);
                }
                return super.newSnapshot();
            }
        }
    }

    private long getWrittenOffset() throws IOException {
        return channel.position();
    }

    /**
     * Syncs the translog up to at least the given offset unless already synced
     *
     * @return <code>true</code> if this call caused an actual sync operation
     * 将此时的检查点状态持久化
     */
    final boolean syncUpTo(long offset) throws IOException {
        // 每次提交的位置超过上次提交的offset
        if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
            synchronized (syncLock) { // only one sync/checkpoint should happen concurrently but we wait
                if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
                    // double checked locking - we don't want to fsync unless we have to and now that we have
                    // the lock we should check again since if this code is busy we might have fsynced enough already
                    final Checkpoint checkpointToSync;
                    final LongArrayList flushedSequenceNumbers;
                    synchronized (this) {
                        ensureOpen();
                        try {
                            // 将输出流中的数据都写入到channel中
                            outputStream.flush();
                            // 在使用当前偏移量等新数据生成一个新的 checkPoint后
                            checkpointToSync = getCheckpoint();
                            flushedSequenceNumbers = nonFsyncedSequenceNumbers;
                            nonFsyncedSequenceNumbers = new LongArrayList(64);
                        } catch (final Exception ex) {
                            closeWithTragicEvent(ex);
                            throw ex;
                        }
                    }
                    // now do the actual fsync outside of the synchronized block such that
                    // we can continue writing to the buffer etc.
                    try {
                        // 将channel内部的数据写入到磁盘
                        channel.force(false);
                        // 将最新的检查点数据写入到文件中
                        writeCheckpoint(channelFactory, path.getParent(), checkpointToSync);
                    } catch (final Exception ex) {
                        closeWithTragicEvent(ex);
                        throw ex;
                    }
                    flushedSequenceNumbers.forEach((LongProcedure) persistedSequenceNumberConsumer::accept);
                    assert lastSyncedCheckpoint.offset <= checkpointToSync.offset :
                        "illegal state: " + lastSyncedCheckpoint.offset + " <= " + checkpointToSync.offset;
                    lastSyncedCheckpoint = checkpointToSync; // write protected by syncLock
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 读取数据的核心方法  其他reader对象都是基于初始化时传入的channel
     * @param targetBuffer
     * @param position
     * @throws IOException
     */
    @Override
    protected void readBytes(ByteBuffer targetBuffer, long position) throws IOException {
        try {
            // 代表有足够的空间
            if (position + targetBuffer.remaining() > getWrittenOffset()) {
                synchronized (this) {
                    // we only flush here if it's really really needed - try to minimize the impact of the read operation
                    // in some cases ie. a tragic event we might still be able to read the relevant value
                    // which is not really important in production but some test can make most strict assumptions
                    // if we don't fail in this call unless absolutely necessary.
                    if (position + targetBuffer.remaining() > getWrittenOffset()) {
                        // 将数据都写入到channel中
                        outputStream.flush();
                    }
                }
            }
        } catch (final Exception ex) {
            closeWithTragicEvent(ex);
            throw ex;
        }
        // we don't have to have a lock here because we only write ahead to the file, so all writes has been complete
        // for the requested location.
        // 从channel中读取数据到buffer中
        Channels.readFromFileChannelWithEofException(channel, position, targetBuffer);
    }

    /**
     * @param channelFactory
     * @param translogFile   事务文件的上层目录
     * @param checkpoint     本次需要写入到文件中的检查点
     * @throws IOException
     */
    private static void writeCheckpoint(
        final ChannelFactory channelFactory,
        final Path translogFile,
        final Checkpoint checkpoint) throws IOException {
        Checkpoint.write(channelFactory, translogFile.resolve(Translog.CHECKPOINT_FILE_NAME), checkpoint, StandardOpenOption.WRITE);
    }

    /**
     * The last synced checkpoint for this translog.
     *
     * @return the last synced checkpoint
     */
    Checkpoint getLastSyncedCheckpoint() {
        return lastSyncedCheckpoint;
    }

    protected final void ensureOpen() {
        if (isClosed()) {
            throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed", tragedy.get());
        }
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


    private final class BufferedChannelOutputStream extends BufferedOutputStream {

        BufferedChannelOutputStream(OutputStream out, int size) throws IOException {
            super(out, size);
        }

        @Override
        public synchronized void flush() throws IOException {
            if (count > 0) {
                try {
                    ensureOpen();
                    super.flush();
                } catch (final Exception ex) {
                    closeWithTragicEvent(ex);
                    throw ex;
                }
            }
        }

        @Override
        public void close() throws IOException {
            // the stream is intentionally not closed because
            // closing it will close the FileChannel
            throw new IllegalStateException("never close this stream");
        }
    }

}
