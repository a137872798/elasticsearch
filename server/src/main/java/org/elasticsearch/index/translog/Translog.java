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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Translog is a per index shard component that records all non-committed index operations in a durable manner.
 * In Elasticsearch there is one Translog instance per {@link org.elasticsearch.index.engine.InternalEngine}.
 * Additionally, since Elasticsearch 2.0 the engine also records a {@link #TRANSLOG_UUID_KEY} with each commit to ensure a strong
 * association between the lucene index an the transaction log file. This UUID is used to prevent accidental recovery from a transaction
 * log that belongs to a
 * different engine.
 * <p>
 * Each Translog has only one translog file open for writes at any time referenced by a translog generation ID. This ID is written to a
 * {@code translog.ckp} file that is designed to fit in a single disk block such that a write of the file is atomic. The checkpoint file
 * is written on each fsync operation of the translog and records the number of operations written, the current translog's file generation,
 * its fsynced offset in bytes, and other important statistics.
 * </p>
 * <p>
 * When the current translog file reaches a certain size ({@link IndexSettings#INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING}, or when
 * a clear separation between old and new operations (upon change in primary term), the current file is reopened for read only and a new
 * write only file is created. Any non-current, read only translog file always has a {@code translog-${gen}.ckp} associated with it
 * which is an fsynced copy of its last {@code translog.ckp} such that in disaster recovery last fsynced offsets, number of
 * operation etc. are still preserved.
 * </p>
 * 事务日志  es的每次操作应该都会记录日志 便于还原数据
 * 这种模型在mysql redis中也有使用
 * 在强一致性场景下 原本每执行一个操作都需要确保数据的持久化 但是这是一种重操作 耗费极大资源 且耗时长
 * 而记录操作日志就是一种最轻量级的操作   每当机器重启时 可以根据操作日志重跑相关操作 实现数据的恢复
 * 存储操作日志也有优化点  如果要重跑所有操作日志也会费时 所以比较好的方式 就是定期生成快照 在该时间点前的数据就可以被删除 而后面的操作日志还需要保留
 * 就对应redis2种日志存储方式
 * kafka 有操作日志么???
 *
 * Translog 是以shard为单位的
 */
public class Translog extends AbstractIndexShardComponent implements IndexShardComponent, Closeable {

    /*
     * TODO
     *  - we might need something like a deletion policy to hold on to more than one translog eventually (I think sequence IDs needs this)
     *    but we can refactor as we go
     *  - use a simple BufferedOutputStream to write stuff and fold BufferedTranslogWriter into it's super class... the tricky bit is we
     *    need to be able to do random access reads even from the buffer
     *  - we need random exception on the FileSystem API tests for all this.
     *  - we need to page align the last write before we sync, we can take advantage of ensureSynced for this since we might have already
     *    fsynced far enough
     */
    public static final String TRANSLOG_UUID_KEY = "translog_uuid";
    public static final String TRANSLOG_FILE_PREFIX = "translog-";
    public static final String TRANSLOG_FILE_SUFFIX = ".tlog";
    public static final String CHECKPOINT_SUFFIX = ".ckp";
    public static final String CHECKPOINT_FILE_NAME = "translog" + CHECKPOINT_SUFFIX;

    /**
     * 检测文件名是否以 translog- 开头  .tlog 结尾
     */
    static final Pattern PARSE_STRICT_ID_PATTERN = Pattern.compile("^" + TRANSLOG_FILE_PREFIX + "(\\d+)(\\.tlog)$");

    /**
     * 之后写入的事务头长度
     */
    public static final int DEFAULT_HEADER_SIZE_IN_BYTES = TranslogHeader.headerSizeInBytes(UUIDs.randomBase64UUID());

    // the list of translog readers is guaranteed to be in order of translog generation
    private final List<TranslogReader> readers = new ArrayList<>();
    private BigArrays bigArrays;

    // 在忽略断言的情况下  ReleasableLock 等同于 jdk.Lock
    protected final ReleasableLock readLock;
    protected final ReleasableLock writeLock;
    private final Path location;

    /**
     * 该对象负责往事务日志中写入数据
     */
    private TranslogWriter current;

    protected final TragicExceptionHolder tragedy = new TragicExceptionHolder();
    private final AtomicBoolean closed = new AtomicBoolean();
    /**
     * 包含 shardId 信息 以及分配内存的 BigArrays对象
     */
    private final TranslogConfig config;

    /**
     * 全局检查点是什么
     */
    private final LongSupplier globalCheckpointSupplier;

    /**
     * 这个应该是类似版本号的东西
     */
    private final LongSupplier primaryTermSupplier;


    private final String translogUUID;

    /**
     * 里面就包含了一个 Map<gen,counter> 还不清楚怎么用
     */
    private final TranslogDeletionPolicy deletionPolicy;
    private final LongConsumer persistedSequenceNumberConsumer;

    /**
     * Creates a new Translog instance. This method will create a new transaction log unless the given {@link TranslogGeneration} is
     * {@code null}. If the generation is {@code null} this method is destructive and will delete all files in the translog path given. If
     * the generation is not {@code null}, this method tries to open the given translog generation. The generation is treated as the last
     * generation referenced from already committed data. This means all operations that have not yet been committed should be in the
     * translog file referenced by this generation. The translog creation will fail if this generation can't be opened.
     *
     * @param config                   the configuration of this translog     包含了相关的参数信息
     * @param translogUUID             the translog uuid to open, null for a new translog   为该事务文件分配的uuid
     * @param deletionPolicy           an instance of {@link TranslogDeletionPolicy} that controls when a translog file can be safely
     *                                 deleted
     * @param globalCheckpointSupplier a supplier for the global checkpoint
     * @param primaryTermSupplier      a supplier for the latest value of primary term of the owning index shard. The latest term value is
     *                                 examined and stored in the header whenever a new generation is rolled. It's guaranteed from outside
     *                                 that a new generation is rolled when the term is increased. This guarantee allows to us to validate
     *                                 and reject operation whose term is higher than the primary term stored in the translog header.
     * @param persistedSequenceNumberConsumer a callback that's called whenever an operation with a given sequence number is successfully
     *                                        persisted.
     */
    public Translog(
        final TranslogConfig config, final String translogUUID, TranslogDeletionPolicy deletionPolicy,
        final LongSupplier globalCheckpointSupplier, final LongSupplier primaryTermSupplier,
        final LongConsumer persistedSequenceNumberConsumer) throws IOException {
        super(config.getShardId(), config.getIndexSettings());
        this.config = config;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.primaryTermSupplier = primaryTermSupplier;
        this.persistedSequenceNumberConsumer = persistedSequenceNumberConsumer;
        this.deletionPolicy = deletionPolicy;
        this.translogUUID = translogUUID;
        bigArrays = config.getBigArrays();
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
        this.location = config.getTranslogPath();

        // 初始化的同时创建目录对象
        Files.createDirectories(this.location);

        try {

            // 先检测目录下是否存在 checkPoint文件 存在则读取检查点信息  不存在会抛出异常 奇怪了 一开始不一定存在checkPoint文件的啊
            // 这个就是全局检查点吗
            final Checkpoint checkpoint = readCheckpoint(location);
            // 生成新gen对应的事务文件
            final Path nextTranslogFile = location.resolve(getFilename(checkpoint.generation + 1));
            // 获取之前gen对应的检查点文件 文件名上会携带 gen信息
            final Path currentCheckpointFile = location.resolve(getCommitCheckpointFileName(checkpoint.generation));
            // this is special handling for error condition when we create a new writer but we fail to bake
            // the newly written file (generation+1) into the checkpoint. This is still a valid state
            // we just need to cleanup before we continue
            // we hit this before and then blindly deleted the new generation even though we managed to bake it in and then hit this:
            // https://discuss.elastic.co/t/cannot-recover-index-because-of-missing-tanslog-files/38336 as an example
            //
            // For this to happen we must have already copied the translog.ckp file into translog-gen.ckp so we first check if that
            // file exists. If not we don't even try to clean it up and wait until we fail creating it
            assert Files.exists(nextTranslogFile) == false ||
                    Files.size(nextTranslogFile) <= TranslogHeader.headerSizeInBytes(translogUUID) :
                        "unexpected translog file: [" + nextTranslogFile + "]";
            if (Files.exists(currentCheckpointFile) // current checkpoint is already copied
                && Files.deleteIfExists(nextTranslogFile)) { // delete it and log a warning
                logger.warn("deleted previously created, but not yet committed, next generation [{}]. This can happen due to a" +
                    " tragic exception when creating a new generation", nextTranslogFile.getFileName());
            }
            // 根据全局检查点此时的信息 为有效的 translog-gen 生成reader对象
            this.readers.addAll(recoverFromFiles(checkpoint));
            if (readers.isEmpty()) {
                throw new IllegalStateException("at least one reader must be recovered");
            }
            boolean success = false;
            current = null;
            try {
                // 每次生成事务对象后  都会增加gen 并生成一个专门写入 translog_newgen 文件的writer对象
                // writer在初始化前 还会更新全局检查点对象
                current = createWriter(checkpoint.generation + 1, getMinFileGeneration(), checkpoint.globalCheckpoint,
                    persistedSequenceNumberConsumer);
                success = true;
            } finally {
                // we have to close all the recovered ones otherwise we leak file handles here
                // for instance if we have a lot of tlog and we can't create the writer we keep on holding
                // on to all the uncommitted tlog files if we don't close
                if (success == false) {
                    IOUtils.closeWhileHandlingException(readers);
                }
            }
        } catch (Exception e) {
            // close the opened translog files if we fail to create a new translog...
            IOUtils.closeWhileHandlingException(current);
            IOUtils.closeWhileHandlingException(readers);
            throw e;
        }
    }

    /**
     * recover all translog files found on disk
     * 从一个全局检查点找到此时包含的处于不同gen的所有translog.tlg (也就是记录operation的文件)  并且生成对应的reader对象
     */
    private ArrayList<TranslogReader> recoverFromFiles(Checkpoint checkpoint) throws IOException {
        boolean success = false;
        ArrayList<TranslogReader> foundTranslogs = new ArrayList<>();
        try (ReleasableLock ignored = writeLock.acquire()) {
            logger.debug("open uncommitted translog checkpoint {}", checkpoint);

            // 获取这些事务文件中最小的gen
            final long minGenerationToRecoverFrom = checkpoint.minTranslogGeneration;
            assert minGenerationToRecoverFrom >= 0 : "minTranslogGeneration should be non-negative";

            // we open files in reverse order in order to validate the translog uuid before we start traversing the translog based on
            // the generation id we found in the lucene commit. This gives for better error messages if the wrong
            // translog was found.
            // 根据gen从大到小 读取文件并生成reader对象
            for (long i = checkpoint.generation; i >= minGenerationToRecoverFrom; i--) {

                // 定位到某个gen的 事务文件路径
                Path committedTranslogFile = location.resolve(getFilename(i));
                if (Files.exists(committedTranslogFile) == false) {
                    throw new TranslogCorruptedException(committedTranslogFile.toString(),
                        "translog file doesn't exist with generation: " + i + " recovering from: " + minGenerationToRecoverFrom
                            + " checkpoint: " + checkpoint.generation + " - translog ids must be consecutive");
                }
                // 代表此时最新的检查点文件  它的信息应该跟全局检查点是一样的
                final Checkpoint readerCheckpoint = i == checkpoint.generation ? checkpoint
                    // 否则解析对应gen的检查点对象
                    : Checkpoint.read(location.resolve(getCommitCheckpointFileName(i)));

                // 根据相关信息创建 translogReader
                final TranslogReader reader = openReader(committedTranslogFile, readerCheckpoint);
                assert reader.getPrimaryTerm() <= primaryTermSupplier.getAsLong() :
                    "Primary terms go backwards; current term [" + primaryTermSupplier.getAsLong() + "] translog path [ "
                        + committedTranslogFile + ", existing term [" + reader.getPrimaryTerm() + "]";
                foundTranslogs.add(reader);
                logger.debug("recovered local translog from checkpoint {}", checkpoint);
            }
            // 将reader按照gen从小到大的顺序排序
            Collections.reverse(foundTranslogs);

            // when we clean up files, we first update the checkpoint with a new minReferencedTranslog and then delete them;
            // if we crash just at the wrong moment, it may be that we leave one unreferenced file behind so we delete it if there
            // 删除全局检查点记录的最小gen之前的 事务文件/提交点文件
            IOUtils.deleteFilesIgnoringExceptions(location.resolve(getFilename(minGenerationToRecoverFrom - 1)),
                location.resolve(getCommitCheckpointFileName(minGenerationToRecoverFrom - 1)));

            // 最大gen对应的检查点对象必须与全局检查点一致
            Path commitCheckpoint = location.resolve(getCommitCheckpointFileName(checkpoint.generation));
            if (Files.exists(commitCheckpoint)) {
                Checkpoint checkpointFromDisk = Checkpoint.read(commitCheckpoint);
                if (checkpoint.equals(checkpointFromDisk) == false) {
                    throw new TranslogCorruptedException(commitCheckpoint.toString(),
                        "checkpoint file " + commitCheckpoint.getFileName() + " already exists but has corrupted content: expected "
                            + checkpoint + " but got " + checkpointFromDisk);
                }
            } else {
                copyCheckpointTo(commitCheckpoint);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(foundTranslogs);
            }
        }
        return foundTranslogs;
    }

    /**
     * 当 最新的gen对应检查点文件不存在时  以全局检查点文件作为原始文件 并拷贝一份副本
     * @param targetPath
     * @throws IOException
     */
    private void copyCheckpointTo(Path targetPath) throws IOException {
        // a temp file to copy checkpoint to - note it must be in on the same FS otherwise atomic move won't work
        // 创建临时文件
        final Path tempFile = Files.createTempFile(location, TRANSLOG_FILE_PREFIX, CHECKPOINT_SUFFIX);
        boolean tempFileRenamed = false;

        try {
            // we first copy this into the temp-file and then fsync it followed by an atomic move into the target file
            // that way if we hit a disk-full here we are still in an consistent state.
            Files.copy(location.resolve(CHECKPOINT_FILE_NAME), tempFile, StandardCopyOption.REPLACE_EXISTING);
            IOUtils.fsync(tempFile, false);
            Files.move(tempFile, targetPath, StandardCopyOption.ATOMIC_MOVE);
            tempFileRenamed = true;
            // we only fsync the directory the tempFile was already fsynced
            IOUtils.fsync(targetPath.getParent(), true);
        } finally {
            if (tempFileRenamed == false) {
                try {
                    Files.delete(tempFile);
                } catch (IOException ex) {
                    logger.warn(() -> new ParameterizedMessage("failed to delete temp file {}", tempFile), ex);
                }
            }
        }
    }

    /**
     * 根据某个检查点文件 以及 事务文件 创建reader对象
     * @param path 对应存储operation的文件路径
     * @param checkpoint  对应gen匹配的检查点对象
     * @return
     * @throws IOException
     */
    TranslogReader openReader(Path path, Checkpoint checkpoint) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            assert Translog.parseIdFromFileName(path) == checkpoint.generation : "expected generation: " +
                Translog.parseIdFromFileName(path) + " but got: " + checkpoint.generation;
            TranslogReader reader = TranslogReader.open(channel, path, checkpoint, translogUUID);
            channel = null;
            return reader;
        } finally {
            // 这里应该只是检查文件是否可以正常打开吧
            IOUtils.close(channel);
        }
    }

    /**
     * Extracts the translog generation from a file name.
     *
     * @throws IllegalArgumentException if the path doesn't match the expected pattern.
     */
    public static long parseIdFromFileName(Path translogFile) {
        // 之前涉及到的都是检查点文件  现在是有关操作文件
        final String fileName = translogFile.getFileName().toString();
        final Matcher matcher = PARSE_STRICT_ID_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            try {
                // 将数字解析出来作为id
                return Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                throw new IllegalStateException("number formatting issue in a file that passed PARSE_STRICT_ID_PATTERN: " +
                    fileName + "]", e);
            }
        }
        throw new IllegalArgumentException("can't parse id from file: " + fileName);
    }

    /** Returns {@code true} if this {@code Translog} is still open. */
    public boolean isOpen() {
        return closed.get() == false;
    }

    private static boolean calledFromOutsideOrViaTragedyClose() {
        List<StackTraceElement> frames = Stream.of(Thread.currentThread().getStackTrace()).
                skip(3). //skip getStackTrace, current method and close method frames
                limit(10). //limit depth of analysis to 10 frames, it should be enough to catch closing with, e.g. IOUtils
                filter(f ->
                    {
                        try {
                            return Translog.class.isAssignableFrom(Class.forName(f.getClassName()));
                        } catch (Exception ignored) {
                            return false;
                        }
                    }
                ). //find all inner callers including Translog subclasses
                collect(Collectors.toList());
        //the list of inner callers should be either empty or should contain closeOnTragicEvent method
        return frames.isEmpty() || frames.stream().anyMatch(f -> f.getMethodName().equals("closeOnTragicEvent"));
    }

    /**
     * 在关闭该对象前 先将数据刷盘
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        assert calledFromOutsideOrViaTragedyClose() :
                "Translog.close method is called from inside Translog, but not via closeOnTragicEvent method";
        if (closed.compareAndSet(false, true)) {
            try (ReleasableLock lock = writeLock.acquire()) {
                try {
                    current.sync();
                } finally {
                    closeFilesIfNoPendingRetentionLocks();
                }
            } finally {
                logger.debug("translog closed");
            }
        }
    }

    /**
     * Returns all translog locations as absolute paths.
     * These paths don't contain actual translog files they are
     * directories holding the transaction logs.
     */
    public Path location() {
        return location;
    }

    /**
     * Returns the generation of the current transaction log.
     */
    public long currentFileGeneration() {
        try (ReleasableLock ignored = readLock.acquire()) {
            return current.getGeneration();
        }
    }

    /**
     * Returns the minimum file generation referenced by the translog
     */
    public long getMinFileGeneration() {
        try (ReleasableLock ignored = readLock.acquire()) {
            if (readers.isEmpty()) {
                return current.getGeneration();
            } else {
                assert readers.stream().map(TranslogReader::getGeneration).min(Long::compareTo).get()
                    .equals(readers.get(0).getGeneration()) : "the first translog isn't the one with the minimum generation:" + readers;
                return readers.get(0).getGeneration();
            }
        }
    }

    /**
     * Returns the number of operations in the translog files
     */
    public int totalOperations() {
        // 传入-1 就代表获取所有checkpoint文件的操作数
        return totalOperationsByMinGen(-1);
    }

    /**
     * Returns the size in bytes of the v files
     * 计算总大小
     */
    public long sizeInBytes() {
        return sizeInBytesByMinGen(-1);
    }

    long earliestLastModifiedAge() {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return findEarliestLastModifiedAge(System.currentTimeMillis(), readers, current);
        } catch (IOException e) {
            throw new TranslogException(shardId, "Unable to get the earliest last modified time for the transaction log");
        }
    }

    /**
     * Returns the age of the oldest entry in the translog files in seconds
     * 当前时间距离最旧的事务文件 隔了多久
     */
    static long findEarliestLastModifiedAge(long currentTime, Iterable<TranslogReader> readers, TranslogWriter writer) throws IOException {
        long earliestTime = currentTime;
        for (BaseTranslogReader r : readers) {
            earliestTime = Math.min(r.getLastModifiedTime(), earliestTime);
        }
        return Math.max(0, currentTime - Math.min(earliestTime, writer.getLastModifiedTime()));
    }

    /**
     * Returns the number of operations in the translog files at least the given generation
     * 获取指定gen之上的所有检查点文件记录的总操作数
     */
    public int totalOperationsByMinGen(long minGeneration) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            // 包含writer
            return Stream.concat(readers.stream(), Stream.of(current))
                .filter(r -> r.getGeneration() >= minGeneration)
                .mapToInt(BaseTranslogReader::totalOperations)
                .sum();
        }
    }

    /**
     * Returns the number of operations in the transaction files that contain operations with seq# above the given number.
     * 获取在指定seq之上的所有reader对象此时记录的operation总数之和
     */
    public int estimateTotalOperationsFromMinSeq(long minSeqNo) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return readersAboveMinSeqNo(minSeqNo).mapToInt(BaseTranslogReader::totalOperations).sum();
        }
    }

    /**
     * Returns the size in bytes of the translog files at least the given generation
     */
    public long sizeInBytesByMinGen(long minGeneration) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return Stream.concat(readers.stream(), Stream.of(current))
                .filter(r -> r.getGeneration() >= minGeneration)
                // 对于translogReader来说 每个对应的checkPoint.offset 就是事务文件长度
                // 对于translogWriter来说 就是他每次写入都还在更新checkpoint的值 内部会记录当前文件总长度 以及写入了多少operation
                .mapToLong(BaseTranslogReader::sizeInBytes)
                .sum();
        }
    }

    /**
     * Creates a new translog for the specified generation.
     *
     * @param fileGeneration the translog generation
     * @return a writer for the new translog
     * @throws IOException if creating the translog failed
     */
    TranslogWriter createWriter(long fileGeneration) throws IOException {
        final TranslogWriter writer = createWriter(fileGeneration, getMinFileGeneration(), globalCheckpointSupplier.getAsLong(),
            persistedSequenceNumberConsumer);
        assert writer.sizeInBytes() == DEFAULT_HEADER_SIZE_IN_BYTES : "Mismatch translog header size; " +
            "empty translog size [" + writer.sizeInBytes() + ", header size [" + DEFAULT_HEADER_SIZE_IN_BYTES + "]";
        return writer;
    }

    /**
     * creates a new writer
     *
     * @param fileGeneration          the generation of the write to be written   该writer写入事务文件时的gen
     * @param initialMinTranslogGen   the minimum translog generation to be written in the first checkpoint. This is
     *                                needed to solve and initialization problem while constructing an empty translog.
     *                                With no readers and no current, a call to  {@link #getMinFileGeneration()} would not work.
     *                                获取此时目录下可以观测到的最小的gen  对应最小的reader对象
     *
     * @param initialGlobalCheckpoint the global checkpoint to be written in the first checkpoint.
     *                                全局检查点是什么玩意啊 不是那个不携带gen的checkpoint文件么
     */
    TranslogWriter createWriter(long fileGeneration, long initialMinTranslogGen, long initialGlobalCheckpoint,
                                LongConsumer persistedSequenceNumberConsumer) throws IOException {
        final TranslogWriter newFile;
        try {
            newFile = TranslogWriter.create(
                shardId,
                translogUUID,
                fileGeneration,
                // 写入的是事务文件 不是提交点文件
                location.resolve(getFilename(fileGeneration)),
                getChannelFactory(),
                config.getBufferSize(),
                initialMinTranslogGen, initialGlobalCheckpoint,
                globalCheckpointSupplier, this::getMinFileGeneration, primaryTermSupplier.getAsLong(), tragedy,
                persistedSequenceNumberConsumer);
        } catch (final IOException e) {
            throw new TranslogException(shardId, "failed to create new translog file", e);
        }
        return newFile;
    }

    /**
     * Adds an operation to the transaction log.
     *
     * @param operation the operation to add
     * @return the location of the operation in the translog
     * @throws IOException if adding the operation to the translog resulted in an I/O exception
     * 开始往writer中追加一个新的操作记录
     */
    public Location add(final Operation operation) throws IOException {
        //  基于池化技术获取一块可回收的byte[]并包装成输出流  （实际上内部是二级数组在逻辑上形成大数组）
        final ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(bigArrays);
        try {
            // 之后会回到start并回填长度
            final long start = out.position();
            // 这个空位是用于写入长度的
            out.skip(Integer.BYTES);
            writeOperationNoSize(new BufferedChecksumStreamOutput(out), operation);
            final long end = out.position();
            final int operationSize = (int) (end - Integer.BYTES - start);
            out.seek(start);
            out.writeInt(operationSize);
            out.seek(end);
            final ReleasableBytesReference bytes = out.bytes();
            try (ReleasableLock ignored = readLock.acquire()) {
                ensureOpen();
                // TODO 这里term到底怎么用
                if (operation.primaryTerm() > current.getPrimaryTerm()) {
                    assert false :
                        "Operation term is newer than the current term; "
                            + "current term[" + current.getPrimaryTerm() + "], operation term[" + operation + "]";
                    throw new IllegalArgumentException("Operation term is newer than the current term; "
                        + "current term[" + current.getPrimaryTerm() + "], operation term[" + operation + "]");
                }
                // 每个操作都是有seq的  offset相当于是文件长度 为每个事务文件由多个operation数据组成 每个operation对应一个序列号
                return current.add(bytes, operation.seqNo());
            }
        } catch (final AlreadyClosedException | IOException ex) {
            closeOnTragicEvent(ex);
            throw ex;
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw new TranslogException(shardId, "Failed to write operation [" + operation + "]", ex);
        } finally {
            Releasables.close(out);
        }
    }

    /**
     * Tests whether or not the translog generation should be rolled to a new generation. This test
     * is based on the size of the current generation compared to the configured generation
     * threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     * 每个gen都只允许写入一定的operation量 当超标时推荐进行roll
     * roll就是滚动到下个文件 也就是增加gen
     */
    public boolean shouldRollGeneration() {
        final long threshold = this.indexSettings.getGenerationThresholdSize().getBytes();
        try (ReleasableLock ignored = readLock.acquire()) {
            return this.current.sizeInBytes() > threshold;
        }
    }

    /**
     * The a {@linkplain Location} that will sort after the {@linkplain Location} returned by the last write but before any locations which
     * can be returned by the next write.
     * 返回最后一个operation对应的位置
     */
    public Location getLastWriteLocation() {
        try (ReleasableLock lock = readLock.acquire()) {
            /*
             * We use position = current - 1 and size = Integer.MAX_VALUE here instead of position current and size = 0 for two reasons:
             * 1. Translog.Location's compareTo doesn't actually pay attention to size even though it's equals method does.
             * 2. It feels more right to return a *position* that is before the next write's position rather than rely on the size.
             */
            return new Location(current.generation, current.sizeInBytes() - 1, Integer.MAX_VALUE);
        }
    }

    /**
     * The last synced checkpoint for this translog.
     *
     * @return the last synced checkpoint
     * 返回最新的checkPoint对应的全局检查点
     */
    public long getLastSyncedGlobalCheckpoint() {
        return getLastSyncedCheckpoint().globalCheckpoint;
    }

    final Checkpoint getLastSyncedCheckpoint() {
        try (ReleasableLock ignored = readLock.acquire()) {
            return current.getLastSyncedCheckpoint();
        }
    }

    // for testing
    public Snapshot newSnapshot() throws IOException {
        return newSnapshot(0, Long.MAX_VALUE);
    }

    /**
     * Creates a new translog snapshot containing operations from the given range.
     *
     * @param fromSeqNo the lower bound of the range (inclusive)
     * @param toSeqNo   the upper bound of the range (inclusive)
     * @return the new snapshot
     *
     */
    public Snapshot newSnapshot(long fromSeqNo, long toSeqNo) throws IOException {
        assert fromSeqNo <= toSeqNo : fromSeqNo + " > " + toSeqNo;
        assert fromSeqNo >= 0 : "from_seq_no must be non-negative " + fromSeqNo;
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            TranslogSnapshot[] snapshots = Stream.concat(readers.stream(), Stream.of(current))
                .filter(reader -> reader.getCheckpoint().minSeqNo <= toSeqNo && fromSeqNo <= reader.getCheckpoint().maxEffectiveSeqNo())
                // 反正就是将在seq合理范围内的所有reader对象都创建一个快照
                .map(BaseTranslogReader::newSnapshot).toArray(TranslogSnapshot[]::new);
            // 将多个事务文件的快照合并成功一个全局快照对象  它可以遍历下面的所有operation
            final Snapshot snapshot = newMultiSnapshot(snapshots);
            return new SeqNoFilterSnapshot(snapshot, fromSeqNo, toSeqNo);
        }
    }

    /**
     * Reads and returns the operation from the given location if the generation it references is still available. Otherwise
     * this method will return <code>null</code>.
     * 通过指定某个位置 获取对应的operation数据
     */
    public Operation readOperation(Location location) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            // 如果记录对应的gen 比当前存在的事务文件都要早代表要追溯的记录已经被删除了
            if (location.generation < getMinFileGeneration()) {
                return null;
            }
            if (current.generation == location.generation) {
                // no need to fsync here the read operation will ensure that buffers are written to disk
                // if they are still in RAM and we are reading onto that position
                return current.read(location);
            } else {
                // read backwards - it's likely we need to read on that is recent
                for (int i = readers.size() - 1; i >= 0; i--) {
                    TranslogReader translogReader = readers.get(i);
                    if (translogReader.generation == location.generation) {
                        return translogReader.read(location);
                    }
                }
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
        return null;
    }

    private Snapshot newMultiSnapshot(TranslogSnapshot[] snapshots) throws IOException {
        final Closeable onClose;
        if (snapshots.length == 0) {
            onClose = () -> {};
        } else {
            assert Arrays.stream(snapshots).map(BaseTranslogReader::getGeneration).min(Long::compareTo).get()
                == snapshots[0].generation : "first reader generation of " + snapshots + " is not the smallest";
            // 这里只针对最小的gen 生成了计数器对象  应该是在删除事务文件时使用
            onClose = acquireTranslogGenFromDeletionPolicy(snapshots[0].generation);
        }
        boolean success = false;
        try {
            Snapshot result = new MultiSnapshot(snapshots, onClose);
            success = true;
            return result;
        } finally {
            if (success == false) {
                onClose.close();
            }
        }
    }

    /**
     * 返回在该seq之上的所有reader对象 (writer也是一种reader对象)
     * @param minSeqNo
     * @return
     */
    private Stream<? extends BaseTranslogReader> readersAboveMinSeqNo(long minSeqNo) {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread() :
            "callers of readersAboveMinSeqNo must hold a lock: readLock ["
                + readLock.isHeldByCurrentThread() + "], writeLock [" + readLock.isHeldByCurrentThread() + "]";
        return Stream.concat(readers.stream(), Stream.of(current)).filter(reader -> minSeqNo <= reader.getCheckpoint().maxEffectiveSeqNo());
    }

    /**
     * Acquires a lock on the translog files, preventing them from being trimmed
     * 获取保留锁  防止最小的gen文件本删除 (最小的无法删除 再往上自然也无法删除)
     * 这个锁应该是外部调用的 并且在合适的时机触发 并删除旧文件
     */
    public Closeable acquireRetentionLock() {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            final long viewGen = getMinFileGeneration();
            return acquireTranslogGenFromDeletionPolicy(viewGen);
        }
    }

    /**
     * 根据此时最小的gen 生成一个关闭钩子
     * @param viewGen
     * @return
     */
    private Closeable acquireTranslogGenFromDeletionPolicy(long viewGen) {
        // 这里生成计数器对象 并在close时释放计数
        Releasable toClose = deletionPolicy.acquireTranslogGen(viewGen);
        return () -> {
            try {
                toClose.close();
            } finally {
                trimUnreferencedReaders();
                // 如果没有文件被使用 就关闭channel
                closeFilesIfNoPendingRetentionLocks();
            }
        };
    }

    /**
     * Sync's the translog.
     */
    public void sync() throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (closed.get() == false) {
                current.sync();
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
    }

    /**
     *  Returns <code>true</code> if an fsync is required to ensure durability of the translogs operations or it's metadata.
     *  通过writer对象的检查点信息是否发生变化检测是否有刷盘的必要
     */
    public boolean syncNeeded() {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.syncNeeded();
        }
    }

    /** package private for testing */
    public static String getFilename(long generation) {
        return TRANSLOG_FILE_PREFIX + generation + TRANSLOG_FILE_SUFFIX;
    }

    static String getCommitCheckpointFileName(long generation) {
        return TRANSLOG_FILE_PREFIX + generation + CHECKPOINT_SUFFIX;
    }

    /**
     * Trims translog for terms of files below <code>belowTerm</code> and seq# above <code>aboveSeqNo</code>.
     * Effectively it moves max visible seq# {@link Checkpoint#trimmedAboveSeqNo} therefore {@link TranslogSnapshot} skips those operations.
     * 除了以reader为单位进行裁剪 还可以以 operation 进行裁剪
     */
    public void trimOperations(long belowTerm, long aboveSeqNo) throws IOException {
        assert aboveSeqNo >= SequenceNumbers.NO_OPS_PERFORMED : "aboveSeqNo has to a valid sequence number";

        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            if (current.getPrimaryTerm() < belowTerm) {
                throw new IllegalArgumentException("Trimming the translog can only be done for terms lower than the current one. " +
                    "Trim requested for term [ " + belowTerm + " ] , current is [ " + current.getPrimaryTerm() + " ]");
            }
            // we assume that the current translog generation doesn't have trimmable ops. Verify that.
            assert current.assertNoSeqAbove(belowTerm, aboveSeqNo);
            // update all existed ones (if it is necessary) as checkpoint and reader are immutable
            final List<TranslogReader> newReaders = new ArrayList<>(readers.size());
            try {
                for (TranslogReader reader : readers) {
                    final TranslogReader newReader =
                        // 每个事务文件都有一个term 本次函数只处理term低于参数的
                        reader.getPrimaryTerm() < belowTerm
                            ? reader.closeIntoTrimmedReader(aboveSeqNo, getChannelFactory())
                            : reader;
                    newReaders.add(newReader);
                }
            } catch (IOException e) {
                IOUtils.closeWhileHandlingException(newReaders);
                tragedy.setTragicException(e);
                closeOnTragicEvent(e);
                throw e;
            }

            this.readers.clear();
            this.readers.addAll(newReaders);
        }
    }

    /**
     * Ensures that the given location has be synced / written to the underlying storage.
     *
     * @return Returns <code>true</code> iff this call caused an actual sync operation otherwise <code>false</code>
     * 根据这个偏移量判断是否需要刷盘
     */
    public boolean ensureSynced(Location location) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (location.generation == current.getGeneration()) { // if we have a new one it's already synced
                ensureOpen();
                return current.syncUpTo(location.translogLocation + location.size);
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
        return false;
    }

    /**
     * Ensures that all locations in the given stream have been synced / written to the underlying storage.
     * This method allows for internal optimization to minimize the amount of fsync operations if multiple
     * locations must be synced.
     *
     * @return Returns <code>true</code> iff this call caused an actual sync operation otherwise <code>false</code>
     */
    public boolean ensureSynced(Stream<Location> locations) throws IOException {
        final Optional<Location> max = locations.max(Location::compareTo);
        // we only need to sync the max location since it will sync all other
        // locations implicitly
        if (max.isPresent()) {
            return ensureSynced(max.get());
        } else {
            return false;
        }
    }

    /**
     * Closes the translog if the current translog writer experienced a tragic exception.
     *
     * Note that in case this thread closes the translog it must not already be holding a read lock on the translog as it will acquire a
     * write lock in the course of closing the translog
     *
     * @param ex if an exception occurs closing the translog, it will be suppressed into the provided exception
     */
    protected void closeOnTragicEvent(final Exception ex) {
        // we can not hold a read lock here because closing will attempt to obtain a write lock and that would result in self-deadlock
        assert readLock.isHeldByCurrentThread() == false : Thread.currentThread().getName();
        if (tragedy.get() != null) {
            try {
                close();
            } catch (final AlreadyClosedException inner) {
                /*
                 * Don't do anything in this case. The AlreadyClosedException comes from TranslogWriter and we should not add it as
                 * suppressed because it will contain the provided exception as its cause. See also
                 * https://github.com/elastic/elasticsearch/issues/15941.
                 */
            } catch (final Exception inner) {
                assert ex != inner.getCause();
                ex.addSuppressed(inner);
            }
        }
    }

    /**
     * return stats
     */
    public TranslogStats stats() {
        // acquire lock to make the two numbers roughly consistent (no file change half way)
        try (ReleasableLock lock = readLock.acquire()) {
            final long uncommittedGen = minGenerationForSeqNo(deletionPolicy.getLocalCheckpointOfSafeCommit() + 1, current, readers);
            return new TranslogStats(totalOperations(), sizeInBytes(), totalOperationsByMinGen(uncommittedGen),
                sizeInBytesByMinGen(uncommittedGen), earliestLastModifiedAge());
        }
    }

    public TranslogConfig getConfig() {
        return config;
    }

    // public for testing
    public TranslogDeletionPolicy getDeletionPolicy() {
        return deletionPolicy;
    }


    /**
     * 用于定位 operation 的
     */
    public static class Location implements Comparable<Location> {

        public final long generation;
        /**
         * 记录某个operation在事务文件中的偏移量
         */
        public final long translogLocation;
        /**
         * 该operation的长度  根据不同的操作类型 存储时需要的长度也不一样
         */
        public final int size;

        public Location(long generation, long translogLocation, int size) {
            this.generation = generation;
            this.translogLocation = translogLocation;
            this.size = size;
        }

        @Override
        public String toString() {
            return "[generation: " + generation + ", location: " + translogLocation + ", size: " + size + "]";
        }

        @Override
        public int compareTo(Location o) {
            if (generation == o.generation) {
                return Long.compare(translogLocation, o.translogLocation);
            }
            return Long.compare(generation, o.generation);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Location location = (Location) o;

            if (generation != location.generation) {
                return false;
            }
            if (translogLocation != location.translogLocation) {
                return false;
            }
            return size == location.size;

        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(generation);
            result = 31 * result + Long.hashCode(translogLocation);
            result = 31 * result + size;
            return result;
        }
    }

    /**
     * A snapshot of the transaction log, allows to iterate over all the transaction log operations.
     * 定义了事务文件的某次快照
     */
    public interface Snapshot extends Closeable {

        /**
         * The total estimated number of operations in the snapshot.
         * 当前记录了多少个操作
         */
        int totalOperations();

        /**
         * The number of operations have been skipped (overridden or trimmed) in the snapshot so far.
         * Unlike {@link #totalOperations()}, this value is updated each time after {@link #next()}) is called.
         */
        default int skippedOperations() {
            return 0;
        }

        /**
         * Returns the next operation in the snapshot or <code>null</code> if we reached the end.
         * 从快照中挨个迭代操作信息
         */
        Translog.Operation next() throws IOException;
    }

    /**
     * A filtered snapshot consisting of only operations whose sequence numbers are in the given range
     * between {@code fromSeqNo} (inclusive) and {@code toSeqNo} (inclusive). This filtered snapshot
     * shares the same underlying resources with the {@code delegate} snapshot, therefore we should not
     * use the {@code delegate} after passing it to this filtered snapshot.
     * 快照的包装类 禁止读取范围外的seqNo
     */
    private static final class SeqNoFilterSnapshot implements Snapshot {
        private final Snapshot delegate;
        /**
         * 记录被拦截的获取operation次数
         */
        private int filteredOpsCount;
        private final long fromSeqNo; // inclusive
        private final long toSeqNo;   // inclusive

        SeqNoFilterSnapshot(Snapshot delegate, long fromSeqNo, long toSeqNo) {
            assert fromSeqNo <= toSeqNo : "from_seq_no[" + fromSeqNo + "] > to_seq_no[" + toSeqNo + "]";
            this.delegate = delegate;
            this.fromSeqNo = fromSeqNo;
            this.toSeqNo = toSeqNo;
        }

        @Override
        public int totalOperations() {
            return delegate.totalOperations();
        }

        @Override
        public int skippedOperations() {
            return filteredOpsCount + delegate.skippedOperations();
        }

        @Override
        public Operation next() throws IOException {
            Translog.Operation op;
            while ((op = delegate.next()) != null) {
                if (fromSeqNo <= op.seqNo() && op.seqNo() <= toSeqNo) {
                    return op;
                } else {
                    // 快速过滤直到 满足 > fromSeqNo 的基本条件
                    filteredOpsCount++;
                }
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /**
     * A generic interface representing an operation performed on the transaction log.
     * Each is associated with a type.
     * 在事务文件中使用 记录发生的某次操作
     */
    public interface Operation {

        /**
         * 描述了操作类型
         */
        enum Type {
            /**
             * CREATE 已经被 INDEX 替代了
             */
            @Deprecated
            CREATE((byte) 1),
            INDEX((byte) 2),
            DELETE((byte) 3),
            NO_OP((byte) 4);

            private final byte id;

            Type(byte id) {
                this.id = id;
            }

            public byte id() {
                return this.id;
            }

            public static Type fromId(byte id) {
                switch (id) {
                    case 1:
                        return CREATE;
                    case 2:
                        return INDEX;
                    case 3:
                        return DELETE;
                    case 4:
                        return NO_OP;
                    default:
                        throw new IllegalArgumentException("no type mapped for [" + id + "]");
                }
            }
        }

        Type opType();

        /**
         * 记录该操作信息预估的大小
         * @return
         */
        long estimateSize();

        /**
         * 从事务文件中解析数据后 会存储到一个BB 对象中 作为 Operation的source
         * @return
         */
        Source getSource();

        /**
         * 代表这是第几个操作
         * @return
         */
        long seqNo();

        /**
         * 应该是每次裁剪事务文件都会增加该值吧
         * @return
         */
        long primaryTerm();

        /**
         * Reads the type and the operation from the given stream. The operation must be written with
         * {@link Operation#writeOperation(StreamOutput, Operation)}
         * 从数据流中反序列化operation 对象
         */
        static Operation readOperation(final StreamInput input) throws IOException {
            // 第一个byte值 代表了类型
            final Translog.Operation.Type type = Translog.Operation.Type.fromId(input.readByte());
            switch (type) {
                case CREATE:
                    // the de-serialization logic in Index was identical to that of Create when create was deprecated
                case INDEX:
                    return new Index(input);
                case DELETE:
                    return new Delete(input);
                case NO_OP:
                    return new NoOp(input);
                default:
                    throw new AssertionError("no case for [" + type + "]");
            }
        }

        /**
         * Writes the type and translog operation to the given stream
         * 不同的operation 定义了序列化的逻辑
         */
        static void writeOperation(final StreamOutput output, final Operation operation) throws IOException {
            output.writeByte(operation.opType().id());
            switch(operation.opType()) {
                case CREATE:
                    // the serialization logic in Index was identical to that of Create when create was deprecated
                case INDEX:
                    ((Index) operation).write(output);
                    break;
                case DELETE:
                    ((Delete) operation).write(output);
                    break;
                case NO_OP:
                    ((NoOp) operation).write(output);
                    break;
                default:
                    throw new AssertionError("no case for [" + operation.opType() + "]");
            }
        }

    }

    /**
     * 描述某种数据来源
     */
    public static class Source {

        /**
         * 存储数据的容器
         */
        public final BytesReference source;
        /**
         * 这里又出现了路由的概念 路由到底是做什么的
         */
        public final String routing;

        public Source(BytesReference source, String routing) {
            this.source = source;
            this.routing = routing;
        }

    }

    /**
     * 代表创建索引的操作类型
     * 可以理解为一个简单的bean对象 只是存储了各种关键信息
     */
    public static class Index implements Operation {

        // format 可以理解为存储了哪些数据  越往下存储的数据越少
        public static final int FORMAT_NO_PARENT = 9; // since 7.0
        public static final int FORMAT_NO_VERSION_TYPE = FORMAT_NO_PARENT + 1;
        public static final int FORMAT_NO_DOC_TYPE = FORMAT_NO_VERSION_TYPE + 1;
        public static final int SERIALIZATION_FORMAT = FORMAT_NO_DOC_TYPE;

        private final String id;
        private final long autoGeneratedIdTimestamp;
        private final long seqNo;
        private final long primaryTerm;
        private final long version;
        private final BytesReference source;
        /**
         * index 为什么包含路由信息
         */
        private final String routing;

        /**
         * 从数据流中还原一个 Index 操作
         * @param in
         * @throws IOException
         */
        private Index(final StreamInput in) throws IOException {
            final int format = in.readVInt(); // SERIALIZATION_FORMAT
            assert format >= FORMAT_NO_PARENT : "format was: " + format;
            id = in.readString();
            if (format < FORMAT_NO_DOC_TYPE) {
                // 这个就是docType
                in.readString();
                // can't assert that this is _doc because pre-8.0 indexes can have any name for a type
            }
            // 读取一个 byte[] 并包装成BB 对象
            source = in.readBytesReference();
            routing = in.readOptionalString();
            if (format < FORMAT_NO_PARENT) {
                // parent信息
                in.readOptionalString(); // _parent
            }
            this.version = in.readLong();
            if (format < FORMAT_NO_VERSION_TYPE) {
                in.readByte(); // _version_type
            }
            this.autoGeneratedIdTimestamp = in.readLong();
            seqNo = in.readLong();
            primaryTerm = in.readLong();
        }

        /**
         * 根据某次index 成功的结果来初始化 Translog.Index 对象 代表要将数据存储到事务日志中
         * @param index
         * @param indexResult
         */
        public Index(Engine.Index index, Engine.IndexResult indexResult) {
            this.id = index.id();
            this.source = index.source();
            this.routing = index.routing();
            this.seqNo = indexResult.getSeqNo();
            this.primaryTerm = index.primaryTerm();
            this.version = indexResult.getVersion();
            this.autoGeneratedIdTimestamp = index.getAutoGeneratedIdTimestamp();
        }

        public Index(String id, long seqNo, long primaryTerm, byte[] source) {
            this(id, seqNo, primaryTerm, Versions.MATCH_ANY, source, null, -1);
        }

        public Index(String id, long seqNo, long primaryTerm, long version,
                     byte[] source, String routing, long autoGeneratedIdTimestamp) {
            this.id = id;
            this.source = new BytesArray(source);
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
            this.routing = routing;
            this.autoGeneratedIdTimestamp = autoGeneratedIdTimestamp;
        }

        @Override
        public Type opType() {
            return Type.INDEX;
        }

        @Override
        public long estimateSize() {
            return (id.length() * 2) + source.length() + 12;
        }

        public String id() {
            return this.id;
        }

        public String routing() {
            return this.routing;
        }

        public BytesReference source() {
            return this.source;
        }

        @Override
        public long seqNo() {
            return seqNo;
        }

        @Override
        public long primaryTerm() {
            return primaryTerm;
        }

        public long version() {
            return this.version;
        }

        @Override
        public Source getSource() {
            return new Source(source, routing);
        }

        private void write(final StreamOutput out) throws IOException {
            final int format = out.getVersion().onOrAfter(Version.V_8_0_0) ? SERIALIZATION_FORMAT : FORMAT_NO_VERSION_TYPE;
            out.writeVInt(format);
            out.writeString(id);
            if (format < FORMAT_NO_DOC_TYPE) {
                out.writeString(MapperService.SINGLE_MAPPING_NAME);
            }
            out.writeBytesReference(source);
            out.writeOptionalString(routing);
            if (format < FORMAT_NO_PARENT) {
                 out.writeOptionalString(null); // _parent
            }
            out.writeLong(version);
            if (format < FORMAT_NO_VERSION_TYPE) {
                out.writeByte(VersionType.EXTERNAL.getValue());
            }
            out.writeLong(autoGeneratedIdTimestamp);
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Index index = (Index) o;

            if (version != index.version ||
                seqNo != index.seqNo ||
                primaryTerm != index.primaryTerm ||
                id.equals(index.id) == false ||
                autoGeneratedIdTimestamp != index.autoGeneratedIdTimestamp ||
                source.equals(index.source) == false) {
                return false;
            }
            if (routing != null ? !routing.equals(index.routing) : index.routing != null) {
                return false;
            }
            return true;

        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + Long.hashCode(seqNo);
            result = 31 * result + Long.hashCode(primaryTerm);
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + source.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + Long.hashCode(autoGeneratedIdTimestamp);
            return result;
        }

        @Override
        public String toString() {
            return "Index{" +
                "id='" + id + '\'' +
                ", seqNo=" + seqNo +
                ", primaryTerm=" + primaryTerm +
                ", version=" + version +
                ", autoGeneratedIdTimestamp=" + autoGeneratedIdTimestamp +
                '}';
        }

        public long getAutoGeneratedIdTimestamp() {
            return autoGeneratedIdTimestamp;
        }

    }

    /**
     * 对应一个删除操作
     */
    public static class Delete implements Operation {

        private static final int FORMAT_6_0 = 4; // 6.0 - *
        public static final int FORMAT_NO_PARENT = FORMAT_6_0 + 1; // since 7.0
        public static final int FORMAT_NO_VERSION_TYPE = FORMAT_NO_PARENT + 1;
        public static final int FORMAT_NO_DOC_TYPE = FORMAT_NO_VERSION_TYPE + 1;    // since 8.0
        public static final int SERIALIZATION_FORMAT = FORMAT_NO_DOC_TYPE;

        private final String id;
        private final Term uid;
        private final long seqNo;
        private final long primaryTerm;
        private final long version;

        private Delete(final StreamInput in) throws IOException {
            final int format = in.readVInt();// SERIALIZATION_FORMAT
            assert format >= FORMAT_6_0 : "format was: " + format;
            if (format < FORMAT_NO_DOC_TYPE) {
                in.readString();
                // Can't assert that this is _doc because pre-8.0 indexes can have any name for a type
            }
            id = in.readString();
            uid = new Term(in.readString(), in.readBytesRef());
            this.version = in.readLong();
            if (format < FORMAT_NO_VERSION_TYPE) {
                in.readByte(); // versionType
            }
            seqNo = in.readLong();
            primaryTerm = in.readLong();
        }

        public Delete(Engine.Delete delete, Engine.DeleteResult deleteResult) {
            this(delete.id(), delete.uid(), deleteResult.getSeqNo(), delete.primaryTerm(), deleteResult.getVersion());
        }

        /** utility for testing */
        public Delete(String id, long seqNo, long primaryTerm, Term uid) {
            this(id, uid, seqNo, primaryTerm, Versions.MATCH_ANY);
        }

        public Delete(String id, Term uid, long seqNo, long primaryTerm, long version) {
            this.id = Objects.requireNonNull(id);
            this.uid = uid;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
        }

        @Override
        public Type opType() {
            return Type.DELETE;
        }

        @Override
        public long estimateSize() {
            return ((uid.field().length() + uid.text().length()) * 2) + 20;
        }

        public String id() {
            return id;
        }

        public Term uid() {
            return this.uid;
        }

        @Override
        public long seqNo() {
            return seqNo;
        }

        @Override
        public long primaryTerm() {
            return primaryTerm;
        }

        public long version() {
            return this.version;
        }

        @Override
        public Source getSource() {
            throw new IllegalStateException("trying to read doc source from delete operation");
        }

        private void write(final StreamOutput out) throws IOException {
            final int format = out.getVersion().onOrAfter(Version.V_8_0_0) ? SERIALIZATION_FORMAT : FORMAT_NO_VERSION_TYPE;
            out.writeVInt(format);
            if (format < FORMAT_NO_DOC_TYPE) {
                out.writeString(MapperService.SINGLE_MAPPING_NAME);
            }
            out.writeString(id);
            out.writeString(uid.field());
            out.writeBytesRef(uid.bytes());
            out.writeLong(version);
            if (format < FORMAT_NO_VERSION_TYPE) {
                out.writeByte(VersionType.EXTERNAL.getValue());
            }
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Delete delete = (Delete) o;

            return version == delete.version &&
                seqNo == delete.seqNo &&
                primaryTerm == delete.primaryTerm &&
                uid.equals(delete.uid);
        }

        @Override
        public int hashCode() {
            int result = uid.hashCode();
            result = 31 * result + Long.hashCode(seqNo);
            result = 31 * result + Long.hashCode(primaryTerm);
            result = 31 * result + Long.hashCode(version);
            return result;
        }

        @Override
        public String toString() {
            return "Delete{" +
                "uid=" + uid +
                ", seqNo=" + seqNo +
                ", primaryTerm=" + primaryTerm +
                ", version=" + version +
                '}';
        }
    }


    /**
     * 在填充 checkpoint 与 seq 之间的间隙时 会插入NOOP   这个数据也需要被记录到translog中
     */
    public static class NoOp implements Operation {

        private final long seqNo;
        private final long primaryTerm;
        private final String reason;

        @Override
        public long seqNo() {
            return seqNo;
        }

        @Override
        public long primaryTerm() {
            return primaryTerm;
        }

        public String reason() {
            return reason;
        }

        private NoOp(final StreamInput in) throws IOException {
            seqNo = in.readLong();
            primaryTerm = in.readLong();
            reason = in.readString();
        }

        public NoOp(final long seqNo, final long primaryTerm, final String reason) {
            assert seqNo > SequenceNumbers.NO_OPS_PERFORMED;
            assert primaryTerm >= 0;
            assert reason != null;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.reason = reason;
        }

        private void write(final StreamOutput out) throws IOException {
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
            out.writeString(reason);
        }

        @Override
        public Type opType() {
            return Type.NO_OP;
        }

        @Override
        public long estimateSize() {
            return 2 * reason.length() + 2 * Long.BYTES;
        }

        @Override
        public Source getSource() {
            throw new UnsupportedOperationException("source does not exist for a no-op");
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final NoOp that = (NoOp) obj;
            return seqNo == that.seqNo && primaryTerm == that.primaryTerm && reason.equals(that.reason);
        }

        @Override
        public int hashCode() {
            return 31 * 31 * 31 + 31 * 31 * Long.hashCode(seqNo) + 31 * Long.hashCode(primaryTerm) + reason().hashCode();
        }

        @Override
        public String toString() {
            return "NoOp{" +
                "seqNo=" + seqNo +
                ", primaryTerm=" + primaryTerm +
                ", reason='" + reason + '\'' +
                '}';
        }
    }

    /**
     * 持久化策略
     */
    public enum Durability {

        /**
         * Async durability - translogs are synced based on a time interval.
         * 通过定时任务定期触发刷盘
         */
        ASYNC,
        /**
         * Request durability - translogs are synced for each high level request (bulk, index, delete)
         * 基于请求来触发刷盘
         */
        REQUEST

    }

    static void verifyChecksum(BufferedChecksumStreamInput in) throws IOException {
        // This absolutely must come first, or else reading the checksum becomes part of the checksum
        long expectedChecksum = in.getChecksum();
        long readChecksum = Integer.toUnsignedLong(in.readInt());
        if (readChecksum != expectedChecksum) {
            throw new TranslogCorruptedException(in.getSource(), "checksum verification failed - expected: 0x" +
                Long.toHexString(expectedChecksum) + ", got: 0x" + Long.toHexString(readChecksum));
        }
    }

    /**
     * Reads a list of operations written with {@link #writeOperations(StreamOutput, List)}
     */
    public static List<Operation> readOperations(StreamInput input, String source) throws IOException {
        ArrayList<Operation> operations = new ArrayList<>();
        int numOps = input.readInt();
        final BufferedChecksumStreamInput checksumStreamInput = new BufferedChecksumStreamInput(input, source);
        for (int i = 0; i < numOps; i++) {
            operations.add(readOperation(checksumStreamInput));
        }
        return operations;
    }

    /**
     * 从数据流中还原 operation对象
     * @param in
     * @return
     * @throws IOException
     */
    static Translog.Operation readOperation(BufferedChecksumStreamInput in) throws IOException {
        final Translog.Operation operation;
        try {
            // 获取数据大小
            final int opSize = in.readInt();
            if (opSize < 4) { // 4byte for the checksum
                throw new TranslogCorruptedException(in.getSource(), "operation size must be at least 4 but was: " + opSize);
            }
            // 重置摘要  校验和相关的 先忽略
            in.resetDigest(); // size is not part of the checksum!
            if (in.markSupported()) { // if we can we validate the checksum first
                // we are sometimes called when mark is not supported this is the case when
                // we are sending translogs across the network with LZ4 compression enabled - currently there is no way s
                // to prevent this unfortunately.
                in.mark(opSize);

                in.skip(opSize - 4);
                verifyChecksum(in);
                in.reset();
            }
            // 从剩余部分中反序列化operation
            operation = Translog.Operation.readOperation(in);
            verifyChecksum(in);
        } catch (EOFException e) {
            throw new TruncatedTranslogException(in.getSource(), "reached premature end of file, translog is truncated", e);
        }
        return operation;
    }

    /**
     * Writes all operations in the given iterable to the given output stream including the size of the array
     * use {@link #readOperations(StreamInput, String)} to read it back.
     */
    public static void writeOperations(StreamOutput outStream, List<Operation> toWrite) throws IOException {
        final ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(BigArrays.NON_RECYCLING_INSTANCE);
        try {
            outStream.writeInt(toWrite.size());
            final BufferedChecksumStreamOutput checksumStreamOutput = new BufferedChecksumStreamOutput(out);
            for (Operation op : toWrite) {
                out.reset();
                final long start = out.position();
                out.skip(Integer.BYTES);
                writeOperationNoSize(checksumStreamOutput, op);
                long end = out.position();
                int operationSize = (int) (out.position() - Integer.BYTES - start);
                out.seek(start);
                out.writeInt(operationSize);
                out.seek(end);
                ReleasableBytesReference bytes = out.bytes();
                bytes.writeTo(outStream);
            }
        } finally {
            Releasables.close(out);
        }

    }

    /**
     * 将operation写入到output中
     * @param out
     * @param op
     * @throws IOException
     */
    public static void writeOperationNoSize(BufferedChecksumStreamOutput out, Translog.Operation op) throws IOException {
        // This BufferedChecksumStreamOutput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        out.resetDigest();
        Translog.Operation.writeOperation(out, op);
        long checksum = out.getChecksum();
        out.writeInt((int) checksum);
    }

    /**
     * Gets the minimum generation that could contain any sequence number after the specified sequence number, or the current generation if
     * there is no generation that could any such sequence number.
     *
     * @param seqNo the sequence number
     * @return the minimum generation for the sequence number
     */
    public TranslogGeneration getMinGenerationForSeqNo(final long seqNo) {
        try (ReleasableLock ignored = readLock.acquire()) {
            return new TranslogGeneration(translogUUID, minGenerationForSeqNo(seqNo, current, readers));
        }
    }

    /**
     * 找到包含这个seqNo的最大的gen  代表一定要保留
     * @param seqNo
     * @param writer
     * @param readers
     * @return
     */
    private static long minGenerationForSeqNo(long seqNo, TranslogWriter writer, List<TranslogReader> readers) {
        long minGen = writer.generation;
        for (final TranslogReader reader : readers) {
            if (seqNo <= reader.getCheckpoint().maxEffectiveSeqNo()) {
                minGen = Math.min(minGen, reader.getGeneration());
            }
        }
        return minGen;
    }

    /**
     * Roll the current translog generation into a new generation. This does not commit the
     * translog.
     *
     * @throws IOException if an I/O exception occurred during any file operations
     * 主动滚动到下一个事务文件 避免某个文件过大  kafka也有类似的逻辑
     */
    public void rollGeneration() throws IOException {
        // 在滚动前先将所有数据刷盘
        syncBeforeRollGeneration();
        try (Releasable ignored = writeLock.acquire()) {
            ensureOpen();
            try {
                final TranslogReader reader = current.closeIntoReader();
                readers.add(reader);
                assert Checkpoint.read(location.resolve(CHECKPOINT_FILE_NAME)).generation == current.getGeneration();
                // 将检查点文件 拷贝到 translog-gen.ckp 中
                copyCheckpointTo(location.resolve(getCommitCheckpointFileName(current.getGeneration())));
                // create a new translog file; this will sync it and update the checkpoint data;
                current = createWriter(current.getGeneration() + 1);
                logger.trace("current translog set to [{}]", current.getGeneration());
            } catch (final Exception e) {
                tragedy.setTragicException(e);
                closeOnTragicEvent(e);
                throw e;
            }
        }
    }

    void syncBeforeRollGeneration() throws IOException {
        // make sure we move most of the data to disk outside of the writeLock
        // in order to reduce the time the lock is held since it's blocking all threads
        sync();
    }

    /**
     * Trims unreferenced translog generations by asking {@link TranslogDeletionPolicy} for the minimum
     * required generation
     * 当某些reader不被使用时 可以考虑将他们删除
     */
    public void trimUnreferencedReaders() throws IOException {
        // first check under read lock if any readers can be trimmed
        try (ReleasableLock ignored = readLock.acquire()) {
            if (closed.get()) {
                // we're shutdown potentially on some tragic event, don't delete anything
                return;
            }
            // getMinReferencedGen()获取当前系统中允许保留的最小gen
            // getMinFileGeneration() 此时存在的最小的事务文件
            if (getMinReferencedGen() == getMinFileGeneration()) {
                return;
            }
        }

        // move most of the data to disk to reduce the time the write lock is held
        // 将尽可能多的数据先写入磁盘中
        sync();
        try (ReleasableLock ignored = writeLock.acquire()) {
            if (closed.get()) {
                // we're shutdown potentially on some tragic event, don't delete anything
                return;
            }
            // 最小的需要保留的gen  之前的reader 就可以移除
            final long minReferencedGen = getMinReferencedGen();
            for (Iterator<TranslogReader> iterator = readers.iterator(); iterator.hasNext(); ) {
                TranslogReader reader = iterator.next();
                if (reader.getGeneration() >= minReferencedGen) {
                    break;
                }
                // 将小于 gen的所有文件都删除 （分别包含事务文件和检查点文件）
                iterator.remove();
                IOUtils.closeWhileHandlingException(reader);
                final Path translogPath = reader.path();
                logger.trace("delete translog file [{}], not referenced and not current anymore", translogPath);
                // The checkpoint is used when opening the translog to know which files should be recovered from.
                // We now update the checkpoint to ignore the file we are going to remove.
                // Note that there is a provision in recoverFromFiles to allow for the case where we synced the checkpoint
                // but crashed before we could delete the file.
                // sync at once to make sure that there's at most one unreferenced generation.
                current.sync();
                deleteReaderFiles(reader);
            }
            assert readers.isEmpty() == false || current.generation == minReferencedGen :
                "all readers were cleaned but the minReferenceGen [" + minReferencedGen + "] is not the current writer's gen [" +
                    current.generation + "]";
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
    }

    /**
     * 获取此时还在使用中的最小gen
     * @return
     */
    private long getMinReferencedGen() {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread();
        // 通过下面2个参数计算后得到的就是此时要保留的最小gen
        long minReferencedGen = Math.min(
            // 获取最小的gen对应的引用计数  (虽然数据已经提交过了 也就是旧数据可以丢弃了 但是旧的数据仍然在使用中 那么就不能删除)
            deletionPolicy.getMinTranslogGenRequiredByLocks(),
            // 此时需要保留的最小的gen   在lucene配合DeletionPolicy的处理流程中 会更新deletionPolicy.getLocalCheckpointOfSafeCommit()
            // 在 safeCommit之前的数据都可以被删除
            minGenerationForSeqNo(deletionPolicy.getLocalCheckpointOfSafeCommit() + 1, current, readers));
        assert minReferencedGen >= getMinFileGeneration() :
            "deletion policy requires a minReferenceGen of [" + minReferencedGen + "] but the lowest gen available is ["
                + getMinFileGeneration() + "]";
        assert minReferencedGen <= currentFileGeneration() :
            "deletion policy requires a minReferenceGen of [" + minReferencedGen + "] which is higher than the current generation ["
                + currentFileGeneration() + "]";
        return minReferencedGen;
    }

    /**
     * deletes all files associated with a reader. package-private to be able to simulate node failures at this point
     */
    void deleteReaderFiles(TranslogReader reader) {
        IOUtils.deleteFilesIgnoringExceptions(reader.path(),
            reader.path().resolveSibling(getCommitCheckpointFileName(reader.getGeneration())));
    }

    void closeFilesIfNoPendingRetentionLocks() throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            if (closed.get() && deletionPolicy.pendingTranslogRefCount() == 0) {
                logger.trace("closing files. translog is closed and there are no pending retention locks");
                ArrayList<Closeable> toClose = new ArrayList<>(readers);
                toClose.add(current);
                IOUtils.close(toClose);
            }
        }
    }

    /**
     * References a transaction log generation
     */
    public static final class TranslogGeneration {
        public final String translogUUID;
        public final long translogFileGeneration;

        public TranslogGeneration(String translogUUID, long translogFileGeneration) {
            this.translogUUID = translogUUID;
            this.translogFileGeneration = translogFileGeneration;
        }

    }

    /**
     * Returns the current generation of this translog. This corresponds to the latest uncommitted translog generation
     */
    public TranslogGeneration getGeneration() {
        return new TranslogGeneration(translogUUID, currentFileGeneration());
    }

    long getFirstOperationPosition() { // for testing
        return current.getFirstOperationOffset();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new AlreadyClosedException("translog is already closed", tragedy.get());
        }
    }

    ChannelFactory getChannelFactory() {
        return FileChannel::open;
    }

    /**
     * If this {@code Translog} was closed as a side-effect of a tragic exception,
     * e.g. disk full while flushing a new segment, this returns the root cause exception.
     * Otherwise (no tragic exception has occurred) it returns null.
     */
    public Exception getTragicException() {
        return tragedy.get();
    }

    /**
     * Reads and returns the current checkpoint
     * 从 checkPoints 文件中解析 checkPoint对象
     */
    static Checkpoint readCheckpoint(final Path location) throws IOException {
        return Checkpoint.read(location.resolve(CHECKPOINT_FILE_NAME));
    }

    /**
     * Reads the sequence numbers global checkpoint from the translog checkpoint.
     * This ensures that the translogUUID from this translog matches with the provided translogUUID.
     *
     * @param location the location of the translog
     * @param expectedTranslogUUID  主要是用于检测 确保事务日志匹配
     * @return the global checkpoint
     * @throws IOException                if an I/O exception occurred reading the checkpoint
     * @throws TranslogCorruptedException if the translog is corrupted or mismatched with the given uuid
     * 获取全局检查点信息
     */
    public static long readGlobalCheckpoint(final Path location, final String expectedTranslogUUID) throws IOException {
        final Checkpoint checkpoint = readCheckpoint(location, expectedTranslogUUID);
        return checkpoint.globalCheckpoint;
    }

    /**
     *
     * @param location
     * @param expectedTranslogUUID  本次预计解析出来的事务文件对应的uuid
     * @return
     * @throws IOException
     */
    private static Checkpoint readCheckpoint(Path location, String expectedTranslogUUID) throws IOException {
        // 获取  translog.ckp 信息
        final Checkpoint checkpoint = readCheckpoint(location);
        // We need to open at least one translog header to validate the translogUUID.
        // 定位到最新的 translog-gen.tlog 也就是事务文件
        final Path translogFile = location.resolve(getFilename(checkpoint.generation));
        // 这里是在检测 uuid是否匹配
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
            TranslogHeader.read(expectedTranslogUUID, translogFile, channel);
        } catch (TranslogCorruptedException ex) {
            throw ex; // just bubble up.
        } catch (Exception ex) {
            throw new TranslogCorruptedException(location.toString(), ex);
        }
        return checkpoint;
    }

    /**
     * Returns the translog uuid used to associate a lucene index with a translog.
     */
    public String getTranslogUUID() {
        return translogUUID;
    }

    /**
     * Returns the max seq_no of translog operations found in this translog. Since this value is calculated based on the current
     * existing readers, this value is not necessary to be the max seq_no of all operations have been stored in this translog.
     */
    public long getMaxSeqNo() {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            final OptionalLong maxSeqNo = Stream.concat(readers.stream(), Stream.of(current))
                .mapToLong(reader -> reader.getCheckpoint().maxSeqNo).max();
            assert maxSeqNo.isPresent() : "must have at least one translog generation";
            return maxSeqNo.getAsLong();
        }
    }

    TranslogWriter getCurrent() {
        return current;
    }

    List<TranslogReader> getReaders() {
        return readers;
    }

    /**
     * 清空之前所有的事务日志信息
     * @param location  该目录下存储事务文件的路径
     * @param initialGlobalCheckpoint  本次检查点
     * @param shardId  本次使用的分片id
     * @param primaryTerm  每个IndexShard在生成时 有自己的term
     * @return
     * @throws IOException
     */
    public static String createEmptyTranslog(final Path location, final long initialGlobalCheckpoint,
                                             final ShardId shardId, final long primaryTerm) throws IOException {
        final ChannelFactory channelFactory = FileChannel::open;
        return createEmptyTranslog(location, initialGlobalCheckpoint, shardId, channelFactory, primaryTerm);
    }

    static String createEmptyTranslog(Path location, long initialGlobalCheckpoint, ShardId shardId,
                                      ChannelFactory channelFactory, long primaryTerm) throws IOException {
        return createEmptyTranslog(location, shardId, initialGlobalCheckpoint, primaryTerm, null, channelFactory);
    }

    /**
     * Creates a new empty translog within the specified {@code location} that contains the given {@code initialGlobalCheckpoint},
     * {@code primaryTerm} and {@code translogUUID}.
     *
     * This method should be used directly under specific circumstances like for shards that will see no indexing. Specifying a non-unique
     * translog UUID could cause a lot of issues and that's why in all (but one) cases the method
     * {@link #createEmptyTranslog(Path, long, ShardId, long)} should be used instead.
     *
     * @param location                a {@link Path} to the directory that will contains the translog files (translog + translog checkpoint)  存储事务文件信息的目录
     * @param shardId                 the {@link ShardId}
     * @param initialGlobalCheckpoint the global checkpoint to initialize the translog with    本地检查点 是从segment_N文件中获取的
     * @param primaryTerm             the shard's primary term to initialize the translog with   创建该分片信息对应的term
     * @param translogUUID            the unique identifier to initialize the translog with     本次事务日志的id 可以为null
     * @param factory                 a {@link ChannelFactory} used to open translog files    通道工厂
     * @return the translog's unique identifier
     * @throws IOException if something went wrong during translog creation
     * 创建一个空的事务文件
     */
    public static String createEmptyTranslog(final Path location,
                                             final ShardId shardId,
                                             final long initialGlobalCheckpoint,
                                             final long primaryTerm,
                                             @Nullable final String translogUUID,
                                             @Nullable final ChannelFactory factory) throws IOException {
        // 先清空目录下所有文件
        IOUtils.rm(location);
        // 重新创建分片下的translog目录
        Files.createDirectories(location);

        final long generation = 1L;
        final long minTranslogGeneration = 1L;
        // 生成文件通道工厂
        final ChannelFactory channelFactory = factory != null ? factory : FileChannel::open;
        // 当事务id没有手动设置时 生成一个随机数
        final String uuid = Strings.hasLength(translogUUID) ? translogUUID : UUIDs.randomBase64UUID();
        // 生成检查点文件路径
        final Path checkpointFile = location.resolve(CHECKPOINT_FILE_NAME);
        // 这里初始化了一个事务日志文件的路径 gen为1
        final Path translogFile = location.resolve(getFilename(generation));
        // 使用空数据生成检查点对象  每个检查点应该就是存储 此时store已经写到的seq 以及checkpoint信息吧
        final Checkpoint checkpoint = Checkpoint.emptyTranslogCheckpoint(0, generation, initialGlobalCheckpoint, minTranslogGeneration);

        // 通过checkpoint信息生成文件
        Checkpoint.write(channelFactory, checkpointFile, checkpoint, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        // 将文件刷盘
        IOUtils.fsync(checkpointFile, false);
        // 生成一个 事务日志写入对象  因为之前只是写入了检查点信息
        final TranslogWriter writer = TranslogWriter.create(shardId, uuid, generation, translogFile, channelFactory,
            new ByteSizeValue(10), minTranslogGeneration, initialGlobalCheckpoint,
            () -> {
                throw new UnsupportedOperationException();
            }, () -> {
                throw new UnsupportedOperationException();
            },
            primaryTerm,
            new TragicExceptionHolder(),
            seqNo -> {
                throw new UnsupportedOperationException();
            });
        // 这里关闭了文件通道 但是文件已经生成了 是一个 translog-1.tlog  并且写入了一个事务头
        writer.close();
        return uuid;
    }
}
