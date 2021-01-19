/*
 * Licensed to Elasticsearch under one or more contriutor
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

package org.elasticsearch.index.store;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Version;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.RefCounted;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.CombinedDeletionPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * A Store provides plain access to files written by an elasticsearch index shard. Each shard
 * has a dedicated store that is uses to access Lucene's Directory which represents the lowest level
 * of file abstraction in Lucene used to read and write Lucene indices.
 * This class also provides access to metadata information like checksums for committed files. A committed
 * file is a file that belongs to a segment written by a Lucene commit. Files that have not been committed
 * ie. created during a merge or a shard refresh / NRT reopen are not considered in the MetadataSnapshot.
 * <p>
 * Note: If you use a store it's reference count should be increased before using it by calling #incRef and a
 * corresponding #decRef must be called in a try/finally block to release the store again ie.:
 * <pre>
 *      store.incRef();
 *      try {
 *        // use the store...
 *
 *      } finally {
 *          store.decRef();
 *      }
 * </pre>
 * 每个store 对应一个 shard
 */
public class Store extends AbstractIndexShardComponent implements Closeable, RefCounted {
    static final String CODEC = "store";
    static final int CORRUPTED_MARKER_CODEC_VERSION = 2;
    // public is for test purposes
    // 代表store已经被损坏
    public static final String CORRUPTED_MARKER_NAME_PREFIX = "corrupted_";

    /**
     * 统计数据的刷新时间间隔
     * 主要是查看目录内部的文件数据大小等
     */
    public static final Setting<TimeValue> INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING =
        Setting.timeSetting("index.store.stats_refresh_interval", TimeValue.timeValueSeconds(10), Property.IndexScope);

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final StoreDirectory directory;
    private final ReentrantReadWriteLock metadataLock = new ReentrantReadWriteLock();

    /**
     * 每当需要为某个shardId 创建 Shard对象时 需要先获取锁对象
     */
    private final ShardLock shardLock;
    private final OnClose onClose;

    /**
     * 当引用计数归0时 触发 closeInternal
     */
    private final AbstractRefCounted refCounter = new AbstractRefCounted("store") {
        @Override
        protected void closeInternal() {
            // close us once we are done
            Store.this.closeInternal();
        }
    };

    public Store(ShardId shardId, IndexSettings indexSettings, Directory directory, ShardLock shardLock) {
        this(shardId, indexSettings, directory, shardLock, OnClose.EMPTY);
    }

    /**
     * @param shardId       代表本次目录对应的分片id
     * @param indexSettings
     * @param directory     对应存储shard数据的目录
     * @param shardLock     shard锁对象
     * @param onClose       当本对象被关闭时触发的钩子
     *                      当某个index下的某个shard被创建时 就会创建一个store对象
     */
    public Store(ShardId shardId, IndexSettings indexSettings, Directory directory, ShardLock shardLock,
                 OnClose onClose) {
        super(shardId, indexSettings);
        final TimeValue refreshInterval = indexSettings.getValue(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING);
        logger.debug("store stats are refreshed with refresh_interval [{}]", refreshInterval);

        // 该目录会感知内部文件的变化
        ByteSizeCachingDirectory sizeCachingDir = new ByteSizeCachingDirectory(directory, refreshInterval);
        // 对外不允许调用close() 只能在内部调用
        this.directory = new StoreDirectory(sizeCachingDir, Loggers.getLogger("index.store.deletes", shardId));
        this.shardLock = shardLock;
        this.onClose = onClose;

        assert onClose != null;
        assert shardLock != null;
        assert shardLock.getShardId().equals(shardId);
    }

    public Directory directory() {
        ensureOpen();
        return directory;
    }

    /**
     * Returns the last committed segments info for this store
     *
     * @throws IOException if the index is corrupted or the segments file is not present
     *                     获取最后一次提交的segment 信息
     */
    public SegmentInfos readLastCommittedSegmentsInfo() throws IOException {
        // 确保文件没有损坏
        failIfCorrupted();
        try {
            return readSegmentsInfo(null, directory());
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            markStoreCorrupted(ex);
            throw ex;
        }
    }

    /**
     * Returns the segments info for the given commit or for the latest commit if the given commit is <code>null</code>
     *
     * @param commit    commit内部包含一组文件列表以及存储这些文件的目录  代表lucene中某次刷盘生成的文件
     * @param directory 内部存储了各种文件  内部会包含segment_N 文件 可以还原成 segmentInfos
     * @throws IOException if the index is corrupted or the segments file is not present
     */
    private static SegmentInfos readSegmentsInfo(IndexCommit commit, Directory directory) throws IOException {
        assert commit == null || commit.getDirectory() == directory;
        try {
            // 如果传入了 commit 就获取commit对应的 segmentInfos 文件
            return commit == null ? Lucene.readSegmentInfos(directory) : Lucene.readSegmentInfos(commit);
        } catch (EOFException eof) {
            // TODO this should be caught by lucene - EOF is almost certainly an index corruption
            throw new CorruptIndexException("Read past EOF while reading segment infos", "commit(" + commit + ")", eof);
        } catch (IOException exception) {
            throw exception; // IOExceptions like too many open files are not necessarily a corruption - just bubble it up
        } catch (Exception ex) {
            throw new CorruptIndexException("Hit unexpected exception while reading segment infos", "commit(" + commit + ")", ex);
        }

    }

    final void ensureOpen() {
        if (this.refCounter.refCount() <= 0) {
            throw new AlreadyClosedException("store is already closed");
        }
    }

    /**
     * Returns a new MetadataSnapshot for the given commit. If the given commit is <code>null</code>
     * the latest commit point is used.
     * <p>
     * Note that this method requires the caller verify it has the right to access the store and
     * no concurrent file changes are happening. If in doubt, you probably want to use one of the following:
     * <p>
     * {@link #readMetadataSnapshot(Path, ShardId, NodeEnvironment.ShardLocker, Logger)} to read a meta data while locking
     * {@link IndexShard#snapshotStoreMetadata()} to safely read from an existing shard
     * {@link IndexShard#acquireLastIndexCommit(boolean)} to get an {@link IndexCommit} which is safe to use but has to be freed
     *
     * @param commit the index commit to read the snapshot from or <code>null</code> if the latest snapshot should be read from the
     *               directory
     * @throws CorruptIndexException      if the lucene index is corrupted. This can be caused by a checksum mismatch or an
     *                                    unexpected exception when opening the index reading the segments file.
     * @throws IndexFormatTooOldException if the lucene index is too old to be opened.
     * @throws IndexFormatTooNewException if the lucene index is too new to be opened.
     * @throws FileNotFoundException      if one or more files referenced by a commit are not present.
     * @throws NoSuchFileException        if one or more files referenced by a commit are not present.
     * @throws IndexNotFoundException     if the commit point can't be found in this store
     *                                    获取 indexCommit 对应的segment_N 文件此时的信息 作为一个快照
     */
    public MetadataSnapshot getMetadata(IndexCommit commit) throws IOException {
        return getMetadata(commit, false);
    }

    /**
     * Returns a new MetadataSnapshot for the given commit. If the given commit is <code>null</code>
     * the latest commit point is used.
     * <p>
     * Note that this method requires the caller verify it has the right to access the store and
     * no concurrent file changes are happening. If in doubt, you probably want to use one of the following:
     * <p>
     * {@link #readMetadataSnapshot(Path, ShardId, NodeEnvironment.ShardLocker, Logger)} to read a meta data while locking
     * {@link IndexShard#snapshotStoreMetadata()} to safely read from an existing shard
     * {@link IndexShard#acquireLastIndexCommit(boolean)} to get an {@link IndexCommit} which is safe to use but has to be freed
     *
     * @param commit        the index commit to read the snapshot from or <code>null</code> if the latest snapshot should be read from the
     *                      directory      提交点中包含了本次提交的所有文件  文件所在的目录  以及本次gen
     * @param lockDirectory if <code>true</code> the index writer lock will be obtained before reading the snapshot. This should
     *                      only be used if there is no started shard using this store.
     *                      在访问时是否要持有写锁   至少要获取读锁避免数据不一致
     * @throws CorruptIndexException      if the lucene index is corrupted. This can be caused by a checksum mismatch or an
     *                                    unexpected exception when opening the index reading the segments file.
     * @throws IndexFormatTooOldException if the lucene index is too old to be opened.
     * @throws IndexFormatTooNewException if the lucene index is too new to be opened.
     * @throws FileNotFoundException      if one or more files referenced by a commit are not present.
     * @throws NoSuchFileException        if one or more files referenced by a commit are not present.
     * @throws IndexNotFoundException     if the commit point can't be found in this store
     *                                    <p>
     *                                    从commit中获取元数据信息
     */
    public MetadataSnapshot getMetadata(IndexCommit commit, boolean lockDirectory) throws IOException {
        ensureOpen();
        // 检测该目录下是否已经生成代表异常的文件 "corrupt_" 为前缀
        failIfCorrupted();
        assert lockDirectory ? commit == null : true : "IW lock should not be obtained if there is a commit point available";
        // if we lock the directory we also acquire the write lock since that makes sure that nobody else tries to lock the IW
        // on this store at the same time.
        java.util.concurrent.locks.Lock lock = lockDirectory ? metadataLock.writeLock() : metadataLock.readLock();
        lock.lock();
        // 如果需要获取目录锁 会调用lucene的锁方法 内部涉及到基于文件的进程锁
        try (Closeable ignored = lockDirectory ? directory.obtainLock(IndexWriter.WRITE_LOCK_NAME) : () -> {
        }) {
            return new MetadataSnapshot(commit, directory, logger);
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // 当产生异常时 将异常信息写入到一个文件中  这样其他线程一旦检测到异常文件 就不再继续读取了
            // 这也类似一种缓存 代表检测结果的缓存 不需要做同步机制 只要发现就代表校验失败 如果未发现就执行一次校验
            markStoreCorrupted(ex);
            throw ex;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Renames all the given files from the key of the map to the
     * value of the map. All successfully renamed files are removed from the map in-place.
     * 将 key 对应的文件 rename成value对应的名字
     */
    public void renameTempFilesSafe(Map<String, String> tempFileMap) throws IOException {
        // this works just like a lucene commit - we rename all temp files and once we successfully
        // renamed all the segments we rename the commit to ensure we don't leave half baked commits behind.
        final Map.Entry<String, String>[] entries = tempFileMap.entrySet().toArray(new Map.Entry[0]);
        ArrayUtil.timSort(entries, (o1, o2) -> {
            String left = o1.getValue();
            String right = o2.getValue();
            // 将要rename成segment_N 的文件排在后面
            if (left.startsWith(IndexFileNames.SEGMENTS) || right.startsWith(IndexFileNames.SEGMENTS)) {
                if (left.startsWith(IndexFileNames.SEGMENTS) == false) {
                    return -1;
                } else if (right.startsWith(IndexFileNames.SEGMENTS) == false) {
                    return 1;
                }
            }
            return left.compareTo(right);
        });

        // 因为要修改文件 所以要获取写锁
        metadataLock.writeLock().lock();
        // we make sure that nobody fetches the metadata while we do this rename operation here to ensure we don't
        // get exceptions if files are still open.
        try (Lock writeLock = directory().obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            for (Map.Entry<String, String> entry : entries) {
                String tempFile = entry.getKey();
                String origFile = entry.getValue();
                // first, go and delete the existing ones
                try {
                    // 可能已经存在同名文件  先删除
                    directory.deleteFile(origFile);
                } catch (FileNotFoundException | NoSuchFileException e) {
                } catch (Exception ex) {
                    logger.debug(() -> new ParameterizedMessage("failed to delete file [{}]", origFile), ex);
                }
                // now, rename the files... and fail it it won't work
                // 将旧文件rename
                directory.rename(tempFile, origFile);
                final String remove = tempFileMap.remove(tempFile);
                assert remove != null;
            }
            // 将页缓存的数据置换到磁盘中  文件的metadata就是指文件名,长度之类的数据
            directory.syncMetaData();
        } finally {
            metadataLock.writeLock().unlock();
        }

    }

    /**
     * Checks and returns the status of the existing index in this store.
     *
     * @param out where infoStream messages should go. See {@link CheckIndex#setInfoStream(PrintStream)}
     *            这个是lucene内置的索引文件检查类 通过指定索引文件 检测状态
     */
    public CheckIndex.Status checkIndex(PrintStream out) throws IOException {
        metadataLock.writeLock().lock();
        try (CheckIndex checkIndex = new CheckIndex(directory)) {
            checkIndex.setInfoStream(out);
            return checkIndex.checkIndex();
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * 该统计对象 记录了这个目录的大小
     *
     * @return
     * @throws IOException
     */
    public StoreStats stats() throws IOException {
        ensureOpen();
        return new StoreStats(directory.estimateSize());
    }

    /**
     * Increments the refCount of this Store instance.  RefCounts are used to determine when a
     * Store can be closed safely, i.e. as soon as there are no more references. Be sure to always call a
     * corresponding {@link #decRef}, in a finally clause; otherwise the store may never be closed.  Note that
     * {@link #close} simply calls decRef(), which means that the Store will not really be closed until {@link
     * #decRef} has been called for all outstanding references.
     * <p>
     * Note: Close can safely be called multiple times.
     *
     * @throws AlreadyClosedException iff the reference counter can not be incremented.
     * @see #decRef
     * @see #tryIncRef()
     * 在使用该对象前 应该先调用该方法
     */
    @Override
    public final void incRef() {
        refCounter.incRef();
    }

    /**
     * Tries to increment the refCount of this Store instance. This method will return {@code true} iff the refCount was
     * incremented successfully otherwise {@code false}. RefCounts are used to determine when a
     * Store can be closed safely, i.e. as soon as there are no more references. Be sure to always call a
     * corresponding {@link #decRef}, in a finally clause; otherwise the store may never be closed.  Note that
     * {@link #close} simply calls decRef(), which means that the Store will not really be closed until {@link
     * #decRef} has been called for all outstanding references.
     * <p>
     * Note: Close can safely be called multiple times.
     *
     * @see #decRef()
     * @see #incRef()
     * 返回false 代表该对象本身已经被关闭了
     */
    @Override
    public final boolean tryIncRef() {
        return refCounter.tryIncRef();
    }

    /**
     * Decreases the refCount of this Store instance. If the refCount drops to 0, then this
     * store is closed.
     *
     * @see #incRef
     */
    @Override
    public final void decRef() {
        refCounter.decRef();
    }

    @Override
    public void close() {

        if (isClosed.compareAndSet(false, true)) {
            // only do this once!
            decRef();
            logger.debug("store reference count on close: {}", refCounter.refCount());
        }
    }

    /**
     * 当引用计数清零时 会触发该方法
     */
    private void closeInternal() {
        // Leverage try-with-resources to close the shard lock for us
        try (Closeable c = shardLock) {
            try {
                // 关闭目录对象
                directory.innerClose(); // this closes the distributorDirectory as well
            } finally {
                // 使用该钩子去处理 分片锁对象
                onClose.accept(shardLock);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reads a MetadataSnapshot from the given index locations or returns an empty snapshot if it can't be read.
     *
     * @param shardId     每个shard 对应一个 store
     * @param shardLocker 代表一个可上锁对象 暴露一个lock的api 同时还可以指定锁定时间
     * @throws IOException if the index we try to read is corrupted
     *                     通过指定的路径找到元数据快照   MetadataSnapshot 记录的是某个时刻segment_N 内部的数据
     */
    public static MetadataSnapshot readMetadataSnapshot(Path indexLocation, ShardId shardId, NodeEnvironment.ShardLocker shardLocker,
                                                        Logger logger) throws IOException {
        // 代表将某个分片锁定5秒钟
        try (ShardLock lock = shardLocker.lock(shardId, "read metadata snapshot", TimeUnit.SECONDS.toMillis(5));
             // 创建一个使用 FileChannel的简单目录
             Directory dir = new SimpleFSDirectory(indexLocation)) {
            failIfCorrupted(dir);
            return new MetadataSnapshot(null, dir, logger);
        } catch (IndexNotFoundException ex) {
            // that's fine - happens all the time no need to log
        } catch (FileNotFoundException | NoSuchFileException ex) {
            logger.info("Failed to open / find files while reading metadata snapshot", ex);
        } catch (ShardLockObtainFailedException ex) {
            logger.info(() -> new ParameterizedMessage("{}: failed to obtain shard lock", shardId), ex);
        }
        return MetadataSnapshot.EMPTY;
    }

    /**
     * Tries to open an index for the given location. This includes reading the
     * segment infos and possible corruption markers. If the index can not
     * be opened, an exception is thrown
     * 主要是检测目标路径下是否出现了异常 能否正常的加载 segmentInfos信息
     */
    public static void tryOpenIndex(Path indexLocation, ShardId shardId, NodeEnvironment.ShardLocker shardLocker,
                                    Logger logger) throws IOException, ShardLockObtainFailedException {
        try (ShardLock lock = shardLocker.lock(shardId, "open index", TimeUnit.SECONDS.toMillis(5));
             Directory dir = new SimpleFSDirectory(indexLocation)) {
            // 先检测目录下数据是否不正常
            failIfCorrupted(dir);
            // 还原段信息
            SegmentInfos segInfo = Lucene.readSegmentInfos(dir);
            logger.trace("{} loaded segment info [{}]", shardId, segInfo);
        }
    }

    /**
     * The returned IndexOutput validates the files checksum.
     * <p>
     * Note: Checksums are calculated by default since version 4.8.0. This method only adds the
     * verification against the checksum in the given metadata and does not add any significant overhead.
     *
     * @param metadata 描述了目标文件的大小  hash  checksum 等信息
     */
    public IndexOutput createVerifyingOutput(String fileName, final StoreFileMetadata metadata,
                                             final IOContext context) throws IOException {
        IndexOutput output = directory().createOutput(fileName, context);
        boolean success = false;
        try {
            assert metadata.writtenBy() != null;
            // 包装一层校验器
            // TODO 暂时还不知道应用场景
            output = new LuceneVerifyingIndexOutput(metadata, output);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(output);
            }
        }
        return output;
    }

    public static void verify(IndexOutput output) throws IOException {
        if (output instanceof VerifyingIndexOutput) {
            ((VerifyingIndexOutput) output).verify();
        }
    }

    /**
     * 包装输入流 使得可以校验
     *
     * @param filename
     * @param context
     * @param metadata
     * @return
     * @throws IOException
     */
    public IndexInput openVerifyingInput(String filename, IOContext context, StoreFileMetadata metadata) throws IOException {
        assert metadata.writtenBy() != null;
        return new VerifyingIndexInput(directory().openInput(filename, context));
    }

    public static void verify(IndexInput input) throws IOException {
        if (input instanceof VerifyingIndexInput) {
            ((VerifyingIndexInput) input).verify();
        }
    }

    public boolean checkIntegrityNoException(StoreFileMetadata md) {
        return checkIntegrityNoException(md, directory());
    }

    public static boolean checkIntegrityNoException(StoreFileMetadata md, Directory directory) {
        try {
            checkIntegrity(md, directory);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 校验完整性
     * 就是对比校验和
     *
     * @param md
     * @param directory
     * @throws IOException
     */
    public static void checkIntegrity(final StoreFileMetadata md, final Directory directory) throws IOException {
        try (IndexInput input = directory.openInput(md.name(), IOContext.READONCE)) {
            if (input.length() != md.length()) { // first check the length no matter how old this file is
                throw new CorruptIndexException("expected length=" + md.length() + " != actual length: " + input.length() +
                    " : file truncated?", input);
            }
            // throw exception if the file is corrupt
            String checksum = Store.digestToString(CodecUtil.checksumEntireFile(input));
            // throw exception if metadata is inconsistent
            if (!checksum.equals(md.checksum())) {
                throw new CorruptIndexException("inconsistent metadata: lucene checksum=" + checksum +
                    ", metadata checksum=" + md.checksum(), input);
            }
        }
    }

    /**
     * 当前目录下是否有文件被标记成 corrupted
     *
     * @return
     * @throws IOException
     */
    public boolean isMarkedCorrupted() throws IOException {
        ensureOpen();
        /* marking a store as corrupted is basically adding a _corrupted to all
         * the files. This prevent
         */
        final String[] files = directory().listAll();
        for (String file : files) {
            if (file.startsWith(CORRUPTED_MARKER_NAME_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Deletes all corruption markers from this store.
     * 将用于标记目录下有异常文件的  marker文件删除
     */
    public void removeCorruptionMarker() throws IOException {
        ensureOpen();
        final Directory directory = directory();
        IOException firstException = null;
        final String[] files = directory.listAll();
        for (String file : files) {
            if (file.startsWith(CORRUPTED_MARKER_NAME_PREFIX)) {
                try {
                    directory.deleteFile(file);
                } catch (IOException ex) {
                    if (firstException == null) {
                        firstException = ex;
                    } else {
                        firstException.addSuppressed(ex);
                    }
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * 异常信息会被写入一个 corrupt_ 为前缀的文件中  这里就是在检测这样的文件
     *
     * @throws IOException
     */
    public void failIfCorrupted() throws IOException {
        // 如果当前引用计数为负数 代表已经被关闭了
        ensureOpen();
        failIfCorrupted(directory);
    }

    /**
     * 检测内部数据是否已经发生了异常    当发生异常时会在目录下生成一个 corrupt_为前缀的文件
     *
     * @param directory 在创建每个shard时 会专门为该分片维护一个dir
     * @throws IOException
     */
    private static void failIfCorrupted(Directory directory) throws IOException {
        final String[] files = directory.listAll();
        List<CorruptIndexException> ex = new ArrayList<>();
        for (String file : files) {
            if (file.startsWith(CORRUPTED_MARKER_NAME_PREFIX)) {
                try (ChecksumIndexInput input = directory.openChecksumInput(file, IOContext.READONCE)) {
                    CodecUtil.checkHeader(input, CODEC, CORRUPTED_MARKER_CODEC_VERSION, CORRUPTED_MARKER_CODEC_VERSION);
                    final int size = input.readVInt();
                    final byte[] buffer = new byte[size];
                    input.readBytes(buffer, 0, buffer.length);
                    StreamInput in = StreamInput.wrap(buffer);
                    Exception t = in.readException();
                    if (t instanceof CorruptIndexException) {
                        ex.add((CorruptIndexException) t);
                    } else {
                        ex.add(new CorruptIndexException(t.getMessage(), "preexisting_corruption", t));
                    }
                    CodecUtil.checkFooter(input);
                }
            }
        }
        if (ex.isEmpty() == false) {
            ExceptionsHelper.rethrowAndSuppress(ex);
        }
    }

    /**
     * This method deletes every file in this store that is not contained in the given source meta data or is a
     * legacy checksum file. After the delete it pulls the latest metadata snapshot from the store and compares it
     * to the given snapshot. If the snapshots are inconsistent an illegal state exception is thrown.
     *
     * @param reason         the reason for this cleanup operation logged for each deleted file
     * @param sourceMetadata the metadata used for cleanup. all files in this metadata should be kept around.
     *                          这些文件应当被保留
     * @throws IOException           if an IOException occurs
     * @throws IllegalStateException if the latest snapshot in this store differs from the given one after the cleanup.
     *
     * 删除一些无效的文件
     */
    public void cleanupAndVerify(String reason, MetadataSnapshot sourceMetadata) throws IOException {
        metadataLock.writeLock().lock();
        try (Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            for (String existingFile : directory.listAll()) {
                // 锁文件 和 MetadataSnapshot对应的segment_N 下的文件不用删除
                if (Store.isAutogenerated(existingFile) || sourceMetadata.contains(existingFile)) {
                    // don't delete snapshot file, or the checksums file (note, this is extra protection since the Store won't delete
                    // checksum)
                    continue;
                }
                try {
                    // 其余文件会被删除    也就是在恢复过程中 只有本次恢复相关的索引文件需要保留 其余文件 包含事务文件都需要删除
                    directory.deleteFile(reason, existingFile);
                    // FNF should not happen since we hold a write lock?
                } catch (IOException ex) {
                    if (existingFile.startsWith(IndexFileNames.SEGMENTS)
                        || existingFile.equals(IndexFileNames.OLD_SEGMENTS_GEN)
                        || existingFile.startsWith(CORRUPTED_MARKER_NAME_PREFIX)) {
                        // TODO do we need to also fail this if we can't delete the pending commit file?
                        // if one of those files can't be deleted we better fail the cleanup otherwise we might leave an old commit
                        // point around?
                        throw new IllegalStateException("Can't delete " + existingFile + " - cleanup failed", ex);
                    }
                    logger.debug(() -> new ParameterizedMessage("failed to delete file [{}]", existingFile), ex);
                    // ignore, we don't really care, will get deleted later on
                }
            }

            // 删除文件导致目录的元数据发生变化 这里要更新
            directory.syncMetaData();
            // 更新此时的 metadata 信息   正常的情况下 元数据应该没有变化 因为清除的不是segment_N下面的文件
            final Store.MetadataSnapshot metadataOrEmpty = getMetadata(null);
            verifyAfterCleanup(sourceMetadata, metadataOrEmpty);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * 检验清理前后元数据是否发生了变化
     *
     * @param sourceMetadata
     * @param targetMetadata
     */
    final void verifyAfterCleanup(MetadataSnapshot sourceMetadata, MetadataSnapshot targetMetadata) {
        final RecoveryDiff recoveryDiff = targetMetadata.recoveryDiff(sourceMetadata);
        // 2者并非完全一致  必然抛出异常
        if (recoveryDiff.identical.size() != recoveryDiff.size()) {
            // 代表某些文件前后发生了变化
            if (recoveryDiff.missing.isEmpty()) {
                for (StoreFileMetadata meta : recoveryDiff.different) {
                    StoreFileMetadata local = targetMetadata.get(meta.name());
                    StoreFileMetadata remote = sourceMetadata.get(meta.name());
                    // if we have different files then they must have no checksums; otherwise something went wrong during recovery.
                    // we have that problem when we have an empty index is only a segments_1 file so we can't tell if it's a Lucene 4.8 file
                    // and therefore no checksum is included. That isn't a problem since we simply copy it over anyway but those files
                    // come out as different in the diff. That's why we have to double check here again if the rest of it matches.

                    // all is fine this file is just part of a commit or a segment that is different
                    // 代表文件发生了变化
                    if (local.isSame(remote) == false) {
                        logger.debug("Files are different on the recovery target: {} ", recoveryDiff);
                        throw new IllegalStateException("local version: " + local + " is different from remote version after recovery: " +
                            remote, null);
                    }
                }
            } else {
                // 代表此时target出现了之前没有的文件
                logger.debug("Files are missing on the recovery target: {} ", recoveryDiff);
                throw new IllegalStateException("Files are missing on the recovery target: [different="
                    + recoveryDiff.different + ", missing=" + recoveryDiff.missing + ']', null);
            }
        }
    }

    /**
     * Returns the current reference count.
     */
    public int refCount() {
        return refCounter.refCount();
    }

    /**
     * 基于lucene的目录包装类 进行增强
     */
    static final class StoreDirectory extends FilterDirectory {

        private final Logger deletesLogger;

        /**
         * @param delegateDirectory 实际上包装的是 ByteSizeCachingDirectory
         * @param deletesLogger     日志对象 先忽略
         */
        StoreDirectory(ByteSizeCachingDirectory delegateDirectory, Logger deletesLogger) {
            super(delegateDirectory);
            this.deletesLogger = deletesLogger;
        }

        /**
         * Estimate the cumulative size of all files in this directory in bytes.
         */
        long estimateSize() throws IOException {
            return ((ByteSizeCachingDirectory) getDelegate()).estimateSizeInBytes();
        }

        /**
         * 不允许外部调用close
         */
        @Override
        public void close() {
            assert false : "Nobody should close this directory except of the Store itself";
        }

        public void deleteFile(String msg, String name) throws IOException {
            deletesLogger.trace("{}: delete file {}", msg, name);
            super.deleteFile(name);
        }

        @Override
        public void deleteFile(String name) throws IOException {
            deleteFile("StoreDirectory.deleteFile", name);
        }

        /**
         * 通过该方法来实现关闭目录 由 StoreDirectory自己调用
         *
         * @throws IOException
         */
        private void innerClose() throws IOException {
            super.close();
        }

        @Override
        public String toString() {
            return "store(" + in.toString() + ")";
        }
    }

    /**
     * Represents a snapshot of the current directory build from the latest Lucene commit.
     * Only files that are part of the last commit are considered in this datastructure.
     * For backwards compatibility the snapshot might include legacy checksums that
     * are derived from a dedicated checksum file written by older elasticsearch version pre 1.3
     * <p>
     * Note: This class will ignore the {@code segments.gen} file since it's optional and might
     * change concurrently for safety reasons.
     *
     * @see StoreFileMetadata
     * 描述分片某次lucene.commit 对应的 CommitInfo 生成的快照
     */
    public static final class MetadataSnapshot implements Iterable<StoreFileMetadata>, Writeable {

        /**
         * StoreFileMetadata 就是描述某个文件有多大 校验和是啥等等
         */
        private final Map<String, StoreFileMetadata> metadata;

        public static final MetadataSnapshot EMPTY = new MetadataSnapshot();

        private final Map<String, String> commitUserData;

        /**
         * 此时总计有多少doc
         */
        private final long numDocs;

        public MetadataSnapshot(Map<String, StoreFileMetadata> metadata, Map<String, String> commitUserData, long numDocs) {
            this.metadata = metadata;
            this.commitUserData = commitUserData;
            this.numDocs = numDocs;
        }

        MetadataSnapshot() {
            metadata = emptyMap();
            commitUserData = emptyMap();
            numDocs = 0;
        }

        /**
         * 读取本次shard相关的元数据信息
         *
         * @param commit    某次调用lucene.commit 返回的结果  对应一个目标目录 一组文件 以及 gen
         *                  该属性允许为空
         * @param directory 目标文件所在的目录
         * @param logger
         * @throws IOException
         */
        MetadataSnapshot(IndexCommit commit, Directory directory, Logger logger) throws IOException {
            LoadedMetadata loadedMetadata = loadMetadata(commit, directory, logger);
            metadata = loadedMetadata.fileMetadata;
            commitUserData = loadedMetadata.userData;
            numDocs = loadedMetadata.numDocs;
            assert metadata.isEmpty() || numSegmentFiles() == 1 : "numSegmentFiles: " + numSegmentFiles();
        }

        /**
         * Read from a stream.
         */
        public MetadataSnapshot(StreamInput in) throws IOException {
            final int size = in.readVInt();
            Map<String, StoreFileMetadata> metadata = new HashMap<>();
            for (int i = 0; i < size; i++) {
                StoreFileMetadata meta = new StoreFileMetadata(in);
                metadata.put(meta.name(), meta);
            }
            Map<String, String> commitUserData = new HashMap<>();
            int num = in.readVInt();
            for (int i = num; i > 0; i--) {
                commitUserData.put(in.readString(), in.readString());
            }

            this.metadata = unmodifiableMap(metadata);
            this.commitUserData = unmodifiableMap(commitUserData);
            this.numDocs = in.readLong();
            assert metadata.isEmpty() || numSegmentFiles() == 1 : "numSegmentFiles: " + numSegmentFiles();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.metadata.size());
            for (StoreFileMetadata meta : this) {
                meta.writeTo(out);
            }
            out.writeVInt(commitUserData.size());
            for (Map.Entry<String, String> entry : commitUserData.entrySet()) {
                out.writeString(entry.getKey());
                out.writeString(entry.getValue());
            }
            out.writeLong(numDocs);
        }

        /**
         * Returns the number of documents in this store snapshot
         */
        public long getNumDocs() {
            return numDocs;
        }

        /**
         * 对应某个 segment_N 文件
         */
        static class LoadedMetadata {
            /**
             * 内部每个文件的大小 校验和  hash等
             */
            final Map<String, StoreFileMetadata> fileMetadata;
            final Map<String, String> userData;
            /**
             * 本次提交的所有segment 总计涉及到多少doc
             */
            final long numDocs;

            LoadedMetadata(Map<String, StoreFileMetadata> fileMetadata, Map<String, String> userData, long numDocs) {
                this.fileMetadata = fileMetadata;
                this.userData = userData;
                this.numDocs = numDocs;
            }
        }

        /**
         * @param commit
         * @param directory
         * @param logger
         * @return
         * @throws IOException
         */
        static LoadedMetadata loadMetadata(IndexCommit commit, Directory directory, Logger logger) throws IOException {
            long numDocs;
            Map<String, StoreFileMetadata> builder = new HashMap<>();
            Map<String, String> commitUserDataBuilder = new HashMap<>();
            try {
                // 通过commit找到对应的segment_N 并还原成segmentInfos对象
                final SegmentInfos segmentCommitInfos = Store.readSegmentsInfo(commit, directory);
                // 获取本次commit 产生的doc数量
                numDocs = Lucene.getNumDocs(segmentCommitInfos);
                // 将用户自定义的信息存入  其中包含了field的编解码方式
                commitUserDataBuilder.putAll(segmentCommitInfos.getUserData());
                // we don't know which version was used to write so we take the max version.
                // 获取这些段中最小的版本号
                Version maxVersion = segmentCommitInfos.getMinSegmentLuceneVersion();

                // 每次commit 会对应一个segmentInfos  每个线程往lucene写入的数据会对应一个 segment
                for (SegmentCommitInfo info : segmentCommitInfos) {
                    final Version version = info.info.getVersion();
                    if (version == null) {
                        // version is written since 3.1+: we should have already hit IndexFormatTooOld.
                        throw new IllegalArgumentException("expected valid version value: " + info.info.toString());
                    }
                    // 通过遍历每个segment 后  更新maxVersion
                    if (version.onOrAfter(maxVersion)) {
                        maxVersion = version;
                    }
                    // 所有文件都需要校验 checksum  这里不包含segment_n
                    for (String file : info.files()) {
                        // 将每个文件长度 校验和等信息设置到 metadata中
                        checksumFromLuceneFile(directory, file, builder, logger, version,
                            SEGMENT_INFO_EXTENSION.equals(IndexFileNames.getExtension(file)));
                    }
                }
                if (maxVersion == null) {
                    maxVersion = org.elasticsearch.Version.CURRENT.minimumIndexCompatibilityVersion().luceneVersion;
                }
                final String segmentsFile = segmentCommitInfos.getSegmentsFileName();
                // 这里在校验segment_n 文件
                checksumFromLuceneFile(directory, segmentsFile, builder, logger, maxVersion, true);
            } catch (CorruptIndexException | IndexNotFoundException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                // we either know the index is corrupted or it's just not there
                throw ex;
            } catch (Exception ex) {
                try {
                    // Lucene checks the checksum after it tries to lookup the codec etc.
                    // in that case we might get only IAE or similar exceptions while we are really corrupt...
                    // TODO we should check the checksum in lucene if we hit an exception
                    logger.warn(() ->
                        new ParameterizedMessage("failed to build store metadata. checking segment info integrity " +
                            "(with commit [{}])", commit == null ? "no" : "yes"), ex);
                    Lucene.checkSegmentInfoIntegrity(directory);
                } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException cex) {
                    cex.addSuppressed(ex);
                    throw cex;
                } catch (Exception inner) {
                    inner.addSuppressed(ex);
                    throw inner;
                }
                throw ex;
            }
            return new LoadedMetadata(unmodifiableMap(builder), unmodifiableMap(commitUserDataBuilder), numDocs);
        }

        /**
         * @param directory
         * @param file
         * @param builder
         * @param logger
         * @param version
         * @param readFileAsHash 当目标文件的后缀名是 .si 时 该标识为true
         *                       si实际上就是存储 segment信息的
         * @throws IOException
         */
        private static void checksumFromLuceneFile(Directory directory, String file, Map<String, StoreFileMetadata> builder,
                                                   Logger logger, Version version, boolean readFileAsHash) throws IOException {
            final String checksum;
            final BytesRefBuilder fileHash = new BytesRefBuilder();
            try (IndexInput in = directory.openInput(file, IOContext.READONCE)) {
                final long length;
                try {
                    length = in.length();
                    if (length < CodecUtil.footerLength()) {
                        // truncated files trigger IAE if we seek negative... these files are really corrupted though
                        throw new CorruptIndexException("Can't retrieve checksum from file: " + file + " file length must be >= " +
                            CodecUtil.footerLength() + " but was: " + in.length(), in);
                    }
                    // 校验和相关的不细看了
                    if (readFileAsHash) {
                        // additional safety we checksum the entire file we read the hash for...
                        final VerifyingIndexInput verifyingIndexInput = new VerifyingIndexInput(in);
                        // 计算文件的hash值  只适用于小文件
                        hashFile(fileHash, new InputStreamIndexInput(verifyingIndexInput, length), length);
                        checksum = digestToString(verifyingIndexInput.verify());
                    } else {
                        checksum = digestToString(CodecUtil.retrieveChecksum(in));
                    }

                } catch (Exception ex) {
                    logger.debug(() -> new ParameterizedMessage("Can retrieve checksum from file [{}]", file), ex);
                    throw ex;
                }
                builder.put(file, new StoreFileMetadata(file, length, checksum, version, fileHash.get()));
            }
        }

        /**
         * Computes a strong hash value for small files. Note that this method should only be used for files &lt; 1MB
         *
         * @param in   代表lucene写入时产生的某个文件对应的输入流
         * @param size 文件总大小
         */
        public static void hashFile(BytesRefBuilder fileHash, InputStream in, long size) throws IOException {
            // 最多只基于 1M的数据计算hash值
            final int len = (int) Math.min(1024 * 1024, size); // for safety we limit this to 1MB
            fileHash.grow(len);
            fileHash.setLength(len);
            // 使用in 填满buffer
            final int readBytes = Streams.readFully(in, fileHash.bytes(), 0, len);
            assert readBytes == len : Integer.toString(readBytes) + " != " + Integer.toString(len);
            assert fileHash.length() == len : Integer.toString(fileHash.length()) + " != " + Integer.toString(len);
        }

        @Override
        public Iterator<StoreFileMetadata> iterator() {
            return metadata.values().iterator();
        }

        public StoreFileMetadata get(String name) {
            return metadata.get(name);
        }

        public Map<String, StoreFileMetadata> asMap() {
            return metadata;
        }

        private static final String DEL_FILE_EXTENSION = "del"; // legacy delete file
        private static final String LIV_FILE_EXTENSION = "liv"; // lucene 5 delete file
        private static final String SEGMENT_INFO_EXTENSION = "si";

        /**
         * Returns a diff between the two snapshots that can be used for recovery. The given snapshot is treated as the
         * recovery target and this snapshot as the source. The returned diff will hold a list of files that are:
         * <ul>
         * <li>identical: they exist in both snapshots and they can be considered the same ie. they don't need to be recovered</li>
         * <li>different: they exist in both snapshots but their they are not identical</li>
         * <li>missing: files that exist in the source but not in the target</li>
         * </ul>
         * This method groups file into per-segment files and per-commit files. A file is treated as
         * identical if and on if all files in it's group are identical. On a per-segment level files for a segment are treated
         * as identical iff:
         * <ul>
         * <li>all files in this segment have the same checksum</li>
         * <li>all files in this segment have the same length</li>
         * <li>the segments {@code .si} files hashes are byte-identical Note: This is a using a perfect hash function,
         * The metadata transfers the {@code .si} file content as it's hash</li>
         * </ul>
         * <p>
         * The {@code .si} file contains a lot of diagnostics including a timestamp etc. in the future there might be
         * unique segment identifiers in there hardening this method further.
         * <p>
         * The per-commit files handles very similar. A commit is composed of the {@code segments_N} files as well as generational files
         * like deletes ({@code _x_y.del}) or field-info ({@code _x_y.fnm}) files. On a per-commit level files for a commit are treated
         * as identical iff:
         * <ul>
         * <li>all files belonging to this commit have the same checksum</li>
         * <li>all files belonging to this commit have the same length</li>
         * <li>the segments file {@code segments_N} files hashes are byte-identical Note: This is a using a perfect hash function,
         * The metadata transfers the {@code segments_N} file content as it's hash</li>
         * </ul>
         * <p>
         * NOTE: this diff will not contain the {@code segments.gen} file. This file is omitted on recovery.
         * 获取当前元数据快照 与另一个元数据快照不同的部分
         */
        public RecoveryDiff recoveryDiff(MetadataSnapshot recoveryTargetSnapshot) {
            // 存储相同的数据
            final List<StoreFileMetadata> identical = new ArrayList<>();
            // 不同的数据
            final List<StoreFileMetadata> different = new ArrayList<>();
            // recoveryTargetSnapshot 与该对象相比缺失的
            final List<StoreFileMetadata> missing = new ArrayList<>();

            // 某次commit生成的segment下面的文件
            final Map<String, List<StoreFileMetadata>> perSegment = new HashMap<>();
            // 每次提交都会生成的文件
            final List<StoreFileMetadata> perCommitStoreFiles = new ArrayList<>();

            for (StoreFileMetadata meta : this) {
                // 忽略遗留代码
                if (IndexFileNames.OLD_SEGMENTS_GEN.equals(meta.name())) { // legacy
                    continue; // we don't need that file at all
                }

                final String segmentId = IndexFileNames.parseSegmentName(meta.name());
                // 获取拓展名
                final String extension = IndexFileNames.getExtension(meta.name());
                // 代表是 segment_N 文件
                if (IndexFileNames.SEGMENTS.equals(segmentId) ||
                    DEL_FILE_EXTENSION.equals(extension) || LIV_FILE_EXTENSION.equals(extension)) {
                    // only treat del files as per-commit files fnm files are generational but only for upgradable DV
                    perCommitStoreFiles.add(meta);
                } else {
                    perSegment.computeIfAbsent(segmentId, k -> new ArrayList<>()).add(meta);
                }
            }
            final ArrayList<StoreFileMetadata> identicalFiles = new ArrayList<>();
            // 遍历2次 分别是perSegment，perCommitStoreFiles
            for (List<StoreFileMetadata> segmentFiles : Iterables.concat(perSegment.values(), Collections.singleton(perCommitStoreFiles))) {
                identicalFiles.clear();
                boolean consistent = true;
                // 再挨个比较内部的数据
                for (StoreFileMetadata meta : segmentFiles) {
                    StoreFileMetadata storeFileMetadata = recoveryTargetSnapshot.get(meta.name());
                    if (storeFileMetadata == null) {
                        consistent = false;
                        missing.add(meta);
                        // 只要校验和不一样就是false 代表数据内容发生了改变
                    } else if (storeFileMetadata.isSame(meta) == false) {
                        consistent = false;
                        different.add(meta);
                    } else {
                        identicalFiles.add(meta);
                    }
                }
                if (consistent) {
                    identical.addAll(identicalFiles);
                } else {
                    // make sure all files are added - this can happen if only the deletes are different
                    different.addAll(identicalFiles);
                }
            }
            RecoveryDiff recoveryDiff = new RecoveryDiff(Collections.unmodifiableList(identical),
                Collections.unmodifiableList(different), Collections.unmodifiableList(missing));
            assert recoveryDiff.size() == this.metadata.size() - (metadata.containsKey(IndexFileNames.OLD_SEGMENTS_GEN) ? 1 : 0)
                : "some files are missing recoveryDiff size: [" + recoveryDiff.size() + "] metadata size: [" +
                this.metadata.size() + "] contains  segments.gen: [" + metadata.containsKey(IndexFileNames.OLD_SEGMENTS_GEN) + "]";
            return recoveryDiff;
        }

        /**
         * Returns the number of files in this snapshot
         */
        public int size() {
            return metadata.size();
        }

        public Map<String, String> getCommitUserData() {
            return commitUserData;
        }

        /**
         * returns the history uuid the store points at, or null if nonexistent.
         */
        public String getHistoryUUID() {
            return commitUserData.get(Engine.HISTORY_UUID_KEY);
        }

        /**
         * Returns true iff this metadata contains the given file.
         */
        public boolean contains(String existingFile) {
            return metadata.containsKey(existingFile);
        }

        /**
         * Returns the segments file that this metadata snapshot represents or null if the snapshot is empty.
         */
        public StoreFileMetadata getSegmentsFile() {
            for (StoreFileMetadata file : this) {
                if (file.name().startsWith(IndexFileNames.SEGMENTS)) {
                    return file;
                }
            }
            assert metadata.isEmpty();
            return null;
        }


        private int numSegmentFiles() { // only for asserts
            int count = 0;
            for (StoreFileMetadata file : this) {
                if (file.name().startsWith(IndexFileNames.SEGMENTS)) {
                    count++;
                }
            }
            return count;
        }

        /**
         * Returns the sync id of the commit point that this MetadataSnapshot represents.
         *
         * @return sync id if exists, else null
         */
        public String getSyncId() {
            return commitUserData.get(Engine.SYNC_COMMIT_ID);
        }
    }

    /**
     * A class representing the diff between a recovery source and recovery target
     *
     * @see MetadataSnapshot#recoveryDiff(org.elasticsearch.index.store.Store.MetadataSnapshot)
     * 用于表示 前后2个metadata不同的地方
     */
    public static final class RecoveryDiff {
        /**
         * Files that exist in both snapshots and they can be considered the same ie. they don't need to be recovered
         */
        public final List<StoreFileMetadata> identical;
        /**
         * Files that exist in both snapshots but their they are not identical
         */
        public final List<StoreFileMetadata> different;
        /**
         * Files that exist in the source but not in the target
         */
        public final List<StoreFileMetadata> missing;

        RecoveryDiff(List<StoreFileMetadata> identical, List<StoreFileMetadata> different, List<StoreFileMetadata> missing) {
            this.identical = identical;
            this.different = different;
            this.missing = missing;
        }

        /**
         * Returns the sum of the files in this diff.
         */
        public int size() {
            return identical.size() + different.size() + missing.size();
        }

        @Override
        public String toString() {
            return "RecoveryDiff{" +
                "identical=" + identical +
                ", different=" + different +
                ", missing=" + missing +
                '}';
        }
    }


    /**
     * Returns true if the file is auto-generated by the store and shouldn't be deleted during cleanup.
     * This includes write lock and checksum files
     */
    public static boolean isAutogenerated(String name) {
        return IndexWriter.WRITE_LOCK_NAME.equals(name);
    }

    /**
     * Produces a string representation of the given digest value.
     */
    public static String digestToString(long digest) {
        return Long.toString(digest, Character.MAX_RADIX);
    }


    /**
     * VerifyingIndexOutput 定义了一个校验api
     */
    static class LuceneVerifyingIndexOutput extends VerifyingIndexOutput {

        /**
         * 描述文件的hash length checksum
         */
        private final StoreFileMetadata metadata;
        private long writtenBytes;
        /**
         * 读取校验和的起始偏移量
         */
        private final long checksumPosition;
        private String actualChecksum;
        private final byte[] footerChecksum = new byte[8]; // this holds the actual footer checksum data written by to this output

        LuceneVerifyingIndexOutput(StoreFileMetadata metadata, IndexOutput out) {
            super(out);
            this.metadata = metadata;
            // 最后8位就是校验和
            checksumPosition = metadata.length() - 8; // the last 8 bytes are the checksum - we store it in footerChecksum
        }

        /**
         * 对应校验的逻辑
         *
         * @throws IOException
         */
        @Override
        public void verify() throws IOException {
            String footerDigest = null;
            if (metadata.checksum().equals(actualChecksum) && writtenBytes == metadata.length()) {
                ByteArrayIndexInput indexInput = new ByteArrayIndexInput("checksum", this.footerChecksum);
                footerDigest = digestToString(indexInput.readLong());
                if (metadata.checksum().equals(footerDigest)) {
                    return;
                }
            }
            throw new CorruptIndexException("verification failed (hardware problem?) : expected=" + metadata.checksum() +
                " actual=" + actualChecksum + " footer=" + footerDigest + " writtenLength=" + writtenBytes + " expectedLength=" +
                metadata.length() + " (resource=" + metadata.toString() + ")", "VerifyingIndexOutput(" + metadata.name() + ")");
        }

        @Override
        public void writeByte(byte b) throws IOException {
            // 继续往当前文件中输出内容
            final long writtenBytes = this.writtenBytes++;
            if (writtenBytes >= checksumPosition) { // we are writing parts of the checksum....
                if (writtenBytes == checksumPosition) {
                    readAndCompareChecksum();
                }
                final int index = Math.toIntExact(writtenBytes - checksumPosition);
                if (index < footerChecksum.length) {
                    footerChecksum[index] = b;
                    if (index == footerChecksum.length - 1) {
                        verify(); // we have recorded the entire checksum
                    }
                } else {
                    verify(); // fail if we write more than expected
                    throw new AssertionError("write past EOF expected length: " + metadata.length() +
                        " writtenBytes: " + writtenBytes);
                }
            }
            out.writeByte(b);
        }

        private void readAndCompareChecksum() throws IOException {
            actualChecksum = digestToString(getChecksum());
            if (!metadata.checksum().equals(actualChecksum)) {
                throw new CorruptIndexException("checksum failed (hardware problem?) : expected=" + metadata.checksum() +
                    " actual=" + actualChecksum +
                    " (resource=" + metadata.toString() + ")", "VerifyingIndexOutput(" + metadata.name() + ")");
            }
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            if (writtenBytes + length > checksumPosition) {
                for (int i = 0; i < length; i++) { // don't optimze writing the last block of bytes
                    writeByte(b[offset + i]);
                }
            } else {
                out.writeBytes(b, offset, length);
                writtenBytes += length;
            }
        }
    }

    /**
     * Index input that calculates checksum as data is read from the input.
     * <p>
     * This class supports random access (it is possible to seek backward and forward) in order to accommodate retry
     * mechanism that is used in some repository plugins (S3 for example). However, the checksum is only calculated on
     * the first read. All consecutive reads of the same data are not used to calculate the checksum.
     * 这种校验类的先忽略吧  不影响主流程
     */
    static class VerifyingIndexInput extends ChecksumIndexInput {
        private final IndexInput input;
        private final Checksum digest;
        private final long checksumPosition;
        private final byte[] checksum = new byte[8];
        private long verifiedPosition = 0;

        VerifyingIndexInput(IndexInput input) {
            this(input, new BufferedChecksum(new CRC32()));
        }

        VerifyingIndexInput(IndexInput input, Checksum digest) {
            super("VerifyingIndexInput(" + input + ")");
            this.input = input;
            this.digest = digest;
            checksumPosition = input.length() - 8;
        }

        @Override
        public byte readByte() throws IOException {
            long pos = input.getFilePointer();
            final byte b = input.readByte();
            pos++;
            if (pos > verifiedPosition) {
                if (pos <= checksumPosition) {
                    digest.update(b);
                } else {
                    checksum[(int) (pos - checksumPosition - 1)] = b;
                }
                verifiedPosition = pos;
            }
            return b;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len)
            throws IOException {
            long pos = input.getFilePointer();
            input.readBytes(b, offset, len);
            if (pos + len > verifiedPosition) {
                // Conversion to int is safe here because (verifiedPosition - pos) can be at most len, which is integer
                int alreadyVerified = (int) Math.max(0, verifiedPosition - pos);
                if (pos < checksumPosition) {
                    if (pos + len < checksumPosition) {
                        digest.update(b, offset + alreadyVerified, len - alreadyVerified);
                    } else {
                        int checksumOffset = (int) (checksumPosition - pos);
                        if (checksumOffset - alreadyVerified > 0) {
                            digest.update(b, offset + alreadyVerified, checksumOffset - alreadyVerified);
                        }
                        System.arraycopy(b, offset + checksumOffset, checksum, 0, len - checksumOffset);
                    }
                } else {
                    // Conversion to int is safe here because checksumPosition is (file length - 8) so
                    // (pos - checksumPosition) cannot be bigger than 8 unless we are reading after the end of file
                    assert pos - checksumPosition < 8;
                    System.arraycopy(b, offset, checksum, (int) (pos - checksumPosition), len);
                }
                verifiedPosition = pos + len;
            }
        }

        @Override
        public long getChecksum() {
            return digest.getValue();
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < verifiedPosition) {
                // going within verified region - just seek there
                input.seek(pos);
            } else {
                if (verifiedPosition > getFilePointer()) {
                    // portion of the skip region is verified and portion is not
                    // skipping the verified portion
                    input.seek(verifiedPosition);
                    // and checking unverified
                    skipBytes(pos - verifiedPosition);
                } else {
                    skipBytes(pos - getFilePointer());
                }
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        @Override
        public long getFilePointer() {
            return input.getFilePointer();
        }

        @Override
        public long length() {
            return input.length();
        }

        @Override
        public IndexInput clone() {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            throw new UnsupportedOperationException();
        }

        public long getStoredChecksum() {
            return new ByteArrayDataInput(checksum).readLong();
        }

        public long verify() throws CorruptIndexException {
            long storedChecksum = getStoredChecksum();
            if (getChecksum() == storedChecksum) {
                return storedChecksum;
            }
            throw new CorruptIndexException("verification failed : calculated=" + Store.digestToString(getChecksum()) +
                " stored=" + Store.digestToString(storedChecksum), this);
        }

    }

    /**
     * 删除一组指定的文件
     *
     * @param files
     */
    public void deleteQuiet(String... files) {
        ensureOpen();
        StoreDirectory directory = this.directory;
        for (String file : files) {
            try {
                directory.deleteFile("Store.deleteQuiet", file);
            } catch (Exception ex) {
                // ignore :(
            }
        }
    }

    /**
     * Marks this store as corrupted. This method writes a {@code corrupted_${uuid}} file containing the given exception
     * message. If a store contains a {@code corrupted_${uuid}} file {@link #isMarkedCorrupted()} will return <code>true</code>.
     * 当产生了一个异常时 生成一个标记文件 也就是 corrupted开头的文件
     */
    public void markStoreCorrupted(IOException exception) throws IOException {
        ensureOpen();
        // 如果已经写入了异常文件 就不再写入了
        if (!isMarkedCorrupted()) {
            final String corruptionMarkerName = CORRUPTED_MARKER_NAME_PREFIX + UUIDs.randomBase64UUID();
            try (IndexOutput output = this.directory().createOutput(corruptionMarkerName, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(output, CODEC, CORRUPTED_MARKER_CODEC_VERSION);
                BytesStreamOutput out = new BytesStreamOutput();
                out.writeException(exception);
                BytesReference bytes = out.bytes();
                output.writeVInt(bytes.length());
                BytesRef ref = bytes.toBytesRef();
                output.writeBytes(ref.bytes, ref.offset, ref.length);
                CodecUtil.writeFooter(output);
            } catch (IOException ex) {
                logger.warn("Can't mark store as corrupted", ex);
            }
            // 同步刷盘
            directory().sync(Collections.singleton(corruptionMarkerName));
        }
    }

    /**
     * A listener that is executed once the store is closed and all references to it are released
     */
    public interface OnClose extends Consumer<ShardLock> {
        OnClose EMPTY = new OnClose() {
            /**
             * This method is called while the provided {@link org.elasticsearch.env.ShardLock} is held.
             * This method is only called once after all resources for a store are released.
             */
            @Override
            public void accept(ShardLock Lock) {
            }
        };
    }

    /**
     * creates an empty lucene index and a corresponding empty translog. Any existing data will be deleted.
     */
    public void createEmpty(Version luceneVersion) throws IOException {
        metadataLock.writeLock().lock();
        // 打开一个空的indexWriter 该目录下之前还没有其他segmentInfos
        try (IndexWriter writer = newEmptyIndexWriter(directory, luceneVersion)) {
            final Map<String, String> map = new HashMap<>();
            map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
            map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            map.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            map.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
            // 这里预先写入了一些用户数据 在生成metadata时 也会带出来
            updateCommitData(writer, map);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }


    /**
     * Marks an existing lucene index with a new history uuid.
     * This is used to make sure no existing shard will recovery from this index using ops based recovery.
     */
    public void bootstrapNewHistory() throws IOException {
        metadataLock.writeLock().lock();
        try {
            // 从当前目录中获取最后一次提交的segment_N文件     并获取上次提交的数据中用户的数据
            Map<String, String> userData = readLastCommittedSegmentsInfo().getUserData();
            // 获取之前写入了多少次数据  每当执行一次op时 应该就会将seq+1
            final long maxSeqNo = Long.parseLong(userData.get(SequenceNumbers.MAX_SEQ_NO));
            // 同步检查点也会存储在userData中       也就是每次commit工作都会记录此时本地的检查点/seq 并存储到userData持久化到segment_N文件中
            final long localCheckpoint = Long.parseLong(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));

            // 这里生成了一个新的 segment_N文件 区别只是使用的historyId 不同 其他数据没有变化
            bootstrapNewHistory(localCheckpoint, maxSeqNo);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * Marks an existing lucene index with a new history uuid and sets the given local checkpoint
     * as well as the maximum sequence number.
     * This is used to make sure no existing shard will recover from this index using ops based recovery.
     *
     * @see SequenceNumbers#LOCAL_CHECKPOINT_KEY
     * @see SequenceNumbers#MAX_SEQ_NO
     * 使用之前已经commit的最新的segment_N文件中记录的检查点/seq 并用于生成一个新的segment_N文件
     */
    public void bootstrapNewHistory(long localCheckpoint, long maxSeqNo) throws IOException {
        metadataLock.writeLock().lock();
        try (IndexWriter writer = newAppendingIndexWriter(directory, null)) {
            final Map<String, String> map = new HashMap<>();
            // 可以看到这里更新了 historyUUID
            map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
            map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCheckpoint));
            map.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
            updateCommitData(writer, map);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * Force bakes the given translog generation as recovery information in the lucene index. This is
     * used when recovering from a snapshot or peer file based recovery where a new empty translog is
     * created and the existing lucene index needs should be changed to use it.
     * 使用新的事务日志id
     */
    public void associateIndexWithNewTranslog(final String translogUUID) throws IOException {
        metadataLock.writeLock().lock();
        // 打开之前存在的某个segment_N 并在更新事务日志id后刷盘
        try (IndexWriter writer = newAppendingIndexWriter(directory, null)) {
            if (translogUUID.equals(getUserData(writer).get(Translog.TRANSLOG_UUID_KEY))) {
                throw new IllegalArgumentException("a new translog uuid can't be equal to existing one. got [" + translogUUID + "]");
            }
            updateCommitData(writer, Map.of(Translog.TRANSLOG_UUID_KEY, translogUUID));
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * Keeping existing unsafe commits when opening an engine can be problematic because these commits are not safe
     * at the recovering time but they can suddenly become safe in the future.
     * The following issues can happen if unsafe commits are kept oninit.
     * <p>
     * 1. Replica can use unsafe commit in peer-recovery. This happens when a replica with a safe commit c1(max_seqno=1)
     * and an unsafe commit c2(max_seqno=2) recovers from a primary with c1(max_seqno=1). If a new document(seqno=2)
     * is added without flushing, the global checkpoint is advanced to 2; and the replica recovers again, it will use
     * the unsafe commit c2(max_seqno=2 at most gcp=2) as the starting commit for sequenced-based recovery even the
     * commit c2 contains a stale operation and the document(with seqno=2) will not be replicated to the replica.
     * <p>
     * 2. Min translog gen for recovery can go backwards in peer-recovery. This happens when are replica with a safe commit
     * c1(local_checkpoint=1, recovery_translog_gen=1) and an unsafe commit c2(local_checkpoint=2, recovery_translog_gen=2).
     * The replica recovers from a primary, and keeps c2 as the last commit, then sets last_translog_gen to 2. Flushing a new
     * commit on the replica will cause exception as the new last commit c3 will have recovery_translog_gen=1. The recovery
     * translog generation of a commit is calculated based on the current local checkpoint. The local checkpoint of c3 is 1
     * while the local checkpoint of c2 is 2.
     * @param translogPath 存储事务日志的路径
     * 去除一些不安全的commit数据
     *                     安全的提交点即是指已经同步到集群其他节点 并完成持久化的segment信息
     *                     这里使用的deletionPolicy是lucene的默认策略 而在engine中创建的indexWriter会使用ES封装的删除策略
     */
    public void trimUnsafeCommits(final Path translogPath) throws IOException {
        metadataLock.writeLock().lock();
        try {
            // 将此时目录下所有的segment_N 文件读取出来  每个segment_N 还原后都会得到某次commit相关的所有索引文件信息
            final List<IndexCommit> existingCommits = DirectoryReader.listCommits(directory);
            assert existingCommits.isEmpty() == false;
            // 在返回时已经排序过了 最后一个就是最新的commit
            final IndexCommit lastIndexCommit = existingCommits.get(existingCommits.size() - 1);
            // 每个 segmentInfos内的用户数据中有一个translogUUID 可以找到一个事务文件
            final String translogUUID = lastIndexCommit.getUserData().get(Translog.TRANSLOG_UUID_KEY);
            // 除了事务文件外 一起的还有checkPoint文件   这里获取文件中记录的全局检查点
            final long lastSyncedGlobalCheckpoint = Translog.readGlobalCheckpoint(translogPath, translogUUID);
            // 这里只会保留一个segment_N 文件   检查点之前的数据太旧可以丢弃 之后的数据还没有同步到其他节点上也要丢弃
            final IndexCommit startingIndexCommit = CombinedDeletionPolicy.findSafeCommitPoint(existingCommits, lastSyncedGlobalCheckpoint);
            // 在初始化时 会自动删除后面的数据
            if (startingIndexCommit.equals(lastIndexCommit) == false) {
                try (IndexWriter writer = newAppendingIndexWriter(directory, startingIndexCommit)) {
                    // this achieves two things:
                    // - by committing a new commit based on the starting commit, it make sure the starting commit will be opened
                    // - deletes any other commit (by lucene standard deletion policy)
                    //
                    // note that we can't just use IndexCommit.delete() as we really want to make sure that those files won't be used
                    // even if a virus scanner causes the files not to be used.

                    // The new commit will use segment files from the starting commit but userData from the last commit by default.
                    // Thus, we need to manually set the userData from the starting commit to the new commit.
                    // 初始化的时候 userData 默认还是使用最新的segment对应的  即使在初始化时指定了 startingIndexCommit 不会自动替换 所以这里要手动设置
                    writer.setLiveCommitData(startingIndexCommit.getUserData().entrySet());
                    writer.commit();
                }
            }
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    /**
     * Returns a {@link org.elasticsearch.index.seqno.SequenceNumbers.CommitInfo} of the safe commit if exists.
     * 通过全局检查点 反查 safeCommit
     */
    public Optional<SequenceNumbers.CommitInfo> findSafeIndexCommit(long globalCheckpoint) throws IOException {
        final List<IndexCommit> commits = DirectoryReader.listCommits(directory);
        assert commits.isEmpty() == false : "no commit found";
        // 找到所有小于全局检查点的 IndexCommit最大的那个 也就是往上无限接近globalCheckpoint的
        final IndexCommit safeCommit = CombinedDeletionPolicy.findSafeCommitPoint(commits, globalCheckpoint);
        // 获取那个时刻 对应的本地已经写入的数据中最大的 seqNo 以及 localCheckpoint
        // 这个数据可能已经持久化到lucene中了  应该要将 globalCheckpoint -> localCheckpoint之间的数据清除掉吧
        final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(safeCommit.getUserData().entrySet());
        // all operations of the safe commit must be at most the global checkpoint.
        // 代表这个时刻本地的数据 还小于全局检查点的数据 需要恢复 localCheckpoint -> globalCheckpoint 之间的数据
        if (commitInfo.maxSeqNo <= globalCheckpoint) {
            return Optional.of(commitInfo);
        } else {
            return Optional.empty();
        }
    }

    /**
     * 直接生成文件
     *
     * @param writer
     * @param keysToUpdate
     * @throws IOException
     */
    private static void updateCommitData(IndexWriter writer, Map<String, String> keysToUpdate) throws IOException {
        // 从writer中获取其他 提交的用户数据
        final Map<String, String> userData = getUserData(writer);
        // 合并
        userData.putAll(keysToUpdate);
        // 覆盖用户数据
        writer.setLiveCommitData(userData.entrySet());
        // 生成文件
        writer.commit();
    }

    private static Map<String, String> getUserData(IndexWriter writer) {
        final Map<String, String> userData = new HashMap<>();
        writer.getLiveCommitData().forEach(e -> userData.put(e.getKey(), e.getValue()));
        return userData;
    }

    /**
     * 生成一个追加模式的 IndexWriter对象
     * @param dir
     * @param commit
     * @return
     * @throws IOException
     */
    private static IndexWriter newAppendingIndexWriter(final Directory dir, final IndexCommit commit) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig()
            .setIndexCommit(commit)
            .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        return new IndexWriter(dir, iwc);
    }

    /**
     * 初始化 IndexWriter
     *
     * @param dir
     * @param luceneVersion
     * @return
     * @throws IOException
     */
    private static IndexWriter newEmptyIndexWriter(final Directory dir, final Version luceneVersion) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig()
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            .setIndexCreatedVersionMajor(luceneVersion.major);
        return new IndexWriter(dir, iwc);
    }

    private static IndexWriterConfig newIndexWriterConfig() {
        return new IndexWriterConfig(null)
            .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setCommitOnClose(false)
            // we don't want merges to happen here - we call maybe merge on the engine
            // later once we stared it up otherwise we would need to wait for it here
            // we also don't specify a codec here and merges should use the engines for this index
            .setMergePolicy(NoMergePolicy.INSTANCE);
    }
}
