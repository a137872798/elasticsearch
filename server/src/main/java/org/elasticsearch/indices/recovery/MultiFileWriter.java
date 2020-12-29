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
package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 通过MultiFileTransfer传输过来的数据 通过 replicaShard.MultiFileWriter 将数据写入到本地的临时文件
 */
public class MultiFileWriter extends AbstractRefCounted implements Releasable {

    /**
     *
     * @param store  存储相关文件的目录
     * @param indexState  描述本次恢复索引涉及到的所有文件
     * @param tempFilePrefix  在恢复过程中创建的临时文件会以它作为前缀
     * @param logger
     * @param ensureOpen  确保此时恢复任务还处于运行状态
     */
    public MultiFileWriter(Store store, RecoveryState.Index indexState, String tempFilePrefix, Logger logger, Runnable ensureOpen) {
        super("multi_file_writer");
        this.store = store;
        this.indexState = indexState;
        this.tempFilePrefix = tempFilePrefix;
        this.logger = logger;
        this.ensureOpen = ensureOpen;
    }

    private final Runnable ensureOpen;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Logger logger;
    private final Store store;
    private final RecoveryState.Index indexState;
    private final String tempFilePrefix;

    /**
     * key: 源文件  value: 临时文件输出流
     * 当源文件的数据全部写如刀临时文件后 从该容器中移除 而之后想从相同的源文件中复制数据会发现对应的临时文件还没有从tempFileNames中移除 所以不会出现问题
     */
    private final ConcurrentMap<String, IndexOutput> openIndexOutputs = ConcurrentCollections.newConcurrentMap();

    /**
     * MultiFileWriter  代表同时对多个索引文件进行写入
     * 每个FileChunkWriter 对应一个文件
     */
    private final ConcurrentMap<String, FileChunkWriter> fileChunkWriters = ConcurrentCollections.newConcurrentMap();


    /**
     * key: 临时文件 value: 源文件
     */
    final Map<String, String> tempFileNames = ConcurrentCollections.newConcurrentMap();

    /**
     * 将某个文件的某个数据片段写入到临时文件中
     * @param fileMetadata
     * @param position
     * @param content
     * @param lastChunk
     * @throws IOException
     */
    public void writeFileChunk(StoreFileMetadata fileMetadata, long position, BytesReference content, boolean lastChunk)
        throws IOException {
        assert Transports.assertNotTransportThread("multi_file_writer");
        final FileChunkWriter writer = fileChunkWriters.computeIfAbsent(fileMetadata.name(), name -> new FileChunkWriter());
        // 每次填充一个片段
        writer.writeChunk(new FileChunk(fileMetadata, content, position, lastChunk));
    }

    /** Get a temporary name for the provided file name. */
    String getTempNameForFile(String origFile) {
        return tempFilePrefix + origFile;
    }

    public IndexOutput getOpenIndexOutput(String key) {
        ensureOpen.run();
        return openIndexOutputs.get(key);
    }

    /** remove and {@link IndexOutput} for a given file. It is the caller's responsibility to close it */
    public IndexOutput removeOpenIndexOutputs(String name) {
        ensureOpen.run();
        return openIndexOutputs.remove(name);
    }

    /**
     * Creates an {@link IndexOutput} for the given file name. Note that the
     * IndexOutput actually point at a temporary file.
     * <p>
     * Note: You can use {@link #getOpenIndexOutput(String)} with the same filename to retrieve the same IndexOutput
     * at a later stage
     * 因为某个fileChunk对应的position还是0 所以此时才开始打开输出流
     */
    public IndexOutput openAndPutIndexOutput(String fileName, StoreFileMetadata metadata, Store store) throws IOException {
        // 首先确保文件还处于打开状态
        ensureOpen.run();
        // 数据会先写入到临时文件中
        String tempFileName = getTempNameForFile(fileName);
        // 避免在同一个操作流程中对文件操作多次
        if (tempFileNames.containsKey(tempFileName)) {
            throw new IllegalStateException("output for file [" + fileName + "] has already been created");
        }
        // add first, before it's created
        tempFileNames.put(tempFileName, fileName);
        // 创建文件输出流
        IndexOutput indexOutput = store.createVerifyingOutput(tempFileName, metadata, IOContext.DEFAULT);
        // 这里是源文件 对应 临时文件的输出流
        openIndexOutputs.put(fileName, indexOutput);
        return indexOutput;
    }

    /**
     * 将某个file内部的数据写入到某个地方
     * @param fileMetadata
     * @param position
     * @param content
     * @param lastChunk
     * @throws IOException
     */
    private void innerWriteFileChunk(StoreFileMetadata fileMetadata, long position,
                                     BytesReference content, boolean lastChunk) throws IOException {
        final String name = fileMetadata.name();
        IndexOutput indexOutput;
        // 代表还未打开该文件对应的输出流
        if (position == 0) {
            indexOutput = openAndPutIndexOutput(name, fileMetadata, store);
        } else {
            // 找到一个已经打开的输出流
            indexOutput = getOpenIndexOutput(name);
        }
        assert indexOutput.getFilePointer() == position : "file-pointer " + indexOutput.getFilePointer() + " != " + position;
        BytesRefIterator iterator = content.iterator();
        BytesRef scratch;
        // 每个BytesRef 就是一个byte[] BytesRefIterator 是一组连续的byte[]
        while((scratch = iterator.next()) != null) { // we iterate over all pages - this is a 0-copy for all core impls
            indexOutput.writeBytes(scratch.bytes, scratch.offset, scratch.length);
        }
        // 当源文件的数据都转移到临时文件后 更新state内部的数据
        indexState.addRecoveredBytesToFile(name, content.length());
        // 代表此时已经写完了所有数据 或者明确声明此时传入的数据块是最后一个 那么生成校验和
        if (indexOutput.getFilePointer() >= fileMetadata.length() || lastChunk) {
            try {
                Store.verify(indexOutput);
            } finally {
                // we are done
                indexOutput.close();
            }
            final String temporaryFileName = getTempNameForFile(name);
            assert Arrays.asList(store.directory().listAll()).contains(temporaryFileName) :
                "expected: [" + temporaryFileName + "] in " + Arrays.toString(store.directory().listAll());
            // 将临时文件中的数据刷盘
            store.directory().sync(Collections.singleton(temporaryFileName));
            // 写入已经完成了从容器中移除
            IndexOutput remove = removeOpenIndexOutputs(name);
            assert remove == null || remove == indexOutput; // remove maybe null if we got finished
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            decRef();
        }
    }

    /**
     * 当引用计数归0时会触发该方法
     */
    @Override
    protected void closeInternal() {
        fileChunkWriters.clear();
        // clean open index outputs
        // 只要有数据存在于 openIndexOutputs容器中  就代表数据写入未完成 这些数据将不会被保存
        Iterator<Map.Entry<String, IndexOutput>> iterator = openIndexOutputs.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, IndexOutput> entry = iterator.next();
            logger.trace("closing IndexOutput file [{}]", entry.getValue());
            try {
                entry.getValue().close();
            } catch (Exception e) {
                logger.debug(() -> new ParameterizedMessage("error while closing recovery output [{}]", entry.getValue()), e);
            }
            iterator.remove();
        }
        // 这些代表已经持久化了 但是只要没有从容器中移除 应该就代表整个流程未完成 那么数据也需要删除
        if (Strings.hasText(tempFilePrefix)) {
            // trash temporary files
            for (String file : tempFileNames.keySet()) {
                logger.trace("cleaning temporary file [{}]", file);
                store.deleteQuiet(file);
            }
        }
    }

    /**
     * renames all temporary files to their true name, potentially overriding existing files
     * 将所有临时文件修改成 source文件  应该是要在确保所有数据都传输完毕后再调用该方法
     * */
    public void renameAllTempFiles() throws IOException {
        ensureOpen.run();
        store.renameTempFilesSafe(tempFileNames);
    }

    /**
     * 一个文件由多个片段组成 这里是单个片段
     */
    static final class FileChunk {

        /**
         * 存储了有关某个索引文件的元数据信息
         */
        final StoreFileMetadata md;
        /**
         * 文件的内容体是一个 byte[]
         */
        final BytesReference content;
        /**
         * 当前文件指针的位置
         */
        final long position;
        final boolean lastChunk;
        FileChunk(StoreFileMetadata md, BytesReference content, long position, boolean lastChunk) {
            this.md = md;
            this.content = content;
            this.position = position;
            this.lastChunk = lastChunk;
        }
    }

    /**
     * 以chunk为单位将数据写入到文件中
     * 每个 fieldChunkWriter 应该只对应一个索引文件
     */
    private final class FileChunkWriter {
        // chunks can be delivered out of order, we need to buffer chunks if there's a gap between them.
        // 每个文件块 按照position进行排序
        final PriorityQueue<FileChunk> pendingChunks = new PriorityQueue<>(Comparator.comparing(fc -> fc.position));
        long lastPosition = 0;

        /**
         * 将某个fileChunk写入
         * @param newChunk
         * @throws IOException
         */
        void writeChunk(FileChunk newChunk) throws IOException {
            synchronized (this) {
                pendingChunks.add(newChunk);
            }
            while (true) {
                final FileChunk chunk;
                synchronized (this) {
                    chunk = pendingChunks.peek();
                    // 这里其实就是一个排序的逻辑 因为primary可能一次性会发送多个文件片段 接收端可能会出现乱序的情况 在这里通过 lastPosition+优先队列 实现顺序性
                    if (chunk == null || chunk.position != lastPosition) {
                        return;
                    }
                    pendingChunks.remove();
                }
                // chunk.lastChunk == true 代表是对应的源文件的最后一个数据块    也就是数据是断断续续传来的
                innerWriteFileChunk(chunk.md, chunk.position, chunk.content, chunk.lastChunk);
                synchronized (this) {
                    assert lastPosition == chunk.position : "last_position " + lastPosition + " != chunk_position " + chunk.position;
                    // 代表此时写入的文件偏移量已经增加了
                    lastPosition += chunk.content.length();
                    if (chunk.lastChunk) {
                        assert pendingChunks.isEmpty() : "still have pending chunks [" + pendingChunks + "]";
                        // 这个容器管理了 MultiFileWriter下所有的 fieldChunkWriter
                        fileChunkWriters.remove(chunk.md.name());
                        assert fileChunkWriters.containsValue(this) == false : "chunk writer [" + newChunk.md + "] was not removed";
                    }
                }
            }
        }
    }
}
