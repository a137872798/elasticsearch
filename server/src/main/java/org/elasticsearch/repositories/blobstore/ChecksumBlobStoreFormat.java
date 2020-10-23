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
package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.CorruptStateException;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Snapshot metadata file format used in v2.0 and above
 */
public final class ChecksumBlobStoreFormat<T extends ToXContent> {

    // Serialization parameters to specify correct context for metadata serialization
    private static final ToXContent.Params SNAPSHOT_ONLY_FORMAT_PARAMS;

    static {
        Map<String, String> snapshotOnlyParams = new HashMap<>();
        // when metadata is serialized certain elements of the metadata shouldn't be included into snapshot
        // exclusion of these elements is done by setting Metadata.CONTEXT_MODE_PARAM to Metadata.CONTEXT_MODE_SNAPSHOT
        snapshotOnlyParams.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_SNAPSHOT);
        // serialize SnapshotInfo using the SNAPSHOT mode
        snapshotOnlyParams.put(SnapshotInfo.CONTEXT_MODE_PARAM, SnapshotInfo.CONTEXT_MODE_SNAPSHOT);
        SNAPSHOT_ONLY_FORMAT_PARAMS = new ToXContent.MapParams(snapshotOnlyParams);
    }

    // The format version
    public static final int VERSION = 1;

    private static final int BUFFER_SIZE = 4096;

    private final boolean compress;

    private final String codec;

    private final String blobNameFormat;

    private final CheckedFunction<XContentParser, T, IOException> reader;

    private final NamedXContentRegistry namedXContentRegistry;

    /**
     * @param codec          codec name
     * @param blobNameFormat format of the blobname in {@link String#format} format
     * @param reader         prototype object that can deserialize T from XContent
     * @param compress       true if the content should be compressed
     */
    public ChecksumBlobStoreFormat(String codec, String blobNameFormat, CheckedFunction<XContentParser, T, IOException> reader,
                                   NamedXContentRegistry namedXContentRegistry, boolean compress) {
        this.reader = reader;
        this.blobNameFormat = blobNameFormat;
        this.namedXContentRegistry = namedXContentRegistry;
        this.compress = compress;
        this.codec = codec;
    }

    /**
     * Reads and parses the blob with given name, applying name translation using the {link #blobName} method
     *
     * @param blobContainer blob container
     * @param name          name to be translated into
     * @return parsed blob object
     * 指定容器以及快照id  将数据流读取出来后并格式化
     */
    public T read(BlobContainer blobContainer, String name) throws IOException {
        String blobName = blobName(name);
        return readBlob(blobContainer, blobName);
    }

    /**
     * 传入的名字需要替换模板的占位符部分 才能作为有效的blobName
     * @param name
     * @return
     */
    public String blobName(String name) {
        return String.format(Locale.ROOT, blobNameFormat, name);
    }

    /**
     * Reads blob with specified name without resolving the blobName using using {@link #blobName} method.
     *
     * @param blobContainer blob container
     * @param blobName blob name
     *                 根据名字去 container中读取数据流 并格式化成目标对象
     */
    public T readBlob(BlobContainer blobContainer, String blobName) throws IOException {
        // readFully 会使用一个es自己封装的 outputStream 并将数据全部读出后 将bytes取出
        final BytesReference bytes = Streams.readFully(blobContainer.readBlob(blobName));
        final String resourceDesc = "ChecksumBlobStoreFormat.readBlob(blob=\"" + blobName + "\")";
        try {
            // 写入的时候采用了各种压缩方式  而 ByteBuffersIndexInput 内部已经封装了各种读取 Vint Zint 的逻辑
            final IndexInput indexInput = bytes.length() > 0 ? new ByteBuffersIndexInput(
                // 该对象将一组 ByteBuffer 包装成一个输入流
                    new ByteBuffersDataInput(Arrays.asList(BytesReference.toByteBuffers(bytes))), resourceDesc)
                // 以数组为单位创建输入流
                    : new ByteArrayIndexInput(resourceDesc, BytesRef.EMPTY_BYTES);

            // 校验和相关的先忽略 也就是将ES 的输出流转化成lucene的输入流的目的就是为了直接使用 lucene封装的api 比如这个校验功能
            // 同时也代表着blob文件本身满足可以校验的格式
            CodecUtil.checksumEntireFile(indexInput);
            CodecUtil.checkHeader(indexInput, codec, VERSION, VERSION);

            // 在校验header的时候 跳过了校验头的位置
            long filePointer = indexInput.getFilePointer();
            // 总长 - 校验头 - 校验尾  得到内容长
            long contentSize = indexInput.length() - CodecUtil.footerLength() - filePointer;

            // 解析器就不细看了 反正就是适配了第三方的格式化解析器 比如JSON/YML 啥的
            try (XContentParser parser = XContentHelper.createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE,
                bytes.slice((int) filePointer, (int) contentSize), XContentType.SMILE)) {
                return reader.apply(parser);
            }
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // we trick this into a dedicated exception with the original stacktrace
            throw new CorruptStateException(ex);
        }
    }

    /**
     * Writes blob in atomic manner with resolving the blob name using {@link #blobName} method.
     * <p>
     * The blob will be compressed and checksum will be written if required.
     *
     * Atomic move might be very inefficient on some repositories. It also cannot override existing files.
     *
     * @param obj           object to be serialized
     * @param blobContainer blob container
     * @param name          blob name
     */
    public void writeAtomic(T obj, BlobContainer blobContainer, String name) throws IOException {
        final String blobName = blobName(name);
        writeTo(obj, blobName, bytesArray -> {
            try (InputStream stream = bytesArray.streamInput()) {
                blobContainer.writeBlobAtomic(blobName, stream, bytesArray.length(), true);
            }
        });
    }

    /**
     * Writes blob with resolving the blob name using {@link #blobName} method.
     * <p>
     * The blob will be compressed and checksum will be written if required.
     *
     * @param obj                 object to be serialized
     * @param blobContainer       blob container
     * @param name                blob name
     * @param failIfAlreadyExists Whether to fail if the blob already exists
     */
    public void write(T obj, BlobContainer blobContainer, String name, boolean failIfAlreadyExists) throws IOException {
        final String blobName = blobName(name);
        writeTo(obj, blobName, bytesArray -> {
            try (InputStream stream = bytesArray.streamInput()) {
                blobContainer.writeBlob(blobName, stream, bytesArray.length(), failIfAlreadyExists);
            }
        });
    }

    private void writeTo(final T obj, final String blobName,
                         final CheckedConsumer<BytesReference, IOException> consumer) throws IOException {
        try (BytesStreamOutput outputStream = new BytesStreamOutput()) {
            final String resourceDesc = "ChecksumBlobStoreFormat.writeBlob(blob=\"" + blobName + "\")";
            try (OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(resourceDesc, blobName, outputStream, BUFFER_SIZE)) {
                CodecUtil.writeHeader(indexOutput, codec, VERSION);
                try (OutputStream indexOutputOutputStream = new IndexOutputOutputStream(indexOutput) {
                    @Override
                    public void close() {
                        // this is important since some of the XContentBuilders write bytes on close.
                        // in order to write the footer we need to prevent closing the actual index input.
                    }
                }) {
                    if (compress) {
                        try (StreamOutput compressedStreamOutput = CompressorFactory.COMPRESSOR.streamOutput(indexOutputOutputStream)) {
                            write(obj, compressedStreamOutput);
                        }
                    } else {
                        write(obj, indexOutputOutputStream);
                    }
                }
                CodecUtil.writeFooter(indexOutput);
            }
            consumer.accept(outputStream.bytes());
        }
    }

    private void write(T obj, OutputStream streamOutput) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE, streamOutput)) {
            builder.startObject();
            obj.toXContent(builder, SNAPSHOT_ONLY_FORMAT_PARAMS);
            builder.endObject();
        }
    }
}
