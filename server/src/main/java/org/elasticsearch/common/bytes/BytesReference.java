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

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;


/**
 * A reference to bytes.
 * bytes的包装类  有点像lucene的 BytesRef
 */
public interface BytesReference extends Comparable<BytesReference>, ToXContentFragment {

    /**
     * Convert an {@link XContentBuilder} into a BytesReference. This method closes the builder,
     * so no further fields may be added.
     */
    static BytesReference bytes(XContentBuilder xContentBuilder) {
        xContentBuilder.close();
        OutputStream stream = xContentBuilder.getOutputStream();
        if (stream instanceof ByteArrayOutputStream) {
            return new BytesArray(((ByteArrayOutputStream) stream).toByteArray());
        } else {
            return ((BytesStream) stream).bytes();
        }
    }

    /**
     * Returns a compact array from the given BytesReference. The returned array won't be copied unless necessary. If you need
     * to modify the returned array use {@code BytesRef.deepCopyOf(reference.toBytesRef()} instead
     */
    static byte[] toBytes(BytesReference reference) {
        final BytesRef bytesRef = reference.toBytesRef();
        if (bytesRef.offset == 0 && bytesRef.length == bytesRef.bytes.length) {
            return bytesRef.bytes;
        }
        return ArrayUtil.copyOfSubArray(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);
    }

    /**
     * Returns an array of byte buffers from the given BytesReference.
     * 将 ref内部的数据读取出来 并转换成 ByteBuffer对象
     */
    static ByteBuffer[] toByteBuffers(BytesReference reference) {
        BytesRefIterator byteRefIterator = reference.iterator();
        BytesRef r;
        try {
            ArrayList<ByteBuffer> buffers = new ArrayList<>();
            while ((r = byteRefIterator.next()) != null) {
                buffers.add(ByteBuffer.wrap(r.bytes, r.offset, r.length));
            }
            return buffers.toArray(new ByteBuffer[buffers.size()]);

        } catch (IOException e) {
            // this is really an error since we don't do IO in our bytesreferences
            throw new AssertionError("won't happen", e);
        }
    }

    /**
     * Returns BytesReference composed of the provided ByteBuffers.
     */
    static BytesReference fromByteBuffers(ByteBuffer[] buffers) {
        int bufferCount = buffers.length;
        if (bufferCount == 0) {
            return BytesArray.EMPTY;
        } else if (bufferCount == 1) {
            return fromByteBuffer(buffers[0]);
        } else {
            ByteBufferReference[] references = new ByteBufferReference[bufferCount];
            for (int i = 0; i < bufferCount; ++i) {
                references[i] = new ByteBufferReference(buffers[i]);
            }

            return new CompositeBytesReference(references);
        }
    }

    /**
     * Returns BytesReference composed of the provided ByteBuffer.
     * 将byteBuffer对象 包装成BytesReference
     */
    static BytesReference fromByteBuffer(ByteBuffer buffer) {
        return new ByteBufferReference(buffer);
    }

    /**
     * Returns the byte at the specified index. Need to be between 0 and length.
     * 返回下标对应的byte
     */
    byte get(int index);

    /**
     * Returns the integer read from the 4 bytes (BE) starting at the given index.
     * 将对应下标的byte 以int形式返回
     */
    int getInt(int index);

    /**
     * Finds the index of the first occurrence of the given marker between within the given bounds.
     * @param marker marker byte to search
     * @param from lower bound for the index to check (inclusive)
     * @return first index of the marker or {@code -1} if not found
     * 从目标位置开始找到marker第一次出现的位置
     */
    int indexOf(byte marker, int from);

    /**
     * The length.
     * 数据体长度
     */
    int length();

    /**
     * Slice the bytes from the {@code from} index up to {@code length}.
     * 创建分片对象
     */
    BytesReference slice(int from, int length);

    /**
     * The amount of memory used by this BytesReference
     */
    long ramBytesUsed();

    /**
     * A stream input of the bytes.
     * 将当前数据流 以 input形式返回
     */
    StreamInput streamInput() throws IOException;

    /**
     * Writes the bytes directly to the output stream.
     * 通过 outputStream内部的数据来初始化 bytesReference
     */
    void writeTo(OutputStream os) throws IOException;

    /**
     * Interprets the referenced bytes as UTF8 bytes, returning the resulting string
     */
    String utf8ToString();

    /**
     * Converts to Lucene BytesRef.
     * 转换成lucene的 bytesRef
     */
    BytesRef toBytesRef();

    /**
     * Returns a BytesRefIterator for this BytesReference. This method allows
     * access to the internal pages of this reference without copying them. Use with care!
     * @see BytesRefIterator
     * 获取能够遍历 BytesRef 的对象
     */
    BytesRefIterator iterator();
}
