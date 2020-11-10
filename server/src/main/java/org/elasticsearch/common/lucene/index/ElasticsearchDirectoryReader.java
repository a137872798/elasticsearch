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
package org.elasticsearch.common.lucene.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * A {@link org.apache.lucene.index.FilterDirectoryReader} that exposes
 * Elasticsearch internal per shard / index information like the shard ID.
 * ES 封装了原生的 IndexReader
 */
public final class ElasticsearchDirectoryReader extends FilterDirectoryReader {

    /**
     * 代表这份数据是属于哪个分片的
     */
    private final ShardId shardId;
    private final FilterDirectoryReader.SubReaderWrapper wrapper;

    /**
     *
     * @param in   原始的reader对象
     * @param wrapper    定义了如何包装子级reader
     * @param shardId
     * @throws IOException
     */
    private ElasticsearchDirectoryReader(DirectoryReader in, FilterDirectoryReader.SubReaderWrapper wrapper,
            ShardId shardId) throws IOException {
        super(in, wrapper);
        this.wrapper = wrapper;
        this.shardId = shardId;
    }

    /**
     * Returns the shard id this index belongs to.
     */
    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // safe to delegate since this reader does not alter the index
        return in.getReaderCacheHelper();
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new ElasticsearchDirectoryReader(in, wrapper, shardId);
    }

    /**
     * Wraps the given reader in a {@link org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader} as
     * well as all it's sub-readers in {@link org.elasticsearch.common.lucene.index.ElasticsearchLeafReader} to
     * expose the given shard Id.
     *
     * @param reader the reader to wrap
     * @param shardId the shard ID to expose via the elasticsearch internal reader wrappers.
     *                将一个普通的对象包装成 ESReader对象
     */
    public static ElasticsearchDirectoryReader wrap(DirectoryReader reader, ShardId shardId) throws IOException {
        return new ElasticsearchDirectoryReader(reader, new SubReaderWrapper(shardId), shardId);
    }

    /**
     * 定义了对子级reader进行包装   也就是针对每个 segmentReader
     */
    private static final class SubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
        private final ShardId shardId;
        SubReaderWrapper(ShardId shardId) {
            this.shardId = shardId;
        }
        @Override
        public LeafReader wrap(LeafReader reader) {
            return new ElasticsearchLeafReader(reader, shardId);
        }
    }

    /**
     * Adds the given listener to the provided directory reader. The reader
     * must contain an {@link ElasticsearchDirectoryReader} in it's hierarchy
     * otherwise we can't safely install the listener.
     *
     * @throws IllegalArgumentException if the reader doesn't contain an
     *     {@link ElasticsearchDirectoryReader} in it's hierarchy
     *     为 DirectoryReader 追加一个关闭监听器
     */
    @SuppressForbidden(reason = "This is the only sane way to add a ReaderClosedListener")
    public static void addReaderCloseListener(DirectoryReader reader, IndexReader.ClosedListener listener) {
        // 进行解包装
        ElasticsearchDirectoryReader elasticsearchDirectoryReader = getElasticsearchDirectoryReader(reader);
        if (elasticsearchDirectoryReader == null) {
            throw new IllegalArgumentException(
                    "Can't install close listener reader is not an ElasticsearchDirectoryReader/ElasticsearchLeafReader");
        }
        // 将监听器设置到 cacheHelper上
        IndexReader.CacheHelper cacheHelper = elasticsearchDirectoryReader.getReaderCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + elasticsearchDirectoryReader + " does not support caching");
        }
        assert cacheHelper.getKey() == reader.getReaderCacheHelper().getKey();
        cacheHelper.addClosedListener(listener);
    }

    /**
     * Tries to unwrap the given reader until the first
     * {@link ElasticsearchDirectoryReader} instance is found or {@code null}
     * if no instance is found.
     */
    public static ElasticsearchDirectoryReader getElasticsearchDirectoryReader(DirectoryReader reader) {
        if (reader instanceof FilterDirectoryReader) {
            if (reader instanceof ElasticsearchDirectoryReader) {
                return (ElasticsearchDirectoryReader) reader;
            } else {
                // We need to use FilterDirectoryReader#getDelegate and not FilterDirectoryReader#unwrap, because
                // If there are multiple levels of filtered leaf readers then with the unwrap() method it immediately
                // returns the most inner leaf reader and thus skipping of over any other filtered leaf reader that
                // may be instance of ElasticsearchLeafReader. This can cause us to miss the shardId.
                return getElasticsearchDirectoryReader(((FilterDirectoryReader) reader).getDelegate());
            }
        }
        return null;
    }
}
