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

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

/** Utility class to resolve the Lucene doc ID, version, seqNo and primaryTerms for a given uid. */
public final class VersionsAndSeqNoResolver {

    static final ConcurrentMap<IndexReader.CacheKey, CloseableThreadLocal<PerThreadIDVersionAndSeqNoLookup[]>> lookupStates =
        ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    // Evict this reader from lookupStates once it's closed:
    // 当不在使用某个缓存了 将对应的 ThreadLocal内部存储的数据也移除
    private static final IndexReader.ClosedListener removeLookupState = key -> {
        CloseableThreadLocal<PerThreadIDVersionAndSeqNoLookup[]> ctl = lookupStates.remove(key);
        if (ctl != null) {
            ctl.close();
        }
    };

    /**
     *
     * @param reader  通过该对象查询数据
     * @param uidField 对应 “_id"
     * @return
     * @throws IOException
     */
    private static PerThreadIDVersionAndSeqNoLookup[] getLookupState(IndexReader reader, String uidField) throws IOException {
        // We cache on the top level
        // This means cache entries have a shorter lifetime, maybe as low as 1s with the
        // default refresh interval and a steady indexing rate, but on the other hand it
        // proved to be cheaper than having to perform a CHM and a TL get for every segment.
        // See https://github.com/elastic/elasticsearch/pull/19856.
        IndexReader.CacheHelper cacheHelper = reader.getReaderCacheHelper();
        // 每个线程会绑定一组PerThreadIDVersionAndSeqNoLookup
        CloseableThreadLocal<PerThreadIDVersionAndSeqNoLookup[]> ctl = lookupStates.get(cacheHelper.getKey());
        if (ctl == null) {
            // First time we are seeing this reader's core; make a new CTL:
            ctl = new CloseableThreadLocal<>();
            CloseableThreadLocal<PerThreadIDVersionAndSeqNoLookup[]> other = lookupStates.putIfAbsent(cacheHelper.getKey(), ctl);
            if (other == null) {
                // Our CTL won, we must remove it when the reader is closed:
                cacheHelper.addClosedListener(removeLookupState);
            } else {
                // Another thread beat us to it: just use their CTL:
                ctl = other;
            }
        }

        // 获取当前线程关联的 lookup对象 一般 ThreadLocal应该是配合长生命周期的线程使用的  比如线程池
        PerThreadIDVersionAndSeqNoLookup[] lookupState = ctl.get();
        if (lookupState == null) {
            // 原来是每个 segmentReader 对应一个 lookup对象  不过缓存是什么时候失效的呢
            lookupState = new PerThreadIDVersionAndSeqNoLookup[reader.leaves().size()];
            for (LeafReaderContext leaf : reader.leaves()) {
                lookupState[leaf.ord] = new PerThreadIDVersionAndSeqNoLookup(leaf.reader(), uidField);
            }
            ctl.set(lookupState);
        }

        if (lookupState.length != reader.leaves().size()) {
            throw new AssertionError("Mismatched numbers of leaves: " + lookupState.length + " != " + reader.leaves().size());
        }

        // 当field不匹配时抛出异常
        if (lookupState.length > 0 && Objects.equals(lookupState[0].uidField, uidField) == false) {
            throw new AssertionError("Index does not consistently use the same uid field: ["
                    + uidField + "] != [" + lookupState[0].uidField + "]");
        }

        return lookupState;
    }

    private VersionsAndSeqNoResolver() {
    }

    /**
     * Wraps an {@link LeafReaderContext}, a doc ID <b>relative to the context doc base</b> and a version.
     * 用于描述某个doc的id 以及在ES中的 序列号等等   每次往ES中执行一个操作都会有一个序列号
     */
    public static class DocIdAndVersion {
        public final int docId;
        public final long version;
        public final long seqNo;
        public final long primaryTerm;
        public final LeafReader reader;
        public final int docBase;

        public DocIdAndVersion(int docId, long version, long seqNo, long primaryTerm, LeafReader reader, int docBase) {
            this.docId = docId;
            this.version = version;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.reader = reader;
            this.docBase = docBase;
        }
    }

    /** Wraps an {@link LeafReaderContext}, a doc ID <b>relative to the context doc base</b> and a seqNo. */
    public static class DocIdAndSeqNo {
        public final int docId;
        public final long seqNo;
        public final LeafReaderContext context;

        DocIdAndSeqNo(int docId, long seqNo, LeafReaderContext context) {
            this.docId = docId;
            this.seqNo = seqNo;
            this.context = context;
        }
    }


    /**
     * Load the internal doc ID and version for the uid from the reader, returning<ul>
     * <li>null if the uid wasn't found,
     * <li>a doc ID and a version otherwise
     * </ul>
     * @param loadSeqNo 是否要将seq term 等信息一起查出来
     */
    public static DocIdAndVersion loadDocIdAndVersion(IndexReader reader, Term term, boolean loadSeqNo) throws IOException {
        // 先从本地线程缓存 查询数据
        PerThreadIDVersionAndSeqNoLookup[] lookups = getLookupState(reader, term.field());
        List<LeafReaderContext> leaves = reader.leaves();
        // iterate backwards to optimize for the frequently updated documents
        // which are likely to be in the last segments
        // 从后往前 尽可能查找最新的数据
        for (int i = leaves.size() - 1; i >= 0; i--) {
            final LeafReaderContext leaf = leaves.get(i);
            // 找到对应的lookup对象
            PerThreadIDVersionAndSeqNoLookup lookup = lookups[leaf.ord];
            DocIdAndVersion result = lookup.lookupVersion(term.bytes(), loadSeqNo, leaf);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * Loads the internal docId and sequence number of the latest copy for a given uid from the provided reader.
     * The result is either null or the live and latest version of the given uid.
     * 以term为查询条件 在reader中找到符合条件的所有doc
     */
    public static DocIdAndSeqNo loadDocIdAndSeqNo(IndexReader reader, Term term) throws IOException {
        // 获取绑定在线程上的缓存
        final PerThreadIDVersionAndSeqNoLookup[] lookups = getLookupState(reader, term.field());
        final List<LeafReaderContext> leaves = reader.leaves();
        // iterate backwards to optimize for the frequently updated documents
        // which are likely to be in the last segments
        for (int i = leaves.size() - 1; i >= 0; i--) {
            final LeafReaderContext leaf = leaves.get(i);
            final PerThreadIDVersionAndSeqNoLookup lookup = lookups[leaf.ord];
            // 从后往前查找 也就是当term出现多次时 会以最新的doc为准
            final DocIdAndSeqNo result = lookup.lookupSeqNo(term.bytes(), leaf);
            if (result != null) {
                return result;
            }
        }
        return null;
    }
}
