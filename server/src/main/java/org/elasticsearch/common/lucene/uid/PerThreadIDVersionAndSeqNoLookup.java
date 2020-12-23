package org.elasticsearch.common.lucene.uid;

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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;

import java.io.IOException;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;


/** Utility class to do efficient primary-key (only 1 doc contains the
 *  given term) lookups by segment, re-using the enums.  This class is
 *  not thread safe, so it is the caller's job to create and use one
 *  instance of this per thread.  Do not use this if a term may appear
 *  in more than one document!  It will only return the first one it
 *  finds.
 *  This class uses live docs, so it should be cached based on the
 *  {@link org.apache.lucene.index.IndexReader#getReaderCacheHelper() reader cache helper}
 *  rather than the {@link LeafReader#getCoreCacheHelper() core cache helper}.
 *  本对象是通过 ThreadLocal实现线程隔离的  所以不需要做并发处理 同时像是一种查询结果的缓存
 */
final class PerThreadIDVersionAndSeqNoLookup {
    // TODO: do we really need to store all this stuff? some if it might not speed up anything.
    // we keep it around for now, to reduce the amount of e.g. hash lookups by field and stuff

    /** terms enum for uid field */
    final String uidField;

    /**
     * 通过 id 定位到某个doc后
     */
    private final TermsEnum termsEnum;

    /** Reused for iteration (when the term exists) */
    private PostingsEnum docsEnum;

    /** used for assertions to make sure class usage meets assumptions */
    private final Object readerKey;

    /**
     * Initialize lookup for the provided segment
     * @param uidField 代表 name为 _id 的field   本对象本身是作为缓存使用的 实际上还可以使用其他field
     */
    PerThreadIDVersionAndSeqNoLookup(LeafReader reader, String uidField) throws IOException {
        this.uidField = uidField;
        // 获取每个包含_id的doc 的docValue被分词后的结果 也就是terms
        final Terms terms = reader.terms(uidField);
        if (terms == null) {
            // If a segment contains only no-ops, it does not have _uid but has both _soft_deletes and _tombstone fields.
            // 会专门存 _soft_delete/_tombstone 吗 是ES自己加的吧
            final NumericDocValues softDeletesDV = reader.getNumericDocValues(Lucene.SOFT_DELETES_FIELD);
            final NumericDocValues tombstoneDV = reader.getNumericDocValues(SeqNoFieldMapper.TOMBSTONE_NAME);
            // this is a special case when we pruned away all IDs in a segment since all docs are deleted.
            final boolean allDocsDeleted = (softDeletesDV != null && reader.numDocs() == 0);
            if ((softDeletesDV == null || tombstoneDV == null) && allDocsDeleted == false) {
                throw new IllegalArgumentException("reader does not have _uid terms but not a no-op segment; " +
                    "_soft_deletes [" + softDeletesDV + "], _tombstone [" + tombstoneDV + "]");
            }
            termsEnum = null;
        } else {
            termsEnum = terms.iterator();
        }
        if (reader.getNumericDocValues(VersionFieldMapper.NAME) == null) {
            throw new IllegalArgumentException("reader misses the [" + VersionFieldMapper.NAME + "] field; _uid terms [" + terms + "]");
        }
        Object readerKey = null;
        assert (readerKey = reader.getCoreCacheHelper().getKey()) != null;
        this.readerKey = readerKey;
    }

    /** Return null if id is not found.
     * We pass the {@link LeafReaderContext} as an argument so that things
     * still work with reader wrappers that hide some documents while still
     * using the same cache key. Otherwise we'd have to disable caching
     * entirely for these readers.
     */
    public DocIdAndVersion lookupVersion(BytesRef id, boolean loadSeqNo, LeafReaderContext context)
        throws IOException {
        assert context.reader().getCoreCacheHelper().getKey().equals(readerKey) :
            "context's reader is not the same as the reader class was initialized on.";
        // 当这个term能够匹配到多个doc时 只返回最后一个
        int docID = getDocID(id, context);

        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            final long seqNo;
            final long term;
            // 是否连同 seqNo term 一起加载出来
            if (loadSeqNo) {
                seqNo = readNumericDocValues(context.reader(), SeqNoFieldMapper.NAME, docID);
                term = readNumericDocValues(context.reader(), SeqNoFieldMapper.PRIMARY_TERM_NAME, docID);
            } else {
                seqNo = UNASSIGNED_SEQ_NO;
                term = UNASSIGNED_PRIMARY_TERM;
            }
            final long version = readNumericDocValues(context.reader(), VersionFieldMapper.NAME, docID);
            return new DocIdAndVersion(docID, version, seqNo, term, context.reader(), context.docBase);
        } else {
            return null;
        }
    }

    /**
     * returns the internal lucene doc id for the given id bytes.
     * {@link DocIdSetIterator#NO_MORE_DOCS} is returned if not found
     * 找到与id匹配的doc 在semgent下的id
     * */
    private int getDocID(BytesRef id, LeafReaderContext context) throws IOException {
        // termsEnum can possibly be null here if this leaf contains only no-ops.
        if (termsEnum != null && termsEnum.seekExact(id)) {
            final Bits liveDocs = context.reader().getLiveDocs();
            int docID = DocIdSetIterator.NO_MORE_DOCS;
            // there may be more than one matching docID, in the case of nested docs, so we want the last one:
            docsEnum = termsEnum.postings(docsEnum, 0);
            // 当匹配到多个doc时  只返回最后一个
            for (int d = docsEnum.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = docsEnum.nextDoc()) {
                if (liveDocs != null && liveDocs.get(d) == false) {
                    continue;
                }
                docID = d;
            }
            return docID;
        } else {
            return DocIdSetIterator.NO_MORE_DOCS;
        }
    }

    private static long readNumericDocValues(LeafReader reader, String field, int docId) throws IOException {
        final NumericDocValues dv = reader.getNumericDocValues(field);

        // 定位到某个具体的doc
        if (dv == null || dv.advanceExact(docId) == false) {
            assert false : "document [" + docId + "] does not have docValues for [" + field + "]";
            throw new IllegalStateException("document [" + docId + "] does not have docValues for [" + field + "]");
        }
        // 获取docValue
        return dv.longValue();
    }

    /**
     * Return null if id is not found.
     * 通过term 查询数据 并将命中的结果包装成 DocIdAndSeqNo
     */
    DocIdAndSeqNo lookupSeqNo(BytesRef id, LeafReaderContext context) throws IOException {
        assert context.reader().getCoreCacheHelper().getKey().equals(readerKey) :
            "context's reader is not the same as the reader class was initialized on.";
        final int docID = getDocID(id, context);
        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            // 找到同一doc下 seq的值
            final long seqNo = readNumericDocValues(context.reader(), SeqNoFieldMapper.NAME, docID);
            return new DocIdAndSeqNo(docID, seqNo, context);
        } else {
            return null;
        }
    }
}
