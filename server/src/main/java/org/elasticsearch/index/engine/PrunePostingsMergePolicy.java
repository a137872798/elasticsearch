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

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Iterator;

/**
 * This merge policy drops id field postings for all delete documents this can be
 * useful to guarantee consistent update performance even if a large number of deleted / updated documents
 * are retained. Merging postings away is efficient since lucene visits postings term by term and
 * with the original live-docs being available we are adding a negotiable overhead such that we can
 * prune soft-deletes by default. Yet, using this merge policy will cause loosing all search capabilities on top of
 * soft deleted documents independent of the retention policy. Note, in order for this merge policy to be effective it needs to be added
 * before the {@link org.apache.lucene.index.SoftDeletesRetentionMergePolicy} because otherwise only documents that are deleted / removed
 * anyways will be pruned.
 * 修剪mergePolicy???
 */
final class PrunePostingsMergePolicy extends OneMergeWrappingMergePolicy {

    /**
     *
     * @param in
     * @param idField  "_id"
     */
    PrunePostingsMergePolicy(MergePolicy in, String idField) {
        super(in,
            // 这是一个包装函数  OneMergeWrappingMergePolicy 对象在采集到n个OneMerge对象后 会通过该函数进行包装
            toWrap -> new OneMerge(toWrap.segments) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                // 这里对每个segment的reader对象进行包装 先走原方法
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                return wrapReader(wrapped, idField);
            }
        });
    }

    /**
     *
     * @param reader
     * @param idField  doc中 fieldName 为 "_id" 的field
     * @return
     */
    private static CodecReader wrapReader(CodecReader reader, String idField) {
        Bits liveDocs = reader.getLiveDocs();
        // 未设置位图代表所有doc都存活
        if (liveDocs == null) {
            return reader; // no deleted docs - we are good!
        }
        final boolean fullyDeletedSegment = reader.numDocs() == 0;
        // 这里会过滤掉一些doc
        return new FilterCodecReader(reader) {

            @Override
            public FieldsProducer getPostingsReader() {
                FieldsProducer postingsReader = super.getPostingsReader();
                if (postingsReader == null) {
                    return null;
                }
                return new FieldsProducer() {
                    @Override
                    public void close() throws IOException {
                        postingsReader.close();
                    }

                    @Override
                    public void checkIntegrity() throws IOException {
                        postingsReader.checkIntegrity();
                    }

                    @Override
                    public Iterator<String> iterator() {
                        return postingsReader.iterator();
                    }

                    /**
                     * 在获取所有doc下某个field下所有的term时 做了特殊处理
                     * @param field
                     * @return
                     * @throws IOException
                     */
                    @Override
                    public Terms terms(String field) throws IOException {
                        Terms in = postingsReader.terms(field);
                        // 如果本次查询的field不是 "_id" 直接忽略就好
                        if (idField.equals(field) && in != null) {
                            return new FilterLeafReader.FilterTerms(in) {
                                @Override
                                public TermsEnum iterator() throws IOException {
                                    // 每个TermsEnum 对应某个doc下该field分词后的term集合
                                    TermsEnum iterator = super.iterator();
                                    // 每次返回的TermsEnum 会通过该filter进行包装
                                    return new FilteredTermsEnum(iterator, false) {
                                        private PostingsEnum internal;

                                        /**
                                         * 这个函数是判断遍历出来的term能否正常使用
                                         * 在FilteredTermsEnum 中会通过next() 遍历term
                                         * @param term
                                         * @return
                                         * @throws IOException
                                         */
                                        @Override
                                        protected AcceptStatus accept(BytesRef term) throws IOException {
                                            // 代表所有doc 都已经被删除
                                            if (fullyDeletedSegment) {
                                                return AcceptStatus.END; // short-cut this if we don't match anything
                                            }
                                            // 只有当这个id对应的数据还存在于 liveDoc中 才能返回yes 否则跳过
                                            internal = postings(internal, PostingsEnum.NONE);
                                            if (internal.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                                return AcceptStatus.YES;
                                            }
                                            return AcceptStatus.NO;
                                        }

                                        @Override
                                        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                                            if (reuse instanceof OnlyLiveDocsPostingsEnum) {
                                                OnlyLiveDocsPostingsEnum reuseInstance = (OnlyLiveDocsPostingsEnum) reuse;
                                                reuseInstance.reset(super.postings(reuseInstance.in, flags));
                                                return reuseInstance;
                                            }
                                            return new OnlyLiveDocsPostingsEnum(super.postings(null, flags), liveDocs);
                                        }

                                        @Override
                                        public ImpactsEnum impacts(int flags) throws IOException {
                                            throw new UnsupportedOperationException();
                                        }
                                    };
                                }
                            };
                        } else {
                            return in;
                        }
                    }

                    @Override
                    public int size() {
                        return postingsReader.size();
                    }

                    @Override
                    public long ramBytesUsed() {
                        return postingsReader.ramBytesUsed();
                    }
                };
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return null;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }

    /**
     * 代表某个term 此时出现的所有位置的posting
     */
    private static final class OnlyLiveDocsPostingsEnum extends PostingsEnum {

        private final Bits liveDocs;
        private PostingsEnum in;

        OnlyLiveDocsPostingsEnum(PostingsEnum in, Bits liveDocs) {
            this.liveDocs = liveDocs;
            reset(in);
        }

        void reset(PostingsEnum in) {
            this.in = in;
        }

        @Override
        public int docID() {
            return in.docID();
        }

        /**
         * 要求该doc没有被删除
         * @return
         * @throws IOException
         */
        @Override
        public int nextDoc() throws IOException {
            int docId;
            do {
                docId = in.nextDoc();
            } while (docId != DocIdSetIterator.NO_MORE_DOCS && liveDocs.get(docId) == false);
            return docId;
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            return in.cost();
        }

        @Override
        public int freq() throws IOException {
            return in.freq();
        }

        @Override
        public int nextPosition() throws IOException {
            return in.nextPosition();
        }

        @Override
        public int startOffset() throws IOException {
            return in.startOffset();
        }

        @Override
        public int endOffset() throws IOException {
            return in.endOffset();
        }

        @Override
        public BytesRef getPayload() throws IOException {
            return in.getPayload();
        }
    }
}
