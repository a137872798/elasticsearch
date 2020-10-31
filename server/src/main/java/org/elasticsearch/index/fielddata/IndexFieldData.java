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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * Thread-safe utility class that allows to get per-segment values via the
 * {@link #load(LeafReaderContext)} method.
 * 该对象代表可以读取索引下某个field的数据
 */
public interface IndexFieldData<FD extends LeafFieldData> extends IndexComponent {

    /**
     * The field name.
     * 当前field的名字
     */
    String getFieldName();

    /**
     * Loads the atomic field data for the reader, possibly cached.
     * 读取该field下的数据  如果允许的话 使用缓存
     */
    FD load(LeafReaderContext context);

    /**
     * Loads directly the atomic field data for the reader, ignoring any caching involved.
     * 读取field下的数据 并且不使用缓存
     */
    FD loadDirect(LeafReaderContext context) throws Exception;

    /**
     * Returns the {@link SortField} to use for sorting.
     * @param missingValue 当某个doc下不存在该field 那么使用什么值来排序
     * @param sortMode 每个doc中 指定的field下通过分词器可以解析出很多词 这里是在排序时该如何利用这些值  (求和，中位数，最大值等等)
     * 获取这组field下数据的排序规则
     */
    SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse);

    /**
     * Build a sort implementation specialized for aggregations.
     * @param bigArrays 该对象可以分配数组 并且内部利用了内存池技术 减少GC
     * 一个使用桶排序算法的模板
     */
    BucketedSort newBucketedSort(BigArrays bigArrays, @Nullable Object missingValue, MultiValueMode sortMode,
            Nested nested, SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra);

    /**
     * Clears any resources associated with this field data.
     * 将关联的field下所有的数据都清理
     */
    void clear();

    // we need this extended source we we have custom comparators to reuse our field data
    // in this case, we need to reduce type that will be used when search results are reduced
    // on another node (we don't have the custom source them...)
    abstract class XFieldComparatorSource extends FieldComparatorSource {

        protected final MultiValueMode sortMode;
        protected final Object missingValue;
        protected final Nested nested;

        public XFieldComparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
            this.sortMode = sortMode;
            this.missingValue = missingValue;
            this.nested = nested;
        }

        public MultiValueMode sortMode() {
            return this.sortMode;
        }

        public Nested nested() {
            return this.nested;
        }

        /**
         * Simple wrapper class around a filter that matches parent documents
         * and a filter that matches child documents. For every root document R,
         * R will be in the parent filter and its children documents will be the
         * documents that are contained in the inner set between the previous
         * parent + 1, or 0 if there is no previous parent, and R (excluded).
         * 代表一种嵌套体
         */
        public static class Nested {

            /**
             * 通过传入某个segment的上下文信息 (LeafReaderContext)
             * 获取一个位图对象
             */
            private final BitSetProducer rootFilter;
            /**
             * 查询条件对象
             */
            private final Query innerQuery;
            private final NestedSortBuilder nestedSort;
            /**
             * 查询对象
             */
            private final IndexSearcher searcher;

            public Nested(BitSetProducer rootFilter, Query innerQuery, NestedSortBuilder nestedSort, IndexSearcher searcher) {
                this.rootFilter = rootFilter;
                this.innerQuery = innerQuery;
                this.nestedSort = nestedSort;
                this.searcher = searcher;
            }

            public Query getInnerQuery() {
                return innerQuery;
            }

            public NestedSortBuilder getNestedSort() { return nestedSort; }

            /**
             * Get a {@link BitDocIdSet} that matches the root documents.
             */
            public BitSet rootDocs(LeafReaderContext ctx) throws IOException {
                return rootFilter.getBitSet(ctx);
            }

            /**
             * Get a {@link DocIdSet} that matches the inner documents.
             */
            public DocIdSetIterator innerDocs(LeafReaderContext ctx) throws IOException {
                Weight weight = searcher.createWeight(searcher.rewrite(innerQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);
                Scorer s = weight.scorer(ctx);
                return s == null ? null : s.iterator();
            }
        }

        /** Whether missing values should be sorted first. */
        public final boolean sortMissingFirst(Object missingValue) {
            return "_first".equals(missingValue);
        }

        /** Whether missing values should be sorted last, this is the default. */
        public final boolean sortMissingLast(Object missingValue) {
            return missingValue == null || "_last".equals(missingValue);
        }

        /** Return the missing object value according to the reduced type of the comparator. */
        public final Object missingObject(Object missingValue, boolean reversed) {
            if (sortMissingFirst(missingValue) || sortMissingLast(missingValue)) {
                final boolean min = sortMissingFirst(missingValue) ^ reversed;
                switch (reducedType()) {
                case INT:
                    return min ? Integer.MIN_VALUE : Integer.MAX_VALUE;
                case LONG:
                    return min ? Long.MIN_VALUE : Long.MAX_VALUE;
                case FLOAT:
                    return min ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
                case DOUBLE:
                    return min ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
                case STRING:
                case STRING_VAL:
                    return null;
                default:
                    throw new UnsupportedOperationException("Unsupported reduced type: " + reducedType());
                }
            } else {
                switch (reducedType()) {
                case INT:
                    if (missingValue instanceof Number) {
                        return ((Number) missingValue).intValue();
                    } else {
                        return Integer.parseInt(missingValue.toString());
                    }
                case LONG:
                    if (missingValue instanceof Number) {
                        return ((Number) missingValue).longValue();
                    } else {
                        return Long.parseLong(missingValue.toString());
                    }
                case FLOAT:
                    if (missingValue instanceof Number) {
                        return ((Number) missingValue).floatValue();
                    } else {
                        return Float.parseFloat(missingValue.toString());
                    }
                case DOUBLE:
                    if (missingValue instanceof Number) {
                        return ((Number) missingValue).doubleValue();
                    } else {
                        return Double.parseDouble(missingValue.toString());
                    }
                case STRING:
                case STRING_VAL:
                    if (missingValue instanceof BytesRef) {
                        return (BytesRef) missingValue;
                    } else if (missingValue instanceof byte[]) {
                        return new BytesRef((byte[]) missingValue);
                    } else {
                        return new BytesRef(missingValue.toString());
                    }
                default:
                    throw new UnsupportedOperationException("Unsupported reduced type: " + reducedType());
                }
            }
        }

        public abstract SortField.Type reducedType();

        /**
         * Return a missing value that is understandable by {@link SortField#setMissingValue(Object)}.
         * Most implementations return null because they already replace the value at the fielddata level.
         * However this can't work in case of strings since there is no such thing as a string which
         * compares greater than any other string, so in that case we need to return
         * {@link SortField#STRING_FIRST} or {@link SortField#STRING_LAST} so that the coordinating node
         * knows how to deal with null values.
         */
        public Object missingValue(boolean reversed) {
            return null;
        }

        /**
         * Create a {@linkplain BucketedSort} which is useful for sorting inside of aggregations.
         */
        public abstract BucketedSort newBucketedSort(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format,
                int bucketSize, BucketedSort.ExtraData extra);
    }

    interface Builder {

        /**
         * builder对象 当传入相关参数时可以构建一个 存储该field下所有doc数据的对象
         * @param indexSettings  包含所有索引的配置项
         * @param fieldType   描述该field的信息  并且该类型是ES增强过的
         * @param cache
         * @param breakerService  熔断器
         * @param mapperService  映射服务
         * @return
         */
        IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                             CircuitBreakerService breakerService, MapperService mapperService);
    }

    interface Global<FD extends LeafFieldData> extends IndexFieldData<FD> {

        IndexFieldData<FD> loadGlobal(DirectoryReader indexReader);

        IndexFieldData<FD> localGlobalDirect(DirectoryReader indexReader) throws Exception;

    }
}
