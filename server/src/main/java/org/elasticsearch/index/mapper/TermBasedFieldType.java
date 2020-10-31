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

package org.elasticsearch.index.mapper;

import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.query.QueryShardContext;

/** Base {@link MappedFieldType} implementation for a field that is indexed
 *  with the inverted index.
 *  基于倒排数据结构进行查询 实际上就是基于Term进行查询
 *  */
abstract class TermBasedFieldType extends SimpleMappedFieldType {

    TermBasedFieldType() {}

    protected TermBasedFieldType(MappedFieldType ref) {
        super(ref);
    }

    /** Returns the indexed value used to construct search "values".
     *  This method is used for the default implementations of most
     *  query factory methods such as {@link #termQuery}. */
    protected BytesRef indexedValueForSearch(Object value) {
        return BytesRefs.toBytesRef(value);
    }

    /**
     * 生成一个 TermQuery对象
     * @param value
     * @param context
     * @return
     */
    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        failIfNotIndexed();
        // indexedValueForSearch 将value转换成term的类型（BytesRef）
        Query query = new TermQuery(new Term(name(), indexedValueForSearch(value)));
        // 代表权重值需要调整 就再包装一层
        if (boost() != 1f) {
            query = new BoostQuery(query, boost());
        }
        return query;
    }

    /**
     * TermInSetQuery 那时候没看 推测是要同时满足这么多term 才能查询到
     * @param values
     * @param context
     * @return
     */
    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        failIfNotIndexed();
        BytesRef[] bytesRefs = new BytesRef[values.size()];
        for (int i = 0; i < bytesRefs.length; i++) {
            bytesRefs[i] = indexedValueForSearch(values.get(i));
        }
        return new TermInSetQuery(name(), bytesRefs);
    }

}
