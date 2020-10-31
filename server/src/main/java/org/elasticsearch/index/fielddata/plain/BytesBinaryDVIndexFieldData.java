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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * 指定了数据都是二进制
 */
public class BytesBinaryDVIndexFieldData extends DocValuesIndexFieldData implements IndexFieldData<BytesBinaryDVLeafFieldData> {

    public BytesBinaryDVIndexFieldData(Index index, String fieldName) {
        super(index, fieldName);
    }

    /**
     * 二进制数据不支持排序
     * @param missingValue 当某个doc下不存在该field 那么使用什么值来排序
     * @param sortMode 每个doc中 指定的field下通过分词器可以解析出很多词 这里是在排序时该如何利用这些值  (求和，中位数，最大值等等)
     * @param nested
     * @param reverse
     * @return
     */
    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        throw new IllegalArgumentException("can't sort on binary field");
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, Nested nested,
            SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
        throw new IllegalArgumentException("can't sort on binary field");
    }

    @Override
    public BytesBinaryDVLeafFieldData load(LeafReaderContext context) {
        try {
            // 内部的BinaryDocValues是lucene的类 就是可以迭代该field下所有的doc数据
            return new BytesBinaryDVLeafFieldData(DocValues.getBinary(context.reader(), fieldName));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }

    @Override
    public BytesBinaryDVLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    public static class Builder implements IndexFieldData.Builder {

        /**
         * 修改了返回值类型
         * @param indexSettings  包含所有索引的配置项
         * @param fieldType   描述该field的信息  并且该类型是ES增强过的
         * @param cache
         * @param breakerService  熔断器
         * @param mapperService  映射服务
         * @return
         */
        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService) {
            // Ignore breaker
            final String fieldName = fieldType.name();
            return new BytesBinaryDVIndexFieldData(indexSettings.getIndex(), fieldName);
        }

    }
}
