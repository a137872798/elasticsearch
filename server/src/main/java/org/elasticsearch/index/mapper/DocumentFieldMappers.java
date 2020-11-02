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

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 每个mapper对象好像都是只针对一个field  而 DocFieldMapper 就代表该doc下每个field对应的mapper
 */
public final class DocumentFieldMappers implements Iterable<Mapper> {

    /**
     * Full field name to mapper
     * 存储了相关的映射关系
     */
    private final Map<String, Mapper> fieldMappers;

    /**
     * 该对象以field 为单位 使用不同的analyzer进行分词
     * 相当于 Map<String, Analyzer>
     */
    private final FieldNameAnalyzer indexAnalyzer;

    private static void put(Map<String, Analyzer> analyzers, String key, Analyzer value, Analyzer defaultValue) {
        if (value == null) {
            value = defaultValue;
        }
        analyzers.put(key, value);
    }

    /**
     * 以doc下每个field为单位 对应一个mapper对象
     * @param mappers
     * @param aliasMappers   除了针对field 进行匹配的mapper外 还有针对alias进行匹配的mapper
     * @param defaultIndex     index为什么会需要analyzer呢???
     * @param defaultSearch
     * @param defaultSearchQuote
     */
    public DocumentFieldMappers(Collection<FieldMapper> mappers,
                                Collection<FieldAliasMapper> aliasMappers,
                                Analyzer defaultIndex,
                                Analyzer defaultSearch,
                                Analyzer defaultSearchQuote) {
        Map<String, Mapper> fieldMappers = new HashMap<>();
        Map<String, Analyzer> indexAnalyzers = new HashMap<>();
        Map<String, Analyzer> searchAnalyzers = new HashMap<>();
        Map<String, Analyzer> searchQuoteAnalyzers = new HashMap<>();
        for (FieldMapper mapper : mappers) {
            fieldMappers.put(mapper.name(), mapper);
            MappedFieldType fieldType = mapper.fieldType();
            // 每个mapperFieldType 支持定制analyzer 这里将相关映射关系添加到容器中
            put(indexAnalyzers, fieldType.name(), fieldType.indexAnalyzer(), defaultIndex);
            put(searchAnalyzers, fieldType.name(), fieldType.searchAnalyzer(), defaultSearch);
            put(searchQuoteAnalyzers, fieldType.name(), fieldType.searchQuoteAnalyzer(), defaultSearchQuote);
        }

        // 别名不需要将analyzer的映射关系存储起来么
        for (FieldAliasMapper aliasMapper : aliasMappers) {
            fieldMappers.put(aliasMapper.name(), aliasMapper);
        }

        this.fieldMappers = Collections.unmodifiableMap(fieldMappers);
        this.indexAnalyzer = new FieldNameAnalyzer(indexAnalyzers);
    }

    /**
     * Returns the leaf mapper associated with this field name. Note that the returned mapper
     * could be either a concrete {@link FieldMapper}, or a {@link FieldAliasMapper}.
     *
     * To access a field's type information, {@link MapperService#fieldType} should be used instead.
     */
    public Mapper getMapper(String field) {
        return fieldMappers.get(field);
    }

    /**
     * A smart analyzer used for indexing that takes into account specific analyzers configured
     * per {@link FieldMapper}.
     */
    public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    @Override
    public Iterator<Mapper> iterator() {
        return fieldMappers.values().iterator();
    }
}
