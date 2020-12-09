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

package org.elasticsearch.index.termvectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.memory.MemoryIndex;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.termvectors.TermVectorsFilter;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.dfs.AggregatedDfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * 查询词向量信息的服务
 */
public class TermVectorsService  {


    private TermVectorsService() {}

    /**
     * 针对某个分片  根据req信息 查询词向量
     * @param indexShard
     * @param request
     * @return
     */
    public static TermVectorsResponse getTermVectors(IndexShard indexShard, TermVectorsRequest request) {
        return getTermVectors(indexShard, request, System::nanoTime);
    }


    /**
     *
     * @param indexShard   当前查询的是哪个分片
     * @param request
     * @param nanoTimeSupplier
     * @return
     */
    static TermVectorsResponse getTermVectors(IndexShard indexShard, TermVectorsRequest request, LongSupplier nanoTimeSupplier) {
        final long startTime = nanoTimeSupplier.getAsLong();
        final TermVectorsResponse termVectorsResponse = new TermVectorsResponse(indexShard.shardId().getIndex().getName(), request.id());

        // 将id 包装成term
        final Term uidTerm = new Term(IdFieldMapper.NAME, Uid.encodeId(request.id()));

        Fields termVectorsByField = null;
        AggregatedDfs dfs = null;
        TermVectorsFilter termVectorsFilter = null;

        /* handle potential wildcards in fields */
        // 可能selectedFields内部有通配符 转换成明确的field后覆盖
        if (request.selectedFields() != null) {
            handleFieldWildcards(indexShard, request);
        }

        try (Engine.GetResult get = indexShard.get(new Engine.Get(request.realtime(), false, request.id(), uidTerm).version(request.version()).versionType(request.versionType()));
             Engine.Searcher searcher = indexShard.acquireSearcher("term_vector")) {

            // 将查询到的结果包装成了一个 Fields 对象 对应一个doc  通过传入某个field 可以获取到 Terms对象 可以遍历某field在该doc下所有的term
            Fields topLevelFields = fields(get.searcher() != null ? get.searcher().getIndexReader() : searcher.getIndexReader());
            DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
            /* from an artificial document */
            // 如果请求体内已经设置了doc  那么本次结果相当于是人为产生的 doc本身并不存在于shard中
            if (request.doc() != null) {
                // 在内存中解析docValue 并获取词向量
                termVectorsByField = generateTermVectorsFromDoc(indexShard, request);
                // if no document indexed in shard, take the queried document itself for stats
                if (topLevelFields == null) {
                    topLevelFields = termVectorsByField;
                }
                termVectorsResponse.setArtificial(true);
                termVectorsResponse.setExists(true);
            }
            /* or from an existing document */
            // 从已经持久化的文档中获取信息
            else if (docIdAndVersion != null) {
                // fields with stored term vectors
                // 从之前持久化的数据中直接获取词向量信息
                termVectorsByField = docIdAndVersion.reader.getTermVectors(docIdAndVersion.docId);
                Set<String> selectedFields = request.selectedFields();
                // generate tvs for fields where analyzer is overridden
                // 如果req中指定了 analyzer 那么可能需要重新解析doc了 因为之前使用的是默认的 analyzer
                // 判断selectedFields 是为了根据这些field去 perFieldAnalyer 中查找分词器
                if (selectedFields == null && request.perFieldAnalyzer() != null) {
                    // 更新后的selectedField 记录的就是所有需要重新解析的数据
                    selectedFields = getFieldsToGenerate(request.perFieldAnalyzer(), termVectorsByField);
                }
                // fields without term vectors
                if (selectedFields != null) {
                    // 这些field需要重新解析词向量
                    termVectorsByField = addGeneratedTermVectors(indexShard, get, termVectorsByField, request, selectedFields);
                }
                termVectorsResponse.setDocVersion(docIdAndVersion.version);
                termVectorsResponse.setExists(true);
            }
            /* no term vectors generated or found */
            // 没有找到匹配的结果 设置 exist为false
            else {
                termVectorsResponse.setExists(false);
            }
            /* if there are term vectors, optional compute dfs and/or terms filtering */
            if (termVectorsByField != null) {
                if (request.filterSettings() != null) {
                    // 如果设置了过滤信息 那么对结果进行过滤
                    termVectorsFilter = new TermVectorsFilter(termVectorsByField, topLevelFields, request.selectedFields(), dfs);
                    termVectorsFilter.setSettings(request.filterSettings());
                    try {
                        termVectorsFilter.selectBestTerms();
                    } catch (IOException e) {
                        throw new ElasticsearchException("failed to select best terms", e);
                    }
                }
                // write term vectors
                // 将过滤后的结果写入到 response中
                termVectorsResponse.setFields(termVectorsByField, request.selectedFields(), request.getFlags(), topLevelFields, dfs,
                        termVectorsFilter);
            }
            termVectorsResponse.setTookInMillis(TimeUnit.NANOSECONDS.toMillis(nanoTimeSupplier.getAsLong() - startTime));
        } catch (Exception ex) {
            throw new ElasticsearchException("failed to execute term vector request", ex);
        }
        return termVectorsResponse;
    }

    public static Fields fields(IndexReader reader) {
        return new Fields() {
            @Override
            public Iterator<String> iterator() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Terms terms(String field) throws IOException {
                return MultiTerms.getTerms(reader, field);
            }

            @Override
            public int size() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * 处理潜在的通配符
     * @param indexShard
     * @param request
     */
    private static void handleFieldWildcards(IndexShard indexShard, TermVectorsRequest request) {
        Set<String> fieldNames = new HashSet<>();
        for (String pattern : request.selectedFields()) {
            // 根据通配符找到匹配的所有field
            fieldNames.addAll(indexShard.mapperService().simpleMatchToFullName(pattern));
        }
        // 使用这组field去替换之前req内部的field
        request.selectedFields(fieldNames.toArray(Strings.EMPTY_ARRAY));
    }

    private static boolean isValidField(MappedFieldType fieldType) {
        // must be a string
        if (fieldType instanceof StringFieldType == false) {
            return false;
        }
        // and must be indexed
        if (fieldType.indexOptions() == IndexOptions.NONE) {
            return false;
        }
        return true;
    }

    /**
     *
     * @param indexShard
     * @param get
     * @param termVectorsByField
     * @param request
     * @param selectedFields 这些field对应的docValue需要重新解析
     * @return
     * @throws IOException
     */
    private static Fields addGeneratedTermVectors(IndexShard indexShard, Engine.GetResult get, Fields termVectorsByField,
                                                        TermVectorsRequest request, Set<String> selectedFields) throws IOException {
        /* only keep valid fields */
        Set<String> validFields = new HashSet<>();
        for (String field : selectedFields) {
            MappedFieldType fieldType = indexShard.mapperService().fieldType(field);
            if (!isValidField(fieldType)) {
                continue;
            }
            // already retrieved, only if the analyzer hasn't been overridden at the field
            if (fieldType.storeTermVectors() &&
                    (request.perFieldAnalyzer() == null || !request.perFieldAnalyzer().containsKey(field))) {
                continue;
            }
            validFields.add(field);
        }

        // 没有需要解析的field了 返回原本的词向量信息
        if (validFields.isEmpty()) {
            return termVectorsByField;
        }

        /* generate term vectors from fetched document fields */
        String[] getFields = validFields.toArray(new String[validFields.size() + 1]);
        getFields[getFields.length - 1] = SourceFieldMapper.NAME;
        // 仅查询这些field对应的数据
        GetResult getResult = indexShard.getService().get(get, request.id(), getFields, null);
        Fields generatedTermVectors = generateTermVectors(indexShard, getResult.sourceAsMap(), getResult.getFields().values(),
            request.offsets(), request.perFieldAnalyzer(), validFields);

        /* merge with existing Fields */
        if (termVectorsByField == null) {
            return generatedTermVectors;
        } else {
            // 将结果进行合并 应该就是新解析的field数据 会覆盖之前的数据
            return mergeFields(termVectorsByField, generatedTermVectors);
        }
    }

    /**
     * 根据field信息找到匹配的analyzer
     * @param indexShard
     * @param field
     * @param perFieldAnalyzer  通过该容器维护的映射信息找到analyzer
     * @return
     */
    private static Analyzer getAnalyzerAtField(IndexShard indexShard, String field, @Nullable Map<String, String> perFieldAnalyzer) {
        MapperService mapperService = indexShard.mapperService();
        Analyzer analyzer;
        if (perFieldAnalyzer != null && perFieldAnalyzer.containsKey(field)) {
            analyzer = mapperService.getIndexAnalyzers().get(perFieldAnalyzer.get(field).toString());
        } else {
            // 当用户没有强制声明使用的analyzer时 使用默认的分词器
            MappedFieldType fieldType = mapperService.fieldType(field);
            analyzer = fieldType.indexAnalyzer();
        }
        // 如果field在 MapperService中没有指定分词器 那么使用默认分词器
        if (analyzer == null) {
            analyzer = mapperService.getIndexAnalyzers().getDefaultIndexAnalyzer();
        }
        return analyzer;
    }

    private static Set<String> getFieldsToGenerate(Map<String, String> perAnalyzerField, Fields fieldsObject) {
        Set<String> selectedFields = new HashSet<>();
        for (String fieldName : fieldsObject) {
            if (perAnalyzerField.containsKey(fieldName)) {
                selectedFields.add(fieldName);
            }
        }
        return selectedFields;
    }

    /**
     * 获取词向量信息
     * @param indexShard  本次处理的某个分片
     * @param source   对应doc解析后的结构化数据 (json数据)
     * @param getFields    DocumentField存储的是每个field对应的docValue
     * @param withOffsets   是否需要获取offset信息
     * @param perFieldAnalyzer   针对每个field可以配置专门的解析器
     * @param fields    要获取哪些field的数据
     * @return
     * @throws IOException
     */
    private static Fields generateTermVectors(IndexShard indexShard,
                                              Map<String, Object> source,
                                              Collection<DocumentField> getFields,
                                              boolean withOffsets,
                                              @Nullable Map<String, String> perFieldAnalyzer,
                                              Set<String> fields) throws IOException {
        Map<String, Collection<Object>> values = new HashMap<>();
        for (DocumentField getField : getFields) {
            String field = getField.getName();
            if (fields.contains(field)) { // some fields are returned even when not asked for, eg. _timestamp
                values.put(field, getField.getValues());
            }
        }
        // 为了避免某些field对应的docValue 没有配置到 getField  这里通过解析source 又获得了一份数据 并插入到map中
        if (source != null) {
            for (String field : fields) {
                if (values.containsKey(field) == false) {
                    //就是从source中找到目标数据
                    List<Object> v = XContentMapValues.extractRawValues(field, source);
                    if (v.isEmpty() == false) {
                        values.put(field, v);
                    }
                }
            }
        }

        /* store document in memory index */
        // 入参的文档数据实际上是从req.doc()中获取的 在此之前并没有存入到磁盘 所以在内存中解析会更方便
        MemoryIndex index = new MemoryIndex(withOffsets);
        for (Map.Entry<String, Collection<Object>> entry : values.entrySet()) {
            String field = entry.getKey();
            // 通过field找到匹配的 analyzer
            Analyzer analyzer = getAnalyzerAtField(indexShard, field, perFieldAnalyzer);
            if (entry.getValue() instanceof List) {
                for (Object text : entry.getValue()) {
                    index.addField(field, text.toString(), analyzer);
                }
            } else {
                index.addField(field, entry.getValue().toString(), analyzer);
            }
        }
        /* and read vectors from it */
        return index.createSearcher().getIndexReader().getTermVectors(0);
    }

    /**
     * 根据当前分片数据 以及req 生成一个可以遍历指定field下所有term的迭代器对象
     * @param indexShard
     * @param request
     * @return
     * @throws IOException
     */
    private static Fields generateTermVectorsFromDoc(IndexShard indexShard, TermVectorsRequest request) throws IOException {
        // parse the document, at the moment we do update the mapping, just like percolate
        // 将.doc()转换成 文档对象    indexName 可以匹配到合适的mapperService
        ParsedDocument parsedDocument = parseDocument(indexShard, indexShard.shardId().getIndexName(), request.doc(),
            request.xContentType(), request.routing());

        // select the right fields and generate term vectors
        ParseContext.Document doc = parsedDocument.rootDoc();
        Set<String> seenFields = new HashSet<>();
        Collection<DocumentField> documentFields = new HashSet<>();
        // 从root开始遍历每个field信息
        for (IndexableField field : doc.getFields()) {
            MappedFieldType fieldType = indexShard.mapperService().fieldType(field.name());
            // 当前这个field indexOptional 为 NONE 不进行处理
            if (!isValidField(fieldType)) {
                continue;
            }
            // 如果请求体中指定了 field 而该field不属于被选中的field 那么跳过
            if (request.selectedFields() != null && !request.selectedFields().contains(field.name())) {
                continue;
            }
            // field重复出现 跳过
            if (seenFields.contains(field.name())) {
                continue;
            }
            else {
                seenFields.add(field.name());
            }
            // 将field对应的docValue取出来
            String[] values = getValues(doc.getFields(field.name()));
            documentFields.add(new DocumentField(field.name(), Arrays.asList((Object[]) values)));
        }

        // 通过docValue 获取词向量信息
        return generateTermVectors(indexShard,
            XContentHelper.convertToMap(parsedDocument.source(), true, request.xContentType()).v2(), documentFields,
                request.offsets(), request.perFieldAnalyzer(), seenFields);
    }

    /**
     * Returns an array of values of the field specified as the method parameter.
     * This method returns an empty array when there are no
     * matching fields.  It never returns null.
     * @param fields The <code>IndexableField</code> to get the values from
     * @return a <code>String[]</code> of field values
     * 获取这组field 对应的docValue   docValue在lucene中就是字面值  不需要通过分词器解析
     */
    public static String[] getValues(IndexableField[] fields) {
        List<String> result = new ArrayList<>();
        for (IndexableField field : fields) {
            if (field.fieldType() instanceof KeywordFieldMapper.KeywordFieldType) {
                result.add(field.binaryValue().utf8ToString());
            } else if (field.fieldType() instanceof StringFieldType) {
                result.add(field.stringValue());
            }
        }
        return result.toArray(new String[0]);
    }

    /**
     * 将传入的doc 通过mapperService 映射成 ParsedDocument对象
     * @param indexShard
     * @param index
     * @param doc
     * @param xContentType
     * @param routing
     * @return
     */
    private static ParsedDocument parseDocument(IndexShard indexShard, String index, BytesReference doc,
                                                XContentType xContentType, String routing) {
        MapperService mapperService = indexShard.mapperService();
        DocumentMapperForType docMapper = mapperService.documentMapperWithAutoCreate();
        ParsedDocument parsedDocument = docMapper.getDocumentMapper().parse(
                new SourceToParse(index, "_id_for_tv_api", doc, xContentType, routing));
        if (docMapper.getMapping() != null) {
            // TODO
            parsedDocument.addDynamicMappingsUpdate(docMapper.getMapping());
        }
        return parsedDocument;
    }

    /**
     * 将2个 fields内的数据进行合并
     * @param fields1
     * @param fields2
     * @return
     * @throws IOException
     */
    private static Fields mergeFields(Fields fields1, Fields fields2) throws IOException {
        ParallelFields parallelFields = new ParallelFields();
        for (String fieldName : fields2) {
            Terms terms = fields2.terms(fieldName);
            if (terms != null) {
                parallelFields.addField(fieldName, terms);
            }
        }
        for (String fieldName : fields1) {
            if (parallelFields.fields.containsKey(fieldName)) {
                continue;
            }
            Terms terms = fields1.terms(fieldName);
            if (terms != null) {
                parallelFields.addField(fieldName, terms);
            }
        }
        return parallelFields;
    }

    // Poached from Lucene ParallelLeafReader
    private static final class ParallelFields extends Fields {
        final Map<String,Terms> fields = new TreeMap<>();

        ParallelFields() {
        }

        void addField(String fieldName, Terms terms) {
            fields.put(fieldName, terms);
        }

        @Override
        public Iterator<String> iterator() {
            return Collections.unmodifiableSet(fields.keySet()).iterator();
        }

        @Override
        public Terms terms(String field) {
            return fields.get(field);
        }

        @Override
        public int size() {
            return fields.size();
        }
    }
}
