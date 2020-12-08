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

package org.elasticsearch.index.get;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.TranslogLeafReader;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * 获取分片的服务
 * 实际上是借助Engine实现查询功能
 */
public final class ShardGetService extends AbstractIndexShardComponent {
    private final MapperService mapperService;
    private final MeanMetric existsMetric = new MeanMetric();
    private final MeanMetric missingMetric = new MeanMetric();

    /**
     * 当前有几个正在执行的get操作
     */
    private final CounterMetric currentMetric = new CounterMetric();
    private final IndexShard indexShard;

    public ShardGetService(IndexSettings indexSettings, IndexShard indexShard,
                           MapperService mapperService) {
        super(indexShard.shardId(), indexSettings);
        this.mapperService = mapperService;
        this.indexShard = indexShard;
    }

    public GetStats stats() {
        return new GetStats(existsMetric.count(), TimeUnit.NANOSECONDS.toMillis(existsMetric.sum()),
            missingMetric.count(), TimeUnit.NANOSECONDS.toMillis(missingMetric.sum()), currentMetric.count());
    }


    /**
     * 获取有关某个分片下  某几个 field的数据
     *
     * @param id                 是shard的id吧 之后会包装成term 以termQuery的形式查询对应的doc
     * @param gFields
     * @param realtime
     * @param version
     * @param versionType
     * @param fetchSourceContext 描述了在fetch过程中的相关参数 比如需要包含哪些field 排除哪些field
     * @return
     */
    public GetResult get(String id, String[] gFields, boolean realtime, long version,
                         VersionType versionType, FetchSourceContext fetchSourceContext) {
        return
            get(id, gFields, realtime, version, versionType, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, fetchSourceContext);
    }


    /**
     * @param id
     * @param gFields  获取要求的fields的信息
     * @param realtime  是否实时获取数据
     * @param version
     * @param versionType
     * @param ifSeqNo
     * @param ifPrimaryTerm
     * @param fetchSourceContext 本次是否要fetch  source数据
     * @return
     */
    private GetResult get(String id, String[] gFields, boolean realtime, long version, VersionType versionType,
                          long ifSeqNo, long ifPrimaryTerm, FetchSourceContext fetchSourceContext) {
        // 只是在查询前后做了一些模板操作
        currentMetric.inc();
        try {
            long now = System.nanoTime();
            GetResult getResult =
                innerGet(id, gFields, realtime, version, versionType, ifSeqNo, ifPrimaryTerm, fetchSourceContext);

            // 代表查询命中了
            if (getResult.isExists()) {
                existsMetric.inc(System.nanoTime() - now);
            } else {
                missingMetric.inc(System.nanoTime() - now);
            }
            return getResult;
        } finally {
            currentMetric.dec();
        }
    }

    /**
     * 通过相关参数 利用engine进行数据的查询    默认情况下之获取routingField信息
     * @param id
     * @param ifSeqNo
     * @param ifPrimaryTerm
     * @return
     */
    public GetResult getForUpdate(String id, long ifSeqNo, long ifPrimaryTerm) {
        return get(id, new String[]{RoutingFieldMapper.NAME}, true,
            Versions.MATCH_ANY, VersionType.INTERNAL, ifSeqNo, ifPrimaryTerm, FetchSourceContext.FETCH_SOURCE);
    }

    /**
     * Returns {@link GetResult} based on the specified {@link org.elasticsearch.index.engine.Engine.GetResult} argument.
     * This method basically loads specified fields for the associated document in the engineGetResult.
     * This method load the fields from the Lucene index and not from transaction log and therefore isn't realtime.
     * <p>
     * Note: Call <b>must</b> release engine searcher associated with engineGetResult!
     * 应该是对这个结果进行加工吧
     */
    public GetResult get(Engine.GetResult engineGetResult, String id,
                         String[] fields, FetchSourceContext fetchSourceContext) {
        // 当结果不存在时 返回一个空对象
        if (!engineGetResult.exists()) {
            return new GetResult(shardId.getIndexName(), id, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, -1, false, null, null, null);
        }

        currentMetric.inc();
        try {
            long now = System.nanoTime();
            // 当上下文为null时 尝试设置默认值
            fetchSourceContext = normalizeFetchSourceContent(fetchSourceContext, fields);
            GetResult getResult = innerGetLoadFromStoredFields(id, fields, fetchSourceContext, engineGetResult, mapperService);
            if (getResult.isExists()) {
                existsMetric.inc(System.nanoTime() - now);
            } else {
                missingMetric.inc(System.nanoTime() - now); // This shouldn't happen...
            }
            return getResult;
        } finally {
            currentMetric.dec();
        }
    }

    /**
     * decides what needs to be done based on the request input and always returns a valid non-null FetchSourceContext
     * source 实际上可以理解为doc中某个field的数据  因为doc下可以存在很多field 而默认的source对应的数据就是核心数据
     * 类似json
     * {
     * "xxx":"",
     * "source":"核心数据"
     * }
     */
    private FetchSourceContext normalizeFetchSourceContent(@Nullable FetchSourceContext context, @Nullable String[] gFields) {
        // 如果已经设置了context了 不修改原有的上下文
        if (context != null) {
            return context;
        }

        // 未设置context的场景

        // 当用户没有显示声明要获取的field时 就会获取source
        if (gFields == null) {
            return FetchSourceContext.FETCH_SOURCE;
        }
        // 这里是用户显式声明需要source
        for (String field : gFields) {
            if (SourceFieldMapper.NAME.equals(field)) {
                return FetchSourceContext.FETCH_SOURCE;
            }
        }
        return FetchSourceContext.DO_NOT_FETCH_SOURCE;
    }

    /**
     * 发起一次get请求 借助engine从lucene中查询数据
     * @param id
     * @param gFields 本次相关的几个field
     * @param realtime
     * @param version
     * @param versionType
     * @param ifSeqNo
     * @param ifPrimaryTerm
     * @param fetchSourceContext  是否要拉取source
     * @return
     */
    private GetResult innerGet(String id, String[] gFields, boolean realtime, long version, VersionType versionType,
                               long ifSeqNo, long ifPrimaryTerm, FetchSourceContext fetchSourceContext) {
        // 做一些标准化处理
        fetchSourceContext = normalizeFetchSourceContent(fetchSourceContext, gFields);

        // 将请求体中携带的id 作为term 去lucene中查询数据
        Term uidTerm = new Term(IdFieldMapper.NAME, Uid.encodeId(id));
        // 通过engine 对象获取到查询结果 应该就是在底层使用了 indexReader
        // 注意在get请求时 没有传入field信息
        Engine.GetResult get = indexShard.get(new Engine.Get(realtime, realtime, id, uidTerm)
            .version(version).versionType(versionType).setIfSeqNo(ifSeqNo).setIfPrimaryTerm(ifPrimaryTerm));
        assert get.isFromTranslog() == false || realtime : "should only read from translog if realtime enabled";
        // 当没有查询到相关结果时 关闭get对象(关闭searcher对象)
        if (get.exists() == false) {
            get.close();
        }

        // 返回一个空结果
        if (get == null || get.exists() == false) {
            return new GetResult(shardId.getIndexName(), id, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, -1, false, null, null, null);
        }

        try {
            // break between having loaded it from translog (so we only have _source), and having a document to load
            // 查询到结果后 还需要做处理
            return innerGetLoadFromStoredFields(id, gFields, fetchSourceContext, get, mapperService);
        } finally {
            get.close();
        }
    }

    /**
     * 查询到的结果需要映射成结构化对象
     *
     * @param id
     * @param storedFields 本次需要获取的field
     * @param fetchSourceContext 本次拉取数据时是否要求获取source数据
     * @param get  本次查询结果
     * @param mapperService   映射服务内部定义了如何将数据流转换成结构化对象
     * @return
     */
    private GetResult innerGetLoadFromStoredFields(String id, String[] storedFields, FetchSourceContext fetchSourceContext,
                                                   Engine.GetResult get, MapperService mapperService) {
        assert get.exists() : "method should only be called if document could be retrieved";

        // check first if stored fields to be loaded don't contain an object field
        DocumentMapper docMapper = mapperService.documentMapper();
        if (storedFields != null) {
            for (String field : storedFields) {
                // 根据传入的field 找到field级别的映射对象
                Mapper fieldMapper = docMapper.mappers().getMapper(field);

                // 代表在创建index时 没有指定field的模板 这个时候要求不能存在objectMappers 是为啥???
                if (fieldMapper == null) {
                    if (docMapper.objectMappers().get(field) != null) {
                        // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                        throw new IllegalArgumentException("field [" + field + "] isn't a leaf field");
                    }
                }
            }
        }

        Map<String, DocumentField> documentFields = null;
        Map<String, DocumentField> metadataFields = null;
        BytesReference source = null;
        // 本次查询出来的结果对应的版本号 和docId信息
        DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
        // force fetching source if we read from translog and need to recreate stored fields
        // 如果本次查询结果是从事务日志中获取的 并且本次需要获取的field信息中 包含某个fake属性
        // 这样就会强制要求获取 source数据
        boolean forceSourceForComputingTranslogStoredFields = get.isFromTranslog() && storedFields != null &&
            Stream.of(storedFields).anyMatch(f -> TranslogLeafReader.ALL_FIELD_NAMES.contains(f) == false);

        // 因为指定了field信息 会生成记录这些field信息的 visitor对象
        FieldsVisitor fieldVisitor = buildFieldsVisitors(storedFields,
            // 如果需要强制性的获取source数据 则会传入FETCH_SOURCE 上下文
            forceSourceForComputingTranslogStoredFields ? FetchSourceContext.FETCH_SOURCE : fetchSourceContext);
        if (fieldVisitor != null) {
            try {
                // 读取指定的doc 并使用visitor处理doc数据
                docIdAndVersion.reader.document(docIdAndVersion.docId, fieldVisitor);
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get id [" + id + "]", e);
            }

            // 此时已经解析到source了
            source = fieldVisitor.source();

            // in case we read from translog, some extra steps are needed to make _source consistent and to load stored fields
            // 如果数据是从事务日志中获取的
            if (get.isFromTranslog()) {
                // Fast path: if only asked for the source or stored fields that have been already provided by TranslogLeafReader,
                // just make source consistent by reapplying source filters from mapping (possibly also nulling the source)
                // 本次没有强制获取source field
                if (forceSourceForComputingTranslogStoredFields == false) {
                    try {
                        // sourceMapper 就是专门转换field为source的数据流的 这里通过携带的filter做一层过滤
                        source = indexShard.mapperService().documentMapper().sourceMapper().applyFilters(source, null);
                    } catch (IOException e) {
                        throw new ElasticsearchException("Failed to reapply filters for [" + id + "] after reading from translog", e);
                    }
                } else {
                    // 这里获取到了 source对应的数据流
                    // Slow path: recreate stored fields from original source
                    assert source != null : "original source in translog must exist";
                    // 将路由信息/source信息/indexName等包装成 sourceParse对象
                    SourceToParse sourceToParse = new SourceToParse(shardId.getIndexName(), id, source, XContentHelper.xContentType(source),
                        fieldVisitor.routing());

                    // TODO 暂时先不管解析的过程 当解析完成后就返回了一个 ParsedDocument对象
                    ParsedDocument doc = indexShard.mapperService().documentMapper().parse(sourceToParse);
                    assert doc.dynamicMappingsUpdate() == null : "mapping updates should not be required on already-indexed doc";
                    // update special fields
                    // 用本次get请求获取到的seqNo primaryTerm信息去更新doc信息
                    doc.updateSeqID(docIdAndVersion.seqNo, docIdAndVersion.primaryTerm);
                    doc.version().setLongValue(docIdAndVersion.version);

                    // retrieve stored fields from parsed doc
                    // 之前解析的时候 不能直接获取到field信息 因为它们被隐藏在source中了  而当这些数据被解析出来后 就可以使用一个新的visitor去处理它们
                    fieldVisitor = buildFieldsVisitors(storedFields, fetchSourceContext);
                    for (IndexableField indexableField : doc.rootDoc().getFields()) {
                        IndexableFieldType fieldType = indexableField.fieldType();
                        // 确定该field.value可以被读取到 才使用visitor进行处理
                        if (fieldType.stored()) {
                            FieldInfo fieldInfo = new FieldInfo(indexableField.name(), 0, false, false, false, IndexOptions.NONE,
                                DocValuesType.NONE, -1, Collections.emptyMap(), 0, 0, 0, false);
                            StoredFieldVisitor.Status status = fieldVisitor.needsField(fieldInfo);
                            // 如果需要获取这个field的数据 设置到visitor中
                            if (status == StoredFieldVisitor.Status.YES) {
                                if (indexableField.binaryValue() != null) {
                                    fieldVisitor.binaryField(fieldInfo, indexableField.binaryValue());
                                } else if (indexableField.stringValue() != null) {
                                    fieldVisitor.objectField(fieldInfo, indexableField.stringValue());
                                } else if (indexableField.numericValue() != null) {
                                    fieldVisitor.objectField(fieldInfo, indexableField.numericValue());
                                }
                            } else if (status == StoredFieldVisitor.Status.STOP) {
                                break;
                            }
                        }
                    }
                    // retrieve source (with possible transformations, e.g. source filters
                    // 重新获取了source
                    source = fieldVisitor.source();
                }
            }

            // put stored fields into result objects
            // 确保解析到field的数据后 才进行处理
            if (!fieldVisitor.fields().isEmpty()) {
                // 对field对应的数据进行再加工后 重新设置进去
                fieldVisitor.postProcess(mapperService);
                documentFields = new HashMap<>();
                metadataFields = new HashMap<>();
                for (Map.Entry<String, List<Object>> entry : fieldVisitor.fields().entrySet()) {
                    // field如果是描述元数据的就存储到metadataFields 如果是数据型的就存储到docFields
                    if (MapperService.isMetadataField(entry.getKey())) {
                        metadataFields.put(entry.getKey(), new DocumentField(entry.getKey(), entry.getValue()));
                    } else {
                        documentFields.put(entry.getKey(), new DocumentField(entry.getKey(), entry.getValue()));
                    }
                }
            }
        }

        // 代表通过reader获取了doc信息 在当中有source信息
        if (source != null) {
            // apply request-level source filtering
            if (fetchSourceContext.fetchSource() == false) {
                source = null;
                // 根据 includes/excludes 裁剪source     默认生成的fetchSourceContext是没有这些属性的 一定是从外部设置的
            } else if (fetchSourceContext.includes().length > 0 || fetchSourceContext.excludes().length > 0) {
                Map<String, Object> sourceAsMap;
                // TODO: The source might be parsed and available in the sourceLookup but that one uses unordered maps so different.
                //  Do we care?
                Tuple<XContentType, Map<String, Object>> typeMapTuple = XContentHelper.convertToMap(source, true);
                XContentType sourceContentType = typeMapTuple.v1();
                sourceAsMap = typeMapTuple.v2();
                sourceAsMap = XContentMapValues.filter(sourceAsMap, fetchSourceContext.includes(), fetchSourceContext.excludes());
                try {
                    source = BytesReference.bytes(XContentFactory.contentBuilder(sourceContentType).map(sourceAsMap));
                } catch (IOException e) {
                    throw new ElasticsearchException("Failed to get id [" + id + "] with includes/excludes set", e);
                }
            }
        }

        return new GetResult(shardId.getIndexName(), id, get.docIdAndVersion().seqNo, get.docIdAndVersion().primaryTerm,
            get.version(), get.exists(), source, documentFields, metadataFields);
    }

    /**
     * @param fields
     * @param fetchSourceContext
     * @return
     */
    private static FieldsVisitor buildFieldsVisitors(String[] fields, FetchSourceContext fetchSourceContext) {
        // fieldsVisitor 就是拦截field 并记录value的对象 其中 id routing source(可选) 作为特殊字段会被保留
        if (fields == null || fields.length == 0) {
            return fetchSourceContext.fetchSource() ? new FieldsVisitor(true) : null;
        }

        // 当指定了field时 会存储这些field的数据
        return new CustomFieldsVisitor(Sets.newHashSet(fields), fetchSourceContext.fetchSource());
    }
}
