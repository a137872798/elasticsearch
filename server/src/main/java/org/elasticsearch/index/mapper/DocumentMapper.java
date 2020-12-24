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

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.mapper.MetadataFieldMapper.TypeParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;


/**
 * 针对doc级别的映射对象    还有fieldMapper
 */
public class DocumentMapper implements ToXContentFragment {

    public static class Builder {

        private Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers = new LinkedHashMap<>();

        /**
         * 根级的结构转换成的映射对象 内部包含了很多mapper
         */
        private final RootObjectMapper rootObjectMapper;

        /**
         * 从传入的json结构体中 获取 key为 "_meta" 对应的数据 并设置到该字段上
         */
        private Map<String, Object> meta;

        /**
         * 每个Mapper 对应的builder对象需要借助这个context对象才能生成Mapper
         */
        private final Mapper.BuilderContext builderContext;

        /**
         * 通过 RootObjectMapper.Builder对象来初始化  DocumentMapper.Builder 对象
         * @param builder  在解析外部传入的json数据后 抽取了内置的一些属性后 会生成一个RootObjectMapper.Builder 对象
         * @param mapperService
         */
        public Builder(RootObjectMapper.Builder builder, MapperService mapperService) {
            final Settings indexSettings = mapperService.getIndexSettings().getSettings();
            // 生成构建 DocumentMapper时使用的 builderContext
            this.builderContext = new Mapper.BuilderContext(indexSettings, new ContentPath(1));

            // 根据之前抽取到builder中的一些默认属性 构建 mapper对象  在内部形成了类似 trie树的结构
            this.rootObjectMapper = builder.build(builderContext);

            // 可以先简单理解成 _doc
            final String type = rootObjectMapper.name();

            // 感觉内部存储的像是一个全局对象
            final DocumentMapper existingMapper = mapperService.documentMapper();
            final Version indexCreatedVersion = mapperService.getIndexSettings().getIndexVersionCreated();

            // 获取所有元数据相关的parser
            // 在解析过程中也会需要使用到TypeParser   但是是 Mapper.TypeParser 而不是 MetadataFieldMapper.TypeParser 这里是专门准备解析元数据
            final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers =
                mapperService.mapperRegistry.getMetadataMapperParsers(indexCreatedVersion);
            for (Map.Entry<String, MetadataFieldMapper.TypeParser> entry : metadataMapperParsers.entrySet()) {
                final String name = entry.getKey();

                // 如果存在全局对象 直接复用 否则使用默认值
                final MetadataFieldMapper existingMetadataMapper = existingMapper == null
                        ? null
                        : (MetadataFieldMapper) existingMapper.mappers().getMapper(name);
                final MetadataFieldMapper metadataMapper;
                if (existingMetadataMapper == null) {
                    final TypeParser parser = entry.getValue();
                    // 会生成一个默认的 元数据映射对象
                    metadataMapper = parser.getDefault(mapperService.documentMapperParser().parserContext());
                } else {
                    metadataMapper = existingMetadataMapper;
                }
                metadataMappers.put(metadataMapper.getClass(), metadataMapper);
            }
        }

        public Builder meta(Map<String, Object> meta) {
            this.meta = meta;
            return this;
        }

        /**
         * 在解析数据流时 尽可能将所有的key 都去 MapperRegistry中获取 TypeParser并生成builder 然后填充到这里
         * @param mapper
         * @return
         */
        public Builder put(MetadataFieldMapper.Builder<?, ?> mapper) {
            MetadataFieldMapper metadataMapper = mapper.build(builderContext);
            metadataMappers.put(metadataMapper.getClass(), metadataMapper);
            return this;
        }

        /**
         * 在将各种信息从 json结构对象抽取到builder后 开始创建DocumentMapper对象
         * @param mapperService
         * @return
         */
        public DocumentMapper build(MapperService mapperService) {
            Objects.requireNonNull(rootObjectMapper, "Mapper builder must have the root object mapper set");
            Mapping mapping = new Mapping(
                    mapperService.getIndexSettings().getIndexVersionCreated(),
                    rootObjectMapper,
                    metadataMappers.values().toArray(new MetadataFieldMapper[metadataMappers.values().size()]),
                    meta);
            // 将相关信息包装成 DocumentMapper对象
            return new DocumentMapper(mapperService, mapping);
        }
    }

    private final MapperService mapperService;

    /**
     * 代表本次要映射的类型 比如 "_doc"  也就是 RootObjectMapper.name
     */
    private final String type;
    private final Text typeText;

    private final CompressedXContent mappingSource;

    /**
     * 记录该doc下每个field 对应的mapper对象
     */
    private final Mapping mapping;

    private final DocumentParser documentParser;

    /**
     * 针对field 的映射逻辑都委托给该对象
     */
    private final DocumentFieldMappers fieldMappers;

    private final Map<String, ObjectMapper> objectMappers;

    private final boolean hasNestedObjects;

    /**
     * 针对删除操作 会使用一些固定的field
     */
    private final MetadataFieldMapper[] deleteTombstoneMetadataFieldMappers;
    /**
     * 有些metadataFieldMapper 认为是 noop的 本次创建的documentMapper对象 如果命中了会存入该数组
     */
    private final MetadataFieldMapper[] noopTombstoneMetadataFieldMappers;

    /**
     * @param mapperService   存储了实现映射功能需要的各种组件
     * @param mapping   存储了完成一次映射需要的各种信息
     */
    public DocumentMapper(MapperService mapperService, Mapping mapping) {
        this.mapperService = mapperService;
        // 先简单认为是 "_doc"
        this.type = mapping.root().name();
        this.typeText = new Text(this.type);
        final IndexSettings indexSettings = mapperService.getIndexSettings();
        this.mapping = mapping;

        // DocumentParser内定义了映射的步骤
        this.documentParser = new DocumentParser(indexSettings, mapperService.documentMapperParser(), this);

        // collect all the mappers for this type
        List<ObjectMapper> newObjectMappers = new ArrayList<>();
        List<FieldMapper> newFieldMappers = new ArrayList<>();
        List<FieldAliasMapper> newFieldAliasMappers = new ArrayList<>();
        // 这个映射对象中 有关元数据的部分被提取到 newFieldMappers容器
        for (MetadataFieldMapper metadataMapper : this.mapping.metadataMappers) {
            if (metadataMapper instanceof FieldMapper) {
                newFieldMappers.add(metadataMapper);
            }
        }
        // 将整个 trie树下的mapper对象 按照类型存储到不同的list中
        MapperUtils.collect(this.mapping.root,
            newObjectMappers, newFieldMappers, newFieldAliasMappers);

        // 该对象可以按照不同的field 获取到不同的analyzer
        final IndexAnalyzers indexAnalyzers = mapperService.getIndexAnalyzers();

        // 将一些默认的分析器 与 fieldMapper对象整合成一个 以field为单位进行映射的对象
        this.fieldMappers = new DocumentFieldMappers(newFieldMappers,
                newFieldAliasMappers,
                indexAnalyzers.getDefaultIndexAnalyzer(),
                indexAnalyzers.getDefaultSearchAnalyzer(),
                indexAnalyzers.getDefaultSearchQuoteAnalyzer());

        // 下面处理 关于 Object的映射对象  trie树下所有的ObjectMapper都会被抽取出来 设置到map中
        Map<String, ObjectMapper> builder = new HashMap<>();
        for (ObjectMapper objectMapper : newObjectMappers) {
            ObjectMapper previous = builder.put(objectMapper.fullPath(), objectMapper);
            if (previous != null) {
                throw new IllegalStateException("duplicate key " + objectMapper.fullPath() + " encountered");
            }
        }

        boolean hasNestedObjects = false;
        // 将 root中有关ObjectMapper的取出来 单独存储到map中
        this.objectMappers = Collections.unmodifiableMap(builder);
        for (ObjectMapper objectMapper : newObjectMappers) {
            if (objectMapper.nested().isNested()) {
                hasNestedObjects = true;
            }
        }
        // 代表 objectMappers 中至少有某个 ObjectMapper是嵌套的
        this.hasNestedObjects = hasNestedObjects;

        try {
            // 将本对象压缩成数据流 并标记反序列化方式为 JSON
            mappingSource = new CompressedXContent(this, XContentType.JSON, ToXContent.EMPTY_PARAMS);
        } catch (Exception e) {
            throw new ElasticsearchGenerationException("failed to serialize source for type [" + type + "]", e);
        }

        // 针对删除操作 会使用一个固定的结果
        final Collection<String> deleteTombstoneMetadataFields = Arrays.asList(VersionFieldMapper.NAME, IdFieldMapper.NAME,
            TypeFieldMapper.NAME, SeqNoFieldMapper.NAME, SeqNoFieldMapper.PRIMARY_TERM_NAME, SeqNoFieldMapper.TOMBSTONE_NAME);

        this.deleteTombstoneMetadataFieldMappers = Stream.of(mapping.metadataMappers)
            .filter(field -> deleteTombstoneMetadataFields.contains(field.name())).toArray(MetadataFieldMapper[]::new);
        // 一个noop操作对应的doc文档会包含这些field
        final Collection<String> noopTombstoneMetadataFields = Arrays.asList(
            VersionFieldMapper.NAME, SeqNoFieldMapper.NAME, SeqNoFieldMapper.PRIMARY_TERM_NAME, SeqNoFieldMapper.TOMBSTONE_NAME);
        this.noopTombstoneMetadataFieldMappers = Stream.of(mapping.metadataMappers)
            .filter(field -> noopTombstoneMetadataFields.contains(field.name())).toArray(MetadataFieldMapper[]::new);
    }

    public Mapping mapping() {
        return mapping;
    }

    public String type() {
        return this.type;
    }

    public Text typeText() {
        return this.typeText;
    }

    public Map<String, Object> meta() {
        return mapping.meta;
    }

    public CompressedXContent mappingSource() {
        return this.mappingSource;
    }

    public RootObjectMapper root() {
        return mapping.root;
    }

    public <T extends MetadataFieldMapper> T metadataMapper(Class<T> type) {
        return mapping.metadataMapper(type);
    }

    public SourceFieldMapper sourceMapper() {
        return metadataMapper(SourceFieldMapper.class);
    }

    public IdFieldMapper idFieldMapper() {
        return metadataMapper(IdFieldMapper.class);
    }

    public RoutingFieldMapper routingFieldMapper() {
        return metadataMapper(RoutingFieldMapper.class);
    }

    public IndexFieldMapper IndexFieldMapper() {
        return metadataMapper(IndexFieldMapper.class);
    }

    public boolean hasNestedObjects() {
        return hasNestedObjects;
    }

    public DocumentFieldMappers mappers() {
        return this.fieldMappers;
    }

    public Map<String, ObjectMapper> objectMappers() {
        return this.objectMappers;
    }

    /**
     *
     * @param source 包含了 field.name=source/indexName/routing等信息
     * @return
     * @throws MapperParsingException
     */
    public ParsedDocument parse(SourceToParse source) throws MapperParsingException {
        // 该对象负责对整个doc进行解析 而针对不同的field就需要不同的fieldMapper对象
        return documentParser.parseDocument(source, mapping.metadataMappers);
    }

    /**
     * 生成一个删除用的doc
     * @param index
     * @param id
     * @return
     * @throws MapperParsingException
     */
    public ParsedDocument createDeleteTombstoneDoc(String index, String id) throws MapperParsingException {
        // 生成了一个 仅包含 id的doc
        final SourceToParse emptySource = new SourceToParse(index, id, new BytesArray("{}"), XContentType.JSON);
        // 通过解析对象解析格式化数据 并将结果包装成 ParsedDocument
        return documentParser.parseDocument(emptySource, deleteTombstoneMetadataFieldMappers).toTombstone();
    }

    /**
     * 生成写入noop操作的 doc
     * @param index
     * @param reason
     * @return
     * @throws MapperParsingException
     */
    public ParsedDocument createNoopTombstoneDoc(String index, String reason) throws MapperParsingException {
        // noop操作是没有id的 只有seq
        final String id = ""; // _id won't be used.
        final SourceToParse sourceToParse = new SourceToParse(index, id, new BytesArray("{}"), XContentType.JSON);
        final ParsedDocument parsedDoc = documentParser.parseDocument(sourceToParse, noopTombstoneMetadataFieldMappers).toTombstone();
        // Store the reason of a noop as a raw string in the _source field
        final BytesRef byteRef = new BytesRef(reason);
        parsedDoc.rootDoc().add(new StoredField(SourceFieldMapper.NAME, byteRef.bytes, byteRef.offset, byteRef.length));
        return parsedDoc;
    }

    /**
     * Returns the best nested {@link ObjectMapper} instances that is in the scope of the specified nested docId.
     */
    public ObjectMapper findNestedObjectMapper(int nestedDocId, SearchContext sc, LeafReaderContext context) throws IOException {
        ObjectMapper nestedObjectMapper = null;
        for (ObjectMapper objectMapper : objectMappers().values()) {
            if (!objectMapper.nested().isNested()) {
                continue;
            }

            Query filter = objectMapper.nestedTypeFilter();
            if (filter == null) {
                continue;
            }
            // We can pass down 'null' as acceptedDocs, because nestedDocId is a doc to be fetched and
            // therefor is guaranteed to be a live doc.
            final Weight nestedWeight = filter.createWeight(sc.searcher(), ScoreMode.COMPLETE_NO_SCORES, 1f);
            Scorer scorer = nestedWeight.scorer(context);
            if (scorer == null) {
                continue;
            }

            if (scorer.iterator().advance(nestedDocId) == nestedDocId) {
                if (nestedObjectMapper == null) {
                    nestedObjectMapper = objectMapper;
                } else {
                    if (nestedObjectMapper.fullPath().length() < objectMapper.fullPath().length()) {
                        nestedObjectMapper = objectMapper;
                    }
                }
            }
        }
        return nestedObjectMapper;
    }

    public DocumentMapper merge(Mapping mapping) {
        Mapping merged = this.mapping.merge(mapping);
        return new DocumentMapper(mapperService, merged);
    }

    /**
     * Recursively update sub field types.
     * 一般是这个容器更新后 去替换原本的旧数据
     */
    public DocumentMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        Mapping updated = this.mapping.updateFieldType(fullNameToFieldType);
        if (updated == this.mapping) {
            // no change
            return this;
        }
        assert updated == updated.updateFieldType(fullNameToFieldType) : "updateFieldType operation is not idempotent";
        return new DocumentMapper(mapperService, updated);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return mapping.toXContent(builder, params);
    }
}
