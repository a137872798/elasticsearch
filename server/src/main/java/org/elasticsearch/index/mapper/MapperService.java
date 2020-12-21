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

import com.carrotsearch.hppc.ObjectHashSet;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.elasticsearch.Assertions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.ReloadableCustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.InvalidTypeNameException;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * 映射服务  作为一个管理json字符串映射的门面对象 内部关联了各种映射需要的组件
 * 每当创建一个indexService后 会对应一个MapperService
 */
public class MapperService extends AbstractIndexComponent implements Closeable {

    /**
     * The reason why a mapping is being merged.
     */
    public enum MergeReason {
        /**
         * Pre-flight check before sending a mapping update to the master
         * 在发送请求前要检查是否需要更新
         */
        MAPPING_UPDATE_PREFLIGHT,
        /**
         * Create or update a mapping.
         * 更新传入的mapping信息
         */
        MAPPING_UPDATE,
        /**
         * Recovery of an existing mapping, for instance because of a restart,
         * if a shard was moved to a different node or for administrative
         * purposes.
         * 恢复一个存在的 mapping
         */
        MAPPING_RECOVERY;
    }

    public static final String SINGLE_MAPPING_NAME = "_doc";
    public static final Setting<Long> INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING =
        Setting.longSetting("index.mapping.nested_fields.limit", 50L, 0, Property.Dynamic, Property.IndexScope);
    // maximum allowed number of nested json objects across all fields in a single document
    public static final Setting<Long> INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING =
        Setting.longSetting("index.mapping.nested_objects.limit", 10000L, 0, Property.Dynamic, Property.IndexScope);
    public static final Setting<Long> INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING =
        Setting.longSetting("index.mapping.total_fields.limit", 1000L, 0, Property.Dynamic, Property.IndexScope);
    public static final Setting<Long> INDEX_MAPPING_DEPTH_LIMIT_SETTING =
        Setting.longSetting("index.mapping.depth.limit", 20L, 1, Property.Dynamic, Property.IndexScope);
    public static final Setting<Long> INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING =
        Setting.longSetting("index.mapping.field_name_length.limit", Long.MAX_VALUE, 1L, Property.Dynamic, Property.IndexScope);

    //TODO this needs to be cleaned up: _timestamp and _ttl are not supported anymore, _field_names, _seq_no, _version and _source are
    //also missing, not sure if on purpose. See IndicesModule#getMetadataMappers
    // 这些内置的field 不允许从结构化数据中解析出来
    private static final String[] SORTED_META_FIELDS = new String[]{
        "_id", IgnoredFieldMapper.NAME, "_index", "_nested_path", "_routing", "_size", "_timestamp", "_ttl", "_type"
    };

    private static final ObjectHashSet<String> META_FIELDS = ObjectHashSet.from(SORTED_META_FIELDS);

    /**
     * 该对象内存储了各种analyzer
     */
    private final IndexAnalyzers indexAnalyzers;

    private volatile DocumentMapper mapper;

    /**
     * 根据fieldName 可以找到fieldType
     */
    private volatile FieldTypeLookup fieldTypes;
    /**
     * 以 ObjectMapper.fullPath 为key   value存储mapper对象
     */
    private volatile Map<String, ObjectMapper> fullPathObjectMappers = emptyMap();

    /**
     * 默认非嵌套 当发现了嵌套数据时 会更新
     */
    private boolean hasNested = false; // updated dynamically to true when a nested object is added

    private final DocumentMapperParser documentParser;

    // 这3个 analyzers 对象可以在解析不同的field时使用不同的analyzer
    private final MapperAnalyzerWrapper indexAnalyzer;
    private final MapperAnalyzerWrapper searchAnalyzer;
    private final MapperAnalyzerWrapper searchQuoteAnalyzer;

    private volatile Map<String, MappedFieldType> unmappedFieldTypes = emptyMap();

    /**
     * 根据不同的name 能够映射到不同的typeParser上
     */
    final MapperRegistry mapperRegistry;

    private final BooleanSupplier idFieldDataEnabled;

    /**
     * 初始化映射服务对象
     * @param indexAnalyzers 内部包含了各种分词器对象   分词器本身是以index为单位进行划分的
     * @param mapperRegistry 内部存储了各种映射关系
     * @param queryShardContextSupplier 在查询前需要生成一个 QueryShardContext对象
     */
    public MapperService(IndexSettings indexSettings, IndexAnalyzers indexAnalyzers, NamedXContentRegistry xContentRegistry,
                         SimilarityService similarityService, MapperRegistry mapperRegistry,
                         Supplier<QueryShardContext> queryShardContextSupplier, BooleanSupplier idFieldDataEnabled) {
        super(indexSettings);
        this.indexAnalyzers = indexAnalyzers;
        // 初始阶段内部只是几个空容器
        this.fieldTypes = new FieldTypeLookup();
        // 用相关参数生成 docMapperParser
        this.documentParser = new DocumentMapperParser(indexSettings, this, xContentRegistry, similarityService, mapperRegistry,
                queryShardContextSupplier);

        // 这个wrapper的作用是 先尝试用 MapperFieldType本身指定的analyzer 如果不存在则使用默认 analyzer
        this.indexAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultIndexAnalyzer(), p -> p.indexAnalyzer());
        this.searchAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultSearchAnalyzer(), p -> p.searchAnalyzer());
        this.searchQuoteAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultSearchQuoteAnalyzer(), p -> p.searchQuoteAnalyzer());
        this.mapperRegistry = mapperRegistry;
        this.idFieldDataEnabled = idFieldDataEnabled;
    }

    public boolean hasNested() {
        return this.hasNested;
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.indexAnalyzers;
    }

    public NamedAnalyzer getNamedAnalyzer(String analyzerName) {
        return this.indexAnalyzers.get(analyzerName);
    }

    public DocumentMapperParser documentMapperParser() {
        return this.documentParser;
    }

    /**
     * Parses the mappings (formatted as JSON) into a map
     * 将json内部的数据转换成一个map 并返回
     */
    public static Map<String, Object> parseMapping(NamedXContentRegistry xContentRegistry, String mappingSource) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, mappingSource)) {
            return parser.map();
        }
    }

    /**
     * Update mapping by only merging the metadata that is different between received and stored entries
     * 由于索引元数据的变化导致mapping的更新
     */
    public boolean updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) throws IOException {
        assert newIndexMetadata.getIndex().equals(index()) : "index mismatch: expected " + index()
            + " but was " + newIndexMetadata.getIndex();

        final DocumentMapper updatedMapper;
        try {
            // only update entries if needed
            // 更新之前的 docMapper对象
            updatedMapper = internalMerge(newIndexMetadata, MergeReason.MAPPING_RECOVERY, true);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to apply mappings", index()), e);
            throw e;
        }

        if (updatedMapper == null) {
            return false;
        }

        boolean requireRefresh = false;

        assertMappingVersion(currentIndexMetadata, newIndexMetadata, updatedMapper);

        MappingMetadata mappingMetadata = newIndexMetadata.mapping();
        CompressedXContent incomingMappingSource = mappingMetadata.source();

        String op = mapper != null ? "updated" : "added";
        if (logger.isDebugEnabled() && incomingMappingSource.compressed().length < 512) {
            logger.debug("[{}] {} mapping, source [{}]", index(), op, incomingMappingSource.string());
        } else if (logger.isTraceEnabled()) {
            logger.trace("[{}] {} mapping, source [{}]", index(), op, incomingMappingSource.string());
        } else {
            logger.debug("[{}] {} mapping (source suppressed due to length, use TRACE level if needed)",
                index(), op);
        }

        // refresh mapping can happen when the parsing/merging of the mapping from the metadata doesn't result in the same
        // mapping, in this case, we send to the master to refresh its own version of the mappings (to conform with the
        // merge version of it, which it does when refreshing the mappings), and warn log it.
        if (documentMapper().mappingSource().equals(incomingMappingSource) == false) {
            logger.debug("[{}] parsed mapping, and got different sources\noriginal:\n{}\nparsed:\n{}",
                index(), incomingMappingSource, documentMapper().mappingSource());

            requireRefresh = true;
        }


        return requireRefresh;
    }

    private void assertMappingVersion(
            final IndexMetadata currentIndexMetadata,
            final IndexMetadata newIndexMetadata,
            final DocumentMapper updatedMapper) {
        if (Assertions.ENABLED && currentIndexMetadata != null) {
            if (currentIndexMetadata.getMappingVersion() == newIndexMetadata.getMappingVersion()) {
                // if the mapping version is unchanged, then there should not be any updates and all mappings should be the same
                assert updatedMapper == mapper;

                MappingMetadata mapping = newIndexMetadata.mapping();
                if (mapping != null) {
                    final CompressedXContent currentSource = currentIndexMetadata.mapping().source();
                    final CompressedXContent newSource = mapping.source();
                    assert currentSource.equals(newSource) :
                            "expected current mapping [" + currentSource + "] for type [" + mapping.type() + "] "
                                    + "to be the same as new mapping [" + newSource + "]";
                }

            } else {
                // if the mapping version is changed, it should increase, there should be updates, and the mapping should be different
                final long currentMappingVersion = currentIndexMetadata.getMappingVersion();
                final long newMappingVersion = newIndexMetadata.getMappingVersion();
                assert currentMappingVersion < newMappingVersion :
                        "expected current mapping version [" + currentMappingVersion + "] "
                                + "to be less than new mapping version [" + newMappingVersion + "]";
                assert updatedMapper != null;
                final MappingMetadata currentMapping = currentIndexMetadata.mapping();
                if (currentMapping != null) {
                    final CompressedXContent currentSource = currentMapping.source();
                    final CompressedXContent newSource = updatedMapper.mappingSource();
                    assert currentSource.equals(newSource) == false :
                        "expected current mapping [" + currentSource + "] to be different than new mapping";
                }
            }
        }
    }

    /**
     *
     * @param type
     * @param mappings  当创建索引时 在req中会携带一个json格式的字符串 需要处理的应该就是这个字符串解析后的数据
     *                  解析完成后会变成一个map对象
     * @param reason
     * @throws IOException
     */
    public void merge(String type, Map<String, Object> mappings, MergeReason reason) throws IOException {
        // 这里是将 mappings又重新转换成 json结构体存储在 XContentBuilder对象中 方便随时对该结构体进行修改
        // Strings.toString() 会将结构体变回 json字符串
        CompressedXContent content = new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(mappings)));
        // 将解析出来的结果与MapperService原有的数据合并后 更新
        internalMerge(type, content, reason);
    }

    public void merge(IndexMetadata indexMetadata, MergeReason reason) {
        internalMerge(indexMetadata, reason, false);
    }

    public DocumentMapper merge(String type, CompressedXContent mappingSource, MergeReason reason) {
        return internalMerge(type, mappingSource, reason);
    }

    /**
     *
     * @param indexMetadata  根据该元数据的信息更新当前的docMapper
     * @param reason
     * @param onlyUpdateIfNeeded 只有必须要更新时才更新 这里指的必须更新是 DocMapper还未创建 或者 前后2个MappingSource不同
     * @return
     */
    private synchronized DocumentMapper internalMerge(IndexMetadata indexMetadata,
                                                                   MergeReason reason, boolean onlyUpdateIfNeeded) {
        assert reason != MergeReason.MAPPING_UPDATE_PREFLIGHT;
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        // 首先新的索引元数据 必须包含mapping 才有更新的必要
        if (mappingMetadata != null) {
            if (onlyUpdateIfNeeded) {
                // 该对象被初始化时 还未创建DocMapper对象
                DocumentMapper existingMapper = documentMapper();
                // 也就是当数据流发生变化时 会进行merge
                if (existingMapper == null || mappingMetadata.source().equals(existingMapper.mappingSource()) == false) {
                    return internalMerge(mappingMetadata.type(), mappingMetadata.source(), reason);
                }
            } else {
                return internalMerge(mappingMetadata.type(), mappingMetadata.source(), reason);
            }
        }
        return null;
    }

    /**
     * 将内部数据 与 传入的mappings进行合并
     * @param type
     * @param mappings  原本在创建index时 会传入一个json字符串 并且会和index匹配的template的mappings进行合并
     *                  该属性就可以看作是合并后的结果
     * @param reason
     * @return
     */
    private synchronized DocumentMapper internalMerge(String type, CompressedXContent mappings, MergeReason reason) {

        DocumentMapper documentMapper;

        try {
            // 根据此时的数据流重新生成一个 DocMapper
            documentMapper = documentParser.parse(type, mappings);
        } catch (Exception e) {
            throw new MapperParsingException("Failed to parse mapping: {}", e, e.getMessage());
        }

        return internalMerge(documentMapper, reason);
    }

    static void validateTypeName(String type) {
        if (type.length() == 0) {
            throw new InvalidTypeNameException("mapping type name is empty");
        }
        if (type.length() > 255) {
            throw new InvalidTypeNameException("mapping type name [" + type + "] is too long; limit is length 255 but was ["
                + type.length() + "]");
        }
        if (type.charAt(0) == '_' && SINGLE_MAPPING_NAME.equals(type) == false) {
            throw new InvalidTypeNameException("mapping type name [" + type + "] can't start with '_' unless it is called ["
                + SINGLE_MAPPING_NAME + "]");
        }
        if (type.contains("#")) {
            throw new InvalidTypeNameException("mapping type name [" + type + "] should not include '#' in it");
        }
        if (type.contains(",")) {
            throw new InvalidTypeNameException("mapping type name [" + type + "] should not include ',' in it");
        }
        if (type.charAt(0) == '.') {
            throw new IllegalArgumentException("mapping type name [" + type + "] must not start with a '.'");
        }
    }

    /**
     * 当抽取了用户传入的json字符串中相关信息 并转换成Mapper后 会生成一个 DocumentMapper对象
     * 而每个index 会对应一个 mapperService 这里就是将本次传入的新的mapper 与之前旧的mapper进行合并
     * @param mapper
     * @param reason
     * @return
     */
    private synchronized DocumentMapper internalMerge(DocumentMapper mapper, MergeReason reason) {
        // MapperService在 index级别的数据是共享的  所以设置的属性在解析相同index数据时可以复用
        // 默认情况 hasNested为false
        boolean hasNested = this.hasNested;
        // 初始状态为emptyMap
        Map<String, ObjectMapper> fullPathObjectMappers = this.fullPathObjectMappers;
        // 这个对象是通过名字查找 MappedFieldType  默认为空
        FieldTypeLookup fieldTypes = this.fieldTypes;

        assert mapper != null;
        // check naming
        validateTypeName(mapper.type());

        // compute the merged DocumentMapper
        // 默认情况下 内部的mapper为空
        DocumentMapper oldMapper = this.mapper;
        DocumentMapper newMapper;
        if (oldMapper != null) {
            newMapper = oldMapper.merge(mapper.mapping());
        } else {
            newMapper = mapper;
        }

        // check basic sanity of the new mapping
        List<ObjectMapper> objectMappers = new ArrayList<>();
        List<FieldMapper> fieldMappers = new ArrayList<>();
        List<FieldAliasMapper> fieldAliasMappers = new ArrayList<>();
        MetadataFieldMapper[] metadataMappers = newMapper.mapping().metadataMappers;
        Collections.addAll(fieldMappers, metadataMappers);
        // 将root的各种mapper 归类填充到不同的容器中
        MapperUtils.collect(newMapper.mapping().root(), objectMappers, fieldMappers, fieldAliasMappers);

        // 这里对各级mapper 进行校验
        MapperMergeValidator.validateNewMappers(objectMappers, fieldMappers, fieldAliasMappers, fieldTypes);
        // 约束性校验
        checkPartitionedIndexConstraints(newMapper);

        // update lookup data-structures
        // 将mapper数据填充到 fieldType中
        fieldTypes = fieldTypes.copyAndAddAll(fieldMappers, fieldAliasMappers);

        for (ObjectMapper objectMapper : objectMappers) {
            // 第一次循环进入这里
            if (fullPathObjectMappers == this.fullPathObjectMappers) {
                // first time through the loops
                fullPathObjectMappers = new HashMap<>(this.fullPathObjectMappers);
            }
            fullPathObjectMappers.put(objectMapper.fullPath(), objectMapper);

            // 当有某个 objectMapper 是嵌套的  更新hasNested标识
            if (objectMapper.nested().isNested()) {
                hasNested = true;
            }
        }

        // TODO 校验性代码忽略
        MapperMergeValidator.validateFieldReferences(fieldMappers, fieldAliasMappers,
            fullPathObjectMappers, fieldTypes);

        ContextMapping.validateContextPaths(indexSettings.getIndexVersionCreated(), fieldMappers, fieldTypes::get);

        if (reason == MergeReason.MAPPING_UPDATE || reason == MergeReason.MAPPING_UPDATE_PREFLIGHT) {
            // this check will only be performed on the master node when there is
            // a call to the update mapping API. For all other cases like
            // the master node restoring mappings from disk or data nodes
            // deserializing cluster state that was sent by the master node,
            // this check will be skipped.
            // Also, don't take metadata mappers into account for the field limit check
            // TODO 校验性代码先忽略
            checkTotalFieldsLimit(objectMappers.size() + fieldMappers.size() - metadataMappers.length
                + fieldAliasMappers.size() );
            checkFieldNameSoftLimit(objectMappers, fieldMappers, fieldAliasMappers);
        }

        if (reason == MergeReason.MAPPING_UPDATE || reason == MergeReason.MAPPING_UPDATE_PREFLIGHT) {
            // this check will only be performed on the master node when there is
            // a call to the update mapping API. For all other cases like
            // the master node restoring mappings from disk or data nodes
            // deserializing cluster state that was sent by the master node,
            // this check will be skipped.
            checkNestedFieldsLimit(fullPathObjectMappers);
            checkDepthLimit(fullPathObjectMappers.keySet());
        }
        checkIndexSortCompatibility(indexSettings.getIndexSortConfig(), hasNested);

        // 代表合并后的 mapper对象 如果是首次调用该方法 就是首次传入的mapper对象
        if (newMapper != null) {
            // 使用merge后的 fieldTypesLookup对象 去更新mapper内部的数据
            DocumentMapper updatedDocumentMapper = newMapper.updateFieldType(fieldTypes.fullNameToFieldType);
            if (updatedDocumentMapper != newMapper) {
                newMapper = updatedDocumentMapper;
            }
        }

        // 这种MergeReason  不会修改该对象内部的字段
        if (reason == MergeReason.MAPPING_UPDATE_PREFLIGHT) {
            return newMapper;
        }

        // only need to immutably rewrap these if the previous reference was changed.
        // if not then they are already implicitly immutable.
        if (fullPathObjectMappers != this.fullPathObjectMappers) {
            fullPathObjectMappers = Collections.unmodifiableMap(fullPathObjectMappers);
        }

        // 将最新数据覆盖到 mapperService内部的属性中
        // commit the change
        if (newMapper != null) {
            this.mapper = newMapper;
        }
        this.fieldTypes = fieldTypes;
        this.hasNested = hasNested;
        this.fullPathObjectMappers = fullPathObjectMappers;

        assert assertMappersShareSameFieldType();
        assert newMapper == null || assertSerialization(newMapper);

        return newMapper;
    }

    private boolean assertMappersShareSameFieldType() {
        if (mapper != null) {
            List<FieldMapper> fieldMappers = new ArrayList<>();
            Collections.addAll(fieldMappers, mapper.mapping().metadataMappers);
            MapperUtils.collect(mapper.root(), new ArrayList<>(), fieldMappers, new ArrayList<>());
            for (FieldMapper fieldMapper : fieldMappers) {
                assert fieldMapper.fieldType() == fieldTypes.get(fieldMapper.name()) : fieldMapper.name();
            }
        }
        return true;
    }

    private boolean assertSerialization(DocumentMapper mapper) {
        // capture the source now, it may change due to concurrent parsing
        final CompressedXContent mappingSource = mapper.mappingSource();
        DocumentMapper newMapper = parse(mapper.type(), mappingSource);

        if (newMapper.mappingSource().equals(mappingSource) == false) {
            throw new IllegalStateException("DocumentMapper serialization result is different from source. \n--> Source ["
                + mappingSource + "]\n--> Result ["
                + newMapper.mappingSource() + "]");
        }
        return true;
    }

    private void checkNestedFieldsLimit(Map<String, ObjectMapper> fullPathObjectMappers) {
        long allowedNestedFields = indexSettings.getValue(INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING);
        long actualNestedFields = 0;
        for (ObjectMapper objectMapper : fullPathObjectMappers.values()) {
            if (objectMapper.nested().isNested()) {
                actualNestedFields++;
            }
        }
        if (actualNestedFields > allowedNestedFields) {
            throw new IllegalArgumentException("Limit of nested fields [" + allowedNestedFields + "] in index [" + index().getName()
                + "] has been exceeded");
        }
    }

    private void checkTotalFieldsLimit(long totalMappers) {
        long allowedTotalFields = indexSettings.getValue(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING);
        if (allowedTotalFields < totalMappers) {
            throw new IllegalArgumentException("Limit of total fields [" + allowedTotalFields + "] in index [" + index().getName()
                + "] has been exceeded");
        }
    }

    private void checkDepthLimit(Collection<String> objectPaths) {
        final long maxDepth = indexSettings.getValue(INDEX_MAPPING_DEPTH_LIMIT_SETTING);
        for (String objectPath : objectPaths) {
            checkDepthLimit(objectPath, maxDepth);
        }
    }

    private void checkDepthLimit(String objectPath, long maxDepth) {
        int numDots = 0;
        for (int i = 0; i < objectPath.length(); ++i) {
            if (objectPath.charAt(i) == '.') {
                numDots += 1;
            }
        }
        final int depth = numDots + 2;
        if (depth > maxDepth) {
            throw new IllegalArgumentException("Limit of mapping depth [" + maxDepth + "] in index [" + index().getName()
                    + "] has been exceeded due to object field [" + objectPath + "]");
        }
    }

    private void checkFieldNameSoftLimit(Collection<ObjectMapper> objectMappers,
                                         Collection<FieldMapper> fieldMappers,
                                         Collection<FieldAliasMapper> fieldAliasMappers) {
        final long maxFieldNameLength = indexSettings.getValue(INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING);

        Stream.of(objectMappers.stream(), fieldMappers.stream(), fieldAliasMappers.stream())
            .reduce(Stream::concat)
            .orElseGet(Stream::empty)
            .forEach(mapper -> {
                String name = mapper.simpleName();
                if (name.length() > maxFieldNameLength) {
                    throw new IllegalArgumentException("Field name [" + name + "] in index [" + index().getName() +
                        "] is too long. The limit is set to [" + maxFieldNameLength + "] characters but was ["
                        + name.length() + "] characters");
                }
            });
    }

    /**
     * 检测索引分区约束性条件
     * @param newMapper
     */
    private void checkPartitionedIndexConstraints(DocumentMapper newMapper) {
        // 当settings 要求设置 而mapper中没有时 抛出异常
        if (indexSettings.getIndexMetadata().isRoutingPartitionedIndex()) {
            if (!newMapper.routingFieldMapper().required()) {
                throw new IllegalArgumentException("mapping type [" + newMapper.type() + "] must have routing "
                        + "required for partitioned index [" + indexSettings.getIndex().getName() + "]");
            }
        }
    }

    private static void checkIndexSortCompatibility(IndexSortConfig sortConfig, boolean hasNested) {
        if (sortConfig.hasIndexSort() && hasNested) {
            throw new IllegalArgumentException("cannot have nested fields when index sort is activated");
        }
    }

    public DocumentMapper parse(String mappingType, CompressedXContent mappingSource) throws MapperParsingException {
        return documentParser.parse(mappingType, mappingSource);
    }

    /**
     * Return the document mapper, or {@code null} if no mapping has been put yet.
     */
    public DocumentMapper documentMapper() {
        return mapper;
    }

    /**
     * Returns {@code true} if the given {@code mappingSource} includes a type
     * as a top-level object.
     */
    public static boolean isMappingSourceTyped(String type, Map<String, Object> mapping) {
        return mapping.size() == 1 && mapping.keySet().iterator().next().equals(type);
    }

    /**
     * Resolves a type from a mapping-related request into the type that should be used when
     * merging and updating mappings.
     *
     * If the special `_doc` type is provided, then we replace it with the actual type that is
     * being used in the mappings. This allows typeless APIs such as 'index' or 'put mappings'
     * to work against indices with a custom type name.
     */
    public String resolveDocumentType(String type) {
        if (MapperService.SINGLE_MAPPING_NAME.equals(type)) {
            if (mapper != null) {
                return mapper.type();
            }
        }
        return type;
    }

    /**
     * Returns the document mapper for this MapperService.  If no mapper exists,
     * creates one and returns that.
     * 自动创建docMapper对象
     */
    public DocumentMapperForType documentMapperWithAutoCreate() {
        DocumentMapper mapper = documentMapper();
        if (mapper != null) {
            return new DocumentMapperForType(mapper, null);
        }
        mapper = parse(SINGLE_MAPPING_NAME, null);
        return new DocumentMapperForType(mapper, mapper.mapping());
    }

    /**
     * Given the full name of a field, returns its {@link MappedFieldType}.
     */
    public MappedFieldType fieldType(String fullName) {
        return fieldTypes.get(fullName);
    }

    /**
     * Returns all the fields that match the given pattern. If the pattern is prefixed with a type
     * then the fields will be returned with a type prefix.
     * 使用通配符找到匹配的所有field
     */
    public Set<String> simpleMatchToFullName(String pattern) {
        if (Regex.isSimpleMatchPattern(pattern) == false) {
            // no wildcards
            // 非通配符 代表传入的参数就是fieldName
            return Collections.singleton(pattern);
        }
        return fieldTypes.simpleMatchToFullName(pattern);
    }

    /**
     * Returns all mapped field types.
     */
    public Iterable<MappedFieldType> fieldTypes() {
        return fieldTypes;
    }

    public ObjectMapper getObjectMapper(String name) {
        return fullPathObjectMappers.get(name);
    }

    /**
     * Given a type (eg. long, string, ...), return an anonymous field mapper that can be used for search operations.
     */
    public MappedFieldType unmappedFieldType(String type) {
        MappedFieldType fieldType = unmappedFieldTypes.get(type);
        if (fieldType == null) {
            final Mapper.TypeParser.ParserContext parserContext = documentMapperParser().parserContext();
            Mapper.TypeParser typeParser = parserContext.typeParser(type);
            if (typeParser == null) {
                throw new IllegalArgumentException("No mapper found for type [" + type + "]");
            }
            final Mapper.Builder<?, ?> builder = typeParser.parse("__anonymous_" + type, emptyMap(), parserContext);
            final BuilderContext builderContext = new BuilderContext(indexSettings.getSettings(), new ContentPath(1));
            fieldType = ((FieldMapper)builder.build(builderContext)).fieldType();

            // There is no need to synchronize writes here. In the case of concurrent access, we could just
            // compute some mappers several times, which is not a big deal
            Map<String, MappedFieldType> newUnmappedFieldTypes = new HashMap<>(unmappedFieldTypes);
            newUnmappedFieldTypes.put(type, fieldType);
            unmappedFieldTypes = unmodifiableMap(newUnmappedFieldTypes);
        }
        return fieldType;
    }

    public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    public Analyzer searchQuoteAnalyzer() {
        return this.searchQuoteAnalyzer;
    }

    /**
     * Returns <code>true</code> if fielddata is enabled for the {@link IdFieldMapper} field, <code>false</code> otherwise.
     */
    public boolean isIdFieldDataEnabled() {
        return idFieldDataEnabled.getAsBoolean();
    }

    @Override
    public void close() throws IOException {
        indexAnalyzers.close();
    }

    /**
     * @return Whether a field is a metadata field.
     */
    public static boolean isMetadataField(String fieldName) {
        return META_FIELDS.contains(fieldName);
    }

    public static String[] getAllMetaFields() {
        return Arrays.copyOf(SORTED_META_FIELDS, SORTED_META_FIELDS.length);
    }

    /** An analyzer wrapper that can lookup fields within the index mappings */
    final class MapperAnalyzerWrapper extends DelegatingAnalyzerWrapper {

        private final Analyzer defaultAnalyzer;
        private final Function<MappedFieldType, Analyzer> extractAnalyzer;

        MapperAnalyzerWrapper(Analyzer defaultAnalyzer, Function<MappedFieldType, Analyzer> extractAnalyzer) {
            super(Analyzer.PER_FIELD_REUSE_STRATEGY);
            this.defaultAnalyzer = defaultAnalyzer;
            this.extractAnalyzer = extractAnalyzer;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            MappedFieldType fieldType = fieldType(fieldName);
            if (fieldType != null) {
                Analyzer analyzer = extractAnalyzer.apply(fieldType);
                if (analyzer != null) {
                    return analyzer;
                }
            }
            return defaultAnalyzer;
        }
    }

    public synchronized List<String> reloadSearchAnalyzers(AnalysisRegistry registry) throws IOException {
        logger.info("reloading search analyzers");
        // refresh indexAnalyzers and search analyzers
        final Map<String, TokenizerFactory> tokenizerFactories = registry.buildTokenizerFactories(indexSettings);
        final Map<String, CharFilterFactory> charFilterFactories = registry.buildCharFilterFactories(indexSettings);
        final Map<String, TokenFilterFactory> tokenFilterFactories = registry.buildTokenFilterFactories(indexSettings);
        final Map<String, Settings> settings = indexSettings.getSettings().getGroups("index.analysis.analyzer");
        final List<String> reloadedAnalyzers = new ArrayList<>();
        for (NamedAnalyzer namedAnalyzer : indexAnalyzers.getAnalyzers().values()) {
            if (namedAnalyzer.analyzer() instanceof ReloadableCustomAnalyzer) {
                ReloadableCustomAnalyzer analyzer = (ReloadableCustomAnalyzer) namedAnalyzer.analyzer();
                String analyzerName = namedAnalyzer.name();
                Settings analyzerSettings = settings.get(analyzerName);
                analyzer.reload(analyzerName, analyzerSettings, tokenizerFactories, charFilterFactories, tokenFilterFactories);
                reloadedAnalyzers.add(analyzerName);
            }
        }
        return reloadedAnalyzers;
    }

}
