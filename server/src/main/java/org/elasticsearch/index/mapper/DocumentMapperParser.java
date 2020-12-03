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

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.mapper.MapperRegistry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 该对象负责解析数据流 并生成DocumentMapper对象
 */
public class DocumentMapperParser {

    /**
     * 对应创建该对象的映射服务
     */
    final MapperService mapperService;
    private final NamedXContentRegistry xContentRegistry;
    /**
     * 获取打分对象的服务 可以先忽略
     */
    private final SimilarityService similarityService;
    /**
     * 提供能够在创建分片级 lucene query对象的上下文
     */
    private final Supplier<QueryShardContext> queryShardContextSupplier;

    private final RootObjectMapper.TypeParser rootObjectTypeParser = new RootObjectMapper.TypeParser();

    private final Version indexVersionCreated;

    /**
     * 基于 type字段来匹配不同的 type解析器   因为某些属性可能会携带type字段
     */
    private final Map<String, Mapper.TypeParser> typeParsers;

    /**
     * 该对象是从 mapperRegistry 中取出来的
     */
    private final Map<String, MetadataFieldMapper.TypeParser> rootTypeParsers;

    /**
     * 当MapperService初始化时 会创建该对象
     * @param indexSettings
     * @param mapperService
     * @param xContentRegistry
     * @param similarityService  TODO 打分对象先忽略
     * @param mapperRegistry  该对象存储了与TypeParser的映射关系
     * @param queryShardContextSupplier
     */
    public DocumentMapperParser(IndexSettings indexSettings, MapperService mapperService, NamedXContentRegistry xContentRegistry,
            SimilarityService similarityService, MapperRegistry mapperRegistry, Supplier<QueryShardContext> queryShardContextSupplier) {
        this.mapperService = mapperService;
        this.xContentRegistry = xContentRegistry;
        this.similarityService = similarityService;
        this.queryShardContextSupplier = queryShardContextSupplier;
        this.typeParsers = mapperRegistry.getMapperParsers();
        this.indexVersionCreated = indexSettings.getIndexVersionCreated();
        this.rootTypeParsers = mapperRegistry.getMetadataMapperParsers(indexVersionCreated);
    }

    /**
     * 该对象负责生成解析上下文
     * @return
     */
    public Mapper.TypeParser.ParserContext parserContext() {
        return new Mapper.TypeParser.ParserContext(similarityService::getSimilarity, mapperService,
                typeParsers::get, indexVersionCreated, queryShardContextSupplier);
    }

    /**
     * @param type
     * @param source  内部存储了格式化后的数据流 (同时还采用压缩算法)  比如现在就是一个json字符串
     * @return
     * @throws MapperParsingException
     */
    public DocumentMapper parse(@Nullable String type, CompressedXContent source) throws MapperParsingException {
        Map<String, Object> mapping = null;
        if (source != null) {
            // 将json字符串重新转换成map对象
            Map<String, Object> root = XContentHelper.convertToMap(source.compressedReference(), true, XContentType.JSON).v2();
            // 从root中取出type 对应的值  先假设都可以取到 比如在创建索引时 会将相关的mappings字符串抽取出来
            // 解析后传入到一个 key为 _doc 的容器中 这里就会将type设置成_doc 并进行解析

            // 返回的结果 t1 _doc  t2 _doc 对应的值 (也是一个map)
            Tuple<String, Map<String, Object>> t = extractMapping(type, root);
            type = t.v1();
            mapping = t.v2();
        }
        if (mapping == null) {
            mapping = new HashMap<>();
        }
        return parse(type, mapping);
    }

    /**
     * 针对核心数据进行解析
     * @param type  本次解析的key   默认就是 _doc
     * @param mapping  本次解析的value   _doc 对应的数值
     * @return
     * @throws MapperParsingException
     */
    @SuppressWarnings({"unchecked"})
    private DocumentMapper parse(String type, Map<String, Object> mapping) throws MapperParsingException {
        if (type == null) {
            throw new MapperParsingException("Failed to derive type");
        }

        // 因为此时要开始解析了 生成一个解析用的上下文对象
        Mapper.TypeParser.ParserContext parserContext = parserContext();
        // parse RootObjectMapper
        // 将builder转换成 mapper对象 并设置到 DocMapper.Builder中 还会追加一些 MetadataMapper对象到内部容器中
        DocumentMapper.Builder docBuilder = new DocumentMapper.Builder(
            // 交由最上层的解析对象进行解析   会将一些内置的属性从mapping中转移到 builder
            (RootObjectMapper.Builder) rootObjectTypeParser.parse(type, mapping, parserContext),
            mapperService);

        Iterator<Map.Entry<String, Object>> iterator = mapping.entrySet().iterator();
        // parse DocumentMapper
        while(iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            Object fieldNode = entry.getValue();

            // 剩余的属性首先尝试通过rootTypeParsers来解析
            // 在DocMapper.Builder 内部的mapperService.mapperRegistry.getMetadataMapperParsers 中设置的都是默认值
            // 如果在这里找到了对应的值 则会覆盖之前默认的 MetadataMapper
            MetadataFieldMapper.TypeParser typeParser = rootTypeParsers.get(fieldName);
            if (typeParser != null) {
                iterator.remove();
                if (false == fieldNode instanceof Map) {
                    throw new IllegalArgumentException("[_parent] must be an object containing [type]");
                }
                Map<String, Object> fieldNodeMap = (Map<String, Object>) fieldNode;

                // 也是将相关属性填充到 builder中
                docBuilder.put(typeParser.parse(fieldName, fieldNodeMap, parserContext));
                fieldNodeMap.remove("type");
                // 确保fieldNodeMap中所有 entry都被使用完
                checkNoRemainingFields(fieldName, fieldNodeMap, parserContext.indexVersionCreated());
            }
        }

        // 尝试获取元数据信息
        Map<String, Object> meta = (Map<String, Object>) mapping.remove("_meta");
        if (meta != null) {
            /*
             * It may not be required to copy meta here to maintain immutability but the cost is pretty low here.
             *
             * Note: this copy can not be replaced by Map#copyOf because we rely on consistent serialization order since we do byte-level
             * checks on the mapping between what we receive from the master and what we have locally. As Map#copyOf is not necessarily
             * the same underlying map implementation, we could end up with a different iteration order. For reference, see
             * MapperService#assertSerializtion and GitHub issues #10302 and #10318.
             *
             * Do not change this to Map#copyOf or any other method of copying meta that could change the iteration order.
             *
             * TODO:
             *  - this should almost surely be a copy as a LinkedHashMap to have the ordering guarantees that we are relying on
             *  - investigate the above note about whether or not we really need to be copying here, the ideal outcome would be to not
             */
            docBuilder.meta(Collections.unmodifiableMap(new HashMap<>(meta)));
        }

        // 在json结构的根级 不应该存在其他字段了
        checkNoRemainingFields(mapping, parserContext.indexVersionCreated(), "Root mapping definition has unsupported parameters: ");

        // 通过docBuilder 生成docMapper
        return docBuilder.build(mapperService);
    }

    public static void checkNoRemainingFields(String fieldName, Map<?, ?> fieldNodeMap, Version indexVersionCreated) {
        checkNoRemainingFields(fieldNodeMap, indexVersionCreated,
                "Mapping definition for [" + fieldName + "] has unsupported parameters: ");
    }

    public static void checkNoRemainingFields(Map<?, ?> fieldNodeMap, Version indexVersionCreated, String message) {
        if (!fieldNodeMap.isEmpty()) {
            throw new MapperParsingException(message + getRemainingFields(fieldNodeMap));
        }
    }

    private static String getRemainingFields(Map<?, ?> map) {
        StringBuilder remainingFields = new StringBuilder();
        for (Object key : map.keySet()) {
            remainingFields.append(" [").append(key).append(" : ").append(map.get(key)).append("]");
        }
        return remainingFields.toString();
    }

    private Tuple<String, Map<String, Object>> extractMapping(String type, String source) throws MapperParsingException {
        Map<String, Object> root;
        try (XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, source)) {
            root = parser.mapOrdered();
        } catch (Exception e) {
            throw new MapperParsingException("failed to parse mapping definition", e);
        }
        return extractMapping(type, root);
    }

    /**
     * Given an optional type name and mapping definition, returns the type and a normalized form of the mappings.
     *
     * The provided mapping definition may or may not contain the type name as the root key in the map. This method
     * attempts to unwrap the mappings, so that they no longer contain a type name at the root. If no type name can
     * be found, through either the 'type' parameter or by examining the provided mappings, then an exception will be
     * thrown.
     *
     * @param type An optional type name.
     * @param root The mapping definition.
     *
     * @return A tuple of the form (type, normalized mappings).
     * 从root中找到 type对应的值
     */
    @SuppressWarnings({"unchecked"})
    private Tuple<String, Map<String, Object>> extractMapping(String type, Map<String, Object> root) throws MapperParsingException {
        if (root.size() == 0) {
            if (type != null) {
                return new Tuple<>(type, root);
            } else {
                throw new MapperParsingException("malformed mapping, no type name found");
            }
        }

        String rootName = root.keySet().iterator().next();
        Tuple<String, Map<String, Object>> mapping;
        if (type == null || type.equals(rootName) || mapperService.resolveDocumentType(type).equals(rootName)) {
            // 这里会假定 取出来的值还是一个map
            mapping = new Tuple<>(rootName, (Map<String, Object>) root.get(rootName));
        } else {
            // 直接返回 kv
            mapping = new Tuple<>(type, root);
        }
        return mapping;
    }

    NamedXContentRegistry getXContentRegistry() {
        return xContentRegistry;
    }
}
