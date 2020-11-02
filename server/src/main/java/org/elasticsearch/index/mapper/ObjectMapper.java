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

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 之前看到的mapper都是针对field   而且都包含一个parse方法
 * 那么ObjectMapper 是指不需要做解析么
 */
public class ObjectMapper extends Mapper implements Cloneable {
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(ObjectMapper.class));

    public static final String CONTENT_TYPE = "object";
    public static final String NESTED_CONTENT_TYPE = "nested";

    public static class Defaults {
        public static final boolean ENABLED = true;
        public static final Nested NESTED = Nested.NO;
        public static final Dynamic DYNAMIC = null; // not set, inherited from root
    }

    public enum Dynamic {
        TRUE,
        FALSE,
        /**
         * 严格的???
         */
        STRICT
    }

    /**
     * 仅仅是描述一种嵌套的状态 比如是否开启嵌套 是否包含parent/root
     */
    public static class Nested {

        public static final Nested NO = new Nested(false, false, false);

        public static Nested newNested(boolean includeInParent, boolean includeInRoot) {
            return new Nested(true, includeInParent, includeInRoot);
        }

        private final boolean nested;

        /**
         * 代表需要将嵌套doc的field 转移到 parentDoc中
         */
        private final boolean includeInParent;

        /**
         * 代表嵌套doc的field 需要转移到 rootDoc中
         */
        private final boolean includeInRoot;

        private Nested(boolean nested, boolean includeInParent, boolean includeInRoot) {
            this.nested = nested;
            this.includeInParent = includeInParent;
            this.includeInRoot = includeInRoot;
        }

        public boolean isNested() {
            return nested;
        }

        public boolean isIncludeInParent() {
            return includeInParent;
        }

        public boolean isIncludeInRoot() {
            return includeInRoot;
        }
    }

    /**
     * 在每个mapper中 都有一个对应的builder对象 用于构建mapper
     * @param <T>
     * @param <Y>
     */
    @SuppressWarnings("rawtypes")
    public static class Builder<T extends Builder, Y extends ObjectMapper> extends Mapper.Builder<T, Y> {

        protected boolean enabled = Defaults.ENABLED;

        protected Nested nested = Defaults.NESTED;

        protected Dynamic dynamic = Defaults.DYNAMIC;

        /**
         * 该容器内部的属性是从外部插入的
         */
        protected final List<Mapper.Builder> mappersBuilders = new ArrayList<>();

        @SuppressWarnings("unchecked")
        public Builder(String name) {
            super(name);
            this.builder = (T) this;
        }

        public T enabled(boolean enabled) {
            this.enabled = enabled;
            return builder;
        }

        public T dynamic(Dynamic dynamic) {
            this.dynamic = dynamic;
            return builder;
        }

        public T nested(Nested nested) {
            this.nested = nested;
            return builder;
        }

        public T add(Mapper.Builder builder) {
            mappersBuilders.add(builder);
            return this.builder;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Y build(BuilderContext context) {
            // 套路都是一样的 在使用前都要在path上追加什么 之后在builder后 又将name移除
            context.path().add(name);

            Map<String, Mapper> mappers = new HashMap<>();
            // 在解析数据流时 会将一些builder设置进去
            for (Mapper.Builder builder : mappersBuilders) {
                Mapper mapper = builder.build(context);
                Mapper existing = mappers.get(mapper.simpleName());
                if (existing != null) {
                    mapper = existing.merge(mapper);
                }
                // 通过builder构建mapper对象 并设置到容器中
                mappers.put(mapper.simpleName(), mapper);
            }
            context.path().remove();

            ObjectMapper objectMapper = createMapper(name, context.path().pathAsText(name), enabled, nested, dynamic,
                mappers, context.indexSettings());

            return (Y) objectMapper;
        }

        protected ObjectMapper createMapper(String name, String fullPath, boolean enabled, Nested nested, Dynamic dynamic,
                Map<String, Mapper> mappers, @Nullable Settings settings) {
            return new ObjectMapper(name, fullPath, enabled, nested, dynamic, mappers, settings);
        }
    }

    /**
     * 类型解析器的作用好像就是从 解析上下文中抽取相关信息 并生成builder对象 而通过builder对象 就可以创建Mapper了
     */
    public static class TypeParser implements Mapper.TypeParser {
        @Override
        @SuppressWarnings("rawtypes")
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            ObjectMapper.Builder builder = new Builder(name);
            // 从node中获取有关嵌套的信息 并设置到builder中
            parseNested(name, node, builder);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                // 返回true 代表是builder需要的属性 那么在设置完后就可以从迭代器中移除了  看来是有一个很大的数据流 从上往下传入到不同的builder中进行解析
                if (parseObjectOrDocumentTypeProperties(fieldName, fieldNode, parserContext, builder)) {
                    iterator.remove();
                }
            }
            return builder;
        }

        /**
         * 传入的 fieldName fieldNode 是在node 剔除掉无关参数后剩余的数据
         * @param fieldName
         * @param fieldNode
         * @param parserContext 包含了解析过程中需要的参数
         * @param builder
         * @return
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        protected static boolean parseObjectOrDocumentTypeProperties(String fieldName, Object fieldNode, ParserContext parserContext,
                                                                     ObjectMapper.Builder builder) {
            // 设置dynamic 属性
            if (fieldName.equals("dynamic")) {
                String value = fieldNode.toString();
                if (value.equalsIgnoreCase("strict")) {
                    builder.dynamic(Dynamic.STRICT);
                } else {
                    boolean dynamic = XContentMapValues.nodeBooleanValue(fieldNode, fieldName + ".dynamic");
                    builder.dynamic(dynamic ? Dynamic.TRUE : Dynamic.FALSE);
                }
                return true;
            } else if (fieldName.equals("enabled")) {
                builder.enabled(XContentMapValues.nodeBooleanValue(fieldNode, fieldName + ".enabled"));
                return true;
            } else if (fieldName.equals("properties")) {
                if (fieldNode instanceof Collection && ((Collection) fieldNode).isEmpty()) {
                    // nothing to do here, empty (to support "properties: []" case)
                } else if (!(fieldNode instanceof Map)) {
                    throw new ElasticsearchParseException("properties must be a map type");
                } else {
                    parseProperties(builder, (Map<String, Object>) fieldNode, parserContext);
                }
                return true;
            } else if (fieldName.equals("include_in_all")) {
                deprecationLogger.deprecatedAndMaybeLog("include_in_all",
                    "[include_in_all] is deprecated, the _all field have been removed in this version");
                return true;
            }
            return false;
        }

        /**
         * 解析出有关嵌套的信息
         * @param name
         * @param node 这个node好像是代表格式化数据解析后的结果 比如解析完json字符串后各个字段以及对应的值被设置到容器中  而不是分布式中节点的意思
         * @param builder
         */
        @SuppressWarnings("rawtypes")
        protected static void parseNested(String name, Map<String, Object> node, ObjectMapper.Builder builder) {
            boolean nested = false;
            boolean nestedIncludeInParent = false;
            boolean nestedIncludeInRoot = false;
            Object fieldNode = node.get("type");
            if (fieldNode!=null) {
                String type = fieldNode.toString();
                // 对象类型是不使用嵌套的
                if (type.equals(CONTENT_TYPE)) {
                    builder.nested = Nested.NO;
                // 当类型中声明了嵌套时 使用嵌套数据
                } else if (type.equals(NESTED_CONTENT_TYPE)) {
                    nested = true;
                } else {
                    throw new MapperParsingException("Trying to parse an object but has a different type [" + type
                        + "] for [" + name + "]");
                }
            }
            fieldNode = node.get("include_in_parent");
            if (fieldNode != null) {
                nestedIncludeInParent = XContentMapValues.nodeBooleanValue(fieldNode, name + ".include_in_parent");
                node.remove("include_in_parent");
            }
            fieldNode = node.get("include_in_root");
            if (fieldNode != null) {
                nestedIncludeInRoot = XContentMapValues.nodeBooleanValue(fieldNode, name + ".include_in_root");
                node.remove("include_in_root");
            }
            if (nested) {
                builder.nested = Nested.newNested(nestedIncludeInParent, nestedIncludeInRoot);
            }

        }

        /**
         * 当某个node 对应的value为 properties时触发该方法
         * @param objBuilder
         * @param propsNode
         * @param parserContext
         */
        @SuppressWarnings("rawtypes")
        protected static void parseProperties(ObjectMapper.Builder objBuilder, Map<String, Object> propsNode, ParserContext parserContext) {
            Iterator<Map.Entry<String, Object>> iterator = propsNode.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                // Should accept empty arrays, as a work around for when the
                // user can't provide an empty Map. (PHP for example)
                boolean isEmptyList = entry.getValue() instanceof List && ((List<?>) entry.getValue()).isEmpty();

                if (entry.getValue() instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> propNode = (Map<String, Object>) entry.getValue();
                    String type;
                    Object typeNode = propNode.get("type");
                    if (typeNode != null) {
                        type = typeNode.toString();
                    } else {
                        // lets see if we can derive this...
                        if (propNode.get("properties") != null) {
                            type = ObjectMapper.CONTENT_TYPE;
                        } else if (propNode.size() == 1 && propNode.get("enabled") != null) {
                            // if there is a single property with the enabled
                            // flag on it, make it an object
                            // (usually, setting enabled to false to not index
                            // any type, including core values, which
                            type = ObjectMapper.CONTENT_TYPE;
                        } else {
                            throw new MapperParsingException("No type specified for field [" + fieldName + "]");
                        }
                    }

                    // 生成不同的类型解析器
                    Mapper.TypeParser typeParser = parserContext.typeParser(type);
                    if (typeParser == null) {
                        throw new MapperParsingException("No handler for type [" + type + "] declared on field [" + fieldName + "]");
                    }
                    // field 如果使用了多个"." 那么需要做拆分
                    String[] fieldNameParts = fieldName.split("\\.");
                    String realFieldName = fieldNameParts[fieldNameParts.length - 1];
                    Mapper.Builder<?,?> fieldBuilder = typeParser.parse(realFieldName, propNode, parserContext);
                    for (int i = fieldNameParts.length - 2; i >= 0; --i) {
                        // TODO 这一层层嵌套啥意思啊  builder0->builder1->builder2 每个builder中嵌套了 下一层的builder
                        ObjectMapper.Builder<?, ?> intermediate = new ObjectMapper.Builder<>(fieldNameParts[i]);
                        intermediate.add(fieldBuilder);
                        fieldBuilder = intermediate;
                    }
                    // 最后将这个层层嵌套的builder 设置到ObjectMapper.Builder 内部
                    objBuilder.add(fieldBuilder);
                    propNode.remove("type");
                    // 要求propNode 内的参数在typeParser.parse 后都被使用完 否则抛出异常
                    DocumentMapperParser.checkNoRemainingFields(fieldName, propNode, parserContext.indexVersionCreated());
                    iterator.remove();
                } else if (isEmptyList) {
                    iterator.remove();
                } else {
                    throw new MapperParsingException("Expected map for property [fields] on field [" + fieldName + "] but got a "
                            + fieldName.getClass());
                }
            }

            DocumentMapperParser.checkNoRemainingFields(propsNode, parserContext.indexVersionCreated(),
                    "DocType mapping definition has unsupported parameters: ");

        }

    }

    private final String fullPath;

    /**
     * 每个mapper 对象都有一个enabled属性 代表对应的格式化数据是否允许被解析
     */
    private final boolean enabled;

    /**
     * 对应的格式化数据内 可能还嵌套了其他的数据 比如 key:jsonObject
     * 该对象只是描述了 嵌套的状态 比如是否嵌套
     */
    private final Nested nested;

    private final String nestedTypePath;

    private final Query nestedTypeFilter;

    private volatile Dynamic dynamic;

    private volatile CopyOnWriteHashMap<String, Mapper> mappers;

    /**
     * @param name
     * @param fullPath
     * @param enabled
     * @param nested
     * @param dynamic
     * @param mappers  builder 内 MapperBuilder容器生成的Mapper对象会存储到这里
     * @param settings
     */
    ObjectMapper(String name, String fullPath, boolean enabled, Nested nested, Dynamic dynamic,
            Map<String, Mapper> mappers, Settings settings) {
        super(name);
        assert settings != null;
        if (name.isEmpty()) {
            throw new IllegalArgumentException("name cannot be empty string");
        }
        this.fullPath = fullPath;
        this.enabled = enabled;
        this.nested = nested;
        this.dynamic = dynamic;
        if (mappers == null) {
            this.mappers = new CopyOnWriteHashMap<>();
        } else {
            this.mappers = CopyOnWriteHashMap.copyOf(mappers);
        }
        if (Version.indexCreated(settings).before(Version.V_8_0_0)) {
            this.nestedTypePath = "__" + fullPath;
        } else {
            this.nestedTypePath = fullPath;
        }
        this.nestedTypeFilter = NestedPathFieldMapper.filter(settings, nestedTypePath);
    }

    @Override
    protected ObjectMapper clone() {
        ObjectMapper clone;
        try {
            clone = (ObjectMapper) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        return clone;
    }

    /**
     * Build a mapping update with the provided sub mapping update.
     * 直接往 ObjectMapper对象中插入新的mapper
     */
    public ObjectMapper mappingUpdate(Mapper mapper) {
        ObjectMapper mappingUpdate = clone();
        // reset the sub mappers
        mappingUpdate.mappers = new CopyOnWriteHashMap<>();
        mappingUpdate.putMapper(mapper);
        return mappingUpdate;
    }

    @Override
    public String name() {
        return this.fullPath;
    }

    @Override
    public String typeName() {
        return CONTENT_TYPE;
    }

    public boolean isEnabled() {
        return this.enabled;
    }

    public Mapper getMapper(String field) {
        return mappers.get(field);
    }

    public Nested nested() {
        return this.nested;
    }

    public Query nestedTypeFilter() {
        return this.nestedTypeFilter;
    }

    protected void putMapper(Mapper mapper) {
        mappers = mappers.copyAndPut(mapper.simpleName(), mapper);
    }

    @Override
    public Iterator<Mapper> iterator() {
        return mappers.values().iterator();
    }

    public String fullPath() {
        return this.fullPath;
    }

    public String nestedTypePath() {
        return this.nestedTypePath;
    }

    public final Dynamic dynamic() {
        return dynamic;
    }

    /**
     * Returns the parent {@link ObjectMapper} instance of the specified object mapper or <code>null</code> if there
     * isn't any.
     * TODO 映射服务的职能是???
     */
    public ObjectMapper getParentObjectMapper(MapperService mapperService) {
        int indexOfLastDot = fullPath().lastIndexOf('.');
        if (indexOfLastDot != -1) {
            // 截取出最低层外的其他path
            String parentNestObjectPath = fullPath().substring(0, indexOfLastDot);
            // 获取对应的ObjectMapper对象 并返回
            return mapperService.getObjectMapper(parentNestObjectPath);
        } else {
            return null;
        }
    }

    /**
     * Returns whether all parent objects fields are nested too.
     * 检测是否所有的父级都是嵌套的
     */
    public boolean parentObjectMapperAreNested(MapperService mapperService) {
        // 从上层开始 继续往上层获取 直接达到最高层
        for (ObjectMapper parent = getParentObjectMapper(mapperService);
             parent != null;
             parent = parent.getParentObjectMapper(mapperService)) {

            if (parent.nested().isNested() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public ObjectMapper merge(Mapper mergeWith) {
        if (!(mergeWith instanceof ObjectMapper)) {
            throw new IllegalArgumentException("Can't merge a non object mapping [" + mergeWith.name()
                + "] with an object mapping [" + name() + "]");
        }
        ObjectMapper mergeWithObject = (ObjectMapper) mergeWith;
        ObjectMapper merged = clone();
        merged.doMerge(mergeWithObject);
        return merged;
    }

    protected void doMerge(final ObjectMapper mergeWith) {
        if (nested().isNested()) {
            if (!mergeWith.nested().isNested()) {
                throw new IllegalArgumentException("object mapping [" + name() + "] can't be changed from nested to non-nested");
            }
        } else {
            if (mergeWith.nested().isNested()) {
                throw new IllegalArgumentException("object mapping [" + name() + "] can't be changed from non-nested to nested");
            }
        }

        if (mergeWith.dynamic != null) {
            this.dynamic = mergeWith.dynamic;
        }

        checkObjectMapperParameters(mergeWith);

        for (Mapper mergeWithMapper : mergeWith) {
            Mapper mergeIntoMapper = mappers.get(mergeWithMapper.simpleName());

            Mapper merged;
            if (mergeIntoMapper == null) {
                // no mapping, simply add it
                merged = mergeWithMapper;
            } else {
                // root mappers can only exist here for backcompat, and are merged in Mapping
                merged = mergeIntoMapper.merge(mergeWithMapper);
            }
            putMapper(merged);
        }
    }

    private void checkObjectMapperParameters(final ObjectMapper mergeWith) {
        if (isEnabled() != mergeWith.isEnabled()) {
            throw new MapperException("The [enabled] parameter can't be updated for the object mapping [" + name() + "].");
        }

        if (nested().isIncludeInParent() != mergeWith.nested().isIncludeInParent()) {
            throw new MapperException("The [include_in_parent] parameter can't be updated for the nested object mapping [" +
                name() + "].");
        }

        if (nested().isIncludeInRoot() != mergeWith.nested().isIncludeInRoot()) {
            throw new MapperException("The [include_in_root] parameter can't be updated for the nested object mapping [" +
                name() + "].");
        }
    }

    /**
     *
     * @param fullNameToFieldType
     * @return
     */
    @Override
    public ObjectMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        List<Mapper> updatedMappers = null;
        for (Mapper mapper : this) {
            // 内部每个mapper 都是fieldMapper对象 这里是更新field的 fieldType属性
            Mapper updated = mapper.updateFieldType(fullNameToFieldType);
            if (mapper != updated) {
                if (updatedMappers == null) {
                    updatedMappers = new ArrayList<>();
                }
                updatedMappers.add(updated);
            }
        }
        if (updatedMappers == null) {
            return this;
        }
        // 将更新后的mapper设置到容器中
        ObjectMapper updated = clone();
        for (Mapper updatedMapper : updatedMappers) {
            updated.putMapper(updatedMapper);
        }
        return updated;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        toXContent(builder, params, null);
        return builder;
    }

    public void toXContent(XContentBuilder builder, Params params, ToXContent custom) throws IOException {
        builder.startObject(simpleName());
        if (nested.isNested()) {
            builder.field("type", NESTED_CONTENT_TYPE);
            if (nested.isIncludeInParent()) {
                builder.field("include_in_parent", true);
            }
            if (nested.isIncludeInRoot()) {
                builder.field("include_in_root", true);
            }
        } else if (mappers.isEmpty() && custom == null) {
            // only write the object content type if there are no properties, otherwise, it is automatically detected
            builder.field("type", CONTENT_TYPE);
        }
        if (dynamic != null) {
            builder.field("dynamic", dynamic.name().toLowerCase(Locale.ROOT));
        }
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }

        if (custom != null) {
            custom.toXContent(builder, params);
        }

        doXContent(builder, params);

        // sort the mappers so we get consistent serialization format
        Mapper[] sortedMappers = mappers.values().stream().toArray(size -> new Mapper[size]);
        Arrays.sort(sortedMappers, new Comparator<Mapper>() {
            @Override
            public int compare(Mapper o1, Mapper o2) {
                return o1.name().compareTo(o2.name());
            }
        });

        int count = 0;
        for (Mapper mapper : sortedMappers) {
            if (!(mapper instanceof MetadataFieldMapper)) {
                if (count++ == 0) {
                    builder.startObject("properties");
                }
                mapper.toXContent(builder, params);
            }
        }
        if (count > 0) {
            builder.endObject();
        }
        builder.endObject();
    }

    protected void doXContent(XContentBuilder builder, Params params) throws IOException {

    }

}
