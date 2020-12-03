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
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DynamicTemplate.XContentFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.TypeParsers.parseDateTimeFormatter;

/**
 * 根映射对象
 */
public class RootObjectMapper extends ObjectMapper {

    private static final Logger LOGGER = LogManager.getLogger(RootObjectMapper.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LOGGER);

    public static class Defaults {
        public static final DateFormatter[] DYNAMIC_DATE_TIME_FORMATTERS =
                new DateFormatter[]{
                        DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                        DateFormatter.forPattern("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||epoch_millis")
                };
        public static final boolean DATE_DETECTION = true;
        public static final boolean NUMERIC_DETECTION = false;
    }

    public static class Builder extends ObjectMapper.Builder<Builder, RootObjectMapper> {

        // false 代表默认值 true代表用户指定的值
        protected Explicit<DynamicTemplate[]> dynamicTemplates = new Explicit<>(new DynamicTemplate[0], false);
        protected Explicit<DateFormatter[]> dynamicDateTimeFormatters = new Explicit<>(Defaults.DYNAMIC_DATE_TIME_FORMATTERS, false);
        protected Explicit<Boolean> dateDetection = new Explicit<>(Defaults.DATE_DETECTION, false);
        protected Explicit<Boolean> numericDetection = new Explicit<>(Defaults.NUMERIC_DETECTION, false);

        public Builder(String name) {
            super(name);
            this.builder = this;
        }

        public Builder dynamicDateTimeFormatter(Collection<DateFormatter> dateTimeFormatters) {
            this.dynamicDateTimeFormatters = new Explicit<>(dateTimeFormatters.toArray(new DateFormatter[0]), true);
            return this;
        }

        /**
         * 代表在 _doc 的映射过程中遇到了 动态模板属性 在转换后填充到builder中
         * @param templates
         * @return
         */
        public Builder dynamicTemplates(Collection<DynamicTemplate> templates) {
            this.dynamicTemplates = new Explicit<>(templates.toArray(new DynamicTemplate[0]), true);
            return this;
        }

        /**
         * 当填充完相关属性后 开始生成mapper对象
         * @param context
         * @return
         */
        @Override
        public RootObjectMapper build(BuilderContext context) {
            fixRedundantIncludes(this, true);
            return super.build(context);
        }

        /**
         * Removes redundant root includes in {@link ObjectMapper.Nested} trees to avoid duplicate
         * fields on the root mapper when {@code isIncludeInRoot} is {@code true} for a node that is
         * itself included into a parent node, for which either {@code isIncludeInRoot} is
         * {@code true} or which is transitively included in root by a chain of nodes with
         * {@code isIncludeInParent} returning {@code true}.
         * @param omb Builder whose children to check.
         * @param parentIncluded True if node is a child of root or a node that is included in
         * root
         *                       这个方法就是将 ObjectMapper.Builder中嵌套类型 同时设置了 includeRoot includeParent的 修改成 includeParent
         */
        @SuppressWarnings("rawtypes")
        private static void fixRedundantIncludes(ObjectMapper.Builder omb, boolean parentIncluded) {

            // 如果rootObject 层级比较多 子级属性会生成单独的builder对象 并挂载在父级builder上  这里就是遍历挂载的builder
            for (Object mapper : omb.mappersBuilders) {
                // 被遍历的builder 可能是 FieldMapper 也可能是 ObjectMapper类型  具体取决于被解析的json结构体对应的 "type" 属性 (会匹配到不同的typeParser对象 也就会解析出不同的mapper)
                if (mapper instanceof ObjectMapper.Builder) {
                    // 这里只处理 ObjectMapper.Builder 类型
                    ObjectMapper.Builder child = (ObjectMapper.Builder) mapper;
                    Nested nested = child.nested;
                    // object对应的json数据中 type可能为 nested类型 代表是嵌套类型 并且可能会携带 isIncludeInParent/isIncludeInRoot 2个属性
                    boolean isNested = nested.isNested();
                    boolean includeInRootViaParent = parentIncluded && isNested && nested.isIncludeInParent();
                    boolean includedInRoot = isNested && nested.isIncludeInRoot();
                    // 如果嵌套属性 同时指定了 需要包含在 root 以及 parent中  实际上是冲突了  这里修改成 仅包含在parent中
                    if (includeInRootViaParent && includedInRoot) {
                        child.nested = Nested.newNested(true, false);
                    }
                    fixRedundantIncludes(child, includeInRootViaParent || includedInRoot);
                }
            }
        }

        /**
         * 当子级的所有builder都转换成mapper对象后 最终轮到创建 RootObjectMapper
         * @param name  当前解析的key
         * @param fullPath  此时完整的解析路径
         * @param enabled
         * @param nested
         * @param dynamic
         * @param mappers
         * @param settings
         * @return
         */
        @Override
        protected ObjectMapper createMapper(String name, String fullPath, boolean enabled, Nested nested, Dynamic dynamic,
                Map<String, Mapper> mappers, @Nullable Settings settings) {
            assert !nested.isNested();
            return new RootObjectMapper(name, enabled, dynamic, mappers,
                    dynamicDateTimeFormatters,
                    dynamicTemplates,
                    dateDetection, numericDetection, settings);
        }
    }

    public static class TypeParser extends ObjectMapper.TypeParser {


        /**
         * 作为RootObject的映射对象 会从最上层开始解析
         * @param name
         * @param node 在收到新的IndexMetadata.mapping 数据流时 会将其转换成一个map对象
         * @param parserContext  包含解析过程中需要的各种参数
         * @return
         * @throws MapperParsingException
         */
        @Override
        @SuppressWarnings("rawtypes")
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {

            // 生成空的builder对象 并不断往内部填充参数  这是最上层的builder对象 往下每解析一个字段 会生成对应的builder 并追加到上层builder中 形成链式结构
            RootObjectMapper.Builder builder = new Builder(name);
            Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator();
            // _doc 下最外层的各种field
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                // 每个field 可能又是一个map对象
                Object fieldNode = entry.getValue();

                // 在parseObjectOrDocumentTypeProperties 和 processField 会剥离掉一些非业务字段 并填充到builder中
                if (parseObjectOrDocumentTypeProperties(fieldName, fieldNode, parserContext, builder)
                        || processField(builder, fieldName, fieldNode, parserContext)) {
                    iterator.remove();
                }
            }
            return builder;
        }

        /**
         * @param builder
         * @param fieldName
         * @param fieldNode
         * @param parserContext
         * @return
         */
        @SuppressWarnings("unchecked")
        protected boolean processField(RootObjectMapper.Builder builder, String fieldName, Object fieldNode,
                                       ParserContext parserContext) {
            if (fieldName.equals("date_formats") || fieldName.equals("dynamic_date_formats")) {
                if (fieldNode instanceof List) {
                    List<DateFormatter> formatters = new ArrayList<>();
                    for (Object formatter : (List<?>) fieldNode) {
                        if (formatter.toString().startsWith("epoch_")) {
                            throw new MapperParsingException("Epoch ["+ formatter +"] is not supported as dynamic date format");
                        }
                        // 将描述信息的所有字符串解析后设置到 list中
                        formatters.add(parseDateTimeFormatter(formatter));
                    }
                    builder.dynamicDateTimeFormatter(formatters);
                } else if ("none".equals(fieldNode.toString())) {
                    builder.dynamicDateTimeFormatter(Collections.emptyList());
                } else {
                    // 将单个dateTimeFormat对象包装成容器后设置
                    builder.dynamicDateTimeFormatter(Collections.singleton(parseDateTimeFormatter(fieldNode)));
                }
                return true;
            } else if (fieldName.equals("dynamic_templates")) {
                /*
                  "dynamic_templates" : [
                      {
                          "template_1" : {
                              "match" : "*_test",
                              "match_mapping_type" : "string",
                              "mapping" : { "type" : "keyword", "store" : "yes" }
                          }
                      }
                  ]
                */
                // 动态模板必须是list类型
                if ((fieldNode instanceof List) == false) {
                    throw new MapperParsingException("Dynamic template syntax error. An array of named objects is expected.");
                }
                List<?> tmplNodes = (List<?>) fieldNode;
                List<DynamicTemplate> templates = new ArrayList<>();
                for (Object tmplNode : tmplNodes) {
                    Map<String, Object> tmpl = (Map<String, Object>) tmplNode;
                    if (tmpl.size() != 1) {
                        throw new MapperParsingException("A dynamic template must be defined with a name");
                    }
                    Map.Entry<String, Object> entry = tmpl.entrySet().iterator().next();
                    String templateName = entry.getKey();
                    Map<String, Object> templateParams = (Map<String, Object>) entry.getValue();
                    // 通过 templateName 匹配动态模板 使用 params 填充模板信息
                    DynamicTemplate template = DynamicTemplate.parse(templateName, templateParams);
                    if (template != null) {
                        validateDynamicTemplate(parserContext, template);
                        templates.add(template);
                    }
                }
                // 当设置动态模板后返回
                builder.dynamicTemplates(templates);
                return true;
            } else if (fieldName.equals("date_detection")) {
                builder.dateDetection = new Explicit<>(nodeBooleanValue(fieldNode, "date_detection"), true);
                return true;
            } else if (fieldName.equals("numeric_detection")) {
                builder.numericDetection = new Explicit<>(nodeBooleanValue(fieldNode, "numeric_detection"), true);
                return true;
            }
            return false;
        }
    }

    private Explicit<DateFormatter[]> dynamicDateTimeFormatters;
    private Explicit<Boolean> dateDetection;
    private Explicit<Boolean> numericDetection;
    private Explicit<DynamicTemplate[]> dynamicTemplates;

    /**
     * 创建 RootObjectMapper对象 也只是简单的赋值
     * @param name
     * @param enabled
     * @param dynamic
     * @param mappers
     * @param dynamicDateTimeFormatters
     * @param dynamicTemplates
     * @param dateDetection
     * @param numericDetection
     * @param settings
     */
    RootObjectMapper(String name, boolean enabled, Dynamic dynamic, Map<String, Mapper> mappers,
                     Explicit<DateFormatter[]> dynamicDateTimeFormatters, Explicit<DynamicTemplate[]> dynamicTemplates,
                     Explicit<Boolean> dateDetection, Explicit<Boolean> numericDetection, Settings settings) {
        super(name, name, enabled, Nested.NO, dynamic, mappers, settings);
        this.dynamicTemplates = dynamicTemplates;
        this.dynamicDateTimeFormatters = dynamicDateTimeFormatters;
        this.dateDetection = dateDetection;
        this.numericDetection = numericDetection;
    }

    @Override
    public ObjectMapper mappingUpdate(Mapper mapper) {
        // 更新当前mapper的属性
        RootObjectMapper update = (RootObjectMapper) super.mappingUpdate(mapper);
        // for dynamic updates, no need to carry root-specific options, we just
        // set everything to they implicit default value so that they are not
        // applied at merge time
        update.dynamicTemplates = new Explicit<>(new DynamicTemplate[0], false);
        update.dynamicDateTimeFormatters = new Explicit<>(Defaults.DYNAMIC_DATE_TIME_FORMATTERS, false);
        update.dateDetection = new Explicit<>(Defaults.DATE_DETECTION, false);
        update.numericDetection = new Explicit<>(Defaults.NUMERIC_DETECTION, false);
        return update;
    }

    public boolean dateDetection() {
        return this.dateDetection.value();
    }

    public boolean numericDetection() {
        return this.numericDetection.value();
    }

    public DateFormatter[] dynamicDateTimeFormatters() {
        return dynamicDateTimeFormatters.value();
    }

    @SuppressWarnings("rawtypes")
    public Mapper.Builder findTemplateBuilder(ParseContext context, String name, XContentFieldType matchType) {
        return findTemplateBuilder(context, name, matchType.defaultMappingType(), matchType);
    }

    /**
     * Find a template. Returns {@code null} if no template could be found.
     * @param name        the field name
     * @param dynamicType the field type to give the field if the template does not define one
     * @param matchType   the type of the field in the json document or null if unknown
     * @return a mapper builder, or null if there is no template for such a field
     */
    @SuppressWarnings("rawtypes")
    public Mapper.Builder findTemplateBuilder(ParseContext context, String name, String dynamicType, XContentFieldType matchType) {
        // 找到匹配的某个模板
        DynamicTemplate dynamicTemplate = findTemplate(context.path(), name, matchType);
        if (dynamicTemplate == null) {
            return null;
        }
        Mapper.TypeParser.ParserContext parserContext = context.docMapperParser().parserContext();
        String mappingType = dynamicTemplate.mappingType(dynamicType);
        // 这里应该就是 ObjectMapper.TypeParser
        Mapper.TypeParser typeParser = parserContext.typeParser(mappingType);
        if (typeParser == null) {
            throw new MapperParsingException("failed to find type parsed [" + mappingType + "] for [" + name + "]");
        }
        return typeParser.parse(name, dynamicTemplate.mappingForName(name, dynamicType), parserContext);
    }

    /**
     * 当传入的参数能够匹配上某个动态模板时 返回
     * @param path
     * @param name
     * @param matchType
     * @return
     */
    public DynamicTemplate findTemplate(ContentPath path, String name, XContentFieldType matchType) {
        final String pathAsString = path.pathAsText(name);
        for (DynamicTemplate dynamicTemplate : dynamicTemplates.value()) {
            if (dynamicTemplate.match(pathAsString, name, matchType)) {
                return dynamicTemplate;
            }
        }
        return null;
    }

    @Override
    public RootObjectMapper merge(Mapper mergeWith) {
        return (RootObjectMapper) super.merge(mergeWith);
    }

    @Override
    protected void doMerge(ObjectMapper mergeWith) {
        super.doMerge(mergeWith);
        RootObjectMapper mergeWithObject = (RootObjectMapper) mergeWith;
        if (mergeWithObject.numericDetection.explicit()) {
            this.numericDetection = mergeWithObject.numericDetection;
        }
        if (mergeWithObject.dateDetection.explicit()) {
            this.dateDetection = mergeWithObject.dateDetection;
        }
        if (mergeWithObject.dynamicDateTimeFormatters.explicit()) {
            this.dynamicDateTimeFormatters = mergeWithObject.dynamicDateTimeFormatters;
        }
        if (mergeWithObject.dynamicTemplates.explicit()) {
            this.dynamicTemplates = mergeWithObject.dynamicTemplates;
        }
    }

    @Override
    public RootObjectMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        return (RootObjectMapper) super.updateFieldType(fullNameToFieldType);
    }

    @Override
    protected void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        final boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        if (dynamicDateTimeFormatters.explicit() || includeDefaults) {
            builder.startArray("dynamic_date_formats");
            for (DateFormatter dateTimeFormatter : dynamicDateTimeFormatters.value()) {
                builder.value(dateTimeFormatter.pattern());
            }
            builder.endArray();
        }

        if (dynamicTemplates.explicit() || includeDefaults) {
            builder.startArray("dynamic_templates");
            for (DynamicTemplate dynamicTemplate : dynamicTemplates.value()) {
                builder.startObject();
                builder.field(dynamicTemplate.name(), dynamicTemplate);
                builder.endObject();
            }
            builder.endArray();
        }

        if (dateDetection.explicit() || includeDefaults) {
            builder.field("date_detection", dateDetection.value());
        }
        if (numericDetection.explicit() || includeDefaults) {
            builder.field("numeric_detection", numericDetection.value());
        }
    }

    private static void validateDynamicTemplate(Mapper.TypeParser.ParserContext parserContext,
                                                DynamicTemplate dynamicTemplate) {

        if (containsSnippet(dynamicTemplate.getMapping(), "{name}")) {
            // Can't validate template, because field names can't be guessed up front.
            return;
        }

        final XContentFieldType[] types;
        if (dynamicTemplate.getXContentFieldType() != null) {
            types = new XContentFieldType[]{dynamicTemplate.getXContentFieldType()};
        } else {
            types = XContentFieldType.values();
        }

        Exception lastError = null;
        boolean dynamicTemplateInvalid = true;

        for (XContentFieldType contentFieldType : types) {
            String defaultDynamicType = contentFieldType.defaultMappingType();
            String mappingType = dynamicTemplate.mappingType(defaultDynamicType);
            Mapper.TypeParser typeParser = parserContext.typeParser(mappingType);
            if (typeParser == null) {
                lastError = new IllegalArgumentException("No mapper found for type [" + mappingType + "]");
                continue;
            }

            Map<String, Object> fieldTypeConfig = dynamicTemplate.mappingForName("__dummy__", defaultDynamicType);
            fieldTypeConfig.remove("type");
            try {
                Mapper.Builder<?, ?> dummyBuilder = typeParser.parse("__dummy__", fieldTypeConfig, parserContext);
                if (fieldTypeConfig.isEmpty()) {
                    Settings indexSettings = parserContext.mapperService().getIndexSettings().getSettings();
                    BuilderContext builderContext = new BuilderContext(indexSettings, new ContentPath(1));
                    dummyBuilder.build(builderContext);
                    dynamicTemplateInvalid = false;
                    break;
                } else {
                    lastError = new IllegalArgumentException("Unused mapping attributes [" + fieldTypeConfig + "]");
                }
            } catch (Exception e) {
                lastError = e;
            }
        }

        final boolean failInvalidDynamicTemplates = parserContext.indexVersionCreated().onOrAfter(Version.V_8_0_0);
        if (dynamicTemplateInvalid) {
            String message = String.format(Locale.ROOT, "dynamic template [%s] has invalid content [%s]",
                dynamicTemplate.getName(), Strings.toString(dynamicTemplate));
            if (failInvalidDynamicTemplates) {
                throw new IllegalArgumentException(message, lastError);
            } else {
                final String deprecationMessage;
                if (lastError != null) {
                     deprecationMessage = String.format(Locale.ROOT, "%s, caused by [%s]", message, lastError.getMessage());
                } else {
                    deprecationMessage = message;
                }
                DEPRECATION_LOGGER.deprecatedAndMaybeLog("invalid_dynamic_template", deprecationMessage);
            }
        }
    }

    private static boolean containsSnippet(Map<?, ?> map, String snippet) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey().toString();
            if (key.contains(snippet)) {
                return true;
            }

            Object value = entry.getValue();
            if (value instanceof Map) {
                if (containsSnippet((Map<?, ?>) value, snippet)) {
                    return true;
                }
            } else if (value instanceof List) {
                if (containsSnippet((List<?>) value, snippet)) {
                    return true;
                }
            } else if (value instanceof String) {
                String valueString = (String) value;
                if (valueString.contains(snippet)) {
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean containsSnippet(List<?> list, String snippet) {
        for (Object value : list) {
            if (value instanceof Map) {
                if (containsSnippet((Map<?, ?>) value, snippet)) {
                    return true;
                }
            } else if (value instanceof List) {
                if (containsSnippet((List<?>) value, snippet)) {
                    return true;
                }
            } else if (value instanceof String) {
                String valueString = (String) value;
                if (valueString.contains(snippet)) {
                    return true;
                }
            }
        }
        return false;
    }
}
