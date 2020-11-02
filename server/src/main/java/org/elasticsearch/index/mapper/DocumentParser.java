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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DynamicTemplate.XContentFieldType;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType;

import java.io.IOException;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * A parser for documents, given mappings from a DocumentMapper
 * doc解析器
 */
final class DocumentParser {

    private final IndexSettings indexSettings;
    private final DocumentMapperParser docMapperParser;
    /**
     * 该对象维护了 各个Mapper之间的关系 (RootObjectMapper  FieldMapper 等等)
     */
    private final DocumentMapper docMapper;

    DocumentParser(IndexSettings indexSettings, DocumentMapperParser docMapperParser, DocumentMapper docMapper) {
        this.indexSettings = indexSettings;
        this.docMapperParser = docMapperParser;
        this.docMapper = docMapper;
    }


    /**
     *
     * @param source  提供格式化数据流
     * @param metadataFieldsMappers   代表doc下每个field对应的mapper对象
     * @return 代表解析后的结果
     * @throws MapperParsingException
     */
    ParsedDocument parseDocument(SourceToParse source, MetadataFieldMapper[] metadataFieldsMappers) throws MapperParsingException {

        // 该对象内部存储了一组 MetadataFieldMapper
        final Mapping mapping = docMapper.mapping();
        final ParseContext.InternalParseContext context;

        // 描述这组数据流一开始是什么格式的
        final XContentType xContentType = source.getXContentType();

        try (XContentParser parser = XContentHelper.createParser(docMapperParser.getXContentRegistry(),
            LoggingDeprecationHandler.INSTANCE, source.source(), xContentType)) {
            // parser 对象负责解析数据流
            context = new ParseContext.InternalParseContext(indexSettings, docMapperParser, docMapper, source, parser);
            // 确保首个token是 起始符号
            validateStart(parser);
            // 开始解析数据流
            internalParseDocument(mapping, metadataFieldsMappers, context, parser);
            // 确保解析到末尾
            validateEnd(parser);
        } catch (Exception e) {
            throw wrapInMapperParsingException(source, e);
        }
        String remainingPath = context.path().pathAsText("");
        if (remainingPath.isEmpty() == false) {
            throw new IllegalStateException("found leftover path elements: " + remainingPath);
        }

        context.postParse();

        return parsedDocument(source, context, createDynamicUpdate(mapping, docMapper, context.getDynamicMappers()));
    }

    /**
     * 检测是否包含 enabled == false的mapper
     * @param objectMapper
     * @param subfields
     * @return
     */
    private static boolean containsDisabledObjectMapper(ObjectMapper objectMapper, String[] subfields) {
        for (int i = 0; i < subfields.length - 1; ++i) {
            // TODO 什么情况下 会往 mapper中设置mapper[]  目前只看到 在ParserContext中嵌套创建doc
            Mapper mapper = objectMapper.getMapper(subfields[i]);
            if (mapper instanceof ObjectMapper == false) {
                break;
            }
            objectMapper = (ObjectMapper) mapper;
            if (objectMapper.isEnabled() == false) {
                return true;
            }
        }
        return false;
    }

    /**
     * DocumentParser 是整个解析的起始点   代表可以对某个doc进行解析
     * @param mapping   该对象内部存储了一组MetadataFieldMapper
     * @param metadataFieldsMappers
     * @param context  存储了在解析过程中需要的各种参数
     * @param parser  该对象负责解析格式化数据
     * @throws IOException
     */
    private static void internalParseDocument(Mapping mapping, MetadataFieldMapper[] metadataFieldsMappers,
                                              ParseContext.InternalParseContext context, XContentParser parser) throws IOException {
        // 首先检测这个格式化数据是否是空的
        final boolean emptyDoc = isEmptyDoc(mapping, parser);

        // MetadataFieldMapper的特点就是包含前后钩子
        for (MetadataFieldMapper metadataMapper : metadataFieldsMappers) {
            metadataMapper.preParse(context);
        }

        // 不可用的话 直接就跳过内部的数据体
        if (mapping.root.isEnabled() == false) {
            // entire type is disabled
            parser.skipChildren();
        } else if (emptyDoc == false) {
            parseObjectOrNested(context, mapping.root);
        } // 可以看到即使 emptyDoc 为true 还是会走完前后钩子

        for (MetadataFieldMapper metadataMapper : metadataFieldsMappers) {
            metadataMapper.postParse(context);
        }
    }

    private static void validateStart(XContentParser parser) throws IOException {
        // will result in START_OBJECT
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new MapperParsingException("Malformed content, must start with an object");
        }
    }

    private static void validateEnd(XContentParser parser) throws IOException {
        XContentParser.Token token;// only check for end of tokens if we created the parser here
        // try to parse the next token, this should be null if the object is ended properly
        // but will throw a JSON exception if the extra tokens is not valid JSON (this will be handled by the catch)
        token = parser.nextToken();
        if (token != null) {
            throw new IllegalArgumentException("Malformed content, found extra data after parsing: " + token);
        }
    }

    private static boolean isEmptyDoc(Mapping mapping, XContentParser parser) throws IOException {
        // 首先确保允许解析数据  不允许的化 在后面的处理中 会直接 skipChild 所以这里是否为空就不重要了
        if (mapping.root.isEnabled()) {
            final XContentParser.Token token = parser.nextToken();
            // 在解析完开始符 "{" 后 如果紧跟着"}" 代表本次解析的是一个空数据体
            if (token == XContentParser.Token.END_OBJECT) {
                // empty doc, we can handle it...
                return true;
            // 要求必须是 key
            } else if (token != XContentParser.Token.FIELD_NAME) {
                throw new MapperParsingException("Malformed content, after first object, either the type field"
                    + " or the actual properties should exist");
            }
        }
        // 默认情况下认为不是空的
        return false;
    }


    private static ParsedDocument parsedDocument(SourceToParse source, ParseContext.InternalParseContext context, Mapping update) {
        return new ParsedDocument(
            context.version(),
            context.seqID(),
            context.sourceToParse().id(),
            source.routing(),
            context.docs(),
            context.sourceToParse().source(),
            context.sourceToParse().getXContentType(),
            update
        );
    }


    private static MapperParsingException wrapInMapperParsingException(SourceToParse source, Exception e) {
        // if its already a mapper parsing exception, no need to wrap it...
        if (e instanceof MapperParsingException) {
            return (MapperParsingException) e;
        }

        // Throw a more meaningful message if the document is empty.
        if (source.source() != null && source.source().length() == 0) {
            return new MapperParsingException("failed to parse, document is empty");
        }

        return new MapperParsingException("failed to parse", e);
    }

    private static String[] splitAndValidatePath(String fullFieldPath) {
        if (fullFieldPath.contains(".")) {
            // 当存在 "." 时 先进行拆分
            String[] parts = fullFieldPath.split("\\.");
            for (String part : parts) {
                if (Strings.hasText(part) == false) {
                    // check if the field name contains only whitespace
                    if (Strings.isEmpty(part) == false) {
                        throw new IllegalArgumentException(
                                "object field cannot contain only whitespace: ['" + fullFieldPath + "']");
                    }
                    throw new IllegalArgumentException(
                            "object field starting or ending with a [.] makes object resolution ambiguous: [" + fullFieldPath + "]");
                }
            }
            return parts;
        } else {
            if (Strings.isEmpty(fullFieldPath)) {
                throw new IllegalArgumentException("field name cannot be an empty string");
            }
            // 当 格式化数据的key 不包含"." 时 直接返回
            return new String[] {fullFieldPath};
        }
    }

    /** Creates a Mapping containing any dynamically added fields, or returns null if there were no dynamic mappings. */
    static Mapping createDynamicUpdate(Mapping mapping, DocumentMapper docMapper, List<Mapper> dynamicMappers) {
        if (dynamicMappers.isEmpty()) {
            return null;
        }
        // We build a mapping by first sorting the mappers, so that all mappers containing a common prefix
        // will be processed in a contiguous block. When the prefix is no longer seen, we pop the extra elements
        // off the stack, merging them upwards into the existing mappers.
        Collections.sort(dynamicMappers, (Mapper o1, Mapper o2) -> o1.name().compareTo(o2.name()));
        Iterator<Mapper> dynamicMapperItr = dynamicMappers.iterator();
        List<ObjectMapper> parentMappers = new ArrayList<>();
        Mapper firstUpdate = dynamicMapperItr.next();
        parentMappers.add(createUpdate(mapping.root(), splitAndValidatePath(firstUpdate.name()), 0, firstUpdate));
        Mapper previousMapper = null;
        while (dynamicMapperItr.hasNext()) {
            Mapper newMapper = dynamicMapperItr.next();
            if (previousMapper != null && newMapper.name().equals(previousMapper.name())) {
                // We can see the same mapper more than once, for example, if we had foo.bar and foo.baz, where
                // foo did not yet exist. This will create 2 copies in dynamic mappings, which should be identical.
                // Here we just skip over the duplicates, but we merge them to ensure there are no conflicts.
                newMapper.merge(previousMapper);
                continue;
            }
            previousMapper = newMapper;
            String[] nameParts = splitAndValidatePath(newMapper.name());

            // We first need the stack to only contain mappers in common with the previously processed mapper
            // For example, if the first mapper processed was a.b.c, and we now have a.d, the stack will contain
            // a.b, and we want to merge b back into the stack so it just contains a
            int i = removeUncommonMappers(parentMappers, nameParts);

            // Then we need to add back mappers that may already exist within the stack, but are not on it.
            // For example, if we processed a.b, followed by an object mapper a.c.d, and now are adding a.c.d.e
            // then the stack will only have a on it because we will have already merged a.c.d into the stack.
            // So we need to pull a.c, followed by a.c.d, onto the stack so e can be added to the end.
            i = expandCommonMappers(parentMappers, nameParts, i);

            // If there are still parents of the new mapper which are not on the stack, we need to pull them
            // from the existing mappings. In order to maintain the invariant that the stack only contains
            // fields which are updated, we cannot simply add the existing mappers to the stack, since they
            // may have other subfields which will not be updated. Instead, we pull the mapper from the existing
            // mappings, and build an update with only the new mapper and its parents. This then becomes our
            // "new mapper", and can be added to the stack.
            if (i < nameParts.length - 1) {
                newMapper = createExistingMapperUpdate(parentMappers, nameParts, i, docMapper, newMapper);
            }

            if (newMapper instanceof ObjectMapper) {
                parentMappers.add((ObjectMapper)newMapper);
            } else {
                addToLastMapper(parentMappers, newMapper, true);
            }
        }
        popMappers(parentMappers, 1, true);
        assert parentMappers.size() == 1;

        return mapping.mappingUpdate(parentMappers.get(0));
    }

    private static void popMappers(List<ObjectMapper> parentMappers, int keepBefore, boolean merge) {
        assert keepBefore >= 1; // never remove the root mapper
        // pop off parent mappers not needed by the current mapper,
        // merging them backwards since they are immutable
        for (int i = parentMappers.size() - 1; i >= keepBefore; --i) {
            addToLastMapper(parentMappers, parentMappers.remove(i), merge);
        }
    }

    /**
     * Adds a mapper as an update into the last mapper. If merge is true, the new mapper
     * will be merged in with other child mappers of the last parent, otherwise it will be a new update.
     */
    private static void addToLastMapper(List<ObjectMapper> parentMappers, Mapper mapper, boolean merge) {
        assert parentMappers.size() >= 1;
        int lastIndex = parentMappers.size() - 1;
        ObjectMapper withNewMapper = parentMappers.get(lastIndex).mappingUpdate(mapper);
        if (merge) {
            withNewMapper = parentMappers.get(lastIndex).merge(withNewMapper);
        }
        parentMappers.set(lastIndex, withNewMapper);
    }

    /**
     * Removes mappers that exist on the stack, but are not part of the path of the current nameParts,
     * Returns the next unprocessed index from nameParts.
     */
    private static int removeUncommonMappers(List<ObjectMapper> parentMappers, String[] nameParts) {
        int keepBefore = 1;
        while (keepBefore < parentMappers.size() &&
            parentMappers.get(keepBefore).simpleName().equals(nameParts[keepBefore - 1])) {
            ++keepBefore;
        }
        popMappers(parentMappers, keepBefore, true);
        return keepBefore - 1;
    }

    /**
     * Adds mappers from the end of the stack that exist as updates within those mappers.
     * Returns the next unprocessed index from nameParts.
     */
    private static int expandCommonMappers(List<ObjectMapper> parentMappers, String[] nameParts, int i) {
        ObjectMapper last = parentMappers.get(parentMappers.size() - 1);
        while (i < nameParts.length - 1 && last.getMapper(nameParts[i]) != null) {
            Mapper newLast = last.getMapper(nameParts[i]);
            assert newLast instanceof ObjectMapper;
            last = (ObjectMapper) newLast;
            parentMappers.add(last);
            ++i;
        }
        return i;
    }

    /** Creates an update for intermediate object mappers that are not on the stack, but parents of newMapper. */
    private static ObjectMapper createExistingMapperUpdate(List<ObjectMapper> parentMappers, String[] nameParts, int i,
                                                           DocumentMapper docMapper, Mapper newMapper) {
        String updateParentName = nameParts[i];
        final ObjectMapper lastParent = parentMappers.get(parentMappers.size() - 1);
        if (parentMappers.size() > 1) {
            // only prefix with parent mapper if the parent mapper isn't the root (which has a fake name)
            updateParentName = lastParent.name() + '.' + nameParts[i];
        }
        ObjectMapper updateParent = docMapper.objectMappers().get(updateParentName);
        assert updateParent != null : updateParentName + " doesn't exist";
        return createUpdate(updateParent, nameParts, i + 1, newMapper);
    }

    /** Build an update for the parent which will contain the given mapper and any intermediate fields. */
    private static ObjectMapper createUpdate(ObjectMapper parent, String[] nameParts, int i, Mapper mapper) {
        List<ObjectMapper> parentMappers = new ArrayList<>();
        ObjectMapper previousIntermediate = parent;
        for (; i < nameParts.length - 1; ++i) {
            Mapper intermediate = previousIntermediate.getMapper(nameParts[i]);
            assert intermediate != null : "Field " + previousIntermediate.name() + " does not have a subfield " + nameParts[i];
            assert intermediate instanceof ObjectMapper;
            parentMappers.add((ObjectMapper)intermediate);
            previousIntermediate = (ObjectMapper)intermediate;
        }
        if (parentMappers.isEmpty() == false) {
            // add the new mapper to the stack, and pop down to the original parent level
            addToLastMapper(parentMappers, mapper, false);
            popMappers(parentMappers, 1, false);
            mapper = parentMappers.get(0);
        }
        return parent.mappingUpdate(mapper);
    }

    /**
     * 正常解析时会进入这个方法
     * @param context
     * @param mapper
     * @throws IOException
     */
    static void parseObjectOrNested(ParseContext context, ObjectMapper mapper) throws IOException {

        // 每个mapper 对象都有一个enabled 属性 用于判断可否被解析
        if (mapper.isEnabled() == false) {
            context.parser().skipChildren();
            return;
        }
        // 从上下文中获取解析格式化数据流的parser对象
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        // 代表对应的数据为null 也就无法解析
        if (token == XContentParser.Token.VALUE_NULL) {
            // the object is null ("obj1" : null), simply bail
            return;
        }

        String currentFieldName = parser.currentName();
        // ObjectMapper 解析到的不应该是数值类型  必然是 JsonObject
        if (token.isValue()) {
            throw new MapperParsingException("object mapping for [" + mapper.name() + "] tried to parse field [" + currentFieldName
                + "] as object, but found a concrete value");
        }

        // 代表该key对应的数据内 是否还嵌套了其他数据  比如 jsonObject
        /**
         * "value":{
         *     "param1":"",
         *     "param2":""
         * }
         */
        ObjectMapper.Nested nested = mapper.nested();
        // 内部还有嵌套数据
        if (nested.isNested()) {
            // 将当前上下文替换成 嵌套的新上下文 并且设置了一些元数据 (元数据被包装成field add到doc中)
            context = nestedContext(context, mapper);
        }

        // if we are at the end of the previous object, advance
        // 代表读取完了一个 jsonObject 读取下一个
        if (token == XContentParser.Token.END_OBJECT) {
            token = parser.nextToken();
        }
        // 切换到 FIELD_NAME
        if (token == XContentParser.Token.START_OBJECT) {
            // if we are just starting an OBJECT, advance, this is the object we are parsing, we need the name first
            token = parser.nextToken();
        }

        // 没有发生嵌套的时候 此时token应该就是 FIELD_NAME
        innerParseObject(context, mapper, parser, currentFieldName, token);
        // restore the enable path flag
        if (nested.isNested()) {
            nested(context, nested);
        }
    }

    /**
     * 开始解析数据流
     * @param context
     * @param mapper
     * @param parser
     * @param currentFieldName
     * @param token
     * @throws IOException
     */
    private static void innerParseObject(ParseContext context, ObjectMapper mapper, XContentParser parser,
                                         String currentFieldName, XContentParser.Token token) throws IOException {
        assert token == XContentParser.Token.FIELD_NAME || token == XContentParser.Token.END_OBJECT;
        String[] paths = null;
        // 因为START_OBJECT 后面可能就连接着END_OBJECT 那么就不用处理了
        while (token != XContentParser.Token.END_OBJECT) {
            // 对应 key   json格式数据 key都被认为是一个path
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                // 如果key 不包含 "." 直接返回 否则使用dot进行拆分
                paths = splitAndValidatePath(currentFieldName);
                // 如果是 内置的元数据field 抛出异常 不允许在结构化数据中直接写入元数据
                if (MapperService.isMetadataField(context.path().pathAsText(currentFieldName))) {
                    throw new MapperParsingException("Field [" + currentFieldName + "] is a metadata field and cannot be added inside"
                        + " a document. Use the index API request parameters.");
                    // 只要拆分后的path中 有一个对应的mapper对象 enabled == false 就无法解析   跳过数据体
                } else if (containsDisabledObjectMapper(mapper, paths)) {
                    parser.nextToken();
                    parser.skipChildren();
                }
            // 发生了嵌套
            } else if (token == XContentParser.Token.START_OBJECT) {
                parseObject(context, mapper, currentFieldName, paths);
            } else if (token == XContentParser.Token.START_ARRAY) {
                parseArray(context, mapper, currentFieldName, paths);
            } else if (token == XContentParser.Token.VALUE_NULL) {
                parseNullValue(context, mapper, currentFieldName, paths);
            } else if (token == null) {
                throw new MapperParsingException("object mapping for [" + mapper.name() + "] tried to parse field [" + currentFieldName
                    + "] as object, but got EOF, has a concrete value been provided to it?");
            } else if (token.isValue()) {
                parseValue(context, mapper, currentFieldName, token, paths);
            }
            token = parser.nextToken();
        }
    }

    private static void nested(ParseContext context, ObjectMapper.Nested nested) {
        ParseContext.Document nestedDoc = context.doc();
        ParseContext.Document parentDoc = nestedDoc.getParent();
        Settings settings = context.indexSettings().getSettings();
        if (nested.isIncludeInParent()) {
            addFields(settings, nestedDoc, parentDoc);
        }
        if (nested.isIncludeInRoot()) {
            ParseContext.Document rootDoc = context.rootDoc();
            // don't add it twice, if its included in parent, and we are handling the master doc...
            if (!nested.isIncludeInParent() || parentDoc != rootDoc) {
                addFields(settings, nestedDoc, rootDoc);
            }
        }
    }

    private static void addFields(Settings settings, ParseContext.Document nestedDoc, ParseContext.Document rootDoc) {
        String nestedPathFieldName = NestedPathFieldMapper.name(settings);
        for (IndexableField field : nestedDoc.getFields()) {
            if (field.name().equals(nestedPathFieldName) == false) {
                rootDoc.add(field);
            }
        }
    }

    /**
     * 当发生嵌套时 更新解析上下文
     * @param context
     * @param mapper
     * @return
     */
    private static ParseContext nestedContext(ParseContext context, ObjectMapper mapper) {

        // 在处理后context.doc() 将会返回一个新的doc 并且它以外层的doc作为parent  在格式化数据中 每个jsonObject 都被看做一个doc 并且json是可以不断嵌套的
        context = context.createNestedContext(mapper.fullPath());
        // 获取此时最新的doc 以及父级doc
        ParseContext.Document nestedDoc = context.doc();
        ParseContext.Document parentDoc = nestedDoc.getParent();

        // We need to add the uid or id to this nested Lucene document too,
        // If we do not do this then when a document gets deleted only the root Lucene document gets deleted and
        // not the nested Lucene documents! Besides the fact that we would have zombie Lucene documents, the ordering of
        // documents inside the Lucene index (document blocks) will be incorrect, as nested documents of different root
        // documents are then aligned with other root documents. This will lead tothe nested query, sorting, aggregations
        // and inner hits to fail or yield incorrect results.
        // 当发生了嵌套时 可以确定的是 子级jsonObject 必然包含一个 _id 字段
        // TODO 子级 doc在创建时 还没有声明任何field 那么父级doc 是在什么时候设置field的呢 ???
        IndexableField idField = parentDoc.getField(IdFieldMapper.NAME);
        // json格式下数据是按字段拆分并存储到 lucene中的 而不是将整个数据体存入到lucene 那么怎么查询 怎么将数据合并
        if (idField != null) {
            // We just need to store the id as indexed field, so that IndexWriter#deleteDocuments(term) can then
            // delete it when the root document is deleted too.
            // id被传递过去了
            nestedDoc.add(new Field(IdFieldMapper.NAME, idField.binaryValue(), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
        } else {
            throw new IllegalStateException("The root document of a nested document should have an _id field");
        }

        // 将嵌套地址包装成一个field 并增加到 doc中  就是说 写入到lucene中的除了格式化数据外 一些元数据也会被包装成field对象 并加入到doc中
        nestedDoc.add(NestedPathFieldMapper.field(context.indexSettings().getSettings(), mapper.nestedTypePath()));
        return context;
    }

    private static void parseObjectOrField(ParseContext context, Mapper mapper) throws IOException {
        if (mapper instanceof ObjectMapper) {
            parseObjectOrNested(context, (ObjectMapper) mapper);
        } else if (mapper instanceof FieldMapper) {
            FieldMapper fieldMapper = (FieldMapper) mapper;
            fieldMapper.parse(context);
            parseCopyFields(context, fieldMapper.copyTo().copyToFields());
        } else if (mapper instanceof FieldAliasMapper) {
            throw new IllegalArgumentException("Cannot write to a field alias [" + mapper.name() + "].");
        } else {
            throw new IllegalStateException("The provided mapper [" + mapper.name() + "] has an unrecognized type [" +
                mapper.getClass().getSimpleName() + "].");
        }
    }

    /**
     * 在解析过程中
     * @param context
     * @param mapper
     * @param currentFieldName
     * @param paths
     * @throws IOException
     */
    private static void parseObject(final ParseContext context, ObjectMapper mapper, String currentFieldName,
                                    String[] paths) throws IOException {
        assert currentFieldName != null;

        // 获取尾部path对应的mapper
        Mapper objectMapper = getMapper(mapper, currentFieldName, paths);
        if (objectMapper != null) {
            context.path().add(currentFieldName);
            parseObjectOrField(context, objectMapper);
            context.path().remove();
        } else {
            currentFieldName = paths[paths.length - 1];
            Tuple<Integer, ObjectMapper> parentMapperTuple = getDynamicParentMapper(context, paths, mapper);
            ObjectMapper parentMapper = parentMapperTuple.v2();
            ObjectMapper.Dynamic dynamic = dynamicOrDefault(parentMapper, context);
            if (dynamic == ObjectMapper.Dynamic.STRICT) {
                throw new StrictDynamicMappingException(mapper.fullPath(), currentFieldName);
            } else if (dynamic == ObjectMapper.Dynamic.TRUE) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, XContentFieldType.OBJECT);
                if (builder == null) {
                    builder = new ObjectMapper.Builder(currentFieldName).enabled(true);
                }
                Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings().getSettings(), context.path());
                objectMapper = builder.build(builderContext);
                context.addDynamicMapper(objectMapper);
                context.path().add(currentFieldName);
                parseObjectOrField(context, objectMapper);
                context.path().remove();
            } else {
                // not dynamic, read everything up to end object
                context.parser().skipChildren();
            }
            for (int i = 0; i < parentMapperTuple.v1(); i++) {
                context.path().remove();
            }
        }
    }

    private static void parseArray(ParseContext context, ObjectMapper parentMapper, String lastFieldName,
                                   String[] paths) throws IOException {
        String arrayFieldName = lastFieldName;

        Mapper mapper = getMapper(parentMapper, lastFieldName, paths);
        if (mapper != null) {
            // There is a concrete mapper for this field already. Need to check if the mapper
            // expects an array, if so we pass the context straight to the mapper and if not
            // we serialize the array components
            if (mapper instanceof ArrayValueMapperParser) {
                parseObjectOrField(context, mapper);
            } else {
                parseNonDynamicArray(context, parentMapper, lastFieldName, arrayFieldName);
            }
        } else {
            arrayFieldName = paths[paths.length - 1];
            lastFieldName = arrayFieldName;
            Tuple<Integer, ObjectMapper> parentMapperTuple = getDynamicParentMapper(context, paths, parentMapper);
            parentMapper = parentMapperTuple.v2();
            ObjectMapper.Dynamic dynamic = dynamicOrDefault(parentMapper, context);
            if (dynamic == ObjectMapper.Dynamic.STRICT) {
                throw new StrictDynamicMappingException(parentMapper.fullPath(), arrayFieldName);
            } else if (dynamic == ObjectMapper.Dynamic.TRUE) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, arrayFieldName, XContentFieldType.OBJECT);
                if (builder == null) {
                    parseNonDynamicArray(context, parentMapper, lastFieldName, arrayFieldName);
                } else {
                    Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings().getSettings(), context.path());
                    mapper = builder.build(builderContext);
                    assert mapper != null;
                    if (mapper instanceof ArrayValueMapperParser) {
                        context.addDynamicMapper(mapper);
                        context.path().add(arrayFieldName);
                        parseObjectOrField(context, mapper);
                        context.path().remove();
                    } else {
                        parseNonDynamicArray(context, parentMapper, lastFieldName, arrayFieldName);
                    }
                }
            } else {
                // TODO: shouldn't this skip, not parse?
                parseNonDynamicArray(context, parentMapper, lastFieldName, arrayFieldName);
            }
            for (int i = 0; i < parentMapperTuple.v1(); i++) {
                context.path().remove();
            }
        }
    }

    private static void parseNonDynamicArray(ParseContext context, ObjectMapper mapper,
                                             final String lastFieldName, String arrayFieldName) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token;
        final String[] paths = splitAndValidatePath(lastFieldName);
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                parseObject(context, mapper, lastFieldName, paths);
            } else if (token == XContentParser.Token.START_ARRAY) {
                parseArray(context, mapper, lastFieldName, paths);
            } else if (token == XContentParser.Token.VALUE_NULL) {
                parseNullValue(context, mapper, lastFieldName, paths);
            } else if (token == null) {
                throw new MapperParsingException("object mapping for [" + mapper.name() + "] with array for [" + arrayFieldName
                    + "] tried to parse as array, but got EOF, is there a mismatch in types for the same field?");
            } else {
                assert token.isValue();
                parseValue(context, mapper, lastFieldName, token, paths);
            }
        }
    }

    private static void parseValue(final ParseContext context, ObjectMapper parentMapper,
                                   String currentFieldName, XContentParser.Token token, String[] paths) throws IOException {
        if (currentFieldName == null) {
            throw new MapperParsingException("object mapping [" + parentMapper.name() + "] trying to serialize a value with"
                + " no field associated with it, current value [" + context.parser().textOrNull() + "]");
        }
        Mapper mapper = getMapper(parentMapper, currentFieldName, paths);
        if (mapper != null) {
            parseObjectOrField(context, mapper);
        } else {
            currentFieldName = paths[paths.length - 1];
            Tuple<Integer, ObjectMapper> parentMapperTuple = getDynamicParentMapper(context, paths, parentMapper);
            parentMapper = parentMapperTuple.v2();
            parseDynamicValue(context, parentMapper, currentFieldName, token);
            for (int i = 0; i < parentMapperTuple.v1(); i++) {
                context.path().remove();
            }
        }
    }

    private static void parseNullValue(ParseContext context, ObjectMapper parentMapper, String lastFieldName,
                                       String[] paths) throws IOException {
        // we can only handle null values if we have mappings for them
        Mapper mapper = getMapper(parentMapper, lastFieldName, paths);
        if (mapper != null) {
            // TODO: passing null to an object seems bogus?
            parseObjectOrField(context, mapper);
        } else if (parentMapper.dynamic() == ObjectMapper.Dynamic.STRICT) {
            throw new StrictDynamicMappingException(parentMapper.fullPath(), lastFieldName);
        }
    }

    private static Mapper.Builder<?,?> createBuilderFromFieldType(final ParseContext context,
                                                                  MappedFieldType fieldType, String currentFieldName) {
        Mapper.Builder builder = null;
        if (fieldType instanceof TextFieldType) {
            builder = context.root().findTemplateBuilder(context, currentFieldName, "text", XContentFieldType.STRING);
            if (builder == null) {
                builder = new TextFieldMapper.Builder(currentFieldName)
                        .addMultiField(new KeywordFieldMapper.Builder("keyword").ignoreAbove(256));
            }
        } else if (fieldType instanceof KeywordFieldType) {
            builder = context.root().findTemplateBuilder(context, currentFieldName, "keyword", XContentFieldType.STRING);
        } else {
            switch (fieldType.typeName()) {
            case DateFieldMapper.CONTENT_TYPE:
                builder = context.root().findTemplateBuilder(context, currentFieldName, XContentFieldType.DATE);
                break;
            case "long":
                builder = context.root().findTemplateBuilder(context, currentFieldName, "long", XContentFieldType.LONG);
                break;
            case "double":
                builder = context.root().findTemplateBuilder(context, currentFieldName, "double", XContentFieldType.DOUBLE);
                break;
            case "integer":
                builder = context.root().findTemplateBuilder(context, currentFieldName, "integer", XContentFieldType.LONG);
                break;
            case "float":
                builder = context.root().findTemplateBuilder(context, currentFieldName, "float", XContentFieldType.DOUBLE);
                break;
            case BooleanFieldMapper.CONTENT_TYPE:
                builder = context.root().findTemplateBuilder(context, currentFieldName, "boolean", XContentFieldType.BOOLEAN);
                break;
            default:
                break;
            }
        }
        if (builder == null) {
            Mapper.TypeParser.ParserContext parserContext = context.docMapperParser().parserContext();
            Mapper.TypeParser typeParser = parserContext.typeParser(fieldType.typeName());
            if (typeParser == null) {
                throw new MapperParsingException("Cannot generate dynamic mappings of type [" + fieldType.typeName()
                    + "] for [" + currentFieldName + "]");
            }
            builder = typeParser.parse(currentFieldName, new HashMap<>(), parserContext);
        }
        return builder;
    }

    private static Mapper.Builder<?, ?> newLongBuilder(String name) {
        return new NumberFieldMapper.Builder(name, NumberFieldMapper.NumberType.LONG);
    }

    private static Mapper.Builder<?, ?> newFloatBuilder(String name) {
        return new NumberFieldMapper.Builder(name, NumberFieldMapper.NumberType.FLOAT);
    }

    private static Mapper.Builder<?, ?> newDateBuilder(String name, DateFormatter dateTimeFormatter) {
        DateFieldMapper.Builder builder = new DateFieldMapper.Builder(name);
        if (dateTimeFormatter != null) {
            builder.format(dateTimeFormatter.pattern()).locale(dateTimeFormatter.locale());
        }
        return builder;
    }

    private static Mapper.Builder<?,?> createBuilderFromDynamicValue(final ParseContext context,
                                                                     XContentParser.Token token,
                                                                     String currentFieldName) throws IOException {
        if (token == XContentParser.Token.VALUE_STRING) {
            String text = context.parser().text();

            boolean parseableAsLong = false;
            try {
                Long.parseLong(text);
                parseableAsLong = true;
            } catch (NumberFormatException e) {
                // not a long number
            }

            boolean parseableAsDouble = false;
            try {
                Double.parseDouble(text);
                parseableAsDouble = true;
            } catch (NumberFormatException e) {
                // not a double number
            }

            if (parseableAsLong && context.root().numericDetection()) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, XContentFieldType.LONG);
                if (builder == null) {
                    builder = newLongBuilder(currentFieldName);
                }
                return builder;
            } else if (parseableAsDouble && context.root().numericDetection()) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, XContentFieldType.DOUBLE);
                if (builder == null) {
                    builder = newFloatBuilder(currentFieldName);
                }
                return builder;
            } else if (parseableAsLong == false && parseableAsDouble == false && context.root().dateDetection()) {
                // We refuse to match pure numbers, which are too likely to be
                // false positives with date formats that include eg.
                // `epoch_millis` or `YYYY`
                for (DateFormatter dateTimeFormatter : context.root().dynamicDateTimeFormatters()) {
                    try {
                        dateTimeFormatter.parse(text);
                    } catch (ElasticsearchParseException | DateTimeParseException | IllegalArgumentException e) {
                        // failure to parse this, continue
                        continue;
                    }
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, XContentFieldType.DATE);
                    if (builder == null) {
                        builder = newDateBuilder(currentFieldName, dateTimeFormatter);
                    }
                    if (builder instanceof DateFieldMapper.Builder) {
                        DateFieldMapper.Builder dateBuilder = (DateFieldMapper.Builder) builder;
                        if (dateBuilder.isFormatterSet() == false) {
                            dateBuilder.format(dateTimeFormatter.pattern()).locale(dateTimeFormatter.locale());
                        }
                    }
                    return builder;
                }
            }

            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, XContentFieldType.STRING);
            if (builder == null) {
                builder = new TextFieldMapper.Builder(currentFieldName)
                        .addMultiField(new KeywordFieldMapper.Builder("keyword").ignoreAbove(256));
            }
            return builder;
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            XContentParser.NumberType numberType = context.parser().numberType();
            if (numberType == XContentParser.NumberType.INT
                    || numberType == XContentParser.NumberType.LONG
                    || numberType == XContentParser.NumberType.BIG_INTEGER) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, XContentFieldType.LONG);
                if (builder == null) {
                    builder = newLongBuilder(currentFieldName);
                }
                return builder;
            } else if (numberType == XContentParser.NumberType.FLOAT
                    || numberType == XContentParser.NumberType.DOUBLE
                    || numberType == XContentParser.NumberType.BIG_DECIMAL) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, XContentFieldType.DOUBLE);
                if (builder == null) {
                    // no templates are defined, we use float by default instead of double
                    // since this is much more space-efficient and should be enough most of
                    // the time
                    builder = newFloatBuilder(currentFieldName);
                }
                return builder;
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, XContentFieldType.BOOLEAN);
            if (builder == null) {
                builder = new BooleanFieldMapper.Builder(currentFieldName);
            }
            return builder;
        } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, XContentFieldType.BINARY);
            if (builder == null) {
                builder = new BinaryFieldMapper.Builder(currentFieldName);
            }
            return builder;
        } else {
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, XContentFieldType.STRING);
            if (builder != null) {
                return builder;
            }
        }
        // TODO how do we identify dynamically that its a binary value?
        throw new IllegalStateException("Can't handle serializing a dynamic type with content token [" + token + "] and field name ["
            + currentFieldName + "]");
    }

    private static void parseDynamicValue(final ParseContext context, ObjectMapper parentMapper,
                                          String currentFieldName, XContentParser.Token token) throws IOException {
        ObjectMapper.Dynamic dynamic = dynamicOrDefault(parentMapper, context);
        if (dynamic == ObjectMapper.Dynamic.STRICT) {
            throw new StrictDynamicMappingException(parentMapper.fullPath(), currentFieldName);
        }
        if (dynamic == ObjectMapper.Dynamic.FALSE) {
            return;
        }
        final String path = context.path().pathAsText(currentFieldName);
        final Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings().getSettings(), context.path());
        final MappedFieldType existingFieldType = context.mapperService().fieldType(path);
        final Mapper.Builder builder;
        if (existingFieldType != null) {
            // create a builder of the same type
            builder = createBuilderFromFieldType(context, existingFieldType, currentFieldName);
        } else {
            builder = createBuilderFromDynamicValue(context, token, currentFieldName);
        }
        Mapper mapper = builder.build(builderContext);
        if (existingFieldType != null) {
            // try to not introduce a conflict
            mapper = mapper.updateFieldType(Collections.singletonMap(path, existingFieldType));
        }
        context.addDynamicMapper(mapper);

        parseObjectOrField(context, mapper);
    }

    /** Creates instances of the fields that the current field should be copied to */
    private static void parseCopyFields(ParseContext context, List<String> copyToFields) throws IOException {
        if (!context.isWithinCopyTo() && copyToFields.isEmpty() == false) {
            context = context.createCopyToContext();
            for (String field : copyToFields) {
                // In case of a hierarchy of nested documents, we need to figure out
                // which document the field should go to
                ParseContext.Document targetDoc = null;
                for (ParseContext.Document doc = context.doc(); doc != null; doc = doc.getParent()) {
                    if (field.startsWith(doc.getPrefix())) {
                        targetDoc = doc;
                        break;
                    }
                }
                assert targetDoc != null;
                final ParseContext copyToContext;
                if (targetDoc == context.doc()) {
                    copyToContext = context;
                } else {
                    copyToContext = context.switchDoc(targetDoc);
                }
                parseCopy(field, copyToContext);
            }
        }
    }

    /** Creates an copy of the current field with given field name and boost */
    private static void parseCopy(String field, ParseContext context) throws IOException {
        Mapper mapper = context.docMapper().mappers().getMapper(field);
        if (mapper != null) {
            if (mapper instanceof FieldMapper) {
                ((FieldMapper) mapper).parse(context);
            } else if (mapper instanceof FieldAliasMapper) {
                throw new IllegalArgumentException("Cannot copy to a field alias [" + mapper.name() + "].");
            } else {
                throw new IllegalStateException("The provided mapper [" + mapper.name() +
                    "] has an unrecognized type [" + mapper.getClass().getSimpleName() + "].");
            }
        } else {
            // The path of the dest field might be completely different from the current one so we need to reset it
            context = context.overridePath(new ContentPath(0));

            final String[] paths = splitAndValidatePath(field);
            final String fieldName = paths[paths.length-1];
            Tuple<Integer, ObjectMapper> parentMapperTuple = getDynamicParentMapper(context, paths, null);
            ObjectMapper objectMapper = parentMapperTuple.v2();
            parseDynamicValue(context, objectMapper, fieldName, context.parser().currentToken());
            for (int i = 0; i < parentMapperTuple.v1(); i++) {
                context.path().remove();
            }
        }
    }

    private static Tuple<Integer, ObjectMapper> getDynamicParentMapper(ParseContext context, final String[] paths,
            ObjectMapper currentParent) {
        ObjectMapper mapper = currentParent == null ? context.root() : currentParent;
        int pathsAdded = 0;
        ObjectMapper parent = mapper;
        for (int i = 0; i < paths.length-1; i++) {
        String currentPath = context.path().pathAsText(paths[i]);
        Mapper existingFieldMapper = context.docMapper().mappers().getMapper(currentPath);
        if (existingFieldMapper != null) {
            throw new MapperParsingException(
                    "Could not dynamically add mapping for field [{}]. Existing mapping for [{}] must be of type object but found [{}].",
                    null, String.join(".", paths), currentPath, existingFieldMapper.typeName());
        }
        mapper = context.docMapper().objectMappers().get(currentPath);
            if (mapper == null) {
                // One mapping is missing, check if we are allowed to create a dynamic one.
                ObjectMapper.Dynamic dynamic = dynamicOrDefault(parent, context);

                switch (dynamic) {
                    case STRICT:
                        throw new StrictDynamicMappingException(parent.fullPath(), paths[i]);
                    case TRUE:
                        Mapper.Builder builder = context.root().findTemplateBuilder(context, paths[i], XContentFieldType.OBJECT);
                        if (builder == null) {
                            builder = new ObjectMapper.Builder(paths[i]).enabled(true);
                        }
                        Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings().getSettings(),
                            context.path());
                        mapper = (ObjectMapper) builder.build(builderContext);
                        if (mapper.nested() != ObjectMapper.Nested.NO) {
                            throw new MapperParsingException("It is forbidden to create dynamic nested objects (["
                                + context.path().pathAsText(paths[i]) + "]) through `copy_to` or dots in field names");
                        }
                        context.addDynamicMapper(mapper);
                        break;
                    case FALSE:
                       // Should not dynamically create any more mappers so return the last mapper
                    return new Tuple<>(pathsAdded, parent);

                }
            }
            context.path().add(paths[i]);
            pathsAdded++;
            parent = mapper;
        }
        return new Tuple<>(pathsAdded, mapper);
    }

    // find what the dynamic setting is given the current parse context and parent
    private static ObjectMapper.Dynamic dynamicOrDefault(ObjectMapper parentMapper, ParseContext context) {
        ObjectMapper.Dynamic dynamic = parentMapper.dynamic();
        while (dynamic == null) {
            int lastDotNdx = parentMapper.name().lastIndexOf('.');
            if (lastDotNdx == -1) {
                // no dot means we the parent is the root, so just delegate to the default outside the loop
                break;
            }
            String parentName = parentMapper.name().substring(0, lastDotNdx);
            parentMapper = context.docMapper().objectMappers().get(parentName);
            if (parentMapper == null) {
                // If parentMapper is ever null, it means the parent of the current mapper was dynamically created.
                // But in order to be created dynamically, the dynamic setting of that parent was necessarily true
                return ObjectMapper.Dynamic.TRUE;
            }
            dynamic = parentMapper.dynamic();
        }
        if (dynamic == null) {
            return context.root().dynamic() == null ? ObjectMapper.Dynamic.TRUE : context.root().dynamic();
        }
        return dynamic;
    }

    // looks up a child mapper, but takes into account field names that expand to objects
    // 从mapper 中获取关联的其他mapper
    private static Mapper getMapper(ObjectMapper objectMapper, String fieldName, String[] subfields) {
        for (int i = 0; i < subfields.length - 1; ++i) {
            // TODO mapper是什么时候设置的啊
            Mapper mapper = objectMapper.getMapper(subfields[i]);
            // 要求必须是 ObjectMapper类型
            if (mapper == null || (mapper instanceof ObjectMapper) == false) {
                return null;
            }
            objectMapper = (ObjectMapper)mapper;
            // 此时不允许出现嵌套了
            if (objectMapper.nested().isNested()) {
                throw new MapperParsingException("Cannot add a value for field ["
                        + fieldName + "] since one of the intermediate objects is mapped as a nested object: ["
                        + mapper.name() + "]");
            }
        }
        return objectMapper.getMapper(subfields[subfields.length - 1]);
    }
}
