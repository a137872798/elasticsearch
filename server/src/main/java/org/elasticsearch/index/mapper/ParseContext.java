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

import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.ObjectObjectMap;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * 解析上下文  本身支持迭代一组doc
 */
public abstract class ParseContext implements Iterable<ParseContext.Document>{

    /**
     * Fork of {@link org.apache.lucene.document.Document} with additional functionality.
     * 在解析格式化数据的过程中 每个 jsonObject 都被认为是一个document 每当发生嵌套时 就会生成一个新的document
     */
    public static class Document implements Iterable<IndexableField> {

        /**
         * doc 本身会形成链式结构么  之前应该是不需要的
         */
        private final Document parent;
        /**
         * 该doc的数据存储在哪个路径下
         */
        private final String path;
        private final String prefix;

        /**
         * 当前doc下所有的field信息
         */
        private final List<IndexableField> fields;

        /**
         * 支持通过关键字查询field
         */
        private ObjectObjectMap<Object, IndexableField> keyedFields;

        private Document(String path, Document parent) {
            fields = new ArrayList<>();
            this.path = path;
            this.prefix = path.isEmpty() ? "" : path + ".";
            this.parent = parent;
        }

        /**
         * 默认情况下创建的doc 是单层级的 同时path为""
         */
        public Document() {
            this("", null);
        }

        /**
         * Return the path associated with this document.
         */
        public String getPath() {
            return path;
        }

        /**
         * Return a prefix that all fields in this document should have.
         */
        public String getPrefix() {
            return prefix;
        }

        /**
         * Return the parent document, or null if this is the root document.
         */
        public Document getParent() {
            return parent;
        }

        @Override
        public Iterator<IndexableField> iterator() {
            return fields.iterator();
        }

        public List<IndexableField> getFields() {
            return fields;
        }

        /**
         * 在当前doc下追加一个field
         * @param field
         */
        public void add(IndexableField field) {
            // either a meta fields or starts with the prefix
            assert field.name().startsWith("_") || field.name().startsWith(prefix) : field.name() + " " + prefix;
            fields.add(field);
        }

        /** Add fields so that they can later be fetched using {@link #getByKey(Object)}. */
        public void addWithKey(Object key, IndexableField field) {
            if (keyedFields == null) {
                keyedFields = new ObjectObjectHashMap<>();
            } else if (keyedFields.containsKey(key)) {
                throw new IllegalStateException("Only one field can be stored per key");
            }
            // 将field 存储到map中  并且会间接加入到list中
            keyedFields.put(key, field);
            add(field);
        }

        /**
         * Get back fields that have been previously added with {@link #addWithKey(Object, IndexableField)}.
         * 通过 key查找field
         */
        public IndexableField getByKey(Object key) {
            return keyedFields == null ? null : keyedFields.get(key);
        }

        /**
         * 找到该doc 下所有名字相同的field  我记得在lucene中不同field 要求名字不能相同  也就是这里是查询在该doc下相同的field出现了多少次
         * @param name
         * @return
         */
        public IndexableField[] getFields(String name) {
            List<IndexableField> f = new ArrayList<>();
            for (IndexableField field : fields) {
                if (field.name().equals(name)) {
                    f.add(field);
                }
            }
            return f.toArray(new IndexableField[f.size()]);
        }

        /**
         * 找到第一个名字匹配的field
         * @param name
         * @return
         */
        public IndexableField getField(String name) {
            for (IndexableField field : fields) {
                if (field.name().equals(name)) {
                    return field;
                }
            }
            return null;
        }

        /**
         * 如果field 存储的数据可以转换成string类型 则将命中的field.value 返回
         * @param name
         * @return
         */
        public String get(String name) {
            for (IndexableField f : fields) {
                if (f.name().equals(name) && f.stringValue() != null) {
                    return f.stringValue();
                }
            }
            return null;
        }

        /**
         * 将field的二进制数据返回
         * @param name
         * @return
         */
        public BytesRef getBinaryValue(String name) {
            for (IndexableField f : fields) {
                if (f.name().equals(name) && f.binaryValue() != null) {
                    return f.binaryValue();
                }
            }
            return null;
        }

    }

    /**
     * 追加一层过滤功能
     */
    private static class FilterParseContext extends ParseContext {

        private final ParseContext in;

        private FilterParseContext(ParseContext in) {
            this.in = in;
        }

        @Override
        public Iterable<Document> nonRootDocuments() {
            return in.nonRootDocuments();
        }

        @Override
        public DocumentMapperParser docMapperParser() {
            return in.docMapperParser();
        }

        @Override
        public boolean isWithinCopyTo() {
            return in.isWithinCopyTo();
        }

        @Override
        public boolean isWithinMultiFields() {
            return in.isWithinMultiFields();
        }

        @Override
        public IndexSettings indexSettings() {
            return in.indexSettings();
        }

        @Override
        public SourceToParse sourceToParse() {
            return in.sourceToParse();
        }

        @Override
        public ContentPath path() {
            return in.path();
        }

        @Override
        public XContentParser parser() {
            return in.parser();
        }

        @Override
        public Document rootDoc() {
            return in.rootDoc();
        }

        @Override
        public Document doc() {
            return in.doc();
        }

        @Override
        protected void addDoc(Document doc) {
            in.addDoc(doc);
        }

        @Override
        public RootObjectMapper root() {
            return in.root();
        }

        @Override
        public DocumentMapper docMapper() {
            return in.docMapper();
        }

        @Override
        public MapperService mapperService() {
            return in.mapperService();
        }

        @Override
        public Field version() {
            return in.version();
        }

        @Override
        public void version(Field version) {
            in.version(version);
        }

        @Override
        public SeqNoFieldMapper.SequenceIDFields seqID() {
            return in.seqID();
        }

        @Override
        public void seqID(SeqNoFieldMapper.SequenceIDFields seqID) {
            in.seqID(seqID);
        }

        @Override
        public boolean externalValueSet() {
            return in.externalValueSet();
        }

        @Override
        public Object externalValue() {
            return in.externalValue();
        }

        @Override
        public void addDynamicMapper(Mapper update) {
            in.addDynamicMapper(update);
        }

        @Override
        public List<Mapper> getDynamicMappers() {
            return in.getDynamicMappers();
        }

        @Override
        public Iterator<Document> iterator() {
            return in.iterator();
        }

        @Override
        public void addIgnoredField(String field) {
            in.addIgnoredField(field);
        }

        @Override
        public Collection<String> getIgnoredFields() {
            return in.getIgnoredFields();
        }
    }

    /**
     * 每个解析上下文 应当存储了一组doc  每个doc下又存储了一组field   在parseContext中doc可能存在父子级关系
     */
    public static class InternalParseContext extends ParseContext {

        /**
         * TODO
         */
        private final DocumentMapper docMapper;

        /**
         * TODO
         */
        private final DocumentMapperParser docMapperParser;

        /**
         * 指定的是doc的路径么
         */
        private final ContentPath path;

        /**
         * 将格式化字符串转换成实体 比如json字符串转换成 bean
         */
        private final XContentParser parser;

        /**
         * 代表root doc
         */
        private Document document;

        /**
         * 因为 parseContext 实现了 Doc迭代器接口
         */
        private final List<Document> documents;

        /**
         * 所以相关的全部配置都可以从对象中获取
         */
        private final IndexSettings indexSettings;

        /**
         * 该对象内存储了一个数据流 以及转换器对象
         * 该对象还包含了routing信息
         */
        private final SourceToParse sourceToParse;

        private Field version;

        /**
         * TODO
         */
        private SeqNoFieldMapper.SequenceIDFields seqID;

        /**
         * doc最大嵌套层数
         */
        private final long maxAllowedNumNestedDocs;

        /**
         * 当前有多少doc嵌套  实际上就是docs的列表长度
         */
        private long numNestedDocs;

        /**
         * 动态映射器
         */
        private final List<Mapper> dynamicMappers;

        private boolean docsReversed = false;

        private final Set<String> ignoredFields = new HashSet<>();


        /**
         *
         * @param indexSettings
         * @param docMapperParser
         * @param docMapper
         * @param source
         * @param parser  负责解析数据流
         */
        public InternalParseContext(IndexSettings indexSettings, DocumentMapperParser docMapperParser, DocumentMapper docMapper,
                                    SourceToParse source, XContentParser parser) {
            this.indexSettings = indexSettings;
            this.docMapper = docMapper;
            this.docMapperParser = docMapperParser;
            this.path = new ContentPath(0);
            this.parser = parser;
            // 初始阶段生成了一个空的doc
            this.document = new Document();
            this.documents = new ArrayList<>();
            this.documents.add(document);
            this.version = null;
            this.sourceToParse = source;
            this.dynamicMappers = new ArrayList<>();
            // 从配置项中获取最大嵌套层数
            this.maxAllowedNumNestedDocs = indexSettings.getValue(MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING);
            this.numNestedDocs = 0L;
        }

        @Override
        public DocumentMapperParser docMapperParser() {
            return this.docMapperParser;
        }

        @Override
        public IndexSettings indexSettings() {
            return this.indexSettings;
        }

        @Override
        public SourceToParse sourceToParse() {
            return this.sourceToParse;
        }

        @Override
        public ContentPath path() {
            return this.path;
        }

        @Override
        public XContentParser parser() {
            return this.parser;
        }

        @Override
        public Document rootDoc() {
            return documents.get(0);
        }

        List<Document> docs() {
            return this.documents;
        }

        /**
         * 返回 root doc
         * @return
         */
        @Override
        public Document doc() {
            return this.document;
        }

        /**
         * 每当格式化数据产生一次嵌套时 就会生成一个新的doc 并且以之前的doc作为parent
         * @param doc
         */
        @Override
        protected void addDoc(Document doc) {
            numNestedDocs ++;
            if (numNestedDocs > maxAllowedNumNestedDocs) {
                throw new MapperParsingException(
                    "The number of nested documents has exceeded the allowed limit of [" + maxAllowedNumNestedDocs + "]."
                        + " This limit can be set by changing the [" + MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey()
                        + "] index level setting.");
            }
            this.documents.add(doc);
        }

        /**
         * DocMapper 包含一个根映射对象 类型是 RootObjectMapper
         * @return
         */
        @Override
        public RootObjectMapper root() {
            return docMapper.root();
        }

        /**
         * 所有doc 共用一个 docMapper
         * @return
         */
        @Override
        public DocumentMapper docMapper() {
            return this.docMapper;
        }

        /**
         * 获取到关联的映射服务
         * @return
         */
        @Override
        public MapperService mapperService() {
            return docMapperParser.mapperService;
        }

        @Override
        public Field version() {
            return this.version;
        }

        @Override
        public void version(Field version) {
            this.version = version;
        }

        @Override
        public SeqNoFieldMapper.SequenceIDFields seqID() {
            return this.seqID;
        }

        @Override
        public void seqID(SeqNoFieldMapper.SequenceIDFields seqID) {
            this.seqID = seqID;
        }

        @Override
        public void addDynamicMapper(Mapper mapper) {
            dynamicMappers.add(mapper);
        }

        @Override
        public List<Mapper> getDynamicMappers() {
            return dynamicMappers;
        }

        /**
         * 返回除了 root doc 外的其他doc
         * @return
         */
        @Override
        public Iterable<Document> nonRootDocuments() {
            if (docsReversed) {
                throw new IllegalStateException("documents are already reversed");
            }
            return documents.subList(1, documents.size());
        }

        void postParse() {
            if (documents.size() > 1) {
                docsReversed = true;
                // We preserve the order of the children while ensuring that parents appear after them.
                List<Document> newDocs = reorderParent(documents);
                documents.clear();
                documents.addAll(newDocs);
            }
        }

        /**
         * Returns a copy of the provided {@link List} where parent documents appear
         * after their children.
         * TODO 离开了使用场景暂时看不懂这个
         */
        private List<Document> reorderParent(List<Document> docs) {
            List<Document> newDocs = new ArrayList<>(docs.size());
            LinkedList<Document> parents = new LinkedList<>();
            for (Document doc : docs) {
                while (parents.peek() != doc.getParent()){
                    newDocs.add(parents.poll());
                }
                parents.add(0, doc);
            }
            newDocs.addAll(parents);
            return newDocs;
        }

        @Override
        public Iterator<Document> iterator() {
            return documents.iterator();
        }


        @Override
        public void addIgnoredField(String field) {
            ignoredFields.add(field);
        }

        @Override
        public Collection<String> getIgnoredFields() {
            return Collections.unmodifiableCollection(ignoredFields);
        }
    }

    /**
     * Returns an Iterable over all non-root documents. If there are no non-root documents
     * the iterable will return an empty iterator.
     */
    public abstract Iterable<Document> nonRootDocuments();


    /**
     * Add the given {@code field} to the set of ignored fields.
     */
    public abstract void addIgnoredField(String field);

    /**
     * Return the collection of fields that have been ignored so far.
     */
    public abstract Collection<String> getIgnoredFields();

    public abstract DocumentMapperParser docMapperParser();

    /**
     * Return a new context that will be within a copy-to operation.
     */
    public final ParseContext createCopyToContext() {
        return new FilterParseContext(this) {
            @Override
            public boolean isWithinCopyTo() {
                return true;
            }
        };
    }

    public boolean isWithinCopyTo() {
        return false;
    }

    /**
     * Return a new context that will be within multi-fields.
     * 将当前上下文包装成支持 multiField
     */
    public final ParseContext createMultiFieldContext() {
        return new FilterParseContext(this) {
            @Override
            public boolean isWithinMultiFields() {
                return true;
            }
        };
    }

    /**
     * Return a new context that will be used within a nested document.
     * @param fullPath 当前mapper所在的路径
     * 在解析嵌套的格式化数据时 比如某个 jsonObject 内部又嵌套了一个jsonObject
     */
    public final ParseContext createNestedContext(String fullPath) {
        // 将rootDoc 追加fullPath后重新返回并加入到list中
        // doc() 负责获取上下文当前正在使用的doc  并且它会作为新的doc的parent
        final Document doc = new Document(fullPath, doc());
        addDoc(doc);
        return switchDoc(doc);
    }

    /**
     * Return a new context that has the provided document as the current document.
     * 切换doc() 函数返回的doc变量  原本默认是返回rootDoc
     */
    public final ParseContext switchDoc(final Document document) {
        return new FilterParseContext(this) {
            @Override
            public Document doc() {
                return document;
            }
        };
    }

    /**
     * Return a new context that will have the provided path.
     * 更改path 返回的属性
     */
    public final ParseContext overridePath(final ContentPath path) {
        return new FilterParseContext(this) {
            @Override
            public ContentPath path() {
                return path;
            }
        };
    }

    public boolean isWithinMultiFields() {
        return false;
    }

    public abstract IndexSettings indexSettings();

    public abstract SourceToParse sourceToParse();

    public abstract ContentPath path();

    public abstract XContentParser parser();

    public abstract Document rootDoc();

    public abstract Document doc();

    protected abstract void addDoc(Document doc);

    public abstract RootObjectMapper root();

    public abstract DocumentMapper docMapper();

    public abstract MapperService mapperService();

    public abstract Field version();

    public abstract void version(Field version);

    public abstract SeqNoFieldMapper.SequenceIDFields seqID();

    public abstract void seqID(SeqNoFieldMapper.SequenceIDFields seqID);

    /**
     * Return a new context that will have the external value set.
     * 可以从外部设置value的 parseContext
     */
    public final ParseContext createExternalValueContext(final Object externalValue) {
        return new FilterParseContext(this) {
            @Override
            public boolean externalValueSet() {
                return true;
            }
            @Override
            public Object externalValue() {
                return externalValue;
            }
        };
    }

    public boolean externalValueSet() {
        return false;
    }

    public Object externalValue() {
        throw new IllegalStateException("External value is not set");
    }

    /**
     * Try to parse an externalValue if any
     * @param clazz Expected class for external value
     * @return null if no external value has been set or the value
     * 尝试将 externalValue 转换成目标类型
     */
    public final <T> T parseExternalValue(Class<T> clazz) {
        if (!externalValueSet() || externalValue() == null) {
            return null;
        }

        if (!clazz.isInstance(externalValue())) {
            throw new IllegalArgumentException("illegal external value class ["
                    + externalValue().getClass().getName() + "]. Should be " + clazz.getName());
        }
        return clazz.cast(externalValue());
    }

    /**
     * Add a new mapper dynamically created while parsing.
     */
    public abstract void addDynamicMapper(Mapper update);

    /**
     * Get dynamic mappers created while parsing.
     */
    public abstract List<Mapper> getDynamicMappers();
}
