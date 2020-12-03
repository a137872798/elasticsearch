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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/**
 * A field mapper for boolean fields.
 * 该对象与 BinaryFieldMapper类似 不过内部存储的类型是boolean
 */
public class BooleanFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "boolean";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new BooleanFieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.freeze();
        }
    }

    public static class Values {
        public static final BytesRef TRUE = new BytesRef("T");
        public static final BytesRef FALSE = new BytesRef("F");
    }

    /**
     * 每个builder的任务 就是将生成匹配的mapper 对象
     */
    public static class Builder extends FieldMapper.Builder<Builder, BooleanFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        @Override
        public BooleanFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            // 这个mapper对象也关联到一个 multiFields上么
            return new BooleanFieldMapper(name, fieldType, defaultFieldType,
                context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    /**
     * 比如某个属性的 type是 boolean 类型 那么在解析 属性对应的数据体时 就会委托给该对象
     */
    public static class TypeParser implements Mapper.TypeParser {

        /**
         *
         * @param name 本次被解析的属性名
         * @param node 这个属性对应的 数据体
         * @param parserContext  包含了解析过程中需要的各种参数的上下文对象
         * @return
         * @throws MapperParsingException
         */
        @Override
        public BooleanFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            BooleanFieldMapper.Builder builder = new BooleanFieldMapper.Builder(name);
            // 从node中获取相关信息 并填充到builder中
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                // 代表检测到一个  null_value 的属性
                if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    // 设置是否 没有数据
                    builder.nullValue(XContentMapValues.nodeBooleanValue(propNode, name + ".null_value"));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class BooleanFieldType extends TermBasedFieldType {

        public BooleanFieldType() {}

        protected BooleanFieldType(BooleanFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new BooleanFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                // 应该是有个特殊的field 以"field_name" 作为field 内部存储的数据都是 field名字
                // 所以这里将field作为一个term去查询数据
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public Boolean nullValue() {
            return (Boolean)super.nullValue();
        }

        /**
         * 父类 将某个value 转换成 termQuery时会间接调用该方法
         * 这里要求传入的value 必须能表示成 boolean类型 之后转换成2个常量的 bytesRef
         * @param value
         * @return
         */
        @Override
        public BytesRef indexedValueForSearch(Object value) {
            if (value == null) {
                return Values.FALSE;
            }
            if (value instanceof Boolean) {
                return ((Boolean) value) ? Values.TRUE : Values.FALSE;
            }
            String sValue;
            if (value instanceof BytesRef) {
                sValue = ((BytesRef) value).utf8ToString();
            } else {
                sValue = value.toString();
            }
            switch (sValue) {
                case "true":
                    return Values.TRUE;
                case "false":
                    return Values.FALSE;
                default:
                    throw new IllegalArgumentException("Can't parse boolean value [" +
                                    sValue + "], expected [true] or [false]");
            }
        }

        @Override
        public Boolean valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            switch(value.toString()) {
            case "F":
                return false;
            case "T":
                return true;
            default:
                throw new IllegalArgumentException("Expected [T] or [F] but got [" + value + "]");
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder().numericType(NumericType.BOOLEAN);
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            return CoreValuesSourceType.BOOLEAN;
        }

        /**
         * 该对象可以将数据格式化输出
         * @param format
         * @param timeZone
         * @return
         */
        @Override
        public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
            }
            if (timeZone != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName()
                    + "] does not support custom time zones");
            }
            return DocValueFormat.BOOLEAN;
        }

        /**
         * 符合该term范围的数据都可以查询出来
         * @param lowerTerm
         * @param upperTerm
         * @param includeLower
         * @param includeUpper
         * @param context
         * @return
         */
        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            failIfNotIndexed();
            return new TermRangeQuery(name(),
                lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                upperTerm == null ? null : indexedValueForSearch(upperTerm),
                includeLower, includeUpper);
        }
    }

    protected BooleanFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                 Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    @Override
    public BooleanFieldType fieldType() {
        return (BooleanFieldType) super.fieldType();
    }

    /**
     * 在 Mapper对象解析 parserContext时 会间接触发该方法
     * @param context
     * @param fields
     * @throws IOException
     */
    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        // indexOptions 代表在基于分词器进行解析时 要存储的数据
        if (fieldType().indexOptions() == IndexOptions.NONE && !fieldType().stored() && !fieldType().hasDocValues()) {
            return;
        }

        // 将外部传入的值转换成boolean 类型
        Boolean value = context.parseExternalValue(Boolean.class);
        if (value == null) {
            // 如果外部没有设置值 首先想到的是从 parser对象中解析格式化数据 并取出value
            XContentParser.Token token = context.parser().currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                if (fieldType().nullValue() != null) {
                    value = fieldType().nullValue();
                }
            } else {
                value = context.parser().booleanValue();
            }
        }

        if (value == null) {
            return;
        }
        // 增加2个特殊的field
        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            fields.add(new Field(fieldType().name(), value ? "T" : "F", fieldType()));
        }
        if (fieldType().hasDocValues()) {
            fields.add(new SortedNumericDocValuesField(fieldType().name(), value ? 1 : 0));
        } else {
            createFieldNamesField(context, fields);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValue());
        }
    }
}
