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

import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.BytesBinaryDVIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/**
 * 明确该field的值为byte[]
 * 每个mapper 会包含一个 MultiFields(该对象内部存储了一组mapper)
 */
public class BinaryFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "binary";

    /**
     * 默认情况下 二进制数据流是不将数据存储到索引文件中的
     */
    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new BinaryFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.freeze();
        }
    }

    /**
     * builder对象是用来生成mapper的
     */
    public static class Builder extends FieldMapper.Builder<Builder, BinaryFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public BinaryFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new BinaryFieldMapper(name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public BinaryFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            // 先创建一个空的builder对象 之后解析参数 并将相关信息填充到builder中
            BinaryFieldMapper.Builder builder = new BinaryFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    /**
     * 该mapper对象相关的 fieldType
     */
    static final class BinaryFieldType extends MappedFieldType {

        BinaryFieldType() {}

        protected BinaryFieldType(BinaryFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new BinaryFieldType(this);
        }

        /**
         * 返回 field.value 的类型
         * @return
         */
        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        /**
         * DocValueFormat 可以将field.value 格式化
         * @param format
         * @param timeZone
         * @return
         */
        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return DocValueFormat.BINARY;
        }

        @Override
        public BytesReference valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }

            BytesReference bytes;
            if (value instanceof BytesRef) {
                bytes = new BytesArray((BytesRef) value);
            } else if (value instanceof BytesReference) {
                bytes = (BytesReference) value;
            } else if (value instanceof byte[]) {
                bytes = new BytesArray((byte[]) value);
            } else {
                bytes = new BytesArray(Base64.getDecoder().decode(value.toString()));
            }
            return bytes;
        }

        /**
         * 该builder对象传入相关参数可以生成 DVIndexFieldData
         * @param fullyQualifiedIndexName the name of the index this field-data is build for
         *
         * @return
         */
        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            // 如果当前field 没有存储docValue 不允许调用该函数
            failIfNoDocValues();
            return new BytesBinaryDVIndexFieldData.Builder();
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            return CoreValuesSourceType.BYTES;
        }

        /**
         * 生成查询对象
         * @param context
         * @return
         */
        @Override
        public Query existsQuery(QueryShardContext context) {
            // 如果保存了未分词的版本 也就是直接存储docValue
            if (hasDocValues()) {
                // 那么生成针对整个doc数据进行查询的对象
                return new DocValuesFieldExistsQuery(name());
            } else {
                // 生成仅针对某个分词进行查询的对象
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Binary fields do not support searching");
        }
    }

    protected BinaryFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    /**
     * 使用Mapper 对下个解析context内存储的所有doc  并将结果填充到fields中
     * @param context
     * @param fields
     * @throws IOException
     */
    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        // 如果没有存储docValue 那么无法进行填充
        if (!fieldType().stored() && !fieldType().hasDocValues()) {
            return;
        }

        // TODO 到底是在解析什么样的外部数据啊
        // 将外部设置的值转换成byte[] 类型
        byte[] value = context.parseExternalValue(byte[].class);
        if (value == null) {
            // doc内部的数据是从结构化数据还原的 比如json格式
            if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
                return;
            } else {
                // 将当前读取到的值转换成 byte[]
                value = context.parser().binaryValue();
            }
        }
        // 此时没有数据
        if (value == null) {
            return;
        }
        // 如果是支持排序的
        if (fieldType().stored()) {
            fields.add(new Field(fieldType().name(), value, fieldType()));
        }

        // 如果该field 存储的完整的数据
        if (fieldType().hasDocValues()) {
            // 获取 field对应的docValue 列表
            // doc内部的数据是预先存进去的
            CustomBinaryDocValuesField field = (CustomBinaryDocValuesField) context.doc().getByKey(fieldType().name());
            if (field == null) {
                // 如果没有取到数据 构建一个空对象
                field = new CustomBinaryDocValuesField(fieldType().name(), value);
                context.doc().addWithKey(fieldType().name(), field);
            } else {
                field.add(value);
            }
        } else {
            // Only add an entry to the field names field if the field is stored
            // but has no doc values so exists query will work on a field with
            // no doc values
            createFieldNamesField(context, fields);
        }

    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * 代表某个field下所有的docValue 都是二进制数据
     */
    public static class CustomBinaryDocValuesField extends CustomDocValuesField {

        private final ObjectArrayList<byte[]> bytesList;

        private int totalSize = 0;

        public CustomBinaryDocValuesField(String name, byte[] bytes) {
            super(name);
            bytesList = new ObjectArrayList<>();
            add(bytes);
        }

        public void add(byte[] bytes) {
            bytesList.add(bytes);
            totalSize += bytes.length;
        }

        @Override
        public BytesRef binaryValue() {
            try {
                CollectionUtils.sortAndDedup(bytesList);
                int size = bytesList.size();
                final byte[] bytes = new byte[totalSize + (size + 1) * 5];
                ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
                out.writeVInt(size);  // write total number of values
                for (int i = 0; i < size; i ++) {
                    final byte[] value = bytesList.get(i);
                    int valueLength = value.length;
                    out.writeVInt(valueLength);
                    out.writeBytes(value, 0, valueLength);
                }
                return new BytesRef(bytes, 0, out.getPosition());
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get binary value", e);
            }

        }
    }
}
