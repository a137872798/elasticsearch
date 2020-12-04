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
package org.elasticsearch.cluster.metadata;

import static org.elasticsearch.cluster.metadata.DataStream.getBackingIndexName;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_HIDDEN_SETTING;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An index abstraction is a reference to one or more concrete indices.
 * An index abstraction has a unique name and encapsulates all the  {@link IndexMetadata} instances it is pointing to.
 * Also depending on type it may refer to a single or many concrete indices and may or may not have a write index.
 * 可以通过3种维度来查找Index  比如通过别名查找关联的Index 或者通过DataStream 查找下面的子级index
 */
public interface IndexAbstraction {

    /**
     * @return the type of the index abstraction
     * 该对象内部包含了实际的索引 但是 可以是使用name匹配上的 也可以是alias匹配上的
     */
    Type getType();

    /**
     * @return the name of the index abstraction
     */
    String getName();

    /**
     * @return All {@link IndexMetadata} of all concrete indices this index abstraction is referring to.
     */
    List<IndexMetadata> getIndices();

    /**
     * A write index is a dedicated concrete index, that accepts all the new documents that belong to an index abstraction.
     * <p>
     * A write index may also be a regular concrete index of a index abstraction and may therefore also be returned
     * by {@link #getIndices()}. An index abstraction may also not have a dedicated write index.
     *
     * @return the write index of this index abstraction or
     * <code>null</code> if this index abstraction doesn't have a write index.
     */
    @Nullable
    IndexMetadata getWriteIndex();

    /**
     * @return the data stream to which this index belongs or <code>null</code> if this is not a concrete index or
     * if it is a concrete index that does not belong to a data stream.
     */
    @Nullable
    DataStream getParentDataStream();

    /**
     * @return whether this index abstraction is hidden or not
     */
    boolean isHidden();

    /**
     * An index abstraction type.
     */
    enum Type {

        /**
         * An index abstraction that refers to a single concrete index.
         * This concrete index is also the write index.
         */
        CONCRETE_INDEX("concrete index"),

        /**
         * An index abstraction that refers to an alias.
         * An alias typically refers to many concrete indices and
         * may have a write index.
         */
        ALIAS("alias"),

        /**
         * An index abstraction that refers to a data stream.
         * A data stream typically has multiple backing indices, the latest of which
         * is the target for index requests.
         */
        DATA_STREAM("data_stream");

        private final String displayName;

        Type(String displayName) {
            this.displayName = displayName;
        }

        public String getDisplayName() {
            return displayName;
        }
    }

    /**
     * Represents an concrete index and encapsulates its {@link IndexMetadata}
     * 代表通过lookup对象 + indexName 仅能匹配到一个描述索引的对象 此时是一一对应的关系
     */
    class Index implements IndexAbstraction {

        /**
         * 该索引的元数据信息
         */
        private final IndexMetadata concreteIndex;
        private final DataStream dataStream;

        /**
         * 如果该index是由创建某个 dataStream时 间接创建出来的  就会设置 dataStream属性
         * @param indexMetadata
         * @param dataStream
         */
        public Index(IndexMetadata indexMetadata, DataStream dataStream) {
            this.concreteIndex = indexMetadata;
            this.dataStream = dataStream;
        }

        /**
         * 可以看出 默认情况下 DataStream是不设置的
         *
         * @param indexMetadata
         */
        public Index(IndexMetadata indexMetadata) {
            this(indexMetadata, null);
        }

        @Override
        public String getName() {
            return concreteIndex.getIndex().getName();
        }

        @Override
        public Type getType() {
            return Type.CONCRETE_INDEX;
        }

        @Override
        public List<IndexMetadata> getIndices() {
            return List.of(concreteIndex);
        }

        @Override
        public IndexMetadata getWriteIndex() {
            return concreteIndex;
        }

        @Override
        public DataStream getParentDataStream() {
            return dataStream;
        }

        @Override
        public boolean isHidden() {
            return INDEX_HIDDEN_SETTING.get(concreteIndex.getSettings());
        }
    }

    /**
     * Represents an alias and groups all {@link IndexMetadata} instances sharing the same alias name together.
     * 多个index允许使用同一个alias
     */
    class Alias implements IndexAbstraction {

        /**
         * 别名
         */
        private final String aliasName;
        /**
         * 该别名对应的多个index
         */
        private final List<IndexMetadata> referenceIndexMetadatas;
        private final SetOnce<IndexMetadata> writeIndex = new SetOnce<>();
        private final boolean isHidden;

        /**
         * @param aliasMetadata
         * @param indexMetadata
         */
        public Alias(AliasMetadata aliasMetadata, IndexMetadata indexMetadata) {
            this.aliasName = aliasMetadata.getAlias();
            this.referenceIndexMetadatas = new ArrayList<>();
            this.referenceIndexMetadatas.add(indexMetadata);
            this.isHidden = aliasMetadata.isHidden() == null ? false : aliasMetadata.isHidden();
        }

        @Override
        public Type getType() {
            return Type.ALIAS;
        }

        public String getName() {
            return aliasName;
        }

        @Override
        public List<IndexMetadata> getIndices() {
            return referenceIndexMetadatas;
        }


        @Nullable
        public IndexMetadata getWriteIndex() {
            return writeIndex.get();
        }

        @Override
        public DataStream getParentDataStream() {
            // aliases may not be part of a data stream
            return null;
        }

        @Override
        public boolean isHidden() {
            return isHidden;
        }

        /**
         * Returns the unique alias metadata per concrete index.
         * <p>
         * (note that although alias can point to the same concrete indices, each alias reference may have its own routing
         * and filters)
         * 遍历内部所有的 索引元数据
         */
        public Iterable<Tuple<String, AliasMetadata>> getConcreteIndexAndAliasMetadatas() {
            return () -> new Iterator<>() {

                int index = 0;

                @Override
                public boolean hasNext() {
                    return index < referenceIndexMetadatas.size();
                }

                @Override
                public Tuple<String, AliasMetadata> next() {
                    IndexMetadata indexMetadata = referenceIndexMetadatas.get(index++);
                    return new Tuple<>(indexMetadata.getIndex().getName(), indexMetadata.getAliases().get(aliasName));
                }
            };
        }

        public AliasMetadata getFirstAliasMetadata() {
            return referenceIndexMetadatas.get(0).getAliases().get(aliasName);
        }

        /**
         * 追加其他索引的元数据
         * @param indexMetadata
         */
        void addIndex(IndexMetadata indexMetadata) {
            this.referenceIndexMetadatas.add(indexMetadata);
        }

        /**
         * 判定哪个是 writeIndices
         */
        public void computeAndValidateAliasProperties() {
            // Validate write indices
            List<IndexMetadata> writeIndices = referenceIndexMetadatas.stream()
                .filter(idxMeta -> Boolean.TRUE.equals(idxMeta.getAliases().get(aliasName).writeIndex()))
                .collect(Collectors.toList());

            if (writeIndices.isEmpty() && referenceIndexMetadatas.size() == 1
                && referenceIndexMetadatas.get(0).getAliases().get(aliasName).writeIndex() == null) {
                writeIndices.add(referenceIndexMetadatas.get(0));
            }

            if (writeIndices.size() == 1) {
                writeIndex.set(writeIndices.get(0));
            } else if (writeIndices.size() > 1) {
                List<String> writeIndicesStrings = writeIndices.stream()
                    .map(i -> i.getIndex().getName()).collect(Collectors.toList());
                throw new IllegalStateException("alias [" + aliasName + "] has more than one write index [" +
                    Strings.collectionToCommaDelimitedString(writeIndicesStrings) + "]");
            }

            // Validate hidden status
            final Map<Boolean, List<IndexMetadata>> groupedByHiddenStatus = referenceIndexMetadatas.stream()
                .collect(Collectors.groupingBy(idxMeta -> Boolean.TRUE.equals(idxMeta.getAliases().get(aliasName).isHidden())));
            if (isNonEmpty(groupedByHiddenStatus.get(true)) && isNonEmpty(groupedByHiddenStatus.get(false))) {
                List<String> hiddenOn = groupedByHiddenStatus.get(true).stream()
                    .map(idx -> idx.getIndex().getName()).collect(Collectors.toList());
                List<String> nonHiddenOn = groupedByHiddenStatus.get(false).stream()
                    .map(idx -> idx.getIndex().getName()).collect(Collectors.toList());
                throw new IllegalStateException("alias [" + aliasName + "] has is_hidden set to true on indices [" +
                    Strings.collectionToCommaDelimitedString(hiddenOn) + "] but does not have is_hidden set to true on indices [" +
                    Strings.collectionToCommaDelimitedString(nonHiddenOn) + "]; alias must have the same is_hidden setting " +
                    "on all indices");
            }
        }

        private boolean isNonEmpty(List<IndexMetadata> idxMetas) {
            return (Objects.isNull(idxMetas) || idxMetas.isEmpty()) == false;
        }
    }

    /**
     * 当创建 DataStream时 会间接创建index
     */
    class DataStream implements IndexAbstraction {

        private final org.elasticsearch.cluster.metadata.DataStream dataStream;
        /**
         * 该数据流相关的index
         */
        private final List<IndexMetadata> dataStreamIndices;

        /**
         * 一般情况下 dataStreamIndices 的长度应该是1  如果关联了多个index 那么最后一个index被认为是writeIndex
         */
        private final IndexMetadata writeIndex;

        public DataStream(org.elasticsearch.cluster.metadata.DataStream dataStream, List<IndexMetadata> dataStreamIndices) {
            this.dataStream = dataStream;
            this.dataStreamIndices = List.copyOf(dataStreamIndices);
            this.writeIndex = dataStreamIndices.get(dataStreamIndices.size() - 1);
            assert writeIndex.getIndex().getName().equals(getBackingIndexName(dataStream.getName(), dataStream.getGeneration()));
        }

        @Override
        public String getName() {
            return dataStream.getName();
        }

        @Override
        public Type getType() {
            return Type.DATA_STREAM;
        }

        @Override
        public List<IndexMetadata> getIndices() {
            return dataStreamIndices;
        }

        public IndexMetadata getWriteIndex() {
            return writeIndex;
        }

        @Override
        public DataStream getParentDataStream() {
            // a data stream cannot have a parent data stream
            return null;
        }

        @Override
        public boolean isHidden() {
            return false;
        }

        public org.elasticsearch.cluster.metadata.DataStream getDataStream() {
            return dataStream;
        }
    }
}
