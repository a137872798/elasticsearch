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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.index.shard.ShardId;

/**
 * A simple field data cache abstraction on the *index* level.
 * 在索引级别查询field 数据时做了缓存
 */
public interface IndexFieldDataCache {

    /**
     * LeafFieldData 该对象可以获取某个field下所有数据的迭代器
     * IndexFieldData 该对象可以产生 LeafFieldData   可能Index的定位就是对应多个field
     * @param context  根据上下文信息 以及 IndexFieldData 可以获取LeafFieldData
     * @param indexFieldData
     * @param <FD>
     * @param <IFD>
     * @return
     * @throws Exception
     */
    <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(LeafReaderContext context, IFD indexFieldData) throws Exception;

    /**
     * 从DirectoryReader 中加载数据 此时返回的是一个全局的IndexFieldData global应该能够以某种方式获取某个field对应的相关数据
     * @param indexReader
     * @param indexFieldData
     * @param <FD>
     * @param <IFD>
     * @return
     * @throws Exception
     */
    <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(DirectoryReader indexReader, IFD indexFieldData)
        throws Exception;

    /**
     * Clears all the field data stored cached in on this index.
     * 清空缓存
     */
    void clear();

    /**
     * Clears all the field data stored cached in on this index for the specified field name.
     * 只清理某个field下的缓存数据
     */
    void clear(String fieldName);

    /**
     * 监听缓存对象插入新数据  以及移除数据
     */
    interface Listener {

        /**
         * Called after the fielddata is loaded during the cache phase
         * 将某个数据分片下的 某个field的数据生成缓存时触发
         */
        default void onCache(ShardId shardId, String fieldName, Accountable ramUsage){}

        /**
         * Called after the fielddata is unloaded
         * 对应上面将数据移除时触发
         */
        default void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes){}
    }

    /**
     */
    class None implements IndexFieldDataCache {

        @Override
        public <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(LeafReaderContext context, IFD indexFieldData)
            throws Exception {
            return indexFieldData.loadDirect(context);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(DirectoryReader indexReader,
                                                                                          IFD indexFieldData) throws Exception {
            return (IFD) indexFieldData.localGlobalDirect(indexReader);
        }

        @Override
        public void clear() {
        }

        @Override
        public void clear(String fieldName) {
        }
    }
}
