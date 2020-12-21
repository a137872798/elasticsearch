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

import org.apache.lucene.util.Accountable;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 存储 index 下所有  field数据的服务
 * 该服务主要就是将获取数据的逻辑与缓存逻辑整合在一起
 */
public class IndexFieldDataService extends AbstractIndexComponent implements Closeable {

    /**
     * 默认缓存项的配置是 node
     */
    public static final String FIELDDATA_CACHE_VALUE_NODE = "node";
    public static final String FIELDDATA_CACHE_KEY = "index.fielddata.cache";

    /**
     * 要求 必须是node/none
     */
    public static final Setting<String> INDEX_FIELDDATA_CACHE_KEY =
        new Setting<>(FIELDDATA_CACHE_KEY, (s) -> FIELDDATA_CACHE_VALUE_NODE, (s) -> {
            switch (s) {
                case "node":
                case "none":
                    return s;
                default:
                    throw new IllegalArgumentException("failed to parse [" + s + "] must be one of [node,none]");
            }
        }, Property.IndexScope);

    /**
     * 熔断器服务 根据不同的范围获取不同的熔断实例
     */
    private final CircuitBreakerService circuitBreakerService;

    /**
     * 该对象对应多个index
     */
    private final IndicesFieldDataCache indicesFieldDataCache;
    // the below map needs to be modified under a lock
    // 一个index下有多个doc  每个doc下有多个field  这里按照field做缓存
    private final Map<String, IndexFieldDataCache> fieldDataCaches = new HashMap<>();

    /**
     * 映射服务 定义了将数据流转换成
     */
    private final MapperService mapperService;
    private static final IndexFieldDataCache.Listener DEFAULT_NOOP_LISTENER = new IndexFieldDataCache.Listener() {
        @Override
        public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
        }

        @Override
        public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
        }
    };

    /**
     * 初始状态 是一个空的监听器
     * 之后会将 FieldDataCacheListener 设置进来
     * FieldDataCacheListener 本身只是进行一些数据统计
     */
    private volatile IndexFieldDataCache.Listener listener = DEFAULT_NOOP_LISTENER;


    /**
     *
     * @param indexSettings
     * @param indicesFieldDataCache  缓存
     * @param circuitBreakerService  熔断器
     * @param mapperService 映射服务 负责将lucene的数据映射成json数据
     */
    public IndexFieldDataService(IndexSettings indexSettings, IndicesFieldDataCache indicesFieldDataCache,
                                 CircuitBreakerService circuitBreakerService, MapperService mapperService) {
        super(indexSettings);
        this.indicesFieldDataCache = indicesFieldDataCache;
        this.circuitBreakerService = circuitBreakerService;
        this.mapperService = mapperService;
    }

    /**
     * 清理所有缓存
     */
    public synchronized void clear() {
        List<Exception> exceptions = new ArrayList<>(0);
        final Collection<IndexFieldDataCache> fieldDataCacheValues = fieldDataCaches.values();
        for (IndexFieldDataCache cache : fieldDataCacheValues) {
            try {
                cache.clear();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        fieldDataCacheValues.clear();
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }

    /**
     * 清除某个field的缓存
     * @param fieldName
     */
    public synchronized void clearField(final String fieldName) {
        List<Exception> exceptions = new ArrayList<>(0);
        final IndexFieldDataCache cache = fieldDataCaches.remove(fieldName);
        if (cache != null) {
            try {
                cache.clear(fieldName);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }

    /**
     * 获取某个field 相关的 data
     * @param fieldType
     * @param <IFD>
     * @return
     */
    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType) {
        return getForField(fieldType, index().getName());
    }


    /**
     * 获取某个field 相关数据
     * @param fieldType
     * @param fullyQualifiedIndexName  index 的全限定名
     * @param <IFD>
     * @return
     */
    @SuppressWarnings("unchecked")
    public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType, String fullyQualifiedIndexName) {
        final String fieldName = fieldType.name();
        IndexFieldData.Builder builder = fieldType.fielddataBuilder(fullyQualifiedIndexName);

        IndexFieldDataCache cache;
        synchronized (this) {
            // 先尝试从缓存中获取数据
            cache = fieldDataCaches.get(fieldName);
            if (cache == null) {
                String cacheType = indexSettings.getValue(INDEX_FIELDDATA_CACHE_KEY);
                if (FIELDDATA_CACHE_VALUE_NODE.equals(cacheType)) {
                    // 生成某个fieldDataCache 对象
                    cache = indicesFieldDataCache.buildIndexFieldDataCache(listener, index(), fieldName);
                } else if ("none".equals(cacheType)){
                    cache = new IndexFieldDataCache.None();
                } else {
                    throw new IllegalArgumentException("cache type not supported [" + cacheType + "] for field [" + fieldName + "]");
                }
                fieldDataCaches.put(fieldName, cache);
            }
        }

        return (IFD) builder.build(indexSettings, fieldType, cache, circuitBreakerService, mapperService);
    }

    /**
     * Sets a {@link org.elasticsearch.index.fielddata.IndexFieldDataCache.Listener} passed to each {@link IndexFieldData}
     * creation to capture onCache and onRemoval events. Setting a listener on this method will override any previously
     * set listeners.
     * @throws IllegalStateException if the listener is set more than once
     * 指定监听器 因为默认监听器为NOOP
     */
    public void setListener(IndexFieldDataCache.Listener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (this.listener != DEFAULT_NOOP_LISTENER) {
            throw new IllegalStateException("can't set listener more than once");
        }
        this.listener = listener;
    }

    @Override
    public void close() throws IOException {
        clear();
    }
}
