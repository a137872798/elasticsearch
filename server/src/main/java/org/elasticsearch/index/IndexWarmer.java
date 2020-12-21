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

package org.elasticsearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * 索引暖机对象
 * 实际上就是将field级别的数据 预先加载到缓存中
 */
public final class IndexWarmer {

    private static final Logger logger = LogManager.getLogger(IndexWarmer.class);

    /**
     * 监听器包含了预热的api
     * 当在合适的时机会触发预热操作
     */
    private final List<Listener> listeners;


    /**
     * @param threadPool            线程池
     * @param indexFieldDataService 可以以field为单位查询数据
     * @param listeners             一组包含预热逻辑的监听器
     */
    IndexWarmer(ThreadPool threadPool, IndexFieldDataService indexFieldDataService,
                Listener... listeners) {
        ArrayList<Listener> list = new ArrayList<>();
        final Executor executor = threadPool.executor(ThreadPool.Names.WARMER);
        // 这里要追加一个基于 fieldData的预热对象 就是利用了IndexFieldDataService(该对象内部有缓存)
        list.add(new FieldDataWarmer(executor, indexFieldDataService));

        Collections.addAll(list, listeners);
        this.listeners = Collections.unmodifiableList(list);
    }

    /**
     * 针对某个目录的reader 进行暖机工作 加快读取速度
     *
     * @param reader
     * @param shard    针对哪个分片进行暖机
     * @param settings
     */
    void warm(ElasticsearchDirectoryReader reader, IndexShard shard, IndexSettings settings) {
        if (shard.state() == IndexShardState.CLOSED) {
            return;
        }
        // 如果不支持预热 直接返回
        if (settings.isWarmerEnabled() == false) {
            return;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("{} top warming [{}]", shard.shardId(), reader);
        }
        // 做预热前的准备工作   实际上做一些数据统计
        shard.warmerService().onPreWarm();
        long time = System.nanoTime();
        final List<TerminationHandle> terminationHandles = new ArrayList<>();
        // get a handle on pending tasks
        for (final Listener listener : listeners) {
            // 预热工作是由监听器完成的
            terminationHandles.add(listener.warmReader(shard, reader));
        }
        // wait for termination
        for (TerminationHandle terminationHandle : terminationHandles) {
            try {
                // 阻塞直到所有预热完成
                terminationHandle.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("top warming has been interrupted", e);
                break;
            }
        }
        long took = System.nanoTime() - time;
        shard.warmerService().onPostWarm(took);
        if (shard.warmerService().logger().isTraceEnabled()) {
            shard.warmerService().logger().trace("top warming took [{}]", new TimeValue(took, TimeUnit.NANOSECONDS));
        }
    }

    /**
     * A handle on the execution of  warm-up action.
     */
    public interface TerminationHandle {

        TerminationHandle NO_WAIT = () -> {
        };

        /**
         * Wait until execution of the warm-up action completes.
         */
        void awaitTermination() throws InterruptedException;
    }

    /**
     * 在 IndexWarmer中每个监听器有一个预热功能
     */
    public interface Listener {
        /**
         * Queue tasks to warm-up the given segments and return handles that allow to wait for termination of the
         * execution of those tasks.
         */
        TerminationHandle warmReader(IndexShard indexShard, ElasticsearchDirectoryReader reader);
    }


    /**
     * fieldDataWarmer 代表为读取每个field下的数据做预热
     */
    private static class FieldDataWarmer implements IndexWarmer.Listener {

        private final Executor executor;
        /**
         * 该对象内部定义了查询数据的逻辑 还是用到了缓存
         */
        private final IndexFieldDataService indexFieldDataService;

        FieldDataWarmer(Executor executor, IndexFieldDataService indexFieldDataService) {
            this.executor = executor;
            this.indexFieldDataService = indexFieldDataService;
        }

        /**
         * TODO 先不看暖机逻辑
         * @param indexShard
         * @param reader
         * @return
         */
        @Override
        public TerminationHandle warmReader(final IndexShard indexShard, final ElasticsearchDirectoryReader reader) {
            final MapperService mapperService = indexShard.mapperService();
            // TODO 什么是全局顺序
            final Map<String, MappedFieldType> warmUpGlobalOrdinals = new HashMap<>();
            // 这些fieldType 是什么时候设置进去的
            for (MappedFieldType fieldType : mapperService.fieldTypes()) {
                final String indexName = fieldType.name();
                if (fieldType.eagerGlobalOrdinals() == false) {
                    continue;
                }
                // 只有满足全局顺序的field 才会加入到 map中
                warmUpGlobalOrdinals.put(indexName, fieldType);
            }
            final CountDownLatch latch = new CountDownLatch(warmUpGlobalOrdinals.size());
            // 只会为 全局顺序为true的field 进行预热
            for (final MappedFieldType fieldType : warmUpGlobalOrdinals.values()) {
                executor.execute(() -> {
                    try {
                        final long start = System.nanoTime();
                        IndexFieldData.Global<?> ifd = indexFieldDataService.getForField(fieldType);
                        // 预热实际上就是提前加载数据到内存
                        IndexFieldData<?> global = ifd.loadGlobal(reader);
                        if (reader.leaves().isEmpty() == false) {
                            global.load(reader.leaves().get(0));
                        }

                        if (indexShard.warmerService().logger().isTraceEnabled()) {
                            indexShard.warmerService().logger().trace(
                                "warmed global ordinals for [{}], took [{}]",
                                fieldType.name(),
                                TimeValue.timeValueNanos(System.nanoTime() - start));
                        }
                    } catch (Exception e) {
                        indexShard
                            .warmerService()
                            .logger()
                            .warn(() -> new ParameterizedMessage("failed to warm-up global ordinals for [{}]", fieldType.name()), e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            return () -> latch.await();
        }
    }

}
