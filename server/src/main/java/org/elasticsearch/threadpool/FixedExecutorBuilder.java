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

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * A builder for fixed executors.
 */
public final class FixedExecutorBuilder extends ExecutorBuilder<FixedExecutorBuilder.FixedExecutorSettings> {

    private final Setting<Integer> sizeSetting;
    private final Setting<Integer> queueSizeSetting;
    private final boolean trackEWMA;

    /**
     * Construct a fixed executor builder; the settings will have the key prefix "thread_pool." followed by the executor name.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param size      the fixed number of threads
     * @param queueSize the size of the backing queue, -1 for unbounded
     * @param trackEWMA whether to track the exponentially weighted moving average of the task execution time
     */
    FixedExecutorBuilder(final Settings settings, final String name, final int size, final int queueSize, final boolean trackEWMA) {
        this(settings, name, size, queueSize, "thread_pool." + name, trackEWMA);
    }

    /**
     * Construct a fixed executor builder.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param size      the fixed number of threads
     * @param queueSize the size of the backing queue, -1 for unbounded
     * @param prefix    the prefix for the settings keys
     * @param trackEWMA whether to track the exponentially weighted moving average of the task execution time
     */
    public FixedExecutorBuilder(final Settings settings, final String name, final int size, final int queueSize, final String prefix,
                                final boolean trackEWMA) {
        super(name);
        final String sizeKey = settingsKey(prefix, "size");
        this.sizeSetting =
                new Setting<>(
                        sizeKey,
                        s -> Integer.toString(size),
                        s -> Setting.parseInt(s, 1, applyHardSizeLimit(settings, name), sizeKey),
                        Setting.Property.NodeScope);
        final String queueSizeKey = settingsKey(prefix, "queue_size");
        this.queueSizeSetting = Setting.intSetting(queueSizeKey, queueSize, Setting.Property.NodeScope);
        this.trackEWMA = trackEWMA;
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return Arrays.asList(sizeSetting, queueSizeSetting);
    }

    /**
     * 从settings中仅抽取与创建线程池相关的配置
     * @param settings the node-level settings
     * @return
     */
    @Override
    FixedExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        // 获取线程数 以及阻塞队列长度信息
        final int size = sizeSetting.get(settings);
        final int queueSize = queueSizeSetting.get(settings);
        return new FixedExecutorSettings(nodeName, size, queueSize);
    }

    /**
     * 创建线程池持有对象
     * @param settings      the executor settings   仅存储固定线程数量线程池的配置
     * @param threadContext the current thread context    包含一些公用的信息
     * @return
     */
    @Override
    ThreadPool.ExecutorHolder build(final FixedExecutorSettings settings, final ThreadContext threadContext) {
        int size = settings.size;
        int queueSize = settings.queueSize;
        // 获取es专用的线程工厂   与一般的线程工厂不同的地方在于 它创建的线程会归纳到一个线程组中
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(EsExecutors.threadName(settings.nodeName, name()));
        final ExecutorService executor =
                EsExecutors.newFixed(settings.nodeName + "/" + name(), size, queueSize, threadFactory, threadContext, trackEWMA);
        // 生成当前线程池的描述信息对象
        final ThreadPool.Info info =
            new ThreadPool.Info(name(), ThreadPool.ThreadPoolType.FIXED, size, size, null, queueSize < 0 ? null : new SizeValue(queueSize));
        return new ThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], size [%d], queue size [%s]",
            info.getName(),
            info.getMax(),
            info.getQueueSize() == null ? "unbounded" : info.getQueueSize());
    }

    /**
     * 描述固定线程池配置的对象
     */
    static class FixedExecutorSettings extends ExecutorBuilder.ExecutorSettings {

        /**
         * 线程数量固定
         */
        private final int size;
        private final int queueSize;

        FixedExecutorSettings(final String nodeName, final int size, final int queueSize) {
            super(nodeName);
            this.size = size;
            this.queueSize = queueSize;
        }

    }

}
