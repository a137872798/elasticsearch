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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * A builder for scaling executors.
 * 该对象负责构建 范围线程池
 */
public final class ScalingExecutorBuilder extends ExecutorBuilder<ScalingExecutorBuilder.ScalingExecutorSettings> {

    private final Setting<Integer> coreSetting;
    private final Setting<Integer> maxSetting;
    private final Setting<TimeValue> keepAliveSetting;

    /**
     * Construct a scaling executor builder; the settings will have the
     * key prefix "thread_pool." followed by the executor name.
     *
     * @param name      the name of the executor
     * @param core      the minimum number of threads in the pool
     * @param max       the maximum number of threads in the pool
     * @param keepAlive the time that spare threads above {@code core}
     *                  threads will be kept alive
     */
    public ScalingExecutorBuilder(final String name, final int core, final int max, final TimeValue keepAlive) {
        this(name, core, max, keepAlive, "thread_pool." + name);
    }

    /**
     * Construct a scaling executor builder; the settings will have the
     * specified key prefix.
     *
     * @param name      the name of the executor
     * @param core      the minimum number of threads in the pool
     * @param max       the maximum number of threads in the pool
     * @param keepAlive the time that spare threads above {@code core}
     *                  threads will be kept alive
     * @param prefix    the prefix for the settings keys
     *                  通过相关配置设置 builder内部的信息
     */
    public ScalingExecutorBuilder(final String name, final int core, final int max, final TimeValue keepAlive, final String prefix) {
        super(name);
        // settingsKey(prefix, "core") 用于生成配置名    指定该配置的范围是针对某个 node的 应该是值这个线程池的配置只作用在当前服务器(一个es-server实例就是一个node)
        this.coreSetting =
            Setting.intSetting(settingsKey(prefix, "core"), core, Setting.Property.NodeScope);
        this.maxSetting = Setting.intSetting(settingsKey(prefix, "max"), max, Setting.Property.NodeScope);
        this.keepAliveSetting =
            Setting.timeSetting(settingsKey(prefix, "keep_alive"), keepAlive, Setting.Property.NodeScope);
    }

    /**
     * 返回此时builder 中已经设置的配置信息
     * @return
     */
    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return Arrays.asList(coreSetting, maxSetting, keepAliveSetting);
    }

    @Override
    ScalingExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        final int coreThreads = coreSetting.get(settings);
        final int maxThreads = maxSetting.get(settings);
        final TimeValue keepAlive = keepAliveSetting.get(settings);
        return new ScalingExecutorSettings(nodeName, coreThreads, maxThreads, keepAlive);
    }

    ThreadPool.ExecutorHolder build(final ScalingExecutorSettings settings, final ThreadContext threadContext) {
        TimeValue keepAlive = settings.keepAlive;
        int core = settings.core;
        int max = settings.max;
        final ThreadPool.Info info = new ThreadPool.Info(name(), ThreadPool.ThreadPoolType.SCALING, core, max, keepAlive, null);
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(EsExecutors.threadName(settings.nodeName, name()));
        final ExecutorService executor =
            EsExecutors.newScaling(
                    settings.nodeName + "/" + name(),
                    core,
                    max,
                    keepAlive.millis(),
                    TimeUnit.MILLISECONDS,
                    threadFactory,
                    threadContext);
        return new ThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], core [%d], max [%d], keep alive [%s]",
            info.getName(),
            info.getMin(),
            info.getMax(),
            info.getKeepAlive());
    }

    /**
     * 范围线程池配置  这个范围指的应该就是线程数是浮动的吧   按照es的说法jdk默认的线程池就是一种范围线程池
     */
    static class ScalingExecutorSettings extends ExecutorBuilder.ExecutorSettings {

        /**
         * 核心线程数
         */
        private final int core;
        private final int max;
        /**
         * 非核心线程数的存活时间
         */
        private final TimeValue keepAlive;

        ScalingExecutorSettings(final String nodeName, final int core, final int max, final TimeValue keepAlive) {
            super(nodeName);
            this.core = core;
            this.max = max;
            this.keepAlive = keepAlive;
        }
    }

}
