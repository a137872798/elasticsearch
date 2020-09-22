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
package org.elasticsearch.watcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Generic resource watcher service
 *
 * Other elasticsearch services can register their resource watchers with this service using {@link #add(ResourceWatcher)}
 * method. This service will call {@link org.elasticsearch.watcher.ResourceWatcher#checkAndNotify()} method of all
 * registered watcher periodically. The frequency of checks can be specified using {@code resource.reload.interval} setting, which
 * defaults to {@code 60s}. The service can be disabled by setting {@code resource.reload.enabled} setting to {@code false}.
 * 资源监控服务本身只是包含了 3个定时触发监听器的 monitor对象 而处理逻辑则是委托给handler   在es中默认实现为 FileWatcher 主要是监测指定文件/目录的创建/删除
 */
public class ResourceWatcherService implements Closeable {
    private static final Logger logger = LogManager.getLogger(ResourceWatcherService.class);

    public enum Frequency {

        /**
         * Defaults to 5 seconds
         */
        HIGH(TimeValue.timeValueSeconds(5)),

        /**
         * Defaults to 30 seconds
         */
        MEDIUM(TimeValue.timeValueSeconds(30)),

        /**
         * Defaults to 60 seconds
         */
        LOW(TimeValue.timeValueSeconds(60));

        final TimeValue interval;

        Frequency(TimeValue interval) {
            this.interval = interval;
        }
    }

    public static final Setting<Boolean> ENABLED = Setting.boolSetting("resource.reload.enabled", true, Property.NodeScope);
    public static final Setting<TimeValue> RELOAD_INTERVAL_HIGH =
        Setting.timeSetting("resource.reload.interval.high", Frequency.HIGH.interval, Property.NodeScope);
    public static final Setting<TimeValue> RELOAD_INTERVAL_MEDIUM = Setting.timeSetting("resource.reload.interval.medium",
        Setting.timeSetting("resource.reload.interval", Frequency.MEDIUM.interval), Property.NodeScope);
    public static final Setting<TimeValue> RELOAD_INTERVAL_LOW =
        Setting.timeSetting("resource.reload.interval.low", Frequency.LOW.interval, Property.NodeScope);


    /**
     * 是否支持重新加载资源
     */
    private final boolean enabled;

    final ResourceMonitor lowMonitor;
    final ResourceMonitor mediumMonitor;
    final ResourceMonitor highMonitor;

    private final Cancellable lowFuture;
    private final Cancellable mediumFuture;
    private final Cancellable highFuture;

    /**
     *
     * @param settings  包含所有配置的对象
     * @param threadPool  所有使用的线程池的总控对象
     */
    public ResourceWatcherService(Settings settings, ThreadPool threadPool) {
        this.enabled = ENABLED.get(settings);

        // 重新检测资源的间隔时间
        TimeValue interval = RELOAD_INTERVAL_LOW.get(settings);

        // 这里以3种不同的时间间隔 创建了3个资源监视器
        lowMonitor = new ResourceMonitor(interval, Frequency.LOW);
        interval = RELOAD_INTERVAL_MEDIUM.get(settings);
        mediumMonitor = new ResourceMonitor(interval, Frequency.MEDIUM);
        interval = RELOAD_INTERVAL_HIGH.get(settings);
        highMonitor = new ResourceMonitor(interval, Frequency.HIGH);
        // 使用指定的线程池 执行任务   注意他们使用的线程池name 都是 SAME 代表直接在当前线程执行任务 (scheduleWithFixedDelay 通过2层线程池实现 外层是基于JDK内置的线程池实现定时功能  内层执行任务逻辑时还有一层专门的线程池)
        if (enabled) {
            lowFuture = threadPool.scheduleWithFixedDelay(lowMonitor, lowMonitor.interval, Names.SAME);
            mediumFuture = threadPool.scheduleWithFixedDelay(mediumMonitor, mediumMonitor.interval, Names.SAME);
            highFuture = threadPool.scheduleWithFixedDelay(highMonitor, highMonitor.interval, Names.SAME);
        } else {
            lowFuture = null;
            mediumFuture = null;
            highFuture = null;
        }
    }

    @Override
    public void close() {
        if (enabled) {
            lowFuture.cancel();
            mediumFuture.cancel();
            highFuture.cancel();
        }
    }

    /**
     * Register new resource watcher that will be checked in default {@link Frequency#MEDIUM MEDIUM} frequency
     */
    public <W extends ResourceWatcher> WatcherHandle<W> add(W watcher) throws IOException {
        return add(watcher, Frequency.MEDIUM);
    }

    /**
     * Register new resource watcher that will be checked in the given frequency
     * 将某个监控处理器 设置到监控对象上
     */
    public <W extends ResourceWatcher> WatcherHandle<W> add(W watcher, Frequency frequency) throws IOException {
        // 先触发init方法
        watcher.init();
        switch (frequency) {
            case LOW:
                return lowMonitor.add(watcher);
            case MEDIUM:
                return mediumMonitor.add(watcher);
            case HIGH:
                return highMonitor.add(watcher);
            default:
                throw new IllegalArgumentException("Unknown frequency [" + frequency + "]");
        }
    }

    public void notifyNow(Frequency frequency) {
        switch (frequency) {
            case LOW:
                lowMonitor.run();
                break;
            case MEDIUM:
                mediumMonitor.run();
                break;
            case HIGH:
                highMonitor.run();
                break;
            default:
                throw new IllegalArgumentException("Unknown frequency [" + frequency + "]");
        }
    }

    /**
     * 资源监控器对象
     */
    static class ResourceMonitor implements Runnable {

        /**
         * 该资源监控器对应触发任务的间隔时间
         */
        final TimeValue interval;
        /**
         * 触发的频率
         */
        final Frequency frequency;

        final Set<ResourceWatcher> watchers = new CopyOnWriteArraySet<>();

        private ResourceMonitor(TimeValue interval, Frequency frequency) {
            this.interval = interval;
            this.frequency = frequency;
        }

        /**
         * 追加一个资源监视器  同时返回一个句柄对象 通过操作该句柄对象可以将watcher从 watchers中移除
         * @param watcher
         * @param <W>
         * @return
         */
        private <W extends ResourceWatcher> WatcherHandle<W> add(W watcher) {
            watchers.add(watcher);
            return new WatcherHandle<>(this, watcher);
        }

        @Override
        public synchronized void run() {
            for (ResourceWatcher watcher : watchers) {
                try {
                    watcher.checkAndNotify();
                } catch (IOException e) {
                    logger.trace("failed to check resource watcher", e);
                }
            }
        }
    }
}
