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

/**
 * 代表某个监控处理器 以及绑定的 监控器上
 * @param <W>
 */
public class WatcherHandle<W extends ResourceWatcher> {

    private final ResourceWatcherService.ResourceMonitor monitor;
    private final W watcher;

    WatcherHandle(ResourceWatcherService.ResourceMonitor monitor, W watcher) {
        this.monitor = monitor;
        this.watcher = watcher;
    }

    public W watcher() {
        return watcher;
    }

    /**
     * 代表当前监控器的触发频率
     * @return
     */
    public ResourceWatcherService.Frequency frequency() {
        return monitor.frequency;
    }

    public void stop() {
        monitor.watchers.remove(watcher);
    }

    public void resume() {
        monitor.watchers.add(watcher);
    }
}
