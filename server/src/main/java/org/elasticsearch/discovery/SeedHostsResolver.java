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

package org.elasticsearch.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.discovery.PeerFinder.ConfiguredHostsResolver;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 当集群中所有节点都还无法察觉其他节点时 需要一个获取初始服务器地址的对象  这些服务器被称为seed
 * 它跟 jraft/zk 不同  这两种一开始都是已知集群中存在哪些参选节点的   而该对象将管理集群服务器列表的能力转移到了 seed对象中
 */
public class SeedHostsResolver extends AbstractLifecycleComponent implements ConfiguredHostsResolver, SeedHostsProvider.HostsResolver {
    public static final Setting<Integer> DISCOVERY_SEED_RESOLVER_MAX_CONCURRENT_RESOLVERS_SETTING =
        Setting.intSetting("discovery.seed_resolver.max_concurrent_resolvers", 10, 0, Setting.Property.NodeScope);
    public static final Setting<TimeValue> DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("discovery.seed_resolver.timeout", TimeValue.timeValueSeconds(5), Setting.Property.NodeScope);

    private static final Logger logger = LogManager.getLogger(SeedHostsResolver.class);

    private final Settings settings;

    /**
     * 代表此时正在解析中
     */
    private final AtomicBoolean resolveInProgress = new AtomicBoolean();
    private final TransportService transportService;

    /**
     * 实际上种子服务器默认为空
     */
    private final SeedHostsProvider hostsProvider;

    /**
     * 解析使用的线程池
     */
    private final SetOnce<ExecutorService> executorService = new SetOnce<>();
    private final TimeValue resolveTimeout;
    private final String nodeName;

    /**
     * 可以并发解析么
     */
    private final int concurrentConnects;

    /**
     * 被该对象管理的线程 通过调用cancel 可以触发所有线程的打断逻辑
     */
    private final CancellableThreads cancellableThreads = new CancellableThreads();

    public SeedHostsResolver(String nodeName, Settings settings, TransportService transportService,
                             SeedHostsProvider seedProvider) {
        this.settings = settings;
        this.nodeName = nodeName;
        this.transportService = transportService;
        this.hostsProvider = seedProvider;
        resolveTimeout = getResolveTimeout(settings);
        concurrentConnects = getMaxConcurrentResolvers(settings);
    }

    public static int getMaxConcurrentResolvers(Settings settings) {
        return DISCOVERY_SEED_RESOLVER_MAX_CONCURRENT_RESOLVERS_SETTING.get(settings);
    }

    public static TimeValue getResolveTimeout(Settings settings) {
        return DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING.get(settings);
    }

    /**
     * 将一组格式化地址 转换成 TransportAddress  这些地址不包含当前服务器已经绑定的地址
     * @param hosts
     * @return
     */
    @Override
    public List<TransportAddress> resolveHosts(final List<String> hosts) {
        Objects.requireNonNull(hosts);
        if (resolveTimeout.nanos() < 0) {
            throw new IllegalArgumentException("resolve timeout must be non-negative but was [" + resolveTimeout + "]");
        }
        // create tasks to submit to the executor service; we will wait up to resolveTimeout for these tasks to complete
        // 这里包装成回调函数 实际上还没有进行解析
        final List<Callable<TransportAddress[]>> callables =
            hosts
                .stream()
                .map(hn -> (Callable<TransportAddress[]>) () -> transportService.addressesFromString(hn))
                .collect(Collectors.toList());
        final SetOnce<List<Future<TransportAddress[]>>> futures = new SetOnce<>();
        try {
            // 并发执行解析
            cancellableThreads.execute(() ->
                futures.set(executorService.get().invokeAll(callables, resolveTimeout.nanos(), TimeUnit.NANOSECONDS)));
        } catch (CancellableThreads.ExecutionCancelledException e) {
            return Collections.emptyList();
        }
        final List<TransportAddress> transportAddresses = new ArrayList<>();
        final Set<TransportAddress> localAddresses = new HashSet<>();

        // 将当前服务器绑定的所有地址设置到list中
        localAddresses.add(transportService.boundAddress().publishAddress());
        localAddresses.addAll(Arrays.asList(transportService.boundAddress().boundAddresses()));
        // ExecutorService#invokeAll guarantees that the futures are returned in the iteration order of the tasks so we can associate the
        // hostname with the corresponding task by iterating together
        final Iterator<String> it = hosts.iterator();
        for (final Future<TransportAddress[]> future : futures.get()) {
            assert future.isDone();
            // 原始的 格式化字符串
            final String hostname = it.next();
            if (!future.isCancelled()) {
                try {
                    // 对应解析出来的结果
                    final TransportAddress[] addresses = future.get();
                    logger.trace("resolved host [{}] to {}", hostname, addresses);
                    for (int addressId = 0; addressId < addresses.length; addressId++) {
                        final TransportAddress address = addresses[addressId];
                        // no point in pinging ourselves
                        // 只添加还未出现的地址
                        if (localAddresses.contains(address) == false) {
                            transportAddresses.add(address);
                        }
                    }
                } catch (final ExecutionException e) {
                    assert e.getCause() != null;
                    final String message = "failed to resolve host [" + hostname + "]";
                    logger.warn(message, e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // ignore
                }
            } else {
                logger.warn("timed out after [{}] resolving host [{}]", resolveTimeout, hostname);
            }
        }
        return Collections.unmodifiableList(transportAddresses);
    }

    /**
     * 当该组件启动时先触发该方法  就是设置线程池对象
     */
    @Override
    protected void doStart() {
        logger.debug("using max_concurrent_resolvers [{}], resolver timeout [{}]", concurrentConnects, resolveTimeout);
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings, "[unicast_configured_hosts_resolver]");
        executorService.set(EsExecutors.newScaling(nodeName + "/" + "unicast_configured_hosts_resolver",
            0, concurrentConnects, 60, TimeUnit.SECONDS, threadFactory, transportService.getThreadPool().getThreadContext()));
    }

    @Override
    protected void doStop() {
        // 关闭正在解析的线程
        cancellableThreads.cancel("stopping SeedHostsResolver");
        ThreadPool.terminate(executorService.get(), 10, TimeUnit.SECONDS);
    }

    @Override
    protected void doClose() {
    }

    /**
     * @param consumer Consumer for the resolved list. May not be called if an error occurs or if another resolution attempt is in
     *                 定义了如果使用consumer 处理地址
     */
    @Override
    public void resolveConfiguredHosts(Consumer<List<TransportAddress>> consumer) {
        if (lifecycle.started() == false) {
            logger.debug("resolveConfiguredHosts: lifecycle is {}, not proceeding", lifecycle);
            return;
        }

        if (resolveInProgress.compareAndSet(false, true)) {
            transportService.getThreadPool().generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("failure when resolving unicast hosts list", e);
                }

                @Override
                protected void doRun() {
                    if (lifecycle.started() == false) {
                        logger.debug("resolveConfiguredHosts.doRun: lifecycle is {}, not proceeding", lifecycle);
                        return;
                    }

                    // 该对象传入一个 Resolver 会解析出一组地址 并用consumer 处理这组地址
                    List<TransportAddress> providedAddresses = hostsProvider.getSeedAddresses(SeedHostsResolver.this);

                    consumer.accept(providedAddresses);
                }

                @Override
                public void onAfter() {
                    resolveInProgress.set(false);
                }

                @Override
                public String toString() {
                    return "SeedHostsResolver resolving unicast hosts list";
                }
            });
        }
    }
}
