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

package org.elasticsearch.script;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Map;
import java.util.Objects;

/**
 * Script cache and compilation rate limiter.
 * 脚本实例的缓存对象 并且在生成脚本实例时 会涉及到限流的功能
 */
public class ScriptCache {

    private static final Logger logger = LogManager.getLogger(ScriptService.class);

    static final Tuple<Integer, TimeValue> UNLIMITED_COMPILATION_RATE = new Tuple<>(0, TimeValue.ZERO);

    /**
     * 编译生成的实例对象 都会存储在这里
     */
    private final Cache<CacheKey, Object> cache;
    private final ScriptMetrics scriptMetrics;

    private final Object lock = new Object();

    // Mutable fields, visible for tests
    long lastInlineCompileTime;
    double scriptsPerTimeWindow;

    // Cache settings or derived from settings
    final int cacheSize;
    final TimeValue cacheExpire;
    /**
     * 实现限流功能需要的参数
     * v1代表令牌桶本身的容量   v2代表这个容量是以多少纳秒为单位  (最多允许预存多少纳秒的令牌数)
     */
    final Tuple<Integer, TimeValue> rate;
    /**
     * 每纳秒允许触发多少次编译
     */
    private final double compilesAllowedPerNano;
    private final String contextRateSetting;

    ScriptCache(
            int cacheMaxSize,
            TimeValue cacheExpire,
            Tuple<Integer, TimeValue> maxCompilationRate,
            String contextRateSetting
    ) {
        this.cacheSize = cacheMaxSize;
        this.cacheExpire = cacheExpire;
        this.contextRateSetting = contextRateSetting;

        CacheBuilder<CacheKey, Object> cacheBuilder = CacheBuilder.builder();
        if (this.cacheSize >= 0) {
            cacheBuilder.setMaximumWeight(this.cacheSize);
        }

        if (this.cacheExpire.getNanos() != 0) {
            cacheBuilder.setExpireAfterAccess(this.cacheExpire);
        }

        logger.debug("using script cache with max_size [{}], expire [{}]", this.cacheSize, this.cacheExpire);
        this.cache = cacheBuilder.removalListener(new ScriptCacheRemovalListener()).build();

        this.rate = maxCompilationRate;
        this.scriptsPerTimeWindow = this.rate.v1();
        this.compilesAllowedPerNano = ((double) rate.v1()) / rate.v2().nanos();

        this.lastInlineCompileTime = System.nanoTime();
        this.scriptMetrics = new ScriptMetrics();
    }

    /**
     * 编译某个脚本引擎
     * @param context  内部包含要创建的实例类型
     * @param scriptEngine  每种language对应一个engine 配合context用于解析数据  这个language不是指变成语言
     * @param id
     * @param idOrCode
     * @param type
     * @param options
     * @param <FactoryType>
     * @return
     */
    <FactoryType> FactoryType compile(
        ScriptContext<FactoryType> context,
        ScriptEngine scriptEngine,
        String id,
        String idOrCode,
        ScriptType type,
        Map<String, String> options
    ) {
        String lang = scriptEngine.getType();
        CacheKey cacheKey = new CacheKey(lang, idOrCode, context.name, options);
        // 先通过缓存键找到之前编译后保存起来的实例
        Object compiledScript = cache.get(cacheKey);

        // 转换成工厂实例对象 并返回
        if (compiledScript != null) {
            return context.factoryClazz.cast(compiledScript);
        }

        // Synchronize so we don't compile scripts many times during multiple shards all compiling a script
        // 这里还没有生成实例对象  首次创建
        synchronized (lock) {
            // Retrieve it again in case it has been put by a different thread
            compiledScript = cache.get(cacheKey);

            if (compiledScript == null) {
                try {
                    // Either an un-cached inline script or indexed script
                    // If the script type is inline the name will be the same as the code for identification in exceptions
                    // but give the script engine the chance to be better, give it separate name + source code
                    // for the inline case, then its anonymous: null.
                    if (logger.isTraceEnabled()) {
                        logger.trace("context [{}]: compiling script, type: [{}], lang: [{}], options: [{}]", context.name, type,
                            lang, options);
                    }
                    // Check whether too many compilations have happened
                    // 检查当前编译是否被限流
                    checkCompilationLimit();
                    // 通过引擎对象 配合相关参数编译工厂实例
                    compiledScript = scriptEngine.compile(id, idOrCode, context, options);
                } catch (ScriptException good) {
                    // TODO: remove this try-catch completely, when all script engines have good exceptions!
                    throw good; // its already good
                } catch (Exception exception) {
                    throw new GeneralScriptException("Failed to compile " + type + " script [" + id + "] using lang [" + lang + "]",
                            exception);
                }

                // Since the cache key is the script content itself we don't need to
                // invalidate/check the cache if an indexed script changes.
                scriptMetrics.onCompilation();
                cache.put(cacheKey, compiledScript);
            }

        }

        return context.factoryClazz.cast(compiledScript);
    }

    public ScriptStats stats() {
        return scriptMetrics.stats();
    }

    /**
     * Check whether there have been too many compilations within the last minute, throwing a circuit breaking exception if so.
     * This is a variant of the token bucket algorithm: https://en.wikipedia.org/wiki/Token_bucket
     *
     * It can be thought of as a bucket with water, every time the bucket is checked, water is added proportional to the amount of time that
     * elapsed since the last time it was checked. If there is enough water, some is removed and the request is allowed. If there is not
     * enough water the request is denied. Just like a normal bucket, if water is added that overflows the bucket, the extra water/capacity
     * is discarded - there can never be more water in the bucket than the size of the bucket.
     * 编译本身可能是一种比较消耗资源的操作 所以做了限流   采用的算法是令牌桶
     */
    void checkCompilationLimit() {
        if (rate.equals(UNLIMITED_COMPILATION_RATE)) {
            return;
        }

        long now = System.nanoTime();
        // 距离上一次编译 经过了多少时间
        long timePassed = now - lastInlineCompileTime;
        lastInlineCompileTime = now;

        // 得到在这个时间窗口中 允许执行多少次编译
        scriptsPerTimeWindow += (timePassed) * compilesAllowedPerNano;

        // It's been over the time limit anyway, readjust the bucket to be level
        // 如果长时间没有触发 compile 内部的令牌数会比较多 但是不允许超过令牌桶本身的上限
        if (scriptsPerTimeWindow > rate.v1()) {
            scriptsPerTimeWindow = rate.v1();
        }

        // If there is enough tokens in the bucket, allow the request and decrease the tokens by 1
        // 这个时间差换算出来的编译次数至少要超过1
        if (scriptsPerTimeWindow >= 1) {
            scriptsPerTimeWindow -= 1.0;
        } else {
            // 代表2次间隔时间过短 触发熔断
            scriptMetrics.onCompilationLimit();
            // Otherwise reject the request
            throw new CircuitBreakingException("[script] Too many dynamic script compilations within, max: [" +
                rate.v1() + "/" + rate.v2() +"]; please use indexed, or scripts with parameters instead; " +
                "this limit can be changed by the [" + contextRateSetting + "] setting",
                CircuitBreaker.Durability.TRANSIENT);
        }
    }

    /**
     * A small listener for the script cache that calls each
     * {@code ScriptEngine}'s {@code scriptRemoved} method when the
     * script has been removed from the cache
     */
    private class ScriptCacheRemovalListener implements RemovalListener<CacheKey, Object> {
        @Override
        public void onRemoval(RemovalNotification<CacheKey, Object> notification) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "removed [{}] from cache, reason: [{}]",
                    notification.getValue(),
                    notification.getRemovalReason()
                );
            }
            scriptMetrics.onCacheEviction();
        }
    }

    /**
     * 通过相关属性来实现唯一性的 缓存键
     */
    private static final class CacheKey {
        final String lang;
        final String idOrCode;
        final String context;
        final Map<String, String> options;

        private CacheKey(String lang, String idOrCode, String context, Map<String, String> options) {
            this.lang = lang;
            this.idOrCode = idOrCode;
            this.context = context;
            this.options = options;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(lang, cacheKey.lang) &&
                Objects.equals(idOrCode, cacheKey.idOrCode) &&
                Objects.equals(context, cacheKey.context) &&
                Objects.equals(options, cacheKey.options);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lang, idOrCode, context, options);
        }
    }
}
