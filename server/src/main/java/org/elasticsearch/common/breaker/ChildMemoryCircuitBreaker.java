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

package org.elasticsearch.common.breaker;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Breaker that will check a parent's when incrementing
 * 熔断器的默认实现
 */
public class ChildMemoryCircuitBreaker implements CircuitBreaker {

    /**
     * 代表针对使用内存的限制
     */
    private final long memoryBytesLimit;
    /**
     * 类似一个转换率的东西 limit 需要 * 这个值才能与 memoryBytesLimit做比较
     */
    private final double overheadConstant;
    /**
     * 描述持久性
     */
    private final Durability durability;
    private final AtomicLong used;
    private final AtomicLong trippedCount;
    private final Logger logger;
    /**
     * 代表该熔断器是由哪个 CircuitBreakerService 创建的
     */
    private final HierarchyCircuitBreakerService parent;
    private final String name;

    /**
     * Create a circuit breaker that will break if the number of estimated
     * bytes grows above the limit. All estimations will be multiplied by
     * the given overheadConstant. This breaker starts with 0 bytes used.
     * @param settings settings to configure this breaker
     * @param parent parent circuit breaker service to delegate tripped breakers to    该断路器是由哪个 断路service创建的
     * @param name the name of the breaker
     */
    public ChildMemoryCircuitBreaker(BreakerSettings settings, Logger logger,
                                     HierarchyCircuitBreakerService parent, String name) {
        this(settings, null, logger, parent, name);
    }

    /**
     * Create a circuit breaker that will break if the number of estimated
     * bytes grows above the limit. All estimations will be multiplied by
     * the given overheadConstant. Uses the given oldBreaker to initialize
     * the starting offset.
     * @param settings settings to configure this breaker
     * @param parent parent circuit breaker service to delegate tripped breakers to
     * @param name the name of the breaker
     * @param oldBreaker the previous circuit breaker to inherit the used value from (starting offset)   这是有继承关系的么  会复用之前的数据
     */
    public ChildMemoryCircuitBreaker(BreakerSettings settings, ChildMemoryCircuitBreaker oldBreaker,
                                     Logger logger, HierarchyCircuitBreakerService parent, String name) {
        this.name = name;
        this.memoryBytesLimit = settings.getLimit();
        this.overheadConstant = settings.getOverhead();
        this.durability = settings.getDurability();
        if (oldBreaker == null) {
            this.used = new AtomicLong(0);
            this.trippedCount = new AtomicLong(0);
        } else {
            this.used = oldBreaker.used;
            this.trippedCount = oldBreaker.trippedCount;
        }
        this.logger = logger;
        if (logger.isTraceEnabled()) {
            logger.trace("creating ChildCircuitBreaker with settings {}", settings);
        }
        this.parent = parent;
    }

    /**
     * Method used to trip the breaker, delegates to the parent to determine
     * whether to trip the breaker or not
     * 触发熔断
     */
    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        this.trippedCount.incrementAndGet();
        final String message = "[" + this.name + "] Data too large, data for [" + fieldName + "]" +
                " would be [" + bytesNeeded + "/" + new ByteSizeValue(bytesNeeded) + "]" +
                ", which is larger than the limit of [" +
                memoryBytesLimit + "/" + new ByteSizeValue(memoryBytesLimit) + "]";
        logger.debug("{}", message);
        throw new CircuitBreakingException(message, bytesNeeded, memoryBytesLimit, durability);
    }

    /**
     * Add a number of bytes, tripping the circuit breaker if the aggregated
     * estimates are above the limit. Automatically trips the breaker if the
     * memory limit is set to 0. Will never trip the breaker if the limit is
     * set &lt; 0, but can still be used to aggregate estimations.
     * @param bytes number of bytes to add to the breaker
     * @return number of "used" bytes so far
     * 增加将要使用的内存 并检测是否会发生熔断
     */
    @Override
    public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        // short-circuit on no data allowed, immediately throwing an exception
        // 参数本身设置无效值时 立即触发熔断逻辑
        if (memoryBytesLimit == 0) {
            circuitBreak(label, bytes);
        }

        long newUsed;
        // If there is no limit (-1), we can optimize a bit by using
        // .addAndGet() instead of looping (because we don't have to check a
        // limit), which makes the RamAccountingTermsEnum case faster.
        if (this.memoryBytesLimit == -1) {
            // 代表不做限制  此时直接使用AtomicLong.addAndGet 来替代 loop 因为不需要在每次尝试时检测limit是否超标
            // newUsed 代表此时已经使用的总内存
            newUsed = noLimit(bytes, label);
        } else {
            // 在这里没抛出异常 就代表没有触发熔断
            newUsed = limit(bytes, label);
        }

        // Additionally, we need to check that we haven't exceeded the parent's limit
        try {
            // 还需要检测父对象是否发生熔断 推测父对象会维护所有分配的熔断器使用的内存 可能父对象单独维护一个limit值
            parent.checkParentLimit((long) (bytes * overheadConstant), label);
        } catch (CircuitBreakingException e) {
            // If the parent breaker is tripped, this breaker has to be
            // adjusted back down because the allocation is "blocked" but the
            // breaker has already been incremented
            this.addWithoutBreaking(-bytes);
            throw e;
        }
        return newUsed;
    }

    private long noLimit(long bytes, String label) {
        long newUsed;
        newUsed = this.used.addAndGet(bytes);
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] Adding [{}][{}] to used bytes [new used: [{}], limit: [-1b]]",
                this.name, new ByteSizeValue(bytes), label, new ByteSizeValue(newUsed));
        }
        return newUsed;
    }

    /**
     * 尝试申请更多的内存
     * @param bytes
     * @param label
     * @return
     */
    private long limit(long bytes, String label) {
        long newUsed;// Otherwise, check the addition and commit the addition, looping if
        // there are conflicts. May result in additional logging, but it's
        // trace logging and shouldn't be counted on for additions.
        long currentUsed;
        do {
            currentUsed = this.used.get();
            newUsed = currentUsed + bytes;
            long newUsedWithOverhead = (long) (newUsed * overheadConstant);
            if (logger.isTraceEnabled()) {
                logger.trace("[{}] Adding [{}][{}] to used bytes [new used: [{}], limit: {} [{}], estimate: {} [{}]]",
                        this.name,
                        new ByteSizeValue(bytes), label, new ByteSizeValue(newUsed),
                        memoryBytesLimit, new ByteSizeValue(memoryBytesLimit),
                        newUsedWithOverhead, new ByteSizeValue(newUsedWithOverhead));
            }
            if (memoryBytesLimit > 0 && newUsedWithOverhead > memoryBytesLimit) {
                logger.warn("[{}] New used memory {} [{}] for data of [{}] would be larger than configured breaker: {} [{}], breaking",
                        this.name,
                        newUsedWithOverhead, new ByteSizeValue(newUsedWithOverhead), label,
                        memoryBytesLimit, new ByteSizeValue(memoryBytesLimit));
                // 触发熔断
                circuitBreak(label, newUsedWithOverhead);
            }
            // Attempt to set the new used value, but make sure it hasn't changed
            // underneath us, if it has, keep trying until we are able to set it
        } while (!this.used.compareAndSet(currentUsed, newUsed));
        return newUsed;
    }

    /**
     * Add an <b>exact</b> number of bytes, not checking for tripping the
     * circuit breaker. This bypasses the overheadConstant multiplication.
     *
     * Also does not check with the parent breaker to see if the parent limit
     * has been exceeded.
     *
     * @param bytes number of bytes to add to the breaker
     * @return number of "used" bytes so far
     * 代表在熔断前已使用的内存大小
     */
    @Override
    public long addWithoutBreaking(long bytes) {
        long u = used.addAndGet(bytes);
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] Adjusted breaker by [{}] bytes, now [{}]", this.name, bytes, u);
        }
        assert u >= 0 : "Used bytes: [" + u + "] must be >= 0";
        return u;
    }

    /**
     * @return the number of aggregated "used" bytes so far
     */
    @Override
    public long getUsed() {
        return this.used.get();
    }

    /**
     * @return the number of bytes that can be added before the breaker trips
     */
    @Override
    public long getLimit() {
        return this.memoryBytesLimit;
    }

    /**
     * @return the constant multiplier the breaker uses for aggregations
     */
    @Override
    public double getOverhead() {
        return this.overheadConstant;
    }

    /**
     * @return the number of times the breaker has been tripped
     */
    @Override
    public long getTrippedCount() {
        return this.trippedCount.get();
    }

    /**
     * @return the name of the breaker
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * @return whether a tripped circuit breaker will reset itself (transient) or requires manual intervention (permanent).
     */
    @Override
    public Durability getDurability() {
        return this.durability;
    }
}
