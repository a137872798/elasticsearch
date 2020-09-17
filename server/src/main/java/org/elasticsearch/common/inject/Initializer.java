/*
 * Copyright (C) 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.spi.InjectionPoint;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Manages and injects instances at injector-creation time. This is made more complicated by
 * instances that request other instances while they're being injected. We overcome this by using
 * {@link Initializable}, which attempts to perform injection before use.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * 初始化对象   维护所有待注入的对象
 */
class Initializer {
    /**
     * the only thread that we'll use to inject members.
     * 初始化 Initializer 的线程
     */
    private final Thread creatingThread = Thread.currentThread();

    /**
     * zero means everything is injected.
     * 当还未注入时 该值为1 应该是有一个向外暴露的接口检测该值 当该值归0时 代表注入完成
     */
    private final CountDownLatch ready = new CountDownLatch(1);

    /**
     * Maps instances that need injection to a source that registered them
     * 维护所有引用对象 (引用对象代表他们需要被注入属性)
     */
    private final Map<Object, InjectableReference<?>> pendingInjection = new IdentityHashMap<>();

    /**
     * Registers an instance for member injection when that step is performed.
     *
     * @param instance an instance that optionally has members to be injected (each annotated with
     * @param source   the source location that this injection was requested
     * @Inject).
     * 传入指定的注入器 以及待注入的实例对象还有需要处理的注入点 将他们包装成 InjectableReference对象  并设置到容器中
     */
    public <T> Initializable<T> requestInjection(InjectorImpl injector, T instance, Object source,
                                                 Set<InjectionPoint> injectionPoints) {
        Objects.requireNonNull(source);

        // short circuit if the object has no injections
        // 如果待处理对象没有注入点 直接返回   注意这里还需要确保监听器不存在 否则 还是要创建一遍reference对象 以便走监听器的逻辑
        if (instance == null
                || (injectionPoints.isEmpty() && !injector.membersInjectorStore.hasTypeListeners())) {
            return Initializables.of(instance);
        }

        InjectableReference<T> initializable = new InjectableReference<>(injector, instance, source);
        pendingInjection.put(instance, initializable);
        return initializable;
    }

    /**
     * Prepares member injectors for all injected instances. This prompts Guice to do static analysis
     * on the injected instances.
     * 对每个待注入对象进行校验 确保携带的注入器能够为该type注入属性
     */
    void validateOustandingInjections(Errors errors) {
        for (InjectableReference<?> reference : pendingInjection.values()) {
            try {
                reference.validate(errors);
            } catch (ErrorsException e) {
                errors.merge(e.getErrors());
            }
        }
    }

    /**
     * Performs creation-time injections on all objects that require it. Whenever fulfilling an
     * injection depends on another object that requires injection, we inject it first. If the two
     * instances are codependent (directly or transitively), ordering of injection is arbitrary.
     * 调用get() 方法会触发注入  这里就是为所有待注入的对象进行注入
     */
    void injectAll(final Errors errors) {
        // loop over a defensive copy since ensureInjected() mutates the set. Unfortunately, that copy
        // is made complicated by a bug in IBM's JDK, wherein entrySet().toArray(Object[]) doesn't work
        for (InjectableReference<?> reference : new ArrayList<>(pendingInjection.values())) {
            try {
                reference.get(errors);
            } catch (ErrorsException e) {
                errors.merge(e.getErrors());
            }
        }

        if (!pendingInjection.isEmpty()) {
            throw new AssertionError("Failed to satisfy " + pendingInjection);
        }

        // 当注入完成时 唤醒线程
        ready.countDown();
    }

    /**
     * 代表一个 可注入对象的引用  当注入完成时 允许返回对象
     * @param <T>
     */
    private class InjectableReference<T> implements Initializable<T> {

        /**
         * 该对象负责属性的注入
         */
        private final InjectorImpl injector;

        /**
         * 待注入的实例对象
         */
        private final T instance;
        private final Object source;

        /**
         * 从injector获取的 专门针对field注入的对象
         */
        private MembersInjectorImpl<T> membersInjector;

        /**
         *
         * @param injector  为实例注入属性使用的注入器
         * @param instance  实例对象本身
         * @param source   注入发起者
         */
        InjectableReference(InjectorImpl injector, T instance, Object source) {
            this.injector = injector;
            this.instance = Objects.requireNonNull(instance, "instance");
            this.source = Objects.requireNonNull(source, "source");
        }

        /**
         * 确保该注入器具备针对该type进行注入的能力
         * @param errors
         * @throws ErrorsException
         */
        public void validate(Errors errors) throws ErrorsException {
            @SuppressWarnings("unchecked") // the type of 'T' is a TypeLiteral<T>
                    TypeLiteral<T> type = TypeLiteral.get((Class<T>) instance.getClass());
            membersInjector = injector.membersInjectorStore.get(type, errors.withSource(source));
        }

        /**
         * Reentrant. If {@code instance} was registered for injection at injector-creation time, this
         * method will ensure that all its members have been injected before returning.
         */
        @Override
        public T get(Errors errors) throws ErrorsException {
            // 代表注入已经完成了 直接返回实例
            if (ready.getCount() == 0) {
                return instance;
            }

            // just wait for everything to be injected by another thread
            // 其余线程调用 get 会阻塞等待创建线程执行完 injectAll
            if (Thread.currentThread() != creatingThread) {
                try {
                    ready.await();
                    return instance;
                } catch (InterruptedException e) {
                    // Give up, since we don't know if our injection is ready
                    throw new RuntimeException(e);
                }
            }

            // toInject needs injection, do it right away. we only do this once, even if it fails
            // IdentityHashMap 的特性就是key的比较是通过 == 而不是 equals 这里当注入完成时 从待注入列表中移除
            if (pendingInjection.remove(instance) != null) {
                // 注入完成时 触发监听器
                membersInjector.injectAndNotify(instance, errors.withSource(source));
            }

            return instance;
        }

        @Override
        public String toString() {
            return instance.toString();
        }
    }
}
