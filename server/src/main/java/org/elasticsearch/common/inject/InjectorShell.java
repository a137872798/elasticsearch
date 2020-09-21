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
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.internal.InternalFactory;
import org.elasticsearch.common.inject.internal.PrivateElementsImpl;
import org.elasticsearch.common.inject.internal.ProviderInstanceBindingImpl;
import org.elasticsearch.common.inject.internal.Scoping;
import org.elasticsearch.common.inject.internal.SourceProvider;
import org.elasticsearch.common.inject.internal.Stopwatch;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.inject.spi.Element;
import org.elasticsearch.common.inject.spi.Elements;
import org.elasticsearch.common.inject.spi.InjectionPoint;
import org.elasticsearch.common.inject.spi.PrivateElements;
import org.elasticsearch.common.inject.spi.TypeListenerBinding;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

import static java.util.Collections.emptySet;
import static org.elasticsearch.common.inject.Scopes.SINGLETON;

/**
 * A partially-initialized injector. See {@link InjectorBuilder}, which uses this to build a tree
 * of injectors in batch.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
class InjectorShell {

    /**
     * 该shell对象处理了多少element
     */
    private final List<Element> elements;
    /**
     * 这些element被注册到哪个injector.state下
     */
    private final InjectorImpl injector;

    private InjectorShell(List<Element> elements, InjectorImpl injector) {
        this.elements = elements;
        this.injector = injector;
    }

    InjectorImpl getInjector() {
        return injector;
    }

    List<Element> getElements() {
        return elements;
    }


    /**
     */
    static class Builder {
        private final List<Element> elements = new ArrayList<>();

        /**
         * 存储本次生成inject对象需要的module
         */
        private final List<Module> modules = new ArrayList<>();

        /**
         * lazily constructed
         */
        private State state;

        private InjectorImpl parent;
        private Stage stage;

        /**
         * null unless this exists in a {@link Binder#newPrivateBinder private environment}
         */
        private PrivateElementsImpl privateElements;

        Builder parent(InjectorImpl parent) {
            this.parent = parent;
            // 以之前parent.state 作为当前state的父级对象
            this.state = new InheritingState(parent.state);
            return this;
        }

        /**
         * 该对象应该是反复使用的 每次处理一个新的binder对象前 先更新成对应的 stage
         * @param stage
         * @return
         */
        Builder stage(Stage stage) {
            this.stage = stage;
            return this;
        }

        /**
         * 代表当前处理的elements 是从某个binder中分裂出来的新binder
         * @param privateElements
         * @return
         */
        Builder privateElements(PrivateElements privateElements) {
            this.privateElements = (PrivateElementsImpl) privateElements;
            this.elements.addAll(privateElements.getElements());
            return this;
        }

        /**
         * 设置 module  之后会使用一个 binder对象 执行这些module对象 并存储绑定关系
         * @param modules
         */
        void addModules(Iterable<? extends Module> modules) {
            for (Module module : modules) {
                this.modules.add(module);
            }
        }

        /**
         * Synchronize on this before calling {@link #build}.
         */
        Object lock() {
            return getState().lock();
        }

        /**
         * Creates and returns the injector shells for the current modules. Multiple shells will be
         * returned if any modules contain {@link Binder#newPrivateBinder private environments}. The
         * primary injector will be first in the returned list.
         * @param initializer  该对象负责管理下面待注入的实例 并进行统一注入
         * @param bindingProcessor  绑定请求相关的处理器
         * @return 代表总计处理了多少组 element (以binder为单位)
         */
        List<InjectorShell> build(Initializer initializer, BindingProcessor bindingProcessor,
                                  Stopwatch stopwatch, Errors errors) {
            if (stage == null) {
                throw new IllegalStateException("Stage not initialized");
            }
            if (privateElements != null && parent == null) {
                throw new IllegalStateException("PrivateElements with no parent");
            }
            if (state == null) {
                throw new IllegalStateException("no state. Did you remember to lock() ?");
            }

            // 该对象包含一个 jit生成binding的逻辑  并且定义了一个 基于key从ioc容器中查询binding的模板
            InjectorImpl injector = new InjectorImpl(state, initializer);

            // 将injector 设置到privateElements 上
            if (privateElements != null) {
                privateElements.initInjector(injector);
            }

            // bind Stage and Singleton if this is a top-level injector
            if (parent == null) {
                // 在用户设置的各种module之前 添加一个rootModule
                modules.add(0, new RootModule(stage));
                // 设置类型转换器  通过匹配key 后使用转换器将 string字面量转换成需要的值
                // prepareBuiltInConverters() 就是将转换器设置到 injector.state 中
                new TypeConverterBindingProcessor(errors).prepareBuiltInConverters(injector);
            }

            // 生成一个临时的binder 处理所有modules后 生成一组elements 之后填充到 elements 中
            elements.addAll(Elements.getElements(stage, modules));
            // 输出耗时时长
            stopwatch.resetAndLog("Module execution");

            // 生成异常消息处理器 并处理此时生成的所有消息
            new MessageProcessor(errors).process(injector, elements);

            new TypeListenerBindingProcessor(errors).process(injector, elements);
            // 找到之前在elements中设置的监听器  同时也代表在之前处理module时 用户就应该完成所有的绑定工作
            List<TypeListenerBinding> listenerBindings = injector.state.getTypeListenerBindings();
            // 设置membersInjectorStore
            injector.membersInjectorStore = new MembersInjectorStore(injector, listenerBindings);
            stopwatch.resetAndLog("TypeListeners creation");

            // 处理描述范围的注解
            new ScopeBindingProcessor(errors).process(injector, elements);
            stopwatch.resetAndLog("Scopes creation");

            // 如果有用户自行定义的转换器 也加入到 state中
            new TypeConverterBindingProcessor(errors).process(injector, elements);
            stopwatch.resetAndLog("Converters creation");

            // 将injector 与 binding 设置到ioc容器中
            bindInjector(injector);
            // 将Logger以及binding 设置到ioc容器
            bindLogger(injector);
            // 该对象是核心对象  处理binding对象 并存入到 state中
            bindingProcessor.process(injector, elements);
            stopwatch.resetAndLog("Binding creation");

            // 代表已经完成一个阶段的elements处理了 此时将他们包装成一个InjectorShell 对象
            List<InjectorShell> injectorShells = new ArrayList<>();
            injectorShells.add(new InjectorShell(elements, injector));

            // recursively build child shells
            // 在用户定义的module中  可以通过调用newPrivateBinder 生成一个子级binder
            PrivateElementProcessor processor = new PrivateElementProcessor(errors, stage);
            processor.process(injector, elements);
            for (Builder builder : processor.getInjectorShellBuilders()) {
                injectorShells.addAll(builder.build(initializer, bindingProcessor, stopwatch, errors));
            }
            stopwatch.resetAndLog("Private environment creation");

            return injectorShells;
        }

        private State getState() {
            if (state == null) {
                state = new InheritingState(State.NONE);
            }
            return state;
        }
    }

    /**
     * The Injector is a special case because we allow both parent and child injectors to both have
     * a binding for that key.
     * 为 Injector 在ioc容器中注册 binding对象
     * 注意 在父子的state中分别维护 Injector 的binding对象 (其他注入对象会通过黑名单机制确保在整个state链中只存在一个)
     */
    private static void bindInjector(InjectorImpl injector) {
        Key<Injector> key = Key.get(Injector.class);
        InjectorFactory injectorFactory = new InjectorFactory(injector);
        // 手动将映射关系存储到state中
        injector.state.putBinding(key,
                new ProviderInstanceBindingImpl<>(injector, key, SourceProvider.UNKNOWN_SOURCE,
                        injectorFactory, Scoping.UNSCOPED, injectorFactory,
                        emptySet()));
    }

    /**
     * 对应返回实例的工厂 这里就是直接返回初始化该对象时使用的 injector  也就代表它默认是单例模式
     */
    private static class InjectorFactory implements InternalFactory<Injector>, Provider<Injector> {
        private final Injector injector;

        private InjectorFactory(Injector injector) {
            this.injector = injector;
        }

        @Override
        public Injector get(Errors errors, InternalContext context, Dependency<?> dependency)
                throws ErrorsException {
            return injector;
        }

        @Override
        public Injector get() {
            return injector;
        }

        @Override
        public String toString() {
            return "Provider<Injector>";
        }
    }

    /**
     * The Logger is a special case because it knows the injection point of the injected member. It's
     * the only binding that does this.
     */
    private static void bindLogger(InjectorImpl injector) {
        Key<Logger> key = Key.get(Logger.class);
        LoggerFactory loggerFactory = new LoggerFactory();
        injector.state.putBinding(key,
                new ProviderInstanceBindingImpl<>(injector, key,
                        SourceProvider.UNKNOWN_SOURCE, loggerFactory, Scoping.UNSCOPED,
                        loggerFactory, emptySet()));
    }

    private static class LoggerFactory implements InternalFactory<Logger>, Provider<Logger> {
        @Override
        public Logger get(Errors errors, InternalContext context, Dependency<?> dependency) {
            InjectionPoint injectionPoint = dependency.getInjectionPoint();
            return injectionPoint == null
                    ? Logger.getAnonymousLogger()
                    : Logger.getLogger(injectionPoint.getMember().getDeclaringClass().getName());
        }

        @Override
        public Logger get() {
            return Logger.getAnonymousLogger();
        }

        @Override
        public String toString() {
            return "Provider<Logger>";
        }
    }


    /**
     * 代表一个根模块  该module会放置在 modules的第一个位置
     */
    private static class RootModule implements Module {
        final Stage stage;

        private RootModule(Stage stage) {
            this.stage = Objects.requireNonNull(stage, "stage");
        }

        /**
         *
         * @param binder
         */
        @Override
        public void configure(Binder binder) {
            binder = binder.withSource(SourceProvider.UNKNOWN_SOURCE);
            // 下面2步操作添加的对象都需要通过visitor进行处理

            // 当需要注入 stage时 使用 stage实例
            binder.bind(Stage.class).toInstance(stage);
            // 往binder的elements中追加一个 描述注解以及处理范围的对象
            binder.bindScope(Singleton.class, SINGLETON);
        }
    }
}
