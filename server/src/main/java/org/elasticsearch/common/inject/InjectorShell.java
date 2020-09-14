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

    private final List<Element> elements;
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
            this.state = new InheritingState(parent.state);
            return this;
        }

        /**
         * 更新builder的 stage信息
         * @param stage
         * @return
         */
        Builder stage(Stage stage) {
            this.stage = stage;
            return this;
        }

        Builder privateElements(PrivateElements privateElements) {
            this.privateElements = (PrivateElementsImpl) privateElements;
            this.elements.addAll(privateElements.getElements());
            return this;
        }

        /**
         * 设置 module
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
         * @param initializer  该对象所属的初始器
         * @param bindingProcessor  绑定请求相关的处理器
         */
        List<InjectorShell> build(Initializer initializer, BindingProcessor bindingProcessor,
                                  Stopwatch stopwatch, Errors errors) {
            if (stage == null) {
                throw new IllegalStateException("Stage not initialized");
            }
            // 默认情况下 privateElements 为null
            if (privateElements != null && parent == null) {
                throw new IllegalStateException("PrivateElements with no parent");
            }
            if (state == null) {
                throw new IllegalStateException("no state. Did you remember to lock() ?");
            }

            // 生成注入实现类
            InjectorImpl injector = new InjectorImpl(state, initializer);
            if (privateElements != null) {
                privateElements.initInjector(injector);
            }

            // bind Stage and Singleton if this is a top-level injector
            // 默认情况下 parent也是null
            if (parent == null) {
                // 此时在现有module下 追加一个rootModule
                modules.add(0, new RootModule(stage));
                // 往 injector.state 中注入各种转换器/匹配器
                new TypeConverterBindingProcessor(errors).prepareBuiltInConverters(injector);
            }

            // 将之前的module 包装成element
            elements.addAll(Elements.getElements(stage, modules));
            stopwatch.resetAndLog("Module execution");

            new MessageProcessor(errors).process(injector, elements);

            new TypeListenerBindingProcessor(errors).process(injector, elements);
            List<TypeListenerBinding> listenerBindings = injector.state.getTypeListenerBindings();
            injector.membersInjectorStore = new MembersInjectorStore(injector, listenerBindings);
            stopwatch.resetAndLog("TypeListeners creation");

            new ScopeBindingProcessor(errors).process(injector, elements);
            stopwatch.resetAndLog("Scopes creation");

            new TypeConverterBindingProcessor(errors).process(injector, elements);
            stopwatch.resetAndLog("Converters creation");

            bindInjector(injector);
            bindLogger(injector);
            bindingProcessor.process(injector, elements);
            stopwatch.resetAndLog("Binding creation");

            List<InjectorShell> injectorShells = new ArrayList<>();
            injectorShells.add(new InjectorShell(elements, injector));

            // recursively build child shells
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
     */
    private static void bindInjector(InjectorImpl injector) {
        Key<Injector> key = Key.get(Injector.class);
        InjectorFactory injectorFactory = new InjectorFactory(injector);
        injector.state.putBinding(key,
                new ProviderInstanceBindingImpl<>(injector, key, SourceProvider.UNKNOWN_SOURCE,
                        injectorFactory, Scoping.UNSCOPED, injectorFactory,
                        emptySet()));
    }

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
     * 代表一个根模块
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
            binder.bind(Stage.class).toInstance(stage);
            binder.bindScope(Singleton.class, SINGLETON);
        }
    }
}
