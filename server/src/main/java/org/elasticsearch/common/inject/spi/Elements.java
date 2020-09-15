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

package org.elasticsearch.common.inject.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.MembersInjector;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PrivateBinder;
import org.elasticsearch.common.inject.PrivateModule;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Scope;
import org.elasticsearch.common.inject.Stage;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.binder.AnnotatedBindingBuilder;
import org.elasticsearch.common.inject.binder.AnnotatedConstantBindingBuilder;
import org.elasticsearch.common.inject.binder.AnnotatedElementBuilder;
import org.elasticsearch.common.inject.internal.AbstractBindingBuilder;
import org.elasticsearch.common.inject.internal.BindingBuilder;
import org.elasticsearch.common.inject.internal.ConstantBindingBuilderImpl;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ExposureBuilder;
import org.elasticsearch.common.inject.internal.PrivateElementsImpl;
import org.elasticsearch.common.inject.internal.ProviderMethodsModule;
import org.elasticsearch.common.inject.internal.SourceProvider;
import org.elasticsearch.common.inject.matcher.Matcher;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Exposes elements of a module so they can be inspected, validated or {@link
 * Element#applyTo(Binder) rewritten}.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public final class Elements {

    /**
     * Records the elements executed by {@code modules}.
     */
    public static List<Element> getElements(Module... modules) {
        return getElements(Stage.DEVELOPMENT, Arrays.asList(modules));
    }

    /**
     * Records the elements executed by {@code modules}.
     */
    public static List<Element> getElements(Iterable<? extends Module> modules) {
        return getElements(Stage.DEVELOPMENT, modules);
    }

    /**
     * Records the elements executed by {@code modules}.
     * 构建recordsBinder对象 处理 module
     */
    public static List<Element> getElements(Stage stage, Iterable<? extends Module> modules) {
        RecordingBinder binder = new RecordingBinder(stage);
        // 将所有模块对象交由 binder处理
        for (Module module : modules) {
            binder.install(module);
        }
        // 在处理完所有module后  返回 elements
        return Collections.unmodifiableList(binder.elements);
    }

    /**
     * Returns the module composed of {@code elements}.
     */
    public static Module getModule(final Iterable<? extends Element> elements) {
        return new Module() {
            @Override
            public void configure(Binder binder) {
                for (Element element : elements) {
                    element.applyTo(binder);
                }
            }
        };
    }

    /**
     * Binder 内置实现只有该类
     */
    private static class RecordingBinder implements Binder, PrivateBinder {
        private final Stage stage;
        private final Set<Module> modules;

        /**
         * 推测绑定的 对象会存储到这个列表中
         */
        private final List<Element> elements;
        private final Object source;
        private final SourceProvider sourceProvider;

        /**
         * The binder where exposed bindings will be created
         */
        private final RecordingBinder parent;
        private final PrivateElementsImpl privateElements;


        /**
         * 使用一个描述阶段的对象进行初始化
         * @param stage
         */
        private RecordingBinder(Stage stage) {
            this.stage = stage;
            this.modules = new HashSet<>();
            this.elements = new ArrayList<>();
            this.source = null;
            // 标记某些 provider 需要被跳过
            this.sourceProvider = new SourceProvider().plusSkippedClasses(
                    Elements.class, RecordingBinder.class, AbstractModule.class,
                    ConstantBindingBuilderImpl.class, AbstractBindingBuilder.class, BindingBuilder.class);
            this.parent = null;
            this.privateElements = null;
        }

        /**
         * Creates a recording binder that's backed by {@code prototype}.
         * 大部分数据从 旧对象从拷贝 但是本次指定了source
         */
        private RecordingBinder(
                RecordingBinder prototype, Object source, SourceProvider sourceProvider) {
            if (!(source == null ^ sourceProvider == null)) {
                throw new IllegalArgumentException();
            }

            this.stage = prototype.stage;
            this.modules = prototype.modules;
            this.elements = prototype.elements;
            this.source = source;
            this.sourceProvider = sourceProvider;
            this.parent = prototype.parent;
            this.privateElements = prototype.privateElements;
        }

        /**
         * Creates a private recording binder.
         */
        private RecordingBinder(RecordingBinder parent, PrivateElementsImpl privateElements) {
            this.stage = parent.stage;
            this.modules = new HashSet<>();
            this.elements = privateElements.getElementsMutable();
            this.source = parent.source;
            this.sourceProvider = parent.sourceProvider;
            this.parent = parent;
            this.privateElements = privateElements;
        }

        @Override
        public void bindScope(Class<? extends Annotation> annotationType, Scope scope) {
            elements.add(new ScopeBinding(getSource(), annotationType, scope));
        }

        @Override
        @SuppressWarnings("unchecked") // it is safe to use the type literal for the raw type
        public void requestInjection(Object instance) {
            requestInjection((TypeLiteral) TypeLiteral.get(instance.getClass()), instance);
        }

        @Override
        public <T> void requestInjection(TypeLiteral<T> type, T instance) {
            elements.add(new InjectionRequest<>(getSource(), type, instance));
        }

        @Override
        public <T> MembersInjector<T> getMembersInjector(final TypeLiteral<T> typeLiteral) {
            final MembersInjectorLookup<T> element
                    = new MembersInjectorLookup<>(getSource(), typeLiteral);
            elements.add(element);
            return element.getMembersInjector();
        }

        @Override
        public <T> MembersInjector<T> getMembersInjector(Class<T> type) {
            return getMembersInjector(TypeLiteral.get(type));
        }

        @Override
        public void bindListener(Matcher<? super TypeLiteral<?>> typeMatcher, TypeListener listener) {
            elements.add(new TypeListenerBinding(getSource(), listener, typeMatcher));
        }

        @Override
        public void requestStaticInjection(Class<?>... types) {
            for (Class<?> type : types) {
                elements.add(new StaticInjectionRequest(getSource(), type));
            }
        }

        /**
         * 添加这些module对象 在添加过程中 还会进行配置
         * @param module
         */
        @Override
        public void install(Module module) {
            // 重复添加则不处理
            if (modules.add(module)) {
                Binder binder = this;
                // TODO 先忽略私有模块
                if (module instanceof PrivateModule) {
                    binder = binder.newPrivateBinder();
                }

                try {
                    // 装配 binder对象  实际上就是调用binder.bind
                    module.configure(binder);
                } catch (IllegalArgumentException e) {
                    // NOTE: This is not in the original guice. We rethrow here to expose any explicit errors in configure()
                    throw e;
                } catch (RuntimeException e) {
                    Collection<Message> messages = Errors.getMessagesFromThrowable(e);
                    if (!messages.isEmpty()) {
                        elements.addAll(messages);
                    } else {
                        addError(e);
                    }
                }
                // 将module包装后 重新调用了一遍该方法
                binder.install(ProviderMethodsModule.forModule(module));
            }
        }

        @Override
        public Stage currentStage() {
            return stage;
        }

        @Override
        public void addError(String message, Object... arguments) {
            elements.add(new Message(getSource(), Errors.format(message, arguments)));
        }

        @Override
        public void addError(Throwable t) {
            String message = "An exception was caught and reported. Message: " + t.getMessage();
            elements.add(new Message(Collections.singletonList(getSource()), message, t));
        }

        @Override
        public void addError(Message message) {
            elements.add(message);
        }

        /**
         * 每当为该binder对象增加一个要处理的module时 会生成一个新的builder对象
         * @param key
         * @param <T>
         * @return
         */
        @Override
        public <T> AnnotatedBindingBuilder<T> bind(Key<T> key) {
            // getSource() 默认情况下是返回异常栈轨迹信息
            return new BindingBuilder<>(this, elements, getSource(), key);
        }

        @Override
        public <T> AnnotatedBindingBuilder<T> bind(TypeLiteral<T> typeLiteral) {
            return bind(Key.get(typeLiteral));
        }

        /**
         * 在初始化Node对象的过程中 会将各种组件类绑定到Binder上
         * @param type
         * @param <T>
         * @return
         */
        @Override
        public <T> AnnotatedBindingBuilder<T> bind(Class<T> type) {
            return bind(Key.get(type));
        }

        @Override
        public AnnotatedConstantBindingBuilder bindConstant() {
            return new ConstantBindingBuilderImpl<Void>(this, elements, getSource());
        }

        @Override
        public <T> Provider<T> getProvider(final Key<T> key) {
            final ProviderLookup<T> element = new ProviderLookup<>(getSource(), key);
            elements.add(element);
            return element.getProvider();
        }

        @Override
        public <T> Provider<T> getProvider(Class<T> type) {
            return getProvider(Key.get(type));
        }

        @Override
        public void convertToTypes(Matcher<? super TypeLiteral<?>> typeMatcher,
                                   TypeConverter converter) {
            elements.add(new TypeConverterBinding(getSource(), typeMatcher, converter));
        }

        /**
         * 每当指定 source时 会返回一个新的  RecordingBinder 对象
         * @param source any object representing the source location and has a
         *               concise {@link Object#toString() toString()} value
         * @return
         */
        @Override
        public RecordingBinder withSource(final Object source) {
            return new RecordingBinder(this, source, null);
        }

        /**
         * 记录需要跳过的 class
         * @param classesToSkip library classes that create bindings on behalf of
         *                      their clients.
         * @return
         */
        @Override
        public RecordingBinder skipSources(Class... classesToSkip) {
            // if a source is specified explicitly, we don't need to skip sources
            // 如果明确的声明了 source 那么就不需要考虑skip了 skip是针对那种在全范围的基础上跳过某些类
            if (source != null) {
                return this;
            }

            // 这个sourceProvider 仅提供栈轨迹信息   在调用plusSkippedClasses 后某些类的栈轨迹信息就不会被获取
            SourceProvider newSourceProvider = sourceProvider.plusSkippedClasses(classesToSkip);
            return new RecordingBinder(this, null, newSourceProvider);
        }

        @Override
        public PrivateBinder newPrivateBinder() {
            PrivateElementsImpl privateElements = new PrivateElementsImpl(getSource());
            elements.add(privateElements);
            return new RecordingBinder(this, privateElements);
        }

        @Override
        public void expose(Key<?> key) {
            exposeInternal(key);
        }

        @Override
        public AnnotatedElementBuilder expose(Class<?> type) {
            return exposeInternal(Key.get(type));
        }

        @Override
        public AnnotatedElementBuilder expose(TypeLiteral<?> type) {
            return exposeInternal(Key.get(type));
        }

        private <T> AnnotatedElementBuilder exposeInternal(Key<T> key) {
            if (privateElements == null) {
                addError("Cannot expose %s on a standard binder. "
                        + "Exposed bindings are only applicable to private binders.", key);
                return new AnnotatedElementBuilder() {
                    @Override
                    public void annotatedWith(Class<? extends Annotation> annotationType) {
                    }

                    @Override
                    public void annotatedWith(Annotation annotation) {
                    }
                };
            }

            ExposureBuilder<T> builder = new ExposureBuilder<>(this, getSource(), key);
            privateElements.addExposureBuilder(builder);
            return builder;
        }

        private static Logger logger = LogManager.getLogger(Elements.class);

        /**
         * 获取要处理的数据源   可能该对象在初始化时 就指定了source 也可能需要借助 sourceProvider来获取 同时sourceProvider内部会指定哪些class需要被跳过
         * @return
         */
        protected Object getSource() {
            Object ret;
            if (logger.isDebugEnabled()) {
                ret = sourceProvider != null
                        ? sourceProvider.get()
                        : source;
            } else {
                ret = source;
            }
            return ret == null ? "_unknown_" : ret;
        }

        @Override
        public String toString() {
            return "Binder";
        }
    }
}
