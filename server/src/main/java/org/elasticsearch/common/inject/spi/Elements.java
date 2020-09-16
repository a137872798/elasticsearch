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
     * 从 Binder 的api观察 它的核心功能就是维护注入点与实际注入类的绑定关系 还可以通过传入注入点 返回一个提供实例的provider
     */
    private static class RecordingBinder implements Binder, PrivateBinder {

        /**
         * 代表当前ioc容器的模式  是否提前进行检查 还是不检查 etc
         */
        private final Stage stage;

        /**
         * 代表该对象处理过那些module中定义的绑定逻辑
         */
        private final Set<Module> modules;

        /**
         * 推测绑定的 对象会存储到这个列表中
         * TODO element是什么 ???
         */
        private final List<Element> elements;

        /**
         * 表示该binder对象是否在一开始就指定了是针对那个实例而言的
         */
        private final Object source;
        /**
         * source 或者 sourceProvider 仅能存在一个  sourceProvider 负责返回栈轨迹中最先弹出的类 也可以理解为当前类
         */
        private final SourceProvider sourceProvider;

        /**
         * The binder where exposed bindings will be created
         * 看来binder对象本身是具有层级关系的
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
            // 默认情况下 source为null  选择初始化 sourceProvider
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

        /**
         * 代表被某个注解修饰的属性 在被ioc容器注入实例时的方式 (单例 or 原型模式)
         * @param annotationType
         * @param scope  该对象包含一个接口 在加工普通的provider后 使之具备单例模式的能力
         */
        @Override
        public void bindScope(Class<? extends Annotation> annotationType, Scope scope) {
            elements.add(new ScopeBinding(getSource(), annotationType, scope));
        }

        /**
         * 为传入的实例对象完成属性注入
         * @param instance for which members will be injected
         */
        @Override
        @SuppressWarnings("unchecked") // it is safe to use the type literal for the raw type
        public void requestInjection(Object instance) {
            // 获取实例对应的class的信息 之后为实例注入需要的属性
            requestInjection((TypeLiteral) TypeLiteral.get(instance.getClass()), instance);
        }

        /**
         * 为实例注入需要的属性
         * @param type     of instance
         * @param instance for which members will be injected
         * @param <T>
         */
        @Override
        public <T> void requestInjection(TypeLiteral<T> type, T instance) {
            // 发现每个请求都被包装成了 elements对象 并维护在elements中 推测会有一个对象在之后按照这个elements链处理 最终返回注入后的对象
            elements.add(new InjectionRequest<>(getSource(), type, instance));
        }

        /**
         * 为指定的类型返回一个 成员注入器   requestInjection 代表一种被动的注入 将实例交由binder处理 而该方法则是使用者主动获取注入器 注入时机由使用者决定
         * @param typeLiteral type to get members injector for
         * @param <T>
         * @return
         */
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

        /**
         * 为匹配matches的对象设置监听器
         * @param typeMatcher that matches injectable types the listener should be notified of
         * @param listener    for injectable types matched by typeMatcher
         */
        @Override
        public void bindListener(Matcher<? super TypeLiteral<?>> typeMatcher, TypeListener listener) {
            elements.add(new TypeListenerBinding(getSource(), listener, typeMatcher));
        }

        /**
         * 以class 为单位 就是注入静态属性
         * @param types for which static members will be injected
         */
        @Override
        public void requestStaticInjection(Class<?>... types) {
            for (Class<?> type : types) {
                elements.add(new StaticInjectionRequest(getSource(), type));
            }
        }

        /**
         * 实际上就是使用module内部定义的configure 逻辑为本对象绑定更多的映射关系  因为用户在module中就是重写了各种绑定逻辑
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
                // 是这样子  如果在module对象中 写了包含@Provider的方法 它会被抽取出来 作为某种接口的提供类
                binder.install(ProviderMethodsModule.forModule(module));
            }
        }

        @Override
        public Stage currentStage() {
            return stage;
        }

        /**
         * 异常信息被包装后也会作为element 并设置到elements中
         * @param message
         * @param arguments
         */
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
         * 当绑定一个key时 返回一个基于注解的 bindingBuilder对象 可以通过to() 方法指定注入的实现类
         * @param key
         * @param <T>
         * @return
         */
        @Override
        public <T> AnnotatedBindingBuilder<T> bind(Key<T> key) {
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
