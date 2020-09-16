/*
 * Copyright (C) 2006 Google Inc.
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

import org.elasticsearch.common.inject.binder.AnnotatedBindingBuilder;
import org.elasticsearch.common.inject.binder.AnnotatedConstantBindingBuilder;
import org.elasticsearch.common.inject.binder.LinkedBindingBuilder;
import org.elasticsearch.common.inject.matcher.Matcher;
import org.elasticsearch.common.inject.spi.Message;
import org.elasticsearch.common.inject.spi.TypeConverter;
import org.elasticsearch.common.inject.spi.TypeListener;

import java.lang.annotation.Annotation;
import java.util.Objects;

/**
 * A support class for {@link Module}s which reduces repetition and results in
 * a more readable configuration. Simply extend this class, implement {@link
 * #configure()}, and call the inherited methods which mirror those found in
 * {@link Binder}. For example:
 * <pre>
 * public class MyModule extends AbstractModule {
 *   protected void configure() {
 *     bind(Service.class).to(ServiceImpl.class).in(Singleton.class);
 *     bind(CreditCardPaymentService.class);
 *     bind(PaymentService.class).to(CreditCardPaymentService.class);
 *     bindConstant().annotatedWith(Names.named("port")).to(8080);
 *   }
 * }
 * </pre>
 *
 * @author crazybob@google.com (Bob Lee)
 * 绑定逻辑模板的 骨架类
 *
 * 可以看出这个类的套路是 在调用configure时 在内部设置binder对象 之后用户在重写configure时 可以调用各种bind方法 其核心都是委托给binder对象
 * 之后binder对象被置空  全流程结束
 */
public abstract class AbstractModule implements Module {

    /**
     * 该对象维护所有的绑定关系
     */
    Binder binder;

    /**
     * 通过外部设置绑定对象 ， 绑定关系以绑定对象为单位进行隔离
     * @param builder
     */
    @Override
    public final synchronized void configure(Binder builder) {
        if (this.binder != null) {
            throw new IllegalStateException("Re-entry is not allowed.");
        }
        this.binder = Objects.requireNonNull(builder, "builder");
        try {
            // 一旦设置了 binder对象后 就会触发 configure方法
            configure();
        } finally {
            // 可以看到 一旦绑定关系通过binder对象处理后 该属性就被滞空了  推测是在某个地方将一个 binder对象交由所有的module 进行处理
            // 这样会从每个module上采集信息
            this.binder = null;
        }
    }

    /**
     * Configures a {@link Binder} via the exposed methods.
     * 用户通过重写该方法来完成 绑定关系的建立
     */
    protected abstract void configure();

    /**
     * Gets direct access to the underlying {@code Binder}.
     */
    protected Binder binder() {
        return binder;
    }

    /**
     * @see Binder#bindScope(Class, Scope)
     * 范围主要是用来描述 ioc容器注入对象时采用单例模式 还是原型模式   这里应该是代表标注了某个注解的 类 会以指定的scope进行注入
     */
    protected void bindScope(Class<? extends Annotation> scopeAnnotation,
                             Scope scope) {
        binder.bindScope(scopeAnnotation, scope);
    }

    // 从以下几个api来看 可以指定绑定在key/TypeLiteral/Class 上    在调用完bind后一般要调用to 来指定实例

    /**
     * @see Binder#bind(Key)
     */
    protected <T> LinkedBindingBuilder<T> bind(Key<T> key) {
        return binder.bind(key);
    }

    /**
     * @see Binder#bind(TypeLiteral)
     */
    protected <T> AnnotatedBindingBuilder<T> bind(TypeLiteral<T> typeLiteral) {
        return binder.bind(typeLiteral);
    }

    /**
     * @see Binder#bind(Class)
     */
    protected <T> AnnotatedBindingBuilder<T> bind(Class<T> clazz) {
        return binder.bind(clazz);
    }

    /**
     * @see Binder#bindConstant()
     * 代表申请一个常量绑定的builder 之后 通过比如withAnnonation + to 方法 将携带某一注解的属性始终注入某个常量值
     */
    protected AnnotatedConstantBindingBuilder bindConstant() {
        return binder.bindConstant();
    }

    /**
     * @see Binder#install(Module)
     */
    protected void install(Module module) {
        binder.install(module);
    }

    /**
     * @see Binder#addError(String, Object[])
     */
    protected void addError(String message, Object... arguments) {
        binder.addError(message, arguments);
    }

    /**
     * @see Binder#addError(Throwable)
     */
    protected void addError(Throwable t) {
        binder.addError(t);
    }

    /**
     * @see Binder#addError(Message)
     * @since 2.0
     */
    protected void addError(Message message) {
        binder.addError(message);
    }

    /**
     * @see Binder#requestInjection(Object)
     * @since 2.0
     */
    protected void requestInjection(Object instance) {
        binder.requestInjection(instance);
    }

    /**
     * @see Binder#requestStaticInjection(Class[])
     */
    protected void requestStaticInjection(Class<?>... types) {
        binder.requestStaticInjection(types);
    }

    /**
     * Adds a dependency from this module to {@code key}. When the injector is
     * created, Guice will report an error if {@code key} cannot be injected.
     * Note that this requirement may be satisfied by implicit binding, such as
     * a public no-arguments constructor.
     *
     * @since 2.0
     */
    protected void requireBinding(Key<?> key) {
        binder.getProvider(key);
    }

    /**
     * Adds a dependency from this module to {@code type}. When the injector is
     * created, Guice will report an error if {@code type} cannot be injected.
     * Note that this requirement may be satisfied by implicit binding, such as
     * a public no-arguments constructor.
     *
     * @since 2.0
     */
    protected void requireBinding(Class<?> type) {
        binder.getProvider(type);
    }

    /**
     * @see Binder#getProvider(Key)
     * @since 2.0
     */
    protected <T> Provider<T> getProvider(Key<T> key) {
        return binder.getProvider(key);
    }

    /**
     * @see Binder#getProvider(Class)
     * @since 2.0
     */
    protected <T> Provider<T> getProvider(Class<T> type) {
        return binder.getProvider(type);
    }

    /**
     * @see Binder#convertToTypes
     * @since 2.0
     */
    protected void convertToTypes(Matcher<? super TypeLiteral<?>> typeMatcher,
                                  TypeConverter converter) {
        binder.convertToTypes(typeMatcher, converter);
    }

    /**
     * @see Binder#currentStage()
     * @since 2.0
     */
    protected Stage currentStage() {
        return binder.currentStage();
    }

    /**
     * @see Binder#getMembersInjector(Class)
     * @since 2.0
     */
    protected <T> MembersInjector<T> getMembersInjector(Class<T> type) {
        return binder.getMembersInjector(type);
    }

    /**
     * @see Binder#getMembersInjector(TypeLiteral)
     * @since 2.0
     */
    protected <T> MembersInjector<T> getMembersInjector(TypeLiteral<T> type) {
        return binder.getMembersInjector(type);
    }

    /**
     * @see Binder#bindListener(org.elasticsearch.common.inject.matcher.Matcher,
     *      org.elasticsearch.common.inject.spi.TypeListener)
     * @since 2.0
     */
    protected void bindListener(Matcher<? super TypeLiteral<?>> typeMatcher,
                                TypeListener listener) {
        binder.bindListener(typeMatcher, listener);
    }
}
