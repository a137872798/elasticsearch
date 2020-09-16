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

package org.elasticsearch.common.inject.internal;

import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Provides;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.inject.spi.Message;
import org.elasticsearch.common.inject.util.Modules;

import java.lang.annotation.Annotation;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * Creates bindings to methods annotated with {@literal @}{@link Provides}. Use the scope and
 * binding annotations on the provider method to configure the binding.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 * 该对象是将用户定义的module的 携带@Provider注解的方法抽取出来 作为实例的提供者  同时将这种绑定关系设置到binder中
 */
public final class ProviderMethodsModule implements Module {

    /**
     * 原始的module对象 在被builder.install 处理后 会被包装成该对象 再处理一次
     */
    private final Object delegate;
    /**
     * module 类对应的 泛型解析器
     */
    private final TypeLiteral<?> typeLiteral;

    /**
     *
     * @param delegate   module对象
     */
    private ProviderMethodsModule(Object delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.typeLiteral = TypeLiteral.get(this.delegate.getClass());
    }

    /**
     * Returns a module which creates bindings for provider methods from the given module.
     */
    public static Module forModule(Module module) {
        return forObject(module);
    }

    /**
     * Returns a module which creates bindings for provider methods from the given object.
     * This is useful notably for <a href="http://code.google.com/p/google-gin/">GIN</a>
     */
    public static Module forObject(Object object) {
        // avoid infinite recursion, since installing a module always installs itself
        if (object instanceof ProviderMethodsModule) {
            return Modules.EMPTY_MODULE;
        }

        return new ProviderMethodsModule(object);
    }

    /**
     * 每个模块类被加工过后 会被包装成该对象 并再次触发一次configure
     * @param binder
     */
    @Override
    public synchronized void configure(Binder binder) {
        // 找到所有携带@Provider的方法
        for (ProviderMethod<?> providerMethod : getProviderMethods(binder)) {
            providerMethod.configure(binder);
        }
    }

    /**
     * 获取找到 delegate内部所有包含@Provider注解的方法 进行包装后返回
     * @param binder
     * @return
     */
    public List<ProviderMethod<?>> getProviderMethods(Binder binder) {
        List<ProviderMethod<?>> result = new ArrayList<>();
        for (Class<?> c = delegate.getClass(); c != Object.class; c = c.getSuperclass()) {
            for (Method method : c.getMethods()) {
                if (method.getAnnotation(Provides.class) != null) {
                    result.add(createProviderMethod(binder, method));
                }
            }
        }
        return result;
    }

    /**
     *
     * @param binder  负责存储绑定关系的对象
     * @param method   携带@Provider注解的方法
     * @param <T>
     * @return
     */
    <T> ProviderMethod<T> createProviderMethod(Binder binder, final Method method) {
        // 创建一个 source 为  method的 RecordingBuilder  同时该builder仅维护了该method的绑定关系
        binder = binder.withSource(method);
        // 生成描述错误信息的对象
        Errors errors = new Errors(method);

        // prepare the parameter providers
        Set<Dependency<?>> dependencies = new HashSet<>();
        // 每个携带@Provider注解的方法会被包装成 Provider
        List<Provider<?>> parameterProviders = new ArrayList<>();
        // 获取该方法所有参数 (包含泛型信息)
        List<TypeLiteral<?>> parameterTypes = typeLiteral.getParameterTypes(method);
        // 找到每个参数携带的注解
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        for (int i = 0; i < parameterTypes.size(); i++) {
            // 尝试解析参数上是否存在内置@BindingAnnotation的注解  并生成依赖对象    每个内置@BindAnnotation的注解对应的属性 代表需要通过ioc容器进行注入
            // 比如guice的 @Named注解 代表着需要从ioc容器中找到@Named对应名字的bean 并进行注入   当没有找到时 方法参数type会被解析成key
            Key<?> key = getKey(errors, parameterTypes.get(i), method, parameterAnnotations[i]);
            // 将key包装成依赖对象
            dependencies.add(Dependency.get(key));
            // 从binder中获取参数提供者对象  注意此时的binder source虽然改变 但是elements 和 modules是会传递过去的
            parameterProviders.add(binder.getProvider(key));
        }

        @SuppressWarnings("unchecked") // Define T as the method's return type.
                TypeLiteral<T> returnType = (TypeLiteral<T>) typeLiteral.getReturnType(method);

        Key<T> key = getKey(errors, returnType, method, method.getAnnotations());
        Class<? extends Annotation> scopeAnnotation
                = Annotations.findScopeAnnotation(errors, method.getAnnotations());

        for (Message message : errors.getMessages()) {
            binder.addError(message);
        }

        return new ProviderMethod<>(key, method, delegate, unmodifiableSet(dependencies),
                parameterProviders, scopeAnnotation);
    }

    /**
     * 找到包含 @BindingAnnotation的注解 并包装成key 后返回
     * @param errors
     * @param type
     * @param member
     * @param annotations
     * @param <T>
     * @return
     */
    <T> Key<T> getKey(Errors errors, TypeLiteral<T> type, Member member, Annotation[] annotations) {
        Annotation bindingAnnotation = Annotations.findBindingAnnotation(errors, member, annotations);
        return bindingAnnotation == null ? Key.get(type) : Key.get(type, bindingAnnotation);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ProviderMethodsModule
                && ((ProviderMethodsModule) o).delegate == delegate;
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}
