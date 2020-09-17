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
import org.elasticsearch.common.inject.Exposed;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.PrivateBinder;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.inject.spi.ProviderWithDependencies;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

/**
 * A provider that invokes a method and returns its result.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * ProviderWithDependencies 是一个复合接口 代表同时具备Provider 和 HasDependencies的功能
 * ProviderMethod 代表一个 携带@Provider注解的方法 同时它必须写入在 Module的子类下才有用
 */
public class ProviderMethod<T> implements ProviderWithDependencies<T> {
    private final Key<T> key;
    private final Class<? extends Annotation> scopeAnnotation;
    private final Object instance;
    private final Method method;
    private final Set<Dependency<?>> dependencies;
    private final List<Provider<?>> parameterProviders;
    /**
     * 该方法上是否包含 @Exposed 注解
     */
    private final boolean exposed;

    /**
     * @param key 加工returnType 与 包含@BindingAnnotation 的注解生成的key
     * @param method the method to invoke. Its return type must be the same type as {@code key}.
     * @param instance 其实就是实例对象 (或者说module对象)
     * @param dependencies 该方法的参数信息抽取出来生成依赖信息
     * @param parameterProviders 代表以注入的方式提供参数
     * @param scopeAnnotation 标注在方法上用于描述范围信息的注解
     */
    ProviderMethod(Key<T> key, Method method, Object instance,
                   Set<Dependency<?>> dependencies, List<Provider<?>> parameterProviders,
                   Class<? extends Annotation> scopeAnnotation) {
        this.key = key;
        this.scopeAnnotation = scopeAnnotation;
        this.instance = instance;
        this.dependencies = dependencies;
        this.method = method;
        this.parameterProviders = parameterProviders;
        this.exposed = method.getAnnotation(Exposed.class) != null;
    }

    public Key<T> getKey() {
        return key;
    }

    public Method getMethod() {
        return method;
    }

    // exposed for GIN
    public Object getInstance() {
        return instance;
    }

    /**
     * 在 ProviderMethodsModule 中 寻找到module下携带@Provider 注解的方法后 会调用该方法 将绑定关系注册到binder中
     * @param binder
     */
    public void configure(Binder binder) {

        // 以该方法作为 source 生成新的绑定对象
        binder = binder.withSource(method);

        // 指定范围信息  此时key 对应返回值 也就代表着 当需要这个返回值类型的对象时 就可以通过调用该方法获取实例
        if (scopeAnnotation != null) {
            binder.bind(key).toProvider(this).in(scopeAnnotation);
        } else {
            binder.bind(key).toProvider(this);
        }

        // 代表需要暴露出来
        if (exposed) {
            // the cast is safe 'cause the only binder we have implements PrivateBinder. If there's a
            // misplaced @Exposed, calling this will add an error to the binder's error queue
            ((PrivateBinder) binder).expose(key);
        }
    }

    @Override
    public T get() {
        Object[] parameters = new Object[parameterProviders.size()];
        // 使用每个参数对应的provider 对象生成参数对象 并通过这些参数调用方法 并将结果返回
        // 当然如果使用单例Scopes进行包装 在首次生成实例对象后 就会不断的返回同一个实例 实现单例模式
        for (int i = 0; i < parameters.length; i++) {
            parameters[i] = parameterProviders.get(i).get();
        }

        try {
            // We know this cast is safe because T is the method's return type.
            @SuppressWarnings({"unchecked"})
            T result = (T) method.invoke(instance, parameters);
            return result;
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<Dependency<?>> getDependencies() {
        return dependencies;
    }
}
