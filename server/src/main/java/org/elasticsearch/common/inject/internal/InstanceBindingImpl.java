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
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.spi.BindingTargetVisitor;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.inject.spi.HasDependencies;
import org.elasticsearch.common.inject.spi.InjectionPoint;
import org.elasticsearch.common.inject.spi.InstanceBinding;
import org.elasticsearch.common.inject.util.Providers;

import java.util.Set;

/**
 * 代表这组绑定关系是基于实例的 也就是无关上游信息 下游总是返回一个实例
 * @param <T>
 */
public class InstanceBindingImpl<T> extends BindingImpl<T> implements InstanceBinding<T> {

    /**
     * 返回的实例对象信息
     */
    final T instance;
    /**
     * 返回该实例的提供者
     */
    final Provider<T> provider;
    final Set<InjectionPoint> injectionPoints;

    /**
     *
     * @param injector
     * @param key
     * @param source
     * @param internalFactory
     * @param injectionPoints
     * @param instance
     */
    public InstanceBindingImpl(Injector injector, Key<T> key, Object source,
                               InternalFactory<? extends T> internalFactory, Set<InjectionPoint> injectionPoints,
                               T instance) {
        super(injector, key, source, internalFactory, Scoping.UNSCOPED);
        this.injectionPoints = injectionPoints;
        this.instance = instance;
        this.provider = Providers.of(instance);
    }

    /**
     *
     * @param source  栈轨迹信息
     * @param key   绑定关系上游的key
     * @param scoping
     * @param injectionPoints  代表实例中需要的依赖信息对应的注入点
     * @param instance    针对该绑定关系的key 始终使用该实例对象进行注入
     */
    public InstanceBindingImpl(Object source, Key<T> key, Scoping scoping,
                               Set<InjectionPoint> injectionPoints, T instance) {
        super(source, key, scoping);
        this.injectionPoints = injectionPoints;
        this.instance = instance;
        this.provider = Providers.of(instance);
    }

    @Override
    public Provider<T> getProvider() {
        return this.provider;
    }

    @Override
    public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public T getInstance() {
        return instance;
    }

    @Override
    public Set<InjectionPoint> getInjectionPoints() {
        return injectionPoints;
    }

    @Override
    public Set<Dependency<?>> getDependencies() {
        return instance instanceof HasDependencies
                ? Set.copyOf(((HasDependencies) instance).getDependencies())
                : Dependency.forInjectionPoints(injectionPoints);
    }

    @Override
    public BindingImpl<T> withScoping(Scoping scoping) {
        return new InstanceBindingImpl<>(getSource(), getKey(), scoping, injectionPoints, instance);
    }

    @Override
    public BindingImpl<T> withKey(Key<T> key) {
        return new InstanceBindingImpl<>(getSource(), key, getScoping(), injectionPoints, instance);
    }

    /**
     * 这个是绑定实例 而 ConstantBindingBuilderImpl 强制指定绑定的是一个常量
     * @param binder to apply configuration element to
     */
    @Override
    public void applyTo(Binder binder) {
        // instance bindings aren't scoped
        binder.withSource(getSource()).bind(getKey()).toInstance(instance);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(InstanceBinding.class)
                .add("key", getKey())
                .add("source", getSource())
                .add("instance", instance)
                .toString();
    }
}
