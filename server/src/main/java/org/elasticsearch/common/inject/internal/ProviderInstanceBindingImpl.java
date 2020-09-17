/*
Copyright (C) 2007 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
import org.elasticsearch.common.inject.spi.ProviderInstanceBinding;

import java.util.Set;

/**
 * BindingBuilder 通过toProvider() 方法返回 ProviderInstanceBindingImpl
 * @param <T>
 */
public final class ProviderInstanceBindingImpl<T> extends BindingImpl<T>
        implements ProviderInstanceBinding<T> {

    final Provider<? extends T> providerInstance;
    final Set<InjectionPoint> injectionPoints;

    /**
     *
     * @param injector
     * @param key
     * @param source
     * @param internalFactory
     * @param scoping
     * @param providerInstance
     * @param injectionPoints
     */
    public ProviderInstanceBindingImpl(Injector injector, Key<T> key,
                                       Object source, InternalFactory<? extends T> internalFactory, Scoping scoping,
                                       Provider<? extends T> providerInstance,
                                       Set<InjectionPoint> injectionPoints) {
        super(injector, key, source, internalFactory, scoping);
        this.providerInstance = providerInstance;
        this.injectionPoints = injectionPoints;
    }


    /**
     *
     * @param source
     * @param key  原来要处理的类型
     * @param scoping  scoping 与scopes 的区别是 后者更加明确的指定了范围  而前者暴露了一个 visit api 便于使用visitor处理
     * @param injectionPoints   该provider类的注入点
     * @param providerInstance     Provider实现类  在绑定关系上游的key 会通过该对象获取注入的实例
     */
    public ProviderInstanceBindingImpl(Object source, Key<T> key, Scoping scoping,
                                       Set<InjectionPoint> injectionPoints, Provider<? extends T> providerInstance) {
        super(source, key, scoping);
        this.injectionPoints = injectionPoints;
        this.providerInstance = providerInstance;
    }

    @Override
    public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Provider<? extends T> getProviderInstance() {
        return providerInstance;
    }

    @Override
    public Set<InjectionPoint> getInjectionPoints() {
        return injectionPoints;
    }

    @Override
    public Set<Dependency<?>> getDependencies() {
        return providerInstance instanceof HasDependencies
                ? Set.copyOf(((HasDependencies) providerInstance).getDependencies())
                : Dependency.forInjectionPoints(injectionPoints);
    }

    @Override
    public BindingImpl<T> withScoping(Scoping scoping) {
        return new ProviderInstanceBindingImpl<>(
                getSource(), getKey(), scoping, injectionPoints, providerInstance);
    }

    @Override
    public BindingImpl<T> withKey(Key<T> key) {
        return new ProviderInstanceBindingImpl<>(
                getSource(), key, getScoping(), injectionPoints, providerInstance);
    }

    @Override
    public void applyTo(Binder binder) {
        getScoping().applyTo(
                binder.withSource(getSource()).bind(getKey()).toProvider(getProviderInstance()));
    }

    @Override
    public String toString() {
        return new ToStringBuilder(ProviderInstanceBinding.class)
                .add("key", getKey())
                .add("source", getSource())
                .add("scope", getScoping())
                .add("provider", providerInstance)
                .toString();
    }
}
