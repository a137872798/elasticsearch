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
import org.elasticsearch.common.inject.ConfigurationException;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.binder.AnnotatedBindingBuilder;
import org.elasticsearch.common.inject.spi.Element;
import org.elasticsearch.common.inject.spi.InjectionPoint;
import org.elasticsearch.common.inject.spi.Message;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;

/**
 * Bind a non-constant key.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * 代表将某个组件绑定到某个东西上
 */
public class BindingBuilder<T> extends AbstractBindingBuilder<T>
        implements AnnotatedBindingBuilder<T> {

    /**
     *
     * @param binder   该对象是由哪个builder创建的
     * @param elements  当生成有效信息时 会回填到这个list中
     * @param source  默认情况下就是 栈轨迹信息
     * @param key   代表绑定的是哪个类 以及使用的注解策略
     */
    public BindingBuilder(Binder binder, List<Element> elements, Object source, Key<T> key) {
        super(binder, elements, source, key);
    }

    @Override
    public BindingBuilder<T> annotatedWith(Class<? extends Annotation> annotationType) {
        annotatedWithInternal(annotationType);
        return this;
    }

    @Override
    public BindingBuilder<T> annotatedWith(Annotation annotation) {
        annotatedWithInternal(annotation);
        return this;
    }

    @Override
    public BindingBuilder<T> to(Class<? extends T> implementation) {
        return to(Key.get(implementation));
    }

    @Override
    public BindingBuilder<T> to(TypeLiteral<? extends T> implementation) {
        return to(Key.get(implementation));
    }

    @Override
    public BindingBuilder<T> to(Key<? extends T> linkedKey) {
        Objects.requireNonNull(linkedKey, "linkedKey");
        checkNotTargetted();
        BindingImpl<T> base = getBinding();
        setBinding(new LinkedBindingImpl<>(
                base.getSource(), base.getKey(), base.getScoping(), linkedKey));
        return this;
    }

    /**
     * 找到实例上的增强点 并进行增强
     * @param instance
     */
    @Override
    public void toInstance(T instance) {
        // 该对象要求 binding必须是UntargettedBindingImpl 类型
        checkNotTargetted();

        // lookup the injection points, adding any errors to the binder's errors list
        Set<InjectionPoint> injectionPoints;
        if (instance != null) {
            try {
                // 找到实例上所有的增强点   也就是携带@Inject的方法  或者属性
                injectionPoints = InjectionPoint.forInstanceMethodsAndFields(instance.getClass());
            } catch (ConfigurationException e) {
                for (Message message : e.getErrorMessages()) {
                    binder.addError(message);
                }
                injectionPoints = Set.copyOf(e.getPartialValue());
            }
        } else {
            binder.addError(BINDING_TO_NULL);
            injectionPoints = emptySet();
        }

        // 以 UntargettedBindingImpl 为基础 创建实例binding对象
        BindingImpl<T> base = getBinding();
        // 默认情况下 base.getSource() 就是栈轨迹信息   当转换成InstanceBindingImpl后 重新设置到elements中
        setBinding(new InstanceBindingImpl<>(
                base.getSource(), base.getKey(), base.getScoping(), injectionPoints, instance));
    }

    @Override
    public BindingBuilder<T> toProvider(Provider<? extends T> provider) {
        Objects.requireNonNull(provider, "provider");
        checkNotTargetted();

        // lookup the injection points, adding any errors to the binder's errors list
        Set<InjectionPoint> injectionPoints;
        try {
            injectionPoints = InjectionPoint.forInstanceMethodsAndFields(provider.getClass());
        } catch (ConfigurationException e) {
            for (Message message : e.getErrorMessages()) {
                binder.addError(message);
            }
            injectionPoints = Set.copyOf(e.getPartialValue());
        }

        BindingImpl<T> base = getBinding();
        setBinding(new ProviderInstanceBindingImpl<>(
                base.getSource(), base.getKey(), base.getScoping(), injectionPoints, provider));
        return this;
    }

    @Override
    public BindingBuilder<T> toProvider(Class<? extends Provider<? extends T>> providerType) {
        return toProvider(Key.get(providerType));
    }

    @Override
    public BindingBuilder<T> toProvider(Key<? extends Provider<? extends T>> providerKey) {
        Objects.requireNonNull(providerKey, "providerKey");
        checkNotTargetted();

        BindingImpl<T> base = getBinding();
        setBinding(new LinkedProviderBindingImpl<>(
                base.getSource(), base.getKey(), base.getScoping(), providerKey));
        return this;
    }

    @Override
    public String toString() {
        return "BindingBuilder<" + getBinding().getKey().getTypeLiteral() + ">";
    }
}
