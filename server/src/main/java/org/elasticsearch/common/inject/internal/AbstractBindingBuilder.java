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
import org.elasticsearch.common.inject.Scope;
import org.elasticsearch.common.inject.spi.Element;
import org.elasticsearch.common.inject.spi.InstanceBinding;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Objects;

/**
 * Bind a value or constant.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * 将每个模块都包装成一个bindingBuilder
 */
public abstract class AbstractBindingBuilder<T> {

    public static final String IMPLEMENTATION_ALREADY_SET = "Implementation is set more than once.";
    public static final String SINGLE_INSTANCE_AND_SCOPE
            = "Setting the scope is not permitted when binding to a single instance.";
    public static final String SCOPE_ALREADY_SET = "Scope is set more than once.";
    public static final String BINDING_TO_NULL = "Binding to null instances is not allowed. "
            + "Use toProvider(Providers.of(null)) if this is your intended behaviour.";
    public static final String CONSTANT_VALUE_ALREADY_SET = "Constant value is set more than once.";
    public static final String ANNOTATION_ALREADY_SPECIFIED
            = "More than one annotation is specified for this binding.";

    protected static final Key<?> NULL_KEY = Key.get(Void.class);

    /**
     * 每个builder对象都会将 key 包装成一个 bindingImpl 并设置到 elements中
     */
    protected List<Element> elements;
    /**
     * 在数组的下标位置
     */
    protected int position;
    /**
     * 代表该对象是由哪个 binder生成的
     */
    protected final Binder binder;
    private BindingImpl<T> binding;

    /**
     *
     * @param binder  被绑定的对象
     * @param elements   所有绑定对象都会存储到这个列表中
     * @param source
     * @param key
     */
    public AbstractBindingBuilder(Binder binder, List<Element> elements, Object source, Key<T> key) {
        this.binder = binder;
        this.elements = elements;
        // 因为在集中处理binder对象时  每当处理一个对象 就会被加入到 elements中 size会随着bind的调用  不断地增大 这样刚好就对应了下标值
        this.position = elements.size();
        // 默认情况下 没有绑定范围  创建的是一个 Untarget对象
        this.binding = new UntargettedBindingImpl<>(source, key, Scoping.UNSCOPED);
        // 将对象追加到elements 中
        elements.add(position, this.binding);
    }

    protected BindingImpl<T> getBinding() {
        return binding;
    }

    /**
     * 更新内部的binding对象
     * @param binding
     * @return
     */
    protected BindingImpl<T> setBinding(BindingImpl<T> binding) {
        this.binding = binding;
        elements.set(position, binding);
        return binding;
    }

    /**
     * Sets the binding to a copy with the specified annotation on the bound key
     * 更新了binding对象 仅将满足某个注解的地方作为增强点
     */
    protected BindingImpl<T> annotatedWithInternal(Class<? extends Annotation> annotationType) {
        Objects.requireNonNull(annotationType, "annotationType");
        checkNotAnnotated();
        return setBinding(binding.withKey(
                Key.get(this.binding.getKey().getTypeLiteral(), annotationType)));
    }

    /**
     * Sets the binding to a copy with the specified annotation on the bound key
     */
    protected BindingImpl<T> annotatedWithInternal(Annotation annotation) {
        Objects.requireNonNull(annotation, "annotation");
        checkNotAnnotated();
        return setBinding(binding.withKey(
                Key.get(this.binding.getKey().getTypeLiteral(), annotation)));
    }

    /**
     * 将注解转换成scope信息  并设置到binding上
     * @param scopeAnnotation
     */
    public void in(final Class<? extends Annotation> scopeAnnotation) {
        Objects.requireNonNull(scopeAnnotation, "scopeAnnotation");
        checkNotScoped();
        setBinding(getBinding().withScoping(Scoping.forAnnotation(scopeAnnotation)));
    }

    public void in(final Scope scope) {
        Objects.requireNonNull(scope, "scope");
        checkNotScoped();
        setBinding(getBinding().withScoping(Scoping.forInstance(scope)));
    }

    public void asEagerSingleton() {
        checkNotScoped();
        setBinding(getBinding().withScoping(Scoping.EAGER_SINGLETON));
    }

    protected boolean keyTypeIsSet() {
        return !Void.class.equals(binding.getKey().getTypeLiteral().getType());
    }

    protected void checkNotTargetted() {
        if (!(binding instanceof UntargettedBindingImpl)) {
            binder.addError(IMPLEMENTATION_ALREADY_SET);
        }
    }

    protected void checkNotAnnotated() {
        if (binding.getKey().getAnnotationType() != null) {
            binder.addError(ANNOTATION_ALREADY_SPECIFIED);
        }
    }

    protected void checkNotScoped() {
        // Scoping isn't allowed when we have only one instance.
        if (binding instanceof InstanceBinding) {
            binder.addError(SINGLE_INSTANCE_AND_SCOPE);
            return;
        }

        if (binding.getScoping().isExplicitlyScoped()) {
            binder.addError(SCOPE_ALREADY_SET);
        }
    }
}
