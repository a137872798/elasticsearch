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
 * bindingBuilder的骨架类
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
     * 该列表是由某个  binder传递过来的
     * binding在设置完毕后 会追加到列表中
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
        // 在builder刚创建时 可以理解为 在elements中设置了一个占位符  在之后设置完builder 会更改binding 并替换elements中的值
        this.binding = new UntargettedBindingImpl<>(source, key, Scoping.UNSCOPED);
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
     * 更新了binding对象 代表本次绑定关系还限定必须携带XXX注解
     */
    protected BindingImpl<T> annotatedWithInternal(Class<? extends Annotation> annotationType) {
        Objects.requireNonNull(annotationType, "annotationType");
        checkNotAnnotated();
        // 将binding内部的key更改后 重新设置
        return setBinding(binding.withKey(
            // 包装成一个新的key
                Key.get(this.binding.getKey().getTypeLiteral(), annotationType)));
    }

    /**
     * Sets the binding to a copy with the specified annotation on the bound key
     * 在binding原有的基础上 追加了必须满足某个注解的条件
     */
    protected BindingImpl<T> annotatedWithInternal(Annotation annotation) {
        Objects.requireNonNull(annotation, "annotation");
        checkNotAnnotated();
        return setBinding(binding.withKey(
                Key.get(this.binding.getKey().getTypeLiteral(), annotation)));
    }

    /**
     * 使用描述范围的注解 比如 @Singleton  代表注入的为单例 
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
