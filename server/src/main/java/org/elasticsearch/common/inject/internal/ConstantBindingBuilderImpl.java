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
import org.elasticsearch.common.inject.binder.AnnotatedConstantBindingBuilder;
import org.elasticsearch.common.inject.binder.ConstantBindingBuilder;
import org.elasticsearch.common.inject.spi.Element;

import java.lang.annotation.Annotation;
import java.util.List;

import static java.util.Collections.emptySet;

/**
 * Bind a constant.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * 代表上层绑定的是常量
 */
public final class ConstantBindingBuilderImpl<T>
    // AbstractBindingBuilder 在创建时 会对应一个未完成的 Binding对象 在通过设置injector后 可以生成对应的provider 这样就是一个完整的绑定关系了
        extends AbstractBindingBuilder<T>
    // 2个接口分别代表 可以限定携带某些注解的接口绑定到常量上  以及通过 to() 方法将常量绑定在某个位置
        implements AnnotatedConstantBindingBuilder, ConstantBindingBuilder {

    /**
     * constant bindings start out with T unknown
     * 可以看到在生成 常量 BindingBuilder对象时 key 默认就会使用 Void类型的  在调用to时 会根据常量的值自动匹配类型 应该是避免用户的非法操作
     * @param binder
     * @param elements
     * @param source
     */
    @SuppressWarnings("unchecked")
    public ConstantBindingBuilderImpl(Binder binder, List<Element> elements, Object source) {
        super(binder, elements, source, (Key<T>) NULL_KEY);
    }

    @Override
    public ConstantBindingBuilder annotatedWith(Class<? extends Annotation> annotationType) {
        annotatedWithInternal(annotationType);
        return this;
    }

    @Override
    public ConstantBindingBuilder annotatedWith(Annotation annotation) {
        annotatedWithInternal(annotation);
        return this;
    }

    // 针对常量对象 会自动匹配类型 不需要提前设置 同时如果设置了的话 不是Void 会抛出异常

    @Override
    public void to(final String value) {
        toConstant(String.class, value);
    }

    @Override
    public void to(final int value) {
        toConstant(Integer.class, value);
    }

    @Override
    public void to(final long value) {
        toConstant(Long.class, value);
    }

    @Override
    public void to(final boolean value) {
        toConstant(Boolean.class, value);
    }

    @Override
    public void to(final double value) {
        toConstant(Double.class, value);
    }

    @Override
    public void to(final float value) {
        toConstant(Float.class, value);
    }

    @Override
    public void to(final short value) {
        toConstant(Short.class, value);
    }

    @Override
    public void to(final char value) {
        toConstant(Character.class, value);
    }

    @Override
    public void to(final Class<?> value) {
        toConstant(Class.class, value);
    }

    @Override
    public <E extends Enum<E>> void to(final E value) {
        toConstant(value.getDeclaringClass(), value);
    }

    /**
     * 绑定在某个常量上
     * @param type
     * @param instance
     */
    private void toConstant(Class<?> type, Object instance) {
        // this type will define T, so these assignments are safe
        @SuppressWarnings("unchecked")
        Class<T> typeAsClassT = (Class<T>) type;
        @SuppressWarnings("unchecked")
        T instanceAsT = (T) instance;

        // 代表key 已经设置了有效的类型 (不是Void)
        if (keyTypeIsSet()) {
            binder.addError(CONSTANT_VALUE_ALREADY_SET);
            return;
        }

        // 获取内部的 bindingImpl 对象
        BindingImpl<T> base = getBinding();
        // 将目标类型包装成新的key
        Key<T> key;
        if (base.getKey().getAnnotation() != null) {
            key = Key.get(typeAsClassT, base.getKey().getAnnotation());
        } else if (base.getKey().getAnnotationType() != null) {
            key = Key.get(typeAsClassT, base.getKey().getAnnotationType());
        } else {
            key = Key.get(typeAsClassT);
        }

        // 不允许绑定无效的值
        if (instanceAsT == null) {
            binder.addError(BINDING_TO_NULL);
        }

        // 因为绑定关系已经建立 所以可以生成一个有效的 InstanceBindingImpl 对象了  并替换之前的 UntargettedBindingImpl
        setBinding(new InstanceBindingImpl<>(
                base.getSource(), key, base.getScoping(), emptySet(), instanceAsT));
    }

    @Override
    public String toString() {
        return "ConstantBindingBuilder";
    }
}
