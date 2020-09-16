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

import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Scope;

import java.lang.annotation.Annotation;
import java.util.Objects;

/**
 * Registration of a scope annotation with the scope that implements it. Instances are created
 * explicitly in a module using {@link org.elasticsearch.common.inject.Binder#bindScope(Class, Scope) bindScope()}
 * statements:
 * <pre>
 *     Scope recordScope = new RecordScope();
 *     bindScope(RecordScoped.class, new RecordScope());</pre>
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 * 描述当遇到某个注解时  ioc容器注入的对象采用指定的scope (单例or原型)
 */
public final class ScopeBinding implements Element {

    /**
     * 调用 Builder.bindScope() 的对象
     */
    private final Object source;
    /**
     * 被标记的注解
     */
    private final Class<? extends Annotation> annotationType;
    /**
     * 使用的范围
     */
    private final Scope scope;

    ScopeBinding(Object source, Class<? extends Annotation> annotationType, Scope scope) {
        this.source = Objects.requireNonNull(source, "source");
        this.annotationType = Objects.requireNonNull(annotationType, "annotationType");
        this.scope = Objects.requireNonNull(scope, "scope");
    }

    @Override
    public Object getSource() {
        return source;
    }

    public Class<? extends Annotation> getAnnotationType() {
        return annotationType;
    }

    public Scope getScope() {
        return scope;
    }

    @Override
    public <T> T acceptVisitor(ElementVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     * 将该对象的 范围信息应用到另一个binder对象
     * @param binder to apply configuration element to
     */
    @Override
    public void applyTo(Binder binder) {
        binder.withSource(getSource()).bindScope(annotationType, scope);
    }
}
