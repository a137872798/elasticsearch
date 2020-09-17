/*
 * Copyright (C) 2007 Google Inc.
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
import org.elasticsearch.common.inject.spi.BindingTargetVisitor;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.inject.spi.UntargettedBinding;

/**
 * 代表一个未完成的binding对象
 * @param <T>
 */
public class UntargettedBindingImpl<T> extends BindingImpl<T> implements UntargettedBinding<T> {

    /**
     * @param injector
     * @param key
     * @param source
     */
    public UntargettedBindingImpl(Injector injector, Key<T> key, Object source) {
        // 该对象传入了一个无法调用get()的工厂
        super(injector, key, source, new InternalFactory<T>() {
            @Override
            public T get(Errors errors, InternalContext context, Dependency<?> dependency) {
                throw new AssertionError();
            }
        }, Scoping.UNSCOPED);
    }

    /**
     * 某个binder为某个key 建立绑定关系对象时 就会生成一个 BindingBuilder (Binding 就代表一个完整的绑定链) 此时还没有指定注入器 injector
     * @param source
     * @param key
     * @param scoping
     */
    public UntargettedBindingImpl(Object source, Key<T> key, Scoping scoping) {
        super(source, key, scoping);
    }

    // 这里也仅仅是定义一个模板

    @Override
    public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public BindingImpl<T> withScoping(Scoping scoping) {
        return new UntargettedBindingImpl<>(getSource(), getKey(), scoping);
    }

    /**
     * 返回一个新的实例 并更新key
     * @param key
     * @return
     */
    @Override
    public BindingImpl<T> withKey(Key<T> key) {
        return new UntargettedBindingImpl<>(getSource(), key, getScoping());
    }

    @Override
    public void applyTo(Binder binder) {
        getScoping().applyTo(binder.withSource(getSource()).bind(getKey()));
    }

    @Override
    public String toString() {
        return new ToStringBuilder(UntargettedBinding.class)
                .add("key", getKey())
                .add("source", getSource())
                .toString();
    }
}
