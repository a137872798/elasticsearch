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

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.BindingImpl;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.internal.InternalFactory;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.inject.spi.PrivateElements;

/**
 * This factory exists in a parent injector. When invoked, it retrieves its value from a child
 * injector.
 * expose 代表要求子binder必须存在哪些binding
 */
class ExposedKeyFactory<T> implements InternalFactory<T>, BindingProcessor.CreationListener {
    private final Key<T> key;
    private final PrivateElements privateElements;
    private BindingImpl<T> delegate;

    /**
     *
     * @param key  某个需要暴露的key
     * @param privateElements  对应的私有elements
     */
    ExposedKeyFactory(Key<T> key, PrivateElements privateElements) {
        this.key = key;
        this.privateElements = privateElements;
    }

    /**
     * 当所有binding都设置到ioc容器后 在为bean注入属性前会触发该方法
     *  privateElements 本来是在父级调用 newPrivateElement生成的 相当于定义在父级  而在子级通过调用expose 指定必须存在哪些key的binding 否则会抛出异常
     * @param errors
     */
    @Override
    public void notify(Errors errors) {
        // 获取该对象对应的 注入器 每个注入器有自己的 state信息
        InjectorImpl privateInjector = (InjectorImpl) privateElements.getInjector();
        // 从当前state开始 查找 (当没有找到binding时 沿着链路往父类查找)
        BindingImpl<T> explicitBinding = privateInjector.state.getExplicitBinding(key);

        // validate that the child injector has its own factory. If the getInternalFactory() returns
        // this, then that child injector doesn't have a factory (and getExplicitBinding has returned
        // its parent's binding instead
        // 在创建该对象后 会将一个特殊的binding先设置到state中 如果在取出时 获取到的还是同一个binding 代表子类没有覆盖新的binding 所以抛出异常
        if (explicitBinding.getInternalFactory() == this) {
            errors.withSource(explicitBinding.getSource()).exposedButNotBound(key);
            return;
        }

        this.delegate = explicitBinding;
    }

    @Override
    public T get(Errors errors, InternalContext context, Dependency<?> dependency)
            throws ErrorsException {
        return delegate.getInternalFactory().get(errors, context, dependency);
    }
}
