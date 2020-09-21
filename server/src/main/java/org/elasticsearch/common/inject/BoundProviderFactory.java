/*
 * Copyright (C) 2006 Google Inc.
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

import org.elasticsearch.common.inject.BindingProcessor.CreationListener;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.internal.InternalFactory;
import org.elasticsearch.common.inject.spi.Dependency;

/**
 * Delegates to a custom factory which is also bound in the injector.
 * 该工厂在 处理 ProviderKeyBinding 类型时会使用到
 */
class BoundProviderFactory<T> implements InternalFactory<T>, CreationListener {

    private final InjectorImpl injector;
    final Key<? extends Provider<? extends T>> providerKey;
    final Object source;
    private InternalFactory<? extends Provider<? extends T>> providerFactory;

    /**
     *
     * @param injector  此时对应的注入器
     * @param providerKey  ProviderKeyBinding 的下游对象是一个 providerKey
     * @param source
     */
    BoundProviderFactory(
            InjectorImpl injector,
            Key<? extends Provider<? extends T>> providerKey,
            Object source) {
        this.injector = injector;
        this.providerKey = providerKey;
        this.source = source;
    }

    /**
     * 当初始化providerKey 对应的bean时 从ioc容器中生成对应的工厂对象
     * @param errors
     */
    @Override
    public void notify(Errors errors) {
        try {
            providerFactory = injector.getInternalFactory(providerKey, errors.withSource(source));
        } catch (ErrorsException e) {
            errors.merge(e.getErrors());
        }
    }

    /**
     * 从这里可以看出只是简单的调用了 provider.get()
     * @param errors
     * @param context of this injection
     * @param dependency
     * @return
     * @throws ErrorsException
     */
    @Override
    public T get(Errors errors, InternalContext context, Dependency<?> dependency)
            throws ErrorsException {
        errors = errors.withSource(providerKey);
        Provider<? extends T> provider = providerFactory.get(errors, context, dependency);
        try {
            return errors.checkForNull(provider.get(), source, dependency);
        } catch (RuntimeException userException) {
            throw errors.errorInProvider(userException).toException();
        }
    }

    @Override
    public String toString() {
        return providerKey.toString();
    }
}
