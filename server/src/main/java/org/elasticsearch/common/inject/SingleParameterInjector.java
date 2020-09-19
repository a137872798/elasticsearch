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

import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.internal.InternalFactory;
import org.elasticsearch.common.inject.spi.Dependency;

/**
 * Resolves a single parameter, to be used in a constructor or method invocation.
 * 对应方法级上为某个参数进行注入的对象
 */
class SingleParameterInjector<T> {
    private static final Object[] NO_ARGUMENTS = {};

    /**
     * 基于method 创建的 注入点对象 每个参数对应一个依赖对象
     */
    private final Dependency<T> dependency;
    /**
     * 基于依赖的key 获取provider的工厂
     */
    private final InternalFactory<? extends T> factory;

    SingleParameterInjector(Dependency<T> dependency, InternalFactory<? extends T> factory) {
        this.dependency = dependency;
        this.factory = factory;
    }

    /**
     * 返回参数实例
     * @param errors
     * @param context
     * @return
     * @throws ErrorsException
     */
    private T inject(Errors errors, InternalContext context) throws ErrorsException {
        context.setDependency(dependency);
        try {
            return factory.get(errors.withSource(dependency), context, dependency);
        } finally {
            context.setDependency(null);
        }
    }

    /**
     * Returns an array of parameter values.
     * @param parameterInjectors 定义了从ioc容器中获取参数的能力
     *
     * 触发所有参数Injector 获取参数后返回
     */
    static Object[] getAll(Errors errors, InternalContext context,
                           SingleParameterInjector<?>[] parameterInjectors) throws ErrorsException {
        if (parameterInjectors == null) {
            return NO_ARGUMENTS;
        }

        int numErrorsBefore = errors.size();

        int size = parameterInjectors.length;
        Object[] parameters = new Object[size];

        // optimization: use manual for/each to save allocating an iterator here
        for (int i = 0; i < size; i++) {
            SingleParameterInjector<?> parameterInjector = parameterInjectors[i];
            try {
                parameters[i] = parameterInjector.inject(errors, context);
            } catch (ErrorsException e) {
                errors.merge(e.getErrors());
            }
        }

        errors.throwIfNewErrors(numErrorsBefore);
        return parameters;
    }
}
