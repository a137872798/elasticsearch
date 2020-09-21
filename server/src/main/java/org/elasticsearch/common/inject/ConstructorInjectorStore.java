/*
 * Copyright (C) 2009 Google Inc.
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
import org.elasticsearch.common.inject.internal.FailableCache;
import org.elasticsearch.common.inject.spi.InjectionPoint;

/**
 * Constructor injectors by type.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * 跟那个 基于 field/method 进行注入的store类似
 */
class ConstructorInjectorStore {
    private final InjectorImpl injector;

    private final FailableCache<TypeLiteral<?>, ConstructorInjector<?>> cache
            = new FailableCache<TypeLiteral<?>, ConstructorInjector<?>>() {
        @Override
        protected ConstructorInjector<?> create(TypeLiteral<?> type, Errors errors)
                throws ErrorsException {
            return createConstructor(type, errors);
        }
    };

    ConstructorInjectorStore(InjectorImpl injector) {
        this.injector = injector;
    }

    /**
     * Returns a new complete constructor injector with injection listeners registered.
     */
    @SuppressWarnings("unchecked") // the ConstructorInjector type always agrees with the passed type
    public <T> ConstructorInjector<T> get(TypeLiteral<T> key, Errors errors) throws ErrorsException {
        return (ConstructorInjector<T>) cache.get(key, errors);
    }

    /**
     * 创建基于构造函数的注入器对象
     * @param type
     * @param errors
     * @param <T>
     * @return
     * @throws ErrorsException
     */
    private <T> ConstructorInjector<T> createConstructor(TypeLiteral<T> type, Errors errors)
            throws ErrorsException {
        int numErrorsBefore = errors.size();

        InjectionPoint injectionPoint;
        try {
            // 找到目标类型上所有的构造函数 并将携带@Injector的构造函数作为生成存储在ioc容器实例的方法  如果没有携带@Injector的构造函数则使用无参构造函数 如果有多个构造函数使用了@Injector注解 那么也忽略
            injectionPoint = InjectionPoint.forConstructorOf(type);
        } catch (ConfigurationException e) {
            errors.merge(e.getErrorMessages());
            throw errors.toException();
        }

        // 基于依赖对象 生成各种参数注解   就是根据参数类型去ioc容器中获取实例
        SingleParameterInjector<?>[] constructorParameterInjectors
                = injector.getParametersInjectors(injectionPoint.getDependencies(), errors);
        // 找到有关基于 method进行属性注入  以及 直接从ioc中寻找实例信息并进行属性注入的各种 InjectorImpl对象
        MembersInjectorImpl<T> membersInjector = injector.membersInjectorStore.get(type, errors);

        ConstructionProxyFactory<T> factory = new DefaultConstructionProxyFactory<>(injectionPoint);

        errors.throwIfNewErrors(numErrorsBefore);

        return new ConstructorInjector<>(membersInjector.getInjectionPoints(), factory.create(),
                constructorParameterInjectors, membersInjector);
    }
}
