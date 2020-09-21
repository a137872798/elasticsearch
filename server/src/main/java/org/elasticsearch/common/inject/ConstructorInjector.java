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

import org.elasticsearch.common.inject.internal.ConstructionContext;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.spi.InjectionPoint;

import java.lang.reflect.InvocationTargetException;
import java.util.Set;

/**
 * Creates instances using an injectable constructor. After construction, all injectable fields and
 * methods are injected.
 *
 * @author crazybob@google.com (Bob Lee)
 * 代表基于某个构造器进行注入
 */
class ConstructorInjector<T> {

    private final Set<InjectionPoint> injectableMembers;
    private final SingleParameterInjector<?>[] parameterInjectors;
    private final ConstructionProxy<T> constructionProxy;
    private final MembersInjectorImpl<T> membersInjector;

    /**
     *
     * @param injectableMembers  代表有关field/method的注入点描述信息
     * @param constructionProxy  该对象内部包含了构造函数 以及通过调用构造函数生成实例的方法
     * @param parameterInjectors   代表从ioc容器中获取调用构造函数所需参数的对象
     * @param membersInjector   对生成对象进行 field/method 进行注入的对象
     * @throws ErrorsException
     */
    ConstructorInjector(Set<InjectionPoint> injectableMembers,
                        ConstructionProxy<T> constructionProxy,
                        SingleParameterInjector<?>[] parameterInjectors,
                        MembersInjectorImpl<T> membersInjector)
            throws ErrorsException {
        this.injectableMembers = injectableMembers;
        this.constructionProxy = constructionProxy;
        this.parameterInjectors = parameterInjectors;
        this.membersInjector = membersInjector;
    }

    public Set<InjectionPoint> getInjectableMembers() {
        return injectableMembers;
    }

    ConstructionProxy<T> getConstructionProxy() {
        return constructionProxy;
    }

    /**
     * Construct an instance. Returns {@code Object} instead of {@code T} because
     * it may return a proxy.
     * 基于构造器生成实例并返回
     */
    Object construct(Errors errors, InternalContext context, Class<?> expectedType)
            throws ErrorsException {
        // 推测这个是用于判断循环依赖的
        ConstructionContext<T> constructionContext = context.getConstructionContext(this);

        // We have a circular reference between constructors. Return a proxy.
        // 先忽略为true的情况
        if (constructionContext.isConstructing()) {
            // TODO (crazybob): if we can't proxy this object, can we proxy the other object?
            return constructionContext.createProxy(errors, expectedType);
        }

        // If we're re-entering this factory while injecting fields or methods,
        // return the same instance. This prevents infinite loops.
        // 代表对象已经创建完毕了 直接返回
        T t = constructionContext.getCurrentReference();
        if (t != null) {
            return t;
        }

        try {
            // First time through...
            // 代表此时开始生成传入key对应的实例 (开始进行构造)
            constructionContext.startConstruction();
            try {
                // 从 ioc工厂中获取需要的参数实例
                Object[] parameters = SingleParameterInjector.getAll(errors, context, parameterInjectors);
                t = constructionProxy.newInstance(parameters);
                // 为之前所有的动态代理处理器设置代理对象
                constructionContext.setProxyDelegates(t);
            } finally {
                constructionContext.finishConstruction();
            }

            // Store reference. If an injector re-enters this factory, they'll get the same reference.
            // 当处理完毕时 返回实例
            constructionContext.setCurrentReference(t);

            // 当实例生成时 注入field 和调用 注入方法
            membersInjector.injectMembers(t, errors, context);
            // 触发监听器
            membersInjector.notifyListeners(t, errors);

            return t;
        } catch (InvocationTargetException userException) {
            Throwable cause = userException.getCause() != null
                    ? userException.getCause()
                    : userException;
            throw errors.withSource(constructionProxy.getInjectionPoint())
                    .errorInjectingConstructor(cause).toException();
        } finally {
            constructionContext.removeCurrentReference();
        }
    }
}
