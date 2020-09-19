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

import org.elasticsearch.common.inject.InjectorImpl.MethodInvoker;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.spi.InjectionPoint;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Invokes an injectable method.
 * 针对方法 找到注入点并进行注入
 * 可以理解为  spring的 set方法注入
 */
class SingleMethodInjector implements SingleMemberInjector {

    /**
     * 定义了一个调用方法的接口
     */
    final MethodInvoker methodInvoker;
    /**
     * 从这里可以看出 针对携带@Inject注解的 方法  实际上并不是为方法进行注入  而是基于参数进行注入
     */
    final SingleParameterInjector<?>[] parameterInjectors;

    /**
     * 描述方法级别注入点信息
     */
    final InjectionPoint injectionPoint;

    SingleMethodInjector(InjectorImpl injector, InjectionPoint injectionPoint, Errors errors)
            throws ErrorsException {
        this.injectionPoint = injectionPoint;
        final Method method = (Method) injectionPoint.getMember();
        // 创建反射调用方法的对象 等同于 method.invoke(target, parameters)
        methodInvoker = createMethodInvoker(method);
        // 每个 dependency 就对应一个参数   也就对应一个参数注入器
        parameterInjectors = injector.getParametersInjectors(injectionPoint.getDependencies(), errors);
    }

    /**
     * 创建句柄对象 实际上就是调用method
     * @param method
     * @return
     */
    private MethodInvoker createMethodInvoker(final Method method) {

        // We can't use FastMethod if the method is private.
        int modifiers = method.getModifiers();
        // TODO 忽略这个 NOOP 操作
        if (!Modifier.isPrivate(modifiers) && !Modifier.isProtected(modifiers)) {
        }

        return new MethodInvoker() {
            @Override
            public Object invoke(Object target, Object... parameters)
                    throws IllegalAccessException, InvocationTargetException {
                return method.invoke(target, parameters);
            }
        };
    }

    @Override
    public InjectionPoint getInjectionPoint() {
        return injectionPoint;
    }

    /**
     * 调用某个实例的 方法  (参数从ioc容器中获取)
     * @param errors
     * @param context
     * @param o
     */
    @Override
    public void inject(Errors errors, InternalContext context, Object o) {
        Object[] parameters;
        try {
            // 通过parameterInjector 获取参数 之后调用方法
            parameters = SingleParameterInjector.getAll(errors, context, parameterInjectors);
        } catch (ErrorsException e) {
            errors.merge(e.getErrors());
            return;
        }

        try {
            methodInvoker.invoke(o, parameters);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e); // a security manager is blocking us, we're hosed
        } catch (InvocationTargetException userException) {
            Throwable cause = userException.getCause() != null
                    ? userException.getCause()
                    : userException;
            errors.withSource(injectionPoint).errorInjectingMethod(cause);
        }
    }
}
