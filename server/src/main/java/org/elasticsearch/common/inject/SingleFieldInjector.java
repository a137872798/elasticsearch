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
import org.elasticsearch.common.inject.spi.InjectionPoint;

import java.lang.reflect.Field;

/**
 * Sets an injectable field.
 * 针对某个field进行增强的对象
 */
class SingleFieldInjector implements SingleMemberInjector {

    /**
     * 待增强的字段
     */
    final Field field;
    /**
     * 对应的增强点对象
     */
    final InjectionPoint injectionPoint;

    /**
     * 在携带了@Inject注解的field上寻找是否有某个包含 @BindingAnnotation注解的 注解 它将作为依赖项  同时field最多仅支持存在一个依赖项
     */
    final Dependency<?> dependency;
    final InternalFactory<?> factory;


    /**
     *
     * @param injector  整个增强逻辑的中枢对象
     * @param injectionPoint   指定被增强的切入点
     * @param errors
     * @throws ErrorsException
     */
    SingleFieldInjector(InjectorImpl injector, InjectionPoint injectionPoint, Errors errors)
            throws ErrorsException {
        this.injectionPoint = injectionPoint;
        this.field = (Field) injectionPoint.getMember();
        this.dependency = injectionPoint.getDependencies().get(0);
        // dependency.getKey() 返回的是携带 @BindingAnnotation的注解
        factory = injector.getInternalFactory(dependency.getKey(), errors);
    }

    /**
     * 返回当前的注入点
     * @return
     */
    @Override
    public InjectionPoint getInjectionPoint() {
        return injectionPoint;
    }

    /**
     * 进行增强操作
     * @param errors
     * @param context
     * @param o
     */
    @Override
    public void inject(Errors errors, InternalContext context, Object o) {
        errors = errors.withSource(dependency);

        // 在进行增强前设置依赖  并在注入完成后 移除依赖
        context.setDependency(dependency);
        try {
            // 将生成的结果注入到字段中
            Object value = factory.get(errors, context, dependency);
            field.set(o, value);
        } catch (ErrorsException e) {
            errors.withSource(injectionPoint).merge(e.getErrors());
        } catch (IllegalAccessException e) {
            throw new AssertionError(e); // a security manager is blocking us, we're hosed
        } finally {
            context.setDependency(null);
        }
    }
}
