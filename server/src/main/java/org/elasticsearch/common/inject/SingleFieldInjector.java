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
 * 该对象已经具备为某个field进行注入的基本能力了
 */
class SingleFieldInjector implements SingleMemberInjector {

    /**
     * 待注入的字段
     */
    final Field field;
    /**
     * 有关注入的描述信息  比如该注入点的依赖   比如@Injector注解标记在方法层上 那么实际上要注入的是它的所有参数 (推测)
     * 而针对 field级别的注入点信息就是描述了注入的实现类  比如@Named 注解  因为一个field可能有多种实现类 通过@Named 可以定位到精确的实现类
     */
    final InjectionPoint injectionPoint;

    /**
     * 针对field 仅存在一个 dependency  而针对 method级别 每个参数对应一个 dependency
     * 每个field / method 仅对应一个 注入点
     */
    final Dependency<?> dependency;
    final InternalFactory<?> factory;


    /**
     *
     * @param injector  维护了各种binding对象  binding对象就是维护了 key 以及provider的关系
     * @param injectionPoint   指定被增强的切入点
     * @param errors
     * @throws ErrorsException
     */
    SingleFieldInjector(InjectorImpl injector, InjectionPoint injectionPoint, Errors errors)
            throws ErrorsException {
        this.injectionPoint = injectionPoint;
        this.field = (Field) injectionPoint.getMember();
        // 因为 field 最多仅存在一个依赖对象
        this.dependency = injectionPoint.getDependencies().get(0);
        // 找到提供依赖项实例对象的工厂
        // 因为针对field的 dependency 的key 就是当前field的type 所以可以直接通过该工厂获取注入实例  如果是method对应的InjectorPoint 是不能这样操作的
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
     * 为某个实例的 该field属性 进行注入
     * @param errors
     * @param context
     * @param o
     */
    @Override
    public void inject(Errors errors, InternalContext context, Object o) {
        // 更新error的源信息 这样可以弹出正确的异常信息
        errors = errors.withSource(dependency);

        // 在进行增强前设置依赖  并在注入完成后 移除依赖  因为在注入时 依赖信息会作为选择实现类的重要参考
        context.setDependency(dependency);
        try {
            // 通过工厂返回需要的实例对象 并通过反射 将属性设置到实例上
            Object value = factory.get(errors, context, dependency);
            field.set(o, value);
        } catch (ErrorsException e) {
            errors.withSource(injectionPoint).merge(e.getErrors());
        } catch (IllegalAccessException e) {
            throw new AssertionError(e); // a security manager is blocking us, we're hosed
        } finally {
            // 在使用完毕后从上下文中移除依赖信息  便于为下一个对象进行注入  为所有实例进行注入在整个ioc容器中是一种链式操作
            context.setDependency(null);
        }
    }
}
