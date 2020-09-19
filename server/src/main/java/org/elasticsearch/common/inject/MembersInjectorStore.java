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
import org.elasticsearch.common.inject.spi.TypeListenerBinding;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Members injectors by type.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * 本身 MembersInjectors 是 MembersInjector<T> 这样的
 * 这里将 T生成的Key 作为查询条件 获取MembersInjector
 */
class MembersInjectorStore {
    private final InjectorImpl injector;
    private final List<TypeListenerBinding> typeListenerBindings;

    /**
     * 缓存 通过指定要增强的类型 返回一个为field/method执行注入的对象 最终会通过injector执行注入逻辑
     */
    private final FailableCache<TypeLiteral<?>, MembersInjectorImpl<?>> cache
        = new FailableCache<TypeLiteral<?>, MembersInjectorImpl<?>>() {
        @Override
        protected MembersInjectorImpl<?> create(TypeLiteral<?> type, Errors errors)
            throws ErrorsException {
            // 根据 type类型 创建 MembersInjector对象
            return createWithListeners(type, errors);
        }
    };

    /**
     *
     * @param injector  该存储对象关联的注入器
     * @param typeListenerBindings
     */
    MembersInjectorStore(InjectorImpl injector,
                         List<TypeListenerBinding> typeListenerBindings) {
        this.injector = injector;
        this.typeListenerBindings = Collections.unmodifiableList(typeListenerBindings);
    }

    /**
     * Returns true if any type listeners are installed. Other code may take shortcuts when there
     * aren't any type listeners.
     */
    public boolean hasTypeListeners() {
        return !typeListenerBindings.isEmpty();
    }

    /**
     * Returns a new complete members injector with injection listeners registered.
     * the MembersInjector type always agrees with the passed type
     */
    @SuppressWarnings("unchecked")
    public <T> MembersInjectorImpl<T> get(TypeLiteral<T> key, Errors errors) throws ErrorsException {
        return (MembersInjectorImpl<T>) cache.get(key, errors);
    }

    /**
     * Creates a new members injector and attaches both injection listeners and method aspects.
     *
     * @param type 根据 T类型 找到对应的 MembersInjector<T>
     * @return 生成针对 method/field进行增强的对象
     */
    private <T> MembersInjectorImpl<T> createWithListeners(TypeLiteral<T> type, Errors errors)
        throws ErrorsException {
        int numErrorsBefore = errors.size();

        Set<InjectionPoint> injectionPoints;
        try {
            // 从目标类上 找到所有需要注入的 method 和 field
            injectionPoints = InjectionPoint.forInstanceMethodsAndFields(type);
        } catch (ConfigurationException e) {
            errors.merge(e.getErrorMessages());
            injectionPoints = e.getPartialValue();
        }

        // 返回的一组对象已经具备 为实例进行注入的能力了 而调用时机由该对象决定
        List<SingleMemberInjector> injectors = getInjectors(injectionPoints, errors);
        errors.throwIfNewErrors(numErrorsBefore);

        // 将查找对象包装成 EncounterImpl 对象
        EncounterImpl<T> encounter = new EncounterImpl<>(errors, injector.lookups);
        // 如果本次处理的type 满足绑定监听器的条件 绑定监听器  TODO 目前在ES中没有监听器的实现类 先忽略
        for (TypeListenerBinding typeListener : typeListenerBindings) {
            if (typeListener.getTypeMatcher().matches(type)) {
                try {
                    // 触发监听器
                    typeListener.getListener().hear(type, encounter);
                } catch (RuntimeException e) {
                    errors.errorNotifyingTypeListener(typeListener, type, e);
                }
            }
        }
        encounter.invalidate();
        errors.throwIfNewErrors(numErrorsBefore);

        // 将相关信息包装成一个组件 这样外部可以直接通过type 来寻找到这个注入模板 并通过传入实例进行注入
        return new MembersInjectorImpl<>(injector, type, encounter, injectors);
    }

    /**
     * Returns the injectors for the specified injection points.
     * @param injectionPoints T类型包含的所有增强点  方法级 以及 field
     */
    List<SingleMemberInjector> getInjectors(
        Set<InjectionPoint> injectionPoints, Errors errors) {
        List<SingleMemberInjector> injectors = new ArrayList<>();
        for (InjectionPoint injectionPoint : injectionPoints) {
            try {
                // 为true时 代表当某个注入点无法从ioc容器中找到目标对象 不会抛出异常  也就相当于 Spring的 (require = false)
                Errors errorsForMember = injectionPoint.isOptional()
                    ? new Errors(injectionPoint)
                    : errors.withSource(injectionPoint);
                // 注入点对象 与注入器本身 合并成了一个 FieldInjector/MethodInjector  也就是此时对象已经具备注入的能力了
                // 注入器本身已经维护各种绑定关系了  也就可以理解为一个ioc容器
                SingleMemberInjector injector = injectionPoint.getMember() instanceof Field
                    ? new SingleFieldInjector(this.injector, injectionPoint, errorsForMember)
                    : new SingleMethodInjector(this.injector, injectionPoint, errorsForMember);
                injectors.add(injector);
            } catch (ErrorsException ignoredForNow) {
                // ignored for now
            }
        }
        return Collections.unmodifiableList(injectors);
    }
}
