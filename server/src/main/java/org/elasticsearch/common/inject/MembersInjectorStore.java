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
     * 缓存 待增强类 与绑定的增强对象
     */
    private final FailableCache<TypeLiteral<?>, MembersInjectorImpl<?>> cache
        = new FailableCache<TypeLiteral<?>, MembersInjectorImpl<?>>() {
        @Override
        protected MembersInjectorImpl<?> create(TypeLiteral<?> type, Errors errors)
            throws ErrorsException {
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
     * @param type 待增强的对象
     * @return 生成针对 method/field进行增强的对象
     */
    private <T> MembersInjectorImpl<T> createWithListeners(TypeLiteral<T> type, Errors errors)
        throws ErrorsException {
        int numErrorsBefore = errors.size();

        Set<InjectionPoint> injectionPoints;
        try {
            // 从目标类上 找到所有支持增强的method/field
            injectionPoints = InjectionPoint.forInstanceMethodsAndFields(type);
        } catch (ConfigurationException e) {
            errors.merge(e.getErrorMessages());
            injectionPoints = e.getPartialValue();
        }

        List<SingleMemberInjector> injectors = getInjectors(injectionPoints, errors);
        errors.throwIfNewErrors(numErrorsBefore);

        EncounterImpl<T> encounter = new EncounterImpl<>(errors, injector.lookups);
        for (TypeListenerBinding typeListener : typeListenerBindings) {
            if (typeListener.getTypeMatcher().matches(type)) {
                try {
                    typeListener.getListener().hear(type, encounter);
                } catch (RuntimeException e) {
                    errors.errorNotifyingTypeListener(typeListener, type, e);
                }
            }
        }
        encounter.invalidate();
        errors.throwIfNewErrors(numErrorsBefore);

        return new MembersInjectorImpl<>(injector, type, encounter, injectors);
    }

    /**
     * Returns the injectors for the specified injection points.
     */
    List<SingleMemberInjector> getInjectors(
        Set<InjectionPoint> injectionPoints, Errors errors) {
        List<SingleMemberInjector> injectors = new ArrayList<>();
        for (InjectionPoint injectionPoint : injectionPoints) {
            try {
                Errors errorsForMember = injectionPoint.isOptional()
                    ? new Errors(injectionPoint)
                    : errors.withSource(injectionPoint);
                SingleMemberInjector injector = injectionPoint.getMember() instanceof Field
                    // 代表基于field的增强对象
                    ? new SingleFieldInjector(this.injector, injectionPoint, errorsForMember)
                    // 基于method的增强对象
                    : new SingleMethodInjector(this.injector, injectionPoint, errorsForMember);
                injectors.add(injector);
            } catch (ErrorsException ignoredForNow) {
                // ignored for now
            }
        }
        return Collections.unmodifiableList(injectors);
    }
}
