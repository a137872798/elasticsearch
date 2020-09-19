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
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.spi.InjectionListener;
import org.elasticsearch.common.inject.spi.InjectionPoint;

import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

/**
 * Injects members of instances of a given type.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
class MembersInjectorImpl<T> implements MembersInjector<T> {
    private final TypeLiteral<T> typeLiteral;
    private final InjectorImpl injector;
    private final List<SingleMemberInjector> memberInjectors;
    private final List<MembersInjector<? super T>> userMembersInjectors;
    private final List<InjectionListener<? super T>> injectionListeners;

    /**
     *
     * @param injector  注入器
     * @param typeLiteral  待注入实例的类型
     * @param encounter  该对象对外暴露一个获取 MembersInjectors的api  看来所有MembersInjectorImpl 都会交由它管理
     * @param memberInjectors  该列表中存储了一组对实例待注入字段进行注入的逻辑
     */
    MembersInjectorImpl(InjectorImpl injector, TypeLiteral<T> typeLiteral,
                        EncounterImpl<T> encounter, List<SingleMemberInjector> memberInjectors) {
        this.injector = injector;
        this.typeLiteral = typeLiteral;
        this.memberInjectors = memberInjectors;
        // 获取当前已经存在的 MembersInjector
        this.userMembersInjectors = encounter.getMembersInjectors();
        // 获取相关监听器
        this.injectionListeners = encounter.getInjectionListeners();
    }

    public List<SingleMemberInjector> getMemberInjectors() {
        return memberInjectors;
    }

    /**
     * 为某个实例进行注入 同时触发相关监听器
     * @param instance to inject members on. May be {@code null}.
     */
    @Override
    public void injectMembers(T instance) {
        Errors errors = new Errors(typeLiteral);
        try {
            injectAndNotify(instance, errors);
        } catch (ErrorsException e) {
            errors.merge(e.getErrors());
        }

        errors.throwProvisionExceptionIfErrorsExist();
    }

    void injectAndNotify(final T instance, final Errors errors) throws ErrorsException {
        if (instance == null) {
            return;
        }

        // 以回调方式触发注入 由injector执行  看来当以构造器创建对象时 进行的注入是通过 injector完成的
        injector.callInContext(new ContextualCallable<Void>() {
            @Override
            public Void call(InternalContext context) throws ErrorsException {
                injectMembers(instance, errors, context);
                return null;
            }
        });

        // 将处理完的实例触发监听器
        notifyListeners(instance, errors);
    }

    /**
     * 触发监听器
     * @param instance
     * @param errors
     * @throws ErrorsException
     */
    void notifyListeners(T instance, Errors errors) throws ErrorsException {
        int numErrorsBefore = errors.size();
        for (InjectionListener<? super T> injectionListener : injectionListeners) {
            try {
                injectionListener.afterInjection(instance);
            } catch (RuntimeException e) {
                errors.errorNotifyingInjectionListener(injectionListener, typeLiteral, e);
            }
        }
        errors.throwIfNewErrors(numErrorsBefore);
    }

    /**
     * 为实例进行注入工作
     * @param t
     * @param errors
     * @param context
     */
    void injectMembers(T t, Errors errors, InternalContext context) {
        // optimization: use manual for/each to save allocating an iterator here
        // 注入各种需要的属性
        for (int i = 0, size = memberInjectors.size(); i < size; i++) {
            memberInjectors.get(i).inject(errors, context, t);
        }

        // optimization: use manual for/each to save allocating an iterator here
        for (int i = 0, size = userMembersInjectors.size(); i < size; i++) {
            MembersInjector<? super T> userMembersInjector = userMembersInjectors.get(i);
            try {
                // TODO 这个是什么时候设置的 以及 对象的所有待注入属性应该都应该包装成对应的memberInjectors了  应该不需要重复注入了
                userMembersInjector.injectMembers(t);
            } catch (RuntimeException e) {
                errors.errorInUserInjector(userMembersInjector, typeLiteral, e);
            }
        }
    }

    @Override
    public String toString() {
        return "MembersInjector<" + typeLiteral + ">";
    }

    public Set<InjectionPoint> getInjectionPoints() {
        return unmodifiableSet(memberInjectors.stream()
                .map(SingleMemberInjector::getInjectionPoint)
                .collect(toSet()));
    }
}
