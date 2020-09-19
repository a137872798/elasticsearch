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

/**
 * Injects dependencies into the fields and methods on instances of type {@code T}. Ignores the
 * presence or absence of an injectable constructor.
 *
 * @param <T> type to inject members of
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 * 针对实例上标记@Inject的 方法或者 字段进行注入   针对字段就是从ioc容器中找到注入对象并完成注入
 * 针对方法就是将所有参数从ioc容器中找到注入对象并将这些作为参数调用方法  (实际上针对方法级别的注入可以理解为spring的set注入  场景是一样的
 * 核心是从ioc容器中获取参数并调用 同时不在乎返回值  以及在检测方法是否携带@Inject后 还必须确保类型T 内部包含与方法同名的属性  这点就暗示了这个意思)
 * 2个含义有所区别  注意这里都没有涉及到构造器
 */
public interface MembersInjector<T> {

    /**
     * Injects dependencies into the fields and methods of {@code instance}. Ignores the presence or
     * absence of an injectable constructor.
     * <p>
     * Whenever Guice creates an instance, it performs this injection automatically (after first
     * performing constructor injection), so if you're able to let Guice create all your objects for
     * you, you'll never need to use this method.
     *
     * @param instance to inject members on. May be {@code null}.
     *                 传入实例 并对实例上符合条件的字段进行注入
     */
    void injectMembers(T instance);
}
