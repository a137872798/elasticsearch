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

package org.elasticsearch.common.inject.spi;

import org.elasticsearch.common.inject.Key;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * A variable that can be resolved by an injector.
 * <p>
 * Use {@link #get} to build a freestanding dependency, or {@link InjectionPoint} to build one
 * that's attached to a constructor, method or field.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 * 代表一个依赖对象
 */
public final class Dependency<T> {

    /**
     * 依赖于哪个注入点对象
     */
    private final InjectionPoint injectionPoint;
    private final Key<T> key;
    private final boolean nullable;
    /**
     * 当前注入点如果是参数的话 应该是对应参数的下标
     */
    private final int parameterIndex;

    /**
     * 初始化时 只是进行简单的赋值操作
     * @param injectionPoint   该对象是基于哪个注入点生成的
     * @param key   如果某个参数上不包含@BindingAnnonation注解的注解 那么该key 为null
     * @param nullable   参数注解上是否包含 @Nullable注解
     * @param parameterIndex   该参数在参数列表中的下标
     */
    Dependency(InjectionPoint injectionPoint, Key<T> key,
               boolean nullable, int parameterIndex) {
        this.injectionPoint = injectionPoint;
        this.key = key;
        this.nullable = nullable;
        this.parameterIndex = parameterIndex;
    }

    /**
     * Returns a new dependency that is not attached to an injection point. The returned dependency is
     * nullable.
     * 这样会返回一个没有连接到任何注入点的 依赖对象
     */
    public static <T> Dependency<T> get(Key<T> key) {
        return new Dependency<>(null, key, true, -1);
    }

    /**
     * Returns the dependencies from the given injection points.
     * 将一组注入点相关的依赖返回
     */
    public static Set<Dependency<?>> forInjectionPoints(Set<InjectionPoint> injectionPoints) {
        Set<Dependency<?>> dependencies = new HashSet<>();
        for (InjectionPoint injectionPoint : injectionPoints) {
            dependencies.addAll(injectionPoint.getDependencies());
        }
        return unmodifiableSet(dependencies);
    }

    /**
     * Returns the key to the binding that satisfies this dependency.
     */
    public Key<T> getKey() {
        return this.key;
    }

    /**
     * Returns true if null is a legal value for this dependency.
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Returns the injection point to which this dependency belongs, or null if this dependency isn't
     * attached to a particular injection point.
     */
    public InjectionPoint getInjectionPoint() {
        return injectionPoint;
    }

    /**
     * Returns the index of this dependency in the injection point's parameter list, or {@code -1} if
     * this dependency does not belong to a parameter list. Only method and constructor dependencies
     * are elements in a parameter list.
     */
    public int getParameterIndex() {
        return parameterIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(injectionPoint, parameterIndex, key);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Dependency) {
            Dependency<?> dependency = (Dependency<?>) o;
            return Objects.equals(injectionPoint, dependency.injectionPoint)
                    && Objects.equals(parameterIndex, dependency.parameterIndex)
                    && Objects.equals(key, dependency.key);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(key);
        if (injectionPoint != null) {
            builder.append("@").append(injectionPoint);
            if (parameterIndex != -1) {
                builder.append("[").append(parameterIndex).append("]");
            }
        }
        return builder.toString();
    }
}
