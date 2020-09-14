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

package org.elasticsearch.common.inject.internal;

import org.elasticsearch.common.Classes;
import org.elasticsearch.common.inject.BindingAnnotation;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.ScopeAnnotation;
import org.elasticsearch.common.inject.TypeLiteral;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Member;

/**
 * Annotation utilities.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public class Annotations {

    /**
     * Returns true if the given annotation is retained at runtime.
     */
    public static boolean isRetainedAtRuntime(Class<? extends Annotation> annotationType) {
        Retention retention = annotationType.getAnnotation(Retention.class);
        return retention != null && retention.value() == RetentionPolicy.RUNTIME;
    }

    /**
     * Returns the scope annotation on {@code type}, or null if none is specified.
     */
    public static Class<? extends Annotation> findScopeAnnotation(
            Errors errors, Class<?> implementation) {
        return findScopeAnnotation(errors, implementation.getAnnotations());
    }

    /**
     * Returns the scoping annotation, or null if there isn't one.
     */
    public static Class<? extends Annotation> findScopeAnnotation(Errors errors, Annotation[] annotations) {
        Class<? extends Annotation> found = null;

        for (Annotation annotation : annotations) {
            if (annotation.annotationType().getAnnotation(ScopeAnnotation.class) != null) {
                if (found != null) {
                    errors.duplicateScopeAnnotations(found, annotation.annotationType());
                } else {
                    found = annotation.annotationType();
                }
            }
        }

        return found;
    }

    public static boolean isScopeAnnotation(Class<? extends Annotation> annotationType) {
        return annotationType.getAnnotation(ScopeAnnotation.class) != null;
    }

    /**
     * Adds an error if there is a misplaced annotations on {@code type}. Scoping
     * annotations are not allowed on abstract classes or interfaces.
     */
    public static void checkForMisplacedScopeAnnotations(
            Class<?> type, Object source, Errors errors) {
        if (Classes.isConcrete(type)) {
            return;
        }

        Class<? extends Annotation> scopeAnnotation = findScopeAnnotation(errors, type);
        if (scopeAnnotation != null) {
            errors.withSource(type).scopeAnnotationOnAbstractType(scopeAnnotation, type, source);
        }
    }

    /**
     * Gets a key for the given type, member and annotations.
     * @param type 本次待增强的某个类的某个方法参数 抽取泛型信息后对应的对象
     * @param member 增强的方法/构造函数
     * @param annotations  type上携带的所有注解
     */
    public static Key<?> getKey(TypeLiteral<?> type, Member member, Annotation[] annotations,
                                Errors errors) throws ErrorsException {
        int numErrorsBefore = errors.size();
        // 找到内部携带@BindingAnnotation 注解 的注解
        Annotation found = findBindingAnnotation(errors, member, annotations);
        errors.throwIfNewErrors(numErrorsBefore);
        return found == null ? Key.get(type) : Key.get(type, found);
    }

    /**
     * Returns the binding annotation on {@code member}, or null if there isn't one.
     * @param errors 记录错误信息
     * @param member 待增强的字段或方法/构造函数
     * @param annotations 对应member上抽取的所有注解
     */
    public static Annotation findBindingAnnotation(
            Errors errors, Member member, Annotation[] annotations) {
        Annotation found = null;

        for (Annotation annotation : annotations) {
            // 代表member上的注解内部包含了BindingAnnotation
            if (annotation.annotationType().getAnnotation(BindingAnnotation.class) != null) {
                if (found != null) {
                    // 当待增强点上的有个注解中 有不止一个包含了 BindingAnnotation 则增加异常信息
                    errors.duplicateBindingAnnotations(member,
                            found.annotationType(), annotation.annotationType());
                } else {
                    found = annotation;
                }
            }
        }

        return found;
    }
}
