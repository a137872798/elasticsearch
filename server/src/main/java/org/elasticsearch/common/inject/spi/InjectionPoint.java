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

import org.elasticsearch.common.inject.ConfigurationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.internal.Annotations;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.MoreTypes;
import org.elasticsearch.common.inject.internal.Nullability;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.inject.internal.MoreTypes.getRawType;

/**
 * A constructor, field or method that can receive injections. Typically this is a member with the
 * {@literal @}{@link Inject} annotation. For non-private, no argument constructors, the member may
 * omit the annotation.
 *
 * @author crazybob@google.com (Bob Lee)
 * @since 2.0
 * 代表一个注入点  或者说增强点
 */
public final class InjectionPoint {

    private final boolean optional;
    /**
     * Member 在 jdk的默认实现包含 method 和 field (还有constructor) 应该就是代表这个增强点是通过 field 还是 method进行增强的
     */
    private final Member member;

    /**
     * 当增强点是构造函数 或者方法时  这组依赖对象就是参数列表的参数
     * TODO 这些依赖的作用到底是什么
     */
    private final List<Dependency<?>> dependencies;

    private InjectionPoint(Member member,
                           List<Dependency<?>> dependencies, boolean optional) {
        this.member = member;
        this.dependencies = dependencies;
        this.optional = optional;
    }

    /**
     * 通过一个方法对象初始化增强点
     * @param type
     * @param method
     */
    InjectionPoint(TypeLiteral<?> type, Method method) {
        this.member = method;

        // 找到该方法上携带的  @Inject注解
        Inject inject = method.getAnnotation(Inject.class);
        this.optional = inject.optional();

        // 参数列表中每个参数对应一个 dependencies
        this.dependencies = forMember(method, type, method.getParameterAnnotations());
    }

    /**
     *
     * @param type   本次增强点对应的类型
     * @param constructor    携带@Inject注解的构造函数 或者说被增强的目标点
     */
    InjectionPoint(TypeLiteral<?> type, Constructor<?> constructor) {
        this.member = constructor;
        this.optional = false;
        this.dependencies = forMember(constructor, type, constructor.getParameterAnnotations());
    }

    /**
     * 以某个field作为增强点
     * @param type
     * @param field
     */
    InjectionPoint(TypeLiteral<?> type, Field field) {
        this.member = field;

        // 找到携带 @Inject注解的属性
        Inject inject = field.getAnnotation(Inject.class);
        this.optional = inject.optional();

        Annotation[] annotations = field.getAnnotations();

        Errors errors = new Errors(field);
        Key<?> key = null;
        try {
            // 找到该字段上 某个内置了@BindingAnnotation注解的注解  并包装成key对象
            key = Annotations.getKey(type.getFieldType(field), field, annotations, errors);
        } catch (ErrorsException e) {
            errors.merge(e.getErrors());
        }
        errors.throwConfigurationExceptionIfErrorsExist();

        this.dependencies = Collections.<Dependency<?>>singletonList(
            newDependency(key, Nullability.allowsNull(annotations), -1));
    }

    /**
     *
     * @param member   被增强的方法 或构造函数
     * @param type     被增强的目标类
     * @param parameterAnnotations   方法或构造函数上 每个参数携带的注解
     * @return
     */
    private List<Dependency<?>> forMember(Member member, TypeLiteral<?> type,
                                                   Annotation[][] parameterAnnotations) {
        Errors errors = new Errors(member);
        // 当某个参数不包含注解时 会返回一个空数组 而不会被忽略
        Iterator<Annotation[]> annotationsIterator = Arrays.asList(parameterAnnotations).iterator();

        List<Dependency<?>> dependencies = new ArrayList<>();
        int index = 0;

        // 被修饰方法的每个参数都会被包装成依赖对象
        for (TypeLiteral<?> parameterType : type.getParameterTypes(member)) {
            try {
                // 迭代每个参数上对应的一系列注解
                // 如果某个参数不包含任何注解 这里就返回一个空数组
                Annotation[] paramAnnotations = annotationsIterator.next();
                // 找到这么多注解中包含 @BindingAnnotation的那个 并包装成key 后返回
                Key<?> key = Annotations.getKey(parameterType, member, paramAnnotations, errors);
                // 将相关信息生成 依赖对象
                dependencies.add(newDependency(key, Nullability.allowsNull(paramAnnotations), index));
                index++;
            } catch (ErrorsException e) {
                errors.merge(e.getErrors());
            }
        }

        errors.throwConfigurationExceptionIfErrorsExist();
        return Collections.unmodifiableList(dependencies);
    }

    // This method is necessary to create a Dependency<T> with proper generic type information
    // 生成依赖对象
    private <T> Dependency<T> newDependency(Key<T> key, boolean allowsNull, int parameterIndex) {
        return new Dependency<>(this, key, allowsNull, parameterIndex);
    }

    /**
     * Returns the injected constructor, field, or method.
     * 获取该注入点针对的 构造函数/field/method
     */
    public Member getMember() {
        return member;
    }

    /**
     * Returns the dependencies for this injection point. If the injection point is for a method or
     * constructor, the dependencies will correspond to that member's parameters. Field injection
     * points always have a single dependency for the field itself.
     *
     * @return a possibly-empty list
     * 返回当前注入点的所有依赖
     */
    public List<Dependency<?>> getDependencies() {
        return dependencies;
    }

    /**
     * Returns true if this injection point shall be skipped if the injector cannot resolve bindings
     * for all required dependencies. Both explicit bindings (as specified in a module), and implicit
     * bindings ({@literal @}{@link org.elasticsearch.common.inject.ImplementedBy ImplementedBy}, default
     * constructors etc.) may be used to satisfy optional injection points.
     */
    public boolean isOptional() {
        return optional;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof InjectionPoint
                && member.equals(((InjectionPoint) o).member);
    }

    @Override
    public int hashCode() {
        return member.hashCode();
    }

    @Override
    public String toString() {
        return MoreTypes.toString(member);
    }

    /**
     * Returns a new injection point for the injectable constructor of {@code type}.
     *
     * @param type a concrete type with exactly one constructor annotated {@literal @}{@link Inject},
     *             or a no-arguments constructor that is not private.
     * @throws ConfigurationException if there is no injectable constructor, more than one injectable
     *                                constructor, or if parameters of the injectable constructor are malformed, such as a
     *                                parameter with multiple binding annotations.
     *                                为TypeLiteral内部的类创建一个基于构造函数的增强点
     */
    public static InjectionPoint forConstructorOf(TypeLiteral<?> type) {
        // type.getType() 为描述目标类型的方法 如果涉及到泛型 也能正确返回泛型信息   getRawType 针对普通的class不进行处理 而针对泛型类型 返回Object
        Class<?> rawType = getRawType(type.getType());
        // 生成该对象专用的 异常信息对象
        Errors errors = new Errors(rawType);

        Constructor<?> injectableConstructor = null;
        // 遍历目标对象的所有构造函数
        for (Constructor<?> constructor : rawType.getConstructors()) {
            // 当发现某个构造函数使用了 Inject注解时
            Inject inject = constructor.getAnnotation(Inject.class);
            if (inject != null) {
                // 构造函数该属性不应该为true 增加一条错误信息
                if (inject.optional()) {
                    errors.optionalConstructor(constructor);
                }

                // 一个待增强类 应该只存在一个包含 @Inject注解的构造函数
                if (injectableConstructor != null) {
                    errors.tooManyConstructors(rawType);
                }

                injectableConstructor = constructor;
                // TODO 先不管这里在校验啥
                checkForMisplacedBindingAnnotations(injectableConstructor, errors);
            }
        }

        // 此时如果 errors中已经存在异常信息了 选择抛出异常
        errors.throwConfigurationExceptionIfErrorsExist();

        // 生成基于 携带了@Inject注解的 构造函数的增强点对象
        if (injectableConstructor != null) {
            return new InjectionPoint(type, injectableConstructor);
        }

        // If no annotated constructor is found, look for a no-arg constructor instead.
        // 没有找到携带@Inject注解的 构造函数
        try {
            // 寻找无参构造函数
            Constructor<?> noArgConstructor = rawType.getConstructor();

            // Disallow private constructors on non-private classes (unless they have @Inject)
            // 如果该无参构造函数是private的 抛出异常 TODO 为啥 ???
            if (Modifier.isPrivate(noArgConstructor.getModifiers())
                    && !Modifier.isPrivate(rawType.getModifiers())) {
                errors.missingConstructor(rawType);
                throw new ConfigurationException(errors.getMessages());
            }

            checkForMisplacedBindingAnnotations(noArgConstructor, errors);
            // 无参构造函数 对应的增强点的 依赖对象为空
            return new InjectionPoint(type, noArgConstructor);
        } catch (NoSuchMethodException e) {
            errors.missingConstructor(rawType);
            throw new ConfigurationException(errors.getMessages());
        }
    }

    /**
     * Returns a new injection point for the injectable constructor of {@code type}.
     *
     * @param type a concrete type with exactly one constructor annotated {@literal @}{@link Inject},
     *             or a no-arguments constructor that is not private.
     * @throws ConfigurationException if there is no injectable constructor, more than one injectable
     *                                constructor, or if parameters of the injectable constructor are malformed, such as a
     *                                parameter with multiple binding annotations.
     */
    public static InjectionPoint forConstructorOf(Class<?> type) {
        return forConstructorOf(TypeLiteral.get(type));
    }

    /**
     * Returns all static method and field injection points on {@code type}.
     *
     * @return a possibly empty set of injection points. The set has a specified iteration order. All
     *         fields are returned and then all methods. Within the fields, supertype fields are returned
     *         before subtype fields. Similarly, supertype methods are returned before subtype methods.
     * @throws ConfigurationException if there is a malformed injection point on {@code type}, such as
     *                                a field with multiple binding annotations. The exception's {@link
     *                                ConfigurationException#getPartialValue() partial value} is a {@code Set<InjectionPoint>}
     *                                of the valid injection points.
     *                                找到type的所有静态方法 以及field 并找到所有的增强点
     */
    public static Set<InjectionPoint> forStaticMethodsAndFields(TypeLiteral type) {
        Set<InjectionPoint> result = new HashSet<>();
        Errors errors = new Errors();

        addInjectionPoints(type, Factory.FIELDS, true, result, errors);
        addInjectionPoints(type, Factory.METHODS, true, result, errors);

        result = unmodifiableSet(result);
        if (errors.hasErrors()) {
            throw new ConfigurationException(errors.getMessages()).withPartialValue(result);
        }
        return result;
    }

    /**
     * Returns all static method and field injection points on {@code type}.
     *
     * @return a possibly empty set of injection points. The set has a specified iteration order. All
     *         fields are returned and then all methods. Within the fields, supertype fields are returned
     *         before subtype fields. Similarly, supertype methods are returned before subtype methods.
     * @throws ConfigurationException if there is a malformed injection point on {@code type}, such as
     *                                a field with multiple binding annotations. The exception's {@link
     *                                ConfigurationException#getPartialValue() partial value} is a {@code Set<InjectionPoint>}
     *                                of the valid injection points.
     */
    public static Set<InjectionPoint> forStaticMethodsAndFields(Class<?> type) {
        return forStaticMethodsAndFields(TypeLiteral.get(type));
    }

    /**
     * Returns all instance method and field injection points on {@code type}.
     *
     * @return a possibly empty set of injection points. The set has a specified iteration order. All
     *         fields are returned and then all methods. Within the fields, supertype fields are returned
     *         before subtype fields. Similarly, supertype methods are returned before subtype methods.
     * @throws ConfigurationException if there is a malformed injection point on {@code type}, such as
     *                                a field with multiple binding annotations. The exception's {@link
     *                                ConfigurationException#getPartialValue() partial value} is a {@code Set<InjectionPoint>}
     *                                of the valid injection points.
     *                                将该实例所有方法和 field上的增强点找出来 并返回
     */
    public static Set<InjectionPoint> forInstanceMethodsAndFields(TypeLiteral<?> type) {
        Set<InjectionPoint> result = new HashSet<>();
        Errors errors = new Errors();

        // TODO (crazybob): Filter out overridden members.
        addInjectionPoints(type, Factory.FIELDS, false, result, errors);
        addInjectionPoints(type, Factory.METHODS, false, result, errors);

        result = unmodifiableSet(result);
        if (errors.hasErrors()) {
            throw new ConfigurationException(errors.getMessages()).withPartialValue(result);
        }
        return result;
    }

    /**
     * Returns all instance method and field injection points on {@code type}.
     *
     * @return a possibly empty set of injection points. The set has a specified iteration order. All
     *         fields are returned and then all methods. Within the fields, supertype fields are returned
     *         before subtype fields. Similarly, supertype methods are returned before subtype methods.
     * @throws ConfigurationException if there is a malformed injection point on {@code type}, such as
     *                                a field with multiple binding annotations. The exception's {@link
     *                                ConfigurationException#getPartialValue() partial value} is a {@code Set<InjectionPoint>}
     *                                of the valid injection points.
     */
    public static Set<InjectionPoint> forInstanceMethodsAndFields(Class<?> type) {
        return forInstanceMethodsAndFields(TypeLiteral.get(type));
    }

    /**
     *
     * @param member  待增强的 field/constructor/method
     * @param errors
     */
    private static void checkForMisplacedBindingAnnotations(Member member, Errors errors) {
        // 找到包含BindingAnnotation 的注解
        Annotation misplacedBindingAnnotation = Annotations.findBindingAnnotation(
                errors, member, ((AnnotatedElement) member).getAnnotations());
        if (misplacedBindingAnnotation == null) {
            return;
        }

        // don't warn about misplaced binding annotations on methods when there's a field with the same
        // name. In Scala, fields always get accessor methods (that we need to ignore). See bug 242.
        if (member instanceof Method) {
            try {
                // 啥子 存在与方法同名的字段???
                if (member.getDeclaringClass().getField(member.getName()) != null) {
                    return;
                }
            } catch (NoSuchFieldException ignore) {
            }
        }

        errors.misplacedBindingAnnotation(member, misplacedBindingAnnotation);
    }

    /**
     *
     * @param type   待解析的目标类
     * @param factory  本次生成增强点的类型
     * @param statics  是否是静态属性/方法
     * @param injectionPoints  收集解析出来的所有增强点
     * @param errors
     * @param <M>
     */
    private static <M extends Member & AnnotatedElement> void addInjectionPoints(TypeLiteral<?> type,
                                                                                 Factory<M> factory, boolean statics,
                                                                                 Collection<InjectionPoint> injectionPoints,
                                                                                 Errors errors) {
        if (type.getType() == Object.class) {
            return;
        }

        // Add injectors for superclass first.
        // 从父类开始寻找
        TypeLiteral<?> superType = type.getSupertype(type.getRawType().getSuperclass());
        addInjectionPoints(superType, factory, statics, injectionPoints, errors);

        // Add injectors for all members next
        // 真正干活的是这个方法
        addInjectorsForMembers(type, factory, statics, injectionPoints, errors);
    }

    /**
     *
     * @param typeLiteral  当前待解析的类型
     * @param factory    标明本次解析的类型
     * @param statics   是否是静态属性/方法
     * @param injectionPoints   存储解析出来的增强点
     * @param errors
     * @param <M>
     */
    private static <M extends Member & AnnotatedElement> void addInjectorsForMembers(
            TypeLiteral<?> typeLiteral, Factory<M> factory, boolean statics,
            Collection<InjectionPoint> injectionPoints, Errors errors) {
        for (M member : factory.getMembers(getRawType(typeLiteral.getType()))) {
            // 根据是否只处理静态方法/字段 进行过滤
            if (isStatic(member) != statics) {
                continue;
            }

            // 套路都是一样的 关键就是检测是否包含@Inject注解
            Inject inject = member.getAnnotation(Inject.class);
            if (inject == null) {
                continue;
            }

            try {
                injectionPoints.add(factory.create(typeLiteral, member, errors));
            } catch (ConfigurationException ignorable) {
                if (!inject.optional()) {
                    errors.merge(ignorable.getErrorMessages());
                }
            }
        }
    }

    private static boolean isStatic(Member member) {
        return Modifier.isStatic(member.getModifiers());
    }

    /**
     * 代表解析的类型
     * @param <M>
     */
    private interface Factory<M extends Member & AnnotatedElement> {

        /**
         * 找到所有允许增强的field
         */
        Factory<Field> FIELDS = new Factory<Field>() {
            @Override
            public Field[] getMembers(Class<?> type) {
                return type.getFields();
            }

            /**
             * 将每个field都包装成一个注入点对象
             * @param typeLiteral
             * @param member
             * @param errors
             * @return
             */
            @Override
            public InjectionPoint create(TypeLiteral<?> typeLiteral, Field member, Errors errors) {
                return new InjectionPoint(typeLiteral, member);
            }
        };

        Factory<Method> METHODS = new Factory<Method>() {
            @Override
            public Method[] getMembers(Class<?> type) {
                return type.getMethods();
            }

            @Override
            public InjectionPoint create(TypeLiteral<?> typeLiteral, Method member, Errors errors) {
                checkForMisplacedBindingAnnotations(member, errors);
                return new InjectionPoint(typeLiteral, member);
            }
        };

        M[] getMembers(Class<?> type);

        InjectionPoint create(TypeLiteral<?> typeLiteral, M member, Errors errors);
    }
}
