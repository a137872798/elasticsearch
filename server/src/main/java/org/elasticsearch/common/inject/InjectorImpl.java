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

package org.elasticsearch.common.inject;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.elasticsearch.common.inject.internal.Annotations.findScopeAnnotation;

import org.elasticsearch.common.Classes;
import org.elasticsearch.common.inject.internal.Annotations;
import org.elasticsearch.common.inject.internal.BindingImpl;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InstanceBindingImpl;
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.internal.InternalFactory;
import org.elasticsearch.common.inject.internal.LinkedBindingImpl;
import org.elasticsearch.common.inject.internal.LinkedProviderBindingImpl;
import org.elasticsearch.common.inject.internal.MatcherAndConverter;
import org.elasticsearch.common.inject.internal.Scoping;
import org.elasticsearch.common.inject.internal.SourceProvider;
import org.elasticsearch.common.inject.internal.ToStringBuilder;
import org.elasticsearch.common.inject.spi.BindingTargetVisitor;
import org.elasticsearch.common.inject.spi.ConvertedConstantBinding;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.inject.spi.ProviderBinding;
import org.elasticsearch.common.inject.spi.ProviderKeyBinding;
import org.elasticsearch.common.inject.util.Providers;

import java.lang.annotation.Annotation;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default {@link Injector} implementation.
 *
 * @author crazybob@google.com (Bob Lee)
 * @see InjectorBuilder
 * 该对象负责完成整个注入工作
 */
class InjectorImpl implements Injector, Lookups {

    /**
     * 维护需要的各种信息
     */
    final State state;
    boolean readOnly;

    /**
     * 一对多的map
     */
    BindingsMultimap bindingsMultimap = new BindingsMultimap();

    /**
     * 实际上一般的待注入信息存储在这里
     */
    final Initializer initializer;

    /**
     * Just-in-time binding cache. Guarded by state.lock()
     * 专门用于存储 在运行时创建 provider对象
     */
    Map<Key<?>, BindingImpl<?>> jitBindings = new HashMap<>();

    Lookups lookups = new DeferredLookups(this);

    private final ThreadLocal<Object[]> localContext;

    /**
     * Cached constructor injectors for each type
     */
    ConstructorInjectorStore constructors = new ConstructorInjectorStore(this);

    /**
     * Cached field and method injectors for each type.
     */
    MembersInjectorStore membersInjectorStore;

    /**
     * @param state       包含调用过程中需要的各种参数
     * @param initializer 该对象负责维护所有的待注入的实例
     */
    InjectorImpl(State state, Initializer initializer) {
        this.state = state;
        this.initializer = initializer;
        localContext = new ThreadLocal<>();
    }

    /**
     * Indexes bindings by type.
     * 遍历此时存储在state中的所有映射关系 并存储到 bindingsMultimap
     */
    void index() {
        for (Binding<?> binding : state.getExplicitBindingsThisLevel().values()) {
            index(binding);
        }
    }

    /**
     * 将精确的绑定关系追加到 多绑定关系容器中
     * 在state中 binding的存储是以key 作为 map.key 的 也就是typeLiteral + annotationStrategy 所以在这层以 typeLiteral 为key 就可能会存在很多value
     *
     * @param binding
     * @param <T>
     */
    <T> void index(Binding<T> binding) {
        bindingsMultimap.put(binding.getKey().getTypeLiteral(), binding);
    }

    /**
     * 返回type 对应的所有绑定关系
     *
     * @param type
     * @param <T>
     * @return
     */
    @Override
    public <T> List<Binding<T>> findBindingsByType(TypeLiteral<T> type) {
        return bindingsMultimap.getAll(type);
    }

    /**
     * Gets a binding implementation.  First, it check to see if the parent has a binding.  If the
     * parent has a binding and the binding is scoped, it will use that binding.  Otherwise, this
     * checks for an explicit binding. If no explicit binding is found, it looks for a just-in-time
     * binding.
     * 通过key 找到对应的绑定类
     */
    public <T> BindingImpl<T> getBindingOrThrow(Key<T> key, Errors errors)
        throws ErrorsException {
        // Check explicit bindings, i.e. bindings created by modules.
        // 这个函数跟 getExplicitBindingsThisLevel 的区别是 当前state如果没有找到绑定关系会尝试从parent中继续查找绑定关系
        BindingImpl<T> binding = state.getExplicitBinding(key);
        if (binding != null) {
            return binding;
        }

        // Look for an on-demand binding.
        // 代表绑定关系没有提前建立 选择即时创建 (jit)
        return getJustInTimeBinding(key, errors);
    }

    /**
     * Returns a just-in-time binding for {@code key}, creating it if necessary.
     *
     * @throws ErrorsException if the binding could not be created.
     *                         <p>
     *                         生成 jit绑定对象
     */
    private <T> BindingImpl<T> getJustInTimeBinding(Key<T> key, Errors errors)
        throws ErrorsException {
        synchronized (state.lock()) {
            // first try to find a JIT binding that we've already created
            @SuppressWarnings("unchecked") // we only store bindings that match their key
                // 使用缓存 避免重复创建对象
                BindingImpl<T> binding = (BindingImpl<T>) jitBindings.get(key);

            if (binding != null) {
                return binding;
            }

            return createJustInTimeBindingRecursive(key, errors);
        }
    }

    /**
     * Returns true if the key type is Provider (but not a subclass of Provider).
     */
    static boolean isProvider(Key<?> key) {
        return key.getTypeLiteral().getRawType().equals(Provider.class);
    }

    /**
     * Returns true if the key type is MembersInjector (but not a subclass of MembersInjector).
     */
    static boolean isMembersInjector(Key<?> key) {
        return key.getTypeLiteral().getRawType().equals(MembersInjector.class)
            && !key.hasAnnotationType();
    }

    /**
     * 创建 MembersInjector 的绑定对象
     *
     * @param key
     * @param errors
     * @param <T>
     * @return
     * @throws ErrorsException
     */
    private <T> BindingImpl<MembersInjector<T>> createMembersInjectorBinding(
        Key<MembersInjector<T>> key, Errors errors) throws ErrorsException {
        Type membersInjectorType = key.getTypeLiteral().getType();
        // 必须是 MembersInjector<T> 而不是 MembersInjector
        if (!(membersInjectorType instanceof ParameterizedType)) {
            throw errors.cannotInjectRawMembersInjector().toException();
        }

        @SuppressWarnings("unchecked") // safe because T came from Key<MembersInjector<T>>   获取T类型 并生成TypeLiteral
            TypeLiteral<T> instanceType = (TypeLiteral<T>) TypeLiteral.get(
            ((ParameterizedType) membersInjectorType).getActualTypeArguments()[0]);
        MembersInjector<T> membersInjector = membersInjectorStore.get(instanceType, errors);

        // 他跟 Key<Provider<T>> 的逻辑不一样   这里将直接根据membersInjector 生成BindingImpl
        InternalFactory<MembersInjector<T>> factory = new ConstantFactory<>(
            Initializables.of(membersInjector));

        // bindingImpl 代表包含构造器注入 而MembersInjector 仅包含成员变量注入 以及方法注入
        // InstanceBindingImpl 代表每次注入的都是同一个实例 也就是自带单例模式
        return new InstanceBindingImpl<>(this, key, SourceProvider.UNKNOWN_SOURCE,
            factory, emptySet(), membersInjector);
    }

    /**
     * Creates a synthetic binding to {@code Provider<T>}, i.e. a binding to the provider from
     * {@code Binding<T>}.
     * 当 key是provider类型时 生成bindingImpl 对象
     */
    private <T> BindingImpl<Provider<T>> createProviderBinding(Key<Provider<T>> key, Errors errors)
        throws ErrorsException {
        // 获取 provider<T>
        Type providerType = key.getTypeLiteral().getType();

        // If the Provider has no type parameter (raw Provider)...
        // 必须是 Provider<T>  而不是 provider
        if (!(providerType instanceof ParameterizedType)) {
            throw errors.cannotInjectRawProvider().toException();
        }

        // 获取T的类型
        Type entryType = ((ParameterizedType) providerType).getActualTypeArguments()[0];

        @SuppressWarnings("unchecked")
        // safe because T came from Key<Provider<T>>
        // 将 T 类型包装成 Key<T>
        Key<T> providedKey = (Key<T>) key.ofType(entryType);

        // 生成binding对象
        BindingImpl<T> delegate = getBindingOrThrow(providedKey, errors);
        return new ProviderBindingImpl<>(this, key, delegate);
    }

    static class ProviderBindingImpl<T> extends BindingImpl<Provider<T>>
        implements ProviderBinding<Provider<T>> {
        final BindingImpl<T> providedBinding;

        ProviderBindingImpl(InjectorImpl injector, Key<Provider<T>> key, Binding<T> providedBinding) {
            super(injector, key, providedBinding.getSource(), createInternalFactory(providedBinding),
                Scoping.UNSCOPED);
            this.providedBinding = (BindingImpl<T>) providedBinding;
        }

        static <T> InternalFactory<Provider<T>> createInternalFactory(Binding<T> providedBinding) {
            final Provider<T> provider = providedBinding.getProvider();
            return new InternalFactory<Provider<T>>() {
                @Override
                public Provider<T> get(Errors errors, InternalContext context, Dependency dependency) {
                    return provider;
                }
            };
        }

        @Override
        public Key<? extends T> getProvidedKey() {
            return providedBinding.getKey();
        }

        @Override
        public <V> V acceptTargetVisitor(BindingTargetVisitor<? super Provider<T>, V> visitor) {
            return visitor.visit(this);
        }

        @Override
        public void applyTo(Binder binder) {
            throw new UnsupportedOperationException("This element represents a synthetic binding.");
        }

        @Override
        public String toString() {
            return new ToStringBuilder(ProviderKeyBinding.class)
                .add("key", getKey())
                .add("providedKey", getProvidedKey())
                .toString();
        }
    }

    /**
     * Converts a constant string binding to the required type.
     *
     * @return the binding if it could be resolved, or null if the binding doesn't exist
     * @throws org.elasticsearch.common.inject.internal.ErrorsException if there was an error resolving the binding
     * 代表要注入的是string类型
     */
    private <T> BindingImpl<T> convertConstantStringBinding(Key<T> key, Errors errors)
        throws ErrorsException {
        // Find a constant string binding.
        Key<String> stringKey = key.ofType(String.class);
        // 检测是否已经存在binding了 如果不存在直接返回null
        BindingImpl<String> stringBinding = state.getExplicitBinding(stringKey);
        if (stringBinding == null || !stringBinding.isConstant()) {
            return null;
        }

        // 代表在state中 一开始就内置了 string常量
        String stringValue = stringBinding.getProvider().get();
        Object source = stringBinding.getSource();

        // Find a matching type converter.
        TypeLiteral<T> type = key.getTypeLiteral();
        // 寻找对应的转换器
        MatcherAndConverter matchingConverter = state.getConverter(stringValue, type, errors, source);

        // 代表根据 字面量 以及 type 没有找到对应的转换器   这时放弃基于string常量进行匹配
        if (matchingConverter == null) {
            // No converter can handle the given type.
            return null;
        }

        // Try to convert the string. A failed conversion results in an error.
        try {
            @SuppressWarnings("unchecked") // This cast is safe because we double check below.
                // 尝试将字面量转换成 type对应的实例对象
                T converted = (T) matchingConverter.getTypeConverter().convert(stringValue, type);

            if (converted == null) {
                throw errors.converterReturnedNull(stringValue, source, type, matchingConverter)
                    .toException();
            }

            // 转换的类型不匹配 则抛出异常
            if (!type.getRawType().isInstance(converted)) {
                throw errors.conversionTypeError(stringValue, source, type, matchingConverter, converted)
                    .toException();
            }

            // 代表基于转换器 返回的注入实例 同时返回的是一个常量(单例)
            return new ConvertedConstantBindingImpl<>(this, key, converted, stringBinding);
        } catch (ErrorsException e) {
            throw e;
        } catch (RuntimeException e) {
            throw errors.conversionError(stringValue, source, type, matchingConverter, e)
                .toException();
        }
    }

    /**
     * 该对象代表是将一个string常量值转换过来的
     *
     * @param <T>
     */
    private static class ConvertedConstantBindingImpl<T>
        extends BindingImpl<T> implements ConvertedConstantBinding<T> {
        final T value;
        final Provider<T> provider;
        /**
         * 代表提供转换前数据的 binding
         */
        final Binding<String> originalBinding;


        /**
         * @param injector        注入器对象
         * @param key             对应转换后的类型信息
         * @param value           将字面量转换后的结果
         * @param originalBinding 字面量对应的绑定对象
         */
        ConvertedConstantBindingImpl(
            Injector injector, Key<T> key, T value, Binding<String> originalBinding) {
            super(injector, key, originalBinding.getSource(),
                new ConstantFactory<>(Initializables.of(value)), Scoping.UNSCOPED);
            this.value = value;
            provider = Providers.of(value);
            this.originalBinding = originalBinding;
        }

        @Override
        public Provider<T> getProvider() {
            return provider;
        }

        @Override
        public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
            return visitor.visit(this);
        }

        @Override
        public T getValue() {
            return value;
        }

        @Override
        public Key<String> getSourceKey() {
            return originalBinding.getKey();
        }

        @Override
        public Set<Dependency<?>> getDependencies() {
            return singleton(Dependency.get(getSourceKey()));
        }

        @Override
        public void applyTo(Binder binder) {
            throw new UnsupportedOperationException("This element represents a synthetic binding.");
        }

        @Override
        public String toString() {
            return new ToStringBuilder(ConvertedConstantBinding.class)
                .add("key", getKey())
                .add("sourceKey", getSourceKey())
                .add("value", value)
                .toString();
        }
    }

    /**
     * 为binding 做初始化工作
     * @param binding
     * @param errors
     * @param <T>
     * @throws ErrorsException
     */
    <T> void initializeBinding(BindingImpl<T> binding, Errors errors) throws ErrorsException {
        // Put the partially constructed binding in the map a little early. This enables us to handle
        // circular dependencies. Example: FooImpl -> BarImpl -> FooImpl.
        // Note: We don't need to synchronize on state.lock() during injector creation.
        // TODO: for the above example, remove the binding for BarImpl if the binding for FooImpl fails
        if (binding instanceof ConstructorBindingImpl<?>) {
            Key<T> key = binding.getKey();
            jitBindings.put(key, binding);
            boolean successful = false;
            try {
                // 如果是基于构造器的类型  初始化constructorInjector
                ((ConstructorBindingImpl) binding).initialize(this, errors);
                successful = true;
            } finally {
                if (!successful) {
                    jitBindings.remove(key);
                }
            }
        }
    }

    /**
     * Creates a binding for an injectable type with the given scope. Looks for a scope on the type if
     * none is specified.
     * @param scoping 默认没有范围信息
     * 尝试为source 生成binding 对象
     */
    <T> BindingImpl<T> createUnitializedBinding(Key<T> key, Scoping scoping, Object source,
                                                Errors errors) throws ErrorsException {
        Class<?> rawType = key.getTypeLiteral().getRawType();

        // Don't try to inject arrays, or enums.
        // 不支持为 数组和枚举类型注入
        if (rawType.isArray() || rawType.isEnum()) {
            throw errors.missingImplementation(key).toException();
        }

        // Handle TypeLiteral<T> by binding the inner type
        // 如果待注入的类型时 TypeLiteral<T> 生成对应的binding对象  这种算是特殊情况 先忽略
        if (rawType == TypeLiteral.class) {
            @SuppressWarnings("unchecked") // we have to fudge the inner type as Object
                BindingImpl<T> binding = (BindingImpl<T>) createTypeLiteralBinding(
                (Key<TypeLiteral<Object>>) key, errors);
            return binding;
        }

        // Handle @ImplementedBy
        // 检查目标类型是否被 @ImplementedBy 注解修饰 是的话 默认使用该实现类来注入
        ImplementedBy implementedBy = rawType.getAnnotation(ImplementedBy.class);
        if (implementedBy != null) {
            // 使用@ImplementedBy 注解的场景   代表认为该rawType有多个实现类 此时不能使用被ScopeAnnotation标记的注解
            Annotations.checkForMisplacedScopeAnnotations(rawType, source, errors);
            return createImplementedByBinding(key, scoping, implementedBy, errors);
        }

        // Handle @ProvidedBy.
        // 处理 providedBy
        ProvidedBy providedBy = rawType.getAnnotation(ProvidedBy.class);
        if (providedBy != null) {
            Annotations.checkForMisplacedScopeAnnotations(rawType, source, errors);
            return createProvidedByBinding(key, scoping, providedBy, errors);
        }

        // We can't inject abstract classes.
        // TODO: Method interceptors could actually enable us to implement
        // abstract types. Should we remove this restriction?
        // 无法为抽象类进行注入
        if (Modifier.isAbstract(rawType.getModifiers())) {
            throw errors.missingImplementation(key).toException();
        }

        // Error: Inner class.
        // 无法为内部类进行注入
        if (Classes.isInnerClass(rawType)) {
            throw errors.cannotInjectInnerClass(rawType).toException();
        }

        // 如果包含范围信息
        if (!scoping.isExplicitlyScoped()) {
            // 寻找被 @ScopeAnnotation 修饰的注解类
            Class<? extends Annotation> scopeAnnotation = findScopeAnnotation(errors, rawType);
            if (scopeAnnotation != null) {
                // Scoping 定义了一个可以被visitor处理的范围信息
                scoping = Scopes.makeInjectable(Scoping.forAnnotation(scopeAnnotation),
                    this, errors.withSource(rawType));
            }
        }

        // key 照理说一定是实例类型啊 否则没有构造函数 再获取有关构造器的注入点时 会提示 NoSuchMethodException
        return ConstructorBindingImpl.create(this, key, source, scoping);
    }

    /**
     * Converts a binding for a {@code Key<TypeLiteral<T>>} to the value {@code TypeLiteral<T>}. It's
     * a bit awkward because we have to pull out the inner type in the type literal.
     * 当 key内部的类型是TypeLiteral 时 创建对应的 BindingImpl 对象   该banding对象仅返回一个TypeLiteral  这算是特殊情况 所以先忽略
     */
    private <T> BindingImpl<TypeLiteral<T>> createTypeLiteralBinding(
        Key<TypeLiteral<T>> key, Errors errors) throws ErrorsException {
        Type typeLiteralType = key.getTypeLiteral().getType();
        // 确保是 TypeLiteral<T> 而不是 TypeLiteral
        if (!(typeLiteralType instanceof ParameterizedType)) {
            throw errors.cannotInjectRawTypeLiteral().toException();
        }

        ParameterizedType parameterizedType = (ParameterizedType) typeLiteralType;
        Type innerType = parameterizedType.getActualTypeArguments()[0];

        // this is unfortunate. We don't support building TypeLiterals for type variable like 'T'. If
        // this proves problematic, we can probably fix TypeLiteral to support type variables
        // 要求泛型是 显式的 T类型 比如 String  或者 T[] 或者 List<T> 但是不能直接是 T
        if (!(innerType instanceof Class)
            && !(innerType instanceof GenericArrayType)
            && !(innerType instanceof ParameterizedType)) {
            throw errors.cannotInjectTypeLiteralOf(innerType).toException();
        }

        @SuppressWarnings("unchecked") // by definition, innerType == T, so this is safe
            TypeLiteral<T> value = (TypeLiteral<T>) TypeLiteral.get(innerType);
        InternalFactory<TypeLiteral<T>> factory = new ConstantFactory<>(
            Initializables.of(value));
        return new InstanceBindingImpl<>(this, key, SourceProvider.UNKNOWN_SOURCE,
            factory, emptySet(), value);
    }

    /**
     * Creates a binding for a type annotated with @ProvidedBy.
     * 生成基于 @ProvidedBy 注解的binding实现类
     */
    <T> BindingImpl<T> createProvidedByBinding(Key<T> key, Scoping scoping,
                                               ProvidedBy providedBy, Errors errors) throws ErrorsException {
        final Class<?> rawType = key.getTypeLiteral().getRawType();
        final Class<? extends Provider<?>> providerType = providedBy.value();

        // Make sure it's not the same type. TODO: Can we check for deeper loops?
        if (providerType == rawType) {
            throw errors.recursiveProviderType().toException();
        }

        // Assume the provider provides an appropriate type. We double check at runtime.
        @SuppressWarnings("unchecked") final Key<? extends Provider<T>> providerKey
            = (Key<? extends Provider<T>>) Key.get(providerType);
        // 先根据实现类类型找到 提供实例对象的binding
        final BindingImpl<? extends Provider<?>> providerBinding
            = getBindingOrThrow(providerKey, errors);

        InternalFactory<T> internalFactory = new InternalFactory<T>() {
            @Override
            public T get(Errors errors, InternalContext context, Dependency dependency)
                throws ErrorsException {
                errors = errors.withSource(providerKey);
                Provider<?> provider = providerBinding.getInternalFactory().get(
                    errors, context, dependency);
                try {
                    Object o = provider.get();
                    if (o != null && !rawType.isInstance(o)) {
                        throw errors.subtypeNotProvided(providerType, rawType).toException();
                    }
                    @SuppressWarnings("unchecked") // protected by isInstance() check above
                        T t = (T) o;
                    return t;
                } catch (RuntimeException e) {
                    throw errors.errorInProvider(e).toException();
                }
            }
        };

        // 生成基于代理模式的binding
        return new LinkedProviderBindingImpl<>(
            this,
            key,
            rawType /* source */,
            Scopes.<T>scope(key, this, internalFactory, scoping),
            scoping,
            providerKey);
    }

    /**
     * Creates a binding for a type annotated with @ImplementedBy.
     * 根据指定的注入类 生成 binding对象
     */
    <T> BindingImpl<T> createImplementedByBinding(Key<T> key, Scoping scoping,
                                                  ImplementedBy implementedBy, Errors errors)
        throws ErrorsException {
        Class<?> rawType = key.getTypeLiteral().getRawType();
        Class<?> implementationType = implementedBy.value();

        // Make sure it's not the same type. TODO: Can we check for deeper cycles?
        if (implementationType == rawType) {
            throw errors.recursiveImplementationType().toException();
        }

        // Make sure implementationType extends type.
        // 确保 ImplementedBy 对应的类型确实是 key的实现类
        if (!rawType.isAssignableFrom(implementationType)) {
            throw errors.notASubtype(implementationType, rawType).toException();
        }

        @SuppressWarnings("unchecked") // After the preceding check, this cast is safe.
            Class<? extends T> subclass = (Class<? extends T>) implementationType;

        // Look up the target binding.
        final Key<? extends T> targetKey = Key.get(subclass);
        // 将当前类包装成key 去ioc容器中查找 并封装成binding 对象
        final BindingImpl<? extends T> targetBinding = getBindingOrThrow(targetKey, errors);

        // 通过代理模式 生成实例
        InternalFactory<T> internalFactory = new InternalFactory<T>() {
            @Override
            public T get(Errors errors, InternalContext context, Dependency<?> dependency)
                throws ErrorsException {
                return targetBinding.getInternalFactory().get(
                    errors.withSource(targetKey), context, dependency);
            }
        };

        return new LinkedBindingImpl<>(
            this,
            key,
            rawType /* source */,
            Scopes.<T>scope(key, this, internalFactory, scoping),
            scoping,
            targetKey);
    }

    /**
     * Attempts to create a just-in-time binding for {@code key} in the root injector, falling back to
     * other ancestor injectors until this injector is tried.
     * 代表绑定关系没有提前建立 此时在使用某个对象 并发现需要注入依赖时 就创建jit对象
     */
    private <T> BindingImpl<T> createJustInTimeBindingRecursive(Key<T> key, Errors errors)
        throws ErrorsException {
        // key存在与黑名单中 抛出异常
        if (state.isBlacklisted(key)) {
            throw errors.childBindingAlreadySet(key).toException();
        }

        BindingImpl<T> binding = createJustInTimeBinding(key, errors);
        // 将key存储到黑名单中 避免父对象重复创建
        state.parent().blacklist(key);
        // 将关联关系存储到 jitBindings中
        jitBindings.put(key, binding);
        return binding;
    }

    /**
     * Returns a new just-in-time binding created by resolving {@code key}. The strategies used to
     * create just-in-time bindings are:
     * <ol>
     * <li>Internalizing Providers. If the requested binding is for {@code Provider<T>}, we delegate
     * to the binding for {@code T}.
     * <li>Converting constants.
     * <li>ImplementedBy and ProvidedBy annotations. Only for unannotated keys.
     * <li>The constructor of the raw type. Only for unannotated keys.
     * </ol>
     *
     * @throws org.elasticsearch.common.inject.internal.ErrorsException if the binding cannot be created.
     *                                                                  因为之前没有创建绑定对象 所以此时创建即时对象
     */
    <T> BindingImpl<T> createJustInTimeBinding(Key<T> key, Errors errors) throws ErrorsException {
        if (state.isBlacklisted(key)) {
            throw errors.childBindingAlreadySet(key).toException();
        }

        // Handle cases where T is a Provider<?>.
        // 如果key的类型是 Provider
        if (isProvider(key)) {
            // These casts are safe. We know T extends Provider<X> and that given Key<Provider<X>>,
            // createProviderBinding() will return BindingImpl<Provider<X>>.
            @SuppressWarnings("unchecked")
            // 针对provider 的情况 需要对provider进行解包装 也就是仅生成 T 对应的bindingImpl对象
            BindingImpl binding = createProviderBinding((Key) key, errors);
            return binding;
        }

        // Handle cases where T is a MembersInjector<?>
        // 如果key类型为 MembersInjector  这里还关系 key的 annotationStrategy 是否为空
        if (isMembersInjector(key)) {
            // These casts are safe. T extends MembersInjector<X> and that given Key<MembersInjector<X>>,
            // createMembersInjectorBinding() will return BindingImpl<MembersInjector<X>>.
            @SuppressWarnings("unchecked")
            // 处理 MembersInjector 类型的 Key
            BindingImpl binding = createMembersInjectorBinding((Key) key, errors);
            return binding;
        }

        // Try to convert a constant string binding to the requested type.
        // 尝试检测state中是否内置了 绑定string常量  如果是的话 将常量取出来 并按照key与matcher进行匹配 之后将string 通过匹配上的converter进行转换
        BindingImpl<T> convertedBinding = convertConstantStringBinding(key, errors);
        if (convertedBinding != null) {
            return convertedBinding;
        }

        // If the key has an annotation..
        // 当描述本次待注入的类型上 包含一个携带 @BindingAnnotation 注解
        // TODO 这种情况先忽略吧
        if (key.hasAnnotationType()) {
            // Look for a binding without annotation attributes or return null.
            // 代表注解本身还包含一些方法
            if (key.hasAttributes()) {
                try {
                    Errors ignored = new Errors();
                    // 将annotationStrategy 内部的属性去除后 以新的 annotationStrategy 去获取binding
                    return getBindingOrThrow(key.withoutAttributes(), ignored);
                } catch (ErrorsException ignored) {
                    // throw with a more appropriate message below
                }
            }
            // 不包含方法的空注解 直接抛出异常
            throw errors.missingImplementation(key).toException();
        }

        // 根据 本次待注入的key类型获取bindingImpl 对象
        Object source = key.getTypeLiteral().getRawType();
        BindingImpl<T> binding = createUnitializedBinding(key, Scoping.UNSCOPED, source, errors);
        initializeBinding(binding, errors);
        return binding;
    }

    /**
     * 找到为某个key 提供provider 的工厂
     *
     * @param key
     * @param errors
     * @param <T>
     * @return
     * @throws ErrorsException
     */
    <T> InternalFactory<? extends T> getInternalFactory(Key<T> key, Errors errors)
        throws ErrorsException {
        return getBindingOrThrow(key, errors).getInternalFactory();
    }

    /**
     * 因为每个表示接口的 key 或者 TypeLiteral 都可以有多个实现类 所以容器内是一对多的关系
     */
    private static class BindingsMultimap {

        /**
         * 每个 待注入的类型 可能会有多个实现类
         */
        final Map<TypeLiteral<?>, List<Binding<?>>> multimap = new HashMap<>();

        /**
         * 添加一组 映射关系
         *
         * @param type
         * @param binding
         * @param <T>
         */
        <T> void put(TypeLiteral<T> type, Binding<T> binding) {
            List<Binding<?>> bindingsForType = multimap.get(type);
            if (bindingsForType == null) {
                bindingsForType = new ArrayList<>();
                multimap.put(type, bindingsForType);
            }
            bindingsForType.add(binding);
        }


        /**
         * safe because we only put matching entries into the map
         * 获取某个类型下所有的绑定关系
         *
         * @param type
         * @param <T>
         * @return
         */
        @SuppressWarnings("unchecked")
        <T> List<Binding<T>> getAll(TypeLiteral<T> type) {
            List<Binding<?>> bindings = multimap.get(type);
            return bindings != null
                ? Collections.<Binding<T>>unmodifiableList((List) multimap.get(type))
                : Collections.<Binding<T>>emptyList();
        }
    }

    /**
     * Returns parameter injectors, or {@code null} if there are no parameters.
     *
     * @param parameters 将每个参数信息包装成一个 基于参数的注入点对象
     */
    SingleParameterInjector<?>[] getParametersInjectors(
        List<Dependency<?>> parameters, Errors errors) throws ErrorsException {
        if (parameters.isEmpty()) {
            return null;
        }

        int numErrorsBefore = errors.size();
        // 针对方法级的注入点 每个参数都会对应一个 dependency对象
        SingleParameterInjector<?>[] result = new SingleParameterInjector<?>[parameters.size()];
        int i = 0;
        for (Dependency<?> parameter : parameters) {
            try {
                // 创建注入点
                result[i++] = createParameterInjector(parameter, errors.withSource(parameter));
            } catch (ErrorsException rethrownBelow) {
                // rethrown below
            }
        }

        errors.throwIfNewErrors(numErrorsBefore);
        return result;
    }

    /**
     * 创建参数相关的注入点对象
     *
     * @param dependency 这个依赖点一般key是内置了 @BindingAnnotation的注解
     * @param errors
     * @param <T>
     * @return
     * @throws ErrorsException
     */
    <T> SingleParameterInjector<T> createParameterInjector(final Dependency<T> dependency,
                                                           final Errors errors) throws ErrorsException {
        // dependency.key 对应的是参数类型  从ioc容器中 获取对应该key的 provider工厂对象
        InternalFactory<? extends T> factory = getInternalFactory(dependency.getKey(), errors);
        return new SingleParameterInjector<>(dependency, factory);
    }

    /**
     * Invokes a method.
     */
    interface MethodInvoker {
        Object invoke(Object target, Object... parameters)
            throws IllegalAccessException, InvocationTargetException;
    }



    @Override
    @SuppressWarnings("unchecked") // the members injector type is consistent with instance's type
    public void injectMembers(Object instance) {
        MembersInjector membersInjector = getMembersInjector(instance.getClass());
        membersInjector.injectMembers(instance);
    }

    @Override
    public <T> MembersInjector<T> getMembersInjector(TypeLiteral<T> typeLiteral) {
        Errors errors = new Errors(typeLiteral);
        try {
            return membersInjectorStore.get(typeLiteral, errors);
        } catch (ErrorsException e) {
            throw new ConfigurationException(errors.merge(e.getErrors()).getMessages());
        }
    }

    @Override
    public <T> MembersInjector<T> getMembersInjector(Class<T> type) {
        return getMembersInjector(TypeLiteral.get(type));
    }

    @Override
    public <T> Provider<T> getProvider(Class<T> type) {
        return getProvider(Key.get(type));
    }

    <T> Provider<T> getProviderOrThrow(final Key<T> key, Errors errors) throws ErrorsException {
        final InternalFactory<? extends T> factory = getInternalFactory(key, errors);
        // ES: optimize for a common case of read only instance getting from the parent...
        if (factory instanceof InternalFactory.Instance) {
            return new Provider<T>() {
                @Override
                public T get() {
                    try {
                        return (T) ((InternalFactory.Instance) factory).get(null, null, null);
                    } catch (ErrorsException e) {
                        // ignore
                    }
                    // should never happen...
                    assert false;
                    return null;
                }
            };
        }

        final Dependency<T> dependency = Dependency.get(key);
        return new Provider<T>() {
            @Override
            public T get() {
                final Errors errors = new Errors(dependency);
                try {
                    T t = callInContext(new ContextualCallable<T>() {
                        @Override
                        public T call(InternalContext context) throws ErrorsException {
                            context.setDependency(dependency);
                            try {
                                return factory.get(errors, context, dependency);
                            } finally {
                                context.setDependency(null);
                            }
                        }
                    });
                    errors.throwIfNewErrors(0);
                    return t;
                } catch (ErrorsException e) {
                    throw new ProvisionException(errors.merge(e.getErrors()).getMessages());
                }
            }

            @Override
            public String toString() {
                return factory.toString();
            }
        };
    }

    @Override
    public <T> Provider<T> getProvider(final Key<T> key) {
        Errors errors = new Errors(key);
        try {
            Provider<T> result = getProviderOrThrow(key, errors);
            errors.throwIfNewErrors(0);
            return result;
        } catch (ErrorsException e) {
            throw new ConfigurationException(errors.merge(e.getErrors()).getMessages());
        }
    }

    @Override
    public <T> T getInstance(Key<T> key) {
        return getProvider(key).get();
    }

    @Override
    public <T> T getInstance(Class<T> type) {
        return getProvider(type).get();
    }

    /**
     * Looks up thread local context. Creates (and removes) a new context if necessary.
     */
    <T> T callInContext(ContextualCallable<T> callable) throws ErrorsException {
        Object[] reference = localContext.get();
        if (reference == null) {
            reference = new Object[1];
            localContext.set(reference);
        }
        if (reference[0] == null) {
            reference[0] = new InternalContext();
            try {
                return callable.call((InternalContext) reference[0]);
            } finally {
                // Only clear the context if this call created it.
                reference[0] = null;
            }
        } else {
            // Someone else will clean up this context.
            return callable.call((InternalContext) reference[0]);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(Injector.class)
            .add("bindings", state.getExplicitBindingsThisLevel().values())
            .toString();
    }

    // ES_GUICE: clear caches
    // 重置内部属性
    public void clearCache() {
        state.clearBlacklisted();
        constructors = new ConstructorInjectorStore(this);
        membersInjectorStore = new MembersInjectorStore(this, state.getTypeListenerBindings());
        jitBindings = new HashMap<>();
    }

    // ES_GUICE: make all registered bindings act as eager singletons
    public void readOnlyAllSingletons() {
        readOnly = true;
        state.makeAllBindingsToEagerSingletons(this);
        // 更新binding信息后重新设置
        bindingsMultimap = new BindingsMultimap();
        // reindex the bindings
        index();
    }
}
