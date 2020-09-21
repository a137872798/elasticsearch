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

import org.elasticsearch.common.inject.internal.Annotations;
import org.elasticsearch.common.inject.internal.BindingImpl;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.ExposedBindingImpl;
import org.elasticsearch.common.inject.internal.InstanceBindingImpl;
import org.elasticsearch.common.inject.internal.InternalFactory;
import org.elasticsearch.common.inject.internal.LinkedBindingImpl;
import org.elasticsearch.common.inject.internal.LinkedProviderBindingImpl;
import org.elasticsearch.common.inject.internal.ProviderInstanceBindingImpl;
import org.elasticsearch.common.inject.internal.ProviderMethod;
import org.elasticsearch.common.inject.internal.Scoping;
import org.elasticsearch.common.inject.internal.UntargettedBindingImpl;
import org.elasticsearch.common.inject.spi.BindingTargetVisitor;
import org.elasticsearch.common.inject.spi.ConstructorBinding;
import org.elasticsearch.common.inject.spi.ConvertedConstantBinding;
import org.elasticsearch.common.inject.spi.ExposedBinding;
import org.elasticsearch.common.inject.spi.InjectionPoint;
import org.elasticsearch.common.inject.spi.InstanceBinding;
import org.elasticsearch.common.inject.spi.LinkedKeyBinding;
import org.elasticsearch.common.inject.spi.PrivateElements;
import org.elasticsearch.common.inject.spi.ProviderBinding;
import org.elasticsearch.common.inject.spi.ProviderInstanceBinding;
import org.elasticsearch.common.inject.spi.ProviderKeyBinding;
import org.elasticsearch.common.inject.spi.UntargettedBinding;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.util.set.Sets.newHashSet;

/**
 * Handles {@link Binder#bind} and {@link Binder#bindConstant} elements.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 * 处理绑定请求的 处理器
 */
class BindingProcessor extends AbstractProcessor {

    /**
     * 内部维护一组监听器  用于监听bean的创建
     */
    private final List<CreationListener> creationListeners = new ArrayList<>();
    /**
     * Initializer 相当于注入的开关
     */
    private final Initializer initializer;
    /**
     * 该组任务主要是针对 基于构造函数生成bean对象的binding  因为构造函数的参数也来自于ioc容器
     */
    private final List<Runnable> uninitializedBindings = new ArrayList<>();

    BindingProcessor(Errors errors, Initializer initializer) {
        super(errors);
        this.initializer = initializer;
    }

    /**
     * 处理用户之前在module中设置的绑定关系
     * @param command
     * @param <T>
     * @return
     */
    @Override
    public <T> Boolean visit(Binding<T> command) {
        final Object source = command.getSource();

        if (Void.class.equals(command.getKey().getRawType())) {
            if (command instanceof ProviderInstanceBinding
                    && ((ProviderInstanceBinding<?>) command).getProviderInstance() instanceof ProviderMethod) {
                errors.voidProviderMethod();
            } else {
                errors.missingConstantValues();
            }
            return true;
        }

        final Key<T> key = command.getKey();
        // 获取注入的原始类型
        Class<? super T> rawType = key.getTypeLiteral().getRawType();

        // 不允许绑定 Provider
        if (rawType == Provider.class) {
            errors.bindingToProvider();
            return true;
        }

        // 描述范围的注解不应该设置在 key对应的rawType上
        validateKey(command.getSource(), command.getKey());

        // 通过scoping 在state中找到对应的 scope
        final Scoping scoping = Scopes.makeInjectable(
                ((BindingImpl<?>) command).getScoping(), injector, errors);

        // 这里细化到不同的binding实现类 并进行处理
        command.acceptTargetVisitor(new BindingTargetVisitor<T, Void>() {

            // 可以看到创建实例与注入属性是2个阶段  其他考虑属性的注入可能会导致循环依赖问题  所以先确保尽可能在ioc中注册了所有可用的binding后 才考虑为实例进行属性注入

            /**
             * 处理下游绑定实例的 binding对象
             * @param binding
             * @return
             */
            @Override
            public Void visit(InstanceBinding<? extends T> binding) {
                // 如果这个实例本身还有其他需要注入的属性 (或者说依赖于其他bean)
                Set<InjectionPoint> injectionPoints = binding.getInjectionPoints();
                // 获取实例对象
                T instance = binding.getInstance();
                // 针对实例请求进行注入  (此时还没有真正注入 只是设置一个待注入的任务)
                Initializable<T> ref = initializer.requestInjection(
                        injector, instance, source, injectionPoints);
                // 使用常量工厂包裹该 ref 当外部线程调用get时 就是触发ref.get() 此时会阻塞直到注入任务完成
                // 而本线程执行任务就是触发注入动作
                ConstantFactory<? extends T> factory = new ConstantFactory<>(ref);
                // 检测该binding关系是否设置了 scope 设置的话使用该对象加工 provider  比如单例模式就是在包装后的工厂中直接内置实例引用 并在之后的使用中直接返回之前的实例引用
                // 实际上InstanceBinding 是默认单例的 因为返回的总是一开始设置好的instance
                InternalFactory<? extends T> scopedFactory = Scopes.scope(key, injector, factory, scoping);

                /**
                 * 那么实际上在visit中只做了2件事
                 * 1.将原本的binding对象包装成 ref对象
                 * 2.使用scope信息加工工厂
                 */
                putBinding(new InstanceBindingImpl<>(injector, key, source, scopedFactory, injectionPoints,
                        instance));
                return null;
            }

            /**
             * 处理ProviderInstanceBinding 类型的binding
             * @param binding
             * @return
             */
            @Override
            public Void visit(ProviderInstanceBinding<? extends T> binding) {
                Provider<? extends T> provider = binding.getProviderInstance();
                // 获取该提供者需要注入的属性
                Set<InjectionPoint> injectionPoints = binding.getInjectionPoints();
                // 设置一个注入任务到initializer 中  以下套路都是一致的
                Initializable<Provider<? extends T>> initializable = initializer
                        .<Provider<? extends T>>requestInjection(injector, provider, source, injectionPoints);
                InternalFactory<T> factory = new InternalFactoryToProviderAdapter<>(initializable, source);
                InternalFactory<? extends T> scopedFactory = Scopes.scope(key, injector, factory, scoping);
                putBinding(new ProviderInstanceBindingImpl<>(injector, key, source, scopedFactory, scoping,
                        provider, injectionPoints));
                return null;
            }

            /**
             * 代表该binding的提供者是另一个key对应的provider 当另一个key对应的provider在ioc容器中生成完毕时 会触发监听器 此时就可以生成该对象的provider了
             * @param binding
             * @return
             */
            @Override
            public Void visit(ProviderKeyBinding<? extends T> binding) {
                Key<? extends Provider<? extends T>> providerKey = binding.getProviderKey();
                BoundProviderFactory<T> boundProviderFactory
                        = new BoundProviderFactory<>(injector, providerKey, source);
                creationListeners.add(boundProviderFactory);
                // 使用scoping 包装工厂对象
                InternalFactory<? extends T> scopedFactory = Scopes.scope(
                        key, injector, (InternalFactory<? extends T>) boundProviderFactory, scoping);
                putBinding(new LinkedProviderBindingImpl<>(
                        injector, key, source, scopedFactory, scoping, providerKey));
                return null;
            }

            /**
             * 该binding的实例获取同样依赖于下游的key对应的provider
             * @param binding
             * @return
             */
            @Override
            public Void visit(LinkedKeyBinding<? extends T> binding) {
                Key<? extends T> linkedKey = binding.getLinkedKey();
                if (key.equals(linkedKey)) {
                    errors.recursiveBinding();
                }

                FactoryProxy<T> factory = new FactoryProxy<>(injector, key, linkedKey, source);
                creationListeners.add(factory);
                InternalFactory<? extends T> scopedFactory = Scopes.scope(key, injector, factory, scoping);
                putBinding(
                        new LinkedBindingImpl<>(injector, key, source, scopedFactory, scoping, linkedKey));
                return null;
            }

            /**
             * 处理未指定下游的binding对象 (一条还不完整的链路)
             * @param untargetted
             * @return
             */
            @Override
            public Void visit(UntargettedBinding<? extends T> untargetted) {
                // Error: Missing implementation.
                // Example: bind(Date.class).annotatedWith(Red.class);
                // We can't assume abstract types aren't injectable. They may have an
                // @ImplementedBy annotation or something.
                // 这里已经指定特殊注解了 那么无法推断到底该用哪种实现类 无法进行自适应匹配
                // 将binding对象的内部的key 加工成携带注解的方法是 annotatedWith   也就是从用户层面看 已经要根据某个注解指定它的实现类了 这时无法使用自动推断
                if (key.hasAnnotationType()) {
                    errors.missingImplementation(key);
                    // 写入的binding对象在调用get时 会抛出异常
                    putBinding(invalidBinding(injector, key, source));
                    return null;
                }

                // This cast is safe after the preceding check.
                final BindingImpl<T> binding;
                try {
                    // 使用自动推断寻找实现类  比如基于 ImplementedBy
                    binding = injector.createUnitializedBinding(key, scoping, source, errors);
                    putBinding(binding);
                } catch (ErrorsException e) {
                    errors.merge(e.getErrors());
                    putBinding(invalidBinding(injector, key, source));
                    return null;
                }

                uninitializedBindings.add(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // 基于构造器初始化的bean 不会立即创建  因为某些依赖的参数 或者说key  它的binding还没有设置到inject.state中
                            ((InjectorImpl) binding.getInjector()).initializeBinding(
                                    binding, errors.withSource(source));
                        } catch (ErrorsException e) {
                            errors.merge(e.getErrors());
                        }
                    }
                });

                return null;
            }

            @Override
            public Void visit(ExposedBinding<? extends T> binding) {
                throw new IllegalArgumentException("Cannot apply a non-module element");
            }

            @Override
            public Void visit(ConvertedConstantBinding<? extends T> binding) {
                throw new IllegalArgumentException("Cannot apply a non-module element");
            }

            @Override
            public Void visit(ConstructorBinding<? extends T> binding) {
                throw new IllegalArgumentException("Cannot apply a non-module element");
            }

            @Override
            public Void visit(ProviderBinding<? extends T> binding) {
                throw new IllegalArgumentException("Cannot apply a non-module element");
            }
        });

        return true;
    }

    /**
     * 该对象也可以处理 privateElements
     * @param privateElements
     * @return
     */
    @Override
    public Boolean visit(PrivateElements privateElements) {
        for (Key<?> key : privateElements.getExposedKeys()) {
            bindExposed(privateElements, key);
        }
        return false; // leave the private elements for the PrivateElementsProcessor to handle
    }

    private <T> void bindExposed(PrivateElements privateElements, Key<T> key) {
        ExposedKeyFactory<T> exposedKeyFactory = new ExposedKeyFactory<>(key, privateElements);
        creationListeners.add(exposedKeyFactory);
        putBinding(new ExposedBindingImpl<>(
                injector, privateElements.getExposedSource(key), key, exposedKeyFactory, privateElements));
    }

    private <T> void validateKey(Object source, Key<T> key) {
        Annotations.checkForMisplacedScopeAnnotations(key.getRawType(), source, errors);
    }

    <T> UntargettedBindingImpl<T> invalidBinding(InjectorImpl injector, Key<T> key, Object source) {
        return new UntargettedBindingImpl<>(injector, key, source);
    }

    public void initializeBindings() {
        for (Runnable initializer : uninitializedBindings) {
            initializer.run();
        }
    }

    public void runCreationListeners() {
        for (CreationListener creationListener : creationListeners) {
            creationListener.notify(errors);
        }
    }

    /**
     * 当某个binding处理完成时 触发该方法
     * @param binding
     */
    private void putBinding(BindingImpl<?> binding) {
        Key<?> key = binding.getKey();

        Class<?> rawType = key.getRawType();
        // 框架内置的类是不允许指定的
        if (FORBIDDEN_TYPES.contains(rawType)) {
            errors.cannotBindToGuiceType(rawType.getSimpleName());
            return;
        }

        Binding<?> original = injector.state.getExplicitBinding(key);
        // isOkayDuplicate 先忽略 privateElement
        if (original != null && !isOkayDuplicate(original, binding)) {
            errors.bindingAlreadySet(key, original.getSource());
            return;
        }

        // prevent the parent from creating a JIT binding for this key
        // 将处理完后的building设置到state中   可以这样理解所有被processor处理的element 最终都会加入到 state中
        // 同时为了避免在父容器中重复创建 所以加入黑名单
        injector.state.parent().blacklist(key);
        injector.state.putBinding(key, binding);
    }

    /**
     * We tolerate duplicate bindings only if one exposes the other.
     *
     * @param original the binding in the parent injector (candidate for an exposing binding)
     * @param binding  the binding to check (candidate for the exposed binding)
     */
    private boolean isOkayDuplicate(Binding<?> original, BindingImpl<?> binding) {
        if (original instanceof ExposedBindingImpl) {
            ExposedBindingImpl<?> exposed = (ExposedBindingImpl<?>) original;
            InjectorImpl exposedFrom = (InjectorImpl) exposed.getPrivateElements().getInjector();
            return (exposedFrom == binding.getInjector());
        }
        return false;
    }

    // It's unfortunate that we have to maintain a blacklist of specific
    // classes, but we can't easily block the whole package because of
    // all our unit tests.
    private static final Set<Class<?>> FORBIDDEN_TYPES = unmodifiableSet(newHashSet(
            AbstractModule.class,
            Binder.class,
            Binding.class,
            Injector.class,
            Key.class,
            MembersInjector.class,
            Module.class,
            Provider.class,
            Scope.class,
            TypeLiteral.class));
    // TODO(jessewilson): fix BuiltInModule, then add Stage

    interface CreationListener {
        void notify(Errors errors);
    }
}
