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

import org.elasticsearch.common.inject.internal.BindingImpl;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.internal.Stopwatch;
import org.elasticsearch.common.inject.spi.Dependency;

import java.util.List;

/**
 * Builds a tree of injectors. This is a primary injector, plus child injectors needed for each
 * {@link Binder#newPrivateBinder() private environment}. The primary injector is not necessarily a
 * top-level injector.
 * <p>
 * Injector construction happens in two phases.
 * <ol>
 * <li>Static building. In this phase, we interpret commands, create bindings, and inspect
 * dependencies. During this phase, we hold a lock to ensure consistency with parent injectors.
 * No user code is executed in this phase.</li>
 * <li>Dynamic injection. In this phase, we call user code. We inject members that requested
 * injection. This may require user's objects be created and their providers be called. And we
 * create eager singletons. In this phase, user code may have started other threads. This phase
 * is not executed for injectors created using {@link Stage#TOOL the tool stage}</li>
 * </ol>
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 * 该对象负责生成 注入器对象  它定义了设置注入器的模板代码
 */
class InjectorBuilder {

    /**
     * 停表对象 具备记录暂停时间的能力
     */
    private final Stopwatch stopwatch = new Stopwatch();

    /**
     * 具备存储错误信息的能力
     */
    private final Errors errors = new Errors();

    /**
     * 描述当前的运行模式  比如是否开启检测
     */
    private Stage stage;

    /**
     * 初始化对象  该对象负责触发 注入动作
     */
    private final Initializer initializer = new Initializer();
    /**
     * 专门处理绑定请求的 processor
     */
    private final BindingProcessor bindingProcesor;
    /**
     * 处理注入请求的processor
     */
    private final InjectionRequestProcessor injectionRequestProcessor;

    /**
     * 该对象负责构建 InjectorShell 对象
     */
    private final InjectorShell.Builder shellBuilder = new InjectorShell.Builder();

    /**
     * InjectorShell:
     *     在生成该shell时,对应处理的elements
     *     在处理这些elements时,生成的injector对象
     */
    private List<InjectorShell> shells;

    /**
     * 在初始化阶段 通过负责触发注入动作的 initializer 生成2个处理器对象
     */
    InjectorBuilder() {
        injectionRequestProcessor = new InjectionRequestProcessor(errors, initializer);
        bindingProcesor = new BindingProcessor(errors, initializer);
    }

    /**
     * Sets the stage for the created injector. If the stage is {@link Stage#PRODUCTION}, this class
     * will eagerly load singletons.
     * 设置当前所处的阶段
     */
    InjectorBuilder stage(Stage stage) {
        shellBuilder.stage(stage);
        this.stage = stage;
        return this;
    }

    /**
     * 当添加了要处理的module后 才能生成注入对象
     * @param modules
     * @return
     */
    InjectorBuilder addModules(Iterable<? extends Module> modules) {
        shellBuilder.addModules(modules);
        return this;
    }

    /**
     * 构建 injector对象
     * @return
     */
    Injector build() {
        if (shellBuilder == null) {
            throw new AssertionError("Already built, builders are not reusable.");
        }

        // Synchronize while we're building up the bindings and other injector state. This ensures that
        // the JIT bindings in the parent injector don't change while we're being built
        // 避免并发调用build 需要加锁
        synchronized (shellBuilder.lock()) {
            shells = shellBuilder.build(initializer, bindingProcesor, stopwatch, errors);
            stopwatch.resetAndLog("Injector construction");

            // 以上已经将所有binding插入到容器中了

            // 做一些准备工作
            initializeStatically();
        }

        // 为待注入对象进行属性注入
        injectDynamically();

        // 返回顶层injector
        return primaryInjector();
    }

    /**
     * Initialize and validate everything.
     */
    private void initializeStatically() {
        // 此时将之前基于构造器创建的binding初始化   生成下游的instance/provider 是第一优先级 之后才考虑进行属性注入
        bindingProcesor.initializeBindings();
        stopwatch.resetAndLog("Binding initialization");

        for (InjectorShell shell : shells) {
            // 将 state中的binding转移到 injector中
            shell.getInjector().index();
        }
        stopwatch.resetAndLog("Binding indexing");

        // 处理所有为实例/class 注入属性的请求  这里还没有进行注入 只是设置了一些 pending任务
        injectionRequestProcessor.process(shells);
        stopwatch.resetAndLog("Collecting injection requests");

        // 触发监听器 也就是针对 provider是 Key的 (某个key的bean提供者与另一个key的提供者一样)   (还会处理expose数据)
        bindingProcesor.runCreationListeners();
        stopwatch.resetAndLog("Binding validation");

        // 以上已经完成了所有binding的设置了  这里对之前的静态属性/方法 生成从ioc容器中获取相关数据的对象
        injectionRequestProcessor.validate();
        stopwatch.resetAndLog("Static validation");

        // 生成具备读取需要注入的field/method的对象
        initializer.validateOustandingInjections(errors);
        stopwatch.resetAndLog("Instance member validation");

        // 回填查询需要的代理对象
        new LookupProcessor(errors).process(shells);
        // 如果在ioc注入时间接调用了多次查询api 此时回填代理对象
        for (InjectorShell shell : shells) {
            ((DeferredLookups) shell.getInjector().lookups).initialize(errors);
        }
        stopwatch.resetAndLog("Provider verification");

        // 确保所有element都已经处理完
        for (InjectorShell shell : shells) {
            if (!shell.getElements().isEmpty()) {
                throw new AssertionError("Failed to execute " + shell.getElements());
            }
        }

        errors.throwCreationExceptionIfErrorsExist();
    }

    /**
     * Returns the injector being constructed. This is not necessarily the root injector.
     */
    private Injector primaryInjector() {
        return shells.get(0).getInjector();
    }

    /**
     * Inject everything that can be injected. This method is intentionally not synchronized. If we
     * locked while injecting members (ie. running user code), things would deadlock should the user
     * code build a just-in-time binding from another thread.
     */
    private void injectDynamically() {
        // 注入静态属性/调用静态方法
        injectionRequestProcessor.injectMembers();
        stopwatch.resetAndLog("Static member injection");

        // 注入成员变量/调用set方法
        initializer.injectAll(errors);
        stopwatch.resetAndLog("Instance injection");
        errors.throwCreationExceptionIfErrorsExist();

        // 深度遍历的顺序 初始化需要提前创建的对象   TODO 什么场景下使用???
        for (InjectorShell shell : shells) {
            loadEagerSingletons(shell.getInjector(), stage, errors);
        }
        stopwatch.resetAndLog("Preloading singletons");
        errors.throwCreationExceptionIfErrorsExist();
    }

    /**
     * Loads eager singletons, or all singletons if we're in Stage.PRODUCTION. Bindings discovered
     * while we're binding these singletons are not be eager.
     * 加载提早暴露的对象
     */
    public void loadEagerSingletons(InjectorImpl injector, Stage stage, Errors errors) {
        for (final Binding<?> binding : injector.state.getExplicitBindingsThisLevel().values()) {
            loadEagerSingletons(injector, stage, errors, (BindingImpl<?>)binding);
        }
        for (final Binding<?> binding : injector.jitBindings.values()) {
            loadEagerSingletons(injector, stage, errors, (BindingImpl<?>)binding);
        }
    }

    /**
     * 加载提早暴露的对象
     * @param injector
     * @param stage
     * @param errors
     * @param binding
     */
    private void loadEagerSingletons(InjectorImpl injector, Stage stage, final Errors errors, BindingImpl<?> binding) {
        if (binding.getScoping().isEagerSingleton(stage)) {
            try {
                injector.callInContext(new ContextualCallable<Void>() {
                    Dependency<?> dependency = Dependency.get(binding.getKey());

                    @Override
                    public Void call(InternalContext context) {
                        context.setDependency(dependency);
                        Errors errorsForBinding = errors.withSource(dependency);
                        try {
                            // 这种工厂都是包装过的 会记录创建的实例 之后总是返回同一个实例
                            binding.getInternalFactory().get(errorsForBinding, context, dependency);
                        } catch (ErrorsException e) {
                            errorsForBinding.merge(e.getErrors());
                        } finally {
                            context.setDependency(null);
                        }

                        return null;
                    }
                });
            } catch (ErrorsException e) {
                throw new AssertionError();
            }
        }
    }
}
