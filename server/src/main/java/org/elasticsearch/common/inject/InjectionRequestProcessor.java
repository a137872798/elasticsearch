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

import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.spi.InjectionPoint;
import org.elasticsearch.common.inject.spi.InjectionRequest;
import org.elasticsearch.common.inject.spi.StaticInjectionRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Handles {@link Binder#requestInjection} and {@link Binder#requestStaticInjection} commands.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 * @author mikeward@google.com (Mike Ward)
 * 该处理器用于处理 requestInjection/requestStaticInjection
 * 在ioc容器内部的类 在实例化时会自动进行注入  而用户手动创建的类 也可以通过ioc容器进行加工
 */
class InjectionRequestProcessor extends AbstractProcessor {

    private final List<StaticInjection> staticInjections = new ArrayList<>();
    private final Initializer initializer;


    /**
     *
     * @param errors   存储错误信息
     * @param initializer    用于控制注入对象
     */
    InjectionRequestProcessor(Errors errors, Initializer initializer) {
        super(errors);
        this.initializer = initializer;
    }

    @Override
    public Boolean visit(StaticInjectionRequest request) {
        staticInjections.add(new StaticInjection(injector, request));
        return true;
    }

    /**
     * 处理属性注入请求
     * @param request
     * @return
     */
    @Override
    public Boolean visit(InjectionRequest request) {
        Set<InjectionPoint> injectionPoints;
        try {
            // 寻找所有注入点
            injectionPoints = request.getInjectionPoints();
        } catch (ConfigurationException e) {
            errors.merge(e.getErrorMessages());
            injectionPoints = e.getPartialValue();
        }

        initializer.requestInjection(
                injector, request.getInstance(), request.getSource(), injectionPoints);
        return true;
    }

    public void validate() {
        for (StaticInjection staticInjection : staticInjections) {
            staticInjection.validate();
        }
    }

    public void injectMembers() {
        for (StaticInjection staticInjection : staticInjections) {
            staticInjection.injectMembers();
        }
    }

    /**
     * A requested static injection.
     * 如果是针对静态属性的注入请求  会包装一个 StaticInjection 对象
     */
    private class StaticInjection {

        /**
         * 代表从哪一层开始寻找bean (包括父级 但不包括子级)
         */
        final InjectorImpl injector;
        final Object source;
        /**
         * 注入请求
         */
        final StaticInjectionRequest request;
        List<SingleMemberInjector> memberInjectors;

        StaticInjection(InjectorImpl injector, StaticInjectionRequest request) {
            this.injector = injector;
            this.source = request.getSource();
            this.request = request;
        }

        void validate() {
            Errors errorsForMember = errors.withSource(source);
            Set<InjectionPoint> injectionPoints;
            try {
                injectionPoints = request.getInjectionPoints();
            } catch (ConfigurationException e) {
                errors.merge(e.getErrorMessages());
                injectionPoints = e.getPartialValue();
            }
            memberInjectors = injector.membersInjectorStore.getInjectors(
                    injectionPoints, errorsForMember);
        }

        void injectMembers() {
            try {
                injector.callInContext(new ContextualCallable<Void>() {
                    @Override
                    public Void call(InternalContext context) {
                        for (SingleMemberInjector injector : memberInjectors) {
                            injector.inject(errors, context, null);
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
