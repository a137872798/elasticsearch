/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 收集用户生成的各种模块对象  这些module中定义了接口与实现类的绑定关系 通过使用他们配置binder对象（也就是将绑定关系存储到binder中） 完成injector对象的创建
 * 之后injector对象相当于就是ioc容器的入口 当需要获取某个实例时只要传入需要的类型/接口 就可以获取到对应的实例了
 */
public class ModulesBuilder implements Iterable<Module> {

    private final List<Module> modules = new ArrayList<>();

    public ModulesBuilder add(Module... newModules) {
        Collections.addAll(modules, newModules);
        return this;
    }

    @Override
    public Iterator<Module> iterator() {
        return modules.iterator();
    }

    public Injector createInjector() {
        Injector injector = Guice.createInjector(modules);
        ((InjectorImpl) injector).clearCache();
        // in ES, we always create all instances as if they are eager singletons
        // this allows for considerable memory savings (no need to store construction info) as well as cycles
        ((InjectorImpl) injector).readOnlyAllSingletons();
        return injector;
    }
}
