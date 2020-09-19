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

import org.elasticsearch.common.inject.spi.Dependency;

import java.util.HashMap;
import java.util.Map;

/**
 * Internal context. Used to coordinate injections and support circular
 * dependencies.
 *
 * @author crazybob@google.com (Bob Lee)
 * 在进行属性注入时 需要的上下文信息
 */
public final class InternalContext {

    private Map<Object, ConstructionContext<?>> constructionContexts = new HashMap<>();
    /**
     * 在进行注入时   该依赖信息提供了 选择的具体实现  比如针对field 可能有多种实现类 通过 @Named 注解 可以知道应该选用哪个实现类
     */
    private Dependency dependency;

    /**
     * 为某个待增强对象生成构造器上下文
     * @param key
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> ConstructionContext<T> getConstructionContext(Object key) {
        ConstructionContext<T> constructionContext
                = (ConstructionContext<T>) constructionContexts.get(key);
        if (constructionContext == null) {
            constructionContext = new ConstructionContext<>();
            constructionContexts.put(key, constructionContext);
        }
        return constructionContext;
    }

    public Dependency getDependency() {
        return dependency;
    }

    public void setDependency(Dependency dependency) {
        this.dependency = dependency;
    }
}
