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
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.spi.InjectionPoint;

/**
 * Injects a field or method of a given object.
 * memberInjector  代表针对某个实例下所有可增强的 method/field 进行增强
 * 而SingleMemberInjector 则代表仅针对单个 field/method 进行增强
 */
interface SingleMemberInjector {

    /**
     * 为某个实例进行增强
     * @param errors
     * @param context
     * @param o
     */
    void inject(Errors errors, InternalContext context, Object o);

    /**
     * 获取该对象针对的注入点
     * @return
     */
    InjectionPoint getInjectionPoint();
}
