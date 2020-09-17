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

import org.elasticsearch.common.inject.Binding;
import org.elasticsearch.common.inject.Key;

/**
 * A binding to a linked key. The other key's binding is used to resolve injections.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 * 代表该绑定关系将上游的key 绑定到下游的key 上 形成链式结构
 */
public interface LinkedKeyBinding<T> extends Binding<T> {

    /**
     * Returns the linked key used to resolve injections.
     */
    Key<? extends T> getLinkedKey();

}
