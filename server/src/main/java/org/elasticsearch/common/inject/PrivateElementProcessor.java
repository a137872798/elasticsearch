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
import org.elasticsearch.common.inject.spi.PrivateElements;

import java.util.ArrayList;
import java.util.List;

/**
 * Handles {@link Binder#newPrivateBinder()} elements.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
class PrivateElementProcessor extends AbstractProcessor {

    private final Stage stage;
    private final List<InjectorShell.Builder> injectorShellBuilders = new ArrayList<>();

    PrivateElementProcessor(Errors errors, Stage stage) {
        super(errors);
        this.stage = stage;
    }

    /**
     * 检测到elements中存在子级 binder  （一个父级binder下可能有多个子级的binder 也就是调用了多次 newPrivateBinder）
     * @param privateElements
     * @return
     */
    @Override
    public Boolean visit(PrivateElements privateElements) {
        // 创建一个新的 shell.builder 对象 并以当前injector作为父对象 单独处理子elements中定义的element对象(包括binding)
        InjectorShell.Builder builder = new InjectorShell.Builder()
                .parent(injector)
                .stage(stage)
                .privateElements(privateElements);
        injectorShellBuilders.add(builder);
        return true;
    }

    public List<InjectorShell.Builder> getInjectorShellBuilders() {
        return injectorShellBuilders;
    }
}
