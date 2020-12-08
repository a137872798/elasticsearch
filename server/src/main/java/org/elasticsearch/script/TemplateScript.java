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

package org.elasticsearch.script;

import java.util.Map;

/**
 * A string template rendered as a script.
 * 渲染脚本的模板 啥玩意???
 */
public abstract class TemplateScript {

    private final Map<String, Object> params;

    public TemplateScript(Map<String, Object> params) {
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public static final String[] PARAMETERS = {};
    /**
     * Run a template and return the resulting string, encoded in utf8 bytes.
     * 执行模板引擎 并生成结果
     */
    public abstract String execute();

    public interface Factory {
        /**
         * 将一组参数转换成 脚本对象   一般就是直接将params 赋值到 TemplateScript.params上
         * @param params
         * @return
         */
        TemplateScript newInstance(Map<String, Object> params);
    }

    /**
     * 在脚本上下文中 可以看到某个class 以及各种反射获取到的method信息
     */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("template", Factory.class);
}
