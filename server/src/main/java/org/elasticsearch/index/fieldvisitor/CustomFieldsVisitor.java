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
package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.FieldInfo;

import java.util.Set;

/**
 * A field visitor that allows to load a selection of the stored fields by exact name or by pattern.
 * Supported pattern styles: "xxx*", "*xxx", "*xxx*" and "xxx*yyy".
 * The Uid field is always loaded.
 * The class is optimized for source loading as it is a common use case.
 * 这里可以根据自己需要 指定保存的field数据
 */
public class CustomFieldsVisitor extends FieldsVisitor {

    private final Set<String> fields;

    /**
     *
     * @param fields
     * @param loadSource 是否要读取source数据
     */
    public CustomFieldsVisitor(Set<String> fields, boolean loadSource) {
        super(loadSource);
        this.fields = fields;
    }

    /**
     * 针对是否要处理某些field时 额外加了一些限制
     * @param fieldInfo
     * @return
     */
    @Override
    public Status needsField(FieldInfo fieldInfo) {
        if (super.needsField(fieldInfo) == Status.YES) {
            return Status.YES;
        }
        // 只要属于customField 那么也需要被维护
        if (fields.contains(fieldInfo.name)) {
            return Status.YES;
        }
        return Status.NO;
    }
}
