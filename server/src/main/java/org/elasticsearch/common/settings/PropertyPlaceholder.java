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

package org.elasticsearch.common.settings;

import org.elasticsearch.common.Strings;

import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * Utility class for working with Strings that have placeholder values in them. A placeholder takes the form
 * {@code ${name}}. Using {@code PropertyPlaceholder} these placeholders can be substituted for
 * user-supplied values.
 * <p>
 * Values for substitution can be supplied using a {@link Properties} instance or using a
 * {@link PlaceholderResolver}.
 * 该对象负责解析占位符  这个套路跟 skywalking 好像
 */
class PropertyPlaceholder {

    /**
     * 占位符 前缀
     */
    private final String placeholderPrefix;
    /**
     * 占位符 后缀
     */
    private final String placeholderSuffix;
    private final boolean ignoreUnresolvablePlaceholders;

    /**
     * Creates a new <code>PropertyPlaceholderHelper</code> that uses the supplied prefix and suffix.
     *
     * @param placeholderPrefix              the prefix that denotes the start of a placeholder.
     * @param placeholderSuffix              the suffix that denotes the end of a placeholder.
     * @param ignoreUnresolvablePlaceholders indicates whether unresolvable placeholders should be ignored
     *                                       (<code>true</code>) or cause an exception (<code>false</code>).
     */
    PropertyPlaceholder(String placeholderPrefix, String placeholderSuffix,
                               boolean ignoreUnresolvablePlaceholders) {
        this.placeholderPrefix = Objects.requireNonNull(placeholderPrefix);
        this.placeholderSuffix = Objects.requireNonNull(placeholderSuffix);
        this.ignoreUnresolvablePlaceholders = ignoreUnresolvablePlaceholders;
    }

    /**
     * Replaces all placeholders of format <code>${name}</code> with the value returned from the supplied {@link
     * PlaceholderResolver}.
     *
     * @param value               the value containing the placeholders to be replaced.
     * @param placeholderResolver the <code>PlaceholderResolver</code> to use for replacement.   该对象定义了如何处理占位符
     * @return the supplied value with placeholders replaced inline.
     * @throws NullPointerException if value is null
     * 从配置项中检测占位符 并进行替换
     */
    String replacePlaceholders(String value, PlaceholderResolver placeholderResolver) {
        Objects.requireNonNull(value);
        return parseStringValue(value, placeholderResolver, new HashSet<>());
    }

    /**
     * 这个代码好像跟 skywalking一样的...
     * @param strVal   待解析的配置
     * @param placeholderResolver   定义如何处理占位符
     * @param visitedPlaceholders  检测是否发生循环依赖
     * @return
     */
    private String parseStringValue(String strVal, PlaceholderResolver placeholderResolver,
                                    Set<String> visitedPlaceholders) {
        StringBuilder buf = new StringBuilder(strVal);

        int startIndex = strVal.indexOf(this.placeholderPrefix);
        while (startIndex != -1) {
            // 代表解析到一个占位符的前缀 尝试获取后缀的下标
            int endIndex = findPlaceholderEndIndex(buf, startIndex);
            if (endIndex != -1) {
                // 截取占位符的部分 当然内部可能发生了嵌套
                String placeholder = buf.substring(startIndex + this.placeholderPrefix.length(), endIndex);
                // 代表发生循环依赖
                if (!visitedPlaceholders.add(placeholder)) {
                    throw new IllegalArgumentException(
                            "Circular placeholder reference '" + placeholder + "' in property definitions");
                }
                // Recursive invocation, parsing placeholders contained in the placeholder key.
                // 递归 当某次无嵌套情况时 就可以走下面正常处理的逻辑了 这是一种前序递归
                placeholder = parseStringValue(placeholder, placeholderResolver, visitedPlaceholders);

                // Now obtain the value for the fully resolved key...
                int defaultValueIdx = placeholder.indexOf(':');
                String defaultValue = null;
                if (defaultValueIdx != -1) {
                    defaultValue = placeholder.substring(defaultValueIdx + 1);
                    placeholder = placeholder.substring(0, defaultValueIdx);
                }
                String propVal = placeholderResolver.resolvePlaceholder(placeholder);
                if (propVal == null) {
                    propVal = defaultValue;
                }
                if (propVal == null && placeholderResolver.shouldIgnoreMissing(placeholder)) {
                    if (placeholderResolver.shouldRemoveMissingPlaceholder(placeholder)) {
                        propVal = "";
                    } else {
                        return strVal;
                    }
                }
                if (propVal != null) {
                    // Recursive invocation, parsing placeholders contained in the
                    // previously resolved placeholder value.
                    propVal = parseStringValue(propVal, placeholderResolver, visitedPlaceholders);
                    buf.replace(startIndex, endIndex + this.placeholderSuffix.length(), propVal);
                    startIndex = buf.indexOf(this.placeholderPrefix, startIndex + propVal.length());
                } else if (this.ignoreUnresolvablePlaceholders) {
                    // Proceed with unprocessed value.
                    startIndex = buf.indexOf(this.placeholderPrefix, endIndex + this.placeholderSuffix.length());
                } else {
                    throw new IllegalArgumentException("Could not resolve placeholder '" + placeholder + "'");
                }

                visitedPlaceholders.remove(placeholder);
            } else {
                startIndex = -1;
            }
        }

        return buf.toString();
    }

    /**
     * 寻找占位符的后缀  此时内部还可能发生嵌套
     * @param buf
     * @param startIndex
     * @return
     */
    private int findPlaceholderEndIndex(CharSequence buf, int startIndex) {
        // 获取起点
        int index = startIndex + this.placeholderPrefix.length();
        // 记录嵌套的数量
        int withinNestedPlaceholder = 0;
        while (index < buf.length()) {
            // 从首个字符开始匹配后缀
            if (Strings.substringMatch(buf, index, this.placeholderSuffix)) {
                if (withinNestedPlaceholder > 0) {
                    withinNestedPlaceholder--;
                    index = index + this.placeholderSuffix.length();
                } else {
                    // 只有当所有嵌套都处理完后 才得到传入的前缀对应的后缀下标
                    return index;
                }
            // 除了匹配后缀外 还需要考虑嵌套的情况
            } else if (Strings.substringMatch(buf, index, this.placeholderPrefix)) {
                // 代表发生了嵌套
                withinNestedPlaceholder++;
                // 这样在匹配后缀时 起始下标就要额外增加一个前缀的长度
                index = index + this.placeholderPrefix.length();

            // 正常增加匹配的起始下标
            } else {
                index++;
            }
        }
        return -1;
    }

    /**
     * Strategy interface used to resolve replacement values for placeholders contained in Strings.
     *
     * @see PropertyPlaceholder
     * 定义了如果处理占位符
     */
    interface PlaceholderResolver {

        /**
         * Resolves the supplied placeholder name into the replacement value.
         *
         * @param placeholderName the name of the placeholder to resolve.
         * @return the replacement value or <code>null</code> if no replacement is to be made.
         * 通过占位符内的名字 获取一个真实的属性值
         */
        String resolvePlaceholder(String placeholderName);

        /**
         * 是否可以不处理
         * @param placeholderName
         * @return
         */
        boolean shouldIgnoreMissing(String placeholderName);

        /**
         * Allows for special handling for ignored missing placeholders that may be resolved elsewhere
         *
         * @param placeholderName the name of the placeholder to resolve.
         * @return true if the placeholder should be replaced with a empty string
         * 当不处理时 是否应该移除占位符
         */
        boolean shouldRemoveMissingPlaceholder(String placeholderName);
    }
}
