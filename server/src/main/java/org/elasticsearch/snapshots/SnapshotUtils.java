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
package org.elasticsearch.snapshots;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Snapshot utilities
 */
public class SnapshotUtils {

    /**
     * Filters out list of available indices based on the list of selected indices.
     * 根据selectedIndices 过滤availableIndices 中的索引 并返回结果
     *
     * @param availableIndices list of available indices    本次能获取到的全部索引
     * @param selectedIndices  list of selected indices   实际需要的索引
     * @param indicesOptions    ignore indices flag
     * @return filtered out indices
     *
     */
    public static List<String> filterIndices(List<String> availableIndices, String[] selectedIndices, IndicesOptions indicesOptions) {
        // 如果selectedIndices 是 _all 代表需要获取所有的索引
        if (IndexNameExpressionResolver.isAllIndices(Arrays.asList(selectedIndices))) {
            return availableIndices;
        }
        Set<String> result = null;
        for (int i = 0; i < selectedIndices.length; i++) {
            String indexOrPattern = selectedIndices[i];

            // 在默认情况下认为这种过滤是 只有匹配成功的才加入到容器中    也有一种用法就是从available中剔除被选中的索引
            boolean add = true;

            if (!indexOrPattern.isEmpty()) {
                if (availableIndices.contains(indexOrPattern)) {
                    if (result == null) {
                        result = new HashSet<>();
                    }
                    // 也就是默认情况下认为 命中的索引会加入到result中
                    result.add(indexOrPattern);
                    continue;
                }
                // 如果是  "+XXX"  也就是将结果添加到result中
                if (indexOrPattern.charAt(0) == '+') {
                    add = true;
                    indexOrPattern = indexOrPattern.substring(1);
                    // if its the first, add empty set
                    if (i == 0) {
                        result = new HashSet<>();
                    }
                    // 如果是 "-XXX" 将结果从容器中移除
                } else if (indexOrPattern.charAt(0) == '-') {
                    // if its the first, fill it with all the indices...
                    if (i == 0) {
                        result = new HashSet<>(availableIndices);
                    }
                    add = false;
                    indexOrPattern = indexOrPattern.substring(1);
                }
            }
            // 不是"*" 或者是 ""  这种是特殊情况 先忽略
            if (indexOrPattern.isEmpty() || !Regex.isSimpleMatchPattern(indexOrPattern)) {
                if (!availableIndices.contains(indexOrPattern)) {
                    // 如果未找到的index 不能被忽略 那么就抛出 index 未找到的异常
                    if (!indicesOptions.ignoreUnavailable()) {
                        throw new IndexNotFoundException(indexOrPattern);
                    } else {
                        if (result == null) {
                            // add all the previous ones...
                            result = new HashSet<>(availableIndices.subList(0, i));
                        }
                    }
                } else {
                    if (result != null) {
                        if (add) {
                            result.add(indexOrPattern);
                        } else {
                            result.remove(indexOrPattern);
                        }
                    }
                }
                continue;
            }

            // 这种情况应该是不会发生的吧
            if (result == null) {
                // add all the previous ones...
                result = new HashSet<>(availableIndices.subList(0, i));
            }
            boolean found = false;

            // 默认情况下都是将命中的索引添加到result中的  但是在selectedIndices 中可以传入 -XXX 代表从结果容器中移除某个索引
            for (String index : availableIndices) {
                if (Regex.simpleMatch(indexOrPattern, index)) {
                    found = true;
                    if (add) {
                        result.add(index);
                    } else {
                        result.remove(index);
                    }
                }
            }

            // 不允许忽视未找到的索引时 抛出异常
            if (!found && !indicesOptions.allowNoIndices()) {
                throw new IndexNotFoundException(indexOrPattern);
            }
        }
        if (result == null) {
            return List.of(selectedIndices);
        }
        return List.copyOf(result);
    }
}
