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

package org.elasticsearch.index.query;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.function.Predicate;

/**
 * A predicate that checks whether an index pattern matches the current search shard target.
 * 索引名称匹配器
 * 用于检测字符串是否与某些索引名匹配
 */
public class SearchIndexNameMatcher implements Predicate<String> {
    /**
     * 匹配的索引名
     */
    private final String indexName;
    private final String clusterAlias;
    private final ClusterService clusterService;
    /**
     * 索引名称解析器
     * 主要就是这个解析器在起作用
     */
    private final IndexNameExpressionResolver expressionResolver;

    /**
     * Creates a new index name matcher.
     *
     * @param indexName he name of the local index.
     * @param clusterAlias the cluster alias of this search shard target. If it is a local target, the alias
     *                     should be null or equal to {@link RemoteClusterAware#LOCAL_CLUSTER_GROUP_KEY}.
     * @param clusterService the cluster service.
     */
    public SearchIndexNameMatcher(String indexName,
                                  String clusterAlias,
                                  ClusterService clusterService,
                                  IndexNameExpressionResolver expressionResolver) {
        this.indexName = indexName;
        this.clusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias) ? null : clusterAlias;
        this.clusterService = clusterService;
        this.expressionResolver = expressionResolver;
    }

    /**
     *  Given an index pattern, checks whether it matches against the current shard.
     *
     *  If this shard represents a remote shard target, then in order to match the pattern contain
     *  the separator ':', and must match on both the cluster alias and index name.
     *  检测是否匹配   字符串本身应该是一个 clusterAlias:xxx
     *  检测给定的字符串能否匹配上 indexName
     */
    public boolean test(String pattern) {
        int separatorIndex = pattern.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
        if (separatorIndex < 0) {
            // 没有携带集群名的情况 直接匹配后半部分
            return clusterAlias == null && matchesIndex(pattern);
        } else {
            String clusterPattern = pattern.substring(0, separatorIndex);
            String indexPattern = pattern.substring(separatorIndex + 1);

            // 集群名是正则匹配 后面的部分使用 expressionResolver 进行匹配
            return Regex.simpleMatch(clusterPattern, clusterAlias) && matchesIndex(indexPattern);
        }
    }

    private boolean matchesIndex(String pattern) {
        // 从集群中所有的indexName中 找到匹配到一组indexName
        String[] concreteIndices = expressionResolver.concreteIndexNames(
            clusterService.state(), IndicesOptions.lenientExpandOpen(), pattern);
        // 这里还要与当前indexName 进行匹配
        for (String index : concreteIndices) {
            if (Regex.simpleMatch(index, indexName)) {
                return true;
            }
        }
        return false;
    }
}
