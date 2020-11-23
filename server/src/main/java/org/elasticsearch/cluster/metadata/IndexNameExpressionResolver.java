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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.InvalidIndexNameException;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 索引名字解析器
 */
public class IndexNameExpressionResolver {

    private final DateMathExpressionResolver dateMathExpressionResolver = new DateMathExpressionResolver();
    /**
     * 先使用日期格式解析器 之后使用通配符解析器
     */
    private final List<ExpressionResolver> expressionResolvers = List.of(dateMathExpressionResolver, new WildcardExpressionResolver());

    /**
     * Same as {@link #concreteIndexNames(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     * 从当前集群状态中选择一组符合索引状态筛选条件的索引
     */
    public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
        // 将相关参数包裹成上下文
        Context context = new Context(state, request.indicesOptions());
        // 解析表达式 并将匹配的所有索引名称查询返回
        return concreteIndexNames(context, request.indices());
    }

    /**
     * Same as {@link #concreteIndices(ClusterState, IndicesOptions, String...)}, but the index expressions and options
     * are encapsulated in the specified request.
     */
    public Index[] concreteIndices(ClusterState state, IndicesRequest request) {
        Context context = new Context(state, request.indicesOptions());
        return concreteIndices(context, request.indices());
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param state            the cluster state containing all the data to resolve to expressions to concrete indices
     * @param options          defines how the aliases or indices need to be resolved to concrete indices
     * @param indexExpressions expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * @throws IndexNotFoundException   if one of the index expressions is pointing to a missing index or alias and the
     *                                  provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     *                                  contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     *                                  indices options in the context don't allow such a case.
     *                                  将传入的索引表达式转换成 匹配的实际存在的索引
     */
    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, String... indexExpressions) {
        Context context = new Context(state, options);
        return concreteIndexNames(context, indexExpressions);
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param state            the cluster state containing all the data to resolve to expressions to concrete indices
     * @param options          defines how the aliases or indices need to be resolved to concrete indices
     * @param indexExpressions expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * @throws IndexNotFoundException   if one of the index expressions is pointing to a missing index or alias and the
     *                                  provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     *                                  contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     *                                  indices options in the context don't allow such a case.
     */
    public Index[] concreteIndices(ClusterState state, IndicesOptions options, String... indexExpressions) {
        Context context = new Context(state, options, false, false);
        return concreteIndices(context, indexExpressions);
    }

    /**
     * Translates the provided index expression into actual concrete indices, properly deduplicated.
     *
     * @param state            the cluster state containing all the data to resolve to expressions to concrete indices
     * @param options          defines how the aliases or indices need to be resolved to concrete indices
     * @param startTime        The start of the request where concrete indices is being invoked for
     * @param indexExpressions expressions that can be resolved to alias or index names.
     * @return the resolved concrete indices based on the cluster state, indices options and index expressions
     * provided indices options in the context don't allow such a case, or if the final result of the indices resolution
     * contains no indices and the indices options in the context don't allow such a case.
     * @throws IllegalArgumentException if one of the aliases resolve to multiple indices and the provided
     *                                  indices options in the context don't allow such a case.
     */
    public Index[] concreteIndices(ClusterState state, IndicesOptions options, long startTime, String... indexExpressions) {
        Context context = new Context(state, options, startTime);
        return concreteIndices(context, indexExpressions);
    }

    /**
     * @param context          该对象中包含了需要的参数信息  比如IndexOptional
     * @param indexExpressions 本次需要获取的所有index的特殊字符串  内部可能需要通过通配符去匹配index
     * @return
     */
    String[] concreteIndexNames(Context context, String... indexExpressions) {
        // 将表达式转换成索引对象
        Index[] indexes = concreteIndices(context, indexExpressions);
        // 取出索引名称后返回
        String[] names = new String[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            names[i] = indexes[i].getName();
        }
        return names;
    }

    /**
     * 根据索引表达式找到所有匹配的索引
     *
     * @param context
     * @param indexExpressions
     * @return
     */
    Index[] concreteIndices(Context context, String... indexExpressions) {
        // 如果没有传入索引表达式 默认就是全匹配   可以理解为用户没有显式声明要处理的索引 所以处理所有的索引
        if (indexExpressions == null || indexExpressions.length == 0) {
            indexExpressions = new String[]{Metadata.ALL};
        }
        Metadata metadata = context.getState().metadata();

        // 选出的所有还要符合这个选项要求
        IndicesOptions options = context.getOptions();
        // 当找到的index 处于关闭状态 是否抛出异常
        final boolean failClosed = options.forbidClosedIndices() && options.ignoreUnavailable() == false;
        // If only one index is specified then whether we fail a request if an index is missing depends on the allow_no_indices
        // option. At some point we should change this, because there shouldn't be a reason why whether a single index
        // or multiple indices are specified yield different behaviour.
        // 当传入的某个索引搜索键 不能找到对应的数据时  是否抛出异常  如果只有一条搜索键 没有命中时 等价于 noIndices
        // 如果有多个索引键 那么只要!options.ignoreUnavailable() 就代表只要传入了一个无效的索引键就抛出异常
        final boolean failNoIndices = indexExpressions.length == 1 ? !options.allowNoIndices() : !options.ignoreUnavailable();
        List<String> expressions = Arrays.asList(indexExpressions);
        // 从这里可以看出下一个resolver处理的数据是由上一个resolver加工后的  实际上是一个链式调用
        for (ExpressionResolver expressionResolver : expressionResolvers) {
            expressions = expressionResolver.resolve(context, expressions);
        }

        // 在进行各种解析后 这里得到的是一组准确的查询键   比如某些express可能携带通配符 这里就找到存在的索引键 并进行转换
        if (expressions.isEmpty()) {
            // 当没有得到任何有效的索引键时 代表用户传入的表达式没法匹配到有效的索引 抛出异常
            if (!options.allowNoIndices()) {
                IndexNotFoundException infe;
                if (indexExpressions.length == 1) {
                    if (indexExpressions[0].equals(Metadata.ALL)) {
                        infe = new IndexNotFoundException("no indices exist", (String) null);
                    } else {
                        infe = new IndexNotFoundException((String) null);
                    }
                } else {
                    infe = new IndexNotFoundException((String) null);
                }
                infe.setResources("index_expression", indexExpressions);
                throw infe;
            } else {
                return Index.EMPTY_ARRAY;
            }
        }

        final Set<Index> concreteIndices = new HashSet<>(expressions.size());
        for (String expression : expressions) {
            IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(expression);
            if (indexAbstraction == null) {
                // 转换后的索引名无法找到索引数据 或者用户一开始传入的未转换的索引名找不到索引数据
                if (failNoIndices) {
                    IndexNotFoundException infe;
                    if (expression.equals(Metadata.ALL)) {
                        infe = new IndexNotFoundException("no indices exist", expression);
                    } else {
                        infe = new IndexNotFoundException(expression);
                    }
                    infe.setResources("index_expression", expression);
                    throw infe;
                } else {
                    continue;
                }
                // 因为匹配上的被忽略掉了 所以在不允许无效的索引键时就要抛出异常
            } else if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS && context.getOptions().ignoreAliases()) {
                if (failNoIndices) {
                    throw aliasesNotSupportedException(expression);
                } else {
                    continue;
                }
            } else if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM &&
                context.getOptions().includeDataStreams() == false) {
                throw dataStreamsNotSupportedException(expression);
            }


            // 可以看到indexAbstraction中的索引都是经过判断后才设置到 concreteIndices 容器中的
            if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS && context.isResolveToWriteIndex()) {
                IndexMetadata writeIndex = indexAbstraction.getWriteIndex();
                if (writeIndex == null) {
                    throw new IllegalArgumentException("no write index is defined for alias [" + indexAbstraction.getName() + "]." +
                        " The write index may be explicitly disabled using is_write_index=false or the alias points to multiple" +
                        " indices without one being designated as a write index");
                }
                if (addIndex(writeIndex, context)) {
                    concreteIndices.add(writeIndex.getIndex());
                }
                // 逻辑与上面类似
            } else if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM && context.isResolveToWriteIndex()) {
                IndexMetadata writeIndex = indexAbstraction.getWriteIndex();
                if (addIndex(writeIndex, context)) {
                    concreteIndices.add(writeIndex.getIndex());
                }
            } else {
                // 如果通过lookup查找到的 indexAbstraction有多个结果  且options不支持 抛出异常
                if (indexAbstraction.getIndices().size() > 1 && !options.allowAliasesToMultipleIndices()) {
                    String[] indexNames = new String[indexAbstraction.getIndices().size()];
                    int i = 0;
                    for (IndexMetadata indexMetadata : indexAbstraction.getIndices()) {
                        indexNames[i++] = indexMetadata.getIndex().getName();
                    }
                    throw new IllegalArgumentException(indexAbstraction.getType().getDisplayName() + " [" + expression +
                        "] has more than one indices associated with it [" + Arrays.toString(indexNames) +
                        "], can't execute a single index op");
                }

                // 将多个结果写入
                for (IndexMetadata index : indexAbstraction.getIndices()) {
                    if (index.getState() == IndexMetadata.State.CLOSE) {
                        if (failClosed) {
                            throw new IndexClosedException(index.getIndex());
                        } else {
                            // 如果没有禁止被close的索引 那么继续加入
                            if (options.forbidClosedIndices() == false && addIndex(index, context)) {
                                concreteIndices.add(index.getIndex());
                            }
                        }
                    } else if (index.getState() == IndexMetadata.State.OPEN) {
                        if (addIndex(index, context)) {
                            concreteIndices.add(index.getIndex());
                        }
                    } else {
                        throw new IllegalStateException("index state [" + index.getState() + "] not supported");
                    }
                }
            }
        }

        if (options.allowNoIndices() == false && concreteIndices.isEmpty()) {
            IndexNotFoundException infe = new IndexNotFoundException((String) null);
            infe.setResources("index_expression", indexExpressions);
            throw infe;
        }

        // 将所有查询到的索引返回
        return concreteIndices.toArray(new Index[concreteIndices.size()]);
    }

    /**
     * 检测是否允许增加索引
     *
     * @param metadata
     * @param context
     * @return
     */
    private static boolean addIndex(IndexMetadata metadata, Context context) {
        // This used to check the `index.search.throttled` setting, but we eventually decided that it was
        // trappy to hide throttled indices by default. In order to avoid breaking backward compatibility,
        // we changed it to look at the `index.frozen` setting instead, since frozen indices were the only
        // type of index to use the `search_throttled` threadpool at that time.
        // NOTE: We can't reference the Setting object, which is only defined and registered in x-pack.
        return (context.options.ignoreThrottled() && metadata.getSettings().getAsBoolean("index.frozen", false)) == false;
    }

    private static IllegalArgumentException aliasesNotSupportedException(String expression) {
        return new IllegalArgumentException("The provided expression [" + expression + "] matches an " +
            "alias, specify the corresponding concrete indices instead.");
    }

    private static IllegalArgumentException dataStreamsNotSupportedException(String expression) {
        return new IllegalArgumentException("The provided expression [" + expression + "] matches a " +
            "data stream, specify the corresponding concrete indices instead.");
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single concrete index.
     * Callers should make sure they provide proper {@link org.elasticsearch.action.support.IndicesOptions}
     * that require a single index as a result. The indices resolution must in fact return a single index when
     * using this method, an {@link IllegalArgumentException} gets thrown otherwise.
     *
     * @param state   the cluster state containing all the data to resolve to expression to a concrete index
     * @param request The request that defines how the an alias or an index need to be resolved to a concrete index
     *                and the expression that can be resolved to an alias or an index name.
     * @return the concrete index obtained as a result of the index resolution
     * @throws IllegalArgumentException if the index resolution lead to more than one index
     * 从req中获取相关参数 并仅获取一个匹配的index 超过的情况 抛出异常
     */
    public Index concreteSingleIndex(ClusterState state, IndicesRequest request) {
        String indexExpression = request.indices() != null && request.indices().length > 0 ? request.indices()[0] : null;
        Index[] indices = concreteIndices(state, request.indicesOptions(), indexExpression);
        if (indices.length != 1) {
            throw new IllegalArgumentException("unable to return a single index as the index and options" +
                " provided got resolved to multiple indices");
        }
        return indices[0];
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single write index.
     *
     * @param state   the cluster state containing all the data to resolve to expression to a concrete index
     * @param request The request that defines how the an alias or an index need to be resolved to a concrete index
     *                and the expression that can be resolved to an alias or an index name.
     * @return the write index obtained as a result of the index resolution
     * @throws IllegalArgumentException if the index resolution does not lead to an index, or leads to more than one index
     */
    public Index concreteWriteIndex(ClusterState state, IndicesRequest request) {
        if (request.indices() == null || (request.indices() != null && request.indices().length != 1)) {
            throw new IllegalArgumentException("indices request must specify a single index expression");
        }
        return concreteWriteIndex(state, request.indicesOptions(), request.indices()[0], false);
    }

    /**
     * Utility method that allows to resolve an index expression to its corresponding single write index.
     *
     * @param state          the cluster state containing all the data to resolve to expression to a concrete index
     * @param options        defines how the aliases or indices need to be resolved to concrete indices
     * @param index          index that can be resolved to alias or index name.
     * @param allowNoIndices whether to allow resolve to no index
     * @return the write index obtained as a result of the index resolution or null if no index
     * @throws IllegalArgumentException if the index resolution does not lead to an index, or leads to more than one index
     * 因为匹配到的 indexAbstraction 有一个 writeIndex属性 当传入 resolveToWriteIndex 为true时 就是获取这个索引
     */
    public Index concreteWriteIndex(ClusterState state, IndicesOptions options, String index, boolean allowNoIndices) {
        Context context = new Context(state, options, false, true);
        Index[] indices = concreteIndices(context, index);
        if (allowNoIndices && indices.length == 0) {
            return null;
        }
        if (indices.length != 1) {
            throw new IllegalArgumentException("The index expression [" + index +
                "] and options provided did not point to a single write-index");
        }
        return indices[0];
    }

    /**
     * @return whether the specified alias or index exists. If the alias or index contains datemath then that is resolved too.
     */
    public boolean hasIndexOrAlias(String aliasOrIndex, ClusterState state) {
        Context context = new Context(state, IndicesOptions.lenientExpandOpen());
        String resolvedAliasOrIndex = dateMathExpressionResolver.resolveExpression(aliasOrIndex, context);
        return state.metadata().getIndicesLookup().containsKey(resolvedAliasOrIndex);
    }

    /**
     * @return If the specified string is data math expression then this method returns the resolved expression.
     * 使用时间格式解析器 进行解析
     */
    public String resolveDateMathExpression(String dateExpression) {
        // The data math expression resolver doesn't rely on cluster state or indices options, because
        // it just resolves the date math to an actual date.
        return dateMathExpressionResolver.resolveExpression(dateExpression, new Context(null, null));
    }

    /**
     * Resolve an array of expressions to the set of indices and aliases that these expressions match.
     */
    public Set<String> resolveExpressions(ClusterState state, String... expressions) {
        Context context = new Context(state, IndicesOptions.lenientIncludeDataStreamsExpandOpen(), true, false);
        List<String> resolvedExpressions = Arrays.asList(expressions);
        for (ExpressionResolver expressionResolver : expressionResolvers) {
            resolvedExpressions = expressionResolver.resolve(context, resolvedExpressions);
        }
        return Set.copyOf(resolvedExpressions);
    }

    /**
     * Iterates through the list of indices and selects the effective list of filtering aliases for the
     * given index.
     * <p>Only aliases with filters are returned. If the indices list contains a non-filtering reference to
     * the index itself - null is returned. Returns {@code null} if no filtering is required.
     * <b>NOTE</b>: The provided expressions must have been resolved already via {@link #resolveExpressions}.
     */
    public String[] filteringAliases(ClusterState state, String index, Set<String> resolvedExpressions) {
        return indexAliases(state, index, AliasMetadata::filteringRequired, false, resolvedExpressions);
    }

    /**
     * Whether to generate the candidate set from index aliases, or from the set of resolved expressions.
     *
     * @param indexAliasesSize        the number of aliases of the index
     * @param resolvedExpressionsSize the number of resolved expressions
     */
    // pkg-private for testing
    boolean iterateIndexAliases(int indexAliasesSize, int resolvedExpressionsSize) {
        return indexAliasesSize <= resolvedExpressionsSize;
    }

    /**
     * Iterates through the list of indices and selects the effective list of required aliases for the given index.
     * <p>Only aliases where the given predicate tests successfully are returned. If the indices list contains a non-required reference to
     * the index itself - null is returned. Returns {@code null} if no filtering is required.
     * <p><b>NOTE</b>: the provided expressions must have been resolved already via {@link #resolveExpressions}.
     * @param requiredAlias 只有通过谓语条件的才会留下来吧
     * @param resolvedExpressions 这个是此时已知的所有索引名 当将index转换成别名后 别名还必须在resolvedExpressions才可以返回
     *                      获取传入的index相关的别名
     */
    public String[] indexAliases(ClusterState state, String index, Predicate<AliasMetadata> requiredAlias, boolean skipIdentity,
                                 Set<String> resolvedExpressions) {
        if (isAllIndices(resolvedExpressions)) {
            return null;
        }

        // 先通过指定的index 获取元数据信息
        final IndexMetadata indexMetadata = state.metadata().getIndices().get(index);
        if (indexMetadata == null) {
            // Shouldn't happen
            throw new IndexNotFoundException(index);
        }

        if (skipIdentity == false && resolvedExpressions.contains(index)) {
            return null;
        }

        // 获取这个索引相关的所有别名以及元数据信息
        final ImmutableOpenMap<String, AliasMetadata> indexAliases = indexMetadata.getAliases();
        final AliasMetadata[] aliasCandidates;
        // 获取交集

        // 这里只是确认遍历方向 减少遍历次数
        if (iterateIndexAliases(indexAliases.size(), resolvedExpressions.size())) {
            // faster to iterate indexAliases
            aliasCandidates = StreamSupport.stream(Spliterators.spliteratorUnknownSize(indexAliases.values().iterator(), 0), false)
                .map(cursor -> cursor.value)
                .filter(aliasMetadata -> resolvedExpressions.contains(aliasMetadata.alias()))
                .toArray(AliasMetadata[]::new);
        } else {
            // faster to iterate resolvedExpressions
            aliasCandidates = resolvedExpressions.stream()
                .map(indexAliases::get)
                .filter(Objects::nonNull)
                .toArray(AliasMetadata[]::new);
        }

        List<String> aliases = null;
        for (AliasMetadata aliasMetadata : aliasCandidates) {
            // 这里通过谓语进行过滤
            if (requiredAlias.test(aliasMetadata)) {
                // If required - add it to the list of aliases
                if (aliases == null) {
                    aliases = new ArrayList<>();
                }
                aliases.add(aliasMetadata.alias());
            } else {
                // If not, we have a non required alias for this index - no further checking needed
                return null;
            }
        }
        if (aliases == null) {
            return null;
        }
        return aliases.toArray(new String[aliases.size()]);
    }

    /**
     * Resolves the search routing if in the expression aliases are used. If expressions point to concrete indices
     * or aliases with no routing defined the specified routing is used.
     *
     * @param routing     路由信息  应该是要将这个路由信息作用在所有符合条件的索引上
     * @param expressions  用户传入的索引键 之后用于查询数据的
     * @return routing values grouped by concrete index
     * 将索引键查询到的索引的路由信息都设置成传入的参数
     */
    public Map<String, Set<String>> resolveSearchRouting(ClusterState state, @Nullable String routing, String... expressions) {
        List<String> resolvedExpressions = expressions != null ? Arrays.asList(expressions) : Collections.emptyList();
        Context context = new Context(state, IndicesOptions.lenientIncludeDataStreamsExpandOpen());
        // 这里的解析只是针对通配符的情况 比如某些索引键包含通配符 那么就去匹配当前存在的所有索引 成功后只是返回索引键
        for (ExpressionResolver expressionResolver : expressionResolvers) {
            resolvedExpressions = expressionResolver.resolve(context, resolvedExpressions);
        }

        // TODO: it appears that this can never be true?
        // 代表需要获取所有的索引信息
        if (isAllIndices(resolvedExpressions)) {
            // 将路由信息作用在所有命中的索引上  如果没有传入路由信息 直接返回null
            return resolveSearchRoutingAllIndices(state.metadata(), routing);
        }

        Map<String, Set<String>> routings = null;
        Set<String> paramRouting = null;
        // List of indices that don't require any routing
        Set<String> norouting = new HashSet<>();
        if (routing != null) {
            paramRouting = Sets.newHashSet(Strings.splitStringByCommaToArray(routing));
        }

        // 遍历索引键
        for (String expression : resolvedExpressions) {
            // 找到某个索引相关的数据
            IndexAbstraction indexAbstraction = state.metadata().getIndicesLookup().get(expression);
            // 代表是通过 alias命中的
            if (indexAbstraction != null && indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
                IndexAbstraction.Alias alias = (IndexAbstraction.Alias) indexAbstraction;
                // 遍历内部所有的index
                for (Tuple<String, AliasMetadata> item : alias.getConcreteIndexAndAliasMetadatas()) {
                    String concreteIndex = item.v1();
                    AliasMetadata aliasMetadata = item.v2();
                    // 这里是去重的吧 如果已经确定了某个index是没有路由信息的就不需要重复处理了
                    if (!norouting.contains(concreteIndex)) {
                        // 代表存在相关的路由信息
                        if (!aliasMetadata.searchRoutingValues().isEmpty()) {
                            // Routing alias
                            if (routings == null) {
                                routings = new HashMap<>();
                            }
                            Set<String> r = routings.get(concreteIndex);
                            if (r == null) {
                                r = new HashSet<>();
                                routings.put(concreteIndex, r);
                            }
                            // 把找到的所有路由信息设置进去
                            r.addAll(aliasMetadata.searchRoutingValues());
                            // 如果在参数中指定了routing 那么仅保留交集
                            if (paramRouting != null) {
                                r.retainAll(paramRouting);
                            }
                            if (r.isEmpty()) {
                                routings.remove(concreteIndex);
                            }
                        } else {
                            // Non-routing alias
                            // 某个index 没有找到任何路由相关信息 就加入到容器中避免被重复处理
                            if (!norouting.contains(concreteIndex)) {
                                norouting.add(concreteIndex);
                                // 也就是如果原本的路由信息为空 就将参数中的路由信息设置进去 如果一开始就有信息 那么仅保留交集
                                if (paramRouting != null) {
                                    Set<String> r = new HashSet<>(paramRouting);
                                    if (routings == null) {
                                        routings = new HashMap<>();
                                    }
                                    routings.put(concreteIndex, r);
                                } else {
                                    if (routings != null) {
                                        routings.remove(concreteIndex);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                // Index
                // 处理index类型的 IndexAbstraction   这种就直接使用传入的routing
                if (!norouting.contains(expression)) {
                    norouting.add(expression);
                    if (paramRouting != null) {
                        Set<String> r = new HashSet<>(paramRouting);
                        if (routings == null) {
                            routings = new HashMap<>();
                        }
                        routings.put(expression, r);
                    } else {
                        if (routings != null) {
                            routings.remove(expression);
                        }
                    }
                }
            }

        }
        if (routings == null || routings.isEmpty()) {
            return null;
        }
        return routings;
    }

    /**
     * Sets the same routing for all indices
     * 将所有索引的路由信息都修改成传入的值
     * @param routing 路由信息 可能可以拆分
     *                将每个索引 与拆解后的所有路由信息构建映射关系  如果没有路由信息返回null
     */
    public Map<String, Set<String>> resolveSearchRoutingAllIndices(Metadata metadata, String routing) {
        if (routing != null) {
            // ,拆分
            Set<String> r = Sets.newHashSet(Strings.splitStringByCommaToArray(routing));
            Map<String, Set<String>> routings = new HashMap<>();
            String[] concreteIndices = metadata.getConcreteAllIndices();
            for (String index : concreteIndices) {
                routings.put(index, r);
            }
            return routings;
        }
        // 当用户没有传入路由信息时 返回null
        return null;
    }

    /**
     * Identifies whether the array containing index names given as argument refers to all indices
     * The empty or null array identifies all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array maps to all indices, false otherwise
     */
    public static boolean isAllIndices(Collection<String> aliasesOrIndices) {
        return aliasesOrIndices == null || aliasesOrIndices.isEmpty() || isExplicitAllPattern(aliasesOrIndices);
    }

    /**
     * Identifies whether the array containing index names given as argument explicitly refers to all indices
     * The empty or null array doesn't explicitly map to all indices
     *
     * @param aliasesOrIndices the array containing index names
     * @return true if the provided array explicitly maps to all indices, false otherwise
     */
    static boolean isExplicitAllPattern(Collection<String> aliasesOrIndices) {
        return aliasesOrIndices != null && aliasesOrIndices.size() == 1 && Metadata.ALL.equals(aliasesOrIndices.iterator().next());
    }

    /**
     * Identifies whether the first argument (an array containing index names) is a pattern that matches all indices
     *
     * @param indicesOrAliases the array containing index names
     * @param concreteIndices  array containing the concrete indices that the first argument refers to
     * @return true if the first argument is a pattern that maps to all available indices, false otherwise
     * TODO 没看到被使用过
     */
    boolean isPatternMatchingAllIndices(Metadata metadata, String[] indicesOrAliases, String[] concreteIndices) {
        // if we end up matching on all indices, check, if its a wildcard parameter, or a "-something" structure
        if (concreteIndices.length == metadata.getConcreteAllIndices().length && indicesOrAliases.length > 0) {

            //we might have something like /-test1,+test1 that would identify all indices
            //or something like /-test1 with test1 index missing and IndicesOptions.lenient()
            if (indicesOrAliases[0].charAt(0) == '-') {
                return true;
            }

            //otherwise we check if there's any simple regex
            // 只要有一个包含通配符 就返回true
            for (String indexOrAlias : indicesOrAliases) {
                if (Regex.isSimpleMatchPattern(indexOrAlias)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 在匹配过程中需要的上下文信息 只是一个Bean对象
     */
    public static class Context {

        private final ClusterState state;
        private final IndicesOptions options;
        /**
         * 生成该对象后 立刻就要开始解析index表达式了 这里记录的是解析的起始时间
         */
        private final long startTime;
        private final boolean preserveAliases;
        /**
         * 代表当索引键在lookup中匹配到数据后 获取writerIndex属性 作为结果
         */
        private final boolean resolveToWriteIndex;

        Context(ClusterState state, IndicesOptions options) {
            this(state, options, System.currentTimeMillis());
        }

        Context(ClusterState state, IndicesOptions options, boolean preserveAliases, boolean resolveToWriteIndex) {
            this(state, options, System.currentTimeMillis(), preserveAliases, resolveToWriteIndex);
        }

        Context(ClusterState state, IndicesOptions options, long startTime) {
            this(state, options, startTime, false, false);
        }

        protected Context(ClusterState state, IndicesOptions options, long startTime,
                          boolean preserveAliases, boolean resolveToWriteIndex) {
            this.state = state;
            this.options = options;
            this.startTime = startTime;
            this.preserveAliases = preserveAliases;
            this.resolveToWriteIndex = resolveToWriteIndex;
        }

        public ClusterState getState() {
            return state;
        }

        public IndicesOptions getOptions() {
            return options;
        }

        public long getStartTime() {
            return startTime;
        }

        /**
         * This is used to prevent resolving aliases to concrete indices but this also means
         * that we might return aliases that point to a closed index. This is currently only used
         * by {@link #filteringAliases(ClusterState, String, Set)} since it's the only one that needs aliases
         */
        boolean isPreserveAliases() {
            return preserveAliases;
        }

        /**
         * This is used to require that aliases resolve to their write-index. It is currently not used in conjunction
         * with <code>preserveAliases</code>.
         */
        boolean isResolveToWriteIndex() {
            return resolveToWriteIndex;
        }
    }

    /**
     * 表达式解析器对象 在ES中的作用就是将表达式转换成准确的indexName
     */
    private interface ExpressionResolver {

        /**
         * Resolves the list of expressions into other expressions if possible (possible concrete indices and aliases, but
         * that isn't required). The provided implementations can also be left untouched.
         *
         * @return a new list with expressions based on the provided expressions
         */
        List<String> resolve(Context context, List<String> expressions);

    }

    /**
     * Resolves alias/index name expressions with wildcards into the corresponding concrete indices/aliases
     * 基于通配符找到匹配的indexName
     */
    static final class WildcardExpressionResolver implements ExpressionResolver {

        @Override
        public List<String> resolve(Context context, List<String> expressions) {
            IndicesOptions options = context.getOptions();
            Metadata metadata = context.getState().metadata();
            // only check open/closed since if we do not expand to open or closed it doesn't make sense to
            // expand to hidden
            // 如果不支持将通配符拓展成open/close的索引  那么无法解析直接返回
            if (options.expandWildcardsClosed() == false && options.expandWildcardsOpen() == false) {
                return expressions;
            }

            // 代表表达式为空(空就认为是查询所有index) 或者是 "*" 或者 ALL
            if (isEmptyOrTrivialWildcard(expressions)) {
                // 如果不包含数据流 但是数据流不为空 抛出异常
                if (options.includeDataStreams() == false && metadata.dataStreams().isEmpty() == false) {
                    throw dataStreamsNotSupportedException(expressions.toString());
                }
                // 配合options 返回符合条件的IndexName
                return resolveEmptyOrTrivialWildcard(options, metadata);
            }

            // 开始解析
            Set<String> result = innerResolve(context, expressions, options, metadata);

            // 当没有解析出结果时 返回原数据 因为在外层是链式解析 交由下一环解析就好
            if (result == null) {
                return expressions;
            }
            // 当没有解析到任何index时 且!allowNoIndices 抛出异常
            if (result.isEmpty() && !options.allowNoIndices()) {
                IndexNotFoundException infe = new IndexNotFoundException((String) null);
                infe.setResources("index_or_alias", expressions.toArray(new String[0]));
                throw infe;
            }
            return new ArrayList<>(result);
        }

        /**
         * 解析表达式
         *
         * @param context
         * @param expressions index表达式
         * @param options
         * @param metadata
         * @return
         */
        private Set<String> innerResolve(Context context, List<String> expressions, IndicesOptions options, Metadata metadata) {
            Set<String> result = null;
            // 是否检测到了通配符
            boolean wildcardSeen = false;
            for (int i = 0; i < expressions.size(); i++) {
                String expression = expressions.get(i);
                // 不允许传入无效的表达式
                if (Strings.isEmpty(expression)) {
                    throw indexNotFoundException(expression);
                }
                // 如果以"_"开头直接抛出异常
                validateAliasOrIndex(expression);
                // 检查是否直接命中了 name or alias
                if (aliasOrIndexExists(options, metadata, expression)) {
                    if (result != null) {
                        result.add(expression);
                    }
                    continue;
                }
                // 在没有直接命中的情况下 尝试进行解析
                final boolean add;
                // 代表要从符合条件的结果集中排除掉命中的indexName
                // 这个减少必须要前面已经出现了通配符才行  否则 就变成了 indexA,-indexA 这种就没有意义了  所以应该是 indexA*，-indexA 代表明确排除掉indexA
                if (expression.charAt(0) == '-' && wildcardSeen) {
                    add = false;
                    expression = expression.substring(1);
                } else {
                    add = true;
                }
                if (result == null) {
                    // add all the previous ones...
                    // 之前的都通过aliasOrIndexExists 所以不需要解析
                    result = new HashSet<>(expressions.subList(0, i));
                }
                // 如果不包含通配符 代表不需要做解析
                if (Regex.isSimpleMatchPattern(expression) == false) {
                    //TODO why does wildcard resolver throw exceptions regarding non wildcarded expressions? This should not be done here.

                    // 这里就是根据 optional + 本次查询出来的类型进行匹配 不合适立即抛出异常
                    if (options.ignoreUnavailable() == false) {
                        IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(expression);
                        if (indexAbstraction == null) {
                            throw indexNotFoundException(expression);
                        } else if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS && options.ignoreAliases()) {
                            throw aliasesNotSupportedException(expression);
                        } else if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM &&
                            options.includeDataStreams() == false) {
                            throw dataStreamsNotSupportedException(expression);
                        }
                    }
                    if (add) {
                        result.add(expression);
                    } else {
                        result.remove(expression);
                    }
                    continue;
                }

                // 代表哪类index 需要被排除
                // 找到需要被排除的类型
                final IndexMetadata.State excludeState = excludeState(options);
                // 传入的索引名如果包含通配符  在目前所有可选的索引 进行匹配 这样就变成了多个索引名
                final Map<String, IndexAbstraction> matches = matches(context, metadata, expression);
                // 拓展  也就是一个索引名允许被拓展成多个 上面兑换成了所有可能的索引 但是有诸如excludeState这种额外条件 所以要进行过滤
                Set<String> expand = expand(context, excludeState, matches, expression, options.expandWildcardsHidden());
                if (add) {
                    result.addAll(expand);
                } else {
                    result.removeAll(expand);
                }
                if (options.allowNoIndices() == false && matches.isEmpty()) {
                    throw indexNotFoundException(expression);
                }
                if (Regex.isSimpleMatchPattern(expression)) {
                    wildcardSeen = true;
                }
            }
            return result;
        }

        private static void validateAliasOrIndex(String expression) {
            // Expressions can not start with an underscore. This is reserved for APIs. If the check gets here, the API
            // does not exist and the path is interpreted as an expression. If the expression begins with an underscore,
            // throw a specific error that is different from the [[IndexNotFoundException]], which is typically thrown
            // if the expression can't be found.
            if (expression.charAt(0) == '_') {
                throw new InvalidIndexNameException(expression, "must not start with '_'.");
            }
        }

        /**
         * 检测表达式是否直接命中了 别名或者indexName
         *
         * @param options
         * @param metadata
         * @param expression
         * @return
         */
        private static boolean aliasOrIndexExists(IndicesOptions options, Metadata metadata, String expression) {
            // 没办法直接找到匹配的 indexAbstraction 代表expression没有命中别名或者indexName
            IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(expression);
            if (indexAbstraction == null) {
                return false;
            }

            //treat aliases as unavailable indices when ignoreAliases is set to true (e.g. delete index and update aliases api)
            // 如果匹配上的index是别名  且设置了要忽略别名 那么认为没有匹配成功 这样之后它还会被解析
            if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS && options.ignoreAliases()) {
                return false;
            }

            // 如果匹配到的是数据流 且指明了不包含数据流 那么忽略
            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM && options.includeDataStreams() == false) {
                return false;
            }

            return true;
        }

        private static IndexNotFoundException indexNotFoundException(String expression) {
            IndexNotFoundException infe = new IndexNotFoundException(expression);
            infe.setResources("index_or_alias", expression);
            return infe;
        }

        private static IndexMetadata.State excludeState(IndicesOptions options) {
            final IndexMetadata.State excludeState;
            if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                excludeState = null;
            } else if (options.expandWildcardsOpen() && options.expandWildcardsClosed() == false) {
                excludeState = IndexMetadata.State.CLOSE;
            } else if (options.expandWildcardsClosed() && options.expandWildcardsOpen() == false) {
                excludeState = IndexMetadata.State.OPEN;
            } else {
                assert false : "this shouldn't get called if wildcards expand to none";
                excludeState = null;
            }
            return excludeState;
        }

        /**
         * 代表包含通配符  将符合条件的所有index 查询出来
         *
         * @param context
         * @param metadata
         * @param expression
         * @return
         */
        public static Map<String, IndexAbstraction> matches(Context context, Metadata metadata, String expression) {
            if (Regex.isMatchAllPattern(expression)) {
                return filterIndicesLookup(metadata.getIndicesLookup(), null, expression, context.getOptions());
                // 后缀通配符匹配
            } else if (expression.indexOf("*") == expression.length() - 1) {
                return suffixWildcard(context, metadata, expression);
            } else {
                // 代表* 可能存在于任何位置
                return otherWildcard(context, metadata, expression);
            }
        }

        /**
         * 后缀通配符匹配
         *
         * @param context
         * @param metadata
         * @param expression
         * @return
         */
        private static Map<String, IndexAbstraction> suffixWildcard(Context context, Metadata metadata, String expression) {
            assert expression.length() >= 2 : "expression [" + expression + "] should have at least a length of 2";
            // 找到除开通配符的部分
            String fromPrefix = expression.substring(0, expression.length() - 1);
            char[] toPrefixCharArr = fromPrefix.toCharArray();
            // 这里将最后一个char + 1  而所有匹配的字符串中 最后一个char必然是小于它的
            toPrefixCharArr[toPrefixCharArr.length - 1]++;
            String toPrefix = new String(toPrefixCharArr);
            // 只需要在这个子集中进行搜索就可以
            SortedMap<String, IndexAbstraction> subMap = metadata.getIndicesLookup().subMap(fromPrefix, toPrefix);
            return filterIndicesLookup(subMap, null, expression, context.getOptions());
        }

        /**
         * 通配符在其他位置
         *
         * @param context
         * @param metadata
         * @param expression
         * @return
         */
        private static Map<String, IndexAbstraction> otherWildcard(Context context, Metadata metadata, String expression) {
            final String pattern = expression;
            // 这里需要走正则表达式了
            return filterIndicesLookup(metadata.getIndicesLookup(), e -> Regex.simpleMatch(pattern, e.getKey()),
                expression, context.getOptions());
        }

        /**
         * @param indicesLookup 该对象可以通过index alias 查找索引信息  内部包含了所有的索引信息
         * @param filter        基于该函数进行过滤
         * @param expression    使用的表达式
         * @param options
         * @return
         */
        private static Map<String, IndexAbstraction> filterIndicesLookup(SortedMap<String, IndexAbstraction> indicesLookup,
                                                                         Predicate<? super Map.Entry<String, IndexAbstraction>> filter,
                                                                         String expression,
                                                                         IndicesOptions options) {
            boolean shouldConsumeStream = false;
            // 如果不允许通过别名来匹配  从查找容器中排除掉别名的
            Stream<Map.Entry<String, IndexAbstraction>> stream = indicesLookup.entrySet().stream();
            if (options.ignoreAliases()) {
                shouldConsumeStream = true;
                stream = stream.filter(e -> e.getValue().getType() != IndexAbstraction.Type.ALIAS);
            }
            // 当传入了过滤器时 对所有候选的index进行过滤
            if (filter != null) {
                shouldConsumeStream = true;
                stream = stream.filter(filter);
            }
            if (options.includeDataStreams() == false) {
                shouldConsumeStream = true;
                stream = stream.peek(e -> {
                    if (e.getValue().getType() == IndexAbstraction.Type.DATA_STREAM) {
                        throw dataStreamsNotSupportedException(expression);
                    }
                });
            }
            // 代表针对容器做过过滤
            if (shouldConsumeStream) {
                return stream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            } else {
                return indicesLookup;
            }
        }

        /**
         * 将某个索引名拓展成多个索引名 要按照条件进行过滤
         *
         * @param context
         * @param excludeState  这些类型会被排除
         * @param matches       拓展的所有候选索引
         * @param expression
         * @param includeHidden 是否包含 hidden
         * @return
         */
        private static Set<String> expand(Context context, IndexMetadata.State excludeState, Map<String, IndexAbstraction> matches,
                                          String expression, boolean includeHidden) {
            Set<String> expand = new HashSet<>();
            // 遍历之前匹配到的所有index
            for (Map.Entry<String, IndexAbstraction> entry : matches.entrySet()) {
                String aliasOrIndexName = entry.getKey();
                IndexAbstraction indexAbstraction = entry.getValue();

                // 如果不需要隐藏 必然是展示
                // 如果包含隐藏的也是展示
                // 如果表达式是  "." 开头  还不理解 这种情况下也会包含在内
                if (indexAbstraction.isHidden() == false || includeHidden || implicitHiddenMatch(aliasOrIndexName, expression)) {
                    // 允许保留通过别名匹配成功的
                    if (context.isPreserveAliases() && indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
                        expand.add(aliasOrIndexName);
                    } else {
                        // 有些indexAbstraction 可能内部包含一组 index 只要没有命中excludeState 就可以返回
                        for (IndexMetadata meta : indexAbstraction.getIndices()) {
                            if (excludeState == null || meta.getState() != excludeState) {
                                expand.add(meta.getIndex().getName());
                            }
                        }

                    }
                }
            }
            return expand;
        }

        /**
         * 2者都以 "." 开头 且表达式包含通配符  啥意思???
         *
         * @param itemName
         * @param expression
         * @return
         */
        private static boolean implicitHiddenMatch(String itemName, String expression) {
            return itemName.startsWith(".") && expression.startsWith(".") && Regex.isSimpleMatchPattern(expression);
        }

        private boolean isEmptyOrTrivialWildcard(List<String> expressions) {
            return expressions.isEmpty() || (expressions.size() == 1 && (Metadata.ALL.equals(expressions.get(0)) ||
                Regex.isMatchAllPattern(expressions.get(0))));
        }

        /**
         * 处理表达式为空 或者 "*" ALL
         * 根据options中的信息返回不同的IndexName
         *
         * @param options
         * @param metadata
         * @return
         */
        private static List<String> resolveEmptyOrTrivialWildcard(IndicesOptions options, Metadata metadata) {
            // 因为前提是获取所有索引信息了 这里只是根据optional进行过滤
            if (options.expandWildcardsOpen() && options.expandWildcardsClosed() && options.expandWildcardsHidden()) {
                // 获取所有索引
                return Arrays.asList(metadata.getConcreteAllIndices());
                // 只设置了 OPEN CLOSED
            } else if (options.expandWildcardsOpen() && options.expandWildcardsClosed()) {
                // 获取所有可见的索引(非隐藏的)
                return Arrays.asList(metadata.getConcreteVisibleIndices());
                // 获取打开和隐藏的
            } else if (options.expandWildcardsOpen() && options.expandWildcardsHidden()) {
                return Arrays.asList(metadata.getConcreteAllOpenIndices());
            } else if (options.expandWildcardsOpen()) {
                return Arrays.asList(metadata.getConcreteVisibleOpenIndices());
            } else if (options.expandWildcardsClosed() && options.expandWildcardsHidden()) {
                return Arrays.asList(metadata.getConcreteAllClosedIndices());
            } else if (options.expandWildcardsClosed()) {
                return Arrays.asList(metadata.getConcreteVisibleClosedIndices());
            } else {
                return Collections.emptyList();
            }
        }
    }

    /**
     * 日期表达式解析器  于WildcardExpressionResolver之前起作用 就是先将日期部分解析出来  就不细看了
     */
    public static final class DateMathExpressionResolver implements ExpressionResolver {

        private static final DateFormatter DEFAULT_DATE_FORMATTER = DateFormatter.forPattern("uuuu.MM.dd");
        private static final String EXPRESSION_LEFT_BOUND = "<";
        private static final String EXPRESSION_RIGHT_BOUND = ">";
        private static final char LEFT_BOUND = '{';
        private static final char RIGHT_BOUND = '}';
        private static final char ESCAPE_CHAR = '\\';
        private static final char TIME_ZONE_BOUND = '|';

        /**
         * 解析方法
         *
         * @param context
         * @param expressions
         * @return
         */
        @Override
        public List<String> resolve(final Context context, List<String> expressions) {
            List<String> result = new ArrayList<>(expressions.size());
            for (String expression : expressions) {
                result.add(resolveExpression(expression, context));
            }
            return result;
        }

        /**
         * @param expression
         * @param context
         * @return
         */
        @SuppressWarnings("fallthrough")
        String resolveExpression(String expression, final Context context) {
            // 要求表达式 必须以 "<" ">" 作为开头和结尾
            if (expression.startsWith(EXPRESSION_LEFT_BOUND) == false || expression.endsWith(EXPRESSION_RIGHT_BOUND) == false) {
                return expression;
            }

            boolean escape = false;
            boolean inDateFormat = false;
            boolean inPlaceHolder = false;
            final StringBuilder beforePlaceHolderSb = new StringBuilder();
            StringBuilder inPlaceHolderSb = new StringBuilder();
            final char[] text = expression.toCharArray();
            final int from = 1;
            final int length = text.length - 1;
            for (int i = from; i < length; i++) {
                boolean escapedChar = escape;
                if (escape) {
                    escape = false;
                }

                char c = text[i];
                if (c == ESCAPE_CHAR) {
                    if (escapedChar) {
                        beforePlaceHolderSb.append(c);
                        escape = false;
                    } else {
                        escape = true;
                    }
                    continue;
                }
                if (inPlaceHolder) {
                    switch (c) {
                        case LEFT_BOUND:
                            if (inDateFormat && escapedChar) {
                                inPlaceHolderSb.append(c);
                            } else if (!inDateFormat) {
                                inDateFormat = true;
                                inPlaceHolderSb.append(c);
                            } else {
                                throw new ElasticsearchParseException("invalid dynamic name expression [{}]." +
                                    " invalid character in placeholder at position [{}]", new String(text, from, length), i);
                            }
                            break;

                        case RIGHT_BOUND:
                            if (inDateFormat && escapedChar) {
                                inPlaceHolderSb.append(c);
                            } else if (inDateFormat) {
                                inDateFormat = false;
                                inPlaceHolderSb.append(c);
                            } else {
                                String inPlaceHolderString = inPlaceHolderSb.toString();
                                int dateTimeFormatLeftBoundIndex = inPlaceHolderString.indexOf(LEFT_BOUND);
                                String mathExpression;
                                String dateFormatterPattern;
                                DateFormatter dateFormatter;
                                final ZoneId timeZone;
                                if (dateTimeFormatLeftBoundIndex < 0) {
                                    mathExpression = inPlaceHolderString;
                                    dateFormatter = DEFAULT_DATE_FORMATTER;
                                    timeZone = ZoneOffset.UTC;
                                } else {
                                    if (inPlaceHolderString.lastIndexOf(RIGHT_BOUND) != inPlaceHolderString.length() - 1) {
                                        throw new ElasticsearchParseException("invalid dynamic name expression [{}]. missing closing `}`" +
                                            " for date math format", inPlaceHolderString);
                                    }
                                    if (dateTimeFormatLeftBoundIndex == inPlaceHolderString.length() - 2) {
                                        throw new ElasticsearchParseException("invalid dynamic name expression [{}]. missing date format",
                                            inPlaceHolderString);
                                    }
                                    mathExpression = inPlaceHolderString.substring(0, dateTimeFormatLeftBoundIndex);
                                    String patternAndTZid =
                                        inPlaceHolderString.substring(dateTimeFormatLeftBoundIndex + 1, inPlaceHolderString.length() - 1);
                                    int formatPatternTimeZoneSeparatorIndex = patternAndTZid.indexOf(TIME_ZONE_BOUND);
                                    if (formatPatternTimeZoneSeparatorIndex != -1) {
                                        dateFormatterPattern = patternAndTZid.substring(0, formatPatternTimeZoneSeparatorIndex);
                                        timeZone = DateUtils.of(patternAndTZid.substring(formatPatternTimeZoneSeparatorIndex + 1));
                                    } else {
                                        dateFormatterPattern = patternAndTZid;
                                        timeZone = ZoneOffset.UTC;
                                    }
                                    dateFormatter = DateFormatter.forPattern(dateFormatterPattern);
                                }

                                DateFormatter formatter = dateFormatter.withZone(timeZone);
                                DateMathParser dateMathParser = formatter.toDateMathParser();
                                Instant instant = dateMathParser.parse(mathExpression, context::getStartTime, false, timeZone);

                                String time = formatter.format(instant);
                                beforePlaceHolderSb.append(time);
                                inPlaceHolderSb = new StringBuilder();
                                inPlaceHolder = false;
                            }
                            break;

                        default:
                            inPlaceHolderSb.append(c);
                    }
                } else {
                    switch (c) {
                        case LEFT_BOUND:
                            if (escapedChar) {
                                beforePlaceHolderSb.append(c);
                            } else {
                                inPlaceHolder = true;
                            }
                            break;

                        case RIGHT_BOUND:
                            if (!escapedChar) {
                                throw new ElasticsearchParseException("invalid dynamic name expression [{}]." +
                                    " invalid character at position [{}]. `{` and `}` are reserved characters and" +
                                    " should be escaped when used as part of the index name using `\\` (e.g. `\\{text\\}`)",
                                    new String(text, from, length), i);
                            }
                        default:
                            beforePlaceHolderSb.append(c);
                    }
                }
            }

            if (inPlaceHolder) {
                throw new ElasticsearchParseException("invalid dynamic name expression [{}]. date math placeholder is open ended",
                    new String(text, from, length));
            }
            if (beforePlaceHolderSb.length() == 0) {
                throw new ElasticsearchParseException("nothing captured");
            }
            return beforePlaceHolderSb.toString();
        }
    }
}
