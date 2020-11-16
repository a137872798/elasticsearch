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

package org.elasticsearch.common.path;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * 路径查找树
 * 基于单词查找树算法实现
 * @param <T>
 */
public class PathTrie<T> {

    /**
     * 定义了几种查询类型
     */
    enum TrieMatchingMode {
        /*
         * Retrieve only explicitly mapped nodes, no wildcards are
         * matched.
         * 从根节点出发 精确匹配到某个node
         */
        EXPLICIT_NODES_ONLY,
        /*
         * Retrieve only explicitly mapped nodes, with wildcards
         * allowed as root nodes.
         * 普通节点精确匹配 根节点允许使用通配符
         */
        WILDCARD_ROOT_NODES_ALLOWED,
        /*
         * Retrieve only explicitly mapped nodes, with wildcards
         * allowed as leaf nodes.
         * 叶子节点允许使用通配符   叶子节点就是最后一段路径
         */
        WILDCARD_LEAF_NODES_ALLOWED,
        /*
         * Retrieve both explicitly mapped and wildcard nodes.
         */
        WILDCARD_NODES_ALLOWED
    }

    /**
     * 全部采用精确匹配 或者根节点允许使用通配符
     */
    private static final EnumSet<TrieMatchingMode> EXPLICIT_OR_ROOT_WILDCARD =
            EnumSet.of(TrieMatchingMode.EXPLICIT_NODES_ONLY, TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED);

    public interface Decoder {
        String decode(String value);
    }

    /**
     * 这个好像是负责将 url编码的部分解析
     */
    private final Decoder decoder;
    /**
     * 根节点 路径匹配将从这里开始
     */
    private final TrieNode root;
    /**
     * 根节点对应的path字面量
     */
    private T rootValue;

    /**
     * 每个节点之间应该用 / 进行分隔
     */
    private static final String SEPARATOR = "/";
    private static final String WILDCARD = "*";

    public PathTrie(Decoder decoder) {
        this.decoder = decoder;
        root = new TrieNode(SEPARATOR, null, WILDCARD);
    }

    /**
     *
     */
    public class TrieNode {
        /**
         * 该节点对应的路径信息  root节点的路径为"/"
         */
        private transient String key;
        private transient T value;
        private final String wildcard;

        /**
         * 当使用了 { } 时  去除掉符号，剩余的部分
         */
        private transient String namedWildcard;

        /**
         * 这个就是字典树的实现啊 每个字母作为上级节点 内部一个map存储了下面各种可能的字面量对应的node
         */
        private Map<String, TrieNode> children;

        /**
         * 这里的key 可能是一个携带 {} 的特殊字符
         * @param key
         * @param value
         * @param wildcard
         */
        private TrieNode(String key, T value, String wildcard) {
            this.key = key;
            this.wildcard = wildcard;
            this.value = value;
            // 初始状态没有子级节点
            this.children = emptyMap();
            // 代表这段路径使用了占位符 比如   /aaa/{bb}
            if (isNamedWildcard(key)) {
                namedWildcard = key.substring(key.indexOf('{') + 1, key.indexOf('}'));
            } else {
                namedWildcard = null;
            }
        }

        /**
         * 一种路径只允许出现一种 {XXX} 否则无法区分
         * @param key
         */
        private void updateKeyWithNamedWildcard(String key) {
            this.key = key;
            String newNamedWildcard = key.substring(key.indexOf('{') + 1, key.indexOf('}'));
            if (namedWildcard != null && newNamedWildcard.equals(namedWildcard) == false) {
                throw new IllegalArgumentException("Trying to use conflicting wildcard names for same path: "
                    + namedWildcard + " and " + newNamedWildcard);
            }
            namedWildcard = newNamedWildcard;
        }

        /**
         * 将字典查找树下插入子节点
         * @param key
         * @param child
         */
        private void addInnerChild(String key, TrieNode child) {
            // 写时拷贝 好处是不会在写入过程中降低读取性能
            Map<String, TrieNode> newChildren = new HashMap<>(children);
            newChildren.put(key, child);
            children = unmodifiableMap(newChildren);
        }

        /**
         *
         * @param path  路径被拆解后的数组
         * @param index 代表从数组的第几个开始插入
         * @param value 整条路径对应的值 就是对应的处理器
         */
        private synchronized void insert(String[] path, int index, T value) {
            if (index >= path.length)
                return;

            // 获取路径上的某个片段
            String token = path[index];
            String key = token;
            // 如果这个片段上携带了 {}   那么这个时候就将key 替换为 * 那么 * 应该是一类特殊的节点
            // 其余node 的匹配必须是精确匹配  而只要是携带了{} 的都可以匹配上*
            if (isNamedWildcard(token)) {
                key = wildcard;
            }
            TrieNode node = children.get(key);
            if (node == null) {
                // 代表此时已经解析到最后一段路径了 可以将value设置进去了  就是标准单词查找树啊 权重会设置在整个链路上的最后一环
                T nodeValue = index == path.length - 1 ? value : null;
                node = new TrieNode(token, nodeValue, wildcard);
                // 特殊情况插入以 * 作为key的特殊节点
                addInnerChild(key, node);
            } else {
                // 要求本次token 内部的值要与节点内部的值一致
                if (isNamedWildcard(token)) {
                    node.updateKeyWithNamedWildcard(token);
                }
                /*
                 * If the target node already exists, but is without a value,
                 *  then the value should be updated.
                 * 当处理到最后一段路径时
                 */
                if (index == (path.length - 1)) {
                    // 实际上就是冲突了 一条路径应该只能有一个值 反过来讲就是2个值不能落到同一个path
                    if (node.value != null) {
                        throw new IllegalArgumentException("Path [" + String.join("/", path)+ "] already has a value ["
                                + node.value + "]");
                    } else {
                        node.value = value;
                    }
                }
            }

            // 递归
            node.insert(path, index + 1, value);
        }

        /**
         * 跟insert基本相同 区别就是遇到相同的值时 使用update进行处理
         * @param path
         * @param index
         * @param value
         * @param updater
         */
        private synchronized void insertOrUpdate(String[] path, int index, T value, BiFunction<T, T, T> updater) {
            if (index >= path.length)
                return;

            String token = path[index];
            String key = token;
            if (isNamedWildcard(token)) {
                key = wildcard;
            }
            TrieNode node = children.get(key);
            if (node == null) {
                T nodeValue = index == path.length - 1 ? value : null;
                node = new TrieNode(token, nodeValue, wildcard);
                addInnerChild(key, node);
            } else {
                if (isNamedWildcard(token)) {
                    node.updateKeyWithNamedWildcard(token);
                }
                /*
                 * If the target node already exists, but is without a value,
                 *  then the value should be updated.
                 */
                if (index == (path.length - 1)) {
                    if (node.value != null) {
                        node.value = updater.apply(node.value, value);
                    } else {
                        node.value = value;
                    }
                }
            }

            node.insertOrUpdate(path, index + 1, value, updater);
        }

        private boolean isNamedWildcard(String key) {
            return key.indexOf('{') != -1 && key.indexOf('}') != -1;
        }

        private String namedWildcard() {
            return namedWildcard;
        }

        private boolean isNamedWildcard() {
            return namedWildcard != null;
        }

        /**
         * 根据相关路径查找handler
         * @param path
         * @param index
         * @param params
         * @param trieMatchingMode  定义了匹配规则 比如是否要考虑通配符
         * @return
         */
        public T retrieve(String[] path, int index, Map<String, String> params, TrieMatchingMode trieMatchingMode) {
            if (index >= path.length)
                return null;

            String token = path[index];
            TrieNode node = children.get(token);

            // 代表在查找过程中是否使用了通配符
            boolean usedWildcard;

            if (node == null) {
                // 如果允许使用通配符查找的话 将查询键替换成 *
                if (trieMatchingMode == TrieMatchingMode.WILDCARD_NODES_ALLOWED) {
                    // 通配符也没有找到对应节点 代表path确实没有匹配的handler
                    node = children.get(wildcard);
                    if (node == null) {
                        return null;
                    }
                    usedWildcard = true;
                    // 代表只有在根路径下使用通配符
                } else if (trieMatchingMode == TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED && index == 1) {
                    /*
                     * Allow root node wildcard matches.
                     */
                    node = children.get(wildcard);
                    if (node == null) {
                        return null;
                    }
                    usedWildcard = true;
                    // 叶子节点 也就是最后一个节点使用通配符
                } else if (trieMatchingMode == TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED && index + 1 == path.length) {
                    /*
                     * Allow leaf node wildcard matches.
                     */
                    node = children.get(wildcard);
                    if (node == null) {
                        return null;
                    }
                    usedWildcard = true;
                } else {
                    // 不允许使用通配符的情况下直接返回
                    return null;
                }
            } else {
                // 代表找到了匹配的node    这是一种特殊情况 路径完全匹配的情况下 却没有value 那么就猜测可能想匹配的是
                // /path/{XX} 只是 {XX}的部分为空 才错匹配到了 /path 上 这时替换成children处理器
                if (index + 1 == path.length && node.value == null && children.get(wildcard) != null
                        && EXPLICIT_OR_ROOT_WILDCARD.contains(trieMatchingMode) == false) {
                    /*
                     * If we are at the end of the path, the current node does not have a value but
                     * there is a child wildcard node, use the child wildcard node.
                     */
                    node = children.get(wildcard);
                    usedWildcard = true;
                    // 与上面针对的场景是一样的
                } else if (index == 1 && node.value == null && children.get(wildcard) != null
                        && trieMatchingMode == TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED) {
                    /*
                     * If we are at the root, and root wildcards are allowed, use the child wildcard
                     * node.
                     */
                    node = children.get(wildcard);
                    usedWildcard = true;
                } else {
                    // 如果本次解析的path 片段就是 * 那么也使用了通配符
                    usedWildcard = token.equals(wildcard);
                }
            }

            put(params, node, token);

            // 正常情况下 查询到最后一段路径了 返回对应的handler
            if (index == (path.length - 1)) {
                return node.value;
            }

            T nodeValue = node.retrieve(path, index + 1, params, trieMatchingMode);
            // 不是上面的判断逻辑里面已经包含了使用通配符查询了嘛  不细看了
            if (nodeValue == null && !usedWildcard && trieMatchingMode != TrieMatchingMode.EXPLICIT_NODES_ONLY) {
                node = children.get(wildcard);
                if (node != null) {
                    put(params, node, token);
                    nodeValue = node.retrieve(path, index + 1, params, trieMatchingMode);
                }
            }

            return nodeValue;
        }

        /**
         * 将使用到的通配符存储到map中
         * @param params
         * @param node
         * @param value  避免中途出现了 url编码的字符串 所以检查每段字符串 并进行解码
         */
        private void put(Map<String, String> params, TrieNode node, String value) {
            if (params != null && node.isNamedWildcard()) {
                params.put(node.namedWildcard(), decoder.decode(value));
            }
        }

        @Override
        public String toString() {
            return key;
        }
    }

    /**
     * 开始补充单词查找树
     * @param path
     * @param value
     */
    public void insert(String path, T value) {
        String[] strings = path.split(SEPARATOR);
        // 代表本次设置的是根路径
        if (strings.length == 0) {
            if (rootValue != null) {
                throw new IllegalArgumentException("Path [/] already has a value [" + rootValue + "]");
            }
            // 设置根路径的值
            rootValue = value;
            return;
        }
        int index = 0;
        // Supports initial delimiter.
        if (strings[0].isEmpty()) {
            index = 1;
        }
        // 从第一个位置开始插入 应该每插入一个片段 就会继续解析
        root.insert(strings, index, value);
    }

    /**
     * Insert a value for the given path. If the path already exists, replace the value with:
     * <pre>
     * value = updater.apply(oldValue, newValue);
     * </pre>
     * allowing the value to be updated if desired.
     * 往字典查找树插入一个新的路径信息 当出现冲突时选择替换
     */
    public void insertOrUpdate(String path, T value, BiFunction<T, T, T> updater) {
        String[] strings = path.split(SEPARATOR);
        // 代表插入的就是根路径
        if (strings.length == 0) {
            if (rootValue != null) {
                rootValue = updater.apply(rootValue, value);
            } else {
                rootValue = value;
            }
            return;
        }
        int index = 0;
        // Supports initial delimiter.
        if (strings[0].isEmpty()) {
            index = 1;
        }
        root.insertOrUpdate(strings, index, value, updater);
    }

    /**
     * 通过指定路径检索到对应的value对象 实际上就是请求处理器   默认情况下支持使用{}
     * @param path
     * @return
     */
    public T retrieve(String path) {
        return retrieve(path, null, TrieMatchingMode.WILDCARD_NODES_ALLOWED);
    }

    public T retrieve(String path, Map<String, String> params) {
        return retrieve(path, params, TrieMatchingMode.WILDCARD_NODES_ALLOWED);
    }

    /**
     *
     * @param path
     * @param params  当发现了通配符 会存储到这个容器中
     * @param trieMatchingMode
     * @return
     */
    public T retrieve(String path, Map<String, String> params, TrieMatchingMode trieMatchingMode) {
        if (path.length() == 0) {
            return rootValue;
        }
        String[] strings = path.split(SEPARATOR);
        if (strings.length == 0) {
            return rootValue;
        }
        int index = 0;

        // Supports initial delimiter.
        if (strings[0].isEmpty()) {
            index = 1;
        }

        return root.retrieve(strings, index, params, trieMatchingMode);
    }

    /**
     * Returns an iterator of the objects stored in the {@code PathTrie}, using
     * all possible {@code TrieMatchingMode} modes. The {@code paramSupplier}
     * is called between each invocation of {@code next()} to supply a new map
     * of parameters.
     * 采用不同的模式看看有没有可能返回不同的处理器
     */
    public Iterator<T> retrieveAll(String path, Supplier<Map<String, String>> paramSupplier) {
        return new Iterator<>() {

            private int mode;

            @Override
            public boolean hasNext() {
                return mode < TrieMatchingMode.values().length;
            }

            @Override
            public T next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException("called next() without validating hasNext()! no more modes available");
                }
                return retrieve(path, paramSupplier.get(), TrieMatchingMode.values()[mode++]);
            }
        };
    }
}
