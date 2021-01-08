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
package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptySet;

/**
 * Allows to asynchronously fetch shard related data from other nodes for allocation, without blocking
 * the cluster update thread.
 * <p>
 * The async fetch logic maintains a map of which nodes are being fetched from in an async manner,
 * and once the results are back, it makes sure to schedule a reroute to make sure those results will
 * be taken into account.
 * 表示从某个分片拉取数据的动作
 * 该对象应该由leader节点掌控
 */
public abstract class AsyncShardFetch<T extends BaseNodeResponse> implements Releasable {

    /**
     * An action that lists the relevant shard data that needs to be fetched.
     */
    public interface Lister<NodesResponse extends BaseNodesResponse<NodeResponse>, NodeResponse extends BaseNodeResponse> {
        void list(ShardId shardId, @Nullable String customDataPath, DiscoveryNode[] nodes, ActionListener<NodesResponse> listener);
    }

    protected final Logger logger;
    protected final String type;
    /**
     * 拉取的分片id  应该是只会拉取shard.primary的数据 副本应该是不在考虑范围内的
     */
    protected final ShardId shardId;
    /**
     * 自定义的数据路径  推测应该是告知去目标分片的哪个目录获取数据
     */
    protected final String customDataPath;

    /**
     * AsyncShardFetch 本身只是一个模板 代表针对某一分片 在哪些节点上执行拉取任务
     * 具体的拉取动作由该对象决定 同时还决定了 res 的类型
     */
    private final Lister<BaseNodesResponse<T>, T> action;
    /**
     * 发起的通往某个节点的拉取任务会被暂存在这里
     * 同时当获取到结果后 会作为缓存 存储在内部
     * key: nodeId
     */
    private final Map<String, NodeEntry<T>> cache = new HashMap<>();

    /**
     * 代表不会从哪些节点上获取该分片的数据
     */
    private final Set<String> nodesToIgnore = new HashSet<>();

    /**
     * 此时总计发起了几轮拉取任务
     */
    private final AtomicLong round = new AtomicLong();
    private boolean closed;

    /**
     * 推测该对象是指定一个分片 然后从集群中的各个节点获取有关该分片的数据
     * @param logger
     * @param type
     * @param shardId
     * @param customDataPath  这个自定义数据路径指的是什么  以及为什么在网关这个模块中会需要这样一个组件
     * @param action
     */
    protected AsyncShardFetch(Logger logger, String type, ShardId shardId, String customDataPath,
                              Lister<? extends BaseNodesResponse<T>, T> action) {
        this.logger = logger;
        this.type = type;
        this.shardId = Objects.requireNonNull(shardId);
        this.customDataPath = Objects.requireNonNull(customDataPath);
        this.action = (Lister<BaseNodesResponse<T>, T>) action;
    }

    @Override
    public synchronized void close() {
        this.closed = true;
    }

    /**
     * Returns the
     * number of async fetches that are currently ongoing.
     * 此时正在向多少个节点拉取数据
     */
    public synchronized int getNumberOfInFlightFetches() {
        int count = 0;
        for (NodeEntry<T> nodeEntry : cache.values()) {
            if (nodeEntry.isFetching()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Fetches the data for the relevant shard. If there any ongoing async fetches going on, or new ones have
     * been initiated by this call, the result will have no data.
     * <p>
     * The ignoreNodes are nodes that are supposed to be ignored for this round, since fetching is async, we need
     * to keep them around and make sure we add them back when all the responses are fetched and returned.
     * @param nodes   所有数据节点
     * @param ignoreNodes  因为本次shardId的分片必然不会分配在该node上 所以不需要探测了
     */
    public synchronized FetchResult<T> fetchData(DiscoveryNodes nodes, Set<String> ignoreNodes) {
        if (closed) {
            throw new IllegalStateException(shardId + ": can't fetch data on closed async fetch");
        }
        // 会先从ignore对应的node拉取数据 但是在处理时 又会将ignore设置到结果中 这样在处理时就可以跳过这些节点
        nodesToIgnore.addAll(ignoreNodes);

        // 因为短时间内是可能会连续发起 reroute的 都需要获取某分片在某节点上的描述信息  为了避免重复执行 就将以node为单位进行去重
        fillShardCacheWithDataNodes(cache, nodes);

        // 这里是本次还未发起请求的entry   实际上是并发场景下的去重
        List<NodeEntry<T>> nodesToFetch = findNodesToFetch(cache);

        // 可以看到 一旦有新的节点需要拉取数据  之前其他节点拉取的数据并不会立即使用 而是确保获取某次集群中所有的节点后才进行选择
        if (nodesToFetch.isEmpty() == false) {
            // mark all node as fetching and go ahead and async fetch them
            // use a unique round id to detect stale responses in processAsyncFetch
            // 每执行一次拉取任务 就会将round+1
            final long fetchingRound = round.incrementAndGet();
            for (NodeEntry<T> nodeEntry : nodesToFetch) {
                // 标记这些node的描述信息正在拉取中  同时设置轮数
                nodeEntry.markAsFetching(fetchingRound);
            }
            DiscoveryNode[] discoNodesToFetch = nodesToFetch.stream().map(NodeEntry::getNodeId).map(nodes::get)
                .toArray(DiscoveryNode[]::new);
            // 执行拉取任务
            asyncFetch(discoNodesToFetch, fetchingRound);
        }

        // if we are still fetching, return null to indicate it
        // 此时还没有任何数据可处理 处于等待恢复结果的阶段  先返回一个空结果
        if (hasAnyNodeFetching(cache)) {
            return new FetchResult<>(shardId, null, emptySet());
        } else {

            // 这时所有node的数据都已经获得  可以根据它们之前设置的元数据信息来判断哪个节点更适合做主分片节点

            // nothing to fetch, yay, build the return value
            Map<DiscoveryNode, T> fetchData = new HashMap<>();

            // 存储拉取数据失败的节点
            Set<String> failedNodes = new HashSet<>();
            for (Iterator<Map.Entry<String, NodeEntry<T>>> it = cache.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, NodeEntry<T>> entry = it.next();
                String nodeId = entry.getKey();
                NodeEntry<T> nodeEntry = entry.getValue();

                DiscoveryNode node = nodes.get(nodeId);
                if (node != null) {
                    // 在尝试获取该节点相关的数据时失败  不作为考虑对象
                    if (nodeEntry.isFailed()) {
                        // if its failed, remove it from the list of nodes, so if this run doesn't work
                        // we try again next round to fetch it again
                        it.remove();
                        failedNodes.add(nodeEntry.getNodeId());
                    } else {
                        if (nodeEntry.getValue() != null) {
                            fetchData.put(node, nodeEntry.getValue());
                        }
                    }
                }
            }

            // 本轮处理结束后  清空 ignore内的数据
            Set<String> allIgnoreNodes = Set.copyOf(nodesToIgnore);
            // clear the nodes to ignore, we had a successful run in fetching everything we can
            // we need to try them if another full run is needed
            nodesToIgnore.clear();
            // if at least one node failed, make sure to have a protective reroute
            // here, just case this round won't find anything, and we need to retry fetching data
            // 这里的逻辑是针对本轮没有任何结果返回 或者说所有节点都被排除在外 所以在异步线程中又发起了一个拉取任务
            // 新的reroute调用并不会立即产生结果 也是通过回调触发分配  如果本次已经产生分配结果了 那么下次触发回调就不会继续分配了
            if (failedNodes.isEmpty() == false || allIgnoreNodes.isEmpty() == false) {
                reroute(shardId, "nodes failed [" + failedNodes.size() + "], ignored [" + allIgnoreNodes.size() + "]");
            }
            // 返回本次用于参考选择节点的数据  allIgnoreNodes 对应本次排除在外的节点
            return new FetchResult<>(shardId, fetchData, allIgnoreNodes);
        }
    }

    /**
     * Called by the response handler of the async action to fetch data. Verifies that its still working
     * on the same cache generation, otherwise the results are discarded. It then goes and fills the relevant data for
     * the shard (response + failures), issuing a reroute at the end of it to make sure there will be another round
     * of allocations taking this new data into account.
     * @param responses 对应处理成功的结果
     * @param failures 对应处理失败的结果
     * @param fetchingRound 代表当前是第几轮发起的请求
     *
     *                      在为每个分片选择分配的结果时  需要先获取每个节点有关该分片的元数据信息
     *                      如果上一轮是主分片 那么本次就可以将主分片分配到相同的节点上  因为主分片上的数据是最齐全的
     *
     */
    protected synchronized void processAsyncFetch(List<T> responses, List<FailedNodeException> failures, long fetchingRound) {
        if (closed) {
            // we are closed, no need to process this async fetch at all
            logger.trace("{} ignoring fetched [{}] results, already closed", shardId, type);
            return;
        }
        logger.trace("{} processing fetched [{}] results", shardId, type);

        if (responses != null) {
            for (T response : responses) {
                // 在执行拉取任务前 相关entry都应该已经被填入
                NodeEntry<T> nodeEntry = cache.get(response.getNode().getId());
                if (nodeEntry != null) {
                    // 当发起下一轮请求时 会忽略上一轮的结果  照理说加锁了 是不会出现这种情况的
                    if (nodeEntry.getFetchingRound() != fetchingRound) {
                        assert nodeEntry.getFetchingRound() > fetchingRound : "node entries only replaced by newer rounds";
                        logger.trace("{} received response for [{}] from node {} for an older fetching round (expected: {} but was: {})",
                            shardId, nodeEntry.getNodeId(), type, nodeEntry.getFetchingRound(), fetchingRound);
                        // 该节点出现了某种不可逆的错误  只能等待node先下线 (这样cache中的数据会被清理) 并在再次上线后重新拉取该node的数据
                    } else if (nodeEntry.isFailed()) {
                        logger.trace("{} node {} has failed for [{}] (failure [{}])", shardId, nodeEntry.getNodeId(), type,
                            nodeEntry.getFailure());
                        // 设置结果
                    } else {
                        // if the entry is there, for the right fetching round and not marked as failed already, process it
                        logger.trace("{} marking {} as done for [{}], result is [{}]", shardId, nodeEntry.getNodeId(), type, response);
                        nodeEntry.doneFetching(response);
                    }
                }
            }
        }

        // 有些节点的拉取失败   异常情况属于失败
        if (failures != null) {
            for (FailedNodeException failure : failures) {
                logger.trace("{} processing failure {} for [{}]", shardId, failure, type);
                NodeEntry<T> nodeEntry = cache.get(failure.nodeId());
                if (nodeEntry != null) {
                    if (nodeEntry.getFetchingRound() != fetchingRound) {
                        assert nodeEntry.getFetchingRound() > fetchingRound : "node entries only replaced by newer rounds";
                        logger.trace("{} received failure for [{}] from node {} for an older fetching round (expected: {} but was: {})",
                            shardId, nodeEntry.getNodeId(), type, nodeEntry.getFetchingRound(), fetchingRound);

                        // 代表首次设置成failure
                    } else if (nodeEntry.isFailed() == false) {
                        // if the entry is there, for the right fetching round and not marked as failed already, process it
                        Throwable unwrappedCause = ExceptionsHelper.unwrapCause(failure.getCause());
                        // if the request got rejected or timed out, we need to try it again next time...
                        // 可重试异常 等待下一轮拉取
                        if (unwrappedCause instanceof EsRejectedExecutionException ||
                            unwrappedCause instanceof ReceiveTimeoutTransportException ||
                            unwrappedCause instanceof ElasticsearchTimeoutException) {
                            nodeEntry.restartFetching();
                        } else {
                            logger.warn(() -> new ParameterizedMessage("{}: failed to list shard for {} on node [{}]",
                                shardId, type, failure.nodeId()), failure);
                            // 设置失败结果
                            nodeEntry.doneFetching(failure.getCause());
                        }
                    }
                }
            }
        }

        reroute(shardId, "post_response");
    }

    /**
     * Implement this in order to scheduled another round that causes a call to fetch data.
     */
    protected abstract void reroute(ShardId shardId, String reason);

    /**
     * Clear cache for node, ensuring next fetch will fetch a fresh copy.
     */
    synchronized void clearCacheForNode(String nodeId) {
        cache.remove(nodeId);
    }

    /**
     * Fills the shard fetched data with new (data) nodes and a fresh NodeEntry, and removes from
     * it nodes that are no longer part of the state.
     * @param shardCache 存储探测结果的缓存
     * @param nodes      本次需要探测的所有节点
     */
    private void fillShardCacheWithDataNodes(Map<String, NodeEntry<T>> shardCache, DiscoveryNodes nodes) {
        // verify that all current data nodes are there
        for (ObjectObjectCursor<String, DiscoveryNode> cursor : nodes.getDataNodes()) {
            DiscoveryNode node = cursor.value;

            // 代表是新增的 包装成 entry后加入到缓存中
            if (shardCache.containsKey(node.getId()) == false) {
                shardCache.put(node.getId(), new NodeEntry<T>(node.getId()));
            }
        }
        // remove nodes that are not longer part of the data nodes set
        // 不需要再维护某些node时  将NodeEntry清除
        shardCache.keySet().removeIf(nodeId -> !nodes.nodeExists(nodeId));
    }

    /**
     * Finds all the nodes that need to be fetched. Those are nodes that have no
     * data, and are not in fetch mode.
     * 找到所有还未填充数据并且还未开始拉取数据的 entry
     */
    private List<NodeEntry<T>> findNodesToFetch(Map<String, NodeEntry<T>> shardCache) {
        List<NodeEntry<T>> nodesToFetch = new ArrayList<>();
        for (NodeEntry<T> nodeEntry : shardCache.values()) {
            if (nodeEntry.hasData() == false && nodeEntry.isFetching() == false) {
                nodesToFetch.add(nodeEntry);
            }
        }
        return nodesToFetch;
    }

    /**
     * Are there any nodes that are fetching data?
     */
    private boolean hasAnyNodeFetching(Map<String, NodeEntry<T>> shardCache) {
        for (NodeEntry<T> nodeEntry : shardCache.values()) {
            if (nodeEntry.isFetching()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Async fetches data for the provided shard with the set of nodes that need to be fetched from.
     * 执行异步拉取任务
     */
    void asyncFetch(final DiscoveryNode[] nodes, long fetchingRound) {
        logger.trace("{} fetching [{}] from {}", shardId, type, nodes);
        action.list(shardId, customDataPath, nodes, new ActionListener<BaseNodesResponse<T>>() {

            /**
             * 当获取到结果后触发该回调
             * @param response
             */
            @Override
            public void onResponse(BaseNodesResponse<T> response) {
                processAsyncFetch(response.getNodes(), response.failures(), fetchingRound);
            }

            @Override
            public void onFailure(Exception e) {
                // 当出现异常 没有正常返回时 直接将所有node都填充失败信息
                List<FailedNodeException> failures = new ArrayList<>(nodes.length);
                for (final DiscoveryNode node: nodes) {
                    failures.add(new FailedNodeException(node.getId(), "total failure in fetching", e));
                }
                processAsyncFetch(null, failures, fetchingRound);
            }
        });
    }

    /**
     * The result of a fetch operation. Make sure to first check {@link #hasData()} before
     * fetching the actual data.
     * 某次fetch的结果
     */
    public static class FetchResult<T extends BaseNodeResponse> {

        /**
         * 本次拉取针对的分片
         */
        private final ShardId shardId;
        /**
         * 从每个节点上拉取的数据
         */
        private final Map<DiscoveryNode, T> data;
        private final Set<String> ignoreNodes;

        public FetchResult(ShardId shardId, Map<DiscoveryNode, T> data, Set<String> ignoreNodes) {
            this.shardId = shardId;
            this.data = data;
            this.ignoreNodes = ignoreNodes;
        }

        /**
         * Does the result actually contain data? If not, then there are on going fetch
         * operations happening, and it should wait for it.
         */
        public boolean hasData() {
            return data != null;
        }

        /**
         * Returns the actual data, note, make sure to check {@link #hasData()} first and
         * only use this when there is an actual data.
         */
        public Map<DiscoveryNode, T> getData() {
            assert data != null : "getData should only be called if there is data to be fetched, please check hasData first";
            return this.data;
        }

        /**
         * Process any changes needed to the allocation based on this fetch result.
         * 因为这里是第二次发起的 reroute了  需要将之前的  ignoreNode回填进去
         * 第一次reroute发起拉取任务
         * 第二次根据上次拉取的结果进行分配   这里相当于是尽量恢复之前的状态
         */
        public void processAllocation(RoutingAllocation allocation) {
            for (String ignoreNode : ignoreNodes) {
                allocation.addIgnoreShardForNode(shardId, ignoreNode);
            }
        }
    }

    /**
     * A node entry, holding the state of the fetched data for a specific shard
     * for a giving node.
     * 对应某个节点的探测结果
     */
    static class NodeEntry<T> {
        private final String nodeId;
        /**
         * 正在向该节点拉取数据  初始化时为false
         */
        private boolean fetching;
        @Nullable
        private T value;
        private boolean valueSet;
        private Throwable failure;
        /**
         * 代表在第几轮发起拉取
         */
        private long fetchingRound;

        NodeEntry(String nodeId) {
            this.nodeId = nodeId;
        }

        String getNodeId() {
            return this.nodeId;
        }

        boolean isFetching() {
            return fetching;
        }

        /**
         * 代表在对应的 fetch任务中 是第几轮发起的拉取
         * @param fetchingRound
         */
        void markAsFetching(long fetchingRound) {
            assert fetching == false : "double marking a node as fetching";
            this.fetching = true;
            this.fetchingRound = fetchingRound;
        }

        /**
         * 当fetch动作成功时 修改标识 以及设置结果
         * @param value
         */
        void doneFetching(T value) {
            assert fetching : "setting value but not in fetching mode";
            assert failure == null : "setting value when failure already set";
            this.valueSet = true;
            this.value = value;
            this.fetching = false;
        }

        /**
         * 代表从该node拉取shard信息 得到了不可重试的失败结果
         * @param failure
         */
        void doneFetching(Throwable failure) {
            assert fetching : "setting value but not in fetching mode";
            assert valueSet == false : "setting failure when already set value";
            assert failure != null : "setting failure can't be null";
            this.failure = failure;
            this.fetching = false;
        }

        void restartFetching() {
            assert fetching : "restarting fetching, but not in fetching mode";
            assert valueSet == false : "value can't be set when restarting fetching";
            assert failure == null : "failure can't be set when restarting fetching";
            this.fetching = false;
        }

        boolean isFailed() {
            return failure != null;
        }

        boolean hasData() {
            return valueSet || failure != null;
        }

        Throwable getFailure() {
            assert hasData() : "getting failure when data has not been fetched";
            return failure;
        }

        @Nullable
        T getValue() {
            assert failure == null : "trying to fetch value, but its marked as failed, check isFailed";
            assert valueSet : "value is not set, hasn't been fetched yet";
            return value;
        }

        long getFetchingRound() {
            return fetchingRound;
        }
    }
}
