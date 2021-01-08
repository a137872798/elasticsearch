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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.AsyncShardFetch.Lister;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 网关分配器
 * 实际上是一种增强逻辑  某些未分配的分片 本身是交给 balancedShardsAllocator进行处理的
 * 但是该对象可以对满足特殊条件的一些 unassigned分片进行拦截   并做一些特殊处理
 */
public class GatewayAllocator implements ExistingShardsAllocator {

    public static final String ALLOCATOR_NAME = "gateway_allocator";

    private static final Logger logger = LogManager.getLogger(GatewayAllocator.class);

    private final RerouteService rerouteService;

    // 可以看出主分片和副本采用不同的分配逻辑
    private final PrimaryShardAllocator primaryShardAllocator;
    private final ReplicaShardAllocator replicaShardAllocator;

    private final ConcurrentMap<ShardId, AsyncShardFetch<NodeGatewayStartedShards>>
        asyncFetchStarted = ConcurrentCollections.newConcurrentMap();

    /**
     * 以分片作为key  一个index对应多个shard
     * 每个shard 有一个primary 以及多个replicate
     * 应该是这样  这个对象存储了某个分片下所有数据文件的元数据信息  因为在一个分片下 primary和replcate 最终的数据应该是一致的
     * 所以只使用了一份 NodeStoreFilesMetadata 或者说这个数据就是从primary上获取的
     */
    private final ConcurrentMap<ShardId, AsyncShardFetch<NodeStoreFilesMetadata>>
        asyncFetchStore = ConcurrentCollections.newConcurrentMap();

    /**
     * 存储了某个时刻观测到的所有node 的瞬时id  如果新增了节点就可以通过 container函数判断出来
     */
    private Set<String> lastSeenEphemeralIds = Collections.emptySet();

    /**
     * 这些实例是什么时候加入到bean容器的
     * @param rerouteService
     * @param client
     */
    @Inject
    public GatewayAllocator(RerouteService rerouteService, NodeClient client) {
        this.rerouteService = rerouteService;
        this.primaryShardAllocator = new InternalPrimaryShardAllocator(client);
        this.replicaShardAllocator = new InternalReplicaShardAllocator(client);
    }

    @Override
    public void cleanCaches() {
        Releasables.close(asyncFetchStarted.values());
        asyncFetchStarted.clear();
        Releasables.close(asyncFetchStore.values());
        asyncFetchStore.clear();
    }

    // for tests
    protected GatewayAllocator() {
        this.rerouteService = null;
        this.primaryShardAllocator = null;
        this.replicaShardAllocator = null;
    }

    /**
     * 返回此时总计有多少请求发出还未收到结果
     * 每个fetch 每次会向所有相关节点发送请求 所以可能有多个inflight请求
     * @return
     */
    @Override
    public int getNumberOfInFlightFetches() {
        int count = 0;
        for (AsyncShardFetch<NodeGatewayStartedShards> fetch : asyncFetchStarted.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        for (AsyncShardFetch<NodeStoreFilesMetadata> fetch : asyncFetchStore.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        return count;
    }

    /**
     * TODO 为什么分片的启动 要将为了主副本分配的相关数据都清除 ???
     * @param startedShards
     * @param allocation
     */
    @Override
    public void applyStartedShards(final List<ShardRouting> startedShards, final RoutingAllocation allocation) {
        for (ShardRouting startedShard : startedShards) {
            Releasables.close(asyncFetchStarted.remove(startedShard.shardId()));
            Releasables.close(asyncFetchStore.remove(startedShard.shardId()));
        }
    }

    /**
     * 因为某个shardId 的分片分配失败了 之后就要重新分配 同时之前的节点上数据可能发生了变化  这里需要重新拉取
     * @param failedShards
     * @param allocation
     */
    @Override
    public void applyFailedShards(final List<FailedShard> failedShards, final RoutingAllocation allocation) {
        for (FailedShard failedShard : failedShards) {
            Releasables.close(asyncFetchStarted.remove(failedShard.getRoutingEntry().shardId()));
            Releasables.close(asyncFetchStore.remove(failedShard.getRoutingEntry().shardId()));
        }
    }

    /**
     * 每当开始一次新的分配任务时 要将之前的数据清理
     * @param allocation
     */
    @Override
    public void beforeAllocation(final RoutingAllocation allocation) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        ensureAsyncFetchStorePrimaryRecency(allocation);
    }

    /**
     * 当主分片分配完成 而副本分片还未分配完成的时候触发
     * @param allocation
     */
    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {
        assert replicaShardAllocator != null;
        // inactiveShards 对应处于init 的分片数量  只要此时主分片处于start状态 那么处于init状态的副本就会开始恢复数据
        if (allocation.routingNodes().hasInactiveShards()) {
            // cancel existing recoveries if we have a better match
            replicaShardAllocator.processExistingRecoveries(allocation);
        }
    }

    /**
     * 为某个 unassigned分片进行分配
     * @param shardRouting
     * @param allocation
     * @param unassignedAllocationHandler  该对象可以更新此时unassigned分片的状态
     */
    @Override
    public void allocateUnassigned(ShardRouting shardRouting, final RoutingAllocation allocation,
                                   UnassignedAllocationHandler unassignedAllocationHandler) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        innerAllocatedUnassigned(allocation, primaryShardAllocator, replicaShardAllocator, shardRouting, unassignedAllocationHandler);
    }

    /**
     *
     * @param allocation  包含所有分片此时的分配情况
     * @param primaryShardAllocator  主分片使用这个分配器
     * @param replicaShardAllocator  副本使用这个分配器
     * @param shardRouting   本次待分配的目标分片
     * @param unassignedAllocationHandler  该对象可以对当前分片进行状态更新
     */
    protected static void innerAllocatedUnassigned(RoutingAllocation allocation,
                                                   PrimaryShardAllocator primaryShardAllocator,
                                                   ReplicaShardAllocator replicaShardAllocator,
                                                   ShardRouting shardRouting,
                                                   ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler) {
        assert shardRouting.unassigned();
        if (shardRouting.primary()) {
            primaryShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
        } else {
            replicaShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
        }
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        assert unassignedShard.unassigned();
        assert routingAllocation.debugDecision();
        if (unassignedShard.primary()) {
            assert primaryShardAllocator != null;
            return primaryShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        } else {
            assert replicaShardAllocator != null;
            return replicaShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        }
    }

    /**
     * Clear the fetched data for the primary to ensure we do not cancel recoveries based on excessively stale data.
     * @param allocation 包含了此时集群中所有分片的分配信息
     *                   每次执行分配任务的时候 某些节点有关某shard的数据可能已经发生了变化 所以要清除之前存储的探测结果
     */
    private void ensureAsyncFetchStorePrimaryRecency(RoutingAllocation allocation) {
        // 本次分配涉及到的所有node
        DiscoveryNodes nodes = allocation.nodes();

        // 当node本身更新或者新增时触发
        if (hasNewNodes(nodes)) {
            final Set<String> newEphemeralIds = StreamSupport.stream(nodes.getDataNodes().spliterator(), false)
                .map(node -> node.value.getEphemeralId()).collect(Collectors.toSet());
            // Invalidate the cache if a data node has been added to the cluster. This ensures that we do not cancel a recovery if a node
            // drops out, we fetch the shard data, then some indexing happens and then the node rejoins the cluster again. There are other
            // ways we could decide to cancel a recovery based on stale data (e.g. changing allocation filters or a primary failure) but
            // making the wrong decision here is not catastrophic so we only need to cover the common case.
            logger.trace(() -> new ParameterizedMessage(
                "new nodes {} found, clearing primary async-fetch-store cache", Sets.difference(newEphemeralIds, lastSeenEphemeralIds)));

            // 清理上次拉取的数据
            asyncFetchStore.values().forEach(fetch -> clearCacheForPrimary(fetch, allocation));
            // recalc to also (lazily) clear out old nodes.
            // 更新最近的瞬时id 通过它可以判断是否有新增的node
            this.lastSeenEphemeralIds = newEphemeralIds;
        }
    }

    /**
     * 清理之前拉取的缓存数据
     * @param fetch
     * @param allocation
     */
    private static void clearCacheForPrimary(AsyncShardFetch<NodeStoreFilesMetadata> fetch,
                                             RoutingAllocation allocation) {
        // 如果此时主分片都还没有激活 那么其他节点存储的探测结果就不可能发生变化  就不需要清理
        ShardRouting primary = allocation.routingNodes().activePrimary(fetch.shardId);
        if (primary != null) {
            // 将该节点对应的缓存数据清除    不过如果主分片已经分配完成
            // 那么之后就不需要再对主分片进行分配了  asyncFetchStore 也就失去意义了   怎么移除数据都不重要
            fetch.clearCacheForNode(primary.currentNodeId());
        }
    }

    /**
     * 2种情况 一种是同一个node的 ephemeralId发生变化  还有一种就是新增了node
     * @param nodes
     * @return
     */
    private boolean hasNewNodes(DiscoveryNodes nodes) {
        for (ObjectObjectCursor<String, DiscoveryNode> node : nodes.getDataNodes()) {
            if (lastSeenEphemeralIds.contains(node.value.getEphemeralId()) == false) {
                return true;
            }
        }
        return false;
    }

    class InternalAsyncFetch<T extends BaseNodeResponse> extends AsyncShardFetch<T> {

        InternalAsyncFetch(Logger logger, String type, ShardId shardId, String customDataPath,
                           Lister<? extends BaseNodesResponse<T>, T> action) {
            super(logger, type, shardId, customDataPath, action);
        }

        /**
         * 将该分片的primary replicate 在当前集群下进行重分配
         * @param shardId
         * @param reason
         */
        @Override
        protected void reroute(ShardId shardId, String reason) {
            logger.trace("{} scheduling reroute for {}", shardId, reason);
            assert rerouteService != null;
            // 通过优先级的概念来避免某些任务因为线程数不足导致的饥饿
            rerouteService.reroute("async_shard_fetch", Priority.HIGH, ActionListener.wrap(
                r -> logger.trace("{} scheduled reroute completed for {}", shardId, reason),
                e -> logger.debug(new ParameterizedMessage("{} scheduled reroute failed for {}", shardId, reason), e)));
        }
    }

    class InternalPrimaryShardAllocator extends PrimaryShardAllocator {

        private final NodeClient client;

        InternalPrimaryShardAllocator(NodeClient client) {
            this.client = client;
        }

        /**
         * 这里定义了 从其他节点拉取分片数据信息的逻辑  根据返回结果决定主分片最合适分配在哪个节点
         * @param shard
         * @param allocation
         * @return
         */
        @Override
        protected AsyncShardFetch.FetchResult<NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // explicitely type lister, some IDEs (Eclipse) are not able to correctly infer the function type
            Lister<BaseNodesResponse<NodeGatewayStartedShards>, NodeGatewayStartedShards> lister = this::listStartedShards;

            AsyncShardFetch<NodeGatewayStartedShards> fetch =
                // 存储每个shard对应的数据   每个fetch对象 内部以nodeId为key存储该shardId的数据在每个node下的描述信息
                asyncFetchStarted.computeIfAbsent(shard.shardId(),
                            // 初始化拉取任务 同时设置监听器
                            shardId -> new InternalAsyncFetch<>(logger, "shard_started", shardId,
                                IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                                lister));

            // 当短时间内收到多个reroute请求时 只要primary处于未分配状态 处理应该要去重
            AsyncShardFetch.FetchResult<NodeGatewayStartedShards> shardState =
                // 这个时候应该还是不知道分片所在的节点的 选择往全节点上发送   IgnoreNodes 代表 必然不会分配到这些node上
                fetch.fetchData(allocation.nodes(), allocation.getIgnoreNodes(shard.shardId()));

            // 本次已经拿到一部分节点的元数据信息了   其余节点由于未能正常访问等原因 会在其他线程中再执行一次reroute 并重新发起拉取请求
            // 总结就是想尽办法拿到所有的节点元数据
            if (shardState.hasData()) {
                // 因为这里是第二次发起的 reroute了  需要将之前的  ignoreNode回填进去
                shardState.processAllocation(allocation);
            }
            // 返回拉取结果
            return shardState;
        }

        /**
         * 定义了 拉取探测数据的逻辑
         * @param shardId   本次查询的分片
         * @param customDataPath  对端节点应该是通过这个目录来定位数据文件的
         * @param nodes     需要发送fetch请求的所有节点
         * @param listener  当获取到结果后回调的监听器
         */
        private void listStartedShards(ShardId shardId, String customDataPath, DiscoveryNode[] nodes,
                                       ActionListener<BaseNodesResponse<NodeGatewayStartedShards>> listener) {
            var request = new TransportNodesListGatewayStartedShards.Request(shardId, customDataPath, nodes);
            client.executeLocally(TransportNodesListGatewayStartedShards.TYPE, request,
                ActionListener.wrap(listener::onResponse, listener::onFailure));
        }
    }

    /**
     * 对副本而言 通过比较lucene数据与主分片数据的匹配度来决定哪些节点更适合作为副本节点
     *
     */
    class InternalReplicaShardAllocator extends ReplicaShardAllocator {

        private final NodeClient client;

        InternalReplicaShardAllocator(NodeClient client) {
            this.client = client;
        }

        /**
         * 开始向集群中相关节点拉取数据
         * @param shard
         * @param allocation
         * @return
         */
        @Override
        protected AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // explicitely type lister, some IDEs (Eclipse) are not able to correctly infer the function type
            Lister<BaseNodesResponse<NodeStoreFilesMetadata>, NodeStoreFilesMetadata> lister = this::listStoreFilesMetadata;

            AsyncShardFetch<NodeStoreFilesMetadata> fetch = asyncFetchStore.computeIfAbsent(shard.shardId(),
                    shardId -> new InternalAsyncFetch<>(logger, "shard_store", shard.shardId(),
                        IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()), lister));

            AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> shardStores =
                    fetch.fetchData(allocation.nodes(), allocation.getIgnoreNodes(shard.shardId()));
            if (shardStores.hasData()) {
                shardStores.processAllocation(allocation);
            }
            return shardStores;
        }

        /**
         * 拉取逻辑会转发到这个方法
         * @param shardId
         * @param customDataPath
         * @param nodes
         * @param listener
         */
        private void listStoreFilesMetadata(ShardId shardId, String customDataPath, DiscoveryNode[] nodes,
                                            ActionListener<BaseNodesResponse<NodeStoreFilesMetadata>> listener) {
            var request = new TransportNodesListShardStoreMetadata.Request(shardId, customDataPath, nodes);
            client.executeLocally(TransportNodesListShardStoreMetadata.TYPE, request,
                ActionListener.wrap(listener::onResponse, listener::onFailure));
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return asyncFetchStore.get(shard.shardId()) != null;
        }
    }
}
