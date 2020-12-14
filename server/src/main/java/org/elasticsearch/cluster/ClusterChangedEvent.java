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

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An event received by the local node, signaling that the cluster state has changed.
 * 代表集群变化的事件
 */
public class ClusterChangedEvent {

    private final String source;

    private final ClusterState previousState;

    private final ClusterState state;

    private final DiscoveryNodes.Delta nodesDelta;

    public ClusterChangedEvent(String source, ClusterState state, ClusterState previousState) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(previousState, "previousState must not be null");
        this.source = source;
        this.state = state;
        this.previousState = previousState;
        // 比较节点前后变化 生成delta对象
        this.nodesDelta = state.nodes().delta(previousState.nodes());
    }

    /**
     * The source that caused this cluster event to be raised.
     */
    public String source() {
        return this.source;
    }

    /**
     * The new cluster state that caused this change event.
     */
    public ClusterState state() {
        return this.state;
    }

    /**
     * The previous cluster state for this change event.
     */
    public ClusterState previousState() {
        return this.previousState;
    }

    /**
     * Returns <code>true</code> iff the routing tables (for all indices) have
     * changed between the previous cluster state and the current cluster state.
     * Note that this is an object reference equality test, not an equals test.
     * 代表路由表是否发生了变化
     */
    public boolean routingTableChanged() {
        return state.routingTable() != previousState.routingTable();
    }

    /**
     * Returns <code>true</code> iff the routing table has changed for the given index.
     * Note that this is an object reference equality test, not an equals test.
     * 本次该index对应的路由信息是否发生了变化
     */
    public boolean indexRoutingTableChanged(String index) {
        Objects.requireNonNull(index, "index must not be null");

        // 如果前后路由表中都不包含这个索引 代表没有发生变化
        if (!state.routingTable().hasIndex(index) && !previousState.routingTable().hasIndex(index)) {
            return false;
        }
        // 如果该索引的路由表信息发生变化 也返回true
        if (state.routingTable().hasIndex(index) && previousState.routingTable().hasIndex(index)) {
            return state.routingTable().index(index) != previousState.routingTable().index(index);
        }
        // 其余情况代表之前有现在无 或者之前无现在有
        return true;
    }

    /**
     * Returns the indices created in this event
     * 返回此时创建的所有索引
     */
    public List<String> indicesCreated() {
        // 通过元数据是否发生了变化 可以快速的断定是否有新的索引被创建
        if (!metadataChanged()) {
            return Collections.emptyList();
        }
        List<String> created = null;
        for (ObjectCursor<String> cursor : state.metadata().indices().keys()) {
            String index = cursor.value;
            // 找到没有存储在元数据中的索引 就是本次新建的
            if (!previousState.metadata().hasIndex(index)) {
                if (created == null) {
                    created = new ArrayList<>();
                }
                created.add(index);
            }
        }
        return created == null ? Collections.<String>emptyList() : created;
    }

    /**
     * Returns the indices deleted in this event
     * 找到本次删除的索引信息
     */
    public List<Index> indicesDeleted() {
        // 如果此时数据还没有恢复
        if (previousState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // working off of a non-initialized previous state, so use the tombstones for index deletions
            return indicesDeletedFromTombstones();
        } else {
            // examine the diffs in index metadata between the previous and new cluster states to get the deleted indices
            // 基于前后2个集群metadata  比较本次删除的索引
            return indicesDeletedFromClusterState();
        }
    }

    /**
     * Returns <code>true</code> iff the metadata for the cluster has changed between
     * the previous cluster state and the new cluster state. Note that this is an object
     * reference equality test, not an equals test.
     */
    public boolean metadataChanged() {
        return state.metadata() != previousState.metadata();
    }

    /**
     * Returns a set of custom meta data types when any custom metadata for the cluster has changed
     * between the previous cluster state and the new cluster state. custom meta data types are
     * returned iff they have been added, updated or removed between the previous and the current state
     * 返回发生变化的用户自定义的数据
     */
    public Set<String> changedCustomMetadataSet() {
        Set<String> result = new HashSet<>();
        ImmutableOpenMap<String, Metadata.Custom> currentCustoms = state.metadata().customs();
        ImmutableOpenMap<String, Metadata.Custom> previousCustoms = previousState.metadata().customs();
        if (currentCustoms.equals(previousCustoms) == false) {
            for (ObjectObjectCursor<String, Metadata.Custom> currentCustomMetadata : currentCustoms) {
                // new custom md added or existing custom md changed
                // 找到本次新增的 或者发生变化的自定义数据
                if (previousCustoms.containsKey(currentCustomMetadata.key) == false
                        || currentCustomMetadata.value.equals(previousCustoms.get(currentCustomMetadata.key)) == false) {
                    result.add(currentCustomMetadata.key);
                }
            }
            // existing custom md deleted
            // 删除的也加入到列表中    (这样不是没法区分了吗 ???)
            for (ObjectObjectCursor<String, Metadata.Custom> previousCustomMetadata : previousCustoms) {
                if (currentCustoms.containsKey(previousCustomMetadata.key) == false) {
                    result.add(previousCustomMetadata.key);
                }
            }
        }
        return result;
    }

    /**
     * Returns <code>true</code> iff the {@link IndexMetadata} for a given index
     * has changed between the previous cluster state and the new cluster state.
     * Note that this is an object reference equality test, not an equals test.
     */
    public static boolean indexMetadataChanged(IndexMetadata metadata1, IndexMetadata metadata2) {
        assert metadata1 != null && metadata2 != null;
        // no need to check on version, since disco modules will make sure to use the
        // same instance if its a version match
        return metadata1 != metadata2;
    }

    /**
     * Returns <code>true</code> iff the cluster level blocks have changed between cluster states.
     * Note that this is an object reference equality test, not an equals test.
     * 数据块是否发生了变化
     */
    public boolean blocksChanged() {
        return state.blocks() != previousState.blocks();
    }

    /**
     * Returns <code>true</code> iff the local node is the master node of the cluster.
     * 当前节点是否被选举为集群的master节点
     */
    public boolean localNodeMaster() {
        return state.nodes().isLocalNodeElectedMaster();
    }

    /**
     * Returns the {@link org.elasticsearch.cluster.node.DiscoveryNodes.Delta} between
     * the previous cluster state and the new cluster state.
     */
    public DiscoveryNodes.Delta nodesDelta() {
        return this.nodesDelta;
    }

    /**
     * Returns <code>true</code> iff nodes have been removed from the cluster since the last cluster state.
     */
    public boolean nodesRemoved() {
        return nodesDelta.removed();
    }

    /**
     * Returns <code>true</code> iff nodes have been added from the cluster since the last cluster state.
     */
    public boolean nodesAdded() {
        return nodesDelta.added();
    }

    /**
     * Returns <code>true</code> iff nodes have been changed (added or removed) from the cluster since the last cluster state.
     */
    public boolean nodesChanged() {
        return nodesRemoved() || nodesAdded();
    }

    /**
     * Determines whether or not the current cluster state represents an entirely
     * new cluster, either when a node joins a cluster for the first time or when
     * the node receives a cluster state update from a brand new cluster (different
     * UUID from the previous cluster), which will happen when a master node is
     * elected that has never been part of the cluster before.
     */
    public boolean isNewCluster() {
        final String prevClusterUUID = previousState.metadata().clusterUUID();
        final String currClusterUUID = state.metadata().clusterUUID();
        return prevClusterUUID.equals(currClusterUUID) == false;
    }

    // Get the deleted indices by comparing the index metadatas in the previous and new cluster states.
    // If an index exists in the previous cluster state, but not in the new cluster state, it must have been deleted.
    // 基于前后2个state的 metadata 寻找本次删除的索引信息
    private List<Index> indicesDeletedFromClusterState() {
        // If the new cluster state has a new cluster UUID, the likely scenario is that a node was elected
        // master that has had its data directory wiped out, in which case we don't want to delete the indices and lose data;
        // rather we want to import them as dangling indices instead.  So we check here if the cluster UUID differs from the previous
        // cluster UUID, in which case, we don't want to delete indices that the master erroneously believes shouldn't exist.
        // See test DiscoveryWithServiceDisruptionsIT.testIndicesDeleted()
        // See discussion on https://github.com/elastic/elasticsearch/pull/9952 and
        // https://github.com/elastic/elasticsearch/issues/11665
        // 当元数据没有发生变化 或者当前是一个新的集群  返回空列表
        if (metadataChanged() == false || isNewCluster()) {
            return Collections.emptyList();
        }
        List<Index> deleted = null;
        for (ObjectCursor<IndexMetadata> cursor : previousState.metadata().indices().values()) {
            IndexMetadata index = cursor.value;
            // 代表之前的某个索引无法在当前 metadata中找到 就代表索引已经被删除
            IndexMetadata current = state.metadata().index(index.getIndex());
            if (current == null) {
                if (deleted == null) {
                    deleted = new ArrayList<>();
                }
                deleted.add(index.getIndex());
            }
        }
        return deleted == null ? Collections.<Index>emptyList() : deleted;
    }

    /**
     * 当数据还没有恢复的时候 选择从墓碑中找到被删除的索引
     * @return
     */
    private List<Index> indicesDeletedFromTombstones() {
        // We look at the full tombstones list to see which indices need to be deleted.  In the case of
        // a valid previous cluster state, indicesDeletedFromClusterState() will be used to get the deleted
        // list, so a diff doesn't make sense here.  When a node (re)joins the cluster, its possible for it
        // to re-process the same deletes or process deletes about indices it never knew about.  This is not
        // an issue because there are safeguards in place in the delete store operation in case the index
        // folder doesn't exist on the file system.
        List<IndexGraveyard.Tombstone> tombstones = state.metadata().indexGraveyard().getTombstones();
        return tombstones.stream().map(IndexGraveyard.Tombstone::getIndex).collect(Collectors.toList());
    }

}
