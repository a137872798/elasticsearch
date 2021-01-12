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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Triggered by a {@link StartJoinRequest}, instances of this class represent join votes,
 * and have a source and target node. The source node is the node that provides the vote,
 * and the target node is the node for which this vote is cast. A node will only cast
 * a single vote per term, and this for a unique target node. The vote also carries
 * information about the current state of the node that provided the vote, so that
 * the receiver of the vote can determine if it has a more up-to-date state than the
 * source node.
 * 代表一个加入到 尝试晋升leader的候选节点的请求
 */
public class Join implements Writeable {
    private final DiscoveryNode sourceNode;
    private final DiscoveryNode targetNode;
    private final long term;
    private final long lastAcceptedTerm;
    private final long lastAcceptedVersion;

    /**
     *
     * @param sourceNode  本节点
     * @param targetNode  要加入的节点
     * @param term  在于targetNode同步任期后 此时sourceNode 的任期
     * @param lastAcceptedTerm   获取上一次的持久化的clusteState对应的任期信息
     * @param lastAcceptedVersion 获取上一次的持久化的clusteState对应的version  就是对应同一任期中clusterState变化的不同版本
     */
    public Join(DiscoveryNode sourceNode, DiscoveryNode targetNode, long term, long lastAcceptedTerm, long lastAcceptedVersion) {
        assert term >= 0;
        assert lastAcceptedTerm >= 0;
        assert lastAcceptedVersion >= 0;

        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.term = term;
        this.lastAcceptedTerm = lastAcceptedTerm;
        this.lastAcceptedVersion = lastAcceptedVersion;
    }

    public Join(StreamInput in) throws IOException {
        sourceNode = new DiscoveryNode(in);
        targetNode = new DiscoveryNode(in);
        term = in.readLong();
        lastAcceptedTerm = in.readLong();
        lastAcceptedVersion = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        sourceNode.writeTo(out);
        targetNode.writeTo(out);
        out.writeLong(term);
        out.writeLong(lastAcceptedTerm);
        out.writeLong(lastAcceptedVersion);
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public boolean targetMatches(DiscoveryNode matchingNode) {
        return targetNode.getId().equals(matchingNode.getId());
    }

    public long getLastAcceptedVersion() {
        return lastAcceptedVersion;
    }

    public long getTerm() {
        return term;
    }

    public long getLastAcceptedTerm() {
        return lastAcceptedTerm;
    }

    @Override
    public String toString() {
        return "Join{" +
            "term=" + term +
            ", lastAcceptedTerm=" + lastAcceptedTerm +
            ", lastAcceptedVersion=" + lastAcceptedVersion +
            ", sourceNode=" + sourceNode +
            ", targetNode=" + targetNode +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Join join = (Join) o;

        if (sourceNode.equals(join.sourceNode) == false) return false;
        if (targetNode.equals(join.targetNode) == false) return false;
        if (lastAcceptedVersion != join.lastAcceptedVersion) return false;
        if (term != join.term) return false;
        return lastAcceptedTerm == join.lastAcceptedTerm;
    }

    @Override
    public int hashCode() {
        int result = (int) (lastAcceptedVersion ^ (lastAcceptedVersion >>> 32));
        result = 31 * result + sourceNode.hashCode();
        result = 31 * result + targetNode.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (lastAcceptedTerm ^ (lastAcceptedTerm >>> 32));
        return result;
    }
}
