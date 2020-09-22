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

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * A discovery node represents a node that is part of the cluster.
 * 描述某个集群中的节点
 */
public class DiscoveryNode implements Writeable, ToXContentFragment {

    static final String COORDINATING_ONLY = "coordinating_only";

    public static boolean isMasterNode(Settings settings) {
        return Node.NODE_MASTER_SETTING.get(settings);
    }

    public static boolean isDataNode(Settings settings) {
        return Node.NODE_DATA_SETTING.get(settings);
    }

    public static boolean isIngestNode(Settings settings) {
        return Node.NODE_INGEST_SETTING.get(settings);
    }

    public static boolean isRemoteClusterClient(final Settings settings) {
        return Node.NODE_REMOTE_CLUSTER_CLIENT.get(settings);
    }

    // 目标节点的id 名称 地址信息 属性 以及角色等信息

    private final String nodeName;
    private final String nodeId;
    private final String ephemeralId;
    private final String hostName;
    private final String hostAddress;
    private final TransportAddress address;
    private final Map<String, String> attributes;
    private final Version version;
    private final SortedSet<DiscoveryNodeRole> roles;

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param id               the nodes unique (persistent) node id. This constructor will auto generate a random ephemeral id.
     * @param address          the nodes transport address
     * @param version          the version of the node
     */
    public DiscoveryNode(final String id, TransportAddress address, Version version) {
        this(id, address, Collections.emptyMap(), DiscoveryNodeRole.BUILT_IN_ROLES, version);
    }

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param id               the nodes unique (persistent) node id. This constructor will auto generate a random ephemeral id.
     * @param address          the nodes transport address
     * @param attributes       node attributes
     * @param roles            node roles
     * @param version          the version of the node
     */
    public DiscoveryNode(String id, TransportAddress address, Map<String, String> attributes, Set<DiscoveryNodeRole> roles,
                         Version version) {
        this("", id, address, attributes, roles, version);
    }

    /**
     * Creates a new {@link DiscoveryNode}
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param nodeName         the nodes name  当前节点的名字
     * @param nodeId           the nodes unique persistent id. An ephemeral id will be auto generated.   当前节点id
     * @param address          the nodes transport address     对外暴露的地址
     * @param attributes       node attributes        一些额外的属性 通过特殊的配置名从settings中获取
     * @param roles            node roles            当前node包含的角色
     * @param version          the version of the node     es的版本
     */
    public DiscoveryNode(String nodeName, String nodeId, TransportAddress address,
                         Map<String, String> attributes, Set<DiscoveryNodeRole> roles, Version version) {
        this(nodeName, nodeId, UUIDs.randomBase64UUID(), address.address().getHostString(), address.getAddress(), address, attributes,
            roles, version);
    }

    /**
     * Creates a new {@link DiscoveryNode}.
     * <p>
     * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
     * version. it corresponds to the minimum version this elasticsearch version can communicate with. If a higher version is used
     * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
     * and updated.
     * </p>
     *
     * @param nodeName         the nodes name
     * @param nodeId           the nodes unique persistent id
     * @param ephemeralId      the nodes unique ephemeral id     一个临时性的随机id
     * @param hostAddress      the nodes host address           主机地址
     * @param address          the nodes transport address
     * @param attributes       node attributes
     * @param roles            node roles
     * @param version          the version of the node
     *
     *                         在初始化阶段只是做了一些简单的赋值操作
     */
    public DiscoveryNode(String nodeName, String nodeId, String ephemeralId, String hostName, String hostAddress,
                         TransportAddress address, Map<String, String> attributes, Set<DiscoveryNodeRole> roles, Version version) {
        if (nodeName != null) {
            this.nodeName = nodeName.intern();
        } else {
            this.nodeName = "";
        }
        this.nodeId = nodeId.intern();
        this.ephemeralId = ephemeralId.intern();
        this.hostName = hostName.intern();
        this.hostAddress = hostAddress.intern();
        this.address = address;
        if (version == null) {
            this.version = Version.CURRENT;
        } else {
            this.version = version;
        }
        this.attributes = Collections.unmodifiableMap(attributes);
        //verify that no node roles are being provided as attributes
        // 确保角色信息没有设置到 attr中
        Predicate<Map<String, String>> predicate =  (attrs) -> {
            boolean success = true;
            for (final DiscoveryNodeRole role : DiscoveryNode.roleNameToPossibleRoles.values()) {
                success &= attrs.containsKey(role.roleName()) == false;
                assert success : role.roleName();
            }
            return success;
        };
        assert predicate.test(attributes) : attributes;
        this.roles = roles.stream().collect(Sets.toUnmodifiableSortedSet());
    }

    /**
     * Creates a DiscoveryNode representing the local node.
     * 创建一个能被集群发现的节点
     * @param publishAddress  对外暴露的地址
     */
    public static DiscoveryNode createLocal(Settings settings, TransportAddress publishAddress, String nodeId) {
        // 获取描述节点的状态信息
        Map<String, String> attributes = Node.NODE_ATTRIBUTES.getAsMap(settings);

        // 根据当下配置信息 检测这些角色对应的配置项是否为true
        Set<DiscoveryNodeRole> roles = getRolesFromSettings(settings);
        return new DiscoveryNode(Node.NODE_NAME_SETTING.get(settings), nodeId, publishAddress, attributes, roles, Version.CURRENT);
    }

    /** extract node roles from the given settings */
    public static Set<DiscoveryNodeRole> getRolesFromSettings(final Settings settings) {
        return roleNameToPossibleRoles.values().stream().filter(s -> s.roleSetting().get(settings)).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Creates a new {@link DiscoveryNode} by reading from the stream provided as argument
     * @param in the stream
     * @throws IOException if there is an error while reading from the stream
     *
     * 通过一个输入流的数据来还原该对象
     */
    public DiscoveryNode(StreamInput in) throws IOException {
        this.nodeName = in.readString().intern();
        this.nodeId = in.readString().intern();
        this.ephemeralId = in.readString().intern();
        this.hostName = in.readString().intern();
        this.hostAddress = in.readString().intern();
        this.address = new TransportAddress(in);
        int size = in.readVInt();
        this.attributes = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            this.attributes.put(in.readString(), in.readString());
        }
        int rolesSize = in.readVInt();
        final Set<DiscoveryNodeRole> roles = new HashSet<>(rolesSize);
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            for (int i = 0; i < rolesSize; i++) {
                final String roleName = in.readString();
                final String roleNameAbbreviation = in.readString();
                final DiscoveryNodeRole role = roleNameToPossibleRoles.get(roleName);
                if (role == null) {
                    roles.add(new DiscoveryNodeRole.UnknownRole(roleName, roleNameAbbreviation));
                } else {
                    assert roleName.equals(role.roleName()) : "role name [" + roleName + "] does not match role [" + role.roleName() + "]";
                    assert roleNameAbbreviation.equals(role.roleNameAbbreviation())
                            : "role name abbreviation [" + roleName + "] does not match role [" + role.roleNameAbbreviation() + "]";
                    roles.add(role);
                }
            }
        } else {
            // TODO 忽略兼容性代码
            // an old node will only send us legacy roles since pluggable roles is a new concept
            for (int i = 0; i < rolesSize; i++) {
                final LegacyRole legacyRole = in.readEnum(LegacyRole.class);
                switch (legacyRole) {
                    case MASTER:
                        roles.add(DiscoveryNodeRole.MASTER_ROLE);
                        break;
                    case DATA:
                        roles.add(DiscoveryNodeRole.DATA_ROLE);
                        break;
                    case INGEST:
                        roles.add(DiscoveryNodeRole.INGEST_ROLE);
                        break;
                    default:
                        throw new AssertionError(legacyRole.roleName());
                }
            }
        }
        this.roles = roles.stream().collect(Sets.toUnmodifiableSortedSet());
        this.version = Version.readVersion(in);
    }

    /**
     * 看多了lucene 这种都小case了
     * @param out
     * @throws IOException
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeName);
        out.writeString(nodeId);
        out.writeString(ephemeralId);
        out.writeString(hostName);
        out.writeString(hostAddress);
        address.writeTo(out);
        out.writeVInt(attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            out.writeVInt(roles.size());
            for (final DiscoveryNodeRole role : roles) {
                out.writeString(role.roleName());
                out.writeString(role.roleNameAbbreviation());
            }
            // TODO 忽略兼容性代码
        } else {
            // an old node will only understand legacy roles since pluggable roles is a new concept
            final List<DiscoveryNodeRole> rolesToWrite =
                    roles.stream().filter(DiscoveryNodeRole.LEGACY_ROLES::contains).collect(Collectors.toUnmodifiableList());
            out.writeVInt(rolesToWrite.size());
            for (final DiscoveryNodeRole role : rolesToWrite) {
                if (role == DiscoveryNodeRole.MASTER_ROLE) {
                    out.writeEnum(LegacyRole.MASTER);
                } else if (role == DiscoveryNodeRole.DATA_ROLE) {
                    out.writeEnum(LegacyRole.DATA);
                } else if (role == DiscoveryNodeRole.INGEST_ROLE) {
                    out.writeEnum(LegacyRole.INGEST);
                }
            }
        }
        Version.writeVersion(version, out);
    }

    /**
     * The address that the node can be communicated with.
     */
    public TransportAddress getAddress() {
        return address;
    }

    /**
     * The unique id of the node.
     */
    public String getId() {
        return nodeId;
    }

    /**
     * The unique ephemeral id of the node. Ephemeral ids are meant to be attached the life span
     * of a node process. When ever a node is restarted, it's ephemeral id is required to change (while it's {@link #getId()}
     * will be read from the data folder and will remain the same across restarts). Since all node attributes and addresses
     * are maintained during the life span of a node process, we can (and are) using the ephemeralId in
     * {@link DiscoveryNode#equals(Object)}.
     */
    public String getEphemeralId() {
        return ephemeralId;
    }

    /**
     * The name of the node.
     */
    public String getName() {
        return this.nodeName;
    }

    /**
     * The node attributes.
     */
    public Map<String, String> getAttributes() {
        return this.attributes;
    }

    /**
     * Should this node hold data (shards) or not.
     */
    public boolean isDataNode() {
        return roles.contains(DiscoveryNodeRole.DATA_ROLE);
    }

    /**
     * Can this node become master or not.
     */
    public boolean isMasterNode() {
        return roles.contains(DiscoveryNodeRole.MASTER_ROLE);
    }

    /**
     * Returns a boolean that tells whether this an ingest node or not
     */
    public boolean isIngestNode() {
        return roles.contains(DiscoveryNodeRole.INGEST_ROLE);
    }

    /**
     * Returns whether or not the node can be a remote cluster client.
     *
     * @return true if the node can be a remote cluster client, false otherwise
     */
    public boolean isRemoteClusterClient() {
        return roles.contains(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE);
    }

    /**
     * Returns a set of all the roles that the node has. The roles are returned in sorted order by the role name.
     * <p>
     * If a node does not have any specific role, the returned set is empty, which means that the node is a coordinating-only node.
     *
     * @return the sorted set of roles
     */
    public Set<DiscoveryNodeRole> getRoles() {
        return roles;
    }

    public Version getVersion() {
        return this.version;
    }

    public String getHostName() {
        return this.hostName;
    }

    public String getHostAddress() {
        return this.hostAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DiscoveryNode that = (DiscoveryNode) o;

        return ephemeralId.equals(that.ephemeralId);
    }

    @Override
    public int hashCode() {
        // we only need to hash the id because it's highly unlikely that two nodes
        // in our system will have the same id but be different
        // This is done so that this class can be used efficiently as a key in a map
        return ephemeralId.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (nodeName.length() > 0) {
            sb.append('{').append(nodeName).append('}');
        }
        sb.append('{').append(nodeId).append('}');
        sb.append('{').append(ephemeralId).append('}');
        sb.append('{').append(hostName).append('}');
        sb.append('{').append(address).append('}');
        if (roles.isEmpty() == false) {
            sb.append('{');
            roles.stream().map(DiscoveryNodeRole::roleNameAbbreviation).sorted().forEach(sb::append);
            sb.append('}');
        }
        if (!attributes.isEmpty()) {
            sb.append(attributes);
        }
        return sb.toString();
    }

    /**
     * 将当前对象以特殊格式输出 比如json
     * @param builder
     * @param params
     * @return
     * @throws IOException
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getId());
        builder.field("name", getName());
        builder.field("ephemeral_id", getEphemeralId());
        builder.field("transport_address", getAddress().toString());

        builder.startObject("attributes");
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    /**
     * 存储当前节点可能成为的所有角色  处理系统内置的角色外 插件也可以为node指定角色
     */
    private static Map<String, DiscoveryNodeRole> roleNameToPossibleRoles;

    /**
     * 设置当前节点所有可能的角色
     * @param possibleRoles
     */
    public static void setPossibleRoles(final Set<DiscoveryNodeRole> possibleRoles) {
        final Map<String, DiscoveryNodeRole> roleNameToPossibleRoles =
                possibleRoles.stream().collect(Collectors.toUnmodifiableMap(DiscoveryNodeRole::roleName, Function.identity()));
        // collect the abbreviation names into a map to ensure that there are not any duplicate abbreviations

        // roleNameAbbreviation 是role的缩写
        final Map<String, DiscoveryNodeRole> roleNameAbbreviationToPossibleRoles = roleNameToPossibleRoles.values()
                .stream()
                .collect(Collectors.toUnmodifiableMap(DiscoveryNodeRole::roleNameAbbreviation, Function.identity()));
        assert roleNameToPossibleRoles.size() == roleNameAbbreviationToPossibleRoles.size() :
                "roles by name [" + roleNameToPossibleRoles + "], roles by name abbreviation [" + roleNameAbbreviationToPossibleRoles + "]";
        DiscoveryNode.roleNameToPossibleRoles = roleNameToPossibleRoles;
    }

    public static Set<String> getPossibleRoleNames() {
        return roleNameToPossibleRoles.keySet();
    }

    /**
     * Enum that holds all the possible roles that that a node can fulfill in a cluster.
     * Each role has its name and a corresponding abbreviation used by cat apis.
     */
    private enum LegacyRole {
        MASTER("master"),
        DATA("data"),
        INGEST("ingest");

        private final String roleName;

        LegacyRole(final String roleName) {
            this.roleName = roleName;
        }

        public String roleName() {
            return roleName;
        }

    }

}
