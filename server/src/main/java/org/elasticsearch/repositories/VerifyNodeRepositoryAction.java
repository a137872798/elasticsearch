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

package org.elasticsearch.repositories;

import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 难道说该对象会向所有节点同步发送认证请求么
 */
public class VerifyNodeRepositoryAction {

    private static final Logger logger = LogManager.getLogger(VerifyNodeRepositoryAction.class);

    public static final String ACTION_NAME = "internal:admin/repository/verify";

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;


    /**
     * ES中 集中提供一种功能的对象会被抽象成一种service   而下面3个服务分别提供网络通信，于集群内发布消息， 存储数据的能力
     * @param transportService
     * @param clusterService
     * @param repositoriesService
     */
    public VerifyNodeRepositoryAction(TransportService transportService, ClusterService clusterService,
                                      RepositoriesService repositoriesService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        // 注册请求处理器
        transportService.registerRequestHandler(ACTION_NAME, ThreadPool.Names.SNAPSHOT, VerifyNodeRepositoryRequest::new,
            new VerifyNodeRepositoryRequestHandler());
    }

    /**
     * 通过集群服务向其他节点发起认证请求
     * @param repository
     * @param verificationToken
     * @param listener
     */
    public void verify(String repository, String verificationToken, final ActionListener<List<DiscoveryNode>> listener) {
        // 找到此时集群中所有节点
        final DiscoveryNodes discoNodes = clusterService.state().nodes();
        final DiscoveryNode localNode = discoNodes.getLocalNode();

        // 找到所有 参与选举的节点以及数据节点  这些节点应该就是目标节点
        final ObjectContainer<DiscoveryNode> masterAndDataNodes = discoNodes.getMasterAndDataNodes().values();
        final List<DiscoveryNode> nodes = new ArrayList<>();
        for (ObjectCursor<DiscoveryNode> cursor : masterAndDataNodes) {
            DiscoveryNode node = cursor.value;
            nodes.add(node);
        }
        final CopyOnWriteArrayList<VerificationFailure> errors = new CopyOnWriteArrayList<>();
        final AtomicInteger counter = new AtomicInteger(nodes.size());
        for (final DiscoveryNode node : nodes) {
            // 如果是本地节点 直接在本地完成认证
            if (node.equals(localNode)) {
                try {
                    doVerify(repository, verificationToken, localNode);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("[{}] failed to verify repository", repository), e);
                    errors.add(new VerificationFailure(node.getId(), e));
                }
                if (counter.decrementAndGet() == 0) {
                    finishVerification(repository, listener, nodes, errors);
                }
            } else {
                // 对集群中其他节点发起认证请求
                transportService.sendRequest(node, ACTION_NAME, new VerifyNodeRepositoryRequest(repository, verificationToken),
                    new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            if (counter.decrementAndGet() == 0) {
                                finishVerification(repository, listener, nodes, errors);
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            // 每当有一个节点的认证失败了 就将失败信息存储到errors 容器中
                            errors.add(new VerificationFailure(node.getId(), exp));
                            if (counter.decrementAndGet() == 0) {
                                finishVerification(repository, listener, nodes, errors);
                            }
                        }
                    });
            }
        }
    }

    /**
     * 当所有节点都完成验证时触发该方法
     * @param repositoryName
     * @param listener
     * @param nodes
     * @param errors
     */
    private static void finishVerification(String repositoryName, ActionListener<List<DiscoveryNode>> listener, List<DiscoveryNode> nodes,
                                   CopyOnWriteArrayList<VerificationFailure> errors) {
        if (errors.isEmpty() == false) {
            RepositoryVerificationException e = new RepositoryVerificationException(repositoryName, errors.toString());
            for (VerificationFailure error : errors) {
                e.addSuppressed(error.getCause());
            }
            // 有某些节点的仓库无法写入数据
            listener.onFailure(e);
        } else {
            listener.onResponse(nodes);
        }
    }

    /**
     * 进行认证操作
     * @param repositoryName
     * @param verificationToken
     * @param localNode
     */
    private void doVerify(String repositoryName, String verificationToken, DiscoveryNode localNode) {
        Repository repository = repositoriesService.repository(repositoryName);
        repository.verify(verificationToken, localNode);
    }

    public static class VerifyNodeRepositoryRequest extends TransportRequest {

        private String repository;
        private String verificationToken;

        public VerifyNodeRepositoryRequest(StreamInput in) throws IOException {
            super(in);
            repository = in.readString();
            verificationToken = in.readString();
        }

        VerifyNodeRepositoryRequest(String repository, String verificationToken) {
            this.repository = repository;
            this.verificationToken = verificationToken;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            out.writeString(verificationToken);
        }
    }

    /**
     * 该对象定义了如何处理认证请求
     */
    class VerifyNodeRepositoryRequestHandler implements TransportRequestHandler<VerifyNodeRepositoryRequest> {
        @Override
        public void messageReceived(VerifyNodeRepositoryRequest request, TransportChannel channel, Task task) throws Exception {
            DiscoveryNode localNode = clusterService.state().nodes().getLocalNode();
            try {
                doVerify(request.repository, request.verificationToken, localNode);
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to verify repository", request.repository), ex);
                throw ex;
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

}
