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
package org.elasticsearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * Service that can store task results.
 * 该对象负责存储某个 task的结果
 */
public class TaskResultsService {

    private static final Logger logger = LogManager.getLogger(TaskResultsService.class);

    public static final String TASK_INDEX = ".tasks";

    public static final String TASK_RESULT_INDEX_MAPPING_FILE = "task-index-mapping.json";

    public static final String TASK_RESULT_MAPPING_VERSION_META_FIELD = "version";

    public static final int TASK_RESULT_MAPPING_VERSION = 3;

    /**
     * The backoff policy to use when saving a task result fails. The total wait
     * time is 600000 milliseconds, ten minutes.
     * 当存储失败时 采用指数级补偿策略
     */
    static final BackoffPolicy STORE_BACKOFF_POLICY =
            BackoffPolicy.exponentialBackoff(timeValueMillis(250), 14);

    private final Client client;

    /**
     * 该服务负责获取集群相关的信息 内部应该是使用了某种一致性算法
     */
    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    @Inject
    public TaskResultsService(Client client, ClusterService clusterService, ThreadPool threadPool) {
        this.client = new OriginSettingClient(client, TASKS_ORIGIN);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * 存储某个taskResult
     * @param taskResult
     * @param listener
     */
    public void storeResult(TaskResult taskResult, ActionListener<Void> listener) {

        // 获取此时的集群状态  包含了此时有多少node等信息
        ClusterState state = clusterService.state();

        // 如果此时不包含有关 .tasks的路由表
        if (state.routingTable().hasIndex(TASK_INDEX) == false) {
            // 这里创建了一个申请索引的请求   也就是将taskResult当作普通数据存储到es服务器中么
            CreateIndexRequest createIndexRequest = new CreateIndexRequest();
            // 下面的属性是 CreateIndexRequest必备的 而有关taskResult的这些属性是固定的
            createIndexRequest.settings(taskResultIndexSettings());
            createIndexRequest.index(TASK_INDEX);
            // 可以找到 task-index-mapping.json 的文件 内部存储了一系列属性 可以将它设置到req上
            createIndexRequest.mapping(taskResultIndexMapping());
            createIndexRequest.cause("auto(task api)");

            // TODO 先不管请求是如何发送的  当创建索引完成时 触发监听器的逻辑 也就是再发送一个添加索引数据的信息
            client.admin().indices().create(createIndexRequest, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    doStoreResult(taskResult, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        // we have the index, do it
                        try {
                            doStoreResult(taskResult, listener);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            listener.onFailure(inner);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            IndexMetadata metadata = state.getMetadata().index(TASK_INDEX);
            // 先获取集群当前taskIndex的版本    如果版本太旧 尝试更新
            if (getTaskResultMappingVersion(metadata) < TASK_RESULT_MAPPING_VERSION) {
                // The index already exists but doesn't have our mapping
                client.admin().indices().preparePutMapping(TASK_INDEX)
                    .setSource(taskResultIndexMapping(), XContentType.JSON)
                    .execute(ActionListener.delegateFailure(listener, (l, r) -> doStoreResult(taskResult, listener)));
            } else {
                doStoreResult(taskResult, listener);
            }
        }
    }

    private int getTaskResultMappingVersion(IndexMetadata metadata) {
        MappingMetadata mappingMetadata = metadata.mapping();
        if (mappingMetadata == null) {
            return 0;
        }
        @SuppressWarnings("unchecked") Map<String, Object> meta = (Map<String, Object>) mappingMetadata.sourceAsMap().get("_meta");
        if (meta == null || meta.containsKey(TASK_RESULT_MAPPING_VERSION_META_FIELD) == false) {
            return 1; // The mapping was created before meta field was introduced
        }
        return (int) meta.get(TASK_RESULT_MAPPING_VERSION_META_FIELD);
    }

    /**
     * 将某个任务结果发往某个es节点并存储
     * @param taskResult
     * @param listener
     */
    private void doStoreResult(TaskResult taskResult, ActionListener<Void> listener) {
        IndexRequestBuilder index = client.prepareIndex(TASK_INDEX).setId(taskResult.getTask().getTaskId().toString());
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            // 将结果转换成json格式后设置到 indexReq上
            taskResult.toXContent(builder, ToXContent.EMPTY_PARAMS);
            index.setSource(builder);
        } catch (IOException e) {
            throw new ElasticsearchException("Couldn't convert task result to XContent for [{}]", e, taskResult.getTask());
        }
        doStoreResult(STORE_BACKOFF_POLICY.iterator(), index, listener);
    }

    /**
     * 将创建索引的请求发往各个节点 当失败时 在等待一定时间后进行重试
     * @param backoff
     * @param index
     * @param listener
     */
    private void doStoreResult(Iterator<TimeValue> backoff, IndexRequestBuilder index, ActionListener<Void> listener) {
        index.execute(new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                if (false == (e instanceof EsRejectedExecutionException)
                        || false == backoff.hasNext()) {
                    listener.onFailure(e);
                } else {
                    TimeValue wait = backoff.next();
                    logger.warn(() -> new ParameterizedMessage("failed to store task result, retrying in [{}]", wait), e);
                    threadPool.schedule(() -> doStoreResult(backoff, index, listener), wait, ThreadPool.Names.SAME);
                }
            }
        });
    }

    /**
     * 这里获取有关 taskResult的默认配置
     * @return
     */
    private Settings taskResultIndexSettings() {
        return Settings.builder()
            // taskResult的分片数量为1
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            // 在复制时自动扩充???
            .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
            // 最高优先级
            .put(IndexMetadata.SETTING_PRIORITY, Integer.MAX_VALUE)
            .build();
    }

    public String taskResultIndexMapping() {
        try (InputStream is = getClass().getResourceAsStream(TASK_RESULT_INDEX_MAPPING_FILE)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            return out.toString(StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage(
                    "failed to create tasks results index template [{}]", TASK_RESULT_INDEX_MAPPING_FILE), e);
            throw new IllegalStateException("failed to create tasks results index template [" + TASK_RESULT_INDEX_MAPPING_FILE + "]", e);
        }

    }
}
