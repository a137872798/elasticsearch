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

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.io.IOException;

/**
 * handler 仅包含处理逻辑 通过 将handler与action封装在一起使得每个 RequestHandlerRegistry 仅处理一类请求 同时相关上下文信息会封装在  TaskTransportChannel
 * 注意在执行任务前后会先将任务存储到 taskManager中 完成时 会将任务移除
 * @param <Request>
 */
public class RequestHandlerRegistry<Request extends TransportRequest> {

    /**
     * 每个handler 只处理一种行为
     */
    private final String action;

    /**
     * 请求处理器 包含一个 messageReceived 的钩子
     */
    private final TransportRequestHandler<Request> handler;

    /**
     * 这个强制执行应该是针对线程池的
     */
    private final boolean forceExecution;
    private final boolean canTripCircuitBreaker;
    private final String executor;
    private final TaskManager taskManager;
    private final Writeable.Reader<Request> requestReader;

    public RequestHandlerRegistry(String action, Writeable.Reader<Request> requestReader, TaskManager taskManager,
                                  TransportRequestHandler<Request> handler, String executor, boolean forceExecution,
                                  boolean canTripCircuitBreaker) {
        this.action = action;
        this.requestReader = requestReader;
        this.handler = handler;
        this.forceExecution = forceExecution;
        this.canTripCircuitBreaker = canTripCircuitBreaker;
        this.executor = executor;
        this.taskManager = taskManager;
    }

    public String getAction() {
        return action;
    }

    public Request newRequest(StreamInput in) throws IOException {
        return requestReader.read(in);
    }

    /**
     * 处理接受到的请求
     * @param request  处理的请求可以是不同类型的
     * @param channel  不同通道可能会有不同的特性 所以才把通道单独抽象出来
     * @throws Exception
     */
    public void processMessageReceived(Request request, TransportChannel channel) throws Exception {
        // 将请求体交由taskManager 通过抽取req上action的类型 生成对应的task对象
        // 为某种行为的处理 增加一个请求对象
        final Task task = taskManager.register(channel.getChannelType(), action, request);
        boolean success = false;
        try {
            // 包装通道后交给 handler 处理
            handler.messageReceived(request, new TaskTransportChannel(taskManager, task, channel), task);
            success = true;
        } finally {
            // 任务失败会提前注销task   当手动调用 TaskTransportChannel.sendResponse  会触发注销动作
            if (success == false) {
                taskManager.unregister(task);
            }
        }
    }

    public boolean isForceExecution() {
        return forceExecution;
    }

    public boolean canTripCircuitBreaker() {
        return canTripCircuitBreaker;
    }

    public String getExecutor() {
        return executor;
    }

    public TransportRequestHandler<Request> getHandler() {
        return handler;
    }

    @Override
    public String toString() {
        return handler.toString();
    }

    /**
     * 替换registry内部的handler
     * @param registry
     * @param handler
     * @param <R>
     * @return
     */
    public static <R extends TransportRequest> RequestHandlerRegistry<R> replaceHandler(RequestHandlerRegistry<R> registry,
                                                                                        TransportRequestHandler<R> handler) {
        return new RequestHandlerRegistry<>(registry.action, registry.requestReader, registry.taskManager, handler,
            registry.executor, registry.forceExecution, registry.canTripCircuitBreaker);
    }
}
