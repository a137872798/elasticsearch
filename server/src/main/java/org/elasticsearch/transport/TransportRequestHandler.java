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

import org.elasticsearch.tasks.Task;

/**
 * 该对象负责处理各种请求
 * @param <T>
 */
public interface TransportRequestHandler<T extends TransportRequest> {

    /**
     * TODO 当处理完成时 应该会将task从manager中移除
     * @param request  本次的请求对象
     * @param channel   使用的通道信息
     * @param task   本次相关的任务信息
     * @throws Exception
     */
    void messageReceived(T request, TransportChannel channel, Task task) throws Exception;
}
