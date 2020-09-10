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

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.RejectedExecutionHandler;

/**
 * 当线程池满了 继续添加任务 就会交由该对象进行处理  es拓展了原生jdk的处理器 增加了一个返回拒绝任务数量的api
 */
public interface XRejectedExecutionHandler extends RejectedExecutionHandler {

    /**
     * The number of rejected executions.
     */
    long rejected();
}
