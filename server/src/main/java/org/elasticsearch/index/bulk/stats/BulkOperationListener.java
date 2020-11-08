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

package org.elasticsearch.index.bulk.stats;

/**
 * An bulk operation listener for bulk events.
 * bulk操作指的是什么
 * 在lucene中 只知道bulk应该是指代某种大的数据块
 */
public interface BulkOperationListener {
    /**
     * Called after the bulk operation occurred.
     */
    default void afterBulk(long bulkShardSizeInBytes, long tookInNanos) {
    }
}

