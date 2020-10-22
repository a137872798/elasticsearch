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
package org.elasticsearch.common.blobstore;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;

/**
 * An interface for storing blobs.
 * 代表大块数据的仓库  他是一个抽象的概念 需要通过指定path来找到对应的容器  容器应该才是真正存储数据的地方
 */
public interface BlobStore extends Closeable {

    /**
     * Get a blob container instance for storing blobs at the given {@link BlobPath}.
     */
    BlobContainer blobContainer(BlobPath path);

    /**
     * Returns statistics on the count of operations that have been performed on this blob store
     * 获取该仓库下的各种统计信息
     * 仓库与存储服务是一对一的关系
     */
    default Map<String, Long> stats() {
        return Collections.emptyMap();
    }
}
