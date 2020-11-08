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

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Objects;

/**
 * 当通过 getService找到分片的相关信息时 会根据doc内部存储的数据初始化parse对象
 */
public class SourceToParse {

    /**
     * 原始数据流
     */
    private final BytesReference source;

    private final String index;

    private final String id;

    /**
     * 标识当前分片所在的节点
     */
    private final @Nullable String routing;

    /**
     * 这组数据流是什么格式的  比如 JSON/YML
     */
    private final XContentType xContentType;

    /**
     * 根据原始数据流 以及格式化类型生成该对象
     * 当前看到的使用场景是这样  shardGetService 调用get方法  配合engine对象 查询到一组数据 并且doc中应该还有被  id source routing等属性  现在将这些数据传入
     * source应该是格式化数据 之后可能要进行解析
     * @param index
     * @param id
     * @param source
     * @param xContentType
     * @param routing
     */
    public SourceToParse(String index, String id, BytesReference source, XContentType xContentType, @Nullable String routing) {
        this.index = Objects.requireNonNull(index);
        this.id = Objects.requireNonNull(id);
        // we always convert back to byte array, since we store it and Field only supports bytes..
        // so, we might as well do it here, and improve the performance of working with direct byte arrays
        // 代表内部的数据流 由一个 bytes[] 填装
        this.source = new BytesArray(Objects.requireNonNull(source).toBytesRef());
        this.xContentType = Objects.requireNonNull(xContentType);
        this.routing = routing;
    }

    public SourceToParse(String index, String id, BytesReference source, XContentType xContentType) {
        this(index, id, source, xContentType, null);
    }

    public BytesReference source() {
        return this.source;
    }

    public String index() {
        return this.index;
    }

    public String id() {
        return this.id;
    }

    public @Nullable String routing() {
        return this.routing;
    }

    public XContentType getXContentType() {
        return this.xContentType;
    }

    public enum Origin {
        PRIMARY,
        REPLICA
    }
}
