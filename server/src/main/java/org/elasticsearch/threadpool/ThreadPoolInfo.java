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

package org.elasticsearch.threadpool;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.ReportingService;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 返回描述线程池的一组信息
 */
public class ThreadPoolInfo implements ReportingService.Info, Iterable<ThreadPool.Info> {

    /**
     * 每个info对象都是对某个线程池的描述  而 ThreadPool 则维护了es单个节点下所有的线程池
     */
    private final List<ThreadPool.Info> infos;

    /**
     * 通过一组信息对象进行初始化
     * @param infos
     */
    public ThreadPoolInfo(List<ThreadPool.Info> infos) {
        this.infos = Collections.unmodifiableList(infos);
    }

    /**
     * 将输入流转换成 Info
     * @param in
     * @throws IOException
     */
    public ThreadPoolInfo(StreamInput in) throws IOException {
        this.infos = Collections.unmodifiableList(in.readList(ThreadPool.Info::new));
    }

    /**
     * 将当前infos信息写入到一个输出流对象中
     * @param out
     * @throws IOException
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(infos);
    }

    @Override
    public Iterator<ThreadPool.Info> iterator() {
        return infos.iterator();
    }

    static final class Fields {
        static final String THREAD_POOL = "thread_pool";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.THREAD_POOL);
        for (ThreadPool.Info info : infos) {
            info.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
