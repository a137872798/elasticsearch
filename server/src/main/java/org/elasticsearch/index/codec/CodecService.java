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

package org.elasticsearch.index.codec;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.codecs.lucene84.Lucene84Codec;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.mapper.MapperService;

import java.util.HashMap;
import java.util.Map;

/**
 * Since Lucene 4.0 low level index segments are read and written through a
 * codec layer that allows to use use-case specific file formats &amp;
 * data-structures per field. Elasticsearch exposes the full
 * {@link Codec} capabilities through this {@link CodecService}.
 * 编解码服务   在doc的field级别 每个field可以使用不同的编解码方式进行数据存储
 */
public class CodecService {

    private final Map<String, Codec> codecs;

    public static final String DEFAULT_CODEC = "default";
    public static final String BEST_COMPRESSION_CODEC = "best_compression";
    /**
     * the raw unfiltered lucene default. useful for testing
     * 默认情况下使用lucene自带的编解码方式
     */
    public static final String LUCENE_DEFAULT_CODEC = "lucene_default";

    public CodecService(@Nullable MapperService mapperService, Logger logger) {
        final var codecs = new HashMap<String, Codec>();
        // 默认使用lucene自带的编解码器  同时codecs中包含了倾向点不同的2种编解码 一种使用的压缩算法以速度优先
        // 还有一种就是压缩率优先
        if (mapperService == null) {
            codecs.put(DEFAULT_CODEC, new Lucene84Codec());
            codecs.put(BEST_COMPRESSION_CODEC, new Lucene84Codec(Mode.BEST_COMPRESSION));
        } else {
            // 当指定了映射服务时 使用ES 封装过的编解码器
            codecs.put(DEFAULT_CODEC,
                    new PerFieldMappingPostingFormatCodec(Mode.BEST_SPEED, mapperService, logger));
            codecs.put(BEST_COMPRESSION_CODEC,
                    new PerFieldMappingPostingFormatCodec(Mode.BEST_COMPRESSION, mapperService, logger));
        }
        // 会将lucene默认的编解码器存储到map中
        codecs.put(LUCENE_DEFAULT_CODEC, Codec.getDefault());
        // 找到此时能够加载到的所有 Codec 在初始化阶段 ES 应该是能够从其他地方自动装载一些Codec的
        for (String codec : Codec.availableCodecs()) {
            codecs.put(codec, Codec.forName(codec));
        }
        this.codecs = Map.copyOf(codecs);
    }

    public Codec codec(String name) {
        Codec codec = codecs.get(name);
        if (codec == null) {
            throw new IllegalArgumentException("failed to find codec [" + name + "]");
        }
        return codec;
    }

    /**
     * Returns all registered available codec names
     */
    public String[] availableCodecs() {
        return codecs.keySet().toArray(new String[0]);
    }
}
