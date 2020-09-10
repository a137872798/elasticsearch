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

package org.elasticsearch.common.xcontent.yaml;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Set;

/**
 * A YAML based content implementation using Jackson.
 * 该对象具备创建yml文件解析器对象
 */
public class YamlXContent implements XContent {

    public static XContentBuilder contentBuilder() throws IOException {
        return XContentBuilder.builder(yamlXContent);
    }

    /**
     * 全局静态变量
     */
    static final YAMLFactory yamlFactory;

    /**
     * 单例模式
     */
    public static final YamlXContent yamlXContent;

    static {
        // 这里借助了第三方框架
        yamlFactory = new YAMLFactory();
        yamlFactory.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
        yamlXContent = new YamlXContent();
    }

    private YamlXContent() {
    }

    @Override
    public XContentType type() {
        return XContentType.YAML;
    }

    @Override
    public byte streamSeparator() {
        throw new UnsupportedOperationException("yaml does not support stream parsing...");
    }

    @Override
    public XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes) throws IOException {
        return new YamlXContentGenerator(yamlFactory.createGenerator(os, JsonEncoding.UTF8), os, includes, excludes);
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, String content) throws IOException {
        return new YamlXContentParser(xContentRegistry, deprecationHandler, yamlFactory.createParser(new StringReader(content)));
    }

    /**
     * 创建yml文件解析器对象
     * @param xContentRegistry
     * @param deprecationHandler
     * @param is  结构化数据对应的输入流
     * @return
     * @throws IOException
     */
    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, InputStream is) throws IOException {
        // yamlFactory.createParser 是第三方框架的api 通过一个解析器对象解析输入流 并对外开放读取属性的api
        return new YamlXContentParser(xContentRegistry, deprecationHandler, yamlFactory.createParser(is));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, byte[] data) throws IOException {
        return new YamlXContentParser(xContentRegistry, deprecationHandler, yamlFactory.createParser(data));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, byte[] data, int offset, int length) throws IOException {
        return new YamlXContentParser(xContentRegistry, deprecationHandler, yamlFactory.createParser(data, offset, length));
    }

    @Override
    public XContentParser createParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, Reader reader) throws IOException {
        return new YamlXContentParser(xContentRegistry, deprecationHandler, yamlFactory.createParser(reader));
    }
}
