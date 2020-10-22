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

import org.elasticsearch.common.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * The list of paths where a blob can reside.  The contents of the paths are dependent upon the implementation of {@link BlobContainer}.
 * 代表存储大块数据Container的路径信息
 */
public class BlobPath implements Iterable<String> {

    private static final String SEPARATOR = "/";

    /**
     * 实际上他们拼接起来才是完整的路径 但是拆分后按小段存储
     */
    private final List<String> paths;

    public BlobPath() {
        this.paths = Collections.emptyList();
    }

    public static BlobPath cleanPath() {
        return new BlobPath();
    }

    private BlobPath(List<String> paths) {
        this.paths = paths;
    }

    @Override
    public Iterator<String> iterator() {
        return paths.iterator();
    }

    public String[] toArray() {
        return paths.toArray(new String[paths.size()]);
    }

    public BlobPath add(String path) {
        List<String> paths = new ArrayList<>(this.paths);
        paths.add(path);
        return new BlobPath(Collections.unmodifiableList(paths));
    }

    /**
     * 将这组路径使用"/" 拼接
     * @return
     */
    public String buildAsString() {
        String p = String.join(SEPARATOR, paths);
        if (p.isEmpty() || p.endsWith(SEPARATOR)) {
            return p;
        }
        return p + SEPARATOR;
    }

    /**
     * Returns this path's parent path.
     *
     * @return Parent path or {@code null} if there is none
     * 返回上级路径 也就是去除掉最后的路径
     */
    @Nullable
    public BlobPath parent() {
        if (paths.isEmpty()) {
            return null;
        } else {
            return new BlobPath(List.copyOf(paths.subList(0, paths.size() - 1)));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String path : paths) {
            sb.append('[').append(path).append(']');
        }
        return sb.toString();
    }
}
