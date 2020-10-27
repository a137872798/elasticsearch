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

package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.store.RateLimiter;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Rate limiting wrapper for InputStream
 * 通过限流器  对输入流增加限流功能
 * 这个限流器不是那种类似于漏桶算法的 而是 比如每秒允许写入2M数据  当某次打算写入10M时 就会在等待5秒后一次性写入10M
 */
public class RateLimitingInputStream extends FilterInputStream {

    private final RateLimiter rateLimiter;

    private final Listener listener;

    /**
     * 在距离上一次parse 到现在已经积累了多少数据了
     */
    private long bytesSinceLastRateLimit;

    public interface Listener {
        void onPause(long nanos);
    }

    public RateLimitingInputStream(InputStream delegate, RateLimiter rateLimiter, Listener listener) {
        super(delegate);
        this.rateLimiter = rateLimiter;
        this.listener = listener;
    }

    /**
     * @param bytes
     * @throws IOException
     */
    private void maybePause(int bytes) throws IOException {
        bytesSinceLastRateLimit += bytes;
        // 只有当写入的数据量超过了 限流值时 才会触发限流逻辑  比如阈值是2M/s 但是在某次尝试写入10M 那么就会在等待5秒后 一次性写入10M
        if (bytesSinceLastRateLimit >= rateLimiter.getMinPauseCheckBytes()) {
            long pause = rateLimiter.pause(bytesSinceLastRateLimit);
            bytesSinceLastRateLimit = 0;
            if (pause > 0) {
                // 每次触发限流逻辑都会触发监听器
                listener.onPause(pause);
            }
        }
    }

    @Override
    public int read() throws IOException {
        int b = super.read();
        maybePause(1);
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = super.read(b, off, len);
        if (n > 0) {
            maybePause(n);
        }
        return n;
    }
}
