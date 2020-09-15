/*
 * Copyright (C) 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject.internal;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Lazily creates (and caches) values for keys. If creating the value fails (with errors), an
 * exception is thrown on retrieval.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * 缓存对象
 */
public abstract class FailableCache<K, V> {

    /**
     * 实际上缓存功能是委托给该类
     */
    private final ConcurrentHashMap<K, Object> cache = new ConcurrentHashMap<>();

    /**
     * 生成缓存数据的方法
     * @param key
     * @param errors
     * @return
     * @throws ErrorsException
     */
    protected abstract V create(K key, Errors errors) throws ErrorsException;

    public V get(K key, Errors errors) throws ErrorsException {
        // 先从缓存中获取
        Object resultOrError = cache.get(key);
        if (resultOrError == null) {
            synchronized (this) {
                resultOrError = load(key);
                // we can't use cache.computeIfAbsent since this might be recursively call this API
                // 将结果存储到cache中
                cache.putIfAbsent(key, resultOrError);
            }
        }
        // 当检测到生成的是 errors对象 代表创建数据失败 选择抛出异常
        if (resultOrError instanceof Errors) {
            errors.merge((Errors) resultOrError);
            throw errors.toException();
        } else {
            @SuppressWarnings("unchecked") // create returned a non-error result, so this is safe
            V result = (V) resultOrError;
            return result;
        }
    }


    private Object load(K key) {
        Errors errors = new Errors();
        V result = null;
        try {
            result = create(key, errors);
        } catch (ErrorsException e) {
            errors.merge(e.getErrors());
        }
        return errors.hasErrors() ? errors : result;
    }
}
