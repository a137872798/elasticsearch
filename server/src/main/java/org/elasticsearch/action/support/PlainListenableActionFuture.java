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

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;

import java.util.ArrayList;
import java.util.List;

/**
 * 该future对象可以添加监听器处理结果
 * @param <T>
 */
public class PlainListenableActionFuture<T> extends AdapterActionFuture<T, T> implements ListenableActionFuture<T> {

    /**
     * 一组监听器  Object可能是一个 List
     */
    volatile Object listeners;
    /**
     * 代表已经触发过监听器了
     */
    boolean executedListeners = false;

    protected PlainListenableActionFuture() {}

    /**
     * This method returns a listenable future. The listeners will be called on completion of the future.
     * The listeners will be executed by the same thread that completes the future.
     *
     * @param <T> the result of the future
     * @return a listenable future
     */
    public static <T> PlainListenableActionFuture<T> newListenableFuture() {
        return new PlainListenableActionFuture<>();
    }

    @Override
    public void addListener(final ActionListener<T> listener) {
        internalAddListener(listener);
    }

    /**
     * 当填充完结果后触发该方法
     */
    @Override
    protected void done() {
        super.done();
        synchronized (this) {
            executedListeners = true;
        }
        Object listeners = this.listeners;
        if (listeners != null) {
            if (listeners instanceof List) {
                List list = (List) listeners;
                for (Object listener : list) {
                    executeListener((ActionListener<T>) listener);
                }
            } else {
                executeListener((ActionListener<T>) listeners);
            }
        }
    }

    /**
     * 这里没有做任何处理
     */
    @Override
    protected T convert(T listenerResponse) {
        return listenerResponse;
    }

    /**
     * 添加监听器
     * @param listener
     */
    private void internalAddListener(ActionListener<T> listener) {
        boolean executeImmediate = false;
        synchronized (this) {
            // 代表结果已经设置
            if (executedListeners) {
                executeImmediate = true;
            } else {
                Object listeners = this.listeners;
                if (listeners == null) {
                    listeners = listener;
                } else if (listeners instanceof List) {
                    ((List) this.listeners).add(listener);
                } else {
                    Object orig = listeners;
                    listeners = new ArrayList<>(2);
                    ((List) listeners).add(orig);
                    ((List) listeners).add(listener);
                }
                this.listeners = listeners;
            }
        }
        // 结果已经设置的情况下 立即触发监听器
        if (executeImmediate) {
            executeListener(listener);
        }
    }

    private void executeListener(final ActionListener<T> listener) {
        try {
            // we use a timeout of 0 to by pass assertion forbidding to call actionGet() (blocking) on a network thread.
            // here we know we will never block
            listener.onResponse(actionGet(0));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
