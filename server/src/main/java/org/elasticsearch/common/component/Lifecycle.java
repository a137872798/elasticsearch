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

package org.elasticsearch.common.component;

/**
 * Lifecycle state. Allows the following transitions:
 * <ul>
 * <li>INITIALIZED -&gt; STARTED, STOPPED, CLOSED</li>
 * <li>STARTED     -&gt; STOPPED</li>
 * <li>STOPPED     -&gt; STARTED, CLOSED</li>
 * <li>CLOSED      -&gt; </li>
 * </ul>
 * <p>
 * Also allows to stay in the same state. For example, when calling stop on a component, the
 * following logic can be applied:
 * <pre>
 * public void stop() {
 *  if (!lifecycleState.moveToStopped()) {
 *      return;
 *  }
 * // continue with stop logic
 * }
 * </pre>
 * <p>
 * NOTE: The Lifecycle class is thread-safe. It is also possible to prevent concurrent state transitions
 * by locking on the Lifecycle object itself. This is typically useful when chaining multiple transitions.
 * <p>
 * Note, closed is only allowed to be called when stopped, so make sure to stop the component first.
 * Here is how the logic can be applied. A lock of the {@code lifecycleState} object is taken so that
 * another thread cannot move the state from {@code STOPPED} to {@code STARTED} before it has moved to
 * {@code CLOSED}.
 * <pre>
 * public void close() {
 *  synchronized (lifecycleState) {
 *      if (lifecycleState.started()) {
 *          stop();
 *      }
 *      if (!lifecycleState.moveToClosed()) {
 *          return;
 *      }
 *  }
 *  // perform close logic here
 * }
 * </pre>
 */
public class Lifecycle {

    /**
     * 描述生命周期的枚举
     */
    public enum State {
        /**
         * 代表组件初始化完成
         */
        INITIALIZED,
        /**
         * 组件此时处于暂停状态
         */
        STOPPED,
        /**
         * 组件开始运行
         */
        STARTED,
        /**
         * 组件已关闭
         */
        CLOSED
    }

    private volatile State state = State.INITIALIZED;

    public State state() {
        return this.state;
    }

    /**
     * Returns {@code true} if the state is initialized.
     */
    public boolean initialized() {
        return state == State.INITIALIZED;
    }

    /**
     * Returns {@code true} if the state is started.
     */
    public boolean started() {
        return state == State.STARTED;
    }

    /**
     * Returns {@code true} if the state is stopped.
     */
    public boolean stopped() {
        return state == State.STOPPED;
    }

    /**
     * Returns {@code true} if the state is closed.
     */
    public boolean closed() {
        return state == State.CLOSED;
    }

    /**
     * 表示此时状态是否已经停止或关闭
     * @return
     */
    public boolean stoppedOrClosed() {
        Lifecycle.State state = this.state;
        return state == State.STOPPED || state == State.CLOSED;
    }

    /**
     * 当前状态是否允许转换成 started 状态
     * @return
     * @throws IllegalStateException
     */
    public boolean canMoveToStarted() throws IllegalStateException {
        State localState = this.state;
        if (localState == State.INITIALIZED || localState == State.STOPPED) {
            return true;
        }
        if (localState == State.STARTED) {
            return false;
        }
        if (localState == State.CLOSED) {
            throw new IllegalStateException("Can't move to started state when closed");
        }
        throw new IllegalStateException("Can't move to started with unknown state");
    }


    public synchronized boolean moveToStarted() throws IllegalStateException {
        State localState = this.state;
        if (localState == State.INITIALIZED || localState == State.STOPPED) {
            state = State.STARTED;
            return true;
        }
        if (localState == State.STARTED) {
            return false;
        }
        if (localState == State.CLOSED) {
            throw new IllegalStateException("Can't move to started state when closed");
        }
        throw new IllegalStateException("Can't move to started with unknown state");
    }

    public boolean canMoveToStopped() throws IllegalStateException {
        State localState = state;
        if (localState == State.STARTED) {
            return true;
        }
        if (localState == State.INITIALIZED || localState == State.STOPPED) {
            return false;
        }
        if (localState == State.CLOSED) {
            throw new IllegalStateException("Can't move to stopped state when closed");
        }
        throw new IllegalStateException("Can't move to stopped with unknown state");
    }

    public synchronized boolean moveToStopped() throws IllegalStateException {
        State localState = state;
        if (localState == State.STARTED) {
            state = State.STOPPED;
            return true;
        }
        if (localState == State.INITIALIZED || localState == State.STOPPED) {
            return false;
        }
        if (localState == State.CLOSED) {
            throw new IllegalStateException("Can't move to stopped state when closed");
        }
        throw new IllegalStateException("Can't move to stopped with unknown state");
    }

    public boolean canMoveToClosed() throws IllegalStateException {
        State localState = state;
        if (localState == State.CLOSED) {
            return false;
        }
        if (localState == State.STARTED) {
            throw new IllegalStateException("Can't move to closed before moving to stopped mode");
        }
        return true;
    }

    public synchronized boolean moveToClosed() throws IllegalStateException {
        State localState = state;
        if (localState == State.CLOSED) {
            return false;
        }
        if (localState == State.STARTED) {
            throw new IllegalStateException("Can't move to closed before moving to stopped mode");
        }
        state = State.CLOSED;
        return true;
    }

    @Override
    public String toString() {
        return state.toString();
    }

}
