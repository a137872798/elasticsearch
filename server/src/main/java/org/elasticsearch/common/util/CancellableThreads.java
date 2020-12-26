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
package org.elasticsearch.common.util;

import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A utility class for multi threaded operation that needs to be cancellable via interrupts. Every cancellable operation should be
 * executed via {@link #execute(Interruptible)}, which will capture the executing thread and make sure it is interrupted in the case
 * of cancellation.
 *
 * Cancellation policy: This class does not support external interruption via <code>Thread#interrupt()</code>. Always use #cancel() instead.
 * 通过该对象执行的线程 可以感知到外部的打断 同时抛出异常
 */
public class CancellableThreads {

    /**
     * 此时正在执行任务的线程
     */
    private final Set<Thread> threads = new HashSet<>();
    // needs to be volatile as it is also read outside of synchronized blocks.
    private volatile boolean cancelled = false;
    private final SetOnce<OnCancel> onCancel = new SetOnce<>();
    private String reason;

    public synchronized boolean isCancelled() {
        return cancelled;
    }

    public void checkForCancel() {
        checkForCancel(null);
    }


    /**
     * 每当新增一个需要管理的线程时 就会检测当前对象是否处于关闭状态
     * @param beforeCancelException  如果发现此时处于关闭状态 那么处理该异常
     */
    private void checkForCancel(Exception beforeCancelException) {
        if (isCancelled()) {
            final String reason;
            final OnCancel onCancel;
            synchronized (this) {
                reason = this.reason;
                onCancel = this.onCancel.get();
            }
            if (onCancel != null) {
                onCancel.onCancel(reason, beforeCancelException);
            }
            // fallback to the default exception
            final RuntimeException cancelExp = new ExecutionCancelledException("operation was cancelled reason [" + reason + "]");
            if (beforeCancelException != null) {
                cancelExp.addSuppressed(beforeCancelException);
            }
            throw cancelExp;
        }
    }

    private synchronized boolean add() {
        // 确保此时不是cancel
        checkForCancel();
        threads.add(Thread.currentThread());
        // capture and clean the interrupted thread before we start, so we can identify
        // our own interrupt. we do so under lock so we know we don't clear our own.
        return Thread.interrupted();
    }

    /**
     * run the Interruptible, capturing the executing thread. Concurrent calls to {@link #cancel(String)} will interrupt this thread
     * causing the call to prematurely return.
     *
     * @param interruptible code to run
     */
    public void execute(Interruptible interruptible) {
        try {
            executeIO(interruptible);
        } catch (IOException e) {
            assert false : "the passed interruptible can not result in an IOException";
            throw new RuntimeException("unexpected IO exception", e);
        }
    }
    /**
     * run the Interruptible, capturing the executing thread. Concurrent calls to {@link #cancel(String)} will interrupt this thread
     * causing the call to prematurely return.
     *
     * @param interruptible code to run
     *                 执行任务的线程都是可打断的  通过该对象进行统一维护 当这些任务需要被打断时 可以一次性中断所有任务
     */
    public void executeIO(IOInterruptible interruptible) throws IOException {
        // 代表线程在加入前是 被中断状态  在调用add后会恢复成普通状态 这样就可以再次感知到线程被打断并触发对应异常
        boolean wasInterrupted = add();
        boolean cancelledByExternalInterrupt = false;
        RuntimeException runtimeException = null;
        IOException ioException = null;

        try {
            // 执行任务
            interruptible.run();
        } catch (InterruptedException | ThreadInterruptedException e) {
            // ignore, this interrupt has been triggered by us in #cancel()...
            assert cancelled : "Interruption via Thread#interrupt() is unsupported. Use CancellableThreads#cancel() instead";
            // we can only reach here if assertions are disabled. If we reach this code and cancelled is false, this means that we've
            // been interrupted externally (which we don't support).

            // 如果是任务被正常关闭  cacelled为true    如果是false 代表是预期外的中断
            cancelledByExternalInterrupt = !cancelled;
        } catch (RuntimeException t) {
            runtimeException = t;
        } catch (IOException e) {
            ioException = e;
        } finally {
            // 从容器中移除
            remove();
        }
        // we are now out of threads collection so we can't be interrupted any more by this class
        // restore old flag and see if we need to fail
        // 如果之前线程已经被标识成打断状态 恢复之前的标识
        if (wasInterrupted) {
            Thread.currentThread().interrupt();
        } else {
            // clear the flag interrupted flag as we are checking for failure..
            // 如果在加入后发生了中断 需要清除这个标识
            Thread.interrupted();
        }

        // 检查是否被关闭 返回包装后的异常
        checkForCancel(ioException != null ? ioException : runtimeException);
        if (ioException != null) {
            // if we're not canceling, we throw the original exception
            throw ioException;
        }
        if (runtimeException != null) {
            // if we're not canceling, we throw the original exception
            throw runtimeException;
        }
        if (cancelledByExternalInterrupt) {
            // restore interrupt flag to at least adhere to expected behavior
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interruption via Thread#interrupt() is unsupported. Use CancellableThreads#cancel() instead");
        }

    }


    private synchronized void remove() {
        threads.remove(Thread.currentThread());
    }

    /** cancel all current running operations. Future calls to {@link #checkForCancel()} will be failed with the given reason */
    // 将当前对象设置成关闭状态 同时打断所有运行中的线程
    public synchronized void cancel(String reason) {
        if (cancelled) {
            // we were already cancelled, make sure we don't interrupt threads twice
            // this is important in order to make sure that we don't mark
            // Thread.interrupted without handling it
            return;
        }
        cancelled = true;
        this.reason = reason;
        for (Thread thread : threads) {
            thread.interrupt();
        }
        threads.clear();
    }


    public interface Interruptible extends IOInterruptible {
        void run() throws InterruptedException;
    }

    public interface IOInterruptible {
        void run() throws IOException, InterruptedException;
    }

    public static class ExecutionCancelledException extends ElasticsearchException {

        public ExecutionCancelledException(String msg) {
            super(msg);
        }

        public ExecutionCancelledException(StreamInput in) throws IOException {
            super(in);
        }
    }

    /**
     * Registers a callback that will be invoked when some running operations are cancelled or {@link #checkForCancel()} is called.
     */
    public synchronized void setOnCancel(OnCancel onCancel) {
        this.onCancel.set(onCancel);
    }

    @FunctionalInterface
    public interface OnCancel {
        /**
         * Called when some running operations are cancelled or {@link #checkForCancel()} is explicitly called.
         * If this method throws an exception, cancelling tasks will fail with that exception; otherwise they
         * will fail with the default exception {@link ExecutionCancelledException}.
         *
         * @param reason                the reason of the cancellation
         * @param beforeCancelException any error that was encountered during the execution before the operations were cancelled.
         * @see #checkForCancel()
         * @see #setOnCancel(OnCancel)
         */
        void onCancel(String reason, @Nullable Exception beforeCancelException);
    }
}
