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

package org.elasticsearch.index.snapshots;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represent shard snapshot status
 * 描述某个分片执行快照任务的信息
 */
public class IndexShardSnapshotStatus {

    /**
     * Snapshot stage
     */
    public enum Stage {
        /**
         * Snapshot hasn't started yet
         */
        INIT,
        /**
         * Index files are being copied
         */
        STARTED,
        /**
         * Snapshot metadata is being written
         */
        FINALIZE,
        /**
         * Snapshot completed successfully
         */
        DONE,
        /**
         * Snapshot failed
         */
        FAILURE,
        /**
         * Snapshot aborted
         */
        ABORTED
    }

    /**
     * 当前执行到快照的哪一步
     */
    private final AtomicReference<Stage> stage;
    private final AtomicReference<String> generation;
    private long startTime;
    private long totalTime;
    private int incrementalFileCount;
    private int totalFileCount;
    private int processedFileCount;
    private long totalSize;
    private long incrementalSize;
    private long processedSize;
    private long indexVersion;
    private String failure;

    private IndexShardSnapshotStatus(final Stage stage, final long startTime, final long totalTime,
                                     final int incrementalFileCount, final int totalFileCount, final int processedFileCount,
                                     final long incrementalSize, final long totalSize, final long processedSize, final String failure,
                                     final String generation) {
        this.stage = new AtomicReference<>(Objects.requireNonNull(stage));
        this.generation = new AtomicReference<>(generation);
        this.startTime = startTime;
        this.totalTime = totalTime;
        this.incrementalFileCount = incrementalFileCount;
        this.totalFileCount = totalFileCount;
        this.processedFileCount = processedFileCount;
        this.totalSize = totalSize;
        this.processedSize = processedSize;
        this.incrementalSize = incrementalSize;
        this.failure = failure;
    }

    /**
     * 将此时的快照状态机 切换到start状态
     * 如果本次执行快照前 lucene.commit 生成的数据 与 快照仓库中原数据完全一致 那么实际上本次不需要执行快照任务 有关文件数/文件大小的参数都是0
     * @param startTime
     * @param incrementalFileCount  本次增加了多少新的文件
     * @param totalFileCount
     * @param incrementalSize  本次增加的所有文件的总大小
     * @param totalSize
     * @return
     */
    public synchronized Copy moveToStarted(final long startTime, final int incrementalFileCount, final int totalFileCount,
                                           final long incrementalSize, final long totalSize) {
        if (stage.compareAndSet(Stage.INIT, Stage.STARTED)) {
            this.startTime = startTime;
            this.incrementalFileCount = incrementalFileCount;
            this.totalFileCount = totalFileCount;
            this.incrementalSize = incrementalSize;
            this.totalSize = totalSize;
        } else {
            throw new IllegalStateException("Unable to move the shard snapshot status to [STARTED]: " +
                "expecting [INIT] but got [" + stage.get() + "]");
        }
        return asCopy();
    }

    /**
     * 快照任务已经结束  之后会进入到写元数据的阶段
     * @param indexVersion
     * @return
     */
    public synchronized Copy moveToFinalize(final long indexVersion) {
        if (stage.compareAndSet(Stage.STARTED, Stage.FINALIZE)) {
            this.indexVersion = indexVersion;
        } else {
            throw new IllegalStateException("Unable to move the shard snapshot status to [FINALIZE]: " +
                "expecting [STARTED] but got [" + stage.get() + "]");
        }
        return asCopy();
    }

    /**
     * finalize 代表快照文件写入完成  done 代表最新的快照信息已经同步到  BlobStoreIndexShardSnapshots 上
     * 该对象管理所有生成的快照信息 也就是触发了几次快照 每个快照生成多少文件等等
     * @param endTime
     * @param newGeneration  本次生成快照的shard 对应的gen   每执行一次快照任务 对应一个gen
     */
    public synchronized void moveToDone(final long endTime, final String newGeneration) {
        assert newGeneration != null;
        if (stage.compareAndSet(Stage.FINALIZE, Stage.DONE)) {
            this.totalTime = Math.max(0L, endTime - startTime);
            this.generation.set(newGeneration);
        } else {
            throw new IllegalStateException("Unable to move the shard snapshot status to [DONE]: " +
                "expecting [FINALIZE] but got [" + stage.get() + "]");
        }
    }

    /**
     * 将该index级别的快照任务 设置成 aborted
     * 因为ES的选举算法可能会导致某次clusterState的更新被覆盖  只要在整个快照流程中 某个leader发生变化 到了一个之前没有收到快照任务的节点上  就要终止之前的快照任务
     * 也就是只要更新可能会被覆盖的情况 就要借助本地的续约机制
     * @param failure
     */
    public synchronized void abortIfNotCompleted(final String failure) {
        if (stage.compareAndSet(Stage.INIT, Stage.ABORTED) || stage.compareAndSet(Stage.STARTED, Stage.ABORTED)) {
            this.failure = failure;
        }
    }

    /**
     * 快照任务在执行过程中失败了
     * @param endTime
     * @param failure
     */
    public synchronized void moveToFailed(final long endTime, final String failure) {
        if (stage.getAndSet(Stage.FAILURE) != Stage.FAILURE) {
            this.totalTime = Math.max(0L, endTime - startTime);
            this.failure = failure;
        }
    }

    public String generation() {
        return generation.get();
    }

    public boolean isAborted() {
        return stage.get() == Stage.ABORTED;
    }

    /**
     * Increments number of processed files
     * 当某次写入快照文件完成时  会调用该方法
     */
    public synchronized void addProcessedFile(long size) {
        processedFileCount++;
        processedSize += size;
    }

    /**
     * Returns a copy of the current {@link IndexShardSnapshotStatus}. This method is
     * intended to be used when a coherent state of {@link IndexShardSnapshotStatus} is needed.
     *
     * @return a  {@link IndexShardSnapshotStatus.Copy}
     */
    public synchronized IndexShardSnapshotStatus.Copy asCopy() {
        return new IndexShardSnapshotStatus.Copy(stage.get(), startTime, totalTime,
            incrementalFileCount, totalFileCount, processedFileCount,
            incrementalSize, totalSize, processedSize,
            indexVersion, failure);
    }

    /**
     * 描述本地快照任务执行状态的对象   对应的还有 shardSnapshotStatus 描述快照任务的分片级信息
     * @param generation
     * @return
     */
    public static IndexShardSnapshotStatus newInitializing(String generation) {
        return new IndexShardSnapshotStatus(Stage.INIT, 0L, 0L, 0, 0, 0, 0, 0, 0, null, generation);
    }

    public static IndexShardSnapshotStatus newFailed(final String failure) {
        assert failure != null : "expecting non null failure for a failed IndexShardSnapshotStatus";
        if (failure == null) {
            throw new IllegalArgumentException("A failure description is required for a failed IndexShardSnapshotStatus");
        }
        return new IndexShardSnapshotStatus(Stage.FAILURE, 0L, 0L, 0, 0, 0, 0, 0, 0, failure, null);
    }

    public static IndexShardSnapshotStatus newDone(final long startTime, final long totalTime,
                                                   final int incrementalFileCount, final int fileCount,
                                                   final long incrementalSize, final long size, String generation) {
        // The snapshot is done which means the number of processed files is the same as total
        return new IndexShardSnapshotStatus(Stage.DONE, startTime, totalTime, incrementalFileCount, fileCount, incrementalFileCount,
            incrementalSize, size, incrementalSize, null, generation);
    }

    /**
     * Returns an immutable state of {@link IndexShardSnapshotStatus} at a given point in time.
     */
    public static class Copy {

        private final Stage stage;
        private final long startTime;
        private final long totalTime;
        private final int incrementalFileCount;
        private final int totalFileCount;
        private final int processedFileCount;
        private final long totalSize;
        private final long processedSize;
        private final long incrementalSize;
        private final long indexVersion;
        private final String failure;

        public Copy(final Stage stage, final long startTime, final long totalTime,
                    final int incrementalFileCount, final int totalFileCount, final int processedFileCount,
                    final long incrementalSize, final long totalSize, final long processedSize,
                    final long indexVersion, final String failure) {
            this.stage = stage;
            this.startTime = startTime;
            this.totalTime = totalTime;
            this.incrementalFileCount = incrementalFileCount;
            this.totalFileCount = totalFileCount;
            this.processedFileCount = processedFileCount;
            this.totalSize = totalSize;
            this.processedSize = processedSize;
            this.incrementalSize = incrementalSize;
            this.indexVersion = indexVersion;
            this.failure = failure;
        }

        public Stage getStage() {
            return stage;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getTotalTime() {
            return totalTime;
        }

        public int getIncrementalFileCount() {
            return incrementalFileCount;
        }

        public int getTotalFileCount() {
            return totalFileCount;
        }

        public int getProcessedFileCount() {
            return processedFileCount;
        }

        public long getIncrementalSize() {
            return incrementalSize;
        }

        public long getTotalSize() {
            return totalSize;
        }

        public long getProcessedSize() {
            return processedSize;
        }

        public long getIndexVersion() {
            return indexVersion;
        }

        public String getFailure() {
            return failure;
        }

        @Override
        public String toString() {
            return "index shard snapshot status (" +
                "stage=" + stage +
                ", startTime=" + startTime +
                ", totalTime=" + totalTime +
                ", incrementalFileCount=" + incrementalFileCount +
                ", totalFileCount=" + totalFileCount +
                ", processedFileCount=" + processedFileCount +
                ", incrementalSize=" + incrementalSize +
                ", totalSize=" + totalSize +
                ", processedSize=" + processedSize +
                ", indexVersion=" + indexVersion +
                ", failure='" + failure + '\'' +
                ')';
        }
    }
}
