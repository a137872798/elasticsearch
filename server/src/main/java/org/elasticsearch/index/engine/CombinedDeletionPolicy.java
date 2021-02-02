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

package org.elasticsearch.index.engine;

import com.carrotsearch.hppc.ObjectIntHashMap;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * An {@link IndexDeletionPolicy} that coordinates between Lucene's commits and the retention of translog generation files,
 * making sure that all translog files that are needed to recover from the Lucene commit are not deleted.
 * <p>
 * In particular, this policy will delete index commits whose max sequence number is at most
 * the current global checkpoint except the index commit which has the highest max sequence number among those.
 * lucene本身是通过删除策略确定 哪些文件会被删除 默认是KeepOnlyLastCommitDeletionPolicy 也就是仅保留最新的segment
 */
public class CombinedDeletionPolicy extends IndexDeletionPolicy {
    private final Logger logger;
    private final TranslogDeletionPolicy translogDeletionPolicy;
    private final SoftDeletesPolicy softDeletesPolicy;
    private final LongSupplier globalCheckpointSupplier;

    /**
     * 存储在该容器中的对象代表正在被使用 不宜直接删除
     */
    private final ObjectIntHashMap<IndexCommit> snapshottedCommits; // Number of snapshots held against each commit point.

    /**
     * 最近一次检测得到的 与globalCheckpoint 匹配的commit 该commit之后的commit都是还未在集群完成同步的 所以不能删除
     */
    private volatile IndexCommit safeCommit; // the most recent safe commit point - its max_seqno at most the persisted global checkpoint.
    private volatile long maxSeqNoOfNextSafeCommit;
    private volatile IndexCommit lastCommit; // the most recent commit point
    private volatile SafeCommitInfo safeCommitInfo = SafeCommitInfo.EMPTY;

    /**
     *
     * @param logger
     * @param translogDeletionPolicy   事务日志的删除策略
     * @param softDeletesPolicy   软删除策略
     * @param globalCheckpointSupplier  通过translog最新的 checkpoint 获取globalCheckpoint
     */
    CombinedDeletionPolicy(Logger logger, TranslogDeletionPolicy translogDeletionPolicy,
                           SoftDeletesPolicy softDeletesPolicy, LongSupplier globalCheckpointSupplier) {
        this.logger = logger;
        this.translogDeletionPolicy = translogDeletionPolicy;
        this.softDeletesPolicy = softDeletesPolicy;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.snapshottedCommits = new ObjectIntHashMap<>();
    }

    /**
     * 通过ES.engine创建的 indexWriter 使用该删除策略来替代默认的删除策略
     * 在初始化IndexFileDeleter 的过程中会触发 deletionPolicy.onInit()
     * @param commits  这里至少会传入一个空的 IndexCommit
     * @throws IOException
     */
    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        assert commits.isEmpty() == false : "index is opened, but we have no commits";
        onCommit(commits);
        if (safeCommit != commits.get(commits.size() - 1)) {
            throw new IllegalStateException("Engine is opened, but the last commit isn't safe. Global checkpoint ["
                + globalCheckpointSupplier.getAsLong() + "], seqNo is last commit ["
                + SequenceNumbers.loadSeqNoInfoFromLuceneCommit(lastCommit.getUserData().entrySet()) + "], "
                + "seqNos in safe commit [" + SequenceNumbers.loadSeqNoInfoFromLuceneCommit(safeCommit.getUserData().entrySet()) + "]");
        }
    }

    /**
     * 从候选列表中选择哪些文件将会被删除
     * @param commits
     * @throws IOException
     */
    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        final IndexCommit safeCommit;
        synchronized (this) {
            // 找到需要保存的最小的commit 对象
            // 实际上就是检测全局检查点 代表在集群的所有分片上(也可能是超半数节点,目前还不清楚) 数据都已经提交到了某个位置
            // 这样之前的数据就可以删除了
            final int keptPosition = indexOfKeptCommits(commits, globalCheckpointSupplier.getAsLong());
            this.safeCommitInfo = SafeCommitInfo.EMPTY;
            this.lastCommit = commits.get(commits.size() - 1);
            this.safeCommit = commits.get(keptPosition);

            // 安全点代表所有节点都已经同步完前面的数据了 所以可以删除
            for (int i = 0; i < keptPosition; i++) {
                // 将没有被使用的commit删除
                if (snapshottedCommits.containsKey(commits.get(i)) == false) {
                    // 加入到删除队列中  先进入删除队列才有可能减少引用计数 才有可能被删除
                    deleteCommit(commits.get(i));
                }
            }
            updateRetentionPolicy();
            // 本次刚好保留的是最后一个commit
            if (keptPosition == commits.size() - 1) {
                this.maxSeqNoOfNextSafeCommit = Long.MAX_VALUE;
            } else {
                // 选择下一个commit.MAX_SEQ_NO
                this.maxSeqNoOfNextSafeCommit = Long.parseLong(commits.get(keptPosition + 1).getUserData().get(SequenceNumbers.MAX_SEQ_NO));
            }
            safeCommit = this.safeCommit;
        }

        assert Thread.holdsLock(this) == false : "should not block concurrent acquire or relesase";
        // 生成一个安全提交点对象
        safeCommitInfo = new SafeCommitInfo(Long.parseLong(
            safeCommit.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)), getDocCountOfCommit(safeCommit));

        // This is protected from concurrent calls by a lock on the IndexWriter, but this assertion makes sure that we notice if that ceases
        // to be true in future. It is not disastrous if safeCommitInfo refers to an older safeCommit, it just means that we might retain a
        // bit more history and do a few more ops-based recoveries than we would otherwise.
        final IndexCommit newSafeCommit = this.safeCommit;
        assert safeCommit == newSafeCommit
            : "onCommit called concurrently? " + safeCommit.getGeneration() + " vs " + newSafeCommit.getGeneration();
    }

    /**
     * 将 commit 加入到删除列表
     * @param commit
     * @throws IOException
     */
    private void deleteCommit(IndexCommit commit) throws IOException {
        assert commit.isDeleted() == false : "Index commit [" + commitDescription(commit) + "] is deleted twice";
        logger.debug("Delete index commit [{}]", commitDescription(commit));
        commit.delete();
        assert commit.isDeleted() : "Deletion commit [" + commitDescription(commit) + "] was suppressed";
    }

    /**
     * 因为最新的commit可能数据还没有同步到其他节点 并在该删除策略中被废弃
     * 获取可信任的commit中记录的本地检查点 并设置到2个删除策略中
     * @throws IOException
     */
    private void updateRetentionPolicy() throws IOException {
        assert Thread.holdsLock(this);
        logger.debug("Safe commit [{}], last commit [{}]", commitDescription(safeCommit), commitDescription(lastCommit));
        assert safeCommit.isDeleted() == false : "The safe commit must not be deleted";
        assert lastCommit.isDeleted() == false : "The last commit must not be deleted";
        final long localCheckpointOfSafeCommit = Long.parseLong(safeCommit.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
        softDeletesPolicy.setLocalCheckpointOfSafeCommit(localCheckpointOfSafeCommit);
        translogDeletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpointOfSafeCommit);
    }

    /**
     * 检测该segment 下总计有多少doc
     * @param indexCommit
     * @return
     * @throws IOException
     */
    protected int getDocCountOfCommit(IndexCommit indexCommit) throws IOException {
        return SegmentInfos.readCommit(indexCommit.getDirectory(), indexCommit.getSegmentsFileName()).totalMaxDoc();
    }

    SafeCommitInfo getSafeCommitInfo() {
        return safeCommitInfo;
    }

    /**
     * Captures the most recent commit point {@link #lastCommit} or the most recent safe commit point {@link #safeCommit}.
     * Index files of the capturing commit point won't be released until the commit reference is closed.
     *
     * @param acquiringSafeCommit captures the most recent safe commit point if true; otherwise captures the most recent commit point.
     */
    synchronized IndexCommit acquireIndexCommit(boolean acquiringSafeCommit) {
        assert safeCommit != null : "Safe commit is not initialized yet";
        assert lastCommit != null : "Last commit is not initialized yet";
        final IndexCommit snapshotting = acquiringSafeCommit ? safeCommit : lastCommit;
        // 某个副本可能正在借用这个commit进行数据恢复  当加入到该容器后 就不会在onCommit时被标记成需要删除了
        snapshottedCommits.addTo(snapshotting, 1); // increase refCount
        return new SnapshotIndexCommit(snapshotting);
    }

    /**
     * Releases an index commit that acquired by {@link #acquireIndexCommit(boolean)}.
     *
     * @return true if the snapshotting commit can be clean up.
     * 当快照使用完毕后 进行释放
     */
    synchronized boolean releaseCommit(final IndexCommit snapshotCommit) {
        final IndexCommit releasingCommit = ((SnapshotIndexCommit) snapshotCommit).delegate;
        assert snapshottedCommits.containsKey(releasingCommit) : "Release non-snapshotted commit;" +
            "snapshotted commits [" + snapshottedCommits + "], releasing commit [" + releasingCommit + "]";
        // 在获取某个IndexCommit时 会增加相关的引用计数
        final int refCount = snapshottedCommits.addTo(releasingCommit, -1); // release refCount
        assert refCount >= 0 : "Number of snapshots can not be negative [" + refCount + "]";
        if (refCount == 0) {
            snapshottedCommits.remove(releasingCommit);
        }
        // The commit can be clean up only if no pending snapshot and it is neither the safe commit nor last commit.
        // 是否可以清理这个commit
        return refCount == 0 && releasingCommit.equals(safeCommit) == false && releasingCommit.equals(lastCommit) == false;
    }

    /**
     * Find a safe commit point from a list of existing commits based on the supplied global checkpoint.
     * The max sequence number of a safe commit point should be at most the global checkpoint.
     * If an index was created before 6.2 or recovered from remote, we might not have a safe commit.
     * In this case, this method will return the oldest index commit.
     *
     * @param commits          a list of existing commit points
     * @param globalCheckpoint the persisted global checkpoint from the translog, see {@link Translog#readGlobalCheckpoint(Path, String)}
     * @return a safe commit or the oldest commit if a safe commit is not found
     * 找到一个安全的提交点
     */
    public static IndexCommit findSafeCommitPoint(List<IndexCommit> commits, long globalCheckpoint) throws IOException {
        if (commits.isEmpty()) {
            throw new IllegalArgumentException("Commit list must not empty");
        }
        final int keptPosition = indexOfKeptCommits(commits, globalCheckpoint);
        // 只有被返回的提交点会被保留 其余的会被删除
        return commits.get(keptPosition);
    }

    /**
     * Find the highest index position of a safe index commit whose max sequence number is not greater than the global checkpoint.
     * Index commits with different translog UUID will be filtered out as they don't belong to this engine.
     * @param commits   本次从shard目录下扫描到的所有 segmentInfos
     * @param globalCheckpoint   从事务日志上获取的最新刷盘的全局检查点
     */
    private static int indexOfKeptCommits(List<? extends IndexCommit> commits, long globalCheckpoint) throws IOException {
        // 当通过refresh 间接触发刷盘时 不会更新segmentInfos的 userData
        // 当主动发起indexWriter.commit时 会更新segmentInfos的 userData
        final String expectedTranslogUUID = commits.get(commits.size() - 1).getUserData().get(Translog.TRANSLOG_UUID_KEY);

        // Commits are sorted by age (the 0th one is the oldest commit).
        // 从后往前找 commit
        for (int i = commits.size() - 1; i >= 0; i--) {
            final Map<String, String> commitUserData = commits.get(i).getUserData();
            // Ignore index commits with different translog uuid.
            // 至少要确保事务日志id 一致
            if (expectedTranslogUUID.equals(commitUserData.get(Translog.TRANSLOG_UUID_KEY)) == false) {
                return i + 1;
            }

            // 因为userData的更新 与事务日志的更新存在时间差   所以允许userData中的偏移量小于事务日志的全局检查点 但是这部分数据是一定能通过 事务日志进行还原的
            // 既然事务日志能够将全局检查点更新到这里就代表日志文件中至少记录了这么多数据
            final long maxSeqNoFromCommit = Long.parseLong(commitUserData.get(SequenceNumbers.MAX_SEQ_NO));
            if (maxSeqNoFromCommit <= globalCheckpoint) {
                return i;
            }
        }
        // If an index was created before 6.2 or recovered from remote, we might not have a safe commit.
        // In this case, we return the oldest index commit instead.
        return 0;
    }

    /**
     * Checks whether the deletion policy is holding on to snapshotted commits
     */
    synchronized boolean hasSnapshottedCommits() {
        return snapshottedCommits.isEmpty() == false;
    }

    /**
     * Checks if the deletion policy can delete some index commits with the latest global checkpoint.
     * 是否有检测的必要 每当在某个时刻确定了一个safeCommit时 会找到下一个 commit的seq  当全局检查点更新 并超过它时 才有进行删除的必要
     */
    boolean hasUnreferencedCommits() {
        return maxSeqNoOfNextSafeCommit <= globalCheckpointSupplier.getAsLong();
    }

    /**
     * Returns a description for a given {@link IndexCommit}. This should be only used for logging and debugging.
     */
    public static String commitDescription(IndexCommit commit) throws IOException {
        return String.format(Locale.ROOT, "CommitPoint{segment[%s], userData[%s]}", commit.getSegmentsFileName(), commit.getUserData());
    }

    /**
     * A wrapper of an index commit that prevents it from being deleted.
     * 对应某次提交相关信息的快照
     */
    private static class SnapshotIndexCommit extends IndexCommit {
        private final IndexCommit delegate;

        SnapshotIndexCommit(IndexCommit delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getSegmentsFileName() {
            return delegate.getSegmentsFileName();
        }

        @Override
        public Collection<String> getFileNames() throws IOException {
            return delegate.getFileNames();
        }

        @Override
        public Directory getDirectory() {
            return delegate.getDirectory();
        }

        @Override
        public void delete() {
            throw new UnsupportedOperationException("A snapshot commit does not support deletion");
        }

        @Override
        public boolean isDeleted() {
            return delegate.isDeleted();
        }

        @Override
        public int getSegmentCount() {
            return delegate.getSegmentCount();
        }

        @Override
        public long getGeneration() {
            return delegate.getGeneration();
        }

        @Override
        public Map<String, String> getUserData() throws IOException {
            return delegate.getUserData();
        }

        @Override
        public String toString() {
            return "SnapshotIndexCommit{" + delegate + "}";
        }
    }
}
