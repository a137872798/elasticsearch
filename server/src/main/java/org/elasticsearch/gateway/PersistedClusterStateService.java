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
package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.index.Index;

import java.io.Closeable;
import java.io.FilterOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntPredicate;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Stores cluster metadata in a bare Lucene index (per data path) split across a number of documents. This is used by master-eligible nodes
 * to record the last-accepted cluster state during publication. The metadata is written incrementally where possible, leaving alone any
 * documents that have not changed. The index has the following fields:
 *
 * +------------------------------+-----------------------------+----------------------------------------------+
 * | "type" (string field)        | "index_uuid" (string field) | "data" (stored binary field in SMILE format) |
 * +------------------------------+-----------------------------+----------------------------------------------+
 * | GLOBAL_TYPE_NAME == "global" | (omitted)                   | Global metadata                              |
 * | INDEX_TYPE_NAME  == "index"  | Index UUID                  | Index metadata                               |
 * +------------------------------+-----------------------------+----------------------------------------------+
 *
 * Additionally each commit has the following user data:
 *
 * +---------------------------+-------------------------+-------------------------------------------------------------------------------+
 * |        Key symbol         |       Key literal       |                                     Value                                     |
 * +---------------------------+-------------------------+-------------------------------------------------------------------------------+
 * | CURRENT_TERM_KEY          | "current_term"          | Node's "current" term (≥ last-accepted term and the terms of all sent joins)  |
 * | LAST_ACCEPTED_VERSION_KEY | "last_accepted_version" | The cluster state version corresponding with the persisted metadata           |
 * | NODE_ID_KEY               | "node_id"               | The (persistent) ID of the node that wrote this metadata                      |
 * | NODE_VERSION_KEY          | "node_version"          | The (ID of the) version of the node that wrote this metadata                  |
 * +---------------------------+-------------------------+-------------------------------------------------------------------------------+
 *
 * (the last-accepted term is recorded in Metadata → CoordinationMetadata so does not need repeating here)
 * 对集群状态做持久化的服务
 */
public class PersistedClusterStateService {
    private static final Logger logger = LogManager.getLogger(PersistedClusterStateService.class);
    private static final String CURRENT_TERM_KEY = "current_term";
    private static final String LAST_ACCEPTED_VERSION_KEY = "last_accepted_version";
    private static final String NODE_ID_KEY = "node_id";
    private static final String NODE_VERSION_KEY = "node_version";
    private static final String TYPE_FIELD_NAME = "type";
    private static final String DATA_FIELD_NAME = "data";
    private static final String GLOBAL_TYPE_NAME = "global";
    private static final String INDEX_TYPE_NAME = "index";
    private static final String INDEX_UUID_FIELD_NAME = "index_uuid";
    private static final int COMMIT_DATA_SIZE = 4;

    public static final String METADATA_DIRECTORY_NAME = MetadataStateFormat.STATE_DIR_NAME;

    public static final Setting<TimeValue> SLOW_WRITE_LOGGING_THRESHOLD = Setting.timeSetting("gateway.slow_write_logging_threshold",
        TimeValue.timeValueSeconds(10), TimeValue.ZERO, Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * 存储数据的路径 从env中获取
     */
    private final Path[] dataPaths;
    private final String nodeId;
    private final NamedXContentRegistry namedXContentRegistry;
    private final BigArrays bigArrays;
    private final LongSupplier relativeTimeMillisSupplier;

    private volatile TimeValue slowWriteLoggingThreshold;

    public PersistedClusterStateService(NodeEnvironment nodeEnvironment, NamedXContentRegistry namedXContentRegistry, BigArrays bigArrays,
                                        ClusterSettings clusterSettings, LongSupplier relativeTimeMillisSupplier) {
        this(nodeEnvironment.nodeDataPaths(), nodeEnvironment.nodeId(), namedXContentRegistry, bigArrays, clusterSettings,
            relativeTimeMillisSupplier);
    }

    /**
     * 从本地磁盘中加载之前持久化的有关集群的数据
     * @param dataPaths 存储数据的路径
     * @param nodeId  当前节点id
     * @param namedXContentRegistry  通过name可以找到对应的 reader对象 定义了如何将数据流反序列化成对象
     * @param bigArrays
     * @param clusterSettings
     * @param relativeTimeMillisSupplier
     */
    public PersistedClusterStateService(Path[] dataPaths, String nodeId, NamedXContentRegistry namedXContentRegistry, BigArrays bigArrays,
                                        ClusterSettings clusterSettings, LongSupplier relativeTimeMillisSupplier) {
        this.dataPaths = dataPaths;
        this.nodeId = nodeId;
        this.namedXContentRegistry = namedXContentRegistry;
        this.bigArrays = bigArrays;
        this.relativeTimeMillisSupplier = relativeTimeMillisSupplier;
        this.slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD);
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD, this::setSlowWriteLoggingThreshold);
    }

    private void setSlowWriteLoggingThreshold(TimeValue slowWriteLoggingThreshold) {
        this.slowWriteLoggingThreshold = slowWriteLoggingThreshold;
    }

    public String getNodeId() {
        return nodeId;
    }

    /**
     * Creates a new disk-based writer for cluster states
     * 这里创建一个负责持久化 CS的writer对象
     */
    public Writer createWriter() throws IOException {
        final List<MetadataIndexWriter> metadataIndexWriters = new ArrayList<>();
        final List<Closeable> closeables = new ArrayList<>();
        boolean success = false;
        try {
            for (final Path path : dataPaths) {
                // 给每个dataPath创建 存储metadata的目录
                final Directory directory = createDirectory(path.resolve(METADATA_DIRECTORY_NAME));
                closeables.add(directory);

                // 生成对应的indexWriter对象
                final IndexWriter indexWriter = createIndexWriter(directory, false);
                closeables.add(indexWriter);
                metadataIndexWriters.add(new MetadataIndexWriter(directory, indexWriter));
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(closeables);
            }
        }
        // 将一组 metadataIndexWriter合并成writer对象
        return new Writer(metadataIndexWriters, nodeId, bigArrays, relativeTimeMillisSupplier, () -> slowWriteLoggingThreshold);
    }

    /**
     * 创建一个 indexWriter对象 在之后负责写metadata
     * @param directory
     * @param openExisting
     * @return
     * @throws IOException
     */
    private static IndexWriter createIndexWriter(Directory directory, boolean openExisting) throws IOException {
        final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        // start empty since we re-write the whole cluster state to ensure it is all using the same format version
        indexWriterConfig.setOpenMode(openExisting ? IndexWriterConfig.OpenMode.APPEND : IndexWriterConfig.OpenMode.CREATE);
        // only commit when specifically instructed, we must not write any intermediate states
        indexWriterConfig.setCommitOnClose(false);
        // most of the data goes into stored fields which are not buffered, so we only really need a tiny buffer
        indexWriterConfig.setRAMBufferSizeMB(1.0);
        // merge on the write thread (e.g. while flushing)
        indexWriterConfig.setMergeScheduler(new SerialMergeScheduler());

        return new IndexWriter(directory, indexWriterConfig);
    }

    /**
     * Remove all persisted cluster states from the given data paths, for use in tests. Should only be called when there is no open
     * {@link Writer} on these paths.
     */
    public static void deleteAll(Path[] dataPaths) throws IOException {
        for (Path dataPath : dataPaths) {
            Lucene.cleanLuceneIndex(new SimpleFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)));
        }
    }

    // exposed for tests
    Directory createDirectory(Path path) throws IOException {
        // it is possible to disable the use of MMapDirectory for indices, and it may be surprising to users that have done so if we still
        // use a MMapDirectory here, which might happen with FSDirectory.open(path). Concurrency is of no concern here so a
        // SimpleFSDirectory is fine:
        return new SimpleFSDirectory(path);
    }

    public Path[] getDataPaths() {
        return dataPaths;
    }

    /**
     * 描述从磁盘中某个地方读取到的 metadata
     */
    public static class OnDiskState {
        private static final OnDiskState NO_ON_DISK_STATE = new OnDiskState(null, null, 0L, 0L, Metadata.EMPTY_METADATA);

        private final String nodeId;
        private final Path dataPath;
        public final long currentTerm;
        public final long lastAcceptedVersion;
        public final Metadata metadata;

        private OnDiskState(String nodeId, Path dataPath, long currentTerm, long lastAcceptedVersion, Metadata metadata) {
            this.nodeId = nodeId;
            this.dataPath = dataPath;
            this.currentTerm = currentTerm;
            this.lastAcceptedVersion = lastAcceptedVersion;
            this.metadata = metadata;
        }

        public boolean empty() {
            return this == NO_ON_DISK_STATE;
        }
    }

    /**
     * Returns the node metadata for the given data paths, and checks if the node ids are unique
     * @param dataPaths the data paths to scan
     *
     */
    @Nullable
    public static NodeMetadata nodeMetadata(Path... dataPaths) throws IOException {
        String nodeId = null;
        Version version = null;
        for (final Path dataPath : dataPaths) {
            // 对应 /{data_node}/_state 文件夹    而索引级的文件夹下也有一个 _state 文件夹  内部存储的是indexMetadata
            final Path indexPath = dataPath.resolve(METADATA_DIRECTORY_NAME);
            if (Files.exists(indexPath)) {
                // 读取 _state 内部的数据  在这个目录下包含 segment_N文件
                try (DirectoryReader reader = DirectoryReader.open(new SimpleFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)))) {
                    // ES写入数据是先创建index    但是与lucene的index含义不一致  这里的 indexCommit应该指的是最近一次刷盘后更新的数据  而每次更新可能会同时影响到多个ES.index吧

                    final Map<String, String> userData = reader.getIndexCommit().getUserData();
                    assert userData.get(NODE_VERSION_KEY) != null;

                    // 必须确保每个数据文件夹下 _state 文件夹下解析的 segment_N 文件的 nodeId 数据是一致的
                    final String thisNodeId = userData.get(NODE_ID_KEY);
                    assert thisNodeId != null;
                    if (nodeId != null && nodeId.equals(thisNodeId) == false) {
                        throw new IllegalStateException("unexpected node ID in metadata, found [" + thisNodeId +
                            "] in [" + dataPath + "] but expected [" + nodeId + "]");
                    } else if (nodeId == null) {
                        nodeId = thisNodeId;
                        // 获取lucene 版本信息
                        version = Version.fromId(Integer.parseInt(userData.get(NODE_VERSION_KEY)));
                    }
                } catch (IndexNotFoundException e) {
                    logger.debug(new ParameterizedMessage("no on-disk state at {}", indexPath), e);
                }
            }
        }

        // 代表此时还没有存储有关该节点的元数据信息
        if (nodeId == null) {
            return null;
        }
        // 上面的操作是校验nodeId一致 以及获取nodeId 和 lucene版本信息
        return new NodeMetadata(nodeId, version);
    }

    /**
     * Overrides the version field for the metadata in the given data path
     */
    public static void overrideVersion(Version newVersion, Path... dataPaths) throws IOException {
        for (final Path dataPath : dataPaths) {
            final Path indexPath = dataPath.resolve(METADATA_DIRECTORY_NAME);
            if (Files.exists(indexPath)) {
                try (DirectoryReader reader = DirectoryReader.open(new SimpleFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)))) {
                    final Map<String, String> userData = reader.getIndexCommit().getUserData();
                    assert userData.get(NODE_VERSION_KEY) != null;

                    try (IndexWriter indexWriter =
                             createIndexWriter(new SimpleFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME)), true)) {
                        final Map<String, String> commitData = new HashMap<>(userData);
                        commitData.put(NODE_VERSION_KEY, Integer.toString(newVersion.id));
                        indexWriter.setLiveCommitData(commitData.entrySet());
                        indexWriter.commit();
                    }
                } catch (IndexNotFoundException e) {
                    logger.debug(new ParameterizedMessage("no on-disk state at {}", indexPath), e);
                }
            }
        }
    }

    /**
     * Loads the best available on-disk cluster state. Returns {@link OnDiskState#NO_ON_DISK_STATE} if no such state was found.
     * 从磁盘上加载最新的metadata
     */
    public OnDiskState loadBestOnDiskState() throws IOException {
        String committedClusterUuid = null;
        Path committedClusterUuidPath = null;
        OnDiskState bestOnDiskState = OnDiskState.NO_ON_DISK_STATE;
        OnDiskState maxCurrentTermOnDiskState = bestOnDiskState;

        // We use a write-all-read-one strategy: metadata is written to every data path when accepting it, which means it is mostly
        // sufficient to read _any_ copy. "Mostly" sufficient because the user can change the set of data paths when restarting, and may
        // add a data path containing a stale copy of the metadata. We deal with this by using the freshest copy we can find.
        for (final Path dataPath : dataPaths) {
            // 这个是外层的 _state目录 每个index 还专门有一个_state目录
            final Path indexPath = dataPath.resolve(METADATA_DIRECTORY_NAME);
            if (Files.exists(indexPath)) {
                try (Directory directory = createDirectory(indexPath);
                     DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                    // 从目录加载数据
                    final OnDiskState onDiskState = loadOnDiskState(dataPath, directoryReader);

                    // 当读取出来的nodeId不一致时 抛出异常
                    if (nodeId.equals(onDiskState.nodeId) == false) {
                        throw new IllegalStateException("unexpected node ID in metadata, found [" + onDiskState.nodeId +
                            "] in [" + dataPath + "] but expected [" + nodeId + "]");
                    }

                    // 代表集群的最新状态已经通过了publish的commit阶段 也就是已经生效了
                    if (onDiskState.metadata.clusterUUIDCommitted()) {
                        if (committedClusterUuid == null) {
                            committedClusterUuid = onDiskState.metadata.clusterUUID();
                            committedClusterUuidPath = dataPath;
                            // 集群id不一致时也要抛出异常
                        } else if (committedClusterUuid.equals(onDiskState.metadata.clusterUUID()) == false) {
                            throw new IllegalStateException("mismatched cluster UUIDs in metadata, found [" + committedClusterUuid +
                                "] in [" + committedClusterUuidPath + "] and [" + onDiskState.metadata.clusterUUID() + "] in ["
                                + dataPath + "]");
                        }
                    }

                    // 多个dataPath下可能会有多个metadata 只使用term最大的
                    if (maxCurrentTermOnDiskState.empty() || maxCurrentTermOnDiskState.currentTerm < onDiskState.currentTerm) {
                        maxCurrentTermOnDiskState = onDiskState;
                    }

                    long acceptedTerm = onDiskState.metadata.coordinationMetadata().term();
                    long maxAcceptedTerm = bestOnDiskState.metadata.coordinationMetadata().term();
                    // 这里就是尽可能获取最新的 OnDiskState  简化理解的话 可以当作只有一个dataPath
                    if (bestOnDiskState.empty()
                        || acceptedTerm > maxAcceptedTerm
                        || (acceptedTerm == maxAcceptedTerm
                            && (onDiskState.lastAcceptedVersion > bestOnDiskState.lastAcceptedVersion
                                || (onDiskState.lastAcceptedVersion == bestOnDiskState.lastAcceptedVersion)
                                    && onDiskState.currentTerm > bestOnDiskState.currentTerm))) {
                        bestOnDiskState = onDiskState;
                    }
                } catch (IndexNotFoundException e) {
                    logger.debug(new ParameterizedMessage("no on-disk state at {}", indexPath), e);
                }
            }
        }

        if (bestOnDiskState.currentTerm != maxCurrentTermOnDiskState.currentTerm) {
            throw new IllegalStateException("inconsistent terms found: best state is from [" + bestOnDiskState.dataPath +
                "] in term [" + bestOnDiskState.currentTerm + "] but there is a stale state in [" + maxCurrentTermOnDiskState.dataPath +
                "] with greater term [" + maxCurrentTermOnDiskState.currentTerm + "]");
        }

        return bestOnDiskState;
    }

    /**
     *
     * @param dataPath  外层的数据目录
     * @param reader  已经定位到_state目录 并生成输入流
     * @return
     * @throws IOException
     */
    private OnDiskState loadOnDiskState(Path dataPath, DirectoryReader reader) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);

        final SetOnce<Metadata.Builder> builderReference = new SetOnce<>();
        // 通过lucene查找type=global的所有doc 并获取data数据
        consumeFromType(searcher, GLOBAL_TYPE_NAME, bytes ->
        {
            // 反序列化成 metadata对象
            final Metadata metadata = Metadata.Builder.fromXContent(XContentFactory.xContent(XContentType.SMILE)
                .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, bytes.bytes, bytes.offset, bytes.length));
            logger.trace("found global metadata with last-accepted term [{}]", metadata.coordinationMetadata().term());
            // 这里认为metadata 应该只存储了一个
            if (builderReference.get() != null) {
                throw new IllegalStateException("duplicate global metadata found in [" + dataPath + "]");
            }
            builderReference.set(Metadata.builder(metadata));
        });

        final Metadata.Builder builder = builderReference.get();
        if (builder == null) {
            // 当没有查询到 metadata时抛出异常
            throw new IllegalStateException("no global metadata found in [" + dataPath + "]");
        }

        logger.trace("got global metadata, now reading index metadata");

        final Set<String> indexUUIDs = new HashSet<>();
        // 这里读取type为index的data数据   这里有多个数据 每个index对应一个doc
        consumeFromType(searcher, INDEX_TYPE_NAME, bytes ->
        {
            final IndexMetadata indexMetadata = IndexMetadata.fromXContent(XContentFactory.xContent(XContentType.SMILE)
                .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, bytes.bytes, bytes.offset, bytes.length));
            logger.trace("found index metadata for {}", indexMetadata.getIndex());
            if (indexUUIDs.add(indexMetadata.getIndexUUID()) == false) {
                throw new IllegalStateException("duplicate metadata found for " + indexMetadata.getIndex() + " in [" + dataPath + "]");
            }
            builder.put(indexMetadata, false);
        });

        // 从 segment_N 的用户数据中读取相关属性
        final Map<String, String> userData = reader.getIndexCommit().getUserData();
        logger.trace("loaded metadata [{}] from [{}]", userData, reader.directory());
        assert userData.size() == COMMIT_DATA_SIZE : userData;
        assert userData.get(CURRENT_TERM_KEY) != null;
        assert userData.get(LAST_ACCEPTED_VERSION_KEY) != null;
        assert userData.get(NODE_ID_KEY) != null;
        assert userData.get(NODE_VERSION_KEY) != null;
        return new OnDiskState(userData.get(NODE_ID_KEY), dataPath, Long.parseLong(userData.get(CURRENT_TERM_KEY)),
            Long.parseLong(userData.get(LAST_ACCEPTED_VERSION_KEY)), builder.build());
    }

    /**
     * 根据type查询数据
     * @param indexSearcher
     * @param type
     * @param bytesRefConsumer
     * @throws IOException
     */
    private static void consumeFromType(IndexSearcher indexSearcher, String type,
                                        CheckedConsumer<BytesRef, IOException> bytesRefConsumer) throws IOException {

        final Query query = new TermQuery(new Term(TYPE_FIELD_NAME, type));
        final Weight weight = indexSearcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
        logger.trace("running query [{}]", query);

        // 查询所有type=global的doc
        for (LeafReaderContext leafReaderContext : indexSearcher.getIndexReader().leaves()) {
            logger.trace("new leafReaderContext: {}", leafReaderContext);
            final Scorer scorer = weight.scorer(leafReaderContext);
            if (scorer != null) {
                final Bits liveDocs = leafReaderContext.reader().getLiveDocs();
                // 检测某个doc是否还存活
                final IntPredicate isLiveDoc = liveDocs == null ? i -> true : liveDocs::get;
                final DocIdSetIterator docIdSetIterator = scorer.iterator();
                while (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    if (isLiveDoc.test(docIdSetIterator.docID())) {
                        logger.trace("processing doc {}", docIdSetIterator.docID());
                        // 找到文档并获取 data field
                        bytesRefConsumer.accept(
                            leafReaderContext.reader().document(docIdSetIterator.docID()).getBinaryValue(DATA_FIELD_NAME));
                    }
                }
            }
        }
    }

    private static final ToXContent.Params FORMAT_PARAMS;

    static {
        Map<String, String> params = new HashMap<>(2);
        params.put("binary", "true");
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new ToXContent.MapParams(params);
    }

    /**
     * A {@link Document} with a stored field containing serialized metadata written to a {@link ReleasableBytesStreamOutput} which must be
     * released when no longer needed.
     */
    private static class ReleasableDocument implements Releasable {
        private final Document document;
        private final Releasable releasable;

        ReleasableDocument(Document document, Releasable releasable) {
            this.document = document;
            this.releasable = releasable;
        }

        Document getDocument() {
            return document;
        }

        @Override
        public void close() {
            releasable.close();
        }
    }

    /**
     * Encapsulates a single {@link IndexWriter} with its {@link Directory} for ease of closing, and a {@link Logger}. There is one of these
     * for each data path.
     * 该对象负责将 metadata写入到lucene中
     */
    private static class MetadataIndexWriter implements Closeable {

        private final Logger logger;
        private final Directory directory;
        private final IndexWriter indexWriter;

        /**
         * 通过一个要使用的目录 以及一个写入对象初始化
         * @param directory
         * @param indexWriter
         */
        MetadataIndexWriter(Directory directory, IndexWriter indexWriter) {
            this.directory = directory;
            this.indexWriter = indexWriter;
            this.logger = Loggers.getLogger(MetadataIndexWriter.class, directory.toString());
        }

        void deleteAll() throws IOException {
            this.logger.trace("clearing existing metadata");
            this.indexWriter.deleteAll();
        }

        void updateIndexMetadataDocument(Document indexMetadataDocument, Index index) throws IOException {
            this.logger.trace("updating metadata for [{}]", index);
            indexWriter.updateDocument(new Term(INDEX_UUID_FIELD_NAME, index.getUUID()), indexMetadataDocument);
        }

        void updateGlobalMetadata(Document globalMetadataDocument) throws IOException {
            this.logger.trace("updating global metadata doc");
            // 当某个doc命中了这个query将会更新数据
            indexWriter.updateDocument(new Term(TYPE_FIELD_NAME, GLOBAL_TYPE_NAME), globalMetadataDocument);
        }

        void deleteIndexMetadata(String indexUUID) throws IOException {
            this.logger.trace("removing metadata for [{}]", indexUUID);
            indexWriter.deleteDocuments(new Term(INDEX_UUID_FIELD_NAME, indexUUID));
        }

        void flush() throws IOException {
            this.logger.trace("flushing");
            this.indexWriter.flush();
        }

        /**
         * 将最新的描述信息填充到 userData中  并进入prepareCommit阶段
         * @param nodeId
         * @param currentTerm
         * @param lastAcceptedVersion
         * @throws IOException
         */
        void prepareCommit(String nodeId, long currentTerm, long lastAcceptedVersion) throws IOException {
            final Map<String, String> commitData = new HashMap<>(COMMIT_DATA_SIZE);
            commitData.put(CURRENT_TERM_KEY, Long.toString(currentTerm));
            commitData.put(LAST_ACCEPTED_VERSION_KEY, Long.toString(lastAcceptedVersion));
            commitData.put(NODE_VERSION_KEY, Integer.toString(Version.CURRENT.id));
            commitData.put(NODE_ID_KEY, nodeId);
            indexWriter.setLiveCommitData(commitData.entrySet());
            indexWriter.prepareCommit();
        }

        void commit() throws IOException {
            indexWriter.commit();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(indexWriter, directory);
        }
    }


    /**
     * 负责持久化CS
     */
    public static class Writer implements Closeable {

        /**
         * 每个对象对应一个 dataPath
         */
        private final List<MetadataIndexWriter> metadataIndexWriters;
        private final String nodeId;
        private final BigArrays bigArrays;
        private final LongSupplier relativeTimeMillisSupplier;
        private final Supplier<TimeValue> slowWriteLoggingThresholdSupplier;

        boolean fullStateWritten = false;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Writer(List<MetadataIndexWriter> metadataIndexWriters, String nodeId, BigArrays bigArrays,
                       LongSupplier relativeTimeMillisSupplier, Supplier<TimeValue> slowWriteLoggingThresholdSupplier) {
            this.metadataIndexWriters = metadataIndexWriters;
            this.nodeId = nodeId;
            this.bigArrays = bigArrays;
            this.relativeTimeMillisSupplier = relativeTimeMillisSupplier;
            this.slowWriteLoggingThresholdSupplier = slowWriteLoggingThresholdSupplier;
        }

        private void ensureOpen() {
            if (closed.get()) {
                throw new AlreadyClosedException("cluster state writer is closed already");
            }
        }

        public boolean isOpen() {
            return closed.get() == false;
        }

        private void closeIfAnyIndexWriterHasTragedyOrIsClosed() {
            if (metadataIndexWriters.stream().map(writer -> writer.indexWriter)
                .anyMatch(iw -> iw.getTragicException() != null || iw.isOpen() == false)) {
                try {
                    close();
                } catch (Exception e) {
                    logger.warn("failed on closing cluster state writer", e);
                }
            }
        }

        /**
         * Overrides and commits the given current term and cluster state
         * 将此时最新的CS持久化到磁盘中
         */
        public void writeFullStateAndCommit(long currentTerm, ClusterState clusterState) throws IOException {
            ensureOpen();
            try {
                final long startTimeMillis = relativeTimeMillisSupplier.getAsLong();
                // 先清理旧数据 之后写入新数据  stats可以理解为结果对象   这里完成了flush 但是没有完成commit
                final WriterStats stats = overwriteMetadata(clusterState.metadata());
                // flush 好像只是做了数据的持久化 但是lucene只能读取到commit后的数据 也就是还需要执行一步commit 具体记不清了 需要复习下
                commit(currentTerm, clusterState.version());
                fullStateWritten = true;

                // 以下打印日志 忽略
                final long durationMillis = relativeTimeMillisSupplier.getAsLong() - startTimeMillis;
                final TimeValue finalSlowWriteLoggingThreshold = slowWriteLoggingThresholdSupplier.get();
                if (durationMillis >= finalSlowWriteLoggingThreshold.getMillis()) {
                    logger.warn("writing cluster state took [{}ms] which is above the warn threshold of [{}]; " +
                            "wrote full state with [{}] indices",
                        durationMillis, finalSlowWriteLoggingThreshold, stats.numIndicesUpdated);
                } else {
                    logger.debug("writing cluster state took [{}ms]; " +
                            "wrote full state with [{}] indices",
                        durationMillis, stats.numIndicesUpdated);
                }
            } finally {
                closeIfAnyIndexWriterHasTragedyOrIsClosed();
            }
        }

        /**
         * Updates and commits the given cluster state update
         */
        void writeIncrementalStateAndCommit(long currentTerm, ClusterState previousClusterState,
                                            ClusterState clusterState) throws IOException {
            ensureOpen();
            assert fullStateWritten : "Need to write full state first before doing incremental writes";
            try {
                final long startTimeMillis = relativeTimeMillisSupplier.getAsLong();
                final WriterStats stats = updateMetadata(previousClusterState.metadata(), clusterState.metadata());
                commit(currentTerm, clusterState.version());
                final long durationMillis = relativeTimeMillisSupplier.getAsLong() - startTimeMillis;
                final TimeValue finalSlowWriteLoggingThreshold = slowWriteLoggingThresholdSupplier.get();
                if (durationMillis >= finalSlowWriteLoggingThreshold.getMillis()) {
                    logger.warn("writing cluster state took [{}ms] which is above the warn threshold of [{}]; " +
                            "wrote global metadata [{}] and metadata for [{}] indices and skipped [{}] unchanged indices",
                        durationMillis, finalSlowWriteLoggingThreshold, stats.globalMetaUpdated, stats.numIndicesUpdated,
                        stats.numIndicesUnchanged);
                } else {
                    logger.debug("writing cluster state took [{}ms]; " +
                            "wrote global metadata [{}] and metadata for [{}] indices and skipped [{}] unchanged indices",
                        durationMillis, stats.globalMetaUpdated, stats.numIndicesUpdated, stats.numIndicesUnchanged);
                }
            } finally {
                closeIfAnyIndexWriterHasTragedyOrIsClosed();
            }
        }

        /**
         * Update the persisted metadata to match the given cluster state by removing any stale or unnecessary documents and adding any
         * updated documents.
         */
        private WriterStats updateMetadata(Metadata previouslyWrittenMetadata, Metadata metadata) throws IOException {
            assert previouslyWrittenMetadata.coordinationMetadata().term() == metadata.coordinationMetadata().term();
            logger.trace("currentTerm [{}] matches previous currentTerm, writing changes only",
                metadata.coordinationMetadata().term());

            final boolean updateGlobalMeta = Metadata.isGlobalStateEquals(previouslyWrittenMetadata, metadata) == false;
            if (updateGlobalMeta) {
                try (ReleasableDocument globalMetadataDocument = makeGlobalMetadataDocument(metadata)) {
                    for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                        metadataIndexWriter.updateGlobalMetadata(globalMetadataDocument.getDocument());
                    }
                }
            }

            final Map<String, Long> indexMetadataVersionByUUID = new HashMap<>(previouslyWrittenMetadata.indices().size());
            for (ObjectCursor<IndexMetadata> cursor : previouslyWrittenMetadata.indices().values()) {
                final IndexMetadata indexMetadata = cursor.value;
                final Long previousValue = indexMetadataVersionByUUID.putIfAbsent(indexMetadata.getIndexUUID(), indexMetadata.getVersion());
                assert previousValue == null : indexMetadata.getIndexUUID() + " already mapped to " + previousValue;
            }

            int numIndicesUpdated = 0;
            int numIndicesUnchanged = 0;
            for (ObjectCursor<IndexMetadata> cursor : metadata.indices().values()) {
                final IndexMetadata indexMetadata = cursor.value;
                final Long previousVersion = indexMetadataVersionByUUID.get(indexMetadata.getIndexUUID());
                if (previousVersion == null || indexMetadata.getVersion() != previousVersion) {
                    logger.trace("updating metadata for [{}], changing version from [{}] to [{}]",
                        indexMetadata.getIndex(), previousVersion, indexMetadata.getVersion());
                    numIndicesUpdated++;
                    try (ReleasableDocument indexMetadataDocument = makeIndexMetadataDocument(indexMetadata)) {
                        for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                            metadataIndexWriter.updateIndexMetadataDocument(indexMetadataDocument.getDocument(), indexMetadata.getIndex());
                        }
                    }
                } else {
                    numIndicesUnchanged++;
                    logger.trace("no action required for [{}]", indexMetadata.getIndex());
                }
                indexMetadataVersionByUUID.remove(indexMetadata.getIndexUUID());
            }

            for (String removedIndexUUID : indexMetadataVersionByUUID.keySet()) {
                for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                    metadataIndexWriter.deleteIndexMetadata(removedIndexUUID);
                }
            }

            // Flush, to try and expose a failure (e.g. out of disk space) before committing, because we can handle a failure here more
            // gracefully than one that occurs during the commit process.
            for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                metadataIndexWriter.flush();
            }

            return new WriterStats(updateGlobalMeta, numIndicesUpdated, numIndicesUnchanged);
        }

        /**
         * Update the persisted metadata to match the given cluster state by removing all existing documents and then adding new documents.
         * 先清理之前的所有数据 之后插入新数据
         */
        private WriterStats overwriteMetadata(Metadata metadata) throws IOException {
            for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                metadataIndexWriter.deleteAll();
            }
            return addMetadata(metadata);
        }

        /**
         * Add documents for the metadata of the given cluster state, assuming that there are currently no documents.
         */
        private WriterStats addMetadata(Metadata metadata) throws IOException {
            // metadata对应的doc.type是 global
            try (ReleasableDocument globalMetadataDocument = makeGlobalMetadataDocument(metadata)) {
                // 在每个dataPath下 使用最新的metadata doc 去覆盖之前的数据
                for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                    metadataIndexWriter.updateGlobalMetadata(globalMetadataDocument.getDocument());
                }
            }

            // 每个indexMetadata对应的doc.type是 index   这里流程跟上面一样
            for (ObjectCursor<IndexMetadata> cursor : metadata.indices().values()) {
                final IndexMetadata indexMetadata = cursor.value;
                try (ReleasableDocument indexMetadataDocument = makeIndexMetadataDocument(indexMetadata)) {
                    for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                        // 将indexMetadata 转换成doc后写入到 dataPath._state目录下
                        metadataIndexWriter.updateIndexMetadataDocument(indexMetadataDocument.getDocument(), indexMetadata.getIndex());
                    }
                }
            }

            // Flush, to try and expose a failure (e.g. out of disk space) before committing, because we can handle a failure here more
            // gracefully than one that occurs during the commit process.
            // 将之前写入的数据刷盘
            for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                metadataIndexWriter.flush();
            }

            return new WriterStats(true, metadata.indices().size(), 0);
        }

        /**
         * 将之前写入的 metadata/indexMetadata 数据commit 这样就可以更新到Segment_N中
         * @param currentTerm
         * @param lastAcceptedVersion
         * @throws IOException
         */
        public void commit(long currentTerm, long lastAcceptedVersion) throws IOException {
            ensureOpen();
            try {
                for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                    metadataIndexWriter.prepareCommit(nodeId, currentTerm, lastAcceptedVersion);
                }
            } catch (Exception e) {
                try {
                    close();
                } catch (Exception e2) {
                    logger.warn("failed on closing cluster state writer", e2);
                    e.addSuppressed(e2);
                }
                throw e;
            } finally {
                closeIfAnyIndexWriterHasTragedyOrIsClosed();
            }
            try {
                // 将数据写入到segment_N中
                for (MetadataIndexWriter metadataIndexWriter : metadataIndexWriters) {
                    metadataIndexWriter.commit();
                }
            } catch (IOException e) {
                // The commit() call has similar semantics to a fsync(): although it's atomic, if it fails then we've no idea whether the
                // data on disk is now the old version or the new version, and this is a disaster. It's safest to fail the whole node and
                // retry from the beginning.
                try {
                    close();
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                }
                throw new IOError(e);
            } finally {
                closeIfAnyIndexWriterHasTragedyOrIsClosed();
            }
        }

        @Override
        public void close() throws IOException {
            logger.trace("closing PersistedClusterStateService.Writer");
            if (closed.compareAndSet(false, true)) {
                IOUtils.close(metadataIndexWriters);
            }
        }

        /**
         * 可以作为一次写入的结果
         */
        static class WriterStats {
            /**
             * 本次是否写入了新的 global数据
             */
            final boolean globalMetaUpdated;
            /**
             * 写入了多少index数据
             */
            final long numIndicesUpdated;
            final long numIndicesUnchanged;

            WriterStats(boolean globalMetaUpdated, long numIndicesUpdated, long numIndicesUnchanged) {
                this.globalMetaUpdated = globalMetaUpdated;
                this.numIndicesUpdated = numIndicesUpdated;
                this.numIndicesUnchanged = numIndicesUnchanged;
            }
        }

        /**
         * 将某个indexMetadata 转换成lucene.doc
         * @param indexMetadata
         * @return
         * @throws IOException
         */
        private ReleasableDocument makeIndexMetadataDocument(IndexMetadata indexMetadata) throws IOException {
            final ReleasableDocument indexMetadataDocument = makeDocument(INDEX_TYPE_NAME, indexMetadata);
            boolean success = false;
            try {
                final String indexUUID = indexMetadata.getIndexUUID();
                assert indexUUID.equals(IndexMetadata.INDEX_UUID_NA_VALUE) == false;
                // 这里还会追加一个 uuid的field
                indexMetadataDocument.getDocument().add(new StringField(INDEX_UUID_FIELD_NAME, indexUUID, Field.Store.NO));
                success = true;
                return indexMetadataDocument;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(indexMetadataDocument);
                }
            }
        }

        /**
         * 将metadata转换成 一个doc.type=global 的doc对象
         * @param metadata
         * @return
         * @throws IOException
         */
        private ReleasableDocument makeGlobalMetadataDocument(Metadata metadata) throws IOException {
            return makeDocument(GLOBAL_TYPE_NAME, metadata);
        }

        /**
         * 将数据转换成doc
         * @param typeName  doc.type对应的名称
         *
         * @param metadata
         * @return
         * @throws IOException
         */
        private ReleasableDocument makeDocument(String typeName, ToXContent metadata) throws IOException {
            final Document document = new Document();
            // 这里插入一个string类型的field
            document.add(new StringField(TYPE_FIELD_NAME, typeName, Field.Store.NO));

            boolean success = false;
            final ReleasableBytesStreamOutput releasableBytesStreamOutput = new ReleasableBytesStreamOutput(bigArrays);
            try {
                final FilterOutputStream outputStream = new FilterOutputStream(releasableBytesStreamOutput) {
                    @Override
                    public void close() {
                        // closing the XContentBuilder should not release the bytes yet
                    }
                };
                try (XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.SMILE, outputStream)) {
                    xContentBuilder.startObject();
                    metadata.toXContent(xContentBuilder, FORMAT_PARAMS);
                    xContentBuilder.endObject();
                }
                // 将数据设置到data中
                document.add(new StoredField(DATA_FIELD_NAME, releasableBytesStreamOutput.bytes().toBytesRef()));
                final ReleasableDocument releasableDocument = new ReleasableDocument(document, releasableBytesStreamOutput);
                success = true;
                return releasableDocument;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(releasableBytesStreamOutput);
                }
            }
        }
    }
}
