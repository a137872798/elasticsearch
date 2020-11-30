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
package org.elasticsearch.action.admin.indices.analyze;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalyzerComponents;
import org.elasticsearch.index.analysis.AnalyzerComponentsProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.NameOrDefinition;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Transport action used to execute analyze requests
 * 利用node上的分词器 解析req.text 并返回结果
 */
public class TransportAnalyzeAction extends TransportSingleShardAction<AnalyzeAction.Request, AnalyzeAction.Response> {

    private final Settings settings;
    private final IndicesService indicesService;

    @Inject
    public TransportAnalyzeAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                  TransportService transportService, IndicesService indicesService, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver) {
        super(AnalyzeAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
            AnalyzeAction.Request::new, ThreadPool.Names.ANALYZE);
        this.settings = settings;
        this.indicesService = indicesService;
    }

    @Override
    protected Writeable.Reader<AnalyzeAction.Response> getResponseReader() {
        return AnalyzeAction.Response::new;
    }

    @Override
    protected boolean resolveIndex(AnalyzeAction.Request request) {
        return request.index() != null;
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        if (request.concreteIndex() != null) {
            return super.checkRequestBlock(state, request);
        }
        return null;
    }

    /**
     * 通过req中的索引信息 获取分片迭代器
     * @param state
     * @param request
     * @return
     */
    @Override
    protected ShardsIterator shards(ClusterState state, InternalRequest request) {
        if (request.concreteIndex() == null) {
            // just execute locally....
            return null;
        }
        // 这里仅分析还活跃的分片
        return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
    }

    /**
     * 处理每个分片委托给该方法
     * @param request
     * @param shardId
     * @return
     * @throws IOException
     */
    @Override
    protected AnalyzeAction.Response shardOperation(AnalyzeAction.Request request, ShardId shardId) throws IOException {
        // 找到管理该分片的索引服务
        final IndexService indexService = getIndexService(shardId);
        // 如果不存在则使用全局配置
        final int maxTokenCount = indexService == null ?
            IndexSettings.MAX_TOKEN_COUNT_SETTING.get(settings) : indexService.getIndexSettings().getMaxTokenCount();

        // 这里只是解析了req.text 与shard还看不出任何的关系
        return analyze(request, indicesService.getAnalysis(), indexService, maxTokenCount);
    }

    /**
     *
     * @param request
     * @param analysisRegistry  该对象内部管理了所有的分词器
     * @param indexService
     * @param maxTokenCount  最大token数 应该是代表分析过程中会使用到多少token
     * @return
     * @throws IOException
     */
    public static AnalyzeAction.Response analyze(AnalyzeAction.Request request, AnalysisRegistry analysisRegistry,
                                          IndexService indexService, int maxTokenCount) throws IOException {

        IndexSettings settings = indexService == null ? null : indexService.getIndexSettings();

        // First, we check to see if the request requires a custom analyzer.  If so, then we
        // need to build it and then close it after use.
        // 根据请求和配置 组装一个自定义的分词器
        try (Analyzer analyzer = buildCustomAnalyzer(request, analysisRegistry, settings)) {
            if (analyzer != null) {
                return analyze(request, analyzer, maxTokenCount);
            }
        }

        // Otherwise we use a built-in analyzer, which should not be closed
        return analyze(request, getAnalyzer(request, analysisRegistry, indexService), maxTokenCount);
    }

    private IndexService getIndexService(ShardId shardId) {
        if (shardId != null) {
            return indicesService.indexServiceSafe(shardId.getIndex());
        }
        return null;
    }

    /**
     * 从 registry中获取合适的分词器对象
     * @param request
     * @param analysisRegistry
     * @param indexService
     * @return
     * @throws IOException
     */
    private static Analyzer getAnalyzer(AnalyzeAction.Request request, AnalysisRegistry analysisRegistry,
                                        IndexService indexService) throws IOException {

        // 如果req中有指定的分词器 获取实例后返回
        if (request.analyzer() != null) {
            if (indexService == null) {
                Analyzer analyzer = analysisRegistry.getAnalyzer(request.analyzer());
                if (analyzer == null) {
                    throw new IllegalArgumentException("failed to find global analyzer [" + request.analyzer() + "]");
                }
                return analyzer;
            } else {
                Analyzer analyzer = indexService.getIndexAnalyzers().get(request.analyzer());
                if (analyzer == null) {
                    throw new IllegalArgumentException("failed to find analyzer [" + request.analyzer() + "]");
                }
                return analyzer;
            }
        }
        if (request.normalizer() != null) {
            // Get normalizer from indexAnalyzers
            if (indexService == null) {
                throw new IllegalArgumentException("analysis based on a normalizer requires an index");
            }
            Analyzer analyzer = indexService.getIndexAnalyzers().getNormalizer(request.normalizer());
            if (analyzer == null) {
                throw new IllegalArgumentException("failed to find normalizer under [" + request.normalizer() + "]");
            }
            return analyzer;
        }
        if (request.field() != null) {
            if (indexService == null) {
                throw new IllegalArgumentException("analysis based on a specific field requires an index");
            }
            MappedFieldType fieldType = indexService.mapperService().fieldType(request.field());
            if (fieldType != null) {
                if (fieldType.tokenized() || fieldType instanceof KeywordFieldMapper.KeywordFieldType) {
                    return fieldType.indexAnalyzer();
                } else {
                    throw new IllegalArgumentException("Can't process field [" + request.field() +
                        "], Analysis requests are only supported on tokenized fields");
                }
            }
        }
        if (indexService == null) {
            return analysisRegistry.getAnalyzer("standard");
        } else {
            return indexService.getIndexAnalyzers().getDefaultIndexAnalyzer();
        }
    }

    /**
     * 组装一个分词器
     * @param request
     * @param analysisRegistry
     * @param indexSettings
     * @return
     * @throws IOException
     */
    private static Analyzer buildCustomAnalyzer(AnalyzeAction.Request request, AnalysisRegistry analysisRegistry,
                                                IndexSettings indexSettings) throws IOException {
        if (request.tokenizer() != null) {
            // 通过req中指定的各种组件 生成一个分词器对象
            return analysisRegistry.buildCustomAnalyzer(indexSettings, false,
                request.tokenizer(), request.charFilters(), request.tokenFilters());
            // 代表tokenizer为空 其他过滤器存在
        } else if (((request.tokenFilters() != null && request.tokenFilters().size() > 0)
            || (request.charFilters() != null && request.charFilters().size() > 0))) {
            // 如果tokenFilter存在 由于normalizer为true 所以filterFatory类型必须是 NormalizingTokenFilterFactory
            return analysisRegistry.buildCustomAnalyzer(indexSettings, true, new NameOrDefinition("keyword"),
                request.charFilters(), request.tokenFilters());
        }
        return null;
    }

    /**
     * 使用指定的分词器 对某个索引的数据进行解析
     * @param request
     * @param analyzer
     * @param maxTokenCount
     * @return
     */
    private static AnalyzeAction.Response analyze(AnalyzeAction.Request request, Analyzer analyzer, int maxTokenCount) {
        // 代表需要详细的分词???
        if (request.explain()) {
            return new AnalyzeAction.Response(null, detailAnalyze(request, analyzer, maxTokenCount));
        }
        // 只需要进行简单的分词
        return new AnalyzeAction.Response(simpleAnalyze(request, analyzer, maxTokenCount), null);
    }


    /**
     * 进行简单的分词
     * @param request
     * @param analyzer
     * @param maxTokenCount
     * @return
     */
    private static List<AnalyzeAction.AnalyzeToken> simpleAnalyze(AnalyzeAction.Request request,
                                                                  Analyzer analyzer, int maxTokenCount) {
        TokenCounter tc = new TokenCounter(maxTokenCount);
        List<AnalyzeAction.AnalyzeToken> tokens = new ArrayList<>();
        int lastPosition = -1;
        int lastOffset = 0;
        // Note that we always pass "" as the field to the various Analyzer methods, because
        // the analyzers we use here are all field-specific and so ignore this parameter
        for (String text : request.text()) {
            try (TokenStream stream = analyzer.tokenStream("", text)) {
                stream.reset();
                CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
                PositionIncrementAttribute posIncr = stream.addAttribute(PositionIncrementAttribute.class);
                OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);
                TypeAttribute type = stream.addAttribute(TypeAttribute.class);
                PositionLengthAttribute posLen = stream.addAttribute(PositionLengthAttribute.class);

                while (stream.incrementToken()) {
                    int increment = posIncr.getPositionIncrement();
                    if (increment > 0) {
                        lastPosition = lastPosition + increment;
                    }
                    tokens.add(new AnalyzeAction.AnalyzeToken(term.toString(), lastPosition, lastOffset + offset.startOffset(),
                        lastOffset + offset.endOffset(), posLen.getPositionLength(), type.type(), null));
                    tc.increment();
                }
                stream.end();
                lastOffset += offset.endOffset();
                lastPosition += posIncr.getPositionIncrement();

                lastPosition += analyzer.getPositionIncrementGap("");
                lastOffset += analyzer.getOffsetGap("");
            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze", e);
            }
        }
        return tokens;
    }

    /**
     * 进行详细的分析
     * @param request
     * @param analyzer  使用的分词器对象
     * @param maxTokenCount  最多解析多少词
     * @return
     */
    private static AnalyzeAction.DetailAnalyzeResponse detailAnalyze(AnalyzeAction.Request request, Analyzer analyzer,
                                                                     int maxTokenCount) {
        AnalyzeAction.DetailAnalyzeResponse detailResponse;
        final Set<String> includeAttributes = new HashSet<>();
        if (request.attributes() != null) {
            // 将req中的所有attr转移到了 set中
            for (String attribute : request.attributes()) {
                includeAttributes.add(attribute.toLowerCase(Locale.ROOT));
            }
        }

        // maybe unwrap analyzer from NamedAnalyzer
        // 如果是 NamedAnalyzer 进行解包装
        Analyzer potentialCustomAnalyzer = analyzer;
        if (analyzer instanceof NamedAnalyzer) {
            potentialCustomAnalyzer = ((NamedAnalyzer) analyzer).analyzer();
        }

        if (potentialCustomAnalyzer instanceof AnalyzerComponentsProvider) {
            AnalyzerComponentsProvider customAnalyzer = (AnalyzerComponentsProvider) potentialCustomAnalyzer;
            // note: this is not field-name dependent in our cases so we can leave out the argument
            // 默认额外的增量值是0
            int positionIncrementGap = potentialCustomAnalyzer.getPositionIncrementGap("");
            // 默认为1 代表每解析出的2个相邻的token 偏移量茶汁差值是多少
            int offsetGap = potentialCustomAnalyzer.getOffsetGap("");
            // 自定义传入的各种 TokenizerFactory[]  CharFilterFactory[]  TokenFilterFactory[]  会被包装成一个components对象
            AnalyzerComponents components = customAnalyzer.getComponents();
            // divide charfilter, tokenizer tokenfilters
            CharFilterFactory[] charFilterFactories = components.getCharFilters();
            TokenizerFactory tokenizerFactory = components.getTokenizerFactory();
            TokenFilterFactory[] tokenFilterFactories = components.getTokenFilters();

            // 第一维是filter的下标   第二维是req携带了几个文本
            String[][] charFiltersTexts = new String[charFilterFactories != null ? charFilterFactories.length : 0][request.text().length];
            TokenListCreator[] tokenFiltersTokenListCreator = new TokenListCreator[tokenFilterFactories != null ?
                tokenFilterFactories.length : 0];

            // 该对象负责存储分词的结果  在初始化时指定了分词的数量
            TokenListCreator tokenizerTokenListCreator = new TokenListCreator(maxTokenCount);

            // 这里在解析req上携带的文本
            for (int textIndex = 0; textIndex < request.text().length; textIndex++) {
                String charFilteredSource = request.text()[textIndex];

                Reader reader = new StringReader(charFilteredSource);
                if (charFilterFactories != null) {

                    // 按照过滤器下标进行遍历
                    for (int charFilterIndex = 0; charFilterIndex < charFilterFactories.length; charFilterIndex++) {
                        // 通过过滤器处理reader
                        reader = charFilterFactories[charFilterIndex].create(reader);
                        Reader readerForWriteOut = new StringReader(charFilteredSource);
                        // 这里通过过滤器又生成了一个Reader对象
                        readerForWriteOut = charFilterFactories[charFilterIndex].create(readerForWriteOut);
                        // 将stream转换成了string  但是此时应该已经被处理过了
                        charFilteredSource = writeCharStream(readerForWriteOut);
                        // 存储被处理后的结果
                        charFiltersTexts[charFilterIndex][textIndex] = charFilteredSource;
                    }
                }

                // analyzing only tokenizer
                // 获取分词器对象并设置数据流
                Tokenizer tokenizer = tokenizerFactory.create();
                tokenizer.setReader(reader);
                // 开始解析数据
                tokenizerTokenListCreator.analyze(tokenizer, includeAttributes, positionIncrementGap, offsetGap);

                // analyzing each tokenfilter
                // 使用每个tokenFilter 单独解析tokenStream 并存储结果
                if (tokenFilterFactories != null) {
                    for (int tokenFilterIndex = 0; tokenFilterIndex < tokenFilterFactories.length; tokenFilterIndex++) {
                        if (tokenFiltersTokenListCreator[tokenFilterIndex] == null) {
                            tokenFiltersTokenListCreator[tokenFilterIndex] = new TokenListCreator(maxTokenCount);
                        }
                        // 将tokenStream层层包装
                        TokenStream stream = createStackedTokenStream(request.text()[textIndex],
                            charFilterFactories, tokenizerFactory, tokenFilterFactories, tokenFilterIndex + 1);
                        tokenFiltersTokenListCreator[tokenFilterIndex].analyze(stream, includeAttributes, positionIncrementGap, offsetGap);
                    }
                }
            }

            // charFiltersTexts.length 对应charFilter长度
            AnalyzeAction.CharFilteredText[] charFilteredLists =
                new AnalyzeAction.CharFilteredText[charFiltersTexts.length];

            if (charFilterFactories != null) {
                for (int charFilterIndex = 0; charFilterIndex < charFiltersTexts.length; charFilterIndex++) {
                    // 将该filter处理过的text 和对应的工厂name 包装成 CharFilteredText
                    charFilteredLists[charFilterIndex] = new AnalyzeAction.CharFilteredText(
                        charFilterFactories[charFilterIndex].name(), charFiltersTexts[charFilterIndex]);
                }
            }
            AnalyzeAction.AnalyzeTokenList[] tokenFilterLists =
                new AnalyzeAction.AnalyzeTokenList[tokenFiltersTokenListCreator.length];

            // 如果存在 tokenFilter 就代表会对解析出来的token进行处理 需要一个对象存储处理后的结果
            if (tokenFilterFactories != null) {
                for (int tokenFilterIndex = 0; tokenFilterIndex < tokenFiltersTokenListCreator.length; tokenFilterIndex++) {
                    // 将以每个tokenFilter解析后的结果单独包装成TokenList对象 并存储到数组中
                    tokenFilterLists[tokenFilterIndex] = new AnalyzeAction.AnalyzeTokenList(
                        tokenFilterFactories[tokenFilterIndex].name(), tokenFiltersTokenListCreator[tokenFilterIndex].getArrayTokens());
                }
            }
            // 将按照分词器解析后的结果 tokenFilter单独处理后的结果 charFilter单独处理后的结果包装成一个对象
            detailResponse = new AnalyzeAction.DetailAnalyzeResponse(charFilteredLists,
                new AnalyzeAction.AnalyzeTokenList(tokenizerFactory.name(), tokenizerTokenListCreator.getArrayTokens()),
                tokenFilterLists);
        } else {
            String name;
            if (analyzer instanceof NamedAnalyzer) {
                name = ((NamedAnalyzer) analyzer).name();
            } else {
                name = analyzer.getClass().getName();
            }

            // 这种情况比较简单  代表分词器不是多个组件组装起来的
            TokenListCreator tokenListCreator = new TokenListCreator(maxTokenCount);
            // 解析请求体 中的每个text对象 并将结果包装成res
            for (String text : request.text()) {
                tokenListCreator.analyze(analyzer.tokenStream("", text), includeAttributes, analyzer.getPositionIncrementGap(""),
                        analyzer.getOffsetGap(""));
            }
            detailResponse
                = new AnalyzeAction.DetailAnalyzeResponse(new AnalyzeAction.AnalyzeTokenList(name, tokenListCreator.getArrayTokens()));
        }
        return detailResponse;
    }

    /**
     * 使用相关参数包装 tokenStream
     * @param source
     * @param charFilterFactories
     * @param tokenizerFactory
     * @param tokenFilterFactories
     * @param current
     * @return
     */
    private static TokenStream createStackedTokenStream(String source, CharFilterFactory[] charFilterFactories,
                                                        TokenizerFactory tokenizerFactory, TokenFilterFactory[] tokenFilterFactories,
                                                        int current) {
        Reader reader = new StringReader(source);
        for (CharFilterFactory charFilterFactory : charFilterFactories) {
            reader = charFilterFactory.create(reader);
        }
        Tokenizer tokenizer = tokenizerFactory.create();
        tokenizer.setReader(reader);
        TokenStream tokenStream = tokenizer;
        for (int i = 0; i < current; i++) {
            tokenStream = tokenFilterFactories[i].create(tokenStream);
        }
        return tokenStream;
    }

    private static String writeCharStream(Reader input) {
        final int BUFFER_SIZE = 1024;
        char[] buf = new char[BUFFER_SIZE];
        int len;
        StringBuilder sb = new StringBuilder();
        do {
            try {
                len = input.read(buf, 0, BUFFER_SIZE);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze (charFiltering)", e);
            }
            if (len > 0) {
                sb.append(buf, 0, len);
            }
        } while (len == BUFFER_SIZE);
        return sb.toString();
    }

    /**
     * token 计数器
     */
    private static class TokenCounter{
        private int tokenCount = 0;
        private int maxTokenCount;

        /**
         * 最多只允许解析出多少token
         * @param maxTokenCount
         */
        private TokenCounter(int maxTokenCount){
            this.maxTokenCount = maxTokenCount;
        }

        /**
         * 每当解析出一个新的token时 增加计数值
         */
        private void increment(){
            tokenCount++;
            if (tokenCount > maxTokenCount) {
                throw new IllegalStateException(
                    "The number of tokens produced by calling _analyze has exceeded the allowed maximum of [" + maxTokenCount + "]."
                        + " This limit can be set by changing the [index.analyze.max_token_count] index level setting.");
            }
        }
    }

    private static class TokenListCreator {

        /**
         * 用于记录最后读取到的token的位置
         */
        int lastPosition = -1;
        int lastOffset = 0;
        /**
         * 对应解析出来的一组token
         */
        List<AnalyzeAction.AnalyzeToken> tokens;
        private TokenCounter tc;

        /**
         * @param maxTokenCount  通过允许解析的最大token数来初始化
         */
        TokenListCreator(int maxTokenCount) {
            tokens = new ArrayList<>();
            tc = new TokenCounter(maxTokenCount);
        }

        /**
         * 开始解析数据流
         * @param stream
         * @param includeAttributes
         * @param positionIncrementGap
         * @param offsetGap
         */
        private void analyze(TokenStream stream, Set<String> includeAttributes, int positionIncrementGap, int offsetGap) {
            try {
                // 将相关计数值都清零
                stream.reset();
                // 这几个 attr对象负责当解析出token后记录相关信息
                CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
                PositionIncrementAttribute posIncr = stream.addAttribute(PositionIncrementAttribute.class);
                OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);
                TypeAttribute type = stream.addAttribute(TypeAttribute.class);
                PositionLengthAttribute posLen = stream.addAttribute(PositionLengthAttribute.class);

                while (stream.incrementToken()) {
                    int increment = posIncr.getPositionIncrement();
                    if (increment > 0) {
                        lastPosition = lastPosition + increment;
                    }
                    // 将相关信息包装成token对象 比加入到列表中
                    tokens.add(new AnalyzeAction.AnalyzeToken(term.toString(), lastPosition, lastOffset + offset.startOffset(),
                        lastOffset + offset.endOffset(), posLen.getPositionLength(), type.type(),
                        extractExtendedAttributes(stream, includeAttributes)));
                    // 增加计数值
                    tc.increment();
                }
                // 内部的attr会做一些结尾工作
                stream.end();
                lastOffset += offset.endOffset();
                lastPosition += posIncr.getPositionIncrement();

                lastPosition += positionIncrementGap;
                lastOffset += offsetGap;

            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze", e);
            } finally {
                IOUtils.closeWhileHandlingException(stream);
            }
        }

        private AnalyzeAction.AnalyzeToken[] getArrayTokens() {
            return tokens.toArray(new AnalyzeAction.AnalyzeToken[0]);
        }

    }

    /**
     * other attribute extract object.
     * Extracted object group by AttributeClassName
     *
     * @param stream current TokenStream
     * @param includeAttributes filtering attributes
     * @return Map&lt;key value&gt;
     */
    private static Map<String, Object> extractExtendedAttributes(TokenStream stream, final Set<String> includeAttributes) {
        final Map<String, Object> extendedAttributes = new TreeMap<>();

        stream.reflectWith((attClass, key, value) -> {
            if (CharTermAttribute.class.isAssignableFrom(attClass)) {
                return;
            }
            if (PositionIncrementAttribute.class.isAssignableFrom(attClass)) {
                return;
            }
            if (OffsetAttribute.class.isAssignableFrom(attClass)) {
                return;
            }
            if (TypeAttribute.class.isAssignableFrom(attClass)) {
                return;
            }
            if (includeAttributes == null || includeAttributes.isEmpty() || includeAttributes.contains(key.toLowerCase(Locale.ROOT))) {
                if (value instanceof BytesRef) {
                    final BytesRef p = (BytesRef) value;
                    value = p.toString();
                }
                extendedAttributes.put(key, value);
            }
        });

        return extendedAttributes;
    }

}
