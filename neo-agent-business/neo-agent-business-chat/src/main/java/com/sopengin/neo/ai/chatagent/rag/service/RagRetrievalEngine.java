package com.sopengin.neo.ai.chatagent.rag.service;

import lombok.extern.slf4j.Slf4j;
import com.sopengin.neo.ai.chatagent.model.ChannelExecutionView;
import com.sopengin.neo.ai.chatagent.model.RetrievalResultView;
import com.sopengin.neo.ai.chatagent.model.SearchReference;
import com.sopengin.neo.ai.chatagent.rag.config.ChatRagProperties;
import com.sopengin.neo.ai.chatagent.rag.model.ConversationExecutionPlan;
import com.sopengin.neo.ai.chatagent.rag.model.RagRetrievalContext;
import com.sopengin.neo.ai.chatagent.rag.model.SubQuestionChannelTrace;
import com.sopengin.neo.ai.chatagent.rag.model.SubQuestionEvidence;
import com.sopengin.neo.ai.chatagent.rag.retrieve.channel.RetrievalChannel;
import com.sopengin.neo.ai.chatagent.rag.retrieve.channel.RetrievalChannelResult;
import com.sopengin.neo.ai.chatagent.rag.support.SearchReferenceMapper;
import com.sopengin.neo.ai.chatagent.service.ConversationTraceRecorder;
import com.sopengin.neo.ai.manage.service.DocumentKnowledgeService;
import com.sopengin.neo.ai.manage.support.DocumentKnowledgeMetadataKeys;
import com.sopengin.neo.enums.RetrievalChannelEnum;
import org.springframework.ai.document.Document;
import org.springframework.ai.rag.Query;
import org.springframework.ai.rag.postretrieval.document.DocumentPostProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 服务层
 **/

@Slf4j
@Service
public class RagRetrievalEngine {

    /** RRF 公式中的平滑常数 k，标准取值 60，用于削弱排名第一的绝对优势。 */
    private static final int RRF_K = 60;

    private final List<RetrievalChannel> retrievalChannels;
    private final ChatRagProperties properties;
    private final DocumentPostProcessor rerankPostProcessor;
    private final DocumentKnowledgeService documentKnowledgeService;
    private final ExecutorService executorService;

    public RagRetrievalEngine(List<RetrievalChannel> retrievalChannels,
                              ChatRagProperties properties,
                              HttpDocumentRerankPostProcessor rerankPostProcessor,
                              DocumentKnowledgeService documentKnowledgeService,
                              @Qualifier("chatRagExecutorService") ExecutorService executorService) {
        this.retrievalChannels = retrievalChannels;
        this.properties = properties;
        this.rerankPostProcessor = rerankPostProcessor;
        this.documentKnowledgeService = documentKnowledgeService;
        this.executorService = executorService;
    }

    /**
     * 检索入口：对每个子问题并行执行完整检索流水线，最后跨子问题统一分配引用编号。
     */
    public RagRetrievalContext retrieve(ConversationExecutionPlan plan, ConversationTraceRecorder traceRecorder) {
        RagRetrievalContext context = new RagRetrievalContext();
        context.setRetrievalQuestion(plan.getRetrievalQuestion());
        // 多个子问题会并发写入，容器必须线程安全
        context.setUsedChannels(Collections.synchronizedList(new ArrayList<>()));
        context.setRetrievalNotes(Collections.synchronizedList(new ArrayList<>()));
        // 没拆出子问题时，退化为"用改写后的整句问题检索一次"
        List<String> subQuestions = plan.getRetrievalSubQuestions() == null || plan.getRetrievalSubQuestions().isEmpty()
            ? List.of(plan.getRetrievalQuestion())
            : plan.getRetrievalSubQuestions();

        // 【并行】多个子问题同时检索，互不阻塞
        List<CompletableFuture<SubQuestionEvidence>> futures = new ArrayList<>();
        for (int index = 0; index < subQuestions.size(); index++) {
            final int subQuestionIndex = index + 1;
            final String subQuestion = subQuestions.get(index);

            futures.add(CompletableFuture.supplyAsync(
                    () -> retrieveSingleSubQuestion(subQuestionIndex, subQuestion, plan, context.getUsedChannels(), context.getRetrievalNotes(), traceRecorder),
                    executorService
                )
                // 【降级】单个子问题超时（默认 12s）或失败，只记 note，不影响其他子问题
                .orTimeout(Math.max(properties.getSubQuestionTimeoutMs(), 1L), TimeUnit.MILLISECONDS)
                .exceptionally(throwable -> {

                    Throwable rootCause = unwrapThrowable(throwable);
                    log.warn("子问题检索失败: subQuestionIndex={}, subQuestion='{}', exceptionType={}, message={}",
                        subQuestionIndex,
                        subQuestion,
                        rootCause == null ? "" : rootCause.getClass().getName(),
                        rootCause == null ? "" : rootCause.getMessage(),
                        throwable);
                    context.getRetrievalNotes().add("子问题" + subQuestionIndex + "检索失败或超时，已自动忽略。");
                    return new SubQuestionEvidence(subQuestionIndex, subQuestion, List.of(), new ArrayList<>(), List.of(), 0, 0, 0);
                }));
        }

        List<SubQuestionEvidence> evidenceList = futures.stream()
            .map(CompletableFuture::join)
            .toList();
        int acceptedCount = (int) evidenceList.stream().filter(item -> item.getDocuments() != null && !item.getDocuments().isEmpty()).count();
        log.info("RAG 检索完成: retrievalQuestion='{}', originalSubQuestionCount={}, acceptedSubQuestionCount={}, notes={}",
            plan.getRetrievalQuestion(),
            evidenceList.size(),
            acceptedCount,
            context.getRetrievalNotes());
        assignReferenceIds(evidenceList);
        context.setSubQuestionEvidenceList(evidenceList);
        return context;
    }

    private SubQuestionEvidence retrieveSingleSubQuestion(int subQuestionIndex,
                                                          String subQuestion,
                                                          ConversationExecutionPlan plan,
                                                          List<String> usedChannels,
                                                          List<String> notes,
                                                          ConversationTraceRecorder traceRecorder) {

        // 【双通道并行】向量通道与关键词通道同时出发、互不阻塞；
        // 任一通道失败或超时只降级为空结果，不影响另一路召回
        List<CompletableFuture<RetrievalChannelResult>> futures = retrievalChannels.stream()

            .filter(channel -> channel.supports(plan))
            .map(channel -> CompletableFuture.supplyAsync(() -> channel.retrieve(subQuestion, plan), executorService)
                // 单通道超时（默认 5s）即放弃该路召回
                .orTimeout(Math.max(properties.getChannelTimeoutMs(), 1L), TimeUnit.MILLISECONDS)
                .exceptionally(throwable -> {

                    Throwable rootCause = unwrapThrowable(throwable);
                    log.warn("检索通道失败: subQuestionIndex={}, subQuestion='{}', channel='{}', exceptionType={}, message={}",
                        subQuestionIndex,
                        subQuestion,
                        channel.channelName(),
                        rootCause == null ? "" : rootCause.getClass().getName(),
                        rootCause == null ? "" : rootCause.getMessage(),
                        throwable);
                    notes.add("子问题" + subQuestionIndex + "通道[" + channel.channelName() + "]检索失败或超时，已自动降级。");
                    return new RetrievalChannelResult(channel.channelName(), List.of());
                }))
            .toList();
        if (futures.isEmpty()) {
            notes.add("子问题" + subQuestionIndex + "没有可用的检索通道。");
            return new SubQuestionEvidence(subQuestionIndex, subQuestion, List.of(), new ArrayList<>(), List.of(), 0, 0, 0);
        }

        List<RetrievalChannelResult> rawChannelResults = futures.stream()
            .map(CompletableFuture::join)
            .filter(result -> result.getDocuments() != null)
            .toList();
        // ② 证据闸门：按各通道自己的分数特性过滤弱命中（两路策略不同，见 applyEvidenceGate）
        List<RetrievalChannelResult> channelResults = rawChannelResults.stream()
            .map(this::applyEvidenceGate)
            .toList();
        List<SubQuestionChannelTrace> channelTraces = buildChannelTraces(rawChannelResults, channelResults);

        channelResults.stream()
            .filter(result -> !result.getDocuments().isEmpty())
            .forEach(result -> markUsedChannel(usedChannels, result.getChannelName()));

        // ③ RRF 融合：只用排名、不用原始分，规避"两路分数量纲不同不能直接相加"
        List<Document> mergedCandidates = fuseByRrf(channelResults);
        // ④ Parent-Child 聚合：命中的 Child 小块提升为 Parent 大块，保证回答阶段上下文完整
        List<Document> parentCandidates = documentKnowledgeService.elevateToParentBlocks(
            mergedCandidates,
            properties.getParentEvidenceMaxChars()
        );
        // ⑤ 可选外部 Rerank 精排：在较干净的候选集上继续优化排序
        List<Document> rerankedCandidates = applyRerank(subQuestion, parentCandidates, usedChannels);

        // ⑥ 定稿：只保留 finalTopK 条证据注入 Prompt
        List<Document> finalDocuments = rerankedCandidates.stream()
            .limit(properties.getFinalTopK())
            .toList();

        notes.add("子问题" + subQuestionIndex + "检索完成："
            + summarizeChannelResults(channelResults)
            + "，final=" + finalDocuments.size());

        if (traceRecorder != null) {
            try {
                recordChannelObservations(traceRecorder, subQuestionIndex, subQuestion,
                    rawChannelResults, channelResults, channelTraces);
                recordRetrievalResultObservations(traceRecorder, subQuestionIndex, subQuestion,
                    rawChannelResults, channelResults, mergedCandidates, rerankedCandidates, finalDocuments);
            } catch (RuntimeException exception) {
                log.warn("记录检索观测数据失败, subQuestionIndex={}", subQuestionIndex, exception);
            }
        }

        return new SubQuestionEvidence(
            subQuestionIndex,
            subQuestion,
            finalDocuments,
            new ArrayList<>(),
            channelTraces,
            mergedCandidates.size(),
            parentCandidates.size(),
            rerankedCandidates.size()
        );
    }

    /**
     * 证据闸门：按通道分派不同的过滤策略。
     *
     * <p>关键设计：两个通道产出的分数根本不是同一种东西，不能用同一套阈值 —— 见下面两个方法。</p>
     */
    private RetrievalChannelResult applyEvidenceGate(RetrievalChannelResult result) {
        if (result == null || result.getDocuments() == null || result.getDocuments().isEmpty()) {
            return result;
        }

        List<Document> documents = switch (result.getChannelName()) {
            case "vector" -> filterVectorCandidates(result.getDocuments());
            case "keyword" -> filterKeywordCandidates(result.getDocuments());
            default -> result.getDocuments();
        };
        return new RetrievalChannelResult(result.getChannelName(), documents);
    }

    /**
     * 向量通道：绝对阈值。
     *
     * <p>余弦相似度天然落在 0~1 且量纲稳定，用绝对阈值（默认 0.45）是有意义的。</p>
     */
    private List<Document> filterVectorCandidates(List<Document> documents) {
        return documents.stream()

            .filter(document -> {
                Double score = resolveScore(document);
                return score != null && score >= properties.getMinVectorSimilarity();
            })
            .toList();
    }

    /**
     * 关键词通道：相对阈值。
     *
     * <p>BM25 / 词法分数在不同问题之间量纲不稳定（长文档 vs 短文档、生僻词 vs 常见词都不可比），
     * 用绝对阈值会时松时紧；因此只相对"本次召回的最高分"裁剪，
     * 低于 topScore 一定比例（默认 0.35）的弱命中直接丢弃。</p>
     */
    private List<Document> filterKeywordCandidates(List<Document> documents) {
        Double topScore = documents.stream()
            .map(this::resolveScore)
            .filter(Objects::nonNull)
            .max(Double::compareTo)
            .orElse(null);
        if (topScore == null || topScore <= 0D) {
            return documents;
        }

        double acceptedFloor = topScore * Math.max(0D, properties.getKeywordRelativeScoreFloor());
        return documents.stream()
            .filter(document -> {
                Double score = resolveScore(document);
                return score != null && score >= acceptedFloor;
            })
            .toList();
    }

    /**
     * RRF（Reciprocal Rank Fusion）融合。
     *
     * <p>只使用"排名"而不使用"原始分数"，因此天然免疫两路分数量纲不同的问题；
     * 同一文档被多路命中则分数累加，相当于获得"多通道共识加成"。</p>
     */
    private List<Document> fuseByRrf(List<RetrievalChannelResult> channelResults) {
        Map<String, CandidateHolder> holders = new LinkedHashMap<>();

        for (RetrievalChannelResult retrievalChannelResult : channelResults) {
            accumulateRrf(retrievalChannelResult, holders);
        }

        return holders.values().stream()
            .sorted((left, right) -> Double.compare(right.score, left.score))
            .limit(properties.getCandidateTopK())
            .map(holder -> {

                holder.document.getMetadata().put(DocumentKnowledgeMetadataKeys.SCORE, holder.score);
                holder.document.getMetadata().put(DocumentKnowledgeMetadataKeys.RRF_SCORE, holder.score);
                // 标记证据来源通道：多路命中标为 hybrid，观测面板据此展示
                holder.document.getMetadata().put(DocumentKnowledgeMetadataKeys.CHANNEL,
                    holder.channels.size() > 1 ? "hybrid" : holder.channels.iterator().next());
                return holder.document;
            })
            .toList();
    }

    /**
     * 累加单路通道对每个候选的 RRF 贡献。
     */
    private void accumulateRrf(RetrievalChannelResult channelResult, Map<String, CandidateHolder> holders) {
        List<Document> documents = channelResult.getDocuments();
        for (int rank = 0; rank < documents.size(); rank++) {
            Document document = documents.get(rank);
            String documentId = document.getId();

            // RRF 公式：score += 1 / (k + rank + 1)，k 取 60 —— 排名越靠前贡献越大，但被 k 平滑
            double rrfScore = 1D / (RRF_K + rank + 1);
            CandidateHolder holder = holders.computeIfAbsent(documentId, ignored -> new CandidateHolder(document));
            holder.score += rrfScore;
            holder.channels.add(channelResult.getChannelName());
        }
    }

    private List<Document> applyRerank(String subQuestion,
                                       List<Document> candidates,
                                       List<String> usedChannels) {
        if (!properties.isRerankEnabled() || candidates.isEmpty()) {
            return candidates;
        }

        markUsedChannel(usedChannels, RetrievalChannelEnum.RERANK.getName());
        return rerankPostProcessor.process(new Query(subQuestion), candidates);
    }

    private void assignReferenceIds(List<SubQuestionEvidence> evidenceList) {
        final int[] referenceNumber = {1};
        Map<String, String> assignedIds = new LinkedHashMap<>();
        for (SubQuestionEvidence evidence : evidenceList) {
            List<SearchReference> references = new ArrayList<>();
            for (Document document : evidence.getDocuments()) {

                SearchReference reference = SearchReferenceMapper.fromDocument(
                    document,
                    evidence.getSubQuestionIndex(),
                    evidence.getSubQuestion(),
                    0
                );
                String uniqueKey = reference.uniqueKey();

                String assignedId = assignedIds.computeIfAbsent(uniqueKey, ignored -> String.valueOf(referenceNumber[0]++));
                reference.setReferenceId(assignedId);
                references.add(reference);
            }
            evidence.setReferences(references);
        }
    }

    private Double resolveScore(Document document) {
        if (document == null) {
            return null;
        }

        Object metadataScore = document.getMetadata().get(DocumentKnowledgeMetadataKeys.SCORE);
        if (metadataScore instanceof Number number) {
            return number.doubleValue();
        }
        return document.getScore();
    }

    private void markUsedChannel(List<String> usedChannels, String channel) {

        if (!usedChannels.contains(channel)) {
            usedChannels.add(channel);
        }
    }

    private String summarizeChannelResults(List<RetrievalChannelResult> channelResults) {
        if (channelResults.isEmpty()) {
            return "没有启用任何检索通道";
        }
        return channelResults.stream()
            .map(result -> result.getChannelName() + "=" + result.getDocuments().size())
            .reduce((left, right) -> left + "，" + right)
            .orElse("没有检索结果");
    }

    private List<SubQuestionChannelTrace> buildChannelTraces(List<RetrievalChannelResult> rawResults,
                                                             List<RetrievalChannelResult> filteredResults) {
        if ((rawResults == null || rawResults.isEmpty()) && (filteredResults == null || filteredResults.isEmpty())) {
            return List.of();
        }
        Map<String, Integer> rawMap = new LinkedHashMap<>();
        Map<String, Integer> filteredMap = new LinkedHashMap<>();
        if (rawResults != null) {
            rawResults.forEach(result -> rawMap.put(result.getChannelName(), result.getDocuments() == null ? 0 : result.getDocuments().size()));
        }
        if (filteredResults != null) {
            filteredResults.forEach(result -> filteredMap.put(result.getChannelName(), result.getDocuments() == null ? 0 : result.getDocuments().size()));
        }
        LinkedHashSet<String> channelNames = new LinkedHashSet<>();
        channelNames.addAll(rawMap.keySet());
        channelNames.addAll(filteredMap.keySet());
        List<SubQuestionChannelTrace> traces = new ArrayList<>(channelNames.size());
        for (String channelName : channelNames) {
            traces.add(new SubQuestionChannelTrace(
                channelName,
                rawMap.getOrDefault(channelName, 0),
                filteredMap.getOrDefault(channelName, 0)
            ));
        }
        return traces;
    }

    private void recordChannelObservations(ConversationTraceRecorder traceRecorder,
                                           int subQuestionIndex,
                                           String subQuestion,
                                           List<RetrievalChannelResult> rawResults,
                                           List<RetrievalChannelResult> filteredResults,
                                           List<SubQuestionChannelTrace> channelTraces) {
        if (rawResults == null || rawResults.isEmpty()) {
            return;
        }

        List<ChannelExecutionView> executions = new ArrayList<>();
        for (RetrievalChannelResult rawResult : rawResults) {
            String channelName = rawResult.getChannelName();
            int recalledCount = rawResult.getDocuments() == null ? 0 : rawResult.getDocuments().size();

            RetrievalChannelResult filteredResult = filteredResults == null ? null :
                filteredResults.stream().filter(r -> channelName.equals(r.getChannelName())).findFirst().orElse(null);
            int acceptedCount = filteredResult == null || filteredResult.getDocuments() == null ? 0 : filteredResult.getDocuments().size();

            SubQuestionChannelTrace trace = channelTraces == null ? null :
                channelTraces.stream().filter(t -> channelName.equals(t.getChannelName())).findFirst().orElse(null);
            int finalSelectedCount = trace == null ? 0 : trace.getAcceptedCount();

            ChannelExecutionView execution = new ChannelExecutionView();
            execution.setId(traceRecorder.exchangeId());
            execution.setTraceId(traceRecorder.traceId());
            execution.setSubQuestionIndex(subQuestionIndex);
            execution.setSubQuestion(subQuestion);
            execution.setChannelType(channelName);
            execution.setExecutionState(1);
            execution.setRecalledCount(recalledCount);
            execution.setAcceptedCount(acceptedCount);
            execution.setFinalSelectedCount(finalSelectedCount);

            if (rawResult.getDocuments() != null && !rawResult.getDocuments().isEmpty()) {
                List<Double> scores = rawResult.getDocuments().stream()
                    .map(doc -> {
                        Object scoreObj = doc.getMetadata().get(DocumentKnowledgeMetadataKeys.SCORE);
                        if (scoreObj instanceof Number) {
                            return ((Number) scoreObj).doubleValue();
                        }
                        return 0.0;
                    })
                    .filter(score -> score > 0)
                    .toList();

                if (!scores.isEmpty()) {
                    execution.setAvgScore(BigDecimal.valueOf(scores.stream().mapToDouble(Double::doubleValue).average().orElse(0)));
                    execution.setMaxScore(BigDecimal.valueOf(scores.stream().mapToDouble(Double::doubleValue).max().orElse(0)));
                    execution.setMinScore(BigDecimal.valueOf(scores.stream().mapToDouble(Double::doubleValue).min().orElse(0)));
                }
            }

            executions.add(execution);
        }

        traceRecorder.recordChannelExecutions(executions);
    }

    private void recordRetrievalResultObservations(ConversationTraceRecorder traceRecorder,
                                                   int subQuestionIndex,
                                                   String subQuestion,
                                                   List<RetrievalChannelResult> rawResults,
                                                   List<RetrievalChannelResult> filteredResults,
                                                   List<Document> mergedCandidates,
                                                   List<Document> rerankedCandidates,
                                                   List<Document> finalDocuments) {
        List<RetrievalResultView> results = new ArrayList<>();
        Map<String, Integer> finalRankMap = new LinkedHashMap<>();
        if (finalDocuments != null) {
            for (int i = 0; i < finalDocuments.size(); i++) {
                String docId = finalDocuments.get(i).getId();
                if (docId != null) {
                    finalRankMap.put(docId, i + 1);
                }
            }
        }

        if (rawResults != null) {
            for (RetrievalChannelResult rawResult : rawResults) {
                String channelName = rawResult.getChannelName();
                List<Document> rawDocs = rawResult.getDocuments();
                if (rawDocs == null || rawDocs.isEmpty()) {
                    continue;
                }

                for (int i = 0; i < rawDocs.size(); i++) {
                    Document doc = rawDocs.get(i);
                    RetrievalResultView view = new RetrievalResultView();
                    view.setId(traceRecorder.exchangeId());
                    view.setTraceId(traceRecorder.traceId());
                    view.setSubQuestionIndex(subQuestionIndex);
                    view.setSubQuestion(subQuestion);
                    view.setChannelType(channelName);
                    view.setChannelRank(i + 1);

                    Object scoreObj = doc.getMetadata().get(DocumentKnowledgeMetadataKeys.SCORE);
                    if (scoreObj instanceof Number) {
                        view.setOriginalScore(BigDecimal.valueOf(((Number) scoreObj).doubleValue()));
                    }

                    Object rrfScoreObj = doc.getMetadata().get(DocumentKnowledgeMetadataKeys.RRF_SCORE);
                    if (rrfScoreObj instanceof Number) {
                        view.setRrfScore(BigDecimal.valueOf(((Number) rrfScoreObj).doubleValue()));
                    }

                    Object rerankScoreObj = doc.getMetadata().get("rerankScore");
                    if (rerankScoreObj instanceof Number) {
                        view.setRerankScore(BigDecimal.valueOf(((Number) rerankScoreObj).doubleValue()));
                    }

                    Object docIdObj = doc.getMetadata().get(DocumentKnowledgeMetadataKeys.DOCUMENT_ID);
                    if (docIdObj != null) {
                        view.setDocumentId(Long.parseLong(String.valueOf(docIdObj)));
                    }

                    Object docNameObj = doc.getMetadata().get(DocumentKnowledgeMetadataKeys.DOCUMENT_NAME);
                    if (docNameObj != null) {
                        view.setDocumentName(String.valueOf(docNameObj));
                    }

                    Object chunkIdObj = doc.getMetadata().get(DocumentKnowledgeMetadataKeys.CHUNK_ID);
                    if (chunkIdObj != null) {
                        view.setChunkId(Long.parseLong(String.valueOf(chunkIdObj)));
                    }

                    Object chunkNoObj = doc.getMetadata().get(DocumentKnowledgeMetadataKeys.CHUNK_NO);
                    if (chunkNoObj != null) {
                        view.setChunkNo(Integer.parseInt(String.valueOf(chunkNoObj)));
                    }

                    Object sectionPathObj = doc.getMetadata().get(DocumentKnowledgeMetadataKeys.SECTION_PATH);
                    if (sectionPathObj != null) {
                        view.setSectionPath(String.valueOf(sectionPathObj));
                    }

                    String content = doc.getText();
                    if (content != null && !content.isEmpty()) {
                        view.setChunkTextPreview(content.length() > 500 ? content.substring(0, 500) : content);
                        view.setChunkCharCount(content.length());
                    }

                    boolean passedGate = filteredResults != null && filteredResults.stream()
                        .anyMatch(fr -> channelName.equals(fr.getChannelName()) &&
                            fr.getDocuments() != null &&
                            fr.getDocuments().stream().anyMatch(d -> Objects.equals(d.getId(), doc.getId())));
                    view.setGatePassed(passedGate);

                    boolean isSelected = doc.getId() != null && finalRankMap.containsKey(doc.getId());
                    view.setSelected(isSelected);

                    if (isSelected) {
                        view.setFinalRank(finalRankMap.get(doc.getId()));
                        view.setSelectionReason("已选入最终 Prompt");
                    } else if (!passedGate) {

                        Object scoreObj2 = doc.getMetadata().get(DocumentKnowledgeMetadataKeys.SCORE);
                        double score = scoreObj2 instanceof Number ? ((Number) scoreObj2).doubleValue() : 0.0;
                        if ("vector".equals(channelName)) {
                            view.setSelectionReason(String.format(
                                "向量闸门过滤：分数 %.4f < 阈值 %.4f",
                                score, properties.getMinVectorSimilarity()
                            ));
                        } else if ("keyword".equals(channelName)) {
                            view.setSelectionReason(String.format(
                                "关键词闸门过滤：分数 %.4f 低于相对阈值（floor=%.2f）",
                                score, properties.getKeywordRelativeScoreFloor()
                            ));
                        } else {
                            view.setSelectionReason("闸门过滤：分数 " + String.format("%.4f", score));
                        }
                    } else {
                        view.setSelectionReason("超出 finalTopK 限制（topK=" + properties.getFinalTopK() + "）");
                    }

                    results.add(view);
                }
            }
        }

        traceRecorder.recordRetrievalResults(results);
    }

    private static class CandidateHolder {

        private final Document document;
        private final LinkedHashSet<String> channels = new LinkedHashSet<>();
        private double score;

        private CandidateHolder(Document document) {
            this.document = document;
        }
    }

    private Throwable unwrapThrowable(Throwable throwable) {
        if (throwable == null) {
            return null;
        }
        Throwable current = throwable;
        while (current.getCause() != null
            && current.getCause() != current
            && (current instanceof CompletionException
            || current instanceof ExecutionException
            || current instanceof TimeoutException)) {
            current = current.getCause();
        }
        return current;
    }
}
