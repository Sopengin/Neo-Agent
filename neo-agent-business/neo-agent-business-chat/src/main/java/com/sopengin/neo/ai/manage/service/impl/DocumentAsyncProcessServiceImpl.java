package com.sopengin.neo.ai.manage.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baidu.fsg.uid.UidGenerator;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import jakarta.annotation.Resource;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.sopengin.neo.ai.manage.data.NeoAgentDocument;
import com.sopengin.neo.ai.manage.data.NeoAgentDocumentChunk;
import com.sopengin.neo.ai.manage.data.NeoAgentDocumentParentBlock;
import com.sopengin.neo.ai.manage.data.NeoAgentDocumentStrategyPlan;
import com.sopengin.neo.ai.manage.data.NeoAgentDocumentStrategyStep;
import com.sopengin.neo.ai.manage.data.NeoAgentDocumentStructureNode;
import com.sopengin.neo.ai.manage.data.NeoAgentDocumentTask;
import com.sopengin.neo.ai.manage.mapper.NeoAgentDocumentChunkMapper;
import com.sopengin.neo.ai.manage.mapper.NeoAgentDocumentMapper;
import com.sopengin.neo.ai.manage.mapper.NeoAgentDocumentParentBlockMapper;
import com.sopengin.neo.ai.manage.mapper.NeoAgentDocumentStrategyPlanMapper;
import com.sopengin.neo.ai.manage.mapper.NeoAgentDocumentStrategyStepMapper;
import com.sopengin.neo.ai.manage.mapper.NeoAgentDocumentTaskMapper;
import com.sopengin.neo.ai.manage.service.DocumentAsyncProcessService;
import com.sopengin.neo.ai.manage.service.DocumentNavigationIndexService;
import com.sopengin.neo.ai.manage.service.DocumentParserService;
import com.sopengin.neo.ai.manage.service.DocumentProfileService;
import com.sopengin.neo.ai.manage.service.DocumentStorageService;
import com.sopengin.neo.ai.manage.service.DocumentStrategyService;
import com.sopengin.neo.ai.manage.service.DocumentStructureGraphProjectionService;
import com.sopengin.neo.ai.manage.service.DocumentStructureNodeService;
import com.sopengin.neo.ai.manage.service.DocumentTaskLogService;
import com.sopengin.neo.ai.manage.service.DocumentVectorGateway;
import com.sopengin.neo.ai.manage.service.keyword.DocumentKeywordSearchGateway;
import com.sopengin.neo.ai.manage.support.ChunkCandidate;
import com.sopengin.neo.ai.manage.support.DocumentAnalysisResult;
import com.sopengin.neo.ai.manage.support.DocumentStrategyPlanDraft;
import com.sopengin.neo.ai.manage.support.DocumentStrategyStepDraft;
import com.sopengin.neo.ai.manage.support.ParentBlockCandidate;
import com.sopengin.neo.enums.BusinessStatus;
import com.sopengin.neo.enums.DocumentChunkSourceTypeEnum;
import com.sopengin.neo.enums.DocumentFileTypeEnum;
import com.sopengin.neo.enums.DocumentIndexStatusEnum;
import com.sopengin.neo.enums.DocumentLogLevelEnum;
import com.sopengin.neo.enums.DocumentOperatorTypeEnum;
import com.sopengin.neo.enums.DocumentParseStatusEnum;
import com.sopengin.neo.enums.DocumentPlanSourceEnum;
import com.sopengin.neo.enums.DocumentPlanStatusEnum;
import com.sopengin.neo.enums.DocumentStrategyExecuteStatusEnum;
import com.sopengin.neo.enums.DocumentStrategyPipelineTypeEnum;
import com.sopengin.neo.enums.DocumentStrategyStatusEnum;
import com.sopengin.neo.enums.DocumentTaskEventTypeEnum;
import com.sopengin.neo.enums.DocumentTaskStageEnum;
import com.sopengin.neo.enums.DocumentTaskStatusEnum;
import com.sopengin.neo.enums.DocumentVectorStatusEnum;
import com.sopengin.neo.enums.DocumentVectorStoreTypeEnum;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 服务实现层
 **/

@Slf4j
@AllArgsConstructor
@Service
public class DocumentAsyncProcessServiceImpl implements DocumentAsyncProcessService {

    private final NeoAgentDocumentMapper documentMapper;

    private final NeoAgentDocumentStrategyPlanMapper planMapper;

    private final NeoAgentDocumentStrategyStepMapper stepMapper;

    private final NeoAgentDocumentTaskMapper taskMapper;

    private final NeoAgentDocumentParentBlockMapper parentBlockMapper;

    private final NeoAgentDocumentChunkMapper chunkMapper;

    private final DocumentStorageService storageService;

    private final DocumentParserService parserService;

    private final DocumentStrategyService strategyService;

    private final DocumentStructureNodeService structureNodeService;

    private final DocumentTaskLogService taskLogService;

    private final DocumentVectorGateway vectorGateway;

    private final ObjectProvider<DocumentKeywordSearchGateway> keywordSearchGatewayProvider;

    private final ObjectProvider<DocumentNavigationIndexService> navigationIndexServiceProvider;

    private final ObjectProvider<DocumentStructureGraphProjectionService> graphProjectionServiceProvider;

    private final DocumentProfileService documentProfileService;

    @Resource
    private UidGenerator uidGenerator;

    @Override

    public void handleParseRoute(Long documentId, Long taskId) {

        NeoAgentDocument document = documentMapper.selectById(documentId);
        NeoAgentDocumentTask task = taskMapper.selectById(taskId);
        if (document == null || task == null) {
            log.warn("解析任务对应的文档或任务不存在，documentId={}, taskId={}", documentId, taskId);
            return;
        }

        Date startTime = new Date();
        try {

            task.setTaskStatus(DocumentTaskStatusEnum.RUNNING.getCode());
            task.setCurrentStage(DocumentTaskStageEnum.CONTENT_PARSE.getCode());
            task.setStartTime(startTime);
            taskMapper.updateById(task);

            document.setParseStatus(DocumentParseStatusEnum.PARSING.getCode());
            documentMapper.updateById(document);

            taskLogService.saveLog(taskId, documentId,
                DocumentTaskStageEnum.CONTENT_PARSE.getCode(),
                DocumentTaskEventTypeEnum.START.getCode(),
                DocumentLogLevelEnum.INFO.getCode(),
                DocumentOperatorTypeEnum.SYSTEM.getCode(),
                null,
                "开始解析文档内容。",
                Map.of("objectName", document.getObjectName()));

            byte[] fileBytes = storageService.downloadObject(document.getObjectName());
            DocumentAnalysisResult analysisResult = parserService.parse(fileBytes, document.getOriginalFileName(),
                document.getMimeType(), DocumentFileTypeEnum.getRc(document.getFileType()));

            String parseTextPath = storageService.uploadParsedText(documentId, analysisResult.getParsedText());

            List<NeoAgentDocumentStructureNode> structureNodes = structureNodeService.replaceDocumentNodes(
                documentId,
                taskId,
                analysisResult.getStructureNodes()
            );
            int structureNodeCount = structureNodes.size();
            syncNavigationArtifacts(documentId, taskId, structureNodes);
            documentProfileService.generateProfile(documentId, analysisResult, structureNodes);

            taskLogService.saveLog(taskId, documentId,
                DocumentTaskStageEnum.CONTENT_PARSE.getCode(),
                DocumentTaskEventTypeEnum.COMPLETE.getCode(),
                DocumentLogLevelEnum.INFO.getCode(),
                DocumentOperatorTypeEnum.SYSTEM.getCode(),
                null,
                "文档解析完成。",
                Map.of(
                    "charCount", analysisResult.getCharCount(),
                    "tokenCount", analysisResult.getTokenCount(),
                    "structureLevel", analysisResult.getStructureLevel(),
                    "contentQualityLevel", analysisResult.getContentQualityLevel(),
                    "structureNodeCount", structureNodeCount
                ));

            task.setCurrentStage(DocumentTaskStageEnum.STRATEGY_ROUTE.getCode());
            taskMapper.updateById(task);

            DocumentStrategyPlanDraft planDraft = strategyService.recommendStrategy(document, analysisResult);
            Long planId = uidGenerator.getUid();
            int planVersion = getNextPlanVersion(documentId);

            NeoAgentDocumentStrategyPlan plan = new NeoAgentDocumentStrategyPlan();
            plan.setId(planId);
            plan.setDocumentId(documentId);
            plan.setPlanVersion(planVersion);
            plan.setPlanSource(DocumentPlanSourceEnum.SYSTEM_RECOMMEND.getCode());
            plan.setPlanStatus(DocumentPlanStatusEnum.WAIT_CONFIRM.getCode());
            plan.setStrategyCount(planDraft.getParentSteps().size() + planDraft.getChildSteps().size());
            plan.setStrategySnapshot(planDraft.getStrategySnapshot());
            plan.setRecommendReason(planDraft.getRecommendReason());
            plan.setStatus(BusinessStatus.YES.getCode());
            planMapper.insert(plan);

            for (int index = 0; index < planDraft.getParentSteps().size(); index++) {
                DocumentStrategyStepDraft draft = planDraft.getParentSteps().get(index);
                NeoAgentDocumentStrategyStep step = new NeoAgentDocumentStrategyStep();
                step.setId(uidGenerator.getUid());
                step.setPlanId(planId);
                step.setDocumentId(documentId);
                step.setPipelineType(draft.getPipelineType());
                step.setStepNo(index + 1);
                step.setStrategyType(draft.getStrategyType());
                step.setStrategyRole(draft.getStrategyRole());
                step.setSourceType(draft.getSourceType());
                step.setExecuteStatus(DocumentStrategyExecuteStatusEnum.WAIT_EXECUTE.getCode());
                step.setRecommendReason(draft.getRecommendReason());
                step.setStatus(BusinessStatus.YES.getCode());
                stepMapper.insert(step);
            }
            for (int index = 0; index < planDraft.getChildSteps().size(); index++) {
                DocumentStrategyStepDraft draft = planDraft.getChildSteps().get(index);
                NeoAgentDocumentStrategyStep step = new NeoAgentDocumentStrategyStep();
                step.setId(uidGenerator.getUid());
                step.setPlanId(planId);
                step.setDocumentId(documentId);
                step.setPipelineType(draft.getPipelineType());
                step.setStepNo(index + 1);
                step.setStrategyType(draft.getStrategyType());
                step.setStrategyRole(draft.getStrategyRole());
                step.setSourceType(draft.getSourceType());
                step.setExecuteStatus(DocumentStrategyExecuteStatusEnum.WAIT_EXECUTE.getCode());
                step.setRecommendReason(draft.getRecommendReason());
                step.setStatus(BusinessStatus.YES.getCode());
                stepMapper.insert(step);
            }

            document.setParseStatus(DocumentParseStatusEnum.PARSE_SUCCESS.getCode());
            document.setStrategyStatus(DocumentStrategyStatusEnum.RECOMMENDED.getCode());
            document.setCharCount(analysisResult.getCharCount());
            document.setTokenCount(analysisResult.getTokenCount());
            document.setStructureLevel(analysisResult.getStructureLevel());
            document.setContentQualityLevel(analysisResult.getContentQualityLevel());
            document.setParseTextPath(parseTextPath);
            document.setParseErrorMsg(null);
            document.setCurrentPlanId(planId);
            document.setLastParseTaskId(taskId);
            document.setStructureNodeCount(structureNodeCount);
            documentMapper.updateById(document);

            finishTaskSuccess(task, DocumentTaskStageEnum.STRATEGY_ROUTE.getCode(), startTime);
            taskLogService.saveLog(taskId, documentId,
                DocumentTaskStageEnum.STRATEGY_ROUTE.getCode(),
                DocumentTaskEventTypeEnum.RECOMMEND_STRATEGY.getCode(),
                DocumentLogLevelEnum.INFO.getCode(),
                DocumentOperatorTypeEnum.SYSTEM.getCode(),
                null,
                "系统已生成推荐策略。",
                detail("planId", planId,
                    "strategySnapshot", planDraft.getStrategySnapshot(),
                    "parentStepCount", planDraft.getParentSteps().size(),
                    "childStepCount", planDraft.getChildSteps().size(),
                    "structureNodeCount", structureNodeCount,
                    "recommendReason", planDraft.getRecommendReason()));
        }
        catch (Exception exception) {
            log.error("异步解析文档失败，documentId={}, taskId={}", documentId, taskId, exception);

            document.setParseStatus(DocumentParseStatusEnum.PARSE_FAILED.getCode());
            document.setParseErrorMsg(exception.getMessage());
            documentMapper.updateById(document);

            failTask(task, startTime, exception, DocumentTaskStageEnum.CONTENT_PARSE.getCode());
            taskLogService.saveLog(taskId, documentId,
                DocumentTaskStageEnum.CONTENT_PARSE.getCode(),
                DocumentTaskEventTypeEnum.FAILED.getCode(),
                DocumentLogLevelEnum.ERROR.getCode(),
                DocumentOperatorTypeEnum.SYSTEM.getCode(),
                null,
                "文档解析失败。",
                detail("error", exception.getMessage()));
        }
    }

    @Override
    public void handleIndexBuild(Long documentId, Long taskId, Long planId) {

        NeoAgentDocument document = documentMapper.selectById(documentId);
        NeoAgentDocumentTask task = taskMapper.selectById(taskId);
        NeoAgentDocumentStrategyPlan plan = planMapper.selectById(planId);
        if (document == null || task == null || plan == null) {
            log.warn("索引任务对应的数据不存在，documentId={}, taskId={}, planId={}", documentId, taskId, planId);
            return;
        }

        Date startTime = new Date();

        List<NeoAgentDocumentStrategyStep> stepList = listSteps(planId);
        try {

            task.setTaskStatus(DocumentTaskStatusEnum.RUNNING.getCode());
            task.setCurrentStage(DocumentTaskStageEnum.CHUNK_EXECUTE.getCode());
            task.setStartTime(startTime);
            taskMapper.updateById(task);

            document.setIndexStatus(DocumentIndexStatusEnum.BUILDING.getCode());
            documentMapper.updateById(document);

            updateStepExecuteStatus(planId, DocumentStrategyExecuteStatusEnum.EXECUTING.getCode());

            taskLogService.saveLog(taskId, documentId,
                DocumentTaskStageEnum.CHUNK_EXECUTE.getCode(),
                DocumentTaskEventTypeEnum.START.getCode(),
                DocumentLogLevelEnum.INFO.getCode(),
                DocumentOperatorTypeEnum.SYSTEM.getCode(),
                null,
                "开始执行切块流水线。",
                Map.of("strategySnapshot", plan.getStrategySnapshot()));

            String parsedText = storageService.downloadText(document.getParseTextPath());

            List<ParentBlockCandidate> parentBlockCandidateList = strategyService.buildParentBlocks(document, plan, stepList, parsedText);

            updateStepExecuteStatus(planId, DocumentStrategyExecuteStatusEnum.EXECUTE_SUCCESS.getCode());

            taskLogService.saveLog(taskId, documentId,
                DocumentTaskStageEnum.CHUNK_EXECUTE.getCode(),
                DocumentTaskEventTypeEnum.COMPLETE.getCode(),
                DocumentLogLevelEnum.INFO.getCode(),
                DocumentOperatorTypeEnum.SYSTEM.getCode(),
                null,
                "切块执行完成。",
                Map.of(
                    "parentCount", parentBlockCandidateList.size(),
                    "childCount", countChildCandidates(parentBlockCandidateList)
                ));

            task.setCurrentStage(DocumentTaskStageEnum.CHUNK_POST_PROCESS.getCode());
            taskMapper.updateById(task);

            List<ParentBlockCandidate> finalParentBlockList = parentBlockCandidateList.stream()
                .filter(item -> item != null
                    && StrUtil.isNotBlank(item.getText())
                    && item.getChildChunks() != null
                    && item.getChildChunks().stream().anyMatch(child -> StrUtil.isNotBlank(child.getText())))
                .toList();

            taskLogService.saveLog(taskId, documentId,
                DocumentTaskStageEnum.CHUNK_POST_PROCESS.getCode(),
                DocumentTaskEventTypeEnum.COMPLETE.getCode(),
                DocumentLogLevelEnum.INFO.getCode(),
                DocumentOperatorTypeEnum.SYSTEM.getCode(),
                null,
                "切块后处理完成。",
                Map.of(
                    "parentCount", finalParentBlockList.size(),
                    "childCount", countChildCandidates(finalParentBlockList)
                ));

            ParentChildEntityBundle entityBundle = buildParentChildEntities(documentId, taskId, planId, finalParentBlockList);
            List<NeoAgentDocumentParentBlock> parentBlockEntityList = entityBundle.parentBlocks();
            List<NeoAgentDocumentChunk> chunkEntityList = entityBundle.childChunks();

            for (NeoAgentDocumentParentBlock parentBlock : parentBlockEntityList) {
                parentBlockMapper.insert(parentBlock);
            }
            for (NeoAgentDocumentChunk chunk : chunkEntityList) {
                chunkMapper.insert(chunk);
            }

            task.setCurrentStage(DocumentTaskStageEnum.VECTORIZE.getCode());
            taskMapper.updateById(task);

            taskLogService.saveLog(taskId, documentId,
                DocumentTaskStageEnum.VECTORIZE.getCode(),
                DocumentTaskEventTypeEnum.START.getCode(),
                DocumentLogLevelEnum.INFO.getCode(),
                DocumentOperatorTypeEnum.SYSTEM.getCode(),
                null,
                "开始执行向量化。",
                detail("chunkCount", chunkEntityList.size(),
                    "embeddingBatchSize", DefaultDocumentVectorGateway.EMBEDDING_BATCH_SIZE_LIMIT,
                    "embeddingBatchCount",
                    (chunkEntityList.size() + DefaultDocumentVectorGateway.EMBEDDING_BATCH_SIZE_LIMIT - 1)
                        / DefaultDocumentVectorGateway.EMBEDDING_BATCH_SIZE_LIMIT,
                    "vectorStoreType", DocumentVectorStoreTypeEnum.PG_VECTOR.getMsg(),
                    "parentCount", parentBlockEntityList.size()));

            vectorGateway.vectorize(chunkEntityList);

            DocumentKeywordSearchGateway keywordSearchGateway = keywordSearchGatewayProvider.getIfAvailable();
            if (keywordSearchGateway != null) {
                keywordSearchGateway.indexChunks(chunkEntityList);
            }

            for (NeoAgentDocumentChunk chunk : chunkEntityList) {
                chunkMapper.updateById(chunk);
            }

            taskLogService.saveLog(taskId, documentId,
                DocumentTaskStageEnum.VECTORIZE.getCode(),
                DocumentTaskEventTypeEnum.COMPLETE.getCode(),
                DocumentLogLevelEnum.INFO.getCode(),
                DocumentOperatorTypeEnum.SYSTEM.getCode(),
                null,
                "向量化完成。",
                detail("chunkCount", chunkEntityList.size(),
                    "embeddingBatchSize", DefaultDocumentVectorGateway.EMBEDDING_BATCH_SIZE_LIMIT,
                    "embeddingBatchCount",
                    (chunkEntityList.size() + DefaultDocumentVectorGateway.EMBEDDING_BATCH_SIZE_LIMIT - 1)
                        / DefaultDocumentVectorGateway.EMBEDDING_BATCH_SIZE_LIMIT,
                    "vectorStoreType", DocumentVectorStoreTypeEnum.PG_VECTOR.getMsg(),
                    "parentCount", parentBlockEntityList.size()));

            task.setCurrentStage(DocumentTaskStageEnum.STORE_COMPLETE.getCode());
            taskMapper.updateById(task);

            plan.setPlanStatus(DocumentPlanStatusEnum.EXECUTED.getCode());
            planMapper.updateById(plan);

            document.setIndexStatus(DocumentIndexStatusEnum.BUILD_SUCCESS.getCode());
            document.setLastIndexTaskId(taskId);
            documentMapper.updateById(document);

            finishTaskSuccess(task, DocumentTaskStageEnum.STORE_COMPLETE.getCode(), startTime);
            taskLogService.saveLog(taskId, documentId,
                DocumentTaskStageEnum.STORE_COMPLETE.getCode(),
                DocumentTaskEventTypeEnum.COMPLETE.getCode(),
                DocumentLogLevelEnum.INFO.getCode(),
                DocumentOperatorTypeEnum.SYSTEM.getCode(),
                null,
                "索引构建完成。",
                Map.of("taskId", taskId, "chunkCount", chunkEntityList.size(), "parentCount", parentBlockEntityList.size()));
        }
        catch (Exception exception) {
            log.error("异步构建索引失败，documentId={}, taskId={}, planId={}", documentId, taskId, planId, exception);

            document.setIndexStatus(DocumentIndexStatusEnum.BUILD_FAILED.getCode());
            documentMapper.updateById(document);

            chunkMapper.update(null, new LambdaUpdateWrapper<NeoAgentDocumentChunk>()
                .eq(NeoAgentDocumentChunk::getTaskId, taskId)
                .eq(NeoAgentDocumentChunk::getStatus, BusinessStatus.YES.getCode())
                .set(NeoAgentDocumentChunk::getVectorStatus, DocumentVectorStatusEnum.VECTOR_FAILED.getCode())
                .set(NeoAgentDocumentChunk::getVectorStoreType, DocumentVectorStoreTypeEnum.PG_VECTOR.getCode()));

            updateStepExecuteStatus(planId, DocumentStrategyExecuteStatusEnum.EXECUTE_FAILED.getCode());
            failTask(task, startTime, exception, task.getCurrentStage());
            taskLogService.saveLog(taskId, documentId,
                task.getCurrentStage(),
                DocumentTaskEventTypeEnum.FAILED.getCode(),
                DocumentLogLevelEnum.ERROR.getCode(),
                DocumentOperatorTypeEnum.SYSTEM.getCode(),
                null,
                "索引构建失败。",
                detail("error", exception.getMessage()));
        }
    }

    private ParentChildEntityBundle buildParentChildEntities(Long documentId,
                                                             Long taskId,
                                                             Long planId,
                                                             List<ParentBlockCandidate> parentBlockCandidateList) {
        List<NeoAgentDocumentParentBlock> parentBlockEntityList = new ArrayList<>();
        List<NeoAgentDocumentChunk> chunkEntityList = new ArrayList<>();
        int globalChunkNo = 1;

        for (int parentIndex = 0; parentIndex < parentBlockCandidateList.size(); parentIndex++) {
            ParentBlockCandidate parentCandidate = parentBlockCandidateList.get(parentIndex);
            if (parentCandidate == null || StrUtil.isBlank(parentCandidate.getText())) {
                continue;
            }

            NeoAgentDocumentParentBlock parentBlock = new NeoAgentDocumentParentBlock();
            parentBlock.setId(uidGenerator.getUid());
            parentBlock.setDocumentId(documentId);
            parentBlock.setTaskId(taskId);
            parentBlock.setPlanId(planId);
            parentBlock.setParentNo(parentIndex + 1);
            parentBlock.setSourceType(parentCandidate.getSourceType() == null
                ? DocumentChunkSourceTypeEnum.ORIGINAL.getCode() : parentCandidate.getSourceType());
            parentBlock.setSectionPath(parentCandidate.getSectionPath());
            parentBlock.setStructureNodeId(parentCandidate.getStructureNodeId());
            parentBlock.setStructureNodeType(parentCandidate.getStructureNodeType());
            parentBlock.setCanonicalPath(parentCandidate.getCanonicalPath());
            parentBlock.setItemIndex(parentCandidate.getItemIndex());
            parentBlock.setParentText(parentCandidate.getText().trim());
            parentBlock.setCharCount(parentCandidate.getText().length());
            parentBlock.setTokenCount(estimateTokenCount(parentCandidate.getText()));
            parentBlock.setStatus(BusinessStatus.YES.getCode());

            int startChunkNo = globalChunkNo;
            int childCount = 0;
            for (ChunkCandidate childCandidate : parentCandidate.getChildChunks()) {
                if (childCandidate == null || StrUtil.isBlank(childCandidate.getText())) {
                    continue;
                }
                NeoAgentDocumentChunk chunk = new NeoAgentDocumentChunk();
                chunk.setId(uidGenerator.getUid());
                chunk.setDocumentId(documentId);
                chunk.setTaskId(taskId);
                chunk.setPlanId(planId);
                chunk.setParentBlockId(parentBlock.getId());
                chunk.setChunkNo(globalChunkNo++);
                chunk.setSourceType(childCandidate.getSourceType() == null
                    ? DocumentChunkSourceTypeEnum.ORIGINAL.getCode() : childCandidate.getSourceType());
                chunk.setSectionPath(StrUtil.blankToDefault(childCandidate.getSectionPath(), parentCandidate.getSectionPath()));
                chunk.setStructureNodeId(childCandidate.getStructureNodeId());
                chunk.setStructureNodeType(childCandidate.getStructureNodeType());
                chunk.setCanonicalPath(childCandidate.getCanonicalPath());
                chunk.setItemIndex(childCandidate.getItemIndex());
                chunk.setChunkText(childCandidate.getText().trim());
                chunk.setCharCount(childCandidate.getText().length());

                chunk.setTokenCount(estimateTokenCount(childCandidate.getText()));
                chunk.setVectorStatus(DocumentVectorStatusEnum.WAIT_VECTOR.getCode());
                chunk.setVectorStoreType(DocumentVectorStoreTypeEnum.PG_VECTOR.getCode());
                chunk.setStatus(BusinessStatus.YES.getCode());
                chunkEntityList.add(chunk);
                childCount++;
            }

            parentBlock.setChildCount(childCount);
            parentBlock.setStartChunkNo(childCount == 0 ? null : startChunkNo);
            parentBlock.setEndChunkNo(childCount == 0 ? null : globalChunkNo - 1);
            parentBlockEntityList.add(parentBlock);
        }

        return new ParentChildEntityBundle(parentBlockEntityList, chunkEntityList);
    }

    private int countChildCandidates(List<ParentBlockCandidate> parentBlockCandidateList) {
        if (parentBlockCandidateList == null || parentBlockCandidateList.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (ParentBlockCandidate candidate : parentBlockCandidateList) {
            if (candidate == null || candidate.getChildChunks() == null) {
                continue;
            }
            count += (int) candidate.getChildChunks().stream()
                .filter(child -> child != null && StrUtil.isNotBlank(child.getText()))
                .count();
        }
        return count;
    }

    private void updateStepExecuteStatus(Long planId, Integer executeStatus) {

        stepMapper.update(null, new LambdaUpdateWrapper<NeoAgentDocumentStrategyStep>()
            .eq(NeoAgentDocumentStrategyStep::getPlanId, planId)
            .eq(NeoAgentDocumentStrategyStep::getStatus, BusinessStatus.YES.getCode())
            .set(NeoAgentDocumentStrategyStep::getExecuteStatus, executeStatus));
    }

    private List<NeoAgentDocumentStrategyStep> listSteps(Long planId) {
        List<NeoAgentDocumentStrategyStep> stepList = stepMapper.selectList(new LambdaQueryWrapper<NeoAgentDocumentStrategyStep>()
            .eq(NeoAgentDocumentStrategyStep::getPlanId, planId)
            .eq(NeoAgentDocumentStrategyStep::getStatus, BusinessStatus.YES.getCode()));
        return stepList.stream()
            .sorted(Comparator
                .comparingInt((NeoAgentDocumentStrategyStep step) -> pipelineOrder(step.getPipelineType()))
                .thenComparing(NeoAgentDocumentStrategyStep::getStepNo)
                .thenComparing(NeoAgentDocumentStrategyStep::getId))
            .toList();
    }

    private int pipelineOrder(String pipelineType) {
        return DocumentStrategyPipelineTypeEnum.PARENT.getCode().equalsIgnoreCase(
            StrUtil.blankToDefault(pipelineType, "")
        ) ? 0 : 1;
    }

    private int getNextPlanVersion(Long documentId) {

        List<NeoAgentDocumentStrategyPlan> planList = planMapper.selectList(new LambdaQueryWrapper<NeoAgentDocumentStrategyPlan>()
            .eq(NeoAgentDocumentStrategyPlan::getDocumentId, documentId)
            .eq(NeoAgentDocumentStrategyPlan::getStatus, BusinessStatus.YES.getCode())
            .orderByDesc(NeoAgentDocumentStrategyPlan::getPlanVersion)
            .last("limit 1"));
        return planList.isEmpty() ? 1 : planList.get(0).getPlanVersion() + 1;
    }

    private void finishTaskSuccess(NeoAgentDocumentTask task, Integer stage, Date startTime) {

        Date finishTime = new Date();
        task.setTaskStatus(DocumentTaskStatusEnum.SUCCESS.getCode());
        task.setCurrentStage(stage);
        task.setFinishTime(finishTime);
        task.setCostMillis(finishTime.getTime() - startTime.getTime());
        task.setErrorCode(null);
        task.setErrorMsg(null);
        taskMapper.updateById(task);
    }

    private void syncNavigationArtifacts(Long documentId,
                                         Long parseTaskId,
                                         List<NeoAgentDocumentStructureNode> structureNodes) {
        log.info("开始同步导航产物: documentId={}, parseTaskId={}, structureNodeCount={}",
            documentId,
            parseTaskId,
            structureNodes == null ? 0 : structureNodes.size());
        DocumentNavigationIndexService navigationIndexService = navigationIndexServiceProvider.getIfAvailable();
        if (navigationIndexService != null) {
            log.info("同步导航 ES 索引: documentId={}, parseTaskId={}", documentId, parseTaskId);
            navigationIndexService.reindexDocumentNodes(documentId, parseTaskId, structureNodes);
        }
        else {
            log.info("跳过导航 ES 索引同步，因为服务未启用: documentId={}, parseTaskId={}", documentId, parseTaskId);
        }
        DocumentStructureGraphProjectionService graphProjectionService = graphProjectionServiceProvider.getIfAvailable();
        if (graphProjectionService != null && graphProjectionService.enabled()) {
            log.info("同步结构图投影: documentId={}, parseTaskId={}", documentId, parseTaskId);
            graphProjectionService.projectToGraph(documentId, parseTaskId);
        }
        else {
            log.info("跳过结构图投影，因为图服务未启用: documentId={}, parseTaskId={}", documentId, parseTaskId);
        }
    }

    private void failTask(NeoAgentDocumentTask task, Date startTime, Exception exception, Integer currentStage) {

        Date finishTime = new Date();
        task.setTaskStatus(DocumentTaskStatusEnum.FAILED.getCode());
        task.setCurrentStage(currentStage);
        task.setFinishTime(finishTime);
        task.setCostMillis(finishTime.getTime() - startTime.getTime());
        task.setErrorCode("TASK_FAILED");
        task.setErrorMsg(exception.getMessage());
        taskMapper.updateById(task);
    }

    private int estimateTokenCount(String text) {
        if (StrUtil.isBlank(text)) {
            return 0;
        }
        int chineseCount = 0;
        int englishCount = 0;

        for (char current : text.toCharArray()) {
            if (String.valueOf(current).matches("[\\u4e00-\\u9fa5]")) {
                chineseCount++;
            }
        }

        for (String word : text.split("\\s+")) {
            if (word.matches(".*[A-Za-z].*")) {
                englishCount++;
            }
        }

        return chineseCount + englishCount + Math.max(1, (text.length() - chineseCount) / 4);
    }

    private Map<String, Object> detail(Object... keyValues) {
        Map<String, Object> detailMap = new LinkedHashMap<>();

        for (int index = 0; index + 1 < keyValues.length; index += 2) {
            detailMap.put(String.valueOf(keyValues[index]), keyValues[index + 1]);
        }
        return detailMap;
    }

    private record ParentChildEntityBundle(
        List<NeoAgentDocumentParentBlock> parentBlocks,
        List<NeoAgentDocumentChunk> childChunks
    ) {
    }
}
