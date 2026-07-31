package com.sopengin.neo.ai.manage.service;

import com.sopengin.neo.ai.manage.data.NeoAgentDocument;
import com.sopengin.neo.ai.manage.data.NeoAgentDocumentStrategyPlan;
import com.sopengin.neo.ai.manage.data.NeoAgentDocumentStrategyStep;
import com.sopengin.neo.ai.manage.support.DocumentAnalysisResult;
import com.sopengin.neo.ai.manage.support.DocumentStrategyPlanDraft;
import com.sopengin.neo.ai.manage.support.ParentBlockCandidate;

import java.util.List;

/**
 * 服务层
 **/

public interface DocumentStrategyService {

    DocumentStrategyPlanDraft recommendStrategy(NeoAgentDocument document, DocumentAnalysisResult analysisResult);

    List<NeoAgentDocumentStrategyStep> normalizeSteps(NeoAgentDocumentStrategyPlan basePlan,
                                                        List<NeoAgentDocumentStrategyStep> baseSteps,
                                                        List<Integer> requestParentStrategyTypes,
                                                        List<Integer> requestChildStrategyTypes,
                                                        Long documentId);

    List<ParentBlockCandidate> buildParentBlocks(NeoAgentDocument document,
                                                 NeoAgentDocumentStrategyPlan plan,
                                                 List<NeoAgentDocumentStrategyStep> steps,
                                                 String parsedText);
}
