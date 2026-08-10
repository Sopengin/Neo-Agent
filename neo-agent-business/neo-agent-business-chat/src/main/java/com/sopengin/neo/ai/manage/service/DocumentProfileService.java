package com.sopengin.neo.ai.manage.service;

import com.sopengin.neo.ai.manage.data.NeoAgentDocumentProfile;
import com.sopengin.neo.ai.manage.data.NeoAgentDocumentStructureNode;
import com.sopengin.neo.ai.manage.support.DocumentAnalysisResult;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * 服务层
 **/
public interface DocumentProfileService {

    NeoAgentDocumentProfile generateProfile(Long documentId,
                                              DocumentAnalysisResult analysisResult,
                                              List<NeoAgentDocumentStructureNode> structureNodes);

    NeoAgentDocumentProfile regenerateProfile(Long documentId);

    List<NeoAgentDocumentProfile> batchRegenerateProfiles(Collection<Long> documentIds);

    Optional<NeoAgentDocumentProfile> getByDocumentId(Long documentId);
}
