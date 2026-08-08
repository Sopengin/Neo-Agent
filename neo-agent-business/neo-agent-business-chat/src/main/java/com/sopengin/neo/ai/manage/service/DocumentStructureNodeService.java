package com.sopengin.neo.ai.manage.service;

import com.sopengin.neo.ai.manage.data.NeoAgentDocumentStructureNode;
import com.sopengin.neo.ai.manage.support.DocumentStructureNodeCandidate;

import java.util.List;
import java.util.Map;

/**
 * 服务层
 **/

public interface DocumentStructureNodeService {

    List<NeoAgentDocumentStructureNode> replaceDocumentNodes(Long documentId,
                                                               Long parseTaskId,
                                                               List<DocumentStructureNodeCandidate> candidates);

    List<NeoAgentDocumentStructureNode> listDocumentNodes(Long documentId, Long parseTaskId);

    Map<Long, NeoAgentDocumentStructureNode> nodeMap(Long documentId, Long parseTaskId);

    void deleteByDocumentId(Long documentId);
}
