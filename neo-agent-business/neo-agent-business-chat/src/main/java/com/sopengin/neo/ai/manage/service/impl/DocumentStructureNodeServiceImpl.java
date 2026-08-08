package com.sopengin.neo.ai.manage.service.impl;

import lombok.AllArgsConstructor;
import com.baidu.fsg.uid.UidGenerator;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.sopengin.neo.ai.manage.data.NeoAgentDocumentStructureNode;
import com.sopengin.neo.ai.manage.mapper.NeoAgentDocumentStructureNodeMapper;
import com.sopengin.neo.ai.manage.service.DocumentStructureNodeService;
import com.sopengin.neo.ai.manage.support.DocumentStructureNodeCandidate;
import com.sopengin.neo.enums.BusinessStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 服务实现层
 **/

@AllArgsConstructor
@Service
public class DocumentStructureNodeServiceImpl implements DocumentStructureNodeService {

    private final NeoAgentDocumentStructureNodeMapper structureNodeMapper;
    private final UidGenerator uidGenerator;

    @Override
    public List<NeoAgentDocumentStructureNode> replaceDocumentNodes(Long documentId,
                                                                      Long parseTaskId,
                                                                      List<DocumentStructureNodeCandidate> candidates) {
        deleteByDocumentId(documentId);
        if (documentId == null || parseTaskId == null || candidates == null || candidates.isEmpty()) {
            return List.of();
        }
        Map<Integer, Long> nodeIdMap = new LinkedHashMap<>();
        List<NeoAgentDocumentStructureNode> entities = new ArrayList<>();
        for (DocumentStructureNodeCandidate candidate : candidates) {
            if (candidate == null || candidate.getNodeNo() == null) {
                continue;
            }
            long id = uidGenerator.getUid();
            nodeIdMap.put(candidate.getNodeNo(), id);
        }
        for (DocumentStructureNodeCandidate candidate : candidates) {
            if (candidate == null || candidate.getNodeNo() == null) {
                continue;
            }
            NeoAgentDocumentStructureNode entity = new NeoAgentDocumentStructureNode();
            entity.setId(nodeIdMap.get(candidate.getNodeNo()));
            entity.setDocumentId(documentId);
            entity.setParseTaskId(parseTaskId);
            entity.setNodeNo(candidate.getNodeNo());
            entity.setNodeType(candidate.getNodeType());
            entity.setParentNodeId(candidate.getParentNodeNo() == null ? null : nodeIdMap.get(candidate.getParentNodeNo()));
            entity.setPrevSiblingNodeId(candidate.getPrevSiblingNodeNo() == null ? null : nodeIdMap.get(candidate.getPrevSiblingNodeNo()));
            entity.setNextSiblingNodeId(candidate.getNextSiblingNodeNo() == null ? null : nodeIdMap.get(candidate.getNextSiblingNodeNo()));
            entity.setDepth(candidate.getDepth());
            entity.setNodeCode(candidate.getNodeCode());
            entity.setTitle(candidate.getTitle());
            entity.setAnchorText(candidate.getAnchorText());
            entity.setCanonicalPath(candidate.getCanonicalPath());
            entity.setSectionPath(candidate.getSectionPath());
            entity.setContentText(candidate.getContentText());
            entity.setItemIndex(candidate.getItemIndex());
            entity.setStatus(BusinessStatus.YES.getCode());
            structureNodeMapper.insert(entity);
            entities.add(entity);
        }
        return entities;
    }

    @Override
    public List<NeoAgentDocumentStructureNode> listDocumentNodes(Long documentId, Long parseTaskId) {
        if (documentId == null) {
            return List.of();
        }
        LambdaQueryWrapper<NeoAgentDocumentStructureNode> wrapper = new LambdaQueryWrapper<NeoAgentDocumentStructureNode>()
            .eq(NeoAgentDocumentStructureNode::getDocumentId, documentId)
            .eq(NeoAgentDocumentStructureNode::getStatus, BusinessStatus.YES.getCode())
            .orderByAsc(NeoAgentDocumentStructureNode::getNodeNo);
        if (parseTaskId != null) {
            wrapper.eq(NeoAgentDocumentStructureNode::getParseTaskId, parseTaskId);
        }
        return structureNodeMapper.selectList(wrapper);
    }

    @Override
    public Map<Long, NeoAgentDocumentStructureNode> nodeMap(Long documentId, Long parseTaskId) {
        Map<Long, NeoAgentDocumentStructureNode> result = new LinkedHashMap<>();
        for (NeoAgentDocumentStructureNode node : listDocumentNodes(documentId, parseTaskId)) {
            result.put(node.getId(), node);
        }
        return result;
    }

    @Override
    public void deleteByDocumentId(Long documentId) {
        if (documentId == null) {
            return;
        }
        structureNodeMapper.delete(new LambdaQueryWrapper<NeoAgentDocumentStructureNode>()
            .eq(NeoAgentDocumentStructureNode::getDocumentId, documentId));
    }
}
