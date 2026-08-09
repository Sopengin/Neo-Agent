package com.sopengin.neo.ai.manage.service.impl;

import lombok.AllArgsConstructor;
import com.baidu.fsg.uid.UidGenerator;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import cn.hutool.core.util.StrUtil;
import com.sopengin.neo.ai.manage.data.NeoAgentDocument;
import com.sopengin.neo.ai.manage.data.NeoAgentDocumentProfile;
import com.sopengin.neo.ai.manage.data.NeoAgentKnowledgeScopeNode;
import com.sopengin.neo.ai.manage.data.NeoAgentKnowledgeTopicNode;
import com.sopengin.neo.ai.manage.data.NeoAgentTopicDocumentRelation;
import com.sopengin.neo.ai.manage.dto.DocumentProfileBatchRegenerateDto;
import com.sopengin.neo.ai.manage.dto.DocumentProfileDetailQueryDto;
import com.sopengin.neo.ai.manage.dto.DocumentProfileRegenerateDto;
import com.sopengin.neo.ai.manage.dto.KnowledgeRouteTraceQueryDto;
import com.sopengin.neo.ai.manage.dto.KnowledgeScopeDeleteDto;
import com.sopengin.neo.ai.manage.dto.KnowledgeScopeSaveDto;
import com.sopengin.neo.ai.manage.dto.KnowledgeTopicDeleteDto;
import com.sopengin.neo.ai.manage.dto.KnowledgeTopicQueryDto;
import com.sopengin.neo.ai.manage.dto.KnowledgeTopicSaveDto;
import com.sopengin.neo.ai.manage.dto.TopicDocumentRelationListQueryDto;
import com.sopengin.neo.ai.manage.dto.TopicDocumentRelationRemoveDto;
import com.sopengin.neo.ai.manage.dto.TopicDocumentRelationSaveDto;
import com.sopengin.neo.ai.manage.mapper.NeoAgentDocumentMapper;
import com.sopengin.neo.ai.manage.mapper.NeoAgentKnowledgeScopeNodeMapper;
import com.sopengin.neo.ai.manage.mapper.NeoAgentKnowledgeTopicNodeMapper;
import com.sopengin.neo.ai.manage.mapper.NeoAgentKnowledgeRouteTraceMapper;
import com.sopengin.neo.ai.manage.mapper.NeoAgentTopicDocumentRelationMapper;
import com.sopengin.neo.ai.manage.service.DocumentProfileService;
import com.sopengin.neo.ai.manage.service.KnowledgeManageService;
import com.sopengin.neo.ai.manage.vo.DocumentProfileVo;
import com.sopengin.neo.ai.manage.vo.KnowledgeRouteTraceItemVo;
import com.sopengin.neo.ai.manage.vo.KnowledgeRouteTracePageVo;
import com.sopengin.neo.ai.manage.vo.KnowledgeScopeItemVo;
import com.sopengin.neo.ai.manage.vo.KnowledgeTopicItemVo;
import com.sopengin.neo.ai.manage.vo.TopicDocumentRelationItemVo;
import com.sopengin.neo.enums.BaseCode;
import com.sopengin.neo.enums.BusinessStatus;
import com.sopengin.neo.exception.NeoAgentFrameException;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

/**
 * 服务实现层
 **/
@AllArgsConstructor
@Service
public class KnowledgeManageServiceImpl implements KnowledgeManageService {

    private final NeoAgentKnowledgeScopeNodeMapper scopeNodeMapper;
    private final NeoAgentKnowledgeTopicNodeMapper topicNodeMapper;
    private final NeoAgentTopicDocumentRelationMapper topicDocumentRelationMapper;
    private final NeoAgentKnowledgeRouteTraceMapper knowledgeRouteTraceMapper;
    private final NeoAgentDocumentMapper documentMapper;
    private final DocumentProfileService documentProfileService;
    private final UidGenerator uidGenerator;

    @Override
    public KnowledgeScopeItemVo saveScope(KnowledgeScopeSaveDto dto) {
        validateScope(dto);
        NeoAgentKnowledgeScopeNode entity = scopeNodeMapper.selectOne(new LambdaQueryWrapper<NeoAgentKnowledgeScopeNode>()
            .eq(NeoAgentKnowledgeScopeNode::getScopeCode, dto.getScopeCode().trim())
            .eq(NeoAgentKnowledgeScopeNode::getStatus, BusinessStatus.YES.getCode())
            .last("LIMIT 1"));
        if (entity == null) {
            entity = new NeoAgentKnowledgeScopeNode();
            entity.setId(uidGenerator.getUid());
            entity.setStatus(BusinessStatus.YES.getCode());
            entity.setScopeCode(dto.getScopeCode().trim());
        }
        entity.setScopeName(safeText(dto.getScopeName()));
        entity.setParentScopeCode(safeText(dto.getParentScopeCode()));
        entity.setDescription(safeText(dto.getDescription()));
        entity.setAliases(safeText(dto.getAliases()));
        entity.setExamples(safeText(dto.getExamples()));
        entity.setSortOrder(parseInteger(dto.getSortOrder(), 0));
        if (entity.getCreateTime() == null) {
            scopeNodeMapper.insert(entity);
        }
        else {
            scopeNodeMapper.updateById(entity);
        }
        return toScopeVo(entity);
    }

    @Override
    public boolean deleteScope(KnowledgeScopeDeleteDto dto) {
        String scopeCode = safeText(dto.getScopeCode());
        if (scopeCode.isBlank()) {
            throw new NeoAgentFrameException(BaseCode.PARAMETER_ERROR.getCode(), "scopeCode 不能为空。");
        }
        return scopeNodeMapper.update(null, new LambdaUpdateWrapper<NeoAgentKnowledgeScopeNode>()
            .eq(NeoAgentKnowledgeScopeNode::getScopeCode, scopeCode)
            .eq(NeoAgentKnowledgeScopeNode::getStatus, BusinessStatus.YES.getCode())
            .set(NeoAgentKnowledgeScopeNode::getStatus, BusinessStatus.NO.getCode())) > 0;
    }

    @Override
    public List<KnowledgeScopeItemVo> listScopes() {
        return scopeNodeMapper.selectList(new LambdaQueryWrapper<NeoAgentKnowledgeScopeNode>()
                .eq(NeoAgentKnowledgeScopeNode::getStatus, BusinessStatus.YES.getCode())
                .orderByAsc(NeoAgentKnowledgeScopeNode::getSortOrder, NeoAgentKnowledgeScopeNode::getId))
            .stream()
            .map(this::toScopeVo)
            .toList();
    }

    @Override
    public KnowledgeTopicItemVo saveTopic(KnowledgeTopicSaveDto dto) {
        validateTopic(dto);
        NeoAgentKnowledgeTopicNode entity = topicNodeMapper.selectOne(new LambdaQueryWrapper<NeoAgentKnowledgeTopicNode>()
            .eq(NeoAgentKnowledgeTopicNode::getTopicCode, dto.getTopicCode().trim())
            .eq(NeoAgentKnowledgeTopicNode::getStatus, BusinessStatus.YES.getCode())
            .last("LIMIT 1"));
        if (entity == null) {
            entity = new NeoAgentKnowledgeTopicNode();
            entity.setId(uidGenerator.getUid());
            entity.setStatus(BusinessStatus.YES.getCode());
            entity.setTopicCode(dto.getTopicCode().trim());
        }
        entity.setTopicName(safeText(dto.getTopicName()));
        entity.setScopeCode(safeText(dto.getScopeCode()));
        entity.setDescription(safeText(dto.getDescription()));
        entity.setAliases(safeText(dto.getAliases()));
        entity.setExamples(safeText(dto.getExamples()));
        entity.setAnswerShape(safeText(dto.getAnswerShape()));
        entity.setExecutionPreference(safeText(dto.getExecutionPreference()));
        entity.setSortOrder(parseInteger(dto.getSortOrder(), 0));
        if (entity.getCreateTime() == null) {
            topicNodeMapper.insert(entity);
        }
        else {
            topicNodeMapper.updateById(entity);
        }
        return toTopicVo(entity);
    }

    @Override
    public boolean deleteTopic(KnowledgeTopicDeleteDto dto) {
        String topicCode = safeText(dto.getTopicCode());
        if (topicCode.isBlank()) {
            throw new NeoAgentFrameException(BaseCode.PARAMETER_ERROR.getCode(), "topicCode 不能为空。");
        }
        return topicNodeMapper.update(null, new LambdaUpdateWrapper<NeoAgentKnowledgeTopicNode>()
            .eq(NeoAgentKnowledgeTopicNode::getTopicCode, topicCode)
            .eq(NeoAgentKnowledgeTopicNode::getStatus, BusinessStatus.YES.getCode())
            .set(NeoAgentKnowledgeTopicNode::getStatus, BusinessStatus.NO.getCode())) > 0;
    }

    @Override
    public List<KnowledgeTopicItemVo> listTopics(KnowledgeTopicQueryDto dto) {
        String scopeCode = dto == null ? "" : safeText(dto.getScopeCode());
        LambdaQueryWrapper<NeoAgentKnowledgeTopicNode> wrapper = new LambdaQueryWrapper<NeoAgentKnowledgeTopicNode>()
            .eq(NeoAgentKnowledgeTopicNode::getStatus, BusinessStatus.YES.getCode())
            .orderByAsc(NeoAgentKnowledgeTopicNode::getSortOrder, NeoAgentKnowledgeTopicNode::getId);
        if (scopeCode != null && !scopeCode.isBlank()) {
            wrapper.eq(NeoAgentKnowledgeTopicNode::getScopeCode, scopeCode);
        }
        return topicNodeMapper.selectList(wrapper).stream().map(this::toTopicVo).toList();
    }

    @Override
    public DocumentProfileVo queryProfile(DocumentProfileDetailQueryDto dto) {
        Long documentId = parseRequiredLong(dto == null ? null : dto.getDocumentId(), "documentId");
        NeoAgentDocumentProfile profile = documentProfileService.getByDocumentId(documentId)
            .orElseThrow(() -> new NeoAgentFrameException(BaseCode.PARAMETER_ERROR.getCode(), "文档画像不存在。"));
        return toProfileVo(profile);
    }

    @Override
    public DocumentProfileVo regenerateProfile(DocumentProfileRegenerateDto dto) {
        Long documentId = parseRequiredLong(dto == null ? null : dto.getDocumentId(), "documentId");
        return toProfileVo(documentProfileService.regenerateProfile(documentId));
    }

    @Override
    public List<DocumentProfileVo> batchRegenerateProfiles(DocumentProfileBatchRegenerateDto dto) {
        List<Long> documentIds = dto == null || dto.getDocumentIds() == null
            ? List.of()
            : dto.getDocumentIds().stream().map(value -> parseRequiredLong(value, "documentId")).toList();
        return documentProfileService.batchRegenerateProfiles(documentIds).stream()
            .map(this::toProfileVo)
            .toList();
    }

    @Override
    public List<TopicDocumentRelationItemVo> listTopicDocuments(TopicDocumentRelationListQueryDto dto) {
        String topicCode = dto == null ? "" : safeText(dto.getTopicCode());
        LambdaQueryWrapper<NeoAgentTopicDocumentRelation> wrapper = new LambdaQueryWrapper<NeoAgentTopicDocumentRelation>()
            .eq(NeoAgentTopicDocumentRelation::getStatus, BusinessStatus.YES.getCode())
            .orderByDesc(NeoAgentTopicDocumentRelation::getRelationScore, NeoAgentTopicDocumentRelation::getId);
        if (topicCode != null && !topicCode.isBlank()) {
            wrapper.eq(NeoAgentTopicDocumentRelation::getTopicCode, topicCode);
        }
        return topicDocumentRelationMapper.selectList(wrapper).stream()
            .map(this::toRelationVo)
            .toList();
    }

    @Override
    public TopicDocumentRelationItemVo saveTopicDocumentRelation(TopicDocumentRelationSaveDto dto) {
        String topicCode = safeText(dto.getTopicCode());
        Long documentId = parseRequiredLong(dto.getDocumentId(), "documentId");
        if (topicCode.isBlank()) {
            throw new NeoAgentFrameException(BaseCode.PARAMETER_ERROR.getCode(), "topicCode 不能为空。");
        }
        NeoAgentTopicDocumentRelation relation = topicDocumentRelationMapper.selectOne(new LambdaQueryWrapper<NeoAgentTopicDocumentRelation>()
            .eq(NeoAgentTopicDocumentRelation::getTopicCode, topicCode)
            .eq(NeoAgentTopicDocumentRelation::getDocumentId, documentId)
            .eq(NeoAgentTopicDocumentRelation::getStatus, BusinessStatus.YES.getCode())
            .last("LIMIT 1"));
        if (relation == null) {
            relation = new NeoAgentTopicDocumentRelation();
            relation.setId(uidGenerator.getUid());
            relation.setTopicCode(topicCode);
            relation.setDocumentId(documentId);
            relation.setStatus(BusinessStatus.YES.getCode());
        }
        relation.setRelationScore(parseDecimal(dto.getRelationScore(), BigDecimal.ZERO));
        relation.setRelationSource(firstNonBlank(dto.getRelationSource(), "manual"));
        relation.setReason(safeText(dto.getReason()));
        if (relation.getCreateTime() == null) {
            topicDocumentRelationMapper.insert(relation);
        }
        else {
            topicDocumentRelationMapper.updateById(relation);
        }
        return toRelationVo(relation);
    }

    @Override
    public boolean removeTopicDocumentRelation(TopicDocumentRelationRemoveDto dto) {
        String topicCode = safeText(dto.getTopicCode());
        Long documentId = parseRequiredLong(dto.getDocumentId(), "documentId");
        if (topicCode.isBlank()) {
            throw new NeoAgentFrameException(BaseCode.PARAMETER_ERROR.getCode(), "topicCode 不能为空。");
        }
        return topicDocumentRelationMapper.update(null, new LambdaUpdateWrapper<NeoAgentTopicDocumentRelation>()
            .eq(NeoAgentTopicDocumentRelation::getTopicCode, topicCode)
            .eq(NeoAgentTopicDocumentRelation::getDocumentId, documentId)
            .eq(NeoAgentTopicDocumentRelation::getStatus, BusinessStatus.YES.getCode())
            .set(NeoAgentTopicDocumentRelation::getStatus, BusinessStatus.NO.getCode())) > 0;
    }

    @Override
    public KnowledgeRouteTracePageVo queryRouteTracePage(KnowledgeRouteTraceQueryDto dto) {
        int pageNo = parseInteger(dto == null ? null : dto.getPageNo(), 1);
        int pageSize = parseInteger(dto == null ? null : dto.getPageSize(), 20);
        String conversationId = dto == null ? "" : safeText(dto.getConversationId());
        String mode = dto == null ? "" : safeText(dto.getMode());
        String routeStatus = dto == null ? "" : safeText(dto.getRouteStatus());
        LambdaQueryWrapper<com.sopengin.neo.ai.manage.data.NeoAgentKnowledgeRouteTrace> wrapper =
            new LambdaQueryWrapper<com.sopengin.neo.ai.manage.data.NeoAgentKnowledgeRouteTrace>()
                .eq(com.sopengin.neo.ai.manage.data.NeoAgentKnowledgeRouteTrace::getStatus, BusinessStatus.YES.getCode())
                .orderByDesc(com.sopengin.neo.ai.manage.data.NeoAgentKnowledgeRouteTrace::getCreateTime,
                    com.sopengin.neo.ai.manage.data.NeoAgentKnowledgeRouteTrace::getId);
        if (StrUtil.isNotBlank(conversationId)) {
            wrapper.eq(com.sopengin.neo.ai.manage.data.NeoAgentKnowledgeRouteTrace::getConversationId, conversationId);
        }
        if (StrUtil.isNotBlank(mode)) {
            wrapper.eq(com.sopengin.neo.ai.manage.data.NeoAgentKnowledgeRouteTrace::getMode, mode);
        }
        if (StrUtil.isNotBlank(routeStatus)) {
            Integer parsedStatus = parseInteger(routeStatus, -1);
            if (parsedStatus > 0) {
                wrapper.eq(com.sopengin.neo.ai.manage.data.NeoAgentKnowledgeRouteTrace::getRouteStatus, parsedStatus);
            }
        }
        long total = knowledgeRouteTraceMapper.selectCount(wrapper);
        List<KnowledgeRouteTraceItemVo> records = knowledgeRouteTraceMapper.selectList(wrapper.last("LIMIT " + ((long) (pageNo - 1) * pageSize) + "," + pageSize))
            .stream()
            .map(item -> new KnowledgeRouteTraceItemVo(
                String.valueOf(item.getId()),
                safeText(item.getConversationId()),
                item.getExchangeId() == null ? "" : String.valueOf(item.getExchangeId()),
                safeText(item.getQuestion()),
                safeText(item.getRewriteQuestion()),
                safeText(item.getMode()),
                safeText(item.getTopScopesJson()),
                safeText(item.getTopTopicsJson()),
                safeText(item.getTopDocumentsJson()),
                item.getSelectedDocumentId() == null ? "" : String.valueOf(item.getSelectedDocumentId()),
                item.getHitSelectedDocument() == null ? "" : String.valueOf(item.getHitSelectedDocument()),
                item.getConfidence() == null ? "0.0000" : item.getConfidence().toPlainString(),
                item.getRouteStatus() == null ? "" : String.valueOf(item.getRouteStatus()),
                safeText(item.getErrorMsg()),
                item.getCreateTime() == null ? "" : String.valueOf(item.getCreateTime().getTime())
            ))
            .toList();
        long totalPages = total <= 0 ? 0 : (total + pageSize - 1) / pageSize;
        return new KnowledgeRouteTracePageVo(
            String.valueOf(pageNo),
            String.valueOf(pageSize),
            String.valueOf(total),
            String.valueOf(totalPages),
            records
        );
    }

    private void validateScope(KnowledgeScopeSaveDto dto) {
        if (dto == null || safeText(dto.getScopeCode()).isBlank() || safeText(dto.getScopeName()).isBlank()) {
            throw new NeoAgentFrameException(BaseCode.PARAMETER_ERROR.getCode(), "scopeCode 和 scopeName 不能为空。");
        }
    }

    private void validateTopic(KnowledgeTopicSaveDto dto) {
        if (dto == null || safeText(dto.getTopicCode()).isBlank() || safeText(dto.getTopicName()).isBlank() || safeText(dto.getScopeCode()).isBlank()) {
            throw new NeoAgentFrameException(BaseCode.PARAMETER_ERROR.getCode(), "topicCode、topicName、scopeCode 不能为空。");
        }
    }

    private KnowledgeScopeItemVo toScopeVo(NeoAgentKnowledgeScopeNode node) {
        return new KnowledgeScopeItemVo(
            String.valueOf(node.getId()),
            safeText(node.getScopeCode()),
            safeText(node.getScopeName()),
            safeText(node.getParentScopeCode()),
            safeText(node.getDescription()),
            safeText(node.getAliases()),
            safeText(node.getExamples()),
            String.valueOf(Optional.ofNullable(node.getSortOrder()).orElse(0))
        );
    }

    private KnowledgeTopicItemVo toTopicVo(NeoAgentKnowledgeTopicNode node) {
        return new KnowledgeTopicItemVo(
            String.valueOf(node.getId()),
            safeText(node.getTopicCode()),
            safeText(node.getTopicName()),
            safeText(node.getScopeCode()),
            safeText(node.getDescription()),
            safeText(node.getAliases()),
            safeText(node.getExamples()),
            safeText(node.getAnswerShape()),
            safeText(node.getExecutionPreference()),
            String.valueOf(Optional.ofNullable(node.getSortOrder()).orElse(0))
        );
    }

    private DocumentProfileVo toProfileVo(NeoAgentDocumentProfile profile) {
        return new DocumentProfileVo(
            String.valueOf(profile.getDocumentId()),
            safeText(profile.getDocumentSummary()),
            safeText(profile.getDocumentType()),
            safeText(profile.getCoreTopics()),
            safeText(profile.getExampleQuestions()),
            String.valueOf(Optional.ofNullable(profile.getGraphFriendly()).orElse(0)),
            String.valueOf(Optional.ofNullable(profile.getSupportsGraphOutline()).orElse(0)),
            String.valueOf(Optional.ofNullable(profile.getSupportsItemLookup()).orElse(0)),
            String.valueOf(Optional.ofNullable(profile.getSupportsGraphAssist()).orElse(0)),
            safeText(profile.getProfileSource()),
            String.valueOf(Optional.ofNullable(profile.getProfileStatus()).orElse(0)),
            safeText(profile.getErrorMsg())
        );
    }

    private TopicDocumentRelationItemVo toRelationVo(NeoAgentTopicDocumentRelation relation) {
        NeoAgentDocument document = documentMapper.selectById(relation.getDocumentId());
        return new TopicDocumentRelationItemVo(
            safeText(relation.getTopicCode()),
            String.valueOf(relation.getDocumentId()),
            document == null ? "" : safeText(document.getDocumentName()),
            document == null ? "" : safeText(document.getKnowledgeScopeCode()),
            document == null ? "" : safeText(document.getKnowledgeScopeName()),
            document == null ? "" : safeText(document.getBusinessCategory()),
            document == null ? "" : safeText(document.getDocumentTags()),
            relation.getRelationScore() == null ? "0.0000" : relation.getRelationScore().toPlainString(),
            safeText(relation.getRelationSource()),
            safeText(relation.getReason())
        );
    }

    private Long parseRequiredLong(String rawValue, String fieldName) {
        if (StrUtil.isBlank(rawValue)) {
            throw new NeoAgentFrameException(BaseCode.PARAMETER_ERROR.getCode(), fieldName + "不能为空。");
        }
        try {
            Long value = Long.valueOf(rawValue.trim());
            if (value <= 0) {
                throw new NumberFormatException("must be positive");
            }
            return value;
        }
        catch (NumberFormatException exception) {
            throw new NeoAgentFrameException(BaseCode.PARAMETER_ERROR.getCode(), fieldName + "格式非法。");
        }
    }

    private Integer parseInteger(String rawValue, Integer fallback) {
        if (StrUtil.isBlank(rawValue)) {
            return fallback;
        }
        try {
            return Integer.valueOf(rawValue.trim());
        }
        catch (NumberFormatException exception) {
            return fallback;
        }
    }

    private BigDecimal parseDecimal(String rawValue, BigDecimal fallback) {
        if (StrUtil.isBlank(rawValue)) {
            return fallback;
        }
        try {
            return new BigDecimal(rawValue.trim());
        }
        catch (NumberFormatException exception) {
            return fallback;
        }
    }

    private String safeText(String text) {
        return text == null ? "" : text.trim();
    }

    private String firstNonBlank(String primary, String fallback) {
        if (StrUtil.isNotBlank(primary)) {
            return primary.trim();
        }
        return StrUtil.blankToDefault(fallback, "");
    }
}
