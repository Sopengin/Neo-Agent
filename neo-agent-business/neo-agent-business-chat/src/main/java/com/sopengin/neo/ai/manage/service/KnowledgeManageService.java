package com.sopengin.neo.ai.manage.service;

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
import com.sopengin.neo.ai.manage.vo.DocumentProfileVo;
import com.sopengin.neo.ai.manage.vo.KnowledgeRouteTracePageVo;
import com.sopengin.neo.ai.manage.vo.KnowledgeScopeItemVo;
import com.sopengin.neo.ai.manage.vo.KnowledgeTopicItemVo;
import com.sopengin.neo.ai.manage.vo.TopicDocumentRelationItemVo;

import java.util.List;

/**
 * 服务层
 **/
public interface KnowledgeManageService {

    KnowledgeScopeItemVo saveScope(KnowledgeScopeSaveDto dto);

    boolean deleteScope(KnowledgeScopeDeleteDto dto);

    List<KnowledgeScopeItemVo> listScopes();

    KnowledgeTopicItemVo saveTopic(KnowledgeTopicSaveDto dto);

    boolean deleteTopic(KnowledgeTopicDeleteDto dto);

    List<KnowledgeTopicItemVo> listTopics(KnowledgeTopicQueryDto dto);

    DocumentProfileVo queryProfile(DocumentProfileDetailQueryDto dto);

    DocumentProfileVo regenerateProfile(DocumentProfileRegenerateDto dto);

    List<DocumentProfileVo> batchRegenerateProfiles(DocumentProfileBatchRegenerateDto dto);

    List<TopicDocumentRelationItemVo> listTopicDocuments(TopicDocumentRelationListQueryDto dto);

    TopicDocumentRelationItemVo saveTopicDocumentRelation(TopicDocumentRelationSaveDto dto);

    boolean removeTopicDocumentRelation(TopicDocumentRelationRemoveDto dto);

    KnowledgeRouteTracePageVo queryRouteTracePage(KnowledgeRouteTraceQueryDto dto);
}
