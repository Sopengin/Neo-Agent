package com.sopengin.neo.ai.chatagent.service;

import cn.hutool.core.util.StrUtil;
import com.baidu.fsg.uid.UidGenerator;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import jakarta.annotation.Resource;
import com.sopengin.neo.ai.chatagent.data.NeoAgentChatChannelExecution;
import com.sopengin.neo.ai.chatagent.data.NeoAgentChatRetrievalResult;
import com.sopengin.neo.ai.chatagent.mapper.NeoAgentChatChannelExecutionMapper;
import com.sopengin.neo.ai.chatagent.mapper.NeoAgentChatRetrievalResultMapper;
import com.sopengin.neo.ai.chatagent.model.ChannelExecutionView;
import com.sopengin.neo.ai.chatagent.model.RetrievalResultView;
import com.sopengin.neo.enums.BusinessStatus;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;
import java.util.List;

/**
 * 服务层
 **/

@Repository
public class MybatisRetrievalObserveStore implements RetrievalObserveStore {

    private final NeoAgentChatRetrievalResultMapper retrievalResultMapper;
    private final NeoAgentChatChannelExecutionMapper channelExecutionMapper;

    @Resource
    private UidGenerator uidGenerator;

    public MybatisRetrievalObserveStore(NeoAgentChatRetrievalResultMapper retrievalResultMapper,
                                        NeoAgentChatChannelExecutionMapper channelExecutionMapper) {
        this.retrievalResultMapper = retrievalResultMapper;
        this.channelExecutionMapper = channelExecutionMapper;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void batchSaveResults(String conversationId, long exchangeId, List<RetrievalResultView> results) {
        if (results == null || results.isEmpty()) {
            return;
        }
        for (RetrievalResultView view : results) {
            NeoAgentChatRetrievalResult entity = new NeoAgentChatRetrievalResult();
            entity.setId(uidGenerator.getUid());
            entity.setConversationId(conversationId);
            entity.setExchangeId(exchangeId);
            entity.setTraceId(view.getTraceId());
            entity.setSubQuestionIndex(view.getSubQuestionIndex());
            entity.setSubQuestion(view.getSubQuestion());
            entity.setChannelType(view.getChannelType());
            entity.setChannelRank(view.getChannelRank());
            entity.setRrfRank(view.getRrfRank());
            entity.setFinalRank(view.getFinalRank());
            entity.setOriginalScore(view.getOriginalScore());
            entity.setRrfScore(view.getRrfScore());
            entity.setRerankScore(view.getRerankScore());
            entity.setGatePassed(view.isGatePassed() ? 1 : 0);
            entity.setIsElevated(view.isElevated() ? 1 : 0);
            entity.setIsSelected(view.isSelected() ? 1 : 0);
            entity.setSelectionReason(view.getSelectionReason());
            entity.setDocumentId(view.getDocumentId());
            entity.setDocumentName(view.getDocumentName());
            entity.setChunkId(view.getChunkId());
            entity.setChunkNo(view.getChunkNo());
            entity.setParentBlockId(view.getParentBlockId());
            entity.setParentBlockNo(view.getParentBlockNo());
            entity.setSectionPath(view.getSectionPath());
            entity.setChunkTextPreview(view.getChunkTextPreview());
            entity.setChunkCharCount(view.getChunkCharCount());
            entity.setStatus(BusinessStatus.YES.getCode());
            retrievalResultMapper.insert(entity);
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void batchSaveChannelExecutions(String conversationId, long exchangeId, List<ChannelExecutionView> executions) {
        if (executions == null || executions.isEmpty()) {
            return;
        }
        for (ChannelExecutionView view : executions) {
            NeoAgentChatChannelExecution entity = new NeoAgentChatChannelExecution();
            entity.setId(uidGenerator.getUid());
            entity.setConversationId(conversationId);
            entity.setExchangeId(exchangeId);
            entity.setTraceId(view.getTraceId());
            entity.setSubQuestionIndex(view.getSubQuestionIndex());
            entity.setSubQuestion(view.getSubQuestion());
            entity.setChannelType(view.getChannelType());
            entity.setExecutionState(view.getExecutionState());
            entity.setStartTime(view.getStartTime() == null ? null : Date.from(view.getStartTime()));
            entity.setEndTime(view.getEndTime() == null ? null : Date.from(view.getEndTime()));
            entity.setDurationMs(view.getDurationMs());
            entity.setRecalledCount(view.getRecalledCount());
            entity.setAcceptedCount(view.getAcceptedCount());
            entity.setFinalSelectedCount(view.getFinalSelectedCount());
            entity.setAvgScore(view.getAvgScore());
            entity.setMaxScore(view.getMaxScore());
            entity.setMinScore(view.getMinScore());
            entity.setErrorMessage(view.getErrorMessage());
            entity.setStatus(BusinessStatus.YES.getCode());
            channelExecutionMapper.insert(entity);
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<RetrievalResultView> listResults(String conversationId, long exchangeId) {
        return retrievalResultMapper.selectList(
                new LambdaQueryWrapper<NeoAgentChatRetrievalResult>()
                    .eq(NeoAgentChatRetrievalResult::getConversationId, conversationId)
                    .eq(NeoAgentChatRetrievalResult::getExchangeId, exchangeId)
                    .orderByAsc(NeoAgentChatRetrievalResult::getSubQuestionIndex)
                    .orderByAsc(NeoAgentChatRetrievalResult::getFinalRank)
            )
            .stream()
            .map(this::toResultView)
            .toList();
    }

    @Override
    @Transactional(readOnly = true)
    public List<ChannelExecutionView> listChannelExecutions(String conversationId, long exchangeId) {
        return channelExecutionMapper.selectList(
                new LambdaQueryWrapper<NeoAgentChatChannelExecution>()
                    .eq(NeoAgentChatChannelExecution::getConversationId, conversationId)
                    .eq(NeoAgentChatChannelExecution::getExchangeId, exchangeId)
                    .orderByAsc(NeoAgentChatChannelExecution::getSubQuestionIndex)
                    .orderByAsc(NeoAgentChatChannelExecution::getStartTime)
            )
            .stream()
            .map(this::toExecutionView)
            .toList();
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void deleteByConversation(String conversationId) {
        retrievalResultMapper.delete(
            new LambdaQueryWrapper<NeoAgentChatRetrievalResult>()
                .eq(NeoAgentChatRetrievalResult::getConversationId, conversationId)
        );
        channelExecutionMapper.delete(
            new LambdaQueryWrapper<NeoAgentChatChannelExecution>()
                .eq(NeoAgentChatChannelExecution::getConversationId, conversationId)
        );
    }

    private RetrievalResultView toResultView(NeoAgentChatRetrievalResult entity) {
        return new RetrievalResultView(
            entity.getId(),
            StrUtil.blankToDefault(entity.getTraceId(), ""),
            entity.getSubQuestionIndex() == null ? 1 : entity.getSubQuestionIndex(),
            StrUtil.blankToDefault(entity.getSubQuestion(), ""),
            StrUtil.blankToDefault(entity.getChannelType(), ""),
            entity.getChannelRank(),
            entity.getRrfRank(),
            entity.getFinalRank(),
            entity.getOriginalScore(),
            entity.getRrfScore(),
            entity.getRerankScore(),
            entity.getGatePassed() != null && entity.getGatePassed() == 1,
            entity.getIsElevated() != null && entity.getIsElevated() == 1,
            entity.getIsSelected() != null && entity.getIsSelected() == 1,
            StrUtil.blankToDefault(entity.getSelectionReason(), ""),
            entity.getDocumentId(),
            StrUtil.blankToDefault(entity.getDocumentName(), ""),
            entity.getChunkId(),
            entity.getChunkNo(),
            entity.getParentBlockId(),
            entity.getParentBlockNo(),
            StrUtil.blankToDefault(entity.getSectionPath(), ""),
            StrUtil.blankToDefault(entity.getChunkTextPreview(), ""),
            entity.getChunkCharCount(),
            toInstant(entity.getCreateTime())
        );
    }

    private ChannelExecutionView toExecutionView(NeoAgentChatChannelExecution entity) {
        return new ChannelExecutionView(
            entity.getId(),
            StrUtil.blankToDefault(entity.getTraceId(), ""),
            entity.getSubQuestionIndex() == null ? 1 : entity.getSubQuestionIndex(),
            StrUtil.blankToDefault(entity.getSubQuestion(), ""),
            StrUtil.blankToDefault(entity.getChannelType(), ""),
            entity.getExecutionState() == null ? 1 : entity.getExecutionState(),
            toInstant(entity.getStartTime()),
            toInstant(entity.getEndTime()),
            entity.getDurationMs(),
            entity.getRecalledCount() == null ? 0 : entity.getRecalledCount(),
            entity.getAcceptedCount() == null ? 0 : entity.getAcceptedCount(),
            entity.getFinalSelectedCount() == null ? 0 : entity.getFinalSelectedCount(),
            entity.getAvgScore(),
            entity.getMaxScore(),
            entity.getMinScore(),
            StrUtil.blankToDefault(entity.getErrorMessage(), ""),
            toInstant(entity.getCreateTime())
        );
    }

    private Instant toInstant(Date value) {
        return value == null ? null : value.toInstant();
    }
}
