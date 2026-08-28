package com.sopengin.neo.ai.chatagent.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.sopengin.neo.ai.chatagent.model.trace.ConversationTraceStageView;

import java.util.List;

/**
 * 视图对象
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConversationExchangeDetailView {

    private String conversationId;

    private ConversationExchangeView exchange;

    private List<ConversationTraceStageView> stageTraces;
}
