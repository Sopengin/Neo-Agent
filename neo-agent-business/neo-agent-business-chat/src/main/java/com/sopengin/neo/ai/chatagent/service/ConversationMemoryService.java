package com.sopengin.neo.ai.chatagent.service;

import com.sopengin.neo.ai.chatagent.model.ConversationMemorySummaryView;
import com.sopengin.neo.ai.chatagent.model.memory.ConversationMemoryContext;

/**
 * 服务层
 **/

public interface ConversationMemoryService {

    ConversationMemoryContext loadMemoryContext(String conversationId);

    default ConversationMemoryContext loadMemoryContext(String conversationId, ConversationTraceRecorder traceRecorder) {
        return loadMemoryContext(conversationId);
    }

    void refreshConversationSummaryAsync(String conversationId);

    ConversationMemorySummaryView getConversationSummary(String conversationId);

    ConversationMemorySummaryView rebuildConversationSummary(String conversationId);

    void deleteConversationSummary(String conversationId);
}
