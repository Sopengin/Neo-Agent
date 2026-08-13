package com.sopengin.neo.ai.chatagent.rag.retrieve.channel;

import com.sopengin.neo.ai.chatagent.rag.model.ConversationExecutionPlan;

/**
 * 检索通道抽象
 **/

public interface RetrievalChannel {

    String channelName();

    boolean supports(ConversationExecutionPlan plan);

    RetrievalChannelResult retrieve(String subQuestion, ConversationExecutionPlan plan);
}
