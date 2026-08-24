package com.sopengin.neo.ai.chatagent.rag.executor;

import com.sopengin.neo.ai.chatagent.rag.model.ExecutionMode;
import com.sopengin.neo.ai.chatagent.service.TaskInfo;
import reactor.core.publisher.Flux;

/**
 * 统一对话执行器抽象
 **/

public interface ConversationExecutor {

    ExecutionMode mode();

    Flux<String> execute(TaskInfo taskInfo);
}
