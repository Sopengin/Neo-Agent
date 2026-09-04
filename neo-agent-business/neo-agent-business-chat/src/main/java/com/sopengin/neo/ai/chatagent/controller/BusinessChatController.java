package com.sopengin.neo.ai.chatagent.controller;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import com.sopengin.neo.ai.chatagent.dto.ChatRequestDto;
import com.sopengin.neo.ai.chatagent.dto.ConversationExchangeDetailQueryDto;
import com.sopengin.neo.ai.chatagent.dto.ConversationIdentityDto;
import com.sopengin.neo.ai.chatagent.dto.ConversationSessionListQueryDto;
import com.sopengin.neo.ai.chatagent.dto.RetrievalObserveQueryDto;
import com.sopengin.neo.ai.chatagent.model.ChannelExecutionView;
import com.sopengin.neo.ai.chatagent.model.ConversationExchangeDetailView;
import com.sopengin.neo.ai.chatagent.model.ConversationMemorySummaryView;
import com.sopengin.neo.ai.chatagent.model.ConversationSessionView;
import com.sopengin.neo.ai.chatagent.model.KnowledgeDocumentOptionView;
import com.sopengin.neo.ai.chatagent.model.RetrievalResultView;
import com.sopengin.neo.ai.chatagent.model.StageBenchmarkView;
import com.sopengin.neo.ai.chatagent.service.BusinessChatService;
import com.sopengin.neo.ai.chatagent.vo.ConversationResetVo;
import com.sopengin.neo.ai.chatagent.vo.ConversationSessionListVo;
import com.sopengin.neo.ai.chatagent.vo.ConversationStopVo;
import com.sopengin.neo.common.ApiResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * 控制层
 **/
@AllArgsConstructor
@RestController
@RequestMapping("/api/chat")
public class BusinessChatController {

    private final BusinessChatService businessChatService;

    /**
     * 对话流式入口。
     *
     * <p>produces 声明为 text/event-stream、返回类型是 Flux&lt;String&gt;：
     * 返回的是"一条数据流"而不是"一个结果"，由 Spring WebFlux 以 SSE 形式边产生边推送给浏览器。</p>
     *
     * <p>注意：调用 openConversationStream 时并不会立刻执行业务逻辑，
     * 真正的执行发生在客户端订阅这条 Flux 之后（详见 BusinessChatService#openConversationStream）。</p>
     */
    @PostMapping(value = "/stream", produces = "text/event-stream;charset=UTF-8")
    public Flux<String> stream(@Valid @RequestBody ChatRequestDto dto) {
        // 交给服务层；此处拿到的是一条惰性流，尚未抢锁、落库或执行
        return businessChatService.openConversationStream(dto);
    }

    @PostMapping("/document/options")
    public ApiResponse<List<KnowledgeDocumentOptionView>> documentOptions() {
        return ApiResponse.ok(businessChatService.listKnowledgeDocumentOptions());
    }

    @PostMapping("/session/stop")
    public ApiResponse<ConversationStopVo> stop(@Valid @RequestBody ConversationIdentityDto dto) {
        return ApiResponse.ok(businessChatService.stopConversation(dto.getConversationId()));
    }

    @PostMapping("/session/detail")
    public ApiResponse<ConversationSessionView> session(@Valid @RequestBody ConversationIdentityDto dto) {
        return ApiResponse.ok(businessChatService.getSession(dto.getConversationId()));
    }

    @PostMapping("/exchange/detail")
    public ApiResponse<ConversationExchangeDetailView> exchange(@Valid @RequestBody ConversationExchangeDetailQueryDto dto) {
        return ApiResponse.ok(businessChatService.getExchangeDetail(dto.getConversationId(), dto.getExchangeId()));
    }

    @PostMapping("/session/list")
    public ApiResponse<ConversationSessionListVo> sessions(@RequestBody(required = false) ConversationSessionListQueryDto dto) {
        return ApiResponse.ok(businessChatService.listSessions(dto));
    }

    @PostMapping("/session/reset")
    public ApiResponse<ConversationResetVo> reset(@Valid @RequestBody ConversationIdentityDto dto) {
        return ApiResponse.ok(businessChatService.resetConversation(dto.getConversationId()));
    }

    @PostMapping("/session/summary/rebuild")
    public ApiResponse<ConversationMemorySummaryView> rebuildSummary(@Valid @RequestBody ConversationIdentityDto dto) {
        return ApiResponse.ok(businessChatService.rebuildConversationSummary(dto.getConversationId()));
    }

    @PostMapping("/exchange/retrieval/results")
    public ApiResponse<List<RetrievalResultView>> retrievalResults(@Valid @RequestBody RetrievalObserveQueryDto dto) {
        return ApiResponse.ok(businessChatService.getRetrievalResults(dto.getConversationId(), Long.parseLong(dto.getExchangeId())));
    }

    @PostMapping("/exchange/channel/executions")
    public ApiResponse<List<ChannelExecutionView>> channelExecutions(@Valid @RequestBody RetrievalObserveQueryDto dto) {
        return ApiResponse.ok(businessChatService.getChannelExecutions(dto.getConversationId(), Long.parseLong(dto.getExchangeId())));
    }

    @PostMapping("/stage/benchmarks")
    public ApiResponse<List<StageBenchmarkView>> stageBenchmarks() {
        return ApiResponse.ok(businessChatService.getStageBenchmarks());
    }
}
