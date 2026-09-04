package com.sopengin.neo.ai.chatagent.service;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.StrUtil;
import com.alibaba.cloud.ai.graph.RunnableConfig;
import com.alibaba.cloud.ai.graph.agent.ReactAgent;
import com.alibaba.cloud.ai.graph.checkpoint.Checkpoint;
import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.sopengin.neo.ai.chatagent.config.ChatAgentProperties;
import com.sopengin.neo.ai.chatagent.dto.ChatRequestDto;
import com.sopengin.neo.ai.chatagent.dto.ConversationSessionListQueryDto;
import com.sopengin.neo.ai.chatagent.model.ConversationExchangeDetailView;
import com.sopengin.neo.ai.chatagent.model.ConversationExchangeView;
import com.sopengin.neo.ai.chatagent.model.KnowledgeDocumentOptionView;
import com.sopengin.neo.ai.chatagent.model.ConversationMemorySummaryView;
import com.sopengin.neo.ai.chatagent.model.ConversationSessionView;
import com.sopengin.neo.ai.chatagent.model.ChannelExecutionView;
import com.sopengin.neo.ai.chatagent.model.RetrievalResultView;
import com.sopengin.neo.ai.chatagent.model.StageBenchmarkView;
import com.sopengin.neo.ai.chatagent.model.SearchReference;
import com.sopengin.neo.ai.chatagent.model.debug.ChatDebugTrace;
import com.sopengin.neo.ai.chatagent.rag.executor.ConversationExecutor;
import com.sopengin.neo.ai.chatagent.rag.executor.ConversationExecutorRegistry;
import com.sopengin.neo.ai.chatagent.rag.model.ConversationExecutionPlan;
import com.sopengin.neo.ai.manage.model.KnowledgeDocumentDescriptor;
import com.sopengin.neo.ai.manage.service.DocumentKnowledgeService;
import com.sopengin.neo.ai.chatagent.rag.service.ChatPreparationOrchestrator;
import com.sopengin.neo.ai.chatagent.support.ChatContextKeys;
import com.sopengin.neo.ai.chatagent.support.SinkEmitHelper;
import com.sopengin.neo.ai.chatagent.support.StreamEventMetadata;
import com.sopengin.neo.ai.chatagent.support.StreamEventWriter;
import com.sopengin.neo.ai.chatagent.vo.ConversationResetVo;
import com.sopengin.neo.ai.chatagent.vo.ConversationSessionListVo;
import com.sopengin.neo.ai.chatagent.vo.ConversationStopVo;
import com.sopengin.neo.ai.prompt.PromptTemplateNames;
import com.sopengin.neo.ai.prompt.PromptTemplateService;
import com.sopengin.neo.enums.ChatTurnStatus;
import com.sopengin.neo.enums.ChatQueryMode;
import com.sopengin.neo.exception.NeoAgentFrameException;
import com.sopengin.neo.lease.RedisLeaseManager;
import org.springframework.ai.chat.messages.AbstractMessage;
import org.springframework.ai.chat.messages.MessageType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 对话主链路总调度。
 *
 * <p>一条用户消息的完整生命周期：</p>
 * <ol>
 *   <li>{@link #openConversationStream} 惰性返回流，此时不执行任何业务逻辑</li>
 *   <li>客户端订阅后进入 {@link #openDeferredConversationStream}，开始启动引导</li>
 *   <li>{@code buildLaunchPlan} 参数标准化 + 生成租约标识</li>
 *   <li>{@link #claimConversationLease} 抢 Redis 租约（跨实例防重第一道）</li>
 *   <li>{@link #bootstrapConversation} 落库占位 + 建 TaskInfo + JVM 注册（防重第二道）+ 绑 SSE 通道</li>
 *   <li>{@link #activateGeneration} 启动租约续期，并订阅执行流</li>
 *   <li>{@link #buildConversationExecution} 编排决策 → 分发执行器 → 流式输出</li>
 *   <li>{@code finishSuccessfully / finishWithFailure / stopTask} 收尾并统一清理资源</li>
 * </ol>
 **/

@Slf4j
@AllArgsConstructor
@Service
public class BusinessChatService {

    private static final ZoneId CHAT_ZONE_ID = ZoneId.of("Asia/Shanghai");

    /** 租约 key 前缀：最终形态 chat:running:{conversationId}，一个会话一把锁。 */
    private static final String CHAT_RUNNING_LEASE_PREFIX = "chat:running:";

    /** 租约 TTL：到期自动失效，避免持锁进程崩溃后形成死锁。 */
    private static final Duration CHAT_RUNNING_LEASE_TTL = Duration.ofSeconds(30);

    /** 续期间隔：按 1/3 TTL 的频率续期，保证任务存活期间锁不会过期被他人抢占。 */
    private static final Duration CHAT_RUNNING_LEASE_RENEW_INTERVAL = Duration.ofSeconds(10);

    private final ReactAgent businessChatReactAgent;
    private final ChatCheckpointManager checkpointManager;
    private final ChatAgentProperties chatAgentProperties;
    private final ConversationArchiveStore conversationArchiveStore;
    private final ChatRuntimeRegistry chatRuntimeRegistry;
    private final RecommendationService recommendationService;
    private final StreamEventWriter streamEventWriter;
    private final RedisLeaseManager redisLeaseManager;
    private final ChatPreparationOrchestrator chatPreparationOrchestrator;
    private final ConversationExecutorRegistry conversationExecutorRegistry;
    private final ConversationMemoryService conversationMemoryService;
    private final DocumentKnowledgeService documentKnowledgeService;
    private final ConversationTraceStageStore conversationTraceStageStore;
    private final RetrievalObserveStore retrievalObserveStore;
    private final StageBenchmarkService stageBenchmarkService;
    private final PromptTemplateService promptTemplateService;

    /**
     * 对话入口。
     *
     * <p>必须用 Flux.defer 包一层：openDeferredConversationStream 内部有副作用
     * （抢 Redis 租约、写数据库、注册 JVM 任务）。若直接返回它的执行结果，这些副作用会在
     * "方法被调用时"立即发生；而此刻客户端还没订阅，一旦连接建立失败就会占着资源不放，造成锁泄漏。
     * 用 defer 把整段逻辑推迟到订阅时刻执行，使"资源占用"与"客户端订阅"严格对齐。</p>
     */
    public Flux<String> openConversationStream(ChatRequestDto request) {

        return Flux.defer(() -> openDeferredConversationStream(request));
    }

    /**
     * 启动引导：把"一个请求"转成"一条可执行的流"。
     *
     * <p>无论成功还是失败都返回 Flux（成功是数据流、失败是只含一个 error 事件的流），
     * 不把异常抛给上层；同时严格保证锁的"获取-释放"守恒。</p>
     */
    private Flux<String> openDeferredConversationStream(ChatRequestDto request) {

        log.info("======request内容：{}", JSON.toJSONString(request));

        // 声明在 try 外：catch 要据此判断"当前持有哪些资源"，才能正确回滚
        StreamLaunchPlan launchPlan = null;
        boolean leaseClaimed = false;
        try {

            // ① 参数标准化：校验 question / chatMode / 所选文档，并生成租约 key 与 ownerToken。
            //    校验失败会抛异常，此时尚未抢锁，catch 中无需回滚
            launchPlan = buildLaunchPlan(request);

            // ② 抢 Redis 租约（防重第一道，跨实例）：key = chat:running:{conversationId}
            leaseClaimed = claimConversationLease(launchPlan);
            if (!leaseClaimed) {
                // 同一会话已有请求在执行 → 直接返回错误流，不抛异常
                return rejectionFlux("该会话当前正在执行中，请稍后再试", launchPlan.getConversationId(), null);
            }

            // ③ 引导启动：落库占位 + 组装 TaskInfo + JVM 注册 + 绑 SSE 通道
            BootstrapResult bootstrapResult = bootstrapConversation(launchPlan);
            if (StrUtil.isNotBlank(bootstrapResult.getRejectionMessage())) {
                // 引导内部已自行回滚资源（如还锁），这里只需返回拒绝原因
                return rejectionFlux(bootstrapResult.getRejectionMessage(), launchPlan.getConversationId(), null);
            }

            // ④ 返回出站流：此刻仍未执行，等客户端订阅后才真正开跑
            return bootstrapResult.getOutbound();
        }
        catch (RuntimeException exception) {
            log.error("会话启动失败, conversationId={}, question={}",
                launchPlan == null ? "" : launchPlan.getConversationId(),
                request.getQuestion(),
                exception);

            // 资源守恒：只有"确实抢到过锁"才需要还锁。
            // 若在校验阶段就失败（leaseClaimed=false），此时去释放反而可能误删他人的锁
            if (leaseClaimed && launchPlan != null) {
                releaseLeaseQuietly(launchPlan.getLeaseKey(), launchPlan.getLeaseOwnerToken());
            }
            // 失败同样返回 Flux（只含一个 error 事件），不向上抛异常
            return rejectionFlux(
                buildErrorMessage(exception),
                launchPlan == null ? null : launchPlan.getConversationId(),
                null
            );
        }
    }

    /**
     * 引导启动（bootstrap）：为真正执行做准备工作，本身不执行对话。
     *
     * <p>四步：落库占位 → 组装 TaskInfo → JVM 注册（防重第二道）→ 绑定 SSE 通道。</p>
     */
    private BootstrapResult bootstrapConversation(StreamLaunchPlan launchPlan) {

        ConversationExchangeView exchangeView = null;
        try {

            // ① 落库占位：写 dialogue（upsert，状态 RUNNING）+ exchange（insert，答案为空）
            //    作用一：拿到 exchangeId，后续 TaskInfo / Trace / 结果回填都靠它关联
            //    作用二：先占位后回填，让"进行中"对前端与观测面板可见，失败也能留痕
            exchangeView = conversationArchiveStore.startExchange(
                launchPlan.getConversationId(),
                launchPlan.getQuestion(),
                launchPlan.getChatMode(),
                launchPlan.getSelectedDocumentId(),
                launchPlan.getSelectedDocumentName()
            );

            // ② 组装"任务工作台"：聚合身份、锁、SSE 通道、结果累积器与生命周期控制器
            TaskInfo taskInfo = createTaskInfo(launchPlan, exchangeView);

            // ③ JVM 注册表（防重第二道，进程内）：putIfAbsent 返回 null 表示注册成功
            if (!chatRuntimeRegistry.register(taskInfo)) {

                // 注册失败：把已落库的占位记录标记为 FAILED
                failBootstrappedExchange(launchPlan.getConversationId(), exchangeView.getExchangeId(), "该会话当前正在执行中，请稍后再试");

                // 关键：必须回滚上一步已获取的 Redis 租约，否则 30s 内该会话都无法再次进入
                releaseLeaseQuietly(launchPlan.getLeaseKey(), launchPlan.getLeaseOwnerToken());
                return BootstrapResult.rejected("该会话当前正在执行中，请稍后再试");
            }

            // ④ 绑定 SSE 通道，返回一条"订阅即执行"的流
            return BootstrapResult.ready(bindClientChannel(taskInfo));
        }
        catch (RuntimeException exception) {

            // 异常路径同样要还锁 + 把占位记录置为 FAILED，保证不留孤儿资源
            releaseLeaseQuietly(launchPlan.getLeaseKey(), launchPlan.getLeaseOwnerToken());
            if (exchangeView != null) {

                failBootstrappedExchange(launchPlan.getConversationId(), exchangeView.getExchangeId(), buildErrorMessage(exception));
            }
            return BootstrapResult.rejected(buildErrorMessage(exception));
        }
    }

    /**
     * 组装"任务工作台"。
     *
     * <p>把这一轮对话需要的所有东西聚合到一个对象里随响应式链流动，避免几十个参数层层传递。
     * 注意响应式执行会跨线程（boundedElastic），因此不能用 ThreadLocal 承载上下文。</p>
     */
    private TaskInfo createTaskInfo(StreamLaunchPlan launchPlan, ConversationExchangeView exchangeView) {

        // SSE 通道的"写端"：unicast 单播（一个会话一条流）+ 缓冲背压。
        // 编排层、执行器、收尾逻辑都通过它往同一条流里写事件
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();

        // ReAct Agent 的会话配置，threadId = conversationId，用于 Checkpoint 持久化
        RunnableConfig runnableConfig = buildSessionConfig(launchPlan.getConversationId());

        // 结果累积器：流式输出"过手即走"，必须边流边攒才能最后落库；
        // 因执行发生在多线程，容器必须是线程安全的
        List<String> thinkingSteps = Collections.synchronizedList(new ArrayList<>());
        List<SearchReference> references = Collections.synchronizedList(new ArrayList<>());
        Set<String> usedTools = ConcurrentHashMap.newKeySet();
        // 全链路追踪 ID（去掉横线便于展示）
        String traceId = UUID.randomUUID().toString().replace("-", "");
        ConversationTraceRecorder traceRecorder = new ConversationTraceRecorder(
            conversationTraceStageStore,
            retrievalObserveStore,
            launchPlan.getConversationId(),
            exchangeView.getExchangeId(),
            traceId
        );
        StreamEventMetadata eventMetadata = new StreamEventMetadata(
            launchPlan.getConversationId(),
            exchangeView.getExchangeId()
        );

        // 把上下文塞进 Agent 的 context，供执行器内部（如 ReAct 工具回调）读取
        runnableConfig.context().put(ChatContextKeys.EVENT_SINK, sink);
        runnableConfig.context().put(ChatContextKeys.EVENT_METADATA, eventMetadata);
        runnableConfig.context().put(ChatContextKeys.THINKING_STEPS, thinkingSteps);
        runnableConfig.context().put(ChatContextKeys.REFERENCES, references);
        runnableConfig.context().put(ChatContextKeys.USED_TOOLS, usedTools);
        runnableConfig.context().put(ChatContextKeys.TRACE_ID, traceId);

        runnableConfig.context().put(ChatContextKeys.QUESTION, launchPlan.getQuestion());

        runnableConfig.context().put(ChatContextKeys.CHAT_MODE, launchPlan.getChatMode().name());

        runnableConfig.context().put(ChatContextKeys.CURRENT_DATE, launchPlan.getCurrentDate().toString());
        runnableConfig.context().put(ChatContextKeys.CURRENT_DATE_TEXT, launchPlan.getCurrentDateText());

        putContextIfNotNull(runnableConfig, ChatContextKeys.SELECTED_DOCUMENT_ID, launchPlan.getSelectedDocumentId());
        putContextIfNotBlank(runnableConfig, ChatContextKeys.SELECTED_DOCUMENT_NAME, launchPlan.getSelectedDocumentName());
        putContextIfNotNull(runnableConfig, ChatContextKeys.SELECTED_TASK_ID, launchPlan.getSelectedTaskId());

        ChatDebugTrace debugTrace = initializeDebugTrace(null);
        runnableConfig.context().put(ChatContextKeys.DEBUG_TRACE, debugTrace);

        // executionPlan 此处传 null —— 它要等编排器 prepare 之后才被 set 进工作台（故声明为 volatile）
        return new TaskInfo(
            launchPlan.getConversationId(),
            exchangeView.getExchangeId(),
            launchPlan.getQuestion(),
            launchPlan.getChatMode(),
            traceId,
            launchPlan.getSelectedDocumentId(),
            launchPlan.getSelectedDocumentName(),
            launchPlan.getSelectedTaskId(),
            launchPlan.getCurrentDate(),
            launchPlan.getCurrentDateText(),
            null,
            debugTrace,
            runnableConfig,
            traceRecorder,
            sink,
            eventMetadata,
            launchPlan.getLeaseKey(),
            launchPlan.getLeaseOwnerToken(),
            thinkingSteps,
            references,
            usedTools,
            System.currentTimeMillis()
        );
    }

    /**
     * 绑定 SSE 通道：把 Sink（写端）转成 Flux（读端），并把任务生命周期挂到订阅生命周期上。
     */
    private Flux<String> bindClientChannel(TaskInfo taskInfo) {

        return taskInfo.sink().asFlux()

            // 订阅即执行：客户端连上 SSE 才真正开跑（惰性）
            .doOnSubscribe(ignored -> activateGeneration(taskInfo))

            // 断开即停止：客户端关页面 / 点停止 / 断网 → 中断生成并释放资源
            .doOnCancel(() -> stopTask(taskInfo, "客户端已取消请求"));
    }

    /**
     * 激活执行：启动租约续期，并订阅执行流。
     *
     * <p>启动过程不是原子的，中途随时可能被 stopTask 并发打断，
     * 因此每完成一步都要用 finalized 复核一次并及时止损。</p>
     */
    private void activateGeneration(TaskInfo taskInfo) {
        try {
            // 检查①：进来时可能已被停止（停止线程先跑完了）
            if (taskInfo.finalized().get()) {
                return;
            }

            // 启动租约续期：对话耗时不确定，靠心跳续期防止 30s TTL 到期后锁被他人抢占
            Disposable leaseRenewalDisposable = startLeaseRenewal(taskInfo);
            taskInfo.setLeaseRenewalDisposable(leaseRenewalDisposable);
            // 检查②：续期刚启动就被停了 → 把续期任务也停掉
            if (taskInfo.finalized().get() && !leaseRenewalDisposable.isDisposed()) {
                leaseRenewalDisposable.dispose();
                return;
            }

            // 订阅执行流，真正开始编排与执行
            Disposable disposable = buildConversationExecution(taskInfo).subscribe();

            taskInfo.setDisposable(disposable);
            // 检查③：执行流刚订阅就被停了 → dispose 掉
            if (taskInfo.finalized().get() && !disposable.isDisposed()) {
                disposable.dispose();
            }
        }
        catch (RuntimeException exception) {

            finishWithFailure(taskInfo, exception);
        }
    }

    /**
     * 执行链：发首个思考事件 → 编排决策 → 分发执行器 → 逐片包事件 → 挂三条收尾路径。
     */
    private Flux<String> buildConversationExecution(TaskInfo taskInfo) {
        return Flux.defer(() -> {

                // 先给前端一个即时反馈，避免"点了没反应"
                safeEmit(taskInfo.sink(), streamEventWriter.thinking("正在分析问题上下文。", taskInfo.eventMetadata()));

                // 关键：编排决策属于阻塞操作（要调模型、查库），必须切到 boundedElastic 线程池，
                // 不能占用事件循环线程，否则会阻塞整条响应式链路
                return Mono.fromCallable(() -> prepareExecutionPlan(taskInfo))
                    .subscribeOn(Schedulers.boundedElastic())
                    .flatMapMany(plan -> {

                        // 策略模式 + 注册表：按执行模式（RETRIEVAL / REACT_AGENT / GRAPH_ONLY ...）取执行器
                        ConversationExecutor executor = conversationExecutorRegistry.get(plan.getMode());
                        // 执行器返回正文流（Flux<String>）；思考 / 状态事件由执行器内部旁路写入 sink
                        return executor.execute(taskInfo);
                    });
            })
            .publishOn(Schedulers.boundedElastic())

            // 每个正文分片：累积完整答案 + 记录首字耗时 + 包成 text 事件写入事件总线
            .doOnNext(chunk -> emitModelChunk(taskInfo, chunk))
            // 三条收尾路径，靠 finalized 的 CAS 保证只走一条
            .doOnError(error -> finishWithFailure(taskInfo, error))
            .doOnComplete(() -> finishSuccessfully(taskInfo));
    }

    private StreamLaunchPlan buildLaunchPlan(ChatRequestDto request) {

        String question = normalizeQuestion(request.getQuestion());

        String conversationId = normalizeConversationId(request.getConversationId());
        ChatQueryMode chatMode = parseRequiredChatMode(request.getChatMode());

        KnowledgeDocumentDescriptor selectedDocument = resolveSelectedDocument(chatMode, request.getSelectedDocumentId());

        LocalDate currentDate = LocalDate.now(CHAT_ZONE_ID);
        String currentDateText = formatCurrentDate(currentDate);
        return new StreamLaunchPlan(
            question,
            conversationId,
            chatMode,
            selectedDocument == null ? null : selectedDocument.getDocumentId(),
            selectedDocument == null ? "" : selectedDocument.getDocumentName(),
            selectedDocument == null ? null : selectedDocument.getLastIndexTaskId(),

            buildChatLeaseKey(conversationId),

            UUID.randomUUID().toString(),
            currentDate,
            currentDateText
        );
    }

    private boolean claimConversationLease(StreamLaunchPlan launchPlan) {

        return redisLeaseManager.acquire(
            launchPlan.getLeaseKey(),
            launchPlan.getLeaseOwnerToken(),
            CHAT_RUNNING_LEASE_TTL
        );
    }

    private void failBootstrappedExchange(String conversationId, long exchangeId, String errorMessage) {

        conversationArchiveStore.completeExchange(
            conversationId,
            exchangeId,
            "",
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            null,
            ChatTurnStatus.FAILED,
            errorMessage,
            null,
            null
        );
    }

    /**
     * 构造"拒绝流"：只发一个 error 事件就结束。
     *
     * <p>用于抢锁失败、参数校验失败、引导启动失败等场景 ——
     * 统一以"流"的形式返回错误，而不是向上抛异常。</p>
     */
    private Flux<String> rejectionFlux(String message) {
        return rejectionFlux(message, null, null);
    }

    private Flux<String> rejectionFlux(String message, String conversationId, Long exchangeId) {

        return Flux.just(streamEventWriter.error(message, new StreamEventMetadata(conversationId, exchangeId)));
    }

    public ConversationStopVo stopConversation(String conversationId) {
        return stopConversation(conversationId, "用户已停止生成");
    }

    public ConversationStopVo stopConversation(String conversationId, String reason) {
        Optional<TaskInfo> taskInfoOptional = chatRuntimeRegistry.get(conversationId);
        if (taskInfoOptional.isEmpty()) {
            return new ConversationStopVo(conversationId, false, "没有找到正在执行的会话");
        }
        return stopTask(taskInfoOptional.get(), reason);
    }

    private ConversationStopVo stopTask(TaskInfo taskInfo, String reason) {

        if (!taskInfo.finalized().compareAndSet(false, true)) {
            return new ConversationStopVo(taskInfo.conversationId(), false, "会话已经结束");
        }

        Optional<TaskInfo> currentTask = chatRuntimeRegistry.get(taskInfo.conversationId());
        if (currentTask.isPresent() && currentTask.get() != taskInfo) {

            return new ConversationStopVo(taskInfo.conversationId(), false, "会话已由新的执行接管");
        }

        try {

            businessChatReactAgent.interrupt(taskInfo.runnableConfig());
        }
        catch (RuntimeException exception) {
            log.debug("中断 ReactAgent 时出现异常，继续释放资源", exception);
        }

        Disposable disposable = taskInfo.disposable();
        if (disposable != null && !disposable.isDisposed()) {

            disposable.dispose();
        }

        String responseMessage = "已停止会话生成";
        ConversationTraceRecorder.StageHandle finalizeStage = taskInfo.traceRecorder() == null
            ? null
            : taskInfo.traceRecorder().startStage(
                com.sopengin.neo.ai.chatagent.model.trace.ConversationTraceStageCode.FINALIZE,
                taskInfo.executionPlan() == null || taskInfo.executionPlan().getMode() == null ? "" : taskInfo.executionPlan().getMode().name(),
                "正在收尾停止中的会话。",
                null
            );
        try {
            safeEmit(taskInfo.sink(), streamEventWriter.status("⏹ " + reason, taskInfo.eventMetadata()));
        }
        catch (RuntimeException exception) {
            log.warn("发送停止事件失败, conversationId={}, exchangeId={}", taskInfo.conversationId(), taskInfo.exchangeId(), exception);
            responseMessage = "会话已停止，停止事件发送失败";
        }
        finally {
            try {
                safeComplete(taskInfo.sink());
            }
            catch (RuntimeException exception) {
                log.warn("关闭停止中的 SSE 流失败, conversationId={}, exchangeId={}", taskInfo.conversationId(), taskInfo.exchangeId(), exception);
            }
            try {
                refreshDebugTraceRuntimeStats(taskInfo);
                conversationArchiveStore.completeExchange(
                    taskInfo.conversationId(),
                    taskInfo.exchangeId(),
                    taskInfo.answerBuffer().toString(),
                    snapshotStringList(taskInfo.thinkingSteps()),
                    deduplicateReferences(snapshotReferenceList(taskInfo.references())),
                    List.of(),
                    snapshotUsedTools(taskInfo.usedTools()),
                    taskInfo.debugTrace(),
                    ChatTurnStatus.STOPPED,
                    reason,
                    toNullable(taskInfo.firstResponseTimeMs().get()),
                    System.currentTimeMillis() - taskInfo.startTime()
                );
                if (taskInfo.traceRecorder() != null) {
                    taskInfo.traceRecorder().completeStage(finalizeStage, "会话已按停止状态收尾。", Map.of(
                        "finalStatus", ChatTurnStatus.STOPPED.name(),
                        "reason", reason,
                        "answerLength", taskInfo.answerBuffer().length()
                    ));
                }
            }
            catch (RuntimeException exception) {
                log.error("停止会话落库失败, conversationId={}, exchangeId={}", taskInfo.conversationId(), taskInfo.exchangeId(), exception);
                responseMessage = "会话已停止，收尾落库失败";
                if (taskInfo.traceRecorder() != null) {
                    taskInfo.traceRecorder().failStage(finalizeStage, "停止态收尾失败。", exception.getMessage(), null);
                }
            }
            finally {
                safeRefreshConversationSummary(taskInfo.conversationId());
                cleanup(taskInfo);
            }
        }
        return new ConversationStopVo(taskInfo.conversationId(), true, responseMessage);
    }

    public ConversationSessionView getSession(String conversationId) {
        ConversationArchiveStore.ConversationArchiveRecord archiveRecord = conversationArchiveStore.getSessionRecord(conversationId)
            .orElseThrow(() -> new IllegalArgumentException("会话不存在: " + conversationId));
        return overlayRuntimeSnapshot(toSessionView(archiveRecord, true, true));
    }

    public ConversationExchangeDetailView getExchangeDetail(String conversationId, String exchangeId) {
        long resolvedExchangeId = parseRequiredLong(exchangeId, "exchangeId");
        ConversationSessionView sessionView = getSession(conversationId);
        ConversationExchangeView exchangeView = sessionView.getExchanges().stream()
            .filter(item -> item != null && item.getExchangeId() == resolvedExchangeId)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("轮次不存在: " + exchangeId));
        return new ConversationExchangeDetailView(
            conversationId,
            exchangeView,
            conversationTraceStageStore.listStageViews(conversationId, resolvedExchangeId)
        );
    }

    public ConversationSessionListVo listSessions(ConversationSessionListQueryDto dto) {
        int pageNo = parsePositiveInt(dto == null ? null : dto.getPageNo(), 1);
        int pageSize = parsePositiveInt(dto == null ? null : dto.getPageSize(), 20);
        String keyword = normalizeOptionalText(dto == null ? null : dto.getKeyword());
        ChatQueryMode chatMode = parseOptionalChatMode(dto == null ? null : dto.getChatMode());
        ChatTurnStatus turnStatus = parseOptionalTurnStatus(dto == null ? null : dto.getTurnStatus());

        ConversationArchiveStore.ConversationArchivePage archivePage = conversationArchiveStore.listSessionRecordPage(
            pageNo,
            pageSize,
            keyword,
            chatMode,
            turnStatus
        );
        List<ConversationSessionView> sessions = archivePage.records()
            .stream()
            .map(record -> toSessionView(record, false, false))
            .toList();

        long totalPages = archivePage.totalSize() <= 0
            ? 0
            : (archivePage.totalSize() + archivePage.pageSize() - 1) / archivePage.pageSize();
        return new ConversationSessionListVo(
            archivePage.pageNo(),
            archivePage.pageSize(),
            archivePage.totalSize(),
            totalPages,
            sessions
        );
    }

    public List<KnowledgeDocumentOptionView> listKnowledgeDocumentOptions() {
        return documentKnowledgeService.listRetrievableDocuments().stream()
            .map(this::toKnowledgeDocumentOptionView)
            .toList();
    }

    public ConversationMemorySummaryView rebuildConversationSummary(String conversationId) {
        return conversationMemoryService.rebuildConversationSummary(conversationId);
    }

    public ConversationResetVo resetConversation(String conversationId) {

        ConversationStopVo stopResult = stopConversation(conversationId, "会话被重置");

        ConversationArchiveStore.ConversationRemovalResult removalResult = conversationArchiveStore.deleteSession(conversationId);

        conversationMemoryService.deleteConversationSummary(conversationId);
        conversationTraceStageStore.deleteStages(conversationId);
        retrievalObserveStore.deleteByConversation(conversationId);
        int removedCheckpointCount = checkpointManager.clearThread(conversationId);
        return new ConversationResetVo(
            conversationId,
            stopResult.isStopped(),
            removalResult.removedDialogueCount(),
            removalResult.removedExchangeCount(),
            removedCheckpointCount,
            "会话已重置"
        );
    }

    /**
     * 处理模型的每一个正文分片。
     *
     * <p>流式输出"过手即走"，所以这里要同时做三件事：攒全文、记首字耗时、包成 text 事件推送。</p>
     */
    private void emitModelChunk(TaskInfo taskInfo, String chunk) {

        // 累积完整答案，供最后落库（流式本身不会留下全文）
        taskInfo.answerBuffer().append(chunk);

        // 首字响应耗时只记第一次，用 CAS 保证并发下不被覆盖
        if (taskInfo.firstResponseTimeMs().get() == 0L) {

            taskInfo.firstResponseTimeMs().compareAndSet(0L, System.currentTimeMillis() - taskInfo.startTime());
        }

        // 包成 {"type":"text","content":"..."} 事件写入事件总线，前端逐字渲染
        safeEmit(taskInfo.sink(), streamEventWriter.text(chunk, taskInfo.eventMetadata()));
    }

    /**
     * 成功收尾。
     *
     * <p>生成推荐追问 → 补发引用与推荐事件 → 关闭 SSE 流 → 回填数据库 → 清理资源。</p>
     */
    private void finishSuccessfully(TaskInfo taskInfo) {
        // 关键：CAS 抢占"收尾权"。成功 / 失败 / 停止三条路径会并发竞争，
        // 只有抢到的那一条能继续执行，保证收尾逻辑与落库只发生一次
        if (!taskInfo.finalized().compareAndSet(false, true)) {
            return;
        }

        String answer = taskInfo.answerBuffer().toString();
        // 引用去重：同一份证据可能被多个子问题同时命中
        List<SearchReference> uniqueReferences = deduplicateReferences(snapshotReferenceList(taskInfo.references()));
        ConversationTraceRecorder.StageHandle finalizeStage = taskInfo.traceRecorder() == null
            ? null
            : taskInfo.traceRecorder().startStage(
                com.sopengin.neo.ai.chatagent.model.trace.ConversationTraceStageCode.FINALIZE,
                taskInfo.executionPlan() == null || taskInfo.executionPlan().getMode() == null ? "" : taskInfo.executionPlan().getMode().name(),
                "正在收尾已完成会话。",
                null
            );
        ConversationTraceRecorder.StageHandle recommendationStage = taskInfo.traceRecorder() == null
            ? null
            : taskInfo.traceRecorder().startStage(
                com.sopengin.neo.ai.chatagent.model.trace.ConversationTraceStageCode.RECOMMENDATION,
                taskInfo.executionPlan() == null || taskInfo.executionPlan().getMode() == null ? "" : taskInfo.executionPlan().getMode().name(),
                "正在生成推荐追问。",
                null
            );
        List<String> recommendations;
        // 澄清模式：直接把"可选文档"当作推荐追问返回，无需再调一次模型
        if (taskInfo.executionPlan() != null
            && taskInfo.executionPlan().getMode() == com.sopengin.neo.ai.chatagent.rag.model.ExecutionMode.CLARIFICATION) {
            recommendations = taskInfo.executionPlan().getClarificationOptions() == null
                ? List.of()
                : new ArrayList<>(taskInfo.executionPlan().getClarificationOptions());
        }
        else {
            // 常规模式：额外调一次模型生成最多 3 个推荐追问，引导用户继续深入
            recommendations = recommendationService.generateRecommendations(
                taskInfo.question(),
                answer,
                historicalRecentExchanges(taskInfo),
                taskInfo.traceRecorder()
            );
        }
        if (taskInfo.traceRecorder() != null) {
            taskInfo.traceRecorder().completeStage(recommendationStage, "推荐追问生成完成。", Map.of(
                "recommendationCount", recommendations.size(),
                "recommendations", recommendations
            ));
        }

        try {
            // SSE 协议顺序：正文（前面已推） → 引用来源 → 推荐追问
            if (!uniqueReferences.isEmpty()) {
                safeEmit(taskInfo.sink(), streamEventWriter.references(uniqueReferences, taskInfo.eventMetadata()));
            }
            if (!recommendations.isEmpty()) {
                safeEmit(taskInfo.sink(), streamEventWriter.recommendations(recommendations, taskInfo.eventMetadata()));
            }
        }
        catch (RuntimeException exception) {
            log.warn("补发引用或推荐事件失败, conversationId={}, exchangeId={}", taskInfo.conversationId(), taskInfo.exchangeId(), exception);
        }
        finally {
            try {
                // 关闭 SSE 流，前端据此结束本次流式接收
                safeComplete(taskInfo.sink());
            }
            catch (RuntimeException exception) {
                log.warn("关闭成功完成的 SSE 流失败, conversationId={}, exchangeId={}", taskInfo.conversationId(), taskInfo.exchangeId(), exception);
            }
            try {
                refreshDebugTraceRuntimeStats(taskInfo);
                // 回填数据库：answerBuffer 全文 + 引用 + 推荐 + 工具 + 耗时，状态置 COMPLETED；
                // 同时把 dialogue 会话状态置回 IDLE
                conversationArchiveStore.completeExchange(
                    taskInfo.conversationId(),
                    taskInfo.exchangeId(),
                    answer,
                    snapshotStringList(taskInfo.thinkingSteps()),
                    uniqueReferences,
                    recommendations,
                    snapshotUsedTools(taskInfo.usedTools()),
                    taskInfo.debugTrace(),
                    ChatTurnStatus.COMPLETED,
                    "",
                    toNullable(taskInfo.firstResponseTimeMs().get()),
                    System.currentTimeMillis() - taskInfo.startTime()
                );
                if (taskInfo.traceRecorder() != null) {
                    taskInfo.traceRecorder().completeStage(finalizeStage, "会话已按完成状态收尾。", Map.of(
                        "finalStatus", ChatTurnStatus.COMPLETED.name(),
                        "referenceCount", uniqueReferences.size(),
                        "recommendationCount", recommendations.size(),
                        "answerLength", answer.length()
                    ));
                }
            }
            catch (RuntimeException exception) {
                log.error("成功会话收尾落库失败, conversationId={}, exchangeId={}", taskInfo.conversationId(), taskInfo.exchangeId(), exception);
                if (taskInfo.traceRecorder() != null) {
                    taskInfo.traceRecorder().failStage(finalizeStage, "完成态收尾失败。", exception.getMessage(), null);
                }
            }
            finally {
                // 异步刷新会话摘要（长期摘要 + 最近窗口，供下一轮编排使用）
                safeRefreshConversationSummary(taskInfo.conversationId());
                // 关键：无论成功失败都要走到这里，释放租约与注册表，保证资源守恒
                cleanup(taskInfo);
            }
        }
    }

    /**
     * 失败收尾：推送 error 事件 → 关闭流 → 把轮次记录标记为 FAILED → 清理资源。
     */
    private void finishWithFailure(TaskInfo taskInfo, Throwable error) {
        // 同样用 CAS 抢占收尾权，与成功 / 停止路径互斥
        if (!taskInfo.finalized().compareAndSet(false, true)) {
            return;
        }

        String errorMessage = buildErrorMessage(error);
        ConversationTraceRecorder.StageHandle finalizeStage = taskInfo.traceRecorder() == null
            ? null
            : taskInfo.traceRecorder().startStage(
                com.sopengin.neo.ai.chatagent.model.trace.ConversationTraceStageCode.FINALIZE,
                taskInfo.executionPlan() == null || taskInfo.executionPlan().getMode() == null ? "" : taskInfo.executionPlan().getMode().name(),
                "正在收尾失败会话。",
                null
            );

        log.error("会话执行失败, conversationId={}, exchangeId={}, error={}",
            taskInfo.conversationId(),
            taskInfo.exchangeId(),
            errorMessage,
            error);

        try {
            safeEmit(taskInfo.sink(), streamEventWriter.error(errorMessage, taskInfo.eventMetadata()));
        }
        catch (RuntimeException exception) {
            log.warn("发送失败事件失败, conversationId={}, exchangeId={}", taskInfo.conversationId(), taskInfo.exchangeId(), exception);
        }
        finally {
            try {
                safeComplete(taskInfo.sink());
            }
            catch (RuntimeException exception) {
                log.warn("关闭失败中的 SSE 流失败, conversationId={}, exchangeId={}", taskInfo.conversationId(), taskInfo.exchangeId(), exception);
            }
            try {
                refreshDebugTraceRuntimeStats(taskInfo);
                conversationArchiveStore.completeExchange(
                    taskInfo.conversationId(),
                    taskInfo.exchangeId(),
                    taskInfo.answerBuffer().toString(),
                    snapshotStringList(taskInfo.thinkingSteps()),
                    deduplicateReferences(snapshotReferenceList(taskInfo.references())),
                    List.of(),
                    snapshotUsedTools(taskInfo.usedTools()),
                    taskInfo.debugTrace(),
                    ChatTurnStatus.FAILED,
                    errorMessage,
                    toNullable(taskInfo.firstResponseTimeMs().get()),
                    System.currentTimeMillis() - taskInfo.startTime()
                );
                if (taskInfo.traceRecorder() != null) {
                    taskInfo.traceRecorder().completeStage(finalizeStage, "会话已按失败状态收尾。", Map.of(
                        "finalStatus", ChatTurnStatus.FAILED.name(),
                        "errorMessage", errorMessage,
                        "answerLength", taskInfo.answerBuffer().length()
                    ));
                }
            }
            catch (RuntimeException exception) {
                log.error("失败会话收尾落库失败, conversationId={}, exchangeId={}", taskInfo.conversationId(), taskInfo.exchangeId(), exception);
                if (taskInfo.traceRecorder() != null) {
                    taskInfo.traceRecorder().failStage(finalizeStage, "失败态收尾失败。", exception.getMessage(), null);
                }
            }
            finally {
                safeRefreshConversationSummary(taskInfo.conversationId());
                cleanup(taskInfo);
            }
        }
    }

    private String buildErrorMessage(Throwable error) {

        Throwable current = error;
        while (current != null) {

            if (current instanceof WebClientResponseException responseException) {
                String responseBody = responseException.getResponseBodyAsString();
                if (StrUtil.isNotBlank(responseBody)) {
                    return responseException.getStatusCode()
                        + " from "
                        + responseException.getRequest().getMethod()
                        + " "
                        + responseException.getRequest().getURI()
                        + " | responseBody="
                        + responseBody;
                }

                return responseException.getMessage();
            }
            current = current.getCause();
        }

        return error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName();
    }

    private void refreshDebugTraceRuntimeStats(TaskInfo taskInfo) {
        if (taskInfo == null || taskInfo.debugTrace() == null || taskInfo.traceRecorder() == null) {
            return;
        }
        taskInfo.debugTrace().setModelUsageTraces(taskInfo.traceRecorder().snapshotModelUsageTraces());
        com.sopengin.neo.ai.chatagent.model.debug.ChatLimitStats limitStats = taskInfo.traceRecorder().limitStats();
        limitStats.setModelCallsUsed(taskInfo.traceRecorder().snapshotModelUsageTraces().size());
        limitStats.setModelCallsRunLimit(chatAgentProperties.getMaxModelCallsPerRun());
        limitStats.setModelCallsThreadLimit(chatAgentProperties.getMaxModelCallsPerThread());
        limitStats.setToolCallsUsed(snapshotUsedTools(taskInfo.usedTools()).size());
        limitStats.setToolCallsRunLimit(chatAgentProperties.getMaxToolCallsPerRun());
        limitStats.setToolCallsThreadLimit(chatAgentProperties.getMaxToolCallsPerThread());
        taskInfo.debugTrace().setLimitStats(limitStats);
    }

    /**
     * 统一资源清理。
     *
     * <p>成功 / 失败 / 停止三条收尾路径最终都会走到这里，保证"资源守恒"——
     * 无论对话以何种方式结束，租约与注册表项都一定被释放，不留孤儿锁。</p>
     */
    private void cleanup(TaskInfo taskInfo) {

        Disposable disposable = taskInfo.disposable();
        Disposable leaseRenewalDisposable = taskInfo.leaseRenewalDisposable();

        // 停掉租约续期定时任务
        if (leaseRenewalDisposable != null && !leaseRenewalDisposable.isDisposed()) {

            leaseRenewalDisposable.dispose();
        }

        // 停掉执行流
        if (disposable != null && !disposable.isDisposed()) {

            disposable.dispose();
        }

        // 释放 Redis 租约（内部按 ownerToken 校验归属，避免误删他人的锁）
        releaseLeaseQuietly(taskInfo.leaseKey(), taskInfo.leaseOwnerToken());

        // 摘除进程内任务注册表项，释放"防重"标记
        chatRuntimeRegistry.remove(taskInfo.conversationId(), taskInfo);
    }

    private List<SearchReference> deduplicateReferences(List<SearchReference> references) {
        Map<String, SearchReference> unique = new LinkedHashMap<>();

        for (SearchReference reference : references) {
            if (reference == null) {
                continue;
            }
            unique.putIfAbsent(reference.uniqueKey(), reference);
        }
        return new ArrayList<>(unique.values());
    }

    private ChatDebugTrace initializeDebugTrace(ConversationExecutionPlan executionPlan) {
        if (executionPlan == null) {
            return ChatDebugTrace.builder()
                .retrievalNotes(Collections.synchronizedList(new ArrayList<>()))
                .usedChannels(Collections.synchronizedList(new ArrayList<>()))
                .build();
        }
        return ChatDebugTrace.builder()

            .executionMode(executionPlan.getMode() == null ? "" : executionPlan.getMode().name())
            .chatMode(executionPlan.getChatMode())

            .originalQuestion(executionPlan.getOriginalQuestion())
            .rewriteQuestion(executionPlan.getRewriteQuestion())
            .rewriteSubQuestions(executionPlan.getRewriteSubQuestions() == null ? List.of() : new ArrayList<>(executionPlan.getRewriteSubQuestions()))
            .retrievalQuestion(executionPlan.getRetrievalQuestion())
            .agentQuestion(executionPlan.getAgentQuestion())
            .navigationDecision(executionPlan.getNavigationDecision())

            .historySummary(executionPlan.getHistorySummary())
            .longTermSummary(executionPlan.getLongTermSummary())
            .recentHistoryTranscript(executionPlan.getRecentHistoryTranscript())
            .answerRecentTranscript(executionPlan.getAnswerRecentTranscript())
            .answerHistoryContext(executionPlan.getAnswerHistoryContext() == null
                ? ""
                : executionPlan.getAnswerHistoryContext().getRenderedText())
            .answerHistoryFollowUpQuestion(executionPlan.getAnswerHistoryContext() != null
                && executionPlan.getAnswerHistoryContext().isFollowUpQuestion())
            .historyCompressionApplied(executionPlan.isHistoryCompressionApplied())
            .historyCoveredExchangeId(executionPlan.getHistoryCoveredExchangeId())
            .historyCoveredExchangeCount(executionPlan.getHistoryCoveredExchangeCount())
            .historyCompressionCount(executionPlan.getHistoryCompressionCount())
            .currentDateText(executionPlan.getCurrentDateText())
            .requiresFreshSearch(executionPlan.isRequiresFreshSearch())
            .requiresCurrentDateAnchoring(executionPlan.isRequiresCurrentDateAnchoring())

            .retrievalSubQuestions(executionPlan.getRetrievalSubQuestions() == null ? List.of() : new ArrayList<>(executionPlan.getRetrievalSubQuestions()))
            .selectedDocumentId(executionPlan.getSelectedDocumentId())
            .selectedTaskId(executionPlan.getSelectedTaskId())

            .retrievalNotes(Collections.synchronizedList(new ArrayList<>()))
            .usedChannels(Collections.synchronizedList(new ArrayList<>()))
            .toolTraces(Collections.synchronizedList(new ArrayList<>()))
            .noEvidenceReply(executionPlan.getNoEvidenceReply())
            .build();
    }

    /**
     * 调用前置编排器产出"执行计划"，并写回工作台。
     *
     * <p>编排器内部完成五步决策：会话记忆装载 → 时间敏感检测 → chatMode 分流 →
     * 问题改写与子问题拆分 → 知识路由与意图路由。</p>
     */
    private ConversationExecutionPlan prepareExecutionPlan(TaskInfo taskInfo) {

        // 五步决策链，产出 mode / 改写问题 / 子问题 / 文档范围等
        ConversationExecutionPlan executionPlan = chatPreparationOrchestrator.prepare(taskInfo);

        // 为 ReAct Agent 组装带时间锚点与历史摘要的提问
        executionPlan.setAgentQuestion(buildAgentQuestion(executionPlan));
        // 自动路由可能重新锁定了文档，此时同步刷新会话范围与 Agent 上下文
        if (executionPlan.getSelectedDocumentId() != null
            && !Objects.equals(executionPlan.getSelectedDocumentId(), taskInfo.selectedDocumentId())) {
            conversationArchiveStore.refreshSessionScope(
                taskInfo.conversationId(),
                executionPlan.getChatMode(),
                executionPlan.getSelectedDocumentId(),
                executionPlan.getSelectedDocumentName()
            );
            putContextIfNotNull(taskInfo.runnableConfig(), ChatContextKeys.SELECTED_DOCUMENT_ID, executionPlan.getSelectedDocumentId());
            putContextIfNotBlank(taskInfo.runnableConfig(), ChatContextKeys.SELECTED_DOCUMENT_NAME, executionPlan.getSelectedDocumentName());
            putContextIfNotNull(taskInfo.runnableConfig(), ChatContextKeys.SELECTED_TASK_ID, executionPlan.getSelectedTaskId());
        }
        // 写回工作台：执行器后续都要从 taskInfo 上取执行计划
        taskInfo.setExecutionPlan(executionPlan);
        taskInfo.setDebugTrace(initializeDebugTrace(executionPlan));
        taskInfo.runnableConfig().context().put(ChatContextKeys.DEBUG_TRACE, taskInfo.debugTrace());
        return executionPlan;
    }

    private ConversationSessionView toSessionView(ConversationArchiveStore.ConversationArchiveRecord archiveRecord,
                                                  boolean includeMemorySummary,
                                                  boolean includeExchanges) {

        RunnableConfig runnableConfig = RunnableConfig.builder()
            .threadId(archiveRecord.conversationId())
            .build();

        Map<String, Object> state = checkpointManager.get(runnableConfig)
            .map(Checkpoint::getState)
            .orElseGet(Map::of);
        Object messages = state.getOrDefault("messages", List.of());
        List<?> messageList = messages instanceof List<?> list ? list : List.of();
        List<ConversationExchangeView> archiveExchanges = archiveRecord.exchanges() == null ? List.of() : archiveRecord.exchanges();
        List<ConversationExchangeView> exchanges = includeExchanges ? archiveExchanges : List.of();
        int businessMessageCount = businessMessageCount(archiveExchanges);
        String businessLatestUserMessage = latestExchangeQuestion(archiveExchanges);
        String businessLatestAssistantMessage = latestExchangeAnswer(archiveExchanges);
        ConversationExchangeView latestExchange = latestExchange(archiveExchanges);

        return new ConversationSessionView(
            archiveRecord.conversationId(),
            archiveRecord.running(),

            checkpointManager.list(runnableConfig).size(),

            businessMessageCount > 0 ? businessMessageCount : messageList.size(),
            StrUtil.isNotBlank(businessLatestUserMessage) ? businessLatestUserMessage : latestMessage(messageList, MessageType.USER),
            StrUtil.isNotBlank(businessLatestAssistantMessage) ? businessLatestAssistantMessage : latestMessage(messageList, MessageType.ASSISTANT),
            latestExchange == null ? null : latestExchange.getExchangeId(),
            latestExchange == null || latestExchange.getStatus() == null ? "" : latestExchange.getStatus().name(),
            latestExchange == null || latestExchange.getErrorMessage() == null ? "" : latestExchange.getErrorMessage(),
            archiveRecord.chatMode(),
            archiveRecord.selectedDocumentId() == null ? "" : String.valueOf(archiveRecord.selectedDocumentId()),
            archiveRecord.selectedDocumentName(),
            archiveRecord.createdAt(),
            archiveRecord.updatedAt(),
            exchanges,
            includeMemorySummary ? conversationMemoryService.getConversationSummary(archiveRecord.conversationId()) : null
        );
    }

    private ConversationSessionView overlayRuntimeSnapshot(ConversationSessionView sessionView) {
        if (sessionView == null || sessionView.getExchanges() == null || sessionView.getExchanges().isEmpty()) {
            return sessionView;
        }
        Optional<TaskInfo> runtimeOptional = chatRuntimeRegistry.get(sessionView.getConversationId());
        if (runtimeOptional.isEmpty()) {
            return sessionView;
        }
        TaskInfo taskInfo = runtimeOptional.get();
        List<ConversationExchangeView> exchanges = new ArrayList<>(sessionView.getExchanges().size());
        boolean replaced = false;
        for (ConversationExchangeView exchange : sessionView.getExchanges()) {
            if (exchange == null) {
                continue;
            }
            if (exchange.getExchangeId() == taskInfo.exchangeId()) {
                exchanges.add(mergeRuntimeExchange(exchange, taskInfo));
                replaced = true;
                continue;
            }
            exchanges.add(exchange);
        }
        if (!replaced) {
            return sessionView;
        }
        sessionView.setExchanges(exchanges);
        sessionView.setMessageCount(businessMessageCount(exchanges));
        sessionView.setRunning(true);
        sessionView.setUpdatedAt(Instant.now());
        sessionView.setLatestExchangeId(taskInfo.exchangeId());
        sessionView.setLatestTurnStatus(ChatTurnStatus.RUNNING.name());
        String liveAnswer = taskInfo.answerBuffer().toString();
        if (StrUtil.isNotBlank(liveAnswer)) {
            sessionView.setLatestAssistantMessage(liveAnswer);
        }
        return sessionView;
    }

    private ConversationExchangeView mergeRuntimeExchange(ConversationExchangeView exchange,
                                                          TaskInfo taskInfo) {
        return new ConversationExchangeView(
            exchange.getExchangeId(),
            exchange.getQuestion(),
            taskInfo.answerBuffer().toString(),
            snapshotStringList(taskInfo.thinkingSteps()),
            deduplicateReferences(snapshotReferenceList(taskInfo.references())),
            exchange.getRecommendations() == null ? List.of() : exchange.getRecommendations(),
            snapshotUsedTools(taskInfo.usedTools()),
            taskInfo.debugTrace(),
            ChatTurnStatus.RUNNING,
            exchange.getErrorMessage(),
            toNullable(taskInfo.firstResponseTimeMs().get()),
            System.currentTimeMillis() - taskInfo.startTime(),
            exchange.getCreateTime(),
            exchange.getEditTime()
        );
    }

    private KnowledgeDocumentDescriptor resolveSelectedDocument(ChatQueryMode chatMode, String selectedDocumentId) {
        if (chatMode == null) {
            throw new IllegalArgumentException("chatMode 不能为空");
        }
        String normalizedDocumentId = StrUtil.trimToNull(selectedDocumentId);
        if (chatMode == ChatQueryMode.OPEN_CHAT) {

            if (normalizedDocumentId != null) {
                throw new IllegalArgumentException("开放式提问模式下不能传 selectedDocumentId");
            }
            return null;
        }
        if (chatMode == ChatQueryMode.AUTO_DOCUMENT) {
            if (normalizedDocumentId != null) {
                throw new IllegalArgumentException("自动知识问答模式下不能传 selectedDocumentId");
            }
            return null;
        }

        if (normalizedDocumentId == null) {
            throw new IllegalArgumentException("当前文档问答模式下必须选择一个文档");
        }
        final Long resolvedDocumentId = parseRequiredLong(normalizedDocumentId, "selectedDocumentId");
        return documentKnowledgeService.listRetrievableDocuments().stream()
            .filter(item -> Objects.equals(item.getDocumentId(), resolvedDocumentId))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("所选文档当前不可检索: " + normalizedDocumentId));
    }

    private KnowledgeDocumentOptionView toKnowledgeDocumentOptionView(KnowledgeDocumentDescriptor descriptor) {
        return new KnowledgeDocumentOptionView(
            descriptor.getDocumentId() == null ? "" : String.valueOf(descriptor.getDocumentId()),
            descriptor.getDocumentName(),
            descriptor.getKnowledgeScopeName(),
            descriptor.getBusinessCategory(),
            descriptor.getDocumentTags()
        );
    }

    private Long parseRequiredLong(String value, String fieldName) {
        try {
            return Long.parseLong(value);
        }
        catch (NumberFormatException exception) {
            throw new IllegalArgumentException(fieldName + " 非法: " + value, exception);
        }
    }

    private int parsePositiveInt(String value, int defaultValue) {
        if (StrUtil.isBlank(value)) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed > 0 ? parsed : defaultValue;
        }
        catch (NumberFormatException exception) {
            throw new IllegalArgumentException("分页参数非法: " + value, exception);
        }
    }

    private String normalizeOptionalText(String value) {
        return StrUtil.isBlank(value) ? null : value.trim();
    }

    private ChatQueryMode parseOptionalChatMode(String value) {
        if (StrUtil.isBlank(value) || "ALL".equalsIgnoreCase(value.trim())) {
            return null;
        }
        try {
            return ChatQueryMode.valueOf(value.trim().toUpperCase());
        }
        catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("chatMode 非法: " + value, exception);
        }
    }

    private ChatQueryMode parseRequiredChatMode(String value) {
        ChatQueryMode chatMode = parseOptionalChatMode(value);
        if (chatMode == null) {
            throw new IllegalArgumentException("chatMode 不能为空");
        }
        return chatMode;
    }

    private ChatTurnStatus parseOptionalTurnStatus(String value) {
        if (StrUtil.isBlank(value) || "ALL".equalsIgnoreCase(value.trim())) {
            return null;
        }
        try {
            return ChatTurnStatus.valueOf(value.trim().toUpperCase());
        }
        catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("turnStatus 非法: " + value, exception);
        }
    }

    private int businessMessageCount(List<ConversationExchangeView> exchanges) {
        if (exchanges == null || exchanges.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (ConversationExchangeView exchange : exchanges) {
            if (exchange == null) {
                continue;
            }
            if (StrUtil.isNotBlank(exchange.getQuestion())) {
                count++;
            }
            if (StrUtil.isNotBlank(exchange.getAnswer())) {
                count++;
            }
        }
        return count;
    }

    private String latestExchangeQuestion(List<ConversationExchangeView> exchanges) {
        if (exchanges == null || exchanges.isEmpty()) {
            return "";
        }
        for (int index = exchanges.size() - 1; index >= 0; index--) {
            ConversationExchangeView exchange = exchanges.get(index);
            if (exchange != null && StrUtil.isNotBlank(exchange.getQuestion())) {
                return exchange.getQuestion();
            }
        }
        return "";
    }

    private String latestExchangeAnswer(List<ConversationExchangeView> exchanges) {
        if (exchanges == null || exchanges.isEmpty()) {
            return "";
        }
        for (int index = exchanges.size() - 1; index >= 0; index--) {
            ConversationExchangeView exchange = exchanges.get(index);
            if (exchange != null && StrUtil.isNotBlank(exchange.getAnswer())) {
                return exchange.getAnswer();
            }
        }
        return "";
    }

    private ConversationExchangeView latestExchange(List<ConversationExchangeView> exchanges) {
        if (exchanges == null || exchanges.isEmpty()) {
            return null;
        }
        for (int index = exchanges.size() - 1; index >= 0; index--) {
            ConversationExchangeView exchange = exchanges.get(index);
            if (exchange != null) {
                return exchange;
            }
        }
        return null;
    }

    private String latestMessage(List<?> messages, MessageType type) {

        for (int index = messages.size() - 1; index >= 0; index--) {
            Object candidate = messages.get(index);
            if (candidate instanceof AbstractMessage message && message.getMessageType() == type) {
                return message.getText();
            }
        }
        return "";
    }

    private List<ConversationExchangeView> recentExchanges(String conversationId) {

        return conversationArchiveStore.listRecentExchanges(
            conversationId,
            Math.max(1, chatAgentProperties.getHistoryPreviewTurns())
        );
    }

    private List<ConversationExchangeView> historicalRecentExchanges(TaskInfo taskInfo) {
        return recentExchanges(taskInfo.conversationId()).stream()
            .filter(exchange -> exchange.getExchangeId() != taskInfo.exchangeId())
            .toList();
    }

    private RunnableConfig buildSessionConfig(String conversationId) {

        return RunnableConfig.builder()
            .threadId(conversationId)
            .build();
    }

    private void putContextIfNotNull(RunnableConfig runnableConfig, String key, Object value) {
        if (runnableConfig == null || StrUtil.isBlank(key) || value == null) {
            return;
        }
        runnableConfig.context().put(key, value);
    }

    private void putContextIfNotBlank(RunnableConfig runnableConfig, String key, String value) {
        if (runnableConfig == null || StrUtil.isBlank(key) || StrUtil.isBlank(value)) {
            return;
        }
        runnableConfig.context().put(key, value.trim());
    }

    /**
     * 启动租约续期：每 10s 触发一次 renewLeaseOrStop。
     */
    private Disposable startLeaseRenewal(TaskInfo taskInfo) {

        return Flux.interval(CHAT_RUNNING_LEASE_RENEW_INTERVAL, CHAT_RUNNING_LEASE_RENEW_INTERVAL)

            .subscribe(ignored -> renewLeaseOrStop(taskInfo), error ->
                log.warn("租约续期任务出现异常, conversationId={}, exchangeId={}",
                    taskInfo.conversationId(),
                    taskInfo.exchangeId(),
                    error)
            );
    }

    /**
     * 续期一次；失败则主动停止会话。
     *
     * <p>续期失败说明这把锁已经不归本实例（被抢占或本地假死）。此时绝不能"忽略继续跑"，
     * 否则会出现两个实例同时为同一会话生成回答 —— 宁可停，不可双跑。</p>
     */
    private void renewLeaseOrStop(TaskInfo taskInfo) {

        boolean renewed = redisLeaseManager.renew(
            taskInfo.leaseKey(),
            taskInfo.leaseOwnerToken(),
            CHAT_RUNNING_LEASE_TTL
        );
        if (renewed) {

            return;
        }

        log.warn("会话租约续期失败，准备停止当前会话, conversationId={}, exchangeId={}",
            taskInfo.conversationId(),
            taskInfo.exchangeId());
        Disposable leaseRenewalDisposable = taskInfo.leaseRenewalDisposable();
        if (leaseRenewalDisposable != null && !leaseRenewalDisposable.isDisposed()) {
            leaseRenewalDisposable.dispose();
        }
        // 主动止损：停止本会话，避免与其他实例重复生成
        stopTask(taskInfo, "会话租约已失效，已停止生成");
    }

    /**
     * 静默释放租约：释放失败只记日志，不抛异常。
     *
     * <p>因为此时主流程往往已经失败，不能让"释放锁失败"再掩盖掉原始错误。</p>
     */
    private void releaseLeaseQuietly(String leaseKey, String leaseOwnerToken) {
        try {

            redisLeaseManager.release(leaseKey, leaseOwnerToken);
        }
        catch (RuntimeException exception) {

            log.warn("释放会话租约时出现异常, leaseKey={}", leaseKey, exception);
        }
    }

    private String buildChatLeaseKey(String conversationId) {

        return CHAT_RUNNING_LEASE_PREFIX + conversationId;
    }

    private Long toNullable(long value) {

        return value > 0 ? value : null;
    }

    private String normalizeQuestion(String question) {
        if (StrUtil.isBlank(question)) {
            throw new NeoAgentFrameException("question 不能为空");
        }

        return question.trim();
    }

    private String normalizeConversationId(String conversationId) {
        if (StrUtil.isNotBlank(conversationId)) {

            return conversationId.trim();
        }

        return UUID.randomUUID().toString().replace("-", "");
    }

    private String buildAgentQuestion(ConversationExecutionPlan executionPlan) {
        return promptTemplateService.render(PromptTemplateNames.AGENT_QUESTION, Map.of(
            "currentDateText", StrUtil.blankToDefault(executionPlan.getCurrentDateText(), ""),
            "requiresCurrentDateAnchoring", executionPlan.isRequiresCurrentDateAnchoring(),
            "requiresFreshSearch", executionPlan.isRequiresFreshSearch(),
            "hasHistorySummary", StrUtil.isNotBlank(executionPlan.getHistorySummary()),
            "historySummary", StrUtil.blankToDefault(executionPlan.getHistorySummary(), ""),
            "question", StrUtil.blankToDefault(executionPlan.getOriginalQuestion(), "")
        ));
    }

    private String formatCurrentDate(LocalDate currentDate) {

        return currentDate + "（" + chineseWeekday(currentDate.getDayOfWeek()) + "）";
    }

    private String chineseWeekday(DayOfWeek dayOfWeek) {

        return switch (dayOfWeek) {
            case MONDAY -> "星期一";
            case TUESDAY -> "星期二";
            case WEDNESDAY -> "星期三";
            case THURSDAY -> "星期四";
            case FRIDAY -> "星期五";
            case SATURDAY -> "星期六";
            case SUNDAY -> "星期日";
        };
    }

    private void safeEmit(Sinks.Many<String> sink, String payload) {

        SinkEmitHelper.emitNext(sink, payload);
    }

    private void safeComplete(Sinks.Many<String> sink) {

        SinkEmitHelper.emitComplete(sink);
    }

    private List<String> snapshotStringList(List<String> source) {
        synchronized (source) {
            return List.copyOf(source);
        }
    }

    private List<SearchReference> snapshotReferenceList(List<SearchReference> source) {
        synchronized (source) {
            return new ArrayList<>(source);
        }
    }

    private List<String> snapshotUsedTools(Set<String> source) {
        return new ArrayList<>(source);
    }

    public List<RetrievalResultView> getRetrievalResults(String conversationId, long exchangeId) {
        return retrievalObserveStore.listResults(conversationId, exchangeId);
    }

    public List<ChannelExecutionView> getChannelExecutions(String conversationId, long exchangeId) {
        return retrievalObserveStore.listChannelExecutions(conversationId, exchangeId);
    }

    public List<StageBenchmarkView> getStageBenchmarks() {
        return stageBenchmarkService.listAll();
    }

    private void safeRefreshConversationSummary(String conversationId) {
        try {
            conversationMemoryService.refreshConversationSummaryAsync(conversationId);
        }
        catch (RuntimeException exception) {
            log.warn("刷新会话摘要失败, conversationId={}", conversationId, exception);
        }
    }

}
