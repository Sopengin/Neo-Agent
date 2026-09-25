# AGENTS.md

本文件为 AI 编码助手（Claude Code / Cursor / Codex 等）提供项目上下文入口。

---

## 项目是什么

**Neo Agent** — 企业级 AI 智能体对话平台（Java + Spring Boot + Spring AI）。

覆盖：智能对话、文档知识问答、联网搜索、RAG 检索、MCP 工具协议、Skills 能力扩展、会话记忆管理、文档全生命周期治理。

**核心设计哲学：「确定性编排 + 场景化执行」**
先用确定性的编排逻辑做好决策，再把执行交给最合适的引擎。不是所有问题都丢给 Agent。

---

## ⭐ 先读这里：架构文档

**`docs/architecture/`** 是理解本项目最高效的入口：

| 你需要什么 | 读这个 |
|-----------|--------|
| 项目全貌与设计哲学 | [`docs/architecture/README.md`](docs/architecture/README.md) |
| **功能 → 类 → 方法 代码地图** | [`docs/architecture/05-功能与代码地图.md`](docs/architecture/05-功能与代码地图.md) |
| 完整请求链路与关键参数 | [`docs/architecture/01-核心架构设计.md`](docs/architecture/01-核心架构设计.md) |
| 知识路由 / Neo4j 图谱 | [`docs/architecture/03-图数据库与知识路由.md`](docs/architecture/03-图数据库与知识路由.md) |
| 工程化与选型理由 | [`docs/architecture/02-工程化技术概括.md`](docs/architecture/02-工程化技术概括.md) |

> **找代码位置时，优先查 `05-功能与代码地图.md` 的表格**，里面有功能、核心类、关键方法三列。

---

## 模块结构

```
neo-agent/
├── neo-agent-business/
│   └── neo-agent-business-chat/     # ⭐ AI 对话业务核心（大部分逻辑在这）
│       └── src/main/java/com/sopengin/neo/ai/
│           ├── chatagent/             # 对话主链路
│           │   ├── controller/        # 入口 BusinessChatController
│           │   ├── service/           # BusinessChatService 总调度
│           │   ├── rag/               # ⭐ RAG 引擎
│           │   │   ├── executor/      # 五种执行器
│           │   │   ├── retrieve/channel/  # 双通道检索
│           │   │   └── service/       # 编排、检索、Prompt 组装
│           │   ├── tool/              # Tavily 搜索
│           │   └── support/           # SSE 输出
│           └── manage/                # 文档生命周期 + 知识路由
├── neo-agent-common/                # 通用能力（frame / web）
├── neo-agent-redisson-framework/    # ⭐ 分布式锁、租约、延迟队列
├── neo-agent-redis-tool-framework/  # Redis 工具
├── neo-agent-id-generator-framework/# 雪花 ID
├── samples/                           # 知识库样例文档
├── vue/                               # 前端
└── sql/                               # 建表脚本（Mysql / PostgresSql）
```

---

## 核心链路（改代码前必看）

```
BusinessChatController.stream()
  → BusinessChatService.openConversationStream()
      ① buildLaunchPlan           参数标准化 + 生成租约标识
      ② claimConversationLease    Redis 租约（跨实例锁，TTL 30s）
      ③ bootstrapConversation     落库 + 建 Sink 通道 + JVM 任务注册
      ④ bindClientChannel         返回 Flux（此时还没执行）
      ⑤ activateGeneration        客户端订阅后才真正开跑
           → prepareExecutionPlan → ChatPreparationOrchestrator（五步决策）
           → conversationExecutorRegistry.get(mode)
           → executor.execute()   （RAG / 图查询 / ReAct Agent）
           → 逐片推 SSE
      ⑥ finishSuccessfully / finishWithFailure / stopTask
```

**关键类**：
- `BusinessChatService` — 总调度（约 1300 行）
- `ChatPreparationOrchestrator` — 五步决策链
- `ConversationExecutorRegistry` — 按 `ExecutionMode` 分发执行器
- `RagRetrievalEngine` — 双通道检索 + RRF 融合
- `KnowledgeRouteServiceImpl` — 三级漏斗知识路由

---

## 改代码时的注意事项

### 并发安全（很重要）
- 双层锁：**Redis 租约**（跨实例）+ **`ChatRuntimeRegistry`**（进程内）
- 收尾用 `finalized` 的 **CAS** 保证只执行一次（成功/失败/停止三条路径会竞争）
- 改动收尾逻辑时必须保证 `cleanup()` 一定被调用，**不能留孤儿锁**

### 线程模型
- `Schedulers.boundedElastic()` 用于阻塞操作（前置编排会调模型、查库）
- 用 `Sinks` 把手写流程桥接成 `Flux`（见 `SinkEmitHelper`）
- 上下文通过 `runnableConfig.context()`（`ChatContextKeys` 常量）向下传递

### RAG 相关
- 证据必须走预算裁剪（单子问题 2200 / 总 5200 / 单父块 2200 字符）
- **无证据必须短路**，不能放行让模型编造（防幻觉）
- 向量阈值 0.45 / 关键词相对阈值 0.35

### 其他
- 统一响应格式 `ApiResponse`，全局异常已拦截，业务代码不需要到处 try-catch
- Prompt 模板在 `src/main/resources/prompt/*.st`，用 `PromptTemplateService.render()` 加载
- 注释头统一格式（`@author` 已移除）：
  ```java
  /**
   * 配置类
   **/
  ```

---

## 技术栈

| 类别 | 技术 |
|------|------|
| 基础 | JDK 17、Spring Boot 3.5.6、Spring AI 1.1.0、Spring AI Alibaba 1.1.2.0 |
| 存储 | MySQL、PostgreSQL+PGVector、Elasticsearch、Neo4j、Redis、MinIO |
| 消息 | Kafka |
| 文档 | Apache Tika 3.2.3 |
| 模型 | 阿里云百炼 DashScope、Tavily（搜索）、SiliconFlow（Rerank） |
| 前端 | Vue 3 + Vite |
| 工具 | MyBatis-Plus、Redisson、Knife4j、Lombok、Hutool、JWT、Log4j2 |

---

## 参考

- 项目说明：[`README.md`](README.md)
- 架构文档：[`docs/architecture/README.md`](docs/architecture/README.md)
