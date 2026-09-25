<p align="center">
  <strong>企业级多编排 AI 智能体平台</strong><br />
  <span>文档知识问答 · 混合检索 · ReAct 工具调用 · 全链路可观测</span>
</p>

<p align="center">
  <a href="./LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-4a9b8f?style=flat-square" /></a>
  <img alt="Java" src="https://img.shields.io/badge/Java-17-df5b46?style=flat-square&logo=openjdk" />
  <img alt="Spring Boot" src="https://img.shields.io/badge/Spring%20Boot-3.5.6-6db33f?style=flat-square&logo=springboot&logoColor=white" />
  <img alt="Spring AI" src="https://img.shields.io/badge/Spring%20AI-1.1.0-6db33f?style=flat-square" />
  <img alt="Vue 3" src="https://img.shields.io/badge/Vue-3-42b883?style=flat-square&logo=vuedotjs&logoColor=white" />
  <img alt="Agentic RAG" src="https://img.shields.io/badge/Agentic%20RAG-Enterprise-2d6a8a?style=flat-square" />
  <img alt="ReAct Agent" src="https://img.shields.io/badge/ReAct%20Agent-Tool%20Calling-e87545?style=flat-square" />
  <img alt="Hybrid Search" src="https://img.shields.io/badge/Hybrid%20Search-RRF%20Fusion-7c6ee6?style=flat-square" />
  <img alt="Knowledge Graph" src="https://img.shields.io/badge/Knowledge%20Graph-Neo4j-4581c3?style=flat-square&logo=neo4j&logoColor=white" />
</p>

# Neo Agent

**Neo Agent** 是一个企业级 AI 智能体对话平台，覆盖文档知识治理、知识路由、混合检索、证据驱动生成、ReAct 工具调用与全链路可观测的完整闭环。

核心设计思想是 **「确定性编排 + 场景化执行」**：先用确定性的编排逻辑做好决策，再把执行交给最合适的引擎 —— 而不是把所有问题都丢给 Agent。

![对话工作台](docs/images/screenshot-chat.png)

---

## 功能特性

| 能力 | 说明 |
| :--- | :--- |
| **多编排执行** | 前置编排器五步决策链，按场景分流到五种执行器；新增模式只需加一个实现类 |
| **Agentic RAG** | 问题改写 → 子问题拆分 → 双通道检索 → RRF 融合 → 父子块聚合 → 证据预算 → 引用可追溯 |
| **知识路由** | 三级漏斗（Scope → Topic → Document）自动锁定文档；置信度不足时主动澄清而非硬猜 |
| **结构图导航** | Neo4j 构建 `Document → Section → Item` 层级图谱，支持章节定位、邻接遍历、编号项取证 |
| **ReAct Agent** | 联网搜索 + 工具调用 + Checkpoint 持久化，带模型 / 工具调用次数护栏与重试兜底 |
| **文档治理** | Tika 多格式解析 → 组合式切块（结构 / 递归 / 语义 / LLM）→ 向量 + 倒排双引擎索引，Kafka 异步流水线 |
| **会话记忆** | 无记忆 / 滑动窗口 / 摘要压缩三种策略，增量摘要保证单次成本恒定 |
| **流式输出** | SSE 六类事件：思考进度 / 正文分片 / 引用来源 / 推荐追问 / 状态 / 错误 |
| **可观测** | 全链路 Trace：阶段耗时、检索通道命中、证据来源、模型用量与成本 |
| **集群安全** | Redis 租约 + JVM 注册表双层防重，租约自动续期，收尾统一释放不留孤儿锁 |

---

## 架构设计

### 整体架构

```mermaid
flowchart TD
    U["用户提问"] --> CTRL["BusinessChatController<br/>POST /api/chat/stream"]
    CTRL --> SVC["BusinessChatService<br/>主链路总调度"]
    SVC --> LOCK{"Redis 租约 + JVM 注册表<br/>双层防重"}
    LOCK -->|"抢占失败"| REJ["返回「会话正在执行中」"]
    LOCK -->|"抢占成功"| ORCH["ChatPreparationOrchestrator<br/>前置编排五步决策"]
    ORCH --> PLAN["ConversationExecutionPlan<br/>执行计划"]
    PLAN --> REG["ConversationExecutorRegistry<br/>策略模式分发"]

    REG --> E1["ClarificationExecutor<br/>歧义澄清"]
    REG --> E2["GraphOnlyExecutor<br/>结构图直答"]
    REG --> E3["GraphThenEvidenceExecutor<br/>图定位取证"]
    REG --> E4["RagChatExecutor<br/>混合检索问答"]
    REG --> E5["ReactAgentExecutor<br/>ReAct Agent"]

    E4 --> ENG["RagRetrievalEngine"]
    ENG --> CH1["向量通道<br/>PGVector"]
    ENG --> CH2["关键词通道<br/>Elasticsearch"]
    E5 --> TOOL["Tavily 联网搜索"]

    E1 --> SSE["SSE 事件流<br/>thinking / text / reference / recommend"]
    E2 --> SSE
    E3 --> SSE
    E4 --> SSE
    E5 --> SSE
    SSE --> U
```

### 对话执行链路

```mermaid
sequenceDiagram
    autonumber
    participant B as 浏览器
    participant C as BusinessChatController
    participant S as BusinessChatService
    participant L as Redis 租约
    participant R as ChatRuntimeRegistry
    participant D as MySQL
    participant O as 前置编排器
    participant E as 执行器（五选一）

    B->>C: POST /api/chat/stream
    C->>S: openConversationStream()
    Note over S: Flux.defer 惰性包裹，此刻尚未执行
    B->>S: 订阅 SSE 流（真正触发）
    S->>S: buildLaunchPlan() 参数标准化
    S->>L: claimConversationLease() 跨实例防重
    alt 抢占失败
        L-->>B: error 事件：会话正在执行中
    else 抢占成功
        S->>D: startExchange() 落库占位 RUNNING
        S->>R: register() 进程内防重
        S->>S: bindClientChannel() 绑定 SSE 通道
        S->>S: activateGeneration() 启动租约续期（10s）
        S->>O: prepareExecutionPlan() 五步决策
        O-->>S: 执行计划（模式 / 改写问题 / 子问题 / 文档范围）
        S->>E: executor.execute()
        E-->>S: Flux 正文流
        S-->>B: text 事件（逐片推送）
        S->>D: completeExchange() 回填答案与状态
        S-->>B: reference + recommend 事件
        S->>L: releaseLease()
        S->>S: cleanup() 释放租约与注册表
    end
```

### 五种执行器

判断顺序：**歧义澄清 > 知识问答 > 开放式 Agent**，优先用最稳定的方式回答。

| 执行模式 | 触发条件 | 处理方式 |
| :--- | :--- | :--- |
| `CLARIFICATION` | 文档范围或意图存在歧义 | 返回澄清问题与候选，引导用户补充 |
| `GRAPH_ONLY` | 纯结构关系（"上一节""包含哪些章节"） | 只查 Neo4j，不调用大模型 |
| `GRAPH_THEN_EVIDENCE` | 编号项定位（"第几步""第几项"） | 图定位章节后读取条目证据 |
| `RETRIEVAL` | 常规知识问答 | 双通道混合检索 + 证据驱动生成 |
| `REACT_AGENT` | 需联网搜索 / 多步推理的开放问题 | ReAct 循环，自主决策与工具调用 |

### RAG 检索链路

```mermaid
flowchart TD
    Q["检索问题（改写后）"] --> SPLIT{"是否拆出多个子问题?"}
    SPLIT -->|"是"| PAR["子问题并行检索<br/>CompletableFuture · 12s 超时"]
    SPLIT -->|"否"| SINGLE["单问题检索"]
    PAR --> V["向量通道 PGVector<br/>topK = 8"]
    PAR --> K["关键词通道 Elasticsearch<br/>topK = 8"]
    SINGLE --> V
    SINGLE --> K
    V --> GV["向量闸门：相似度 ≥ 0.45<br/>（绝对阈值）"]
    K --> GK["关键词闸门：≥ topScore × 0.35<br/>（相对阈值）"]
    GV --> RRF["RRF 排名倒数融合<br/>score += 1 / (60 + rank + 1)"]
    GK --> RRF
    RRF --> PC["Parent-Child 聚合<br/>Child 命中 → Parent 取证"]
    PC --> RR{"是否启用外部 Rerank?"}
    RR -->|"是"| RERANK["精排 topN = 5"]
    RR -->|"否"| FINAL
    RERANK --> FINAL["finalTopK = 5 定稿"]
    FINAL --> EMPTY{"是否有有效证据?"}
    EMPTY -->|"否"| SHORT["无证据短路<br/>返回兜底话术，抑制幻觉"]
    EMPTY -->|"是"| BUDGET["证据预算裁剪<br/>单子问题 2200 / 总 5200 字符"]
    BUDGET --> PROMPT["组装 Prompt 并标注证据编号 [1][2]"]
    PROMPT --> GEN["模型基于证据流式生成"]
```

### ReAct Agent 执行流程

```mermaid
flowchart TD
    A["进入 ReactAgentExecutor"] --> B["组装 agentQuestion<br/>注入当前日期、历史摘要与时效性标记"]
    B --> C["ReactAgent.stream() 启动 ReAct 推理循环"]
    C --> D{"模型判断是否需要工具?"}
    D -->|"需要"| E["调用 Tavily 联网搜索"]
    E --> F{"ToolCallLimitHook<br/>工具调用 ≤ 6 次?"}
    F -->|"否"| G["拒绝调用并回退"]
    F -->|"是"| H["ToolRetryInterceptor<br/>指数退避重试（最多 2 次）"]
    H --> I{"执行成功?"}
    I -->|"否"| J["ToolErrorInterceptor 异常兜底"]
    I -->|"是"| K["工具结果回填上下文"]
    J --> K
    K --> L{"ModelCallLimitHook<br/>模型调用 ≤ 8 次?"}
    L -->|"否"| M["中断并回收资源"]
    L -->|"是"| D
    D -->|"不需要"| N["输出最终回答"]
    N --> O["SSE 逐片推送正文"]
    G --> D
```

### 文档知识闭环

```mermaid
flowchart LR
    subgraph IN["入库阶段"]
        direction LR
        U1["文档上传<br/>MinIO"] --> U2["Kafka<br/>异步触发"] --> U3["Tika<br/>多格式解析"] --> U4["组合式切块"] --> U5["向量化 + 倒排索引"] --> U6["Neo4j<br/>结构图谱"] --> U7["三级知识路由索引"]
    end

    subgraph OUT["检索阶段"]
        direction LR
        R1["用户提问"] --> R2["知识路由<br/>Scope → Topic → Document"] --> R3["混合检索 + 证据生成"]
    end

    U7 --> R2
    R3 --> OBS["影子路由观测<br/>系统推荐 vs 用户实际选择"]
    OBS -.->|"反向优化"| R2
```

组合式切块引擎的四种策略各司其职：

| 策略 | 角色 | 使用场景 |
| :--- | :--- | :--- |
| 结构切块 | 主干 | 按标题 / 章节 / 段落切成语义完整的块 |
| 递归分块 | 兜底 | 结构块过大时继续裁剪，控制块大小 |
| 语义分块 | 优化 | 在结构切块基础上做边界精修 |
| LLM 切块 | 增强 | 处理低质量或复杂文档，默认关闭 |

### 模块结构

```mermaid
flowchart LR
    ROOT["neo-agent"] --> BIZ["neo-agent-business"]
    ROOT --> COMMON["neo-agent-common"]
    ROOT --> IDGEN["neo-agent-id-generator-framework"]
    ROOT --> REDIS["neo-agent-redis-tool-framework"]
    ROOT --> REDISSON["neo-agent-redisson-framework"]
    ROOT --> VUE["vue"]
    ROOT --> SQL["sql"]

    BIZ --> CHAT["neo-agent-business-chat<br/>对话业务核心"]
    CHAT --> CHATAGENT["chatagent<br/>对话主链路 / RAG 引擎"]
    CHAT --> MANAGE["manage<br/>文档生命周期 / 知识路由"]
    CHAT --> AUTH["auth · prompt"]

    COMMON --> CF["neo-agent-common-frame<br/>统一响应 / 全局异常"]
    COMMON --> CW["neo-agent-common-web<br/>MyBatis-Plus / Swagger"]

    REDIS --> RC["neo-agent-redis-common-framework"]
    REDIS --> RF["neo-agent-redis-framework"]

    REDISSON --> RSF["neo-agent-redisson-service-framework"]
    REDISSON --> DQ["neo-agent-service-delay-queue-framework"]
    RSF --> LOCK["neo-agent-service-lock-framework"]
    RSF --> LEASE["neo-agent-service-lease-framework"]
    RSF --> REPEAT["neo-agent-repeat-execute-limit-framework"]
    RSF --> RCOMMON["neo-agent-redisson-common-framework"]
```

---

## 技术栈

| 类别 | 技术 |
| :--- | :--- |
| 基础框架 | JDK 17 · Spring Boot 3.5.6 · Spring AI 1.1.0 · Spring AI Alibaba 1.1.2.0 |
| 业务存储 | MySQL · MyBatis-Plus |
| 检索存储 | PostgreSQL + PGVector（向量）· Elasticsearch（关键词 / 倒排） |
| 图存储 | Neo4j（文档结构图谱 + 知识路由） |
| 缓存与协调 | Redis · Redisson |
| 消息与对象存储 | Kafka · MinIO |
| 文档解析 | Apache Tika 3.2.3 |
| 模型服务 | 阿里云百炼 DashScope（对话 + Embedding）· Tavily（联网搜索）· SiliconFlow（Rerank，可选） |
| 前端 | Vue 3 · Vite · Tailwind CSS · shadcn-vue · Cytoscape |
| 工具链 | Reactor · Knife4j · Lombok · Hutool · JWT · Log4j2 |

---

## 快速开始

### 环境要求

| 依赖 | 版本 | 默认端口 |
| :--- | :--- | :--- |
| JDK | 17+ | — |
| Maven | 3.8+ | — |
| Node.js | 18+ | — |
| MySQL | 8.x | 3307 |
| PostgreSQL + PGVector | 16 / 0.7+ | 5432 |
| Elasticsearch | 8.x（建议装 IK 分词插件） | 9200 |
| Neo4j | 5.x | 7687 |
| Redis | 7.x | 6379 |
| Kafka | 3.x | 9092 |
| MinIO | 最新版 | 9000 |

> 端口与地址均可在 `neo-agent-business/neo-agent-business-chat/src/main/resources/application.yaml` 中调整。

### 1. 初始化数据库

MySQL：

```bash
mysql -h127.0.0.1 -P3307 -uroot -p < sql/Mysql/create_database_mysql.sql
mysql -h127.0.0.1 -P3307 -uroot -p < sql/Mysql/create_table_mysql.sql
```

PostgreSQL（PGVector）：

```bash
psql -h127.0.0.1 -p5432 -U postgres -f sql/PostgresSql/create_database_postgres_sql.sql
psql -h127.0.0.1 -p5432 -U postgres -d neo_agent_pgvector -f sql/PostgresSql/create_table_postgres_sql.sql
```

Elasticsearch 索引与 Neo4j 图谱由应用启动时自动创建，无需手动初始化。

### 2. 配置密钥

配置文件中的敏感项均通过环境变量注入，启动前需设置：

```bash
# 必填：阿里云百炼（对话 + Embedding）
export ALI_BAI_LIAN_API_KEY=sk-xxxxxxxx

# 必填：Tavily 联网搜索
export TAVILY_API_KEY=tvly-xxxxxxxx

# 中间件密码（按本机实际配置，不设置则使用默认值）
export MYSQL_PASSWORD=root
export POSTGRES_PASSWORD=postgres
export ELASTICSEARCH_PASSWORD=elastic
export NEO4J_PASSWORD=12345678
export REDIS_PASSWORD=

# 管理后台账号（默认 admin / admin123456）
export NEO_AGENT_ADMIN_USERNAME=admin
export NEO_AGENT_ADMIN_PASSWORD=admin123456

# 可选：外部 Rerank 精排
export RERANK_API_KEY=sk-xxxxxxxx
```

### 3. 启动后端

```bash
# 安装各依赖模块到本地仓库
mvn clean install -DskipTests

# 启动对话业务服务（端口 9082）
mvn -pl neo-agent-business/neo-agent-business-chat spring-boot:run
```

也可以打包后运行：

```bash
java -jar neo-agent-business/neo-agent-business-chat/target/neo-agent-business-chat-0.0.1-SNAPSHOT.jar
```

接口文档（Knife4j）：<http://localhost:9082/doc.html>

### 4. 启动前端

```bash
cd vue
npm install
npm run dev
```

前端开发服务器为 `5173` 端口，已配置 `/api`、`/admin/auth`、`/manage` 到后端 `9082` 的代理；如需指向其他后端地址，设置 `VITE_PROXY_TARGET`。

### 5. 访问地址

| 入口 | 地址 |
| :--- | :--- |
| 对话工作台 | <http://localhost:5173/chat> |
| 管理后台 | <http://localhost:5173/admin/login> |
| 后端接口文档 | <http://localhost:9082/doc.html> |

### 6. 首次使用

1. 登录管理后台 → **知识路由**，创建知识域（Scope）与主题（Topic）
2. → **文档管理**，上传文档并配置知识域、业务分类与标签
3. 等待 Kafka 异步流水线完成解析 → 查看系统推荐的切块策略 → 确认后构建索引
4. 回到**对话工作台**提问，即可看到检索证据、引用来源与推荐追问

---

## 项目结构

```
neo-agent/
├── neo-agent-business/
│   └── neo-agent-business-chat/          # 对话业务核心
│       └── src/main/java/com/sopengin/neo/ai/
│           ├── chatagent/
│           │   ├── controller/           # 入口 BusinessChatController
│           │   ├── service/              # BusinessChatService 主链路调度
│           │   ├── rag/                  # RAG 引擎
│           │   │   ├── executor/         # 五种执行器
│           │   │   ├── retrieve/channel/ # 双通道检索
│           │   │   └── service/          # 编排、检索、Prompt 组装
│           │   ├── tool/                 # Tavily 联网搜索
│           │   └── support/              # SSE 事件输出
│           ├── manage/                   # 文档生命周期 + 知识路由
│           ├── auth/                     # 管理后台鉴权
│           └── prompt/                   # Prompt 模板加载
├── neo-agent-common/                     # 通用能力（frame / web）
├── neo-agent-redisson-framework/         # 分布式锁、租约、延迟队列
├── neo-agent-redis-tool-framework/       # Redis 工具封装
├── neo-agent-id-generator-framework/     # 分布式 ID 生成
├── vue/                                  # 前端（Vue 3 + Vite）
├── sql/                                  # 建表脚本（MySQL / PostgresSql）
├── samples/                              # 知识库样例文档
└── docs/                                 # 架构文档与界面截图
```

架构细节见 [`docs/architecture/`](docs/architecture/README.md)。

---

## License

[Apache License 2.0](LICENSE)
