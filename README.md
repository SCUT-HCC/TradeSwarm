# TradeSwarm - 多智能体量化交易系统

## 项目概述

TradeSwarm是一个基于多智能体协作的量化交易系统，采用完全并行的Pipeline架构设计。系统包含6个独立的Pipeline，所有Pipeline同时启动并行运行，通过SQLite数据库实现解耦通信。每条Pipeline内部是一个完整的Agent工作流(Workflow)，通过多个Agent的顺序协作完成特定的信息处理或决策任务。

## 核心特性

- **完全并行Pipeline架构**：6个Pipeline同时启动并行运行，通过SQLite数据库解耦通信
- **数据库驱动解耦**：Pipeline间零直接依赖，通过轮询数据库实现数据共享和协调
- **专业化Agent分工**：每个Agent专注特定领域，Pipeline内部通过顺序Workflow协作
- **迭代优化机制**：Research Pipeline内部通过Bullish/Bearish Agent多轮迭代优化投资建议
- **风险管理集成**：Trading Pipeline内置Trader/Risk Officer/Manager三阶段决策流程
- **异步执行框架**：基于asyncio实现真正的并发执行，所有Pipeline同时运行

## 系统架构

### 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Main Entry (asyncio.run)                        │
│                      asyncio.gather() 并行启动6个Pipeline                  │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                ┌────────────────┴────────────────┐
                │                                 │
       ┌────────▼────────┐              ┌────────▼────────┐
       │ 信息采集Pipeline组 │              │ 分析决策Pipeline组 │
       │   (4个并行)       │              │   (2个并行)       │
       └────────┬────────┘              └────────┬────────┘
                │                                 │
    ┌───────────┼───────────┬─────────────┐      │
    │           │           │             │      │
┌───▼───┐   ┌──▼───┐   ┌───▼───┐   ┌────▼────┐ │
│Market │   │Social│   │ News  │   │Fundamen-│ │
│Pipeline   │Pipeline   │Pipeline   │tals     │ │
│       │   │      │   │       │   │Pipeline │ │
└───┬───┘   └──┬───┘   └───┬───┘   └────┬────┘ │
    │          │           │             │      │
    │  ┌───────┴───────────┴─────────────┘      │
    │  │                                         │
    └──┼─────────────────────────────────────────┤
       │                                         │
       │         ┌───────────────┐               │
       │         │               │               │
       └─────────►  Database     ◄───────────────┘
                 │  Manager      │
                 │  (单例模式)    │
                 └───────┬───────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   ┌────▼─────┐    ┌────▼─────┐    ┌────▼─────┐
   │ Write    │    │ Write    │    │  Read    │
   │  Queue   │───►│  Worker  │    │ Methods  │
   │(异步队列) │    │(后台协程) │    │(并发读取) │
   └──────────┘    └────┬─────┘    └──────────┘
                        │
                        ▼
                ┌───────────────┐
                │  SQLite DB    │
                │  (WAL模式)    │
                │tradeswarm.db  │
                └───────────────┘
                        ▲
                        │
        ┌───────────────┴───────────────┐
        │                               │
   ┌────▼────────┐               ┌─────▼──────┐
   │  Research   │               │  Trading   │
   │  Pipeline   │               │  Pipeline  │
   │             │               │            │
   │ • Bullish   │               │ • Trader   │
   │ • Bearish   │               │ • Risk     │
   │ (迭代优化)   │               │ • Manager  │
   └─────────────┘               └────────────┘
```

### Pipeline设计

系统包含6个完全并行运行的Pipeline，通过SQLite数据库实现解耦通信。所有Pipeline同时启动，需要前置数据的Pipeline通过轮询数据库等待数据就绪。

![workflow](./docs/access/Workflow.png)

### Pipeline解耦机制

**并行执行模式**：
所有Pipeline通过`asyncio.gather()`同时启动并行运行，不存在层级执行顺序。系统启动时6个Pipeline完全并发，各自独立执行。

**数据通信机制**：
- Pipeline间通过SQLite数据库完全解耦，零直接调用
- 每个Pipeline将输出写入`pipeline_outputs`表
- 需要前置数据的Pipeline主动轮询数据库，等待所需数据就绪
- 使用`session_id`关联同一决策周期的所有数据

**Pipeline内部结构**：
每个Pipeline是一个顺序执行的Workflow，包含多个Agent的串行工作流程。例如Research Pipeline内部流程：
1. 轮询数据库等待4个数据源就绪（market/social/news/fundamentals）
2. Bullish Agent分析看涨信号
3. Bearish Agent分析看跌信号
4. 两个Agent迭代优化分析结论
5. 将最终报告写入数据库供Trading Pipeline使用

### Pipeline与Agent职责分工

为便于理解，将6个Pipeline按功能分为三组（注意：这只是逻辑分组，实际执行时所有Pipeline完全并行）：

#### 信息采集Pipeline组（4个并行Pipeline）

所有信息采集Pipeline同时启动，独立处理各自数据源，将分析结果写入数据库。

**Pipeline 1: Market Pipeline**
- 包含Agent：Market Agent
- 职责：处理实时市场数据、价格走势、成交量等技术指标
- 数据库输出：`market_analysis`类型的分析报告

**Pipeline 2: Social Pipeline**
- 包含Agent：Social Apps Agent
- 职责：监控社交媒体舆情、投资者情绪、热点话题
- 数据库输出：`social_analysis`类型的情绪分析报告

**Pipeline 3: News Pipeline**
- 包含Agent：News Report Agent
- 职责：分析财经新闻、行业动态、政策变化
- 数据库输出：`news_analysis`类型的新闻摘要与影响评估

**Pipeline 4: Fundamentals Pipeline**
- 包含Agent：Fundamentals Agent
- 职责：评估公司基本面、财务数据、行业地位
- 数据库输出：`fundamentals_analysis`类型的基本面分析报告

#### 研究分析Pipeline（1个Pipeline，内部迭代Workflow）

**Pipeline 5: Research Pipeline**
- 包含Agent：Bullish Analyst Agent + Bearish Analyst Agent
- 数据依赖：从数据库查询上述4类分析报告（`market_analysis`, `social_analysis`, `news_analysis`, `fundamentals_analysis`）
- 内部Workflow：
  1. 轮询数据库等待4个数据源就绪
  2. Bullish Agent识别看涨信号，分析上涨潜力、支撑位、催化剂
  3. Bearish Agent识别看跌信号，分析下跌风险、阻力位、负面因素
  4. 两个Agent相互审视对方报告，迭代优化分析结论
  5. 达到收敛条件或最大迭代次数后终止
- 数据库输出：`research_report`类型的综合投资建议

#### 交易决策Pipeline（1个Pipeline，内部三阶段Workflow）

**Pipeline 6: Trading Pipeline**
- 包含Agent：Trader Agent + Risk Officer Agent + Manager Agent
- 数据依赖：从数据库查询`research_report`
- 内部Workflow：
  1. 轮询数据库等待Research Pipeline的输出
  2. Trader Agent制定交易策略，生成交易指令草案
  3. Risk Officer Agent评估风险敞口、资金管理、止损止盈
  4. Manager Agent整合意见，做出最终交易决定
- 数据库输出：`trading_decision`类型的最终交易决策

### 核心组件

```
TradeSwarm/
├── core/                          # 核心框架
│   ├── agent/                     # Agent基础组件
│   │   ├── base_agent.py          # BaseAgent基类（三阶段执行框架）
│   │   ├── config_validator.py    # 配置验证器
│   │   └── prompt_manager.py      # Prompt模板管理器
│   │
│   ├── pipelines/                 # Pipeline工作流（待实现）
│   │   ├── base_pipeline.py       # BasePipeline基类（解耦机制）
│   │   ├── market_pipeline.py     # Market数据处理Pipeline
│   │   ├── social_pipeline.py     # Social媒体分析Pipeline
│   │   ├── news_pipeline.py       # News报道分析Pipeline
│   │   ├── fundamentals_pipeline.py # Fundamentals分析Pipeline
│   │   ├── research_pipeline.py   # Research迭代分析Pipeline
│   │   └── trading_pipeline.py    # Trading决策生成Pipeline
│   │
│   ├── storage/                   # 数据存储（待实现）
│   │   ├── database.py            # DatabaseManager（SQLite管理）
│   │   └── schema.py              # 数据库表结构定义
│   │
│   └── __init__.py                # 核心模块导出
│
├── tools/                         # 工具系统
│   ├── builtin_tools.py           # CAMEL内置工具封装
│   ├── registry.py                # 工具注册表（单例模式）
│   └── __init__.py
│
├── utils/                         # 工具函数
│   ├── config.py                  # 环境配置管理
│   └── __init__.py
│
├── configs/                       # Agent配置文件（待实现）
│   └── trading_agents/            # 交易Agent配置
│       ├── market_agent.json           # Market Agent配置
│       ├── social_agent.json           # Social Apps Agent配置
│       ├── news_agent.json             # News Report Agent配置
│       ├── fundamentals_agent.json     # Fundamentals Agent配置
│       ├── bullish_agent.json          # Bullish Analyst配置
│       ├── bearish_agent.json          # Bearish Analyst配置
│       ├── trader_agent.json           # Trader Agent配置
│       ├── risk_agent.json             # Risk Officer配置
│       └── manager_agent.json          # Manager Agent配置
│
├── main.py                        # 主程序入口
├── README.md                      # 项目文档
└── .env                           # 环境变量配置
```

### 设计原则

- **完全解耦**：Pipeline间通过数据库通信，零直接依赖，易于扩展和维护
- **并行优先**：所有Pipeline同时启动，最大化系统吞吐量和响应速度
- **专业化分工**：每个Agent专注特定领域，Pipeline内部通过Workflow协作
- **数据驱动**：Pipeline通过轮询数据库获取所需数据，自动等待前置任务完成
- **迭代优化**：Research Pipeline内部通过多轮Agent迭代提升分析质量
- **风险优先**：Trading Pipeline内置三阶段决策流程，确保风险管理
- **可观测性**：完整记录Pipeline执行和Agent交互日志，支持决策追溯
- **容错机制**：单个Pipeline失败不影响其他Pipeline运行（优雅降级）

## 技术实现

### BaseAgent三阶段执行框架

每个Agent内部采用统一的三阶段执行流程：

1. **Observing阶段**：观察与理解输入信息，可能调用工具获取上下文
2. **Planning阶段**：基于observation进行Chain-of-Thought推理，制定执行计划
3. **Acting阶段**：根据plan执行具体操作，调用工具并整合结果

### Pipeline并行执行与数据库解耦

**Pipeline并行启动**：
```python
# 所有Pipeline通过asyncio.gather()同时启动
await asyncio.gather(
    MarketPipeline(db, session_id).run(),
    SocialPipeline(db, session_id).run(),
    NewsPipeline(db, session_id).run(),
    FundamentalsPipeline(db, session_id).run(),
    ResearchPipeline(db, session_id).run(),
    TradingPipeline(db, session_id).run()
)
```

**数据库驱动协调**：
- 每个Pipeline将输出写入SQLite的`pipeline_outputs`表
- 需要数据的Pipeline轮询数据库等待所需数据就绪
- 使用`session_id`关联同一决策周期的所有数据
- 通过写队列避免SQLite并发写冲突

### 工具系统

采用插件化工具架构：
- **ToolRegistry**：全局工具注册表（单例模式）
- **内置工具**：基于CAMEL框架的SearchToolkit、FileWriteToolkit
- **配置驱动**：通过JSON配置文件启用/禁用工具

## 技术栈

### 核心技术

| 类别 | 工具/库 | 说明 |
|------|---------|------|
| **语言** | Python 3.12 | 主要开发语言 |
| **Agent框架** | CAMEL | 多Agent协作框架 |
| **LLM接口** | 阿里百联API | 兼容OpenAI格式，通过环境变量配置 |
| **并发机制** | asyncio | Python标准异步执行框架 |
| **数据存储** | SQLite + aiosqlite | 轻量级数据库，Pipeline解耦通信 |
| **日志系统** | logging | Python标准日志库 |
| **依赖管理** | uv | 快速Python包管理器 |

### 环境配置

#### 虚拟环境设置

使用 `uv` 工具创建和管理 Python 虚拟环境：

```bash
# 进入项目目录
cd TradeSwarm

# 使用 Python 3.12 创建新的虚拟环境
uv venv --python 3.12

# 激活虚拟环境
source .venv/bin/activate

# 同步现有安装包
uv sync

# 安装rust，不然安装camel时候会报错
# macOS
brew install rust

# Linux
sudo apt update
sudo apt install rustc cargo

# 安装 camel-ai 及其所有依赖，如果时间不够还可以增加时间
UV_HTTP_TIMEOUT=900 uv pip install 'camel-ai[all]'

# 安装异步SQLite库（Pipeline解耦所需）
uv pip install aiosqlite
```

#### 环境变量配置

项目使用`.env`文件管理环境变量，主要配置项：

```bash
# 阿里百联API配置
ALIBABA_API_KEY=your_api_key_here
ALIBABA_API_BASE=https://dashscope.aliyuncs.com/compatible-mode/v1
ALIBABA_MODEL_NAME=qwen-max

# 可选：多模型配置
ALIBABA_MODEL_CHEAP=qwen-turbo      # 简单任务使用
ALIBABA_MODEL_POWERFUL=qwen-max     # 复杂任务使用
```

详细配置模板参见`.env.example`文件。

## 快速开始

### 运行示例

```bash
# 运行完整的Pipeline并行执行演示
python main.py
```

## 开发路线图

### 阶段0：基础设施层
- [x] **BaseAgent三阶段执行框架**：实现Observing-Planning-Acting统一执行流程，为所有Agent提供标准化工作模式
- [x] **工具系统与配置管理**：构建插件化工具注册表和JSON配置驱动的Agent配置验证机制

### 阶段1：数据存储与Pipeline基础设施
- [ ] **DatabaseManager数据库管理器**：实现异步写队列模式和WAL模式SQLite管理，支持Pipeline并发安全访问
- [ ] **数据库Schema设计**：定义pipeline_outputs表结构，支持session_id关联和多类型数据存储
- [ ] **BasePipeline基类**：实现数据库轮询等待机制和Pipeline生命周期管理，为所有Pipeline提供统一接口

### 阶段2：信息采集Pipeline组（并行开发）
- [ ] **Market Agent & Pipeline**：实现市场数据分析Agent及其Pipeline，输出技术指标和价格走势分析
- [ ] **Social Agent & Pipeline**：实现社交媒体监控Agent及其Pipeline，输出情绪分析和热点话题报告
- [ ] **News Agent & Pipeline**：实现新闻分析Agent及其Pipeline，输出财经新闻摘要和影响评估
- [ ] **Fundamentals Agent & Pipeline**：实现基本面分析Agent及其Pipeline，输出公司财务和行业地位评估

### 阶段3：研究分析Pipeline（迭代机制）
- [ ] **Bullish Analyst Agent**：实现看涨分析Agent，识别上涨潜力、支撑位和催化剂
- [ ] **Bearish Analyst Agent**：实现看跌分析Agent，识别下跌风险、阻力位和负面因素
- [ ] **Research Pipeline**：实现双Agent迭代优化Workflow，包含数据等待、迭代分析和收敛判断逻辑

### 阶段4：交易决策Pipeline（三阶段决策流程）
- [ ] **Trader Agent**：实现交易策略制定Agent，生成交易指令草案和仓位建议
- [ ] **Risk Officer Agent**：实现风险评估Agent，评估风险敞口、资金管理和止损止盈方案
- [ ] **Manager Agent**：实现最终决策Agent，整合交易员和风控意见，做出最终交易决定
- [ ] **Trading Pipeline**：实现三阶段决策Workflow，包含数据等待和顺序决策流程

### 阶段5：数据接口与系统集成
- [ ] **市场数据接口集成**：对接实时行情API，为Market Pipeline提供数据源
- [ ] **社交媒体数据采集**：对接Twitter/Reddit等平台API，为Social Pipeline提供数据源
- [ ] **新闻数据接口集成**：对接财经新闻API，为News Pipeline提供数据源
- [ ] **财务数据接口集成**：对接财务报表API，为Fundamentals Pipeline提供数据源

### 阶段6：监控、测试与优化
- [ ] **Pipeline监控与日志系统**：实现Pipeline执行状态追踪、Agent交互日志记录和决策过程可视化
- [ ] **回测系统**：构建历史数据回测框架，验证交易决策质量和系统稳定性
- [ ] **风险控制优化**：基于回测结果优化Risk Officer策略，完善资金管理和止损机制

## 研究规范

本项目遵循研究型代码开发规范：

- **MVP开发准则**：不使用默认值和测试代码，避免逻辑错误被掩盖
- **类型注解规范**：所有方法签名包含完整类型注解
- **文档规范**：所有模块、类、方法包含中文docstring
- **代码组织**：使用阶段化注释组织复杂逻辑
- **命名规范**：遵循Python PEP 8标准

## 许可证

本项目采用MIT许可证开源。

## 参考资料

- [TradingAgents](https://github.com/TauricResearch/TradingAgents) - 多智能体量化交易系统的相关参考实现

## 联系方式

如有问题或建议，欢迎通过GitHub Issues联系。
