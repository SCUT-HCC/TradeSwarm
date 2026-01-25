# TradeSwarm 项目历史文档

## 项目概述

**TradeSwarm** 是一个基于多智能体（Multi-Agent）架构的 A 股市场交易决策系统，从 **TradingAgents** 项目重构而来，专注于中国 A 股市场的分析和交易。

---

## 项目发展时间线

### 阶段一：项目起源与架构设计（2025年初）

#### 1.1 项目背景
- **参考项目**: TradingAgents
  - 一个基于 LangGraph 的多智能体交易系统
  - 使用 yfinance、alpha_vantage 等数据源
  - 主要面向美股市场

#### 1.2 重构目标
1. **数据源适配**: 从美股数据源（yfinance, alpha_vantage）迁移到 A 股数据源（tushare, akshare）
2. **架构优化**: 
   - 实现并行、持续的数据采集机制
   - 分析团队独立于下游团队，定期从数据源获取信息
   - 决策层可直接访问原始数据存储（Data Store）作为补充机制
3. **技术栈升级**: 全面使用 LangChain 1.0.3 API

#### 1.3 核心架构设计
```
┌─────────────────────────────────────────────────────────┐
│                    TradeSwarm 架构                       │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────────────────────────────────────────────┐  │
│  │  数据采集层 (Background Layer)                    │  │
│  │  - 并行、持续的数据采集                            │  │
│  │  - 数据版本化（时间戳）                            │  │
│  │  - SQLite Data Store                              │  │
│  └──────────────────────────────────────────────────┘  │
│                        ↓                                 │
│  ┌──────────────────────────────────────────────────┐  │
│  │  分析层 (Analysis Layer)                          │  │
│  │  - LangChain Agents                               │  │
│  │  - 从 Data Store 读取数据                          │  │
│  │  - Fallback 到外部数据源（Tushare）                │  │
│  └──────────────────────────────────────────────────┘  │
│                        ↓                                 │
│  ┌──────────────────────────────────────────────────┐  │
│  │  决策层 (Decision Layer)                          │  │
│  │  - LangGraph Pipelines                           │  │
│  │  - 可访问原始 Data Store（Shortcut 机制）          │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

---

### 阶段二：数据源集成与工具框架（2025年初）

#### 2.1 Tushare 数据源集成

**目标**: 集成 Tushare 作为 A 股市场的主要数据源

**实现内容**:
- 创建 `BaseDataProvider` 抽象基类
- 实现 `TushareProvider`:
  - 支持 A 股股票代码转换（如 `000001` → `000001.SZ`）
  - 异步封装同步 Tushare API
  - 使用 `pro.daily()` 获取 K 线数据

**关键文件**:
```
core/data_sources/
├── base_provider.py      # 数据源抽象基类
└── tushare_provider.py   # Tushare 实现
```

#### 2.2 工具框架设计

**设计理念**: 
- 工具优先从 Data Store 读取数据
- 如果 Data Store 没有数据，fallback 到 Tushare
- 从外部获取的数据自动缓存到 Data Store

**实现内容**:
- `market_tools.py`: `get_stock_data` 工具
- `technical_tools.py`: `get_indicators` 工具（支持 MA, RSI, MACD 等）

**关键文件**:
```
core/tradingagents/tools/
├── market_tools.py       # 市场数据工具
└── technical_tools.py    # 技术指标工具
```

#### 2.3 数据存储扩展

**新增功能**:
- `collector_data` 表：存储原始采集数据
- 支持数据版本化（通过时间戳）
- 提供 `get_latest_collector_data` 和 `get_collector_data` 方法

**关键文件**:
```
core/storage/
├── schema.py    # 数据库模式定义（包含 collector_data 表）
└── database.py  # 数据库管理器（扩展了数据采集相关方法）
```

---

### 阶段三：Agent 开发与集成（2025年初）

#### 3.1 Market Analyst 开发

**参考**: TradingAgents 的 `market_analyst.py`

**实现内容**:
- 使用 LangChain 1.0.3 的 `create_agent` API
- 集成 `get_stock_data` 和 `get_indicators` 工具
- 适配 A 股市场（股票代码格式、数据源）

**关键决策**:
- 保持与 TradingAgents 相似的 agent 结构，便于迁移
- 使用 `core.tradingagents.*` 导入路径（后迁移到 `tradingagents.*`）

#### 3.2 项目结构迁移

**从**:
```
TradeSwarm/
└── core/
    ├── tradingagents/
    ├── data_sources/
    └── storage/
```

**到**:
```
TradeSwarm/
└── tradingagents/
    ├── agents/
    ├── dataflows/
    └── graph/
```

**原因**: 
- 简化项目结构
- 与 TradingAgents 项目结构保持一致
- 便于代码迁移和团队协作

---

### 阶段四：项目重构与清理（2025年初）

#### 4.1 代码清理
- 删除冗余的空壳代码
- 移除未使用的文档文件
- 统一导入路径

#### 4.2 当前项目状态

**已完成**:
- ✅ Market Analyst（市场分析师）
- ✅ Social Media Analyst（社交媒体分析师）
- ✅ News Analyst（新闻分析师）
- ⏳ Fundamentals Analyst（基本面分析师）- 待完成

**项目结构**:
```
TradeSwarm/
├── README.md
├── tradingagents/
│   ├── agents/
│   │   ├── analysts/          # 分析团队
│   │   │   ├── market_analyst.py
│   │   │   ├── social_media_analyst.py
│   │   │   ├── news_analyst.py
│   │   │   └── fundamentals_analyst.py (待完成)
│   │   ├── utils/
│   │   │   └── agent_states.py
│   │   └── init_llm.py
│   ├── dataflows/              # 数据流（待实现）
│   └── graph/                  # LangGraph 工作流（待实现）
```

---

## 技术栈演变

### 初始技术栈（TradingAgents）
- LangGraph（工作流编排）
- LangChain（Agent 框架）
- yfinance, alpha_vantage（数据源）
- 面向美股市场

### 当前技术栈（TradeSwarm）
- **LangChain 1.0.3**: Agent 创建和工具集成
- **LangGraph**: 工作流编排（待完善）
- **Tushare**: A 股市场数据源
- **SQLite (aiosqlite)**: 数据存储和版本管理
- **Pandas**: 数据处理
- **DashScope API (Qwen)**: LLM 服务

---

## 关键设计决策

### 1. 并行数据采集架构
**决策**: 分析团队独立于下游团队，持续从数据源获取信息

**原因**: 
- 确保数据实时性
- 避免下游团队调用时的延迟
- 支持数据版本化

**实现**: 
- 后台数据采集器（待实现）
- SQLite Data Store 存储原始数据
- 时间戳版本管理

### 2. 决策层直接数据访问（Shortcut）
**决策**: 决策层 agent 可直接访问原始 Data Store

**原因**: 
- 当分析结果不足时，提供补充数据源
- 增强决策的灵活性和可追溯性

**实现**: 
- `DatabaseManager` 提供 `get_latest_collector_data` 方法
- 决策层专用工具（待实现）

### 3. Fallback 机制
**决策**: 工具优先从 Data Store 读取，失败时 fallback 到 Tushare

**原因**: 
- 减少外部 API 调用
- 提高系统稳定性
- 自动缓存机制

**实现**: 
- `market_tools.get_stock_data` 实现 fallback 逻辑
- `technical_tools.get_indicators` 实现 fallback 逻辑

### 4. A 股市场适配
**决策**: 全面适配 A 股市场

**实现**: 
- 股票代码格式转换（6位数字 → Tushare 格式）
- 支持上海、深圳、创业板、科创板
- 使用 Tushare 作为主要数据源

---

## 重要里程碑

### ✅ 里程碑 1: 数据源集成完成
- **时间**: 2025年初
- **内容**: 
  - TushareProvider 实现
  - 工具框架搭建
  - Fallback 机制实现

### ✅ 里程碑 2: Market Analyst 集成
- **时间**: 2025年初
- **内容**: 
  - Market Analyst 工具集成
  - 测试验证通过

### ✅ 里程碑 3: 项目结构迁移
- **时间**: 2025年初
- **内容**: 
  - 从 `core/tradingagents` 迁移到 `tradingagents`
  - 代码清理和重构

### ⏳ 里程碑 4: 完整工作流实现（进行中）
- **内容**: 
  - Fundamentals Analyst 完成
  - LangGraph 工作流完整实现
  - 数据采集器实现

### ✅ 里程碑 5: Agent 工具重构与基本面落地
- **时间**: 2025年12月中
- **内容**:
  - AkShare/Tushare provider 重构；新闻工具微/宏拆分，Markdown 输出。
  - 基本面工具：公司信息、报表、财务指标、估值、业绩（预告/快报）；AkShare 优先，Tushare 兜底；输出 core/preview/meta。
  - `fundamentals_analyst` 中文提示补全并接入上述工具；示例运行 `python -m tradingagents.agents.analysts.fundamentals_analyst`。
  - 产物：`full_fundamentals_sample.json`、`full_fundamentals_sample_core.json`。

---

## 待完成工作

### 短期目标
1. **Fundamentals Analyst**: 完成基本面分析师的实现
2. **LangGraph 工作流**: 实现完整的 agent 工作流
3. **数据采集器**: 实现后台数据采集机制

### 中期目标
1. **决策层实现**: 实现交易决策和风险管理 agent
2. **数据流完善**: 实现完整的数据流管道
3. **测试覆盖**: 完善单元测试和集成测试

### 长期目标
1. **性能优化**: 优化数据采集和分析性能
2. **扩展性**: 支持更多数据源和分析指标
3. **生产部署**: 准备生产环境部署

---

## 参考资源

### 原始项目
- **TradingAgents**: 项目重构的参考基础
  - 多智能体架构设计
  - LangGraph 工作流编排
  - Agent 实现模式

### 文档
- **README.md**: 项目概述和运行说明
- **tradingagents/agents/analysts/README.md**: 分析师开发指南

### 外部资源
- **Tushare**: https://tushare.pro/
- **LangChain 1.0.3**: https://python.langchain.com/
- **LangGraph**: https://langchain-ai.github.io/langgraph/

---

## 版本历史

### v0.2.0 (2025年12月中)
- AkShare/Tushare provider 重构，新闻工具微/宏拆分，统一 Markdown。
- 基本面工具上线，AkShare 优先、Tushare 兜底，输出 core/preview/meta。
- fundamentals_analyst 中文提示完善并接入工具，附运行示例。
- 导出示例：`full_fundamentals_sample_core.json`。

### v0.1.0 (2025年初)
- 初始版本
- 完成数据源集成
- 完成 Market Analyst 工具集成
- 项目结构迁移

---

## 贡献者

- 项目架构设计
- Tushare 数据源集成
- 工具框架开发
- Market Analyst 实现

---

## 注意事项

1. **环境变量**: 需要设置 `TUSHARE_TOKEN` 和 `DASHSCOPE_API_KEY`
2. **数据存储**: 使用 SQLite 数据库，路径在配置中指定
3. **API 限制**: Tushare 有调用频率限制，注意缓存机制
4. **代码风格**: 遵循项目代码规范，使用中文注释

---

**最后更新**: 2025年12月中  
**文档维护**: 项目团队

