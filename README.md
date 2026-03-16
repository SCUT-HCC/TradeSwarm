# TradeSwarm

基于多智能体架构的交易决策系统，采用 LangGraph 构建，支持连续自治运行、多智能体协作和长期记忆机制。

## 快速开始

```bash
# 1. 环境准备
conda create -n TradeSwarm python=3.12
conda activate TradeSwarm
pip install -r requirements.txt

# 2. 配置环境变量
export DASHSCOPE_API_KEY="your-api-key"
export ALPHA_VANTAGE_API_KEY="your-alpha-vantage-key"

# 3. 创建配置文件 config/config.yaml

# 4. 运行回测
python run_single_symbol_backtest.py \
    --symbol NVDA \
    --start 2025-11-06 \
    --end 2025-11-08 \
    --cash 100000 \
    --db memory.db \
    --output backtest_results_nvda
```

## 完整系统流程图

### 日级交易流程（完整）

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│  回测驱动器 (run_single_symbol_backtest.py)                                  │
│  按日期循环，每天依次执行以下步骤：                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  步骤 1: Analyst 节点（并行运行，图外执行）                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │ Market       │  │ News         │  │ Sentiment    │  │ Fundamentals │    │
│  │ Analyst      │  │ Analyst      │  │ Analyst      │  │ Analyst      │    │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘    │
│         │                  │                  │                  │            │
│         └──────────────────┴──────────────────┴──────────────────┘            │
│                                    │                                            │
│                                    ▼                                            │
│                         ┌──────────────────────┐                               │
│                         │ analyst_reports 表   │                               │
│                         │ (SQLite 数据库)      │                               │
│                         └──────────────────────┘                               │
└─────────────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  步骤 2: Pre-Open 决策图 (trading_graph.py) - LangGraph 内部                │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Summary 节点（串行）                                                  │  │
│  │  market_summary → news_summary → sentiment_summary →                 │  │
│  │  fundamentals_summary                                               │  │
│  │  （从 analyst_reports 表读取，生成结构化摘要）                        │  │
│  └───────────────────────────┬───────────────────────────────────────────┘  │
│                              │                                                │
│                              ▼                                                │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Research 子图（2 轮辩论）                                             │  │
│  │  Bull Researcher (R1) → Bear Researcher (R1) →                      │  │
│  │  Bull Researcher (R2) → Bear Researcher (R2) →                      │  │
│  │  Research Manager                                                    │  │
│  │  （整合 Analyst 摘要 + 多空辩论，生成 investment_plan）             │  │
│  └───────────────────────────┬───────────────────────────────────────────┘  │
│                              │                                                │
│                              ▼                                                │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Trader 节点                                                           │  │
│  │  （根据 Research Plan，生成交易方向和止盈止损规则）                   │  │
│  └───────────────────────────┬───────────────────────────────────────────┘  │
│                              │                                                │
│                              ▼                                                │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Strategy Selector 节点                                                │  │
│  │  （判断市场状态 market_regime，选择交易策略）                          │  │
│  └───────────────────────────┬───────────────────────────────────────────┘  │
│                              │                                                │
│                              ▼                                                │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Risk 子图（2 轮辩论）                                                 │  │
│  │  Aggressive (R1) → Neutral (R1) → Conservative (R1) →                │  │
│  │  Aggressive (R2) → Neutral (R2) → Conservative (R2) →               │  │
│  │  Risk Manager                                                        │  │
│  │  （基于 Research Plan + 风险辩论，生成 final_trade_decision）        │  │
│  └───────────────────────────┬───────────────────────────────────────────┘  │
│                              │                                                │
│                              ▼                                                │
│  输出状态 (AgentState):                                                      │
│  - trader_investment_plan: 交易计划（BUY/SELL/HOLD + 止盈止损）            │
│  - strategy_selection: 策略选择（market_regime + selected_strategy）        │
│  - risk_summary: 风险决策（final_trade_decision + 仓位限制）              │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  步骤 3: Market Open 节点（图外执行）                                        │
│  - 读取 Pre-Open 的决策结果（risk_summary, strategy_selection）              │
│  - 检查风险决策（如果 final_decision == "HOLD"，不执行交易）                 │
│  - 执行策略（execute_strategy），生成交易信号                                │
│  - 获取 T+1 日开盘价（实际执行价格）                                        │
│  - 调用 portfolio_manager.execute_buy/sell() 执行交易                        │
│  - 更新仓位状态（每天最多执行一次交易）                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  步骤 4: Post Close 节点（图外执行）                                         │
│  - 更新所有持仓的当前价格（使用收盘价）                                       │
│  - 计算单日收益率（相对于前一天的总资产）                                     │
│  - 计算最大回撤（基于持仓的当前价格和建仓价格）                               │
│  - 更新组合状态（PortfolioManager）                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  步骤 5: Daily Summary 保存                                                 │
│  - 从 Pre-Open 结果提取: market_regime, selected_strategy,                 │
│    expected_behavior                                                       │
│  - 从 Post Close 结果提取: actual_return, actual_max_drawdown              │
│  - 保存到 daily_trading_summaries 表（SQLite）                              │
└─────────────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  步骤 6: History Maintainer 节点（图外执行）                                │
│  - 为 4 类 Analyst 生成 7 日滚动摘要                                        │
│  - 保存到 analyst_summaries 表（SQLite）                                   │
│  - 供下一交易日使用（作为 history_report 输入到 Pre-Open 图）              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 架构设计说明

**图内 vs 图外**：
- **Pre-Open 图（LangGraph）**：纯决策逻辑，使用 LangGraph 管理复杂的状态流转
- **Market Open / Post Close（Python 函数）**：执行和结算逻辑，需要直接操作 `PortfolioManager`，不适合放在图中
- **状态传递**：Pre-Open 图通过 `AgentState` 输出决策，Market Open 通过 `state.get()` 读取决策

**数据流**：
```
Analyst 报告 → analyst_reports 表
    ↓
Pre-Open 图读取 → 生成决策（AgentState）
    ↓
Market Open 执行交易 → PortfolioManager
    ↓
Post Close 计算收益 → daily_trading_summaries 表
    ↓
History Maintainer → analyst_summaries 表（7 日滚动摘要）→ 下一交易日使用
```

### 周期级流程（周/月）

```text
周期开始
    │
    ▼
┌─────────────────┐
│  选股与再平衡    │  ← StockSelector 选股 + PortfolioManager 调仓
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  每日交易循环    │  ← 对每个选中标的执行 Pre-Open → Market Open → Post Close
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Reflector      │  ← 周期结束：总结错误模式、成功模式、策略适用条件
│  Agent          │    更新长期记忆（ChromaDB）
└─────────────────┘
```

## 核心模块

| 模块 | 说明 | 位置 |
|------|------|------|
| **Analyst** | 4 类分析师（技术/新闻/情绪/基本面），图外并行执行 | `tradingagents/agents/analysts/` |
| **Pre-Open 图** | LangGraph 决策流程（Summary → Research → Trader → Strategy Selector → Risk） | `tradingagents/graph/trading_graph.py` |
| **Market Open** | 交易执行逻辑，读取决策并执行交易 | `tradingagents/agents/market_open/node.py` |
| **Post Close** | 收益计算逻辑，更新持仓和计算收益 | `tradingagents/agents/post_close/node.py` |
| **History Maintainer** | 维护 7 日滚动摘要 | `tradingagents/agents/post_close/history_maintainer.py` |
| **Memory** | SQLite（结构化数据）+ ChromaDB（向量记忆） | `tradingagents/agents/utils/memory_db_helper.py` |
| **Portfolio** | 组合管理、交易执行、再平衡 | `tradingagents/core/portfolio/portfolio_manager.py` |
| **Stock Selector** | 多因子选股（IC 动态权重/市场状态权重） | `tradingagents/core/selection/stock_selector.py` |
| **Reflector** | 周期反思，总结交易经验 | `tradingagents/agents/post_close/reflector.py` |

## 技术栈

- **Python 3.12+**
- **LangGraph 1.2.0** + LangChain：工作流编排
- **SQLite** + **ChromaDB**：数据持久化
- **yfinance** + **Alpha Vantage**：数据源

## 运行脚本

```bash
# 单标的回测
python run_single_symbol_backtest.py --symbol NVDA --start 2025-11-06 --end 2025-11-08

# 多标的、多周期回测
python run_multi_symbol_backtest.py --start_date 2024-01-01 --end_date 2024-01-31 --cycle_type monthly

# 周期反思
python run_reflector_cycle.py --cycle_type weekly --start_date 2024-01-01 --end_date 2024-01-07
```

## 项目结构

```
TradeSwarm/
├── tradingagents/          # 核心代码
│   ├── agents/            # Agent 实现
│   ├── graph/            # LangGraph 图定义
│   └── core/             # 核心模块（portfolio, selection 等）
├── datasources/          # 数据源模块
├── config/               # 配置文件
├── docs/                 # 文档
├── run_*.py             # 运行脚本
└── requirements.txt     # 依赖
```

## 关键代码位置

| 功能 | 文件路径 | 说明 |
|------|---------|------|
| **回测驱动器** | `run_single_symbol_backtest.py` | 单标的多日回测，包含完整日级流程 |
| **Pre-Open 图定义** | `tradingagents/graph/trading_graph.py` | 主交易决策图（LangGraph） |
| **Research 子图** | `tradingagents/graph/subgraphs/research_subgraph.py` | Bull/Bear 辩论子图 |
| **Risk 子图** | `tradingagents/graph/subgraphs/risk_subgraph.py` | 风险辩论子图 |
| **Market Open 节点** | `tradingagents/agents/market_open/node.py` | 交易执行逻辑 |
| **Post Close 节点** | `tradingagents/agents/post_close/node.py` | 收益计算逻辑 |
| **组合管理** | `tradingagents/core/portfolio/portfolio_manager.py` | 持仓、现金、交易管理 |
| **数据适配器** | `tradingagents/core/data_adapter.py` | 数据源统一接口 |
| **数据库操作** | `tradingagents/agents/utils/memory_db_helper.py` | SQLite 数据库操作 |

## 注意事项

1. **API 限制**：Alpha Vantage 免费版 5 次/分钟，500 次/天，系统已实现多 Key 轮询和缓存
2. **数据库**：首次运行自动创建 SQLite 数据库（`memory.db`）
3. **配置**：需要创建 `config/config.yaml`，参考模板或 README 中的配置示例
4. **图内 vs 图外**：Pre-Open 图只包含决策逻辑，Market Open 和 Post Close 在图外执行

---

**最后更新**: 2026-02-13
