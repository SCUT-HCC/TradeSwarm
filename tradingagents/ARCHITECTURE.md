# 交易系统架构说明

## 目录结构

```
tradingagents/
├── agents/
│   ├── pre_open/          # 开盘前分析阶段
│   │   ├── trader/        # 交易员节点（可访问仓位信息）
│   │   ├── managers/
│   │   │   ├── risk_manager/  # 风险经理（可访问仓位信息）
│   │   │   └── strategy_selector/  # 策略选择器
│   │   └── ...
│   ├── market_open/       # 市场开盘交易执行阶段
│   │   └── node.py        # 交易执行器
│   └── post_close/        # 收盘后收益整理阶段
│       └── node.py         # 收益整理节点
│
├── core/                   # 核心模块
│   ├── portfolio/          # 组合管理
│   │   └── portfolio_manager.py
│   ├── selection/          # 选股服务
│   │   └── stock_selector.py
│   └── data_adapter.py     # 数据适配器
│
└── graph/
    └── trading_graph.py    # 主交易图
```

## 流程说明

### 1. Pre-Open（开盘前分析）

**节点顺序**：
```
Summary Nodes → Research Subgraph → Trader → Strategy Selector → Risk Subgraph
```

**关键特性**：
- `Trader` 节点可以访问 `current_position` 和 `portfolio_state`
- `Risk Manager` 节点可以访问 `current_position` 和 `portfolio_state`
- 生成交易决策和策略选择，但不执行交易

### 2. Market Open（市场开盘交易执行）

**节点**：`market_open/node.py`

**职责**：
- 读取 `pre_open` 阶段的决策（`strategy_selection`, `trader_investment_plan`, `risk_summary`）
- 检查风险决策（如果被否定，不执行）
- 执行策略（调用 `trading_sys` 的策略库）
- 根据信号和 Trader 建议决定是否下单
- 获取 T+1 日开盘价（实际执行价格）
- 执行交易（考虑目标仓位分配）
- 更新仓位状态

### 3. Post-Close（收盘后收益整理）

**节点**：`post_close/node.py`

**职责**：
- 更新所有持仓的当前价格（使用收盘价）
- 计算每日收益
- 更新组合状态
- 记录交易日志

## AgentState 扩展

在 `AgentState` 中添加了以下字段：

```python
current_position: Optional[Dict[str, Any]]  # 当前持仓信息
portfolio_state: Optional[Dict[str, Any]]   # 组合状态
```

## 组合管理

### PortfolioManager

位置：`tradingagents/core/portfolio/portfolio_manager.py`

功能：
- 管理多股票投资组合
- 执行买入/卖出
- 再平衡逻辑
- 跟踪仓位、资金、交易记录

### StockSelectorService

位置：`tradingagents/core/selection/stock_selector.py`

功能：
- 封装 `trading_sys` 的选股功能
- 每月第一个交易日选股
- 判断再平衡日

## 使用方式

### 1. 初始化组合管理器

```python
from tradingagents.core.portfolio import PortfolioManager
from tradingagents.core.data_adapter import DataAdapter

portfolio_manager = PortfolioManager(initial_cash=100000.0, max_positions=5)
data_adapter = DataAdapter(use_cache=True)
```

### 2. 创建 market_open 节点

```python
from tradingagents.agents.market_open.node import create_market_open_executor

market_open_node = create_market_open_executor(portfolio_manager, data_adapter)
```

### 3. 创建 post_close 节点

```python
from tradingagents.agents.post_close.node import create_post_close_node

post_close_node = create_post_close_node(portfolio_manager, data_adapter)
```

### 4. 在 Graph 中集成

需要在 Graph 中添加 `market_open` 和 `post_close` 节点，根据 `trading_session` 字段路由到相应的节点。

## 关键设计

1. **时间处理**：T 日收盘后计算信号，T+1 日开盘价执行交易
2. **风险控制**：如果 Risk Manager 决策为 HOLD，不执行交易
3. **仓位管理**：均分仓位，每个股票分配 `1 / len(target_symbols)` 的仓位
4. **仓位信息共享**：`trader` 和 `risk_manager` 节点都可以访问仓位信息，做出更准确的决策

