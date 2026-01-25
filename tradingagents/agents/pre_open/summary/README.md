# Summary 节点模块

从数据库读取 Analyst 的报告摘要，填充到 `AgentState` 中对应的 `*_analyst_summary` 字段。

## 目录结构

```
summary/
├── __init__.py                    # 导出所有 Summary Node 工厂函数
├── market_summary/                # Market Summary Node
│   ├── __init__.py
│   └── node.py
├── news_summary/                  # News Summary Node
│   ├── __init__.py
│   └── node.py
├── sentiment_summary/             # Sentiment Summary Node
│   ├── __init__.py
│   └── node.py
└── fundamentals_summary/           # Fundamentals Summary Node
    ├── __init__.py
    └── node.py
```

## 核心设计

### 工厂函数模式

每个 Summary Node 都使用工厂函数模式，参考 `fundamentals_analyst` 的实现方式：

- `create_*_summary_node(data_manager)` 返回节点函数
- 节点函数接受 `AgentState`，返回更新后的字典
- 内部使用私有函数 `_query_today_report()` 和 `_query_history_report()` 查询数据库

### 各 Summary Node 特点

| 工厂函数 | Analyst 类型 | 更新频率 | 特殊处理 |
|---------|------------|---------|---------|
| `create_market_summary_node` | market | 分钟级 (intraday) | today_report 可能是当日多个快照中的最新一个 |
| `create_news_summary_node` | news | 天级 (daily) | 每日一份报告 |
| `create_sentiment_summary_node` | sentiment | 天级 (daily) | 每日一份报告 |
| `create_fundamentals_summary_node` | fundamentals | 周级 (slow) | today_report 实际是本周报告 |

## 使用方式

### 1. 在 LangGraph 中使用

```python
from langgraph.graph import StateGraph
from tradingagents.agents.utils.agentstate.agent_states import AgentState
from tradingagents.agents.pre_open.summary import (
    create_market_summary_node,
    create_news_summary_node,
    create_sentiment_summary_node,
    create_fundamentals_summary_node
)
from data_manager.data_manager import DataManager

# 初始化数据管理器
data_manager = DataManager(config)

# 创建 Summary Nodes（使用工厂函数）
market_summary = create_market_summary_node(data_manager)
news_summary = create_news_summary_node(data_manager)
sentiment_summary = create_sentiment_summary_node(data_manager)
fundamentals_summary = create_fundamentals_summary_node(data_manager)

# 创建 Graph
graph = StateGraph(AgentState)

# 添加节点
graph.add_node("market_summary", market_summary)
graph.add_node("news_summary", news_summary)
graph.add_node("sentiment_summary", sentiment_summary)
graph.add_node("fundamentals_summary", fundamentals_summary)

# 设置边（可以并行执行）
graph.add_edge("start", "market_summary")
graph.add_edge("start", "news_summary")
graph.add_edge("start", "sentiment_summary")
graph.add_edge("start", "fundamentals_summary")

# 四个节点完成后，进入下一个阶段
graph.add_edge(
    ["market_summary", "news_summary", "sentiment_summary", "fundamentals_summary"],
    "research_manager"
)
```

### 2. 直接调用

```python
from tradingagents.agents.utils.agentstate.agent_states import AgentState
from tradingagents.agents.pre_open.summary import create_market_summary_node

# 创建节点（使用工厂函数）
market_summary = create_market_summary_node(data_manager)

# 准备 state
state: AgentState = {
    'company_of_interest': '000001',
    'trade_date': '2024-01-15',
    'trading_session': 'post_close',
    'messages': [],
    # ... 其他字段
}

# 调用节点
updated_state = market_summary(state)

# updated_state 包含：
# {
#     'market_analyst_summary': {
#         'today_report': '...',
#         'history_report': '...'
#     }
# }
```

## 数据库查询接口（待实现）

当前使用样例数据，实际实现时需要：

1. **查询 today_report**:
   ```sql
   SELECT report_content FROM analyst_reports
   WHERE analyst_type=? AND symbol=? AND trade_date=?
   ORDER BY trade_timestamp DESC LIMIT 1
   ```

2. **查询 history_report**:
   ```sql
   SELECT history_report FROM analyst_reports
   WHERE analyst_type=? AND symbol=? AND trade_date=?
   ORDER BY created_at DESC LIMIT 1
   ```

## 注意事项

1. **Market Analyst 特殊处理**: 同一日可能有多个快照，需取最新的（按 `trade_timestamp` 排序）
2. **Fundamentals Analyst**: `today_report` 实际是本周报告，查询时需按周范围查询
3. **trading_session**: 影响 `history_report` 的内容（pre_open 或 post_close）
4. **并行执行**: 四个 Summary Node 可以并行执行，互不依赖

## 后续工作

- [ ] 实现真实的数据库查询逻辑
- [ ] 添加错误处理和降级方案
- [ ] 添加缓存机制（如果 history_report 计算成本高）
- [ ] 添加单元测试

