# 策略选择 Agent (Strategy Selector)

## 概述

策略选择 Agent 位于 Risk Manager 之后，负责根据风险调整后的决策和股票特征，为每只股票选择最适合的交易策略。

## 位置

```
Research Subgraph → Risk Subgraph → **Strategy Selector** → Trader Node
```

## 输入

- **Risk Manager 输出**：`risk_summary.final_trade_decision`（买入/卖出/持有）
- **分析师报告**：四个 Analyst 的 `today_report`
- **Research Manager 输出**：`research_summary.investment_plan`
- **基础信息**：`company_of_interest`, `trade_date`

## 输出格式

策略选择 Agent 输出 `StrategySelection` 对象：

```python
{
    "strategy_type": "trend_following",  # 策略ID（必须与 trading_sys 匹配）
    "reasoning": "## 策略选择理由\n\n...",  # Markdown 格式的选择理由
    "strategy_analysis": "## 策略适用性分析\n\n...",  # 策略适用性分析
    "risk_adjustment": "风险调整建议（可选）",  # 风险调整建议
    "confidence": 0.85,  # 置信度 0-1
    "alternative_strategies": ["momentum_breakout", "default_timing"]  # 备选策略
}
```

## 与 trading_sys 对接

### 1. 策略类型映射

策略选择 Agent 输出的 `strategy_type` 必须与 `trading_sys/strategies/strategy_lib.py` 中的 `STRATEGY_MAPPING` 匹配：

```python
STRATEGY_MAPPING = {
    'trend_following': trend_following_strategy,
    'mean_reversion': mean_reversion_strategy,
    'momentum_breakout': momentum_breakout_strategy,
    'reversal': reversal_strategy,
    'range_trading': range_trading_strategy,
    'default_timing': default_timing_strategy,
}
```

### 2. 执行策略

```python
from tradingagents.core.strategies import execute_strategy, STRATEGY_MAPPING

# 从 AgentState 获取策略选择结果
strategy_selection = state.get("strategy_selection")
strategy_type = strategy_selection["strategy_type"]

# 验证策略类型
if strategy_type not in STRATEGY_MAPPING:
    strategy_type = "default_timing"  # 降级到默认策略

# 执行策略（需要股票数据 DataFrame）
result = execute_strategy(
    strategy_type=strategy_type,
    df=stock_data,  # DataFrame with Open, High, Low, Close, Volume
    is_holding=False  # 是否已持仓
)

# 获取策略结果
# result.signal: Signal.BUY / SELL / HOLD
# result.confidence: float (0-1)
# result.stop_loss_price: float
# result.take_profit_price: float
# result.reason: str
```

### 3. 数据流

```
Strategy Selector Agent
    ↓
输出: StrategySelection
    ↓
提取: strategy_type (str)
    ↓
验证: 检查是否在 STRATEGY_MAPPING 中
    ↓
execute_strategy(strategy_type, df, is_holding)
    ↓
输出: StrategyResult (signal, confidence, stop_loss, take_profit, reason)
    ↓
Trader Node 执行交易
```

## 策略选择逻辑

### 决策流程

1. **解析 Risk Manager 决策**：
   - "买入" → 选择适合买入的策略
   - "卖出" → 选择适合卖出的策略（或持有）
   - "持有" → 选择保守策略或默认策略

2. **分析市场特征**：
   - 从 Market Analyst 报告提取技术指标（趋势、RSI、成交量等）
   - 从 News Analyst 报告提取市场情绪和催化剂
   - 从 Fundamentals Analyst 报告提取基本面信息

3. **匹配策略**：
   - **趋势市场** → `trend_following` 或 `momentum_breakout`
   - **震荡市场** → `range_trading` 或 `mean_reversion`
   - **超卖/超买** → `mean_reversion` 或 `reversal`
   - **突破信号** → `momentum_breakout`
   - **不确定** → `default_timing`

4. **风险调整**：
   - Risk Manager 建议降低风险 → 选择更保守的策略
   - Risk Manager 建议激进 → 选择更高风险的策略

## 实现要点

1. **策略信息获取**：在 prompt 中提供所有可用策略的信息（`STRATEGY_INFO`）
2. **输出验证**：验证 `strategy_type` 是否有效，无效时降级到 `default_timing`
3. **错误处理**：策略执行失败时回退到 `default_timing`
4. **状态更新**：将 `StrategySelection` 写入 `AgentState.strategy_selection`

## 输出示例

详见 `DESIGN.md` 文件中的完整示例。

