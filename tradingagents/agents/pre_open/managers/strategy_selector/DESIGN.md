# 策略选择 Agent 设计文档

## 概述

策略选择 Agent（Strategy Selector）位于 Risk Manager 之后，负责根据风险调整后的决策和股票特征，为每只股票选择最适合的交易策略。

## 位置

```
Research Subgraph → Risk Subgraph → **Strategy Selector** → Trader Node
```

## 输入

策略选择 Agent 接收以下输入：

1. **Risk Manager 输出**：
   - `risk_summary.final_trade_decision`: 最终交易决策（买入/卖出/持有）
   - `risk_summary.risk_debate_state`: 风险辩论历史

2. **分析师报告摘要**：
   - `market_analyst_summary.today_report`: 市场技术分析报告
   - `news_analyst_summary.today_report`: 新闻分析报告
   - `sentiment_analyst_summary.today_report`: 情绪分析报告
   - `fundamentals_analyst_summary.today_report`: 基本面分析报告

3. **Research Manager 输出**：
   - `research_summary.investment_plan`: 投资计划

4. **基础信息**：
   - `company_of_interest`: 股票代码
   - `trade_date`: 交易日期

## 输出结构

策略选择 Agent 输出一个 `StrategySelection` 对象，包含以下字段：

```python
class StrategySelection(TypedDict):
    """
    策略选择结果
    
    包含：
    - 选中的策略类型（strategy_type）
    - 策略选择理由（reasoning）
    - 策略适用性分析（strategy_analysis）
    - 风险调整建议（risk_adjustment）
    """
    strategy_type: Annotated[
        Literal[
            "trend_following",
            "mean_reversion", 
            "momentum_breakout",
            "reversal",
            "range_trading",
            "default_timing"
        ],
        "选中的策略类型（必须与 trading_sys/strategies/strategy_lib.py 中的 STRATEGY_MAPPING 匹配）"
    ]
    
    reasoning: Annotated[
        str,
        "策略选择理由（Markdown 格式，说明为什么选择该策略，基于哪些分析师的报告和风险决策）"
    ]
    
    strategy_analysis: Annotated[
        str,
        "策略适用性分析（Markdown 格式，分析该策略在当前市场环境下的适用性，包括优势、风险和注意事项）"
    ]
    
    risk_adjustment: Annotated[
        Optional[str],
        "风险调整建议（可选，如果 Risk Manager 建议降低风险，可以建议使用更保守的策略变体或调整参数）"
    ]
    
    confidence: Annotated[
        float,
        "策略选择置信度（0-1，表示对该策略选择的信心程度）"
    ]
    
    alternative_strategies: Annotated[
        Optional[List[str]],
        "备选策略列表（可选，如果首选策略不适用，可以列出备选策略）"
    ]
```

## 输出示例

### 示例 1：趋势跟踪策略

```json
{
    "strategy_type": "trend_following",
    "reasoning": "## 策略选择理由\n\n基于以下分析，选择趋势跟踪策略：\n\n1. **市场分析**：Market Analyst 报告显示股票处于明确的上升趋势，MA20 > MA50，且成交量放大。\n2. **基本面**：Fundamentals Analyst 报告显示公司财务状况良好，现金流强劲。\n3. **风险决策**：Risk Manager 建议买入，但需要设置止损。\n4. **市场环境**：当前市场处于牛市状态，趋势延续概率较高。\n\n**结论**：趋势跟踪策略最适合当前市场环境，可以捕捉上升趋势并设置ATR追踪止损。",
    
    "strategy_analysis": "## 策略适用性分析\n\n### 优势\n- 当前股票处于明确上升趋势，符合策略假设\n- MA20/MA50 均线系统显示趋势健康\n- ATR追踪止损可以有效控制风险\n\n### 风险\n- 如果趋势突然反转，可能触发止损\n- 需要持续监控趋势变化\n\n### 注意事项\n- 建议在价格回调至MA20附近时买入\n- 止损设置为2倍ATR，止盈设置为3倍ATR",
    
    "risk_adjustment": "Risk Manager 建议设置严格止损，策略已内置ATR追踪止损机制，符合风险要求。",
    
    "confidence": 0.85,
    
    "alternative_strategies": ["momentum_breakout", "default_timing"]
}
```

### 示例 2：均值回归策略

```json
{
    "strategy_type": "mean_reversion",
    "reasoning": "## 策略选择理由\n\n基于以下分析，选择均值回归策略：\n\n1. **市场分析**：Market Analyst 报告显示股票RSI < 35，处于超卖状态。\n2. **新闻分析**：News Analyst 报告显示近期负面新闻导致股价下跌，但基本面未改变。\n3. **风险决策**：Risk Manager 建议谨慎买入，等待反弹机会。\n4. **市场环境**：当前市场处于震荡状态，价格偏离均值后倾向于回归。\n\n**结论**：均值回归策略适合捕捉超卖反弹机会，RSI和布林带指标支持此策略。",
    
    "strategy_analysis": "## 策略适用性分析\n\n### 优势\n- RSI < 35 显示超卖，反弹概率较高\n- 布林带下轨提供支撑位\n- 6%止损、10%止盈的风险收益比合理\n\n### 风险\n- 如果基本面恶化，价格可能继续下跌\n- 需要确认负面新闻的影响是否已充分反映\n\n### 注意事项\n- 建议在RSI < 35 且价格触及布林带下轨时买入\n- 如果价格跌破布林带下轨，需要重新评估",
    
    "risk_adjustment": "Risk Manager 建议谨慎买入，策略已设置6%止损，符合风险控制要求。",
    
    "confidence": 0.75,
    
    "alternative_strategies": ["reversal", "default_timing"]
}
```

### 示例 3：持有决策

```json
{
    "strategy_type": "default_timing",
    "reasoning": "## 策略选择理由\n\n基于以下分析，建议持有：\n\n1. **风险决策**：Risk Manager 建议持有，等待更明确的信号。\n2. **市场分析**：Market Analyst 报告显示市场处于震荡状态，无明显趋势。\n3. **基本面**：Fundamentals Analyst 报告显示公司基本面稳定，但缺乏催化剂。\n\n**结论**：当前市场环境不适合主动交易，建议使用默认择时策略，等待更明确的信号。",
    
    "strategy_analysis": "## 策略适用性分析\n\n### 当前状态\n- 市场处于震荡状态，无明显趋势\n- 缺乏明确的买入或卖出信号\n- 基本面稳定但缺乏催化剂\n\n### 建议\n- 使用默认择时策略，持续监控市场变化\n- 等待明确的趋势信号或催化剂出现\n- 如果出现突破或反转信号，可以切换到相应策略",
    
    "risk_adjustment": null,
    
    "confidence": 0.60,
    
    "alternative_strategies": ["range_trading", "mean_reversion"]
}
```

## 策略选择逻辑

### 决策流程

1. **解析 Risk Manager 决策**：
   - 如果决策是"买入"，选择适合买入的策略
   - 如果决策是"卖出"，选择适合卖出的策略（或持有）
   - 如果决策是"持有"，选择保守策略或默认策略

2. **分析市场特征**：
   - 从 Market Analyst 报告中提取技术指标（趋势、RSI、成交量等）
   - 从 News Analyst 报告中提取市场情绪和催化剂
   - 从 Fundamentals Analyst 报告中提取基本面信息

3. **匹配策略**：
   - **趋势市场** → `trend_following` 或 `momentum_breakout`
   - **震荡市场** → `range_trading` 或 `mean_reversion`
   - **超卖/超买** → `mean_reversion` 或 `reversal`
   - **突破信号** → `momentum_breakout`
   - **不确定** → `default_timing`

4. **风险调整**：
   - 如果 Risk Manager 建议降低风险，优先选择更保守的策略
   - 如果 Risk Manager 建议激进，可以选择更高风险的策略

## 与 trading_sys 对接

### 输出转换

策略选择 Agent 的输出需要转换为 `trading_sys` 可以使用的格式：

```python
from tradingagents.core.strategies import execute_strategy, STRATEGY_MAPPING

# 从 AgentState 获取策略选择结果
strategy_selection = state.get("strategy_selection")

# 验证策略类型
strategy_type = strategy_selection["strategy_type"]
if strategy_type not in STRATEGY_MAPPING:
    strategy_type = "default_timing"  # 降级到默认策略

# 执行策略
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

### 数据流

```
Strategy Selector Agent
    ↓
输出: StrategySelection
    ↓
转换为: strategy_type (str)
    ↓
execute_strategy(strategy_type, df, is_holding)
    ↓
输出: StrategyResult (signal, confidence, stop_loss, take_profit, reason)
    ↓
Trader Node 执行交易
```

## 实现要点

1. **策略信息获取**：
   - 从 `tradingagents/core/strategies` 导入 `STRATEGY_INFO`
   - 在 prompt 中提供所有可用策略的信息，帮助 LLM 做出选择

2. **输出验证**：
   - 验证 `strategy_type` 是否在 `STRATEGY_MAPPING` 中
   - 如果无效，降级到 `default_timing`

3. **错误处理**：
   - 如果策略执行失败，回退到 `default_timing`
   - 记录策略选择日志，便于后续分析

4. **状态更新**：
   - 将 `StrategySelection` 写入 `AgentState.strategy_selection`
   - 供后续 Trader Node 使用

## 后续扩展

1. **多策略组合**：未来可以支持为同一股票选择多个策略，按权重组合
2. **动态参数调整**：虽然当前策略参数固定，但可以记录 LLM 的参数调整建议
3. **策略回测集成**：可以将策略选择结果与历史回测结果对比，优化选择逻辑

