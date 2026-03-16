#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
History Maintainer 节点

职责：
    - 在 post_close 阶段之后执行
    - 针对当前 symbol / trade_date，为每个 Analyst 生成 7 个交易日窗口的结构化 history_summary
    - 将 summary 写入 `analyst_summaries` 表（由 MemoryDBHelper 负责）

当前状态：
    - 只实现结构和调用流程，占位 LLM 调用逻辑
    - 具体 Prompt / JSON Schema 后续与用户一起设计后再补充
"""

from __future__ import annotations

from typing import Any, Callable, Dict

from langchain_core.language_models import BaseChatModel

from tradingagents.agents.utils.agentstate.agent_states import AgentState
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper
from tradingagents.agents.utils.memory import (
    MarketMemoryManager,
    NewsMemoryManager,
    SentimentMemoryManager,
    FundamentalsMemoryManager,
)


def create_history_maintainer_node(
    llm: BaseChatModel,
    db_helper: MemoryDBHelper,
) -> Callable[[AgentState], Dict[str, Any]]:
    """创建 History Maintainer 节点。

    Args:
        llm: 用于生成 summary 的 LLM 实例（后续会结合具体 Prompt 使用）
        db_helper: MemoryDBHelper 实例

    Returns:
        一个符合 LangGraph 节点签名的函数：fn(state) -> updates
    """

    # 为 4 类 Analyst 各自构建 MemoryManager
    market_manager = MarketMemoryManager(db_helper=db_helper)
    news_manager = NewsMemoryManager(db_helper=db_helper)
    sentiment_manager = SentimentMemoryManager(db_helper=db_helper)
    fundamentals_manager = FundamentalsMemoryManager(db_helper=db_helper)

    def history_maintainer_node(state: AgentState) -> Dict[str, Any]:
        """History Maintainer 节点执行函数。

        高层流程：
        1. 从 AgentState 中读取 symbol / trade_date
        2. 依次调用 4 个 MemoryManager 的 run_daily_update
        3. 将执行结果记录到 state（例如写入一个简单的 log 字段）

        注意：
            - 当前仅执行占位 summary 生成（不影响主交易逻辑）
            - 真实的 Prompt / JSON 结构后续补充
        """
        symbol = state.get("company_of_interest")
        trade_date = state.get("trade_date")

        if not symbol or not trade_date:
            print(
                f"[HistoryMaintainer] 缺少必要参数: symbol={symbol}, trade_date={trade_date}"
            )
            return {}

        results: Dict[str, Any] = {
            "history_maintainer_log": [],
        }

        # 依次为 4 类 Analyst 生成 / 更新 summary
        for name, manager in [
            ("market", market_manager),
            ("news", news_manager),
            ("sentiment", sentiment_manager),
            ("fundamentals", fundamentals_manager),
        ]:
            try:
                ok = manager.run_daily_update(
                    llm=llm,
                    symbol=symbol,
                    trade_date=trade_date,
                    window_size=7,
                )
                results["history_maintainer_log"].append(
                    {
                        "analyst_type": name,
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "status": "ok" if ok else "skipped",
                    }
                )
            except Exception as exc:  # noqa: BLE001
                print(f"[HistoryMaintainer] {name} summary 生成失败: {exc}")
                results["history_maintainer_log"].append(
                    {
                        "analyst_type": name,
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "status": "error",
                        "error": str(exc),
                    }
                )

        return results

    return history_maintainer_node


