from __future__ import annotations

from typing import Any, Dict, Callable
from langchain_core.language_models import BaseChatModel
import functools
import time
import json

from tradingagents.agents.utils.agentstate.agent_states import (
    AgentState,
    AnalystMemorySummary,
)


def _build_curr_situation_from_summaries(state: AgentState) -> str:
    """从四个 Analyst 的 MemorySummary 中构造当前情境描述。"""
    market_summary: AnalystMemorySummary = state["market_analyst_summary"]
    news_summary: AnalystMemorySummary = state["news_analyst_summary"]
    sentiment_summary: AnalystMemorySummary = state["sentiment_analyst_summary"]
    fundamentals_summary: AnalystMemorySummary = state["fundamentals_analyst_summary"]

    market_report = market_summary["today_report"]
    news_report = news_summary["today_report"]
    sentiment_report = sentiment_summary["today_report"]
    fundamentals_report = fundamentals_summary["today_report"]

    return (
        f"{market_report}\n\n"
        f"{sentiment_report}\n\n"
        f"{news_report}\n\n"
        f"{fundamentals_report}"
    )


def create_trader(llm: BaseChatModel, memory: Any) -> Callable[[AgentState], Dict[str, Any]]:
    def trader_node(state: AgentState) -> Dict[str, Any]:
        company_name = state["company_of_interest"]
        
        # 读取 research_summary 中的 investment_plan
        research_summary: Dict[str, Any] | None = state.get("research_summary")  # type: ignore[assignment]
        investment_plan = (
            research_summary.get("investment_plan", "")  # type: ignore[union-attr]
            if research_summary is not None
            else state.get("investment_plan", "")
        )
        
        # 从四个 Analyst 的 MemorySummary 中构造当前情境
        curr_situation = _build_curr_situation_from_summaries(state)
        past_memories = memory.get_memories(curr_situation, n_matches=2)

        past_memory_str = ""
        if past_memories:
            for i, rec in enumerate(past_memories, 1):
                past_memory_str += rec["recommendation"] + "\n\n"
        else:
            past_memory_str = "No past memories found."

        context = {
            "role": "user",
            "content": f"基于分析师团队的全面分析，以下是针对 {company_name} 量身定制的投资计划。该计划融合了当前技术市场趋势、宏观经济指标和社交媒体情绪的洞察。使用此计划作为评估你下一个交易决策的基础。\n\n拟议投资计划：{investment_plan}\n\n利用这些洞察做出明智且具有战略性的决策。",
        }

        messages = [
            {
                "role": "system",
                "content": f"""你是一个交易代理，分析市场数据以做出投资决策。基于你的分析，提供具体的买入、卖出或持有建议。以明确的决策结尾，并始终以 '最终交易提案：**买入/持有/卖出**' 结束你的回应以确认你的建议。不要忘记利用过往决策的经验教训，从错误中学习。以下是你过去在类似情况下交易的一些反思和经验教训：{past_memory_str}""",
            },
            context,
        ]

        result = llm.invoke(messages)
        content: str = getattr(result, "content", str(result))

        return {
            "messages": [result],
            "trader_investment_plan": content,
        }

    return trader_node
