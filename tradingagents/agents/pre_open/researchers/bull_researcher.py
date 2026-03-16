from __future__ import annotations

from typing import Any, Dict, Callable
from langchain_core.messages import AIMessage
from langchain_core.language_models import BaseChatModel
import time
import json

from tradingagents.agents.utils.agentstate.agent_states import (
    AgentState,
    InvestDebateState,
    ResearchSummary,
)
from tradingagents.agents.utils.state_helpers import build_curr_situation_from_summaries
from tradingagents.agents.utils.prompt_loader import load_prompt_template


def create_bull_researcher(llm: BaseChatModel, memory: Any) -> Callable[[AgentState], Dict[str, Any]]:
    def bull_node(state: AgentState) -> dict:
        # 1. 读取辩论状态（封装在 research_summary 内）
        prev_summary: ResearchSummary | None = state.get("research_summary")  # type: ignore[assignment]
        prev_debate: InvestDebateState = (
            prev_summary.get("investment_debate_state")  # type: ignore[union-attr]
            if prev_summary is not None
            else {
                "bull_history": "",
                "bear_history": "",
                "history": "",
                "current_response": "",
                "judge_decision": "",
                "count": 0,
            }
        )
        
        history = prev_debate.get("history", "")
        bull_history = prev_debate.get("bull_history", "")
        bear_history = prev_debate.get("bear_history", "")
        current_response = prev_debate.get("current_response", "")
        count = prev_debate.get("count", 0)
        
        # 计算当前轮次（每轮包含 bull 和 bear 各一次发言）
        round_number = (count // 2) + 1
        is_first_round = count < 2

        # 2. 从四个 Analyst 的 MemorySummary 中构造当前情境（仅用于 trader 记忆，不在 research 中显式使用）
        curr_situation = build_curr_situation_from_summaries(state)
        market_summary = state["market_analyst_summary"]
        news_summary = state["news_analyst_summary"]
        sentiment_summary = state["sentiment_analyst_summary"]
        fundamentals_summary = state["fundamentals_analyst_summary"]

        # 当日报告
        market_research_report = market_summary["today_report"]
        news_report = news_summary["today_report"]
        sentiment_report = sentiment_summary["today_report"]
        fundamentals_report = fundamentals_summary["today_report"]

        # 最近 7 日 history 摘要（用于为当日观点提供脉络）
        market_history_summary = market_summary["history_report"]
        news_history_summary = news_summary["history_report"]
        sentiment_history_summary = sentiment_summary["history_report"]
        fundamentals_history_summary = fundamentals_summary["history_report"]

        # 加载并渲染 prompt 模板
        prompt = load_prompt_template(
            agent_type="researchers",
            agent_name="bull_researcher",
            context={
                # 当日四个分析师报告
                "market_research_report": market_research_report,
                "sentiment_report": sentiment_report,
                "news_report": news_report,
                "fundamentals_report": fundamentals_report,
                # 过去 7 日四个分析师的 history 摘要
                "market_history_summary": market_history_summary,
                "news_history_summary": news_history_summary,
                "sentiment_history_summary": sentiment_history_summary,
                "fundamentals_history_summary": fundamentals_history_summary,
                # 辩论状态
                "history": history,
                "current_response": current_response,
                "round_number": round_number,
                "is_first_round": is_first_round,
                "bear_history": bear_history,
            },
        )

        response = llm.invoke(prompt)
        content: str = getattr(response, "content", str(response))

        argument = f"Bull Analyst: {content}"

        # 3. 更新投资辩论状态
        new_investment_debate_state: InvestDebateState = {
            "history": history + "\n" + argument,
            "bull_history": bull_history + "\n" + argument,
            "bear_history": prev_debate.get("bear_history", ""),
            "current_response": argument,
            "judge_decision": prev_debate.get("judge_decision", ""),
            "count": prev_debate.get("count", 0) + 1,
        }

        # 4. 更新 research_summary
        new_summary: ResearchSummary = {
            "investment_debate_state": new_investment_debate_state,
        }
        if prev_summary is not None:
            new_summary["investment_plan"] = prev_summary.get("investment_plan", "")
            new_summary["raw_response"] = prev_summary.get("raw_response", "")

        return {"research_summary": new_summary}

    return bull_node
