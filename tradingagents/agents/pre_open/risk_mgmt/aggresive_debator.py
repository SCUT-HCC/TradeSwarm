from __future__ import annotations

from typing import Any, Dict, Callable
from langchain_core.language_models import BaseChatModel
import time
import json

from tradingagents.agents.utils.agentstate.agent_states import (
    AgentState,
    RiskDebateState,
    RiskSummary,
)
from tradingagents.agents.utils.state_helpers import build_curr_situation_from_summaries
from tradingagents.agents.utils.prompt_loader import load_prompt_template


def create_risky_debator(llm: BaseChatModel) -> Callable[[AgentState], Dict[str, Any]]:
    def risky_node(state: AgentState) -> dict:
        # 1. 读取辩论状态（封装在 risk_summary 内）
        prev_summary: RiskSummary | None = state.get("risk_summary")  # type: ignore[assignment]
        prev_debate: RiskDebateState = (
            prev_summary.get("risk_debate_state")  # type: ignore[union-attr]
            if prev_summary is not None
            else {
                "risky_history": "",
                "safe_history": "",
                "neutral_history": "",
                "history": "",
                "latest_speaker": "",
                "current_risky_response": "",
                "current_safe_response": "",
                "current_neutral_response": "",
                "judge_decision": "",
                "count": 0,
            }
        )
        
        history = prev_debate.get("history", "")
        risky_history = prev_debate.get("risky_history", "")
        safe_history = prev_debate.get("safe_history", "")
        neutral_history = prev_debate.get("neutral_history", "")
        current_safe_response = prev_debate.get("current_safe_response", "")
        current_neutral_response = prev_debate.get("current_neutral_response", "")
        count = prev_debate.get("count", 0)
        
        # 计算当前轮次（每轮包含 risky、neutral、safe 各一次发言）
        round_number = (count // 3) + 1
        is_first_round = count < 3

        # 2. 从四个 Analyst 的 MemorySummary 中读取当日与 7 日脉络（无 memory，仅用已有 summary）
        market_summary = state["market_analyst_summary"]
        news_summary = state["news_analyst_summary"]
        sentiment_summary = state["sentiment_analyst_summary"]
        fundamentals_summary = state["fundamentals_analyst_summary"]

        # 当日报告
        market_research_report = market_summary["today_report"]
        news_report = news_summary["today_report"]
        sentiment_report = sentiment_summary["today_report"]
        fundamentals_report = fundamentals_summary["today_report"]

        # 最近 7 日 history 摘要（用于风险脉络）
        market_history_summary = market_summary["history_report"]
        news_history_summary = news_summary["history_report"]
        sentiment_history_summary = sentiment_summary["history_report"]
        fundamentals_history_summary = fundamentals_summary["history_report"]

        # 3. 读取 research_summary 中的 investment_plan 作为 trader_decision
        research_summary: Dict[str, Any] | None = state.get("research_summary")  # type: ignore[assignment]
        trader_decision = (
            research_summary.get("investment_plan", "")  # type: ignore[union-attr]
            if research_summary is not None
            else state.get("investment_plan", "") or state.get("trader_investment_plan", "")
        )

        # 4. 加载并渲染 prompt 模板
        prompt = load_prompt_template(
            agent_type="risk_mgmt",
            agent_name="aggresive_debator",
            context={
                "trader_decision": trader_decision,
                # 当日本标的四个分析师报告
                "market_research_report": market_research_report,
                "sentiment_report": sentiment_report,
                "news_report": news_report,
                "fundamentals_report": fundamentals_report,
                # 最近 7 日四个分析师的 history 摘要
                "market_history_summary": market_history_summary,
                "news_history_summary": news_history_summary,
                "sentiment_history_summary": sentiment_history_summary,
                "fundamentals_history_summary": fundamentals_history_summary,
                # 风险辩论状态
                "history": history,
                "current_safe_response": current_safe_response,
                "current_neutral_response": current_neutral_response,
                "round_number": round_number,
                "is_first_round": is_first_round,
                "safe_history": safe_history,
                "neutral_history": neutral_history,
            },
        )

        # 5. 调用 LLM 生成论证
        response = llm.invoke(prompt)
        content: str = getattr(response, "content", str(response))

        argument = f"Risky Analyst: {content}"

        # 6. 更新风险辩论状态
        new_risk_debate_state: RiskDebateState = {
            "history": history + "\n" + argument,
            "risky_history": risky_history + "\n" + argument,
            "safe_history": prev_debate.get("safe_history", ""),
            "neutral_history": prev_debate.get("neutral_history", ""),
            "latest_speaker": "Risky",
            "current_risky_response": argument,
            "current_safe_response": prev_debate.get("current_safe_response", ""),
            "current_neutral_response": prev_debate.get("current_neutral_response", ""),
            "judge_decision": prev_debate.get("judge_decision", ""),
            "count": prev_debate.get("count", 0) + 1,
        }

        # 7. 更新或创建 risk_summary
        new_summary: RiskSummary = {
            "risk_debate_state": new_risk_debate_state,
        }
        if prev_summary is not None:
            new_summary["final_trade_decision"] = prev_summary.get("final_trade_decision", "")
            new_summary["raw_response"] = prev_summary.get("raw_response", "")

        return {"risk_summary": new_summary}

    return risky_node
