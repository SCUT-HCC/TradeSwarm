from __future__ import annotations

from typing import Any, Dict, Callable
from langchain_core.language_models import BaseChatModel
import time
import json

from tradingagents.agents.utils.agentstate.agent_states import (
    AgentState,
    AnalystMemorySummary,
    RiskDebateState,
    RiskSummary,
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
        current_safe_response = prev_debate.get("current_safe_response", "")
        current_neutral_response = prev_debate.get("current_neutral_response", "")

        # 2. 从四个 Analyst 的 MemorySummary 中构造当前情境
        market_research_report = _build_curr_situation_from_summaries(state)
        sentiment_report = state["sentiment_analyst_summary"]["today_report"]
        news_report = state["news_analyst_summary"]["today_report"]
        fundamentals_report = state["fundamentals_analyst_summary"]["today_report"]

        # 3. 读取 research_summary 中的 investment_plan 作为 trader_decision
        research_summary: Dict[str, Any] | None = state.get("research_summary")  # type: ignore[assignment]
        trader_decision = (
            research_summary.get("investment_plan", "")  # type: ignore[union-attr]
            if research_summary is not None
            else state.get("investment_plan", "") or state.get("trader_investment_plan", "")
        )

        prompt = f"""作为激进风险分析师，你的角色是积极倡导高风险高回报的机会，强调大胆策略和竞争优势。在评估交易员的决策或计划时，专注于潜在的上行空间、增长潜力和创新收益——即使这些伴随着较高的风险。使用提供的市场数据和情绪分析来强化你的论点并挑战反对观点。具体而言，直接回应保守和中性分析师提出的每个观点，用数据驱动的反驳和具有说服力的推理来反击。强调他们的谨慎可能错失关键机会，或他们的假设可能过于保守的地方。以下是交易员的决策：

{trader_decision}

你的任务是通过质疑和批判保守和中性立场，为交易员的决策创建一个令人信服的案例，展示为什么你的高回报视角提供了最佳的前进道路。将以下来源的洞察融入你的论证中：

市场研究报告：{market_research_report}
社交媒体情绪报告：{sentiment_report}
最新时事报告：{news_report}
公司基本面报告：{fundamentals_report}
以下是当前对话历史：{history} 以下是保守分析师的最新论点：{current_safe_response} 以下是中性分析师的最新论点：{current_neutral_response}。如果没有其他观点的回应，不要编造，只需陈述你的观点。

通过解决提出的具体担忧、反驳他们逻辑中的弱点，并断言冒险带来的好处以超越市场常规，来积极参与辩论。保持专注于辩论和说服，而不仅仅是呈现数据。挑战每个反驳点，以强调为什么高风险方法是最优的。以对话的方式输出，就像在说话一样，不要使用任何特殊格式。"""

        response = llm.invoke(prompt)
        content: str = getattr(response, "content", str(response))

        argument = f"Risky Analyst: {content}"

        # 4. 更新风险辩论状态
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

        # 5. 更新或创建 risk_summary
        new_summary: RiskSummary = {
            "risk_debate_state": new_risk_debate_state,
        }
        if prev_summary is not None:
            new_summary["final_trade_decision"] = prev_summary.get("final_trade_decision", "")
            new_summary["raw_response"] = prev_summary.get("raw_response", "")

        return {"risk_summary": new_summary}

    return risky_node
