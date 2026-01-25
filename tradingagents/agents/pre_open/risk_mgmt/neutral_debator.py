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


def create_neutral_debator(llm: BaseChatModel) -> Callable[[AgentState], Dict[str, Any]]:
    def neutral_node(state: AgentState) -> dict:
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
        neutral_history = prev_debate.get("neutral_history", "")
        current_risky_response = prev_debate.get("current_risky_response", "")
        current_safe_response = prev_debate.get("current_safe_response", "")

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

        prompt = f"""作为中性风险分析师，你的角色是提供平衡的视角，权衡交易员决策或计划的潜在收益和风险。你优先考虑全面方法，评估优缺点，同时考虑更广泛的市场趋势、潜在的经济变化和多元化策略。以下是交易员的决策：

{trader_decision}

你的任务是挑战激进和保守分析师，指出每个视角可能过于乐观或过于谨慎的地方。使用以下数据源的洞察来支持调整交易员决策的中等、可持续策略：

市场研究报告：{market_research_report}
社交媒体情绪报告：{sentiment_report}
最新时事报告：{news_report}
公司基本面报告：{fundamentals_report}
以下是当前对话历史：{history} 以下是激进分析师的最新回应：{current_risky_response} 以下是保守分析师的最新回应：{current_safe_response}。如果没有其他观点的回应，不要编造，只需陈述你的观点。

通过批判性地分析双方，解决激进和保守论证中的弱点，倡导更平衡的方法，来积极参与辩论。挑战他们的每个观点，以说明为什么中等风险策略可能提供两全其美，既提供增长潜力，又防范极端波动。专注于辩论，而不是简单地呈现数据，旨在展示平衡的视角可以带来最可靠的结果。以对话的方式输出，就像在说话一样，不要使用任何特殊格式。"""

        response = llm.invoke(prompt)
        content: str = getattr(response, "content", str(response))

        argument = f"Neutral Analyst: {content}"

        # 4. 更新风险辩论状态
        new_risk_debate_state: RiskDebateState = {
            "history": history + "\n" + argument,
            "risky_history": prev_debate.get("risky_history", ""),
            "safe_history": prev_debate.get("safe_history", ""),
            "neutral_history": neutral_history + "\n" + argument,
            "latest_speaker": "Neutral",
            "current_risky_response": prev_debate.get("current_risky_response", ""),
            "current_safe_response": prev_debate.get("current_safe_response", ""),
            "current_neutral_response": argument,
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

    return neutral_node
