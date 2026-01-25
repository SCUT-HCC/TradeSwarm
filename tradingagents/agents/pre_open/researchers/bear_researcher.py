from __future__ import annotations

from typing import Any, Dict, Callable
from langchain_core.messages import AIMessage
from langchain_core.language_models import BaseChatModel
import time
import json

from tradingagents.agents.utils.agentstate.agent_states import (
    AgentState,
    AnalystMemorySummary,
    InvestDebateState,
    ResearchSummary,
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


def create_bear_researcher(llm: BaseChatModel, memory: Any) -> Callable[[AgentState], Dict[str, Any]]:
    def bear_node(state: AgentState) -> dict:
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
        bear_history = prev_debate.get("bear_history", "")
        current_response = prev_debate.get("current_response", "")

        # 2. 从四个 Analyst 的 MemorySummary 中构造当前情境
        curr_situation = _build_curr_situation_from_summaries(state)
        market_research_report = state["market_analyst_summary"]["today_report"]
        sentiment_report = state["sentiment_analyst_summary"]["today_report"]
        news_report = state["news_analyst_summary"]["today_report"]
        fundamentals_report = state["fundamentals_analyst_summary"]["today_report"]
        
        past_memories = memory.get_memories(curr_situation, n_matches=2)

        past_memory_str = ""
        for i, rec in enumerate(past_memories, 1):
            past_memory_str += rec.get("recommendation", "") + "\n\n"

        prompt = f"""你是一位看跌分析师，主张不投资该股票。你的目标是提出一个有理有据的论证，强调风险、挑战和负面指标。利用提供的研究和数据，有效突出潜在的不利因素并反驳看涨观点。

重点关注：

- 风险和挑战：强调可能阻碍股票表现的因素，如市场饱和、财务不稳定或宏观经济威胁。
- 竞争劣势：强调诸如市场定位较弱、创新下降或来自竞争对手的威胁等脆弱性。
- 负面指标：使用财务数据、市场趋势或最近的不利新闻中的证据来支持你的立场。
- 看涨反驳：用具体数据和合理推理批判性地分析看涨论点，揭露弱点或过度乐观的假设。
- 参与辩论：以对话的方式呈现你的论证，直接参与看涨分析师的观点，有效辩论，而不仅仅是列举事实。

可用资源：

市场研究报告：{market_research_report}
社交媒体情绪报告：{sentiment_report}
最新时事新闻：{news_report}
公司基本面报告：{fundamentals_report}
辩论对话历史：{history}
上次看涨论点：{current_response}
类似情况的反思和经验教训：{past_memory_str}
使用这些信息来提出令人信服的看跌论证，反驳看涨的主张，并参与动态辩论，展示投资该股票的风险和弱点。你还必须处理反思，从过去的经验和错误中学习。
"""

        response = llm.invoke(prompt)
        content: str = getattr(response, "content", str(response))

        argument = f"Bear Analyst: {content}"

        # 3. 更新投资辩论状态
        new_investment_debate_state: InvestDebateState = {
            "history": history + "\n" + argument,
            "bear_history": bear_history + "\n" + argument,
            "bull_history": prev_debate.get("bull_history", ""),
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

    return bear_node
