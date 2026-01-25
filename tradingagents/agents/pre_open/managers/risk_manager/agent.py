from __future__ import annotations

from typing import Callable, Any, Dict
from pathlib import Path
from jinja2 import Template
from langchain_core.language_models import BaseChatModel

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


def create_risk_manager(llm: BaseChatModel, memory: Any) -> Callable[[AgentState], Dict[str, Any]]:
    """
    创建 Risk Manager agent 节点函数
    
    该函数返回一个符合 LangGraph 节点规范的函数，用于评估风险辩论并生成交易决策。
    Risk Manager 作为风险评估促进者，综合激进/保守/中立分析师的观点，
    结合历史记忆中的经验教训，制定明确的交易策略和风险管理参数。
    
    Args:
        llm: LangChain BaseChatModel 实例，用于生成风险评估和交易计划
        memory: FinancialSituationMemory 实例，用于检索过去相似风险评估的经验教训
        
    Returns:
        risk_manager_node: 一个接受 AgentState 并返回更新字典的函数
        
    节点函数返回值:
        dict 包含以下键:
            - risk_summary: 包含 risk_debate_state 和 final_trade_decision 的封装结果
            - final_trade_decision: 最终交易决策文本
    """
    
    def risk_manager_node(state: AgentState) -> Dict[str, Any]:
        """
        Risk Manager 节点的执行函数
        
        Args:
            state: 当前的 AgentState
            
        Returns:
            包含 risk_summary 和 final_trade_decision 的更新字典
        """
        company_name = state["company_of_interest"]

        # 1. 读取上一轮辩论状态（封装在 risk_summary 内）
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

        # 2. 从四个 Analyst 的 MemorySummary 中构造当前情境
        curr_situation = _build_curr_situation_from_summaries(state)
        past_memories = memory.get_memories(curr_situation, n_matches=2)

        past_memory_str = ""
        for rec in past_memories:
            past_memory_str += rec.get("recommendation", "") + "\n\n"

        # 3. 读取 research_summary 中的 investment_plan
        research_summary: Dict[str, Any] | None = state.get("research_summary")  # type: ignore[assignment]
        trader_plan = (
            research_summary.get("investment_plan", "")  # type: ignore[union-attr]
            if research_summary is not None
            else state.get("investment_plan", "")
        )

        # 4. 加载并渲染 prompt 模板（如果存在）
        prompt_path = Path(__file__).parent / "prompt.j2"
        if prompt_path.exists():
            with open(prompt_path, "r", encoding="utf-8") as f:
                template = Template(f.read())
            prompt = template.render(
                trader_plan=trader_plan,
                past_memory_str=past_memory_str,
                history=history
            )
        else:
            # 如果没有模板文件，使用默认 prompt
            prompt = f"""作为风险管理法官和辩论促进者，你的目标是评估三位风险分析师——激进、中性和保守——之间的辩论，并为交易员确定最佳行动方案。你的决策必须得出明确的建议：买入、卖出或持有。仅在有具体论据充分支持时才选择持有，不要在所有观点看似合理时将其作为默认选项。力求清晰和果断。

决策指导原则：
1. **总结关键论点**：提取每位分析师最有力的观点，重点关注与当前情况的关联性。
2. **提供理由**：用辩论中的直接引用和反驳来支持你的建议。
3. **完善交易员的计划**：从交易员的原始计划 **{trader_plan}** 开始，根据分析师的观点进行调整。
4. **从过往错误中学习**：运用 **{past_memory_str}** 中的经验教训来纠正先前的误判，改进你现在的决策，确保不会做出导致亏损的错误买入/卖出/持有决定。

交付内容：
- 清晰且可执行的建议：买入、卖出或持有。
- 基于辩论和过往反思的详细推理。

---

**分析师辩论历史：**  
{history}

---

专注于可执行的洞察和持续改进。基于过往经验，批判性地评估所有观点，确保每个决策都能带来更好的结果。"""

        # 5. 调用 LLM 生成决策
        response = llm.invoke(input=prompt)
        content: str = getattr(response, "content", str(response))

        # 6. 更新风险辩论状态
        new_risk_debate_state: RiskDebateState = {
            "judge_decision": content,
            "history": prev_debate.get("history", ""),
            "risky_history": prev_debate.get("risky_history", ""),
            "safe_history": prev_debate.get("safe_history", ""),
            "neutral_history": prev_debate.get("neutral_history", ""),
            "latest_speaker": "Judge",
            "current_risky_response": prev_debate.get("current_risky_response", ""),
            "current_safe_response": prev_debate.get("current_safe_response", ""),
            "current_neutral_response": prev_debate.get("current_neutral_response", ""),
            "count": prev_debate.get("count", 0) + 1,
        }

        # 7. 构建新的 risk_summary
        new_summary: RiskSummary = {
            "risk_debate_state": new_risk_debate_state,
            "final_trade_decision": content,
            "raw_response": content,
        }

        return {
            "risk_summary": new_summary,
            "final_trade_decision": content,
        }
    
    return risk_manager_node
