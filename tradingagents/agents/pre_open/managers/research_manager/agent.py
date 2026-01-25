from __future__ import annotations

from typing import Callable, Any, Dict
from pathlib import Path
from jinja2 import Template
from langchain_core.language_models import BaseChatModel

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


def create_research_manager(llm: BaseChatModel, memory: Any) -> Callable[[AgentState], Dict[str, Any]]:
    """
    创建 Research Manager agent 节点函数
    
    该函数返回一个符合 LangGraph 节点规范的函数，用于评估投资辩论并生成投资计划。
    Research Manager 作为投资组合经理和辩论促进者，综合牛熊分析师的观点，
    结合历史记忆中的经验教训，做出明确的投资决策（Buy/Sell/Hold）。
    
    Args:
        llm: LangChain BaseChatModel 实例，用于生成投资决策和计划
        memory: FinancialSituationMemory 实例，用于检索过去相似情况的经验教训
        
    Returns:
        research_manager_node: 一个接受 AgentState 并返回更新字典的函数
        
    节点函数返回值:
        dict 包含以下键:
            - research_summary: 包含 investment_debate_state 和 investment_plan 的封装结果
            - investment_plan: 生成的投资计划文本
    """
    
    def research_manager_node(state: AgentState) -> Dict[str, Any]:
        """
        Research Manager 节点的执行函数
        
        Args:
            state: 当前的 AgentState
            
        Returns:
            包含 research_summary 和 investment_plan 的更新字典
        """
        # 1. 读取上一轮辩论状态（封装在 research_summary 内）
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

        # 2. 从四个 Analyst 的 MemorySummary 中构造当前情境
        curr_situation = _build_curr_situation_from_summaries(state)
        past_memories = memory.get_memories(curr_situation, n_matches=2)

        past_memory_str = ""
        for rec in past_memories:
            past_memory_str += rec.get("recommendation", "") + "\n\n"

        # 3. 加载并渲染 prompt 模板
        prompt_path = Path(__file__).parent / "prompt.j2"
        if prompt_path.exists():
            with open(prompt_path, "r", encoding="utf-8") as f:
                template = Template(f.read())
            prompt = template.render(
                past_memory_str=past_memory_str,
                history=history
            )
        else:
            # 如果没有模板文件，使用默认 prompt
            prompt = f"""作为投资组合经理和辩论促进者，你的职责是批判性地评估本轮辩论并做出明确决策：支持看跌分析师、看涨分析师，或仅在基于所提论点有充分理由时选择持有。

简洁地总结双方的关键观点，重点关注最有说服力的证据或推理。你的建议——买入、卖出或持有——必须清晰且可执行。不要仅仅因为双方都有有效观点而默认选择持有；要基于辩论中最有力的论点做出承诺。

此外，为交易员制定详细的投资计划。这应包括：

你的建议：基于最有说服力的论点支持的明确立场。
理由：解释这些论点如何导致你的结论。
战略行动：实施建议的具体步骤。

考虑你在类似情况下的过往错误。运用这些洞察来完善决策过程，确保你不断学习和改进。以对话的方式呈现你的分析，就像自然说话一样，不要使用特殊格式。

以下是你对过往错误的反思：
\"{past_memory_str}\"

以下是辩论：
辩论历史：
{history}"""

        # 4. 调用 LLM 生成决策
        response = llm.invoke(prompt)
        content: str = getattr(response, "content", str(response))

        # 5. 更新投资辩论状态
        new_investment_debate_state: InvestDebateState = {
            "judge_decision": content,
            "history": prev_debate.get("history", ""),
            "bear_history": prev_debate.get("bear_history", ""),
            "bull_history": prev_debate.get("bull_history", ""),
            "current_response": content,
            "count": prev_debate.get("count", 0) + 1,
        }

        # 6. 构建新的 research_summary
        new_summary: ResearchSummary = {
            "investment_debate_state": new_investment_debate_state,
            "investment_plan": content,
            "raw_response": content,
        }

        return {
            "research_summary": new_summary,
            "investment_plan": content,
        }
    
    return research_manager_node
