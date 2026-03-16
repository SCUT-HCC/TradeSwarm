from __future__ import annotations

from typing import Callable, Any, Dict
from langchain_core.language_models import BaseChatModel

from tradingagents.agents.utils.agentstate.agent_states import (
    AgentState,
    InvestDebateState,
    ResearchSummary,
)
from tradingagents.agents.utils.state_helpers import build_curr_situation_from_summaries
from tradingagents.agents.utils.prompt_loader import load_prompt_template


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

        # 2. 从四个 Analyst 的 MemorySummary 中构造当前情境，并检索历史经验记忆
        curr_situation = build_curr_situation_from_summaries(state)

        past_memories = memory.get_memories(curr_situation, n_matches=2)
        past_memory_str = ""
        for rec in past_memories:
            past_memory_str += rec.get("recommendation", "") + "\n\n"

        market_summary = state["market_analyst_summary"]
        news_summary = state["news_analyst_summary"]
        sentiment_summary = state["sentiment_analyst_summary"]
        fundamentals_summary = state["fundamentals_analyst_summary"]

        # 7 日 history 摘要
        market_history_summary = market_summary["history_report"]
        news_history_summary = news_summary["history_report"]
        sentiment_history_summary = sentiment_summary["history_report"]
        fundamentals_history_summary = fundamentals_summary["history_report"]

        # 当日 four analysts 的 today_report
        market_today_report = market_summary["today_report"]
        news_today_report = news_summary["today_report"]
        sentiment_today_report = sentiment_summary["today_report"]
        fundamentals_today_report = fundamentals_summary["today_report"]

        # 3. 加载并渲染 prompt 模板
        prompt = load_prompt_template(
            agent_type="managers",
            agent_name="research_manager",
            context={
                # 7 日脉络
                "market_history_summary": market_history_summary,
                "news_history_summary": news_history_summary,
                "sentiment_history_summary": sentiment_history_summary,
                "fundamentals_history_summary": fundamentals_history_summary,
                # 当日四位分析师的摘要
                "market_today_report": market_today_report,
                "news_today_report": news_today_report,
                "sentiment_today_report": sentiment_today_report,
                "fundamentals_today_report": fundamentals_today_report,
                 # 历史经验记忆
                "past_memory_str": past_memory_str,
                # 辩论历史
                "history": history,
            },
        )

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
