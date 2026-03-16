from __future__ import annotations

from typing import Callable, Any, Dict
from langchain_core.language_models import BaseChatModel

from tradingagents.agents.utils.agentstate.agent_states import (
    AgentState,
    RiskDebateState,
    RiskSummary,
)
from tradingagents.agents.utils.state_helpers import build_curr_situation_from_summaries
from tradingagents.agents.utils.prompt_loader import load_prompt_template


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

        # 3. 读取 research_summary 中的 investment_plan
        research_summary: Dict[str, Any] | None = state.get("research_summary")  # type: ignore[assignment]
        trader_plan = (
            research_summary.get("investment_plan", "")  # type: ignore[union-attr]
            if research_summary is not None
            else state.get("investment_plan", "")
        )

        # 读取当前仓位信息
        current_position = state.get("current_position")
        portfolio_state = state.get("portfolio_state")
        
        # 格式化仓位信息
        position_info = ""
        if current_position:
            position_info = f"""
当前持仓信息：
- 持仓股数: {current_position.get('shares', 0):.0f}
- 建仓价格: ${current_position.get('entry_price', 0):.2f}
- 建仓日期: {current_position.get('entry_date', '')}
- 当前价格: ${current_position.get('current_price', 0):.2f}
- 盈亏金额: ${current_position.get('pnl', 0):.2f}
- 盈亏百分比: {current_position.get('pnl_pct', 0):.2f}%
- 止损价: ${current_position.get('stop_loss_price', 0):.2f if current_position.get('stop_loss_price') else '未设置'}
- 止盈价: ${current_position.get('take_profit_price', 0):.2f if current_position.get('take_profit_price') else '未设置'}
"""
        else:
            position_info = "\n当前未持仓。\n"
        
        # 格式化组合状态
        portfolio_info = ""
        if portfolio_state:
            portfolio_info = f"""
组合状态：
- 总资产: ${portfolio_state.get('total_value', 0):,.2f}
- 现金: ${portfolio_state.get('cash', 0):,.2f}
- 持仓市值: ${portfolio_state.get('positions_value', 0):,.2f}
- 总收益率: {portfolio_state.get('total_return', 0):.2f}%
"""

        # 4. 加载并渲染 prompt 模板
        prompt = load_prompt_template(
            agent_type="managers",
            agent_name="risk_manager",
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
                # 上游研究计划 & 仓位信息 & 辩论历史
                "trader_plan": trader_plan,
                "history": history,
                "position_info": position_info,
                "portfolio_info": portfolio_info,
            },
        )

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
