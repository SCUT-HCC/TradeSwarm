from __future__ import annotations

from typing import Any, Dict, Callable
from langchain_core.language_models import BaseChatModel
import functools
import time
import json

from tradingagents.agents.utils.agentstate.agent_states import AgentState
from tradingagents.agents.utils.state_helpers import build_curr_situation_from_summaries
from tradingagents.agents.utils.prompt_loader import load_prompt_template


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
        curr_situation = build_curr_situation_from_summaries(state)
        past_memories = memory.get_memories(curr_situation, n_matches=2)

        past_memory_str = ""
        if past_memories:
            for i, rec in enumerate(past_memories, 1):
                past_memory_str += rec["recommendation"] + "\n\n"
        else:
            past_memory_str = "No past memories found."

        # 从四个 Analyst 的 MemorySummary 中获取 7 日 history 摘要
        market_summary = state["market_analyst_summary"]
        news_summary = state["news_analyst_summary"]
        sentiment_summary = state["sentiment_analyst_summary"]
        fundamentals_summary = state["fundamentals_analyst_summary"]

        market_history_summary = market_summary.get("history_report", "")
        news_history_summary = news_summary.get("history_report", "")
        sentiment_history_summary = sentiment_summary.get("history_report", "")
        fundamentals_history_summary = fundamentals_summary.get("history_report", "")

        # 加载并渲染 system prompt 模板
        system_prompt = load_prompt_template(
            agent_type="trader",
            agent_name="trader",
            context={
                "past_memory_str": past_memory_str,
                "market_history_summary": market_history_summary,
                "news_history_summary": news_history_summary,
                "sentiment_history_summary": sentiment_history_summary,
                "fundamentals_history_summary": fundamentals_history_summary,
            },
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
        
        # 加载并渲染 user prompt 模板
        from pathlib import Path
        from jinja2 import Template
        user_prompt_path = Path(__file__).parent / "prompt.j2"
        if user_prompt_path.exists():
            with open(user_prompt_path, "r", encoding="utf-8") as f:
                user_template = Template(f.read())
                user_prompt = user_template.render(
                    company_name=company_name,
                    investment_plan=investment_plan,
                    position_info=position_info,
                    portfolio_info=portfolio_info,
                )
        else:
            # Fallback
            user_prompt = f"基于分析师团队的全面分析，以下是针对 {company_name} 量身定制的投资计划。该计划融合了当前技术市场趋势、宏观经济指标和社交媒体情绪的洞察。使用此计划作为评估你下一个交易决策的基础。\n\n拟议投资计划：{investment_plan}\n{position_info}{portfolio_info}\n利用这些洞察做出明智且具有战略性的决策。"

        messages = [
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": user_prompt,
            },
        ]

        result = llm.invoke(messages)
        content: str = getattr(result, "content", str(result))

        return {
            "messages": [result],
            "trader_investment_plan": content,
        }

    return trader_node
