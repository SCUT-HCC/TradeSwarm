"""
Agent State 辅助函数

提供用于处理 AgentState 的公共工具函数。
"""

from typing import Optional
from tradingagents.agents.utils.agentstate.agent_states import (
    AgentState,
    AnalystMemorySummary,
)


def build_curr_situation_from_summaries(
    state: AgentState,
    max_length: Optional[int] = None,
    include_history: bool = False,
) -> str:
    """
    从四个 Analyst 的 MemorySummary 中构造当前情境描述。
    
    该函数统一了从 AgentState 中提取分析师报告的逻辑，确保所有节点使用
    相同的数据格式和顺序。
    
    Args:
        state: 当前的 AgentState
        max_length: 可选的最大长度限制（字符数）。如果提供，会截断超长内容。
        include_history: 是否包含历史报告。如果为 True，会同时包含 today_report
            和 history_report；如果为 False，只包含 today_report。
    
    Returns:
        拼接后的情境描述字符串，格式为：
        "{market_report}\n\n{sentiment_report}\n\n{news_report}\n\n{fundamentals_report}"
        
        如果 include_history=True，每个报告会包含 today_report 和 history_report。
    
    Examples:
        >>> state = {...}  # AgentState
        >>> situation = build_curr_situation_from_summaries(state)
        >>> # 返回包含四个分析师报告的字符串
        
        >>> # 限制长度
        >>> situation = build_curr_situation_from_summaries(state, max_length=5000)
        
        >>> # 包含历史报告
        >>> situation = build_curr_situation_from_summaries(state, include_history=True)
    """
    # 提取四个 Analyst 的 MemorySummary
    market_summary: AnalystMemorySummary = state.get("market_analyst_summary", {})
    news_summary: AnalystMemorySummary = state.get("news_analyst_summary", {})
    sentiment_summary: AnalystMemorySummary = state.get("sentiment_analyst_summary", {})
    fundamentals_summary: AnalystMemorySummary = state.get("fundamentals_analyst_summary", {})
    
    # 提取 today_report
    market_report = market_summary.get("today_report", "") if market_summary else ""
    news_report = news_summary.get("today_report", "") if news_summary else ""
    sentiment_report = sentiment_summary.get("today_report", "") if sentiment_summary else ""
    fundamentals_report = fundamentals_summary.get("today_report", "") if fundamentals_summary else ""
    
    # 如果包含历史报告，追加 history_report
    if include_history:
        market_history = market_summary.get("history_report", "") if market_summary else ""
        news_history = news_summary.get("history_report", "") if news_summary else ""
        sentiment_history = sentiment_summary.get("history_report", "") if sentiment_summary else ""
        fundamentals_history = fundamentals_summary.get("history_report", "") if fundamentals_summary else ""
        
        if market_history:
            market_report = f"{market_report}\n\n{market_history}" if market_report else market_history
        if news_history:
            news_report = f"{news_report}\n\n{news_history}" if news_report else news_history
        if sentiment_history:
            sentiment_report = f"{sentiment_report}\n\n{sentiment_history}" if sentiment_report else sentiment_history
        if fundamentals_history:
            fundamentals_report = f"{fundamentals_report}\n\n{fundamentals_history}" if fundamentals_report else fundamentals_history
    
    # 顺序拼接：market -> sentiment -> news -> fundamentals
    result = (
        f"{market_report}\n\n"
        f"{sentiment_report}\n\n"
        f"{news_report}\n\n"
        f"{fundamentals_report}"
    )
    
    # 如果提供了最大长度限制，进行截断
    if max_length is not None and len(result) > max_length:
        result = result[:max_length] + "\n\n[内容已截断...]"
    
    return result.strip()

