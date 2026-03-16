"""
策略选择器 Agent（Regime 判断者）

作为市场状态（regime）判断者，根据市场分析、Trader 的交易计划（止损、仓位等）和分析师报告，
选择最适合的交易策略。

流程位置：Trader 之后，Risk Subgraph 之前
"""

from __future__ import annotations

from typing import Callable, Any, Dict, Optional
from langchain_core.language_models import BaseChatModel
import json
import re

from tradingagents.agents.utils.agentstate.agent_states import (
    AgentState,
    StrategySelection,
)
from tradingagents.agents.utils.state_helpers import build_curr_situation_from_summaries
from tradingagents.agents.utils.prompt_loader import load_prompt_template
from tradingagents.agents.utils.json_parser import extract_json_from_text


# 有效的策略类型列表（必须与 trading_sys/strategies/strategy_lib.py 中的 STRATEGY_MAPPING 匹配）
VALID_STRATEGY_TYPES = [
    "trend_following",
    "mean_reversion",
    "momentum_breakout",
    "reversal",
    "range_trading",
    "default_timing",
]


def parse_trader_output(trader_output: Optional[str]) -> Dict[str, Any]:
    """
    解析 Trader 的输出，提取交易计划信息
    
    Args:
        trader_output: Trader 节点的输出（trader_investment_plan）
        
    Returns:
        包含 action, order_plan, risk_controls 等信息的字典
    """
    if not trader_output:
        return {
            "action": "HOLD",
            "order_plan": [],
            "risk_controls": {},
            "summary": "",
            "comment": "",
        }
    
    try:
        # 尝试从 JSON 中解析
        trader_json = extract_json_from_text(trader_output)
        if trader_json:
            return {
                "action": trader_json.get("action", "HOLD"),
                "order_plan": trader_json.get("order_plan", []),
                "risk_controls": trader_json.get("risk_controls", {}),
                "summary": trader_json.get("summary", ""),
                "comment": trader_json.get("comment", ""),
            }
    except (json.JSONDecodeError, AttributeError):
        pass
    
    # 如果无法解析 JSON，尝试从文本中提取关键信息
    trader_output_upper = trader_output.upper()
    action = "HOLD"
    if "BUY" in trader_output_upper or "买入" in trader_output:
        action = "BUY"
    elif "SELL" in trader_output_upper or "卖出" in trader_output:
        action = "SELL"
    
    return {
        "action": action,
        "order_plan": [],
        "risk_controls": {},
        "summary": trader_output[:200] if trader_output else "",
        "comment": "",
    }


def create_strategy_selector(
    llm: BaseChatModel, 
    memory: Any
) -> Callable[[AgentState], Dict[str, Any]]:
    """
    创建策略选择器 Agent 节点函数（Regime 判断者）
    
    该函数返回一个符合 LangGraph 节点规范的函数，用于：
    1. 判断当前市场状态（regime）
    2. 根据市场状态和 Trader 的交易计划（止损、仓位等）选择交易策略
    
    流程位置：Trader 之后，Risk Subgraph 之前
    
    Args:
        llm: LangChain BaseChatModel 实例，用于生成策略选择
        memory: FinancialSituationMemory 实例（可选，用于检索历史经验）
        
    Returns:
        strategy_selector_node: 一个接受 AgentState 并返回更新字典的函数
        
    节点函数返回值:
        dict 包含以下键:
            - strategy_selection: StrategySelection 对象（如果 Trader 决策为 BUY/SELL）
            - 如果决策为 HOLD，则返回 default_timing 策略
    """
    
    def strategy_selector_node(state: AgentState) -> Dict[str, Any]:
        """
        策略选择器节点的执行函数（Regime 判断者）
        
        Args:
            state: 当前的 AgentState
            
        Returns:
            包含 strategy_selection 的更新字典
        """
        company_name = state.get("company_of_interest", "")
        trade_date = state.get("trade_date", "")
        
        # 1. 读取 Trader 的输出（交易计划）
        trader_output: Optional[str] = state.get("trader_investment_plan")
        trader_info = parse_trader_output(trader_output)
        trader_action = trader_info.get("action", "HOLD")
        trader_order_plan = trader_info.get("order_plan", [])
        trader_risk_controls = trader_info.get("risk_controls", {})
        trader_summary = trader_info.get("summary", "")
        trader_comment = trader_info.get("comment", "")
        
        # 2. 如果 Trader 决策为 HOLD，不选择策略
        if trader_action not in ["BUY", "SELL"]:
            print(f"[Strategy Selector] Trader 决策为 {trader_action}，不选择具体策略。")
            return {
                "strategy_selection": None,  # 返回 None，不选择策略
            }
        
        # 3. 读取分析师报告
        market_summary = state.get("market_analyst_summary", {})
        news_summary = state.get("news_analyst_summary", {})
        sentiment_summary = state.get("sentiment_analyst_summary", {})
        fundamentals_summary = state.get("fundamentals_analyst_summary", {})
        
        market_report = market_summary.get("today_report", "") if isinstance(market_summary, dict) else ""
        news_report = news_summary.get("today_report", "") if isinstance(news_summary, dict) else ""
        sentiment_report = sentiment_summary.get("today_report", "") if isinstance(sentiment_summary, dict) else ""
        fundamentals_report = fundamentals_summary.get("today_report", "") if isinstance(fundamentals_summary, dict) else ""
        
        # 4. 读取 Research Manager 的输出
        research_summary: Optional[Dict[str, Any]] = state.get("research_summary")
        investment_plan = (
            research_summary.get("investment_plan", "") if research_summary else ""
        )
        
        # 5. 构建当前情境（用于检索历史记忆，可选）
        curr_situation = build_curr_situation_from_summaries(state)
        past_memories = memory.get_memories(curr_situation, n_matches=2) if memory else []
        
        past_memory_str = ""
        if past_memories:
            for rec in past_memories:
                past_memory_str += rec.get("recommendation", "") + "\n\n"
        else:
            past_memory_str = "无历史记忆。"
        
        # 5. 准备 Trader 的交易计划信息（用于策略选择时考虑）
        trader_plan_str = json.dumps({
            "action": trader_action,
            "order_plan": trader_order_plan,
            "risk_controls": trader_risk_controls,
            "summary": trader_summary,
            "comment": trader_comment,
        }, ensure_ascii=False, indent=2)
        
        # 6. 加载并渲染 prompt 模板
        prompt = load_prompt_template(
            agent_type="managers",
            agent_name="strategy_selector",
            context={
                "company_name": company_name,
                "trade_date": trade_date,
                "trader_action": trader_action,
                "trader_plan": trader_plan_str,
                "investment_plan": investment_plan,
                "market_report": market_report,
                "news_report": news_report,
                "sentiment_report": sentiment_report,
                "fundamentals_report": fundamentals_report,
                "past_memory_str": past_memory_str,
            },
        )
        
        # 7. 调用 LLM 生成策略选择
        response = llm.invoke(prompt)
        content: str = getattr(response, "content", str(response))
        
        # 8. 解析 JSON 输出
        try:
            # 提取 JSON
            json_dict = extract_json_from_text(content)
            if json_dict is None:
                raise ValueError("无法从输出中提取 JSON")
            strategy_data = json_dict
            
            # 验证策略类型
            strategy_type = strategy_data.get("strategy_type", "").lower()
            if strategy_type not in VALID_STRATEGY_TYPES:
                # 降级到默认策略
                strategy_type = "default_timing"
                strategy_data["strategy_type"] = strategy_type
                strategy_data["reasoning"] = (
                    strategy_data.get("reasoning", "") + 
                    f"\n\n[注意：原始策略类型无效，已降级到 default_timing]"
                )
            
            # 构建 StrategySelection 对象（包含标准化决策字段）
            strategy_selection: StrategySelection = {
                "strategy_type": strategy_type,  # type: ignore
                "reasoning": strategy_data.get("reasoning", ""),
                "strategy_analysis": strategy_data.get("strategy_analysis", ""),
                "risk_adjustment": strategy_data.get("risk_adjustment"),
                "confidence": float(strategy_data.get("confidence", 0.5)),
                "alternative_strategies": strategy_data.get("alternative_strategies"),
                "market_regime": strategy_data.get("market_regime"),
                "expected_behavior": strategy_data.get("expected_behavior"),
            }
            
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            # 解析失败，使用默认策略
            strategy_selection: StrategySelection = {
                "strategy_type": "default_timing",  # type: ignore
                "reasoning": f"策略选择解析失败: {str(e)}，使用默认策略。",
                "strategy_analysis": "由于无法解析策略选择结果，使用默认择时策略。",
                "risk_adjustment": None,
                "confidence": 0.3,
                "alternative_strategies": None,
            }
        
        return {
            "strategy_selection": strategy_selection,
        }
    
    return strategy_selector_node

