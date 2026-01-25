"""
Risk 子图

实现 Risk 辩论流程：
- 固定 2 轮辩论：risky → neutral → safe → risky → neutral → safe
- 最后交由 risk_manager 生成最终决策
"""

from typing import Any, TYPE_CHECKING
from langgraph.graph import StateGraph, END
from langchain_core.language_models import BaseChatModel

if TYPE_CHECKING:
    from langgraph.graph.graph import CompiledGraph
else:
    CompiledGraph = Any

from tradingagents.agents.utils.agentstate.agent_states import AgentState
from tradingagents.agents.pre_open.risk_mgmt.aggresive_debator import create_risky_debator
from tradingagents.agents.pre_open.risk_mgmt.neutral_debator import create_neutral_debator
from tradingagents.agents.pre_open.risk_mgmt.conservative_debator import create_safe_debator
from tradingagents.agents.pre_open.managers.risk_manager.agent import create_risk_manager


def should_continue_risk_debate(state: AgentState) -> str:
    """
    判断是否继续风险辩论（基于辩论轮次）。
    
    固定 2 轮辩论，通过检查 risk_summary 中的 count 来判断。
    每轮包含 risky、neutral、safe 各一次发言，共 6 次发言。
    
    Args:
        state: 当前的 AgentState
        
    Returns:
        "continue" 表示继续辩论，"finish" 表示进入 manager
    """
    risk_summary = state.get("risk_summary")
    if risk_summary is None:
        return "continue"
    
    debate_state = risk_summary.get("risk_debate_state")
    if debate_state is None:
        return "continue"
    
    count = debate_state.get("count", 0)
    # 固定 2 轮，每轮 3 次发言（risky + neutral + safe），共 6 次
    # 当 count >= 6 时，进入 manager
    if count >= 6:
        return "finish"
    return "continue"


def create_risk_subgraph_simple(
    llm: BaseChatModel,
    memory: Any,
) -> CompiledGraph:
    """
    创建 Risk 子图（固定 2 轮）。
    
    流程：risky → neutral → safe → risky → neutral → safe → risk_manager → END
    
    使用条件边来控制辩论轮次。
    
    Args:
        llm: LangChain BaseChatModel 实例
        memory: Memory 实例
        
    Returns:
        编译好的 StateGraph 子图
    """
    # 创建节点
    risky_node = create_risky_debator(llm)
    neutral_node = create_neutral_debator(llm)
    safe_node = create_safe_debator(llm)
    manager_node = create_risk_manager(llm, memory)
    
    # 创建子图
    workflow = StateGraph(AgentState)
    
    # 添加节点
    workflow.add_node("risky_debator", risky_node)
    workflow.add_node("neutral_debator", neutral_node)
    workflow.add_node("safe_debator", safe_node)
    workflow.add_node("risk_manager", manager_node)
    
    # 设置入口点
    workflow.set_entry_point("risky_debator")
    
    # 第 1 轮：risky → neutral → safe
    workflow.add_edge("risky_debator", "neutral_debator")
    workflow.add_edge("neutral_debator", "safe_debator")
    
    # safe → 判断是否继续辩论
    workflow.add_conditional_edges(
        "safe_debator",
        should_continue_risk_debate,
        {
            "continue": "risky_debator",  # 继续辩论，回到 risky（第 2 轮）
            "finish": "risk_manager",  # 辩论结束，进入 manager
        }
    )
    
    # manager 后结束
    workflow.add_edge("risk_manager", END)
    
    return workflow.compile()

