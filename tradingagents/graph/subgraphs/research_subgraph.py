"""
Research 子图

实现 Research 辩论流程：
- 固定 2 轮辩论：bull → bear → bull → bear
- 最后交由 research_manager 生成最终决策
"""

from typing import Any, TYPE_CHECKING
from langgraph.graph import StateGraph, END
from langchain_core.language_models import BaseChatModel

from tradingagents.agents.utils.agentstate.agent_states import AgentState
from tradingagents.agents.pre_open.researchers.bull_researcher import create_bull_researcher
from tradingagents.agents.pre_open.researchers.bear_researcher import create_bear_researcher
from tradingagents.agents.pre_open.managers.research_manager.agent import create_research_manager

if TYPE_CHECKING:
    from langgraph.graph.graph import CompiledGraph
else:
    CompiledGraph = Any


def should_continue_debate(state: AgentState) -> str:
    """
    判断是否继续辩论（基于辩论轮次）。
    
    固定 2 轮辩论，通过检查 research_summary 中的 count 来判断。
    每轮包含 bull 和 bear 各一次发言，共 4 次发言。
    
    Args:
        state: 当前的 AgentState
        
    Returns:
        "continue" 表示继续辩论，"finish" 表示进入 manager
    """
    research_summary = state.get("research_summary")
    if research_summary is None:
        return "continue"
    
    debate_state = research_summary.get("investment_debate_state")
    if debate_state is None:
        return "continue"
    
    count = debate_state.get("count", 0)
    # 固定 2 轮，每轮 2 次发言（bull + bear），共 4 次
    # 当 count >= 4 时，进入 manager
    if count >= 4:
        return "finish"
    return "continue"


def create_research_subgraph_simple(
    llm: BaseChatModel,
    memory: Any,
) -> CompiledGraph:
    """
    创建 Research 子图（固定 2 轮）。
    
    流程：bull → bear → bull → bear → research_manager → END
    
    使用条件边来控制辩论轮次。
    
    Args:
        llm: LangChain BaseChatModel 实例
        memory: Memory 实例
        
    Returns:
        编译好的 StateGraph 子图
    """
    # 创建节点
    bull_node = create_bull_researcher(llm, memory)
    bear_node = create_bear_researcher(llm, memory)
    manager_node = create_research_manager(llm, memory)
    
    # 创建子图
    workflow = StateGraph(AgentState)
    
    # 添加节点
    workflow.add_node("bull_researcher", bull_node)
    workflow.add_node("bear_researcher", bear_node)
    workflow.add_node("research_manager", manager_node)
    
    # 设置入口点
    workflow.set_entry_point("bull_researcher")
    
    # 使用条件边控制流程
    # bull → bear（总是执行）
    workflow.add_edge("bull_researcher", "bear_researcher")
    
    # bear → 判断是否继续辩论
    workflow.add_conditional_edges(
        "bear_researcher",
        should_continue_debate,
        {
            "continue": "bull_researcher",  # 继续辩论，回到 bull
            "finish": "research_manager",  # 辩论结束，进入 manager
        }
    )
    
    # manager 后结束
    workflow.add_edge("research_manager", END)
    
    return workflow.compile()

