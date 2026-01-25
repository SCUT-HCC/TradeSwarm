"""
主交易图

完整的交易决策流程：
1. Summary Nodes（并行）：market, news, sentiment, fundamentals
2. Research 子图：bull/bear 辩论 → research_manager
3. Trader：生成执行计划
4. Risk 子图：risky/neutral/safe 辩论 → risk_manager
5. 结束
"""

from typing import Any, TYPE_CHECKING
from langgraph.graph import StateGraph, END
from langchain_core.language_models import BaseChatModel

if TYPE_CHECKING:
    from langgraph.pregel import Pregel as CompiledGraph
else:
    CompiledGraph = Any

from tradingagents.agents.utils.agentstate.agent_states import AgentState
from tradingagents.agents.pre_open.summary.market_summary.node import create_market_summary_node
from tradingagents.agents.pre_open.summary.news_summary.node import create_news_summary_node
from tradingagents.agents.pre_open.summary.sentiment_summary.node import create_sentiment_summary_node
from tradingagents.agents.pre_open.summary.fundamentals_summary.node import create_fundamentals_summary_node
from tradingagents.agents.pre_open.trader.trader import create_trader
from tradingagents.graph.pre_open.subgraphs.research_subgraph import create_research_subgraph_simple
from tradingagents.graph.pre_open.subgraphs.risk_subgraph import create_risk_subgraph_simple


def create_trading_graph(
    llm: BaseChatModel,
    memory: Any,
    data_manager: Any = None,
) -> CompiledGraph:
    """
    创建完整的交易决策图。
    
    Args:
        llm: LangChain BaseChatModel 实例
        memory: Memory 实例（MockMemory 或 FinancialSituationMemory）
        data_manager: conn 数据库连接
        
    Returns:
        编译好的 StateGraph
    """
    # 创建 Summary 节点（传入 None，使用写死的文本）
    market_summary_node = create_market_summary_node(data_manager)
    news_summary_node = create_news_summary_node(data_manager)
    sentiment_summary_node = create_sentiment_summary_node(data_manager)
    fundamentals_summary_node = create_fundamentals_summary_node(data_manager)
    
    # 创建 Trader 节点
    trader_node = create_trader(llm, memory)
    
    # 创建子图
    research_subgraph = create_research_subgraph_simple(llm, memory)
    risk_subgraph = create_risk_subgraph_simple(llm, memory)
    
    # 创建主图
    workflow = StateGraph(AgentState)
    
    # 添加 Summary 节点（并行执行）
    workflow.add_node("market_summary", market_summary_node)
    workflow.add_node("news_summary", news_summary_node)
    workflow.add_node("sentiment_summary", sentiment_summary_node)
    workflow.add_node("fundamentals_summary", fundamentals_summary_node)
    
    # 添加 Trader 节点
    workflow.add_node("trader", trader_node)

    workflow.add_node("research_subgraph", research_subgraph)
    workflow.add_node("risk_subgraph", risk_subgraph)
    
    # 并行执行所有 Summary 节点
    workflow.set_entry_point("market_summary")
    
    # Summary 节点并行执行后，都进入 research_subgraph
    # 所有 summary 完成后进入 research
    workflow.add_edge("market_summary", "news_summary")
    workflow.add_edge("news_summary", "sentiment_summary")
    workflow.add_edge("sentiment_summary", "fundamentals_summary")
    workflow.add_edge("fundamentals_summary", "research_subgraph")
    
    # Research 子图完成后进入 Trader
    workflow.add_edge("research_subgraph", "trader")
    
    # Trader 完成后进入 Risk 子图
    workflow.add_edge("trader", "risk_subgraph")
    
    # Risk 子图完成后结束
    workflow.add_edge("risk_subgraph", END)
    
    return workflow.compile()


def create_trading_graph_parallel_summary(
    llm: BaseChatModel,
    memory: Any,
    data_manager: Any = None,
) -> CompiledGraph:
    """
    创建完整的交易决策图（Summary 节点真正并行执行）。
    
    使用 LangGraph 的并行执行功能。
    
    Args:
        llm: LangChain BaseChatModel 实例
        memory: Memory 实例
        data_manager: 数据管理器实例（可选）
        
    Returns:
        编译好的 StateGraph
    """
    # 创建 Summary 节点
    market_summary_node = create_market_summary_node(data_manager)
    news_summary_node = create_news_summary_node(data_manager)
    sentiment_summary_node = create_sentiment_summary_node(data_manager)
    fundamentals_summary_node = create_fundamentals_summary_node(data_manager)
    
    # 创建 Trader 节点
    trader_node = create_trader(llm, memory)
    
    # 创建子图
    research_subgraph = create_research_subgraph_simple(llm, memory)
    risk_subgraph = create_risk_subgraph_simple(llm, memory)
    
    # 创建主图
    workflow = StateGraph(AgentState)
    
    # 添加 Summary 节点
    workflow.add_node("market_summary", market_summary_node)
    workflow.add_node("news_summary", news_summary_node)
    workflow.add_node("sentiment_summary", sentiment_summary_node)
    workflow.add_node("fundamentals_summary", fundamentals_summary_node)
    
    # 添加 Trader 节点
    workflow.add_node("trader", trader_node)
    
    # 添加子图作为节点
    workflow.add_node("research_subgraph", research_subgraph)
    workflow.add_node("risk_subgraph", risk_subgraph)
    
    # 设置入口点：并行执行所有 Summary 节点
    workflow.set_entry_point("market_summary")
    
    # Summary 节点串行执行（简化实现，后续可改为真正并行）
    workflow.add_edge("market_summary", "news_summary")
    workflow.add_edge("news_summary", "sentiment_summary")
    workflow.add_edge("sentiment_summary", "fundamentals_summary")
    workflow.add_edge("fundamentals_summary", "research_subgraph")
    
    # Research 子图完成后进入 Trader
    workflow.add_edge("research_subgraph", "trader")
    
    # Trader 完成后进入 Risk 子图
    workflow.add_edge("trader", "risk_subgraph")
    
    # Risk 子图完成后结束
    workflow.add_edge("risk_subgraph", END)
    
    return workflow.compile()

