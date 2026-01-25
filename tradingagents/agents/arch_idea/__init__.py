"""
Fusion Layer 模块

负责从四个 Analyst 的 Memory Controller 中聚合信息，构建 Fusion State
"""
from tradingagents.agents.utils.agentstate.agent_states import AgentState, AnalystMemorySummary
from .memory_controller import IMemoryController, BaseMemoryController

__all__ = [
    'AgentState',
    'AnalystMemorySummary',
    'IMemoryController',
    'BaseMemoryController',
]

