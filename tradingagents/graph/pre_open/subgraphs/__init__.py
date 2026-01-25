"""
子图模块 (Subgraphs Module)

提供可复用的子图组件，用于构建复杂的交易决策流程。

主要导出:
    - create_research_subgraph_simple: 创建 Research 子图（固定 2 轮辩论）
    - create_risk_subgraph_simple: 创建 Risk 子图（固定 2 轮辩论）
"""

from tradingagents.graph.pre_open.subgraphs.research_subgraph import (
    create_research_subgraph_simple,
)
from tradingagents.graph.pre_open.subgraphs.risk_subgraph import (
    create_risk_subgraph_simple,
)

__all__ = [
    "create_research_subgraph_simple",
    "create_risk_subgraph_simple",
]

