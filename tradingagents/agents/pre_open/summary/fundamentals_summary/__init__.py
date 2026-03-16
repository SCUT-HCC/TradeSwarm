"""
Fundamentals Summary Node 模块

该模块提供 Fundamentals Analyst 的 Summary 节点，用于从数据库读取 Fundamentals Analyst 的
today_report 和 history_report，填充到 AgentState。

主要导出:
    - create_fundamentals_summary_node: 创建 fundamentals summary 节点的工厂函数
"""

from .node import create_fundamentals_summary_node

__all__ = [
    "create_fundamentals_summary_node",
]

