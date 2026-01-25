"""
News Summary Node 模块

该模块提供 News Analyst 的 Summary 节点，用于从数据库读取 News Analyst 的
today_report 和 history_report，填充到 AgentState。

主要导出:
    - create_news_summary_node: 创建 news summary 节点的工厂函数
"""

from .node import create_news_summary_node

__all__ = [
    "create_news_summary_node",
]

