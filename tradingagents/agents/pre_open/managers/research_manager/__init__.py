"""
Research Manager Agent 模块

该模块提供投资研究管理和决策功能。Research Manager 作为投资组合经理和辩论促进者，
评估牛熊分析师的辩论，结合历史经验教训，生成明确的投资决策和详细的投资计划。

主要导出:
    - create_research_manager: 创建 research manager agent 节点的工厂函数
"""

from .agent import create_research_manager

__all__ = [
    "create_research_manager",
]
