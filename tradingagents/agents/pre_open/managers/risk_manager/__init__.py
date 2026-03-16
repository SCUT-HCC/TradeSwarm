"""
Risk Manager Agent 模块

该模块提供风险管理和交易策略制定功能。Risk Manager 作为风险评估促进者，
评估激进/保守/中立分析师的风险辩论，结合历史经验教训，
生成明确的交易策略和风险管理参数（止损、止盈、仓位大小等）。

主要导出:
    - create_risk_manager: 创建 risk manager agent 节点的工厂函数
    - RiskManagerState: Agent 状态的类型定义
"""

from .agent import create_risk_manager

__all__ = [
    "create_risk_manager",
]
