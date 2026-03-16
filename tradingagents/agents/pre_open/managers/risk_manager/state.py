from typing import TypedDict


class RiskManagerState(TypedDict):
    """
    Risk Manager Agent 的状态定义
    
    该状态用于存储风险经理 agent 运行过程中的关键数据。
    Risk Manager 作为风险评估促进者，评估激进/保守/中立分析师的风险辩论并做出交易决策。
    严格遵循研究型代码规范：不允许使用默认值，确保所有字段都被显式提供。
    
    字段说明:
        risk_debate_state: 风险辩论状态字典，包含激进/保守/中立分析师的辩论历史和判断
        investment_plan: 投资计划（来自 research manager）
        trader_investment_plan: 生成的交易员投资计划，由本 agent 输出
    """
    risk_debate_state: dict
    investment_plan: str
    trader_investment_plan: str
