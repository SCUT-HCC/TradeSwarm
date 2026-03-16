from typing import TypedDict


class ResearchManagerState(TypedDict):
    """
    Research Manager Agent 的状态定义
    
    该状态用于存储研究经理 agent 运行过程中的关键数据。
    Research Manager 作为投资组合经理和辩论促进者，评估牛熊分析师的辩论并做出投资决策。
    严格遵循研究型代码规范：不允许使用默认值，确保所有字段都被显式提供。
    
    字段说明:
        investment_debate_state: 投资辩论状态字典，包含牛熊分析师的辩论历史和判断
        market_report: 市场分析报告
        sentiment_report: 社交媒体情绪分析报告
        news_report: 新闻分析报告
        fundamentals_report: 基本面分析报告
        investment_plan: 生成的投资计划，由本 agent 输出
    """
    investment_debate_state: dict
    market_report: str
    sentiment_report: str
    news_report: str
    fundamentals_report: str
    investment_plan: str
