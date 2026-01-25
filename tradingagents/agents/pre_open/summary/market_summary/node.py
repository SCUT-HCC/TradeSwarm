"""
Market Summary Node

从数据库读取 Market Analyst 的 today_report 和 history_report，填充到 AgentState。
"""

from typing import Callable, Any, Dict
from tradingagents.agents.utils.agentstate.agent_states import AgentState, AnalystMemorySummary


def create_market_summary_node(conn: Any) -> Callable[[AgentState], Dict[str, Any]]:
    """
    创建 Market Summary 节点函数
    
    该函数返回一个符合 LangGraph 节点规范的函数，用于从数据库读取 Market Analyst 的
    报告摘要并填充到 AgentState。
    
    Args:
        conn: 数据管理器实例（用于查询数据库）
        
    Returns:
        market_summary_node: 一个接受 AgentState 并返回更新字典的函数
        
    节点函数返回值:
        dict 包含以下键:
            - market_analyst_summary: AnalystMemorySummary 对象，包含 today_report 和 history_report
    
    实现细节:
        - 从数据库查询 Market Analyst 的 today_report（可能是当日多个快照中的最新一个）
        - 从数据库查询 history_report（根据 trading_session 决定）
        - Market Analyst 是分钟级别更新（intraday）
    """
    
    def market_summary_node(state: AgentState) -> Dict[str, Any]:
        """
        Market Summary 节点的执行函数
        
        Args:
            state: 当前的 AgentState
            
        Returns:
            包含 market_analyst_summary 的更新字典
        """
        # 第一阶段：提取参数
        symbol = state["company_of_interest"]
        trade_date = state["trade_date"]
        trading_session = state.get("trading_session", "post_close")
        
        # 第二阶段：查询数据库
        # TODO: 实际实现时，从数据库查询
        # SQL 示例：
        # SELECT report_content FROM analyst_reports
        # WHERE analyst_type='market' AND symbol=? AND trade_date=?
        # ORDER BY trade_timestamp DESC LIMIT 1
        today_report = _query_today_report(conn, symbol, trade_date)
        
        # TODO: 实际实现时，从数据库查询 history_report 字段
        # SQL 示例：
        # SELECT history_report FROM analyst_reports
        # WHERE analyst_type='market' AND symbol=? AND trade_date=?
        # ORDER BY created_at DESC LIMIT 1
        history_report = _query_history_report(conn, symbol, trade_date, trading_session)
        
        # 第三阶段：构建 AnalystMemorySummary
        summary: AnalystMemorySummary = {
            "today_report": today_report,
            "history_report": history_report
        }
        
        return {
            "market_analyst_summary": summary
        }
    
    return market_summary_node


def _query_today_report(conn: Any, symbol: str, trade_date: str) -> str:
    """
    从数据库查询 Market Analyst 的今日报告
    
    注意：Market Analyst 可能在同一日生成多个快照，应返回最新的一个。
    
    Args:
        conn: 数据管理器实例
        symbol: 股票代码（如 '000001'）
        trade_date: 交易日期（如 '2024-01-15'）
    
    Returns:
        今日最新报告内容
    """
    try:
        # 使用 conn 的 cursor 获取游标
        cursor = conn.cursor()
        
        # 执行查询：Market 按周更新
        sql = """
            SELECT report_content 
            FROM analyst_reports
            WHERE analyst_type='market' 
                AND symbol=? 
                AND trade_date=?
            ORDER BY created_at DESC 
            LIMIT 1
        """
        
        cursor.execute(sql, (symbol, trade_date))

        # 返回结果是一个元组
        result = cursor.fetchone()
        cursor.close()
        
        # 如果查询到结果，返回标题 + 报告内容
        if result and result[0]:
            return f"# Market Analysis Report - {symbol} ({trade_date})\n\n{result[0]}"
        else:
            # 如果没有查询到结果，返回标题 + 提示信息
            return f"# Market Analysis Report - {symbol} ({trade_date})\n\n未找到基本面分析报告数据。"
            
    except Exception as e:
        # 异常处理：如果查询失败，返回错误信息（或可以记录日志）
        print(f"查询基本面报告时发生错误: {e}")
        # 返回标题 + 错误信息
        return f"# Market Analysis Report - {symbol} ({trade_date})\n\n查询错误: {str(e)}"


def _query_history_report(conn: Any, symbol: str, trade_date: str, trading_session: str) -> str:
    """
    从数据库查询 Market Analyst 的历史摘要
    
    Args:
        conn: 数据管理器实例
        symbol: 股票代码
        trade_date: 交易日期
        trading_session: 交易时段（'pre_open' 或 'post_close'）
    
    Returns:
        历史摘要内容
    """
    try:
        cursor = conn.cursor()
        sql = """
            SELECT report_content 
            FROM analyst_reports
            WHERE analyst_type='market' 
                AND symbol=? 
                AND trade_date <= ?
                AND trade_date >= date(?, '-7 days')
                AND report_content IS NOT NULL
                AND report_content != ''
            ORDER BY trade_date ASC
        """
        
        cursor.execute(sql, (symbol, trade_date, trade_date))
        results = cursor.fetchall()
        cursor.close()

        # 按顺序拼接所有 report_content
        history_report = ""
        if results:
            # 将所有 report_content 按顺序直接拼接
            report_contents = [row[0] for row in results if row[0]]
            history_report = "\n\n".join(report_contents)
        else:
            # 如果没有查询到结果，返回空字符串
            history_report = ""

        # 返回标题 + 拼接的 history_report
        session_desc = "开盘前" if trading_session == 'pre_open' else "收盘后"
        return f"""# Fundamentals History Summary - {symbol}

## 近期基本面趋势分析 ({session_desc})
{history_report}"""

    except Exception as e:
        # 异常处理：如果查询失败，返回错误信息
        print(f"查询历史摘要时发生错误: {e}")
        session_desc = "开盘前" if trading_session == 'pre_open' else "收盘后"
        return f"""# Fundamentals History Summary - {symbol}

## 近期基本面趋势分析 ({session_desc})

查询错误: {str(e)}"""

# # former: 
#     session_desc = "开盘前" if trading_session == 'pre_open' else "收盘后"
#     return f"""# Market History Summary - {symbol}

# ## 历史趋势分析 ({session_desc})

# **分析周期**: 过去 7 个交易日

# ### 主要趋势

# 1. **价格走势**: 整体呈现震荡上行趋势，近期突破关键阻力位
# 2. **成交量**: 近期成交量明显放大，市场关注度提升
# 3. **技术形态**: 形成上升通道，短期均线多头排列

# ### 关键支撑位和阻力位

# - **支撑位**: 14.50, 14.20
# - **阻力位**: 15.50, 16.00

# ### 风险提示

# - 需关注成交量是否能持续放大
# - 注意回调风险，建议设置止损位
# """