"""
Fundamentals Summary Node

从数据库读取 Fundamentals Analyst 的 today_report 和 history_report，填充到 AgentState。
"""

from typing import Callable, Any, Dict
from tradingagents.agents.utils.agentstate.agent_states import AgentState, AnalystMemorySummary


def create_fundamentals_summary_node(conn: Any) -> Callable[[AgentState], Dict[str, Any]]:
    """
    创建 Fundamentals Summary 节点函数
    
    该函数返回一个符合 LangGraph 节点规范的函数，用于从数据库读取 Fundamentals Analyst 的
    报告摘要并填充到 AgentState。
    
    Args:
        conn: 数据管理器实例（用于查询数据库）
        
    Returns:
        fundamentals_summary_node: 一个接受 AgentState 并返回更新字典的函数
        
    节点函数返回值:
        dict 包含以下键:
            - fundamentals_analyst_summary: AnalystMemorySummary 对象，包含 today_report 和 history_report
    
    实现细节:
        - 从数据库查询 Fundamentals Analyst 的 today_report（周级别更新）
        - 从数据库查询 history_report（根据 trading_session 决定）
        - Fundamentals Analyst 是周级别更新（slow）
    """
    
    def fundamentals_summary_node(state: AgentState) -> Dict[str, Any]:
        """
        Fundamentals Summary 节点的执行函数
        
        Args:
            state: 当前的 AgentState
            
        Returns:
            包含 fundamentals_analyst_summary 的更新字典
        """
        # 第一阶段：提取参数
        symbol = state["company_of_interest"]
        trade_date = state["trade_date"]
        trading_session = state.get("trading_session", "post_close")
        
        # 第二阶段：查询数据库
        today_report = _query_today_report(conn, symbol, trade_date)
        history_report = _query_history_report(conn, symbol, trade_date, trading_session)
        
        # 第三阶段：构建 AnalystMemorySummary
        summary: AnalystMemorySummary = {
            "today_report": today_report,
            "history_report": history_report
        }
        
        return {
            "fundamentals_analyst_summary": summary
        }
    
    return fundamentals_summary_node


def _query_today_report(conn: Any, symbol: str, trade_date: str) -> str:
    """
    从数据库查询 Fundamentals Analyst 的今日报告
    
    注意：Fundamentals 按周更新，today_report 实际是本周的报告。
    
    Args:
        conn: 数据管理器实例
        symbol: 股票代码（如 '000001'）
        trade_date: 交易日期（如 '2024-01-15'）
    
    Returns:
        本周基本面分析报告
    """
    try:
        # 使用 conn 的 cursor 获取游标
        cursor = conn.cursor()
        
        # 执行查询：Fundamentals 按周更新
        sql = """
            SELECT report_content 
            FROM analyst_reports
            WHERE analyst_type='fundamentals' 
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
            return f"# Fundamentals Analysis Report - {symbol} ({trade_date})\n\n{result[0]}"
        else:
            # 如果没有查询到结果，返回标题 + 提示信息
            return f"# Fundamentals Analysis Report - {symbol} ({trade_date})\n\n未找到基本面分析报告数据。"
            
    except Exception as e:
        # 异常处理：如果查询失败，返回错误信息（或可以记录日志）
        print(f"查询基本面报告时发生错误: {e}")
        # 返回标题 + 错误信息
        return f"# Fundamentals Analysis Report - {symbol} ({trade_date})\n\n查询错误: {str(e)}"

def _query_history_report(conn: Any, symbol: str, trade_date: str, trading_session: str) -> str:
    """
    从数据库查询 Fundamentals Analyst 的历史摘要
    
    查询过去 7 天的所有 report_content，按日期顺序拼接。
    
    Args:
        conn: 数据管理器实例
        symbol: 股票代码
        trade_date: 交易日期
        trading_session: 交易时段（'pre_open' 或 'post_close'）
    
    Returns:
        历史基本面摘要（标题 + 过去 7 天的 report_content 按顺序拼接）
    """

    try:
        cursor = conn.cursor()
        sql = """
            SELECT report_content 
            FROM analyst_reports
            WHERE analyst_type='fundamentals' 
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