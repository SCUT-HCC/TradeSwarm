"""
Summary 节点数据库查询辅助函数

提供统一的数据库查询接口，支持 MemoryDBHelper 和 sqlite3.Connection。
"""

from typing import Any, List, Dict


def query_today_report(
    conn: Any,
    analyst_type: str,
    symbol: str,
    trade_date: str,
    report_title: str,
) -> str:
    """
    从数据库查询 Analyst 的今日报告
    
    Args:
        conn: 数据管理器实例（MemoryDBHelper 或 sqlite3.Connection）
        analyst_type: 分析师类型（'market', 'news', 'sentiment', 'fundamentals'）
        symbol: 股票代码
        trade_date: 交易日期
        report_title: 报告标题（如 "Market Analysis Report"）
        
    Returns:
        今日报告内容（包含标题）
    """
    try:
        # 如果 conn 是 MemoryDBHelper，使用其方法
        if hasattr(conn, 'query_today_report'):
            result = conn.query_today_report(analyst_type, symbol, trade_date)
            if result:
                return f"# {report_title} - {symbol} ({trade_date})\n\n{result}"
            else:
                return f"# {report_title} - {symbol} ({trade_date})\n\n未找到{analyst_type}分析报告数据。"
        
        # 否则，假设是 sqlite3.Connection
        cursor = conn.cursor()
        
        sql = """
            SELECT report_content 
            FROM analyst_reports
            WHERE analyst_type=? 
                AND symbol=? 
                AND trade_date=?
            ORDER BY created_at DESC 
            LIMIT 1
        """
        
        cursor.execute(sql, (analyst_type, symbol, trade_date))
        result = cursor.fetchone()
        cursor.close()
        
        if result and result[0]:
            return f"# {report_title} - {symbol} ({trade_date})\n\n{result[0]}"
        else:
            return f"# {report_title} - {symbol} ({trade_date})\n\n未找到{analyst_type}分析报告数据。"
            
    except Exception as e:
        print(f"查询{analyst_type}报告时发生错误: {e}")
        return f"# {report_title} - {symbol} ({trade_date})\n\n查询错误: {str(e)}"


def query_history_report(
    conn: Any,
    analyst_type: str,
    symbol: str,
    trade_date: str,
    trading_session: str,
    report_title: str,
) -> str:
    """
    从数据库查询 Analyst 的历史摘要
    
    Args:
        conn: 数据管理器实例（MemoryDBHelper 或 sqlite3.Connection）
        analyst_type: 分析师类型
        symbol: 股票代码
        trade_date: 交易日期
        trading_session: 交易时段（'pre_open' 或 'post_close'）
        report_title: 报告标题（如 "Market History Summary"）
        
    Returns:
        历史摘要内容（包含标题）
    """
    try:
        # 如果 conn 是 MemoryDBHelper，优先使用结构化 summary（7 日窗口）
        if hasattr(conn, "query_summary"):
            summary = conn.query_summary(analyst_type, symbol, trade_date)
            if summary and summary.get("summary_content"):
                session_desc = "开盘前" if trading_session == "pre_open" else "收盘后"
                # 这里直接返回结构化 summary 内容，由上层 Prompt 再决定如何使用
                return f"""# {report_title} - {symbol}

## 近期趋势分析 ({session_desc})
{summary['summary_content']}"""

        # 否则（或没有 summary），回退到旧逻辑：查询过去 7 天的原始报告并拼接
        if hasattr(conn, "query_history_reports"):
            reports = conn.query_history_reports(
                analyst_type, symbol, trade_date, lookback_days=7
            )
            if reports:
                report_contents = [
                    r["report_content"] for r in reports if r.get("report_content")
                ]
                history_report = "\n\n".join(report_contents)
            else:
                history_report = ""

            session_desc = "开盘前" if trading_session == "pre_open" else "收盘后"
            return f"""# {report_title} - {symbol}

## 近期趋势分析 ({session_desc})
{history_report}"""

        # 否则，假设是 sqlite3.Connection，直接从 analyst_reports 拼接
        cursor = conn.cursor()
        sql = """
            SELECT report_content 
            FROM analyst_reports
            WHERE analyst_type=? 
                AND symbol=? 
                AND trade_date <= ?
                AND trade_date >= date(?, '-7 days')
                AND report_content IS NOT NULL
                AND report_content != ''
            ORDER BY trade_date ASC
        """

        cursor.execute(sql, (analyst_type, symbol, trade_date, trade_date))
        results = cursor.fetchall()
        cursor.close()

        history_report = ""
        if results:
            report_contents = [row[0] for row in results if row[0]]
            history_report = "\n\n".join(report_contents)

        session_desc = "开盘前" if trading_session == "pre_open" else "收盘后"
        return f"""# {report_title} - {symbol}

## 近期趋势分析 ({session_desc})
{history_report}"""

    except Exception as e:
        print(f"查询{analyst_type}历史摘要时发生错误: {e}")
        session_desc = "开盘前" if trading_session == "pre_open" else "收盘后"
        return f"""# {report_title} - {symbol}

## 近期趋势分析 ({session_desc})

查询错误: {str(e)}"""

