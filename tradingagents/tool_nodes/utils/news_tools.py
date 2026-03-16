"""新闻工具"""
import json
import re
from typing import Optional, Dict, List
from datetime import datetime, timedelta
import pandas as pd
from langchain_core.tools import tool
from datasources.data_sources.alphavantage_provider import AlphaVantageProvider
from utils.data_utils import normalize_stock_code, format_date
from utils.config_loader import load_config


def _format_macro_news_section(df: pd.DataFrame) -> str:
    """格式化宏观新闻部分"""
    markdown = f"## 📰 宏观新闻 ({len(df)}条)\n\n"
    
    # 处理列名
    time_col = None
    title_col = None
    url_col = None
    content_col = None
    
    for col in df.columns:
        if '时间' in str(col) or 'time' in str(col).lower() or '日期' in str(col):
            time_col = col
        if '标题' in str(col) or 'title' in str(col).lower():
            title_col = col
        if '链接' in str(col) or 'url' in str(col).lower():
            url_col = col
        if '内容' in str(col) or 'content' in str(col).lower():
            content_col = col
    
    for idx, (_, row) in enumerate(df.iterrows(), 1):
        markdown += f"### {idx}. "
        
        if title_col and title_col in row:
            title = str(row[title_col]).strip()
            if url_col and url_col in row:
                url = str(row[url_col]).strip()
                if url:
                    markdown += f"[{title}]({url})\n\n"
                else:
                    markdown += f"{title}\n\n"
            else:
                markdown += f"{title}\n\n"
        else:
            markdown += f"（无标题）\n\n"
        
        if time_col and time_col in row:
            markdown += f"- **时间**: {str(row[time_col])}\n"
        
        if content_col and content_col in row:
            content = str(row[content_col]).strip()
            if content:
                summary = content[:150] + "..." if len(content) > 150 else content
                markdown += f"- **摘要**: {summary}\n"
        
        markdown += "\n"
    
    return markdown


def _format_money_flow_section(money_flow: Dict) -> str:
    """格式化北向资金部分"""
    markdown = f"## 💰 北向资金流向\n\n"
    markdown += f"- **状态**: {money_flow.get('flow_status', '未知')}\n"
    markdown += f"- **金额**: {money_flow.get('value', 'N/A')}\n"
    markdown += f"- **日期**: {money_flow.get('date', 'N/A')}\n"
    markdown += f"- **数据来源**: {money_flow.get('source', 'N/A')}\n"
    return markdown


def _format_indices_section(indices: List[Dict]) -> str:
    """格式化核心指数部分"""
    markdown = f"## 📊 核心指数表现\n\n"
    markdown += f"| 指数 | 代码 | 最新价 | 涨跌幅 |\n"
    markdown += f"|------|------|--------|--------|\n"
    
    for idx in indices:
        asset = idx.get('asset', 'N/A')
        code = idx.get('code', 'N/A')
        price = idx.get('price', 0)
        change = idx.get('change', 'N/A')
        
        # 格式化价格（大数字用千分位）
        if isinstance(price, (int, float)) and price >= 1000:
            price_str = f"{price:,.2f}"
        elif isinstance(price, (int, float)):
            price_str = f"{price:.2f}"
        else:
            price_str = str(price)
        
        markdown += f"| {asset} | {code} | {price_str} | {change} |\n"
    
    return markdown


def _format_currency_section(currency: Dict) -> str:
    """格式化汇率部分"""
    markdown = f"## 💱 汇率信息\n\n"
    markdown += f"- **货币对**: {currency.get('currency_pair', 'N/A')}\n"
    
    price = currency.get('price')
    if price is not None:
        markdown += f"- **汇率**: {price:.4f}\n"
    else:
        markdown += f"- **汇率**: N/A\n"
    
    markdown += f"- **涨跌幅**: {currency.get('change', 'N/A')}\n"
    markdown += f"- **日期**: {currency.get('date', 'N/A')}\n"
    
    return markdown


# 全局 Provider 实例（懒加载）
_alphavantage_provider: Optional[AlphaVantageProvider] = None


def _get_alphavantage_provider() -> AlphaVantageProvider:
    """获取 Alpha Vantage Provider 实例（单例模式）"""
    global _alphavantage_provider
    if _alphavantage_provider is None:
        config = load_config()
        _alphavantage_provider = AlphaVantageProvider(config)
    return _alphavantage_provider


@tool
def get_news(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days: Optional[int] = 7,
    limit: Optional[int] = 10
) -> str:
    """
    获取股票相关的新闻信息（使用 Alpha Vantage API）
    
    此工具用于获取指定股票在指定日期范围内的新闻信息。
    包括公司公告、新闻资讯等可能影响股价的信息。
    使用 Alpha Vantage API 获取数据。
    
    Args:
        symbol: 股票代码，yfinance格式：
            - 美股：'AAPL', 'MSFT', 'GOOGL' 等
            - A股：'000001.SZ' (深圳), '600519.SS' (上海)
            示例：'AAPL' 或 '000001.SZ' 或 '600519.SS'
        start_date: 可选，开始日期，格式为 'YYYYMMDD' 或 'YYYY-MM-DD'
            如果不提供，默认使用最近 days 天的数据
            示例：'20250101' 或 '2025-01-01'
        end_date: 可选，结束日期，格式为 'YYYYMMDD' 或 'YYYY-MM-DD'
            如果不提供，默认使用当前日期
            示例：'20251231' 或 '2025-12-31'
        days: 可选，如果未提供日期范围，获取最近 days 天的数据（默认 7 天）
            示例：7（获取最近7天的数据）
        limit: 可选，返回的新闻数量限制（默认 10 条）
            示例：10
    
    Returns:
        JSON 格式的字符串，包含以下字段：
        - success: 是否成功
        - message: 提示信息
        - data: 新闻/公告列表，每个元素包含：
            - publish_time: 发布时间
            - title: 新闻标题
            - url: 文章链接
            - content: 新闻内容（如果有）
            - source: 文章来源（如果有）
        - summary: 数据摘要（包含数据条数、日期范围等）
    
    Examples:
        >>> get_news('000001', days=7, limit=10)
        '{"success": true, "data": [...], "summary": {...}}'
        
        >>> get_news('000001', start_date='20250101', end_date='20250131', limit=20)
        '{"success": true, "data": [...], "summary": {...}}'
    """
    try:
        # 处理日期参数（用于后续日期筛选）
        if not start_date or not end_date:
            end_date_obj = datetime.now()
            start_date_obj = end_date_obj - timedelta(days=days)
            start_date = start_date_obj.strftime('%Y%m%d')
            end_date = end_date_obj.strftime('%Y%m%d')
        
        av_provider = _get_alphavantage_provider()
        # 使用 Alpha Vantage NEWS_SENTIMENT API 获取新闻（支持历史日期过滤）
        df = av_provider.get_news(symbol, limit=limit or 10, start_date=start_date, end_date=end_date)
        
        if df is not None and not df.empty:
            # 转换为字典列表
            data_list = df.to_dict('records')
            
            summary = {
                "total_records": len(data_list),
                "data_source": "alphavantage",
                "date_range": {
                    "start": start_date,
                    "end": end_date
                },
                "note": "数据以 JSON 列表格式返回，便于程序处理和 LLM 理解。"
            }
            
            result = {
                "success": True,
                "message": f"成功从 Alpha Vantage 获取股票 {symbol} 的新闻",
                "format": "json",  # 添加格式说明
                "data": data_list,
                "summary": summary
            }
            
            return json.dumps(result, ensure_ascii=False, indent=2, default=str)
        else:
            return json.dumps({
                "success": False,
                "message": f"Alpha Vantage 返回空数据，可能该股票在指定日期范围内暂无新闻",
                "data": [],
                "summary": {
                    "total_records": 0,
                    "data_source": "alphavantage",
                    "date_range": {"start": start_date, "end": end_date},
                    "note": "已使用 time_from 和 time_to 参数请求指定日期范围的新闻"
                }
            }, ensure_ascii=False, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "message": f"获取新闻数据时发生错误: {str(e)}",
            "data": [],
            "summary": {}
        }, ensure_ascii=False, indent=2)


@tool
def get_global_news(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days: Optional[int] = 7,
    limit: Optional[int] = 10
) -> str:
    """
    获取宏观经济新闻和全球市场新闻
    
    此工具用于获取指定日期范围内的宏观经济新闻、政策新闻、市场信号等
    可能影响 A 股市场的全球性新闻信息。使用 Alpha Vantage API 获取数据。
    
    Args:
        start_date: 可选，开始日期，格式为 'YYYYMMDD' 或 'YYYY-MM-DD'
            如果不提供，默认使用最近 days 天的数据
            示例：'20250101' 或 '2025-01-01'
        end_date: 可选，结束日期，格式为 'YYYYMMDD' 或 'YYYY-MM-DD'
            如果不提供，默认使用当前日期
            示例：'20251231' 或 '2025-12-31'
        days: 可选，如果未提供日期范围，获取最近 days 天的数据（默认 7 天）
            示例：7（获取最近7天的数据）
        limit: 可选，返回的新闻数量限制（默认 10 条）
            示例：10
    
    Returns:
        JSON 格式的字符串，包含以下字段：
        - success: 是否成功
        - message: 提示信息
        - data: 新闻列表（如果数据源可用）
        - summary: 数据摘要
    
    Examples:
        >>> get_global_news(days=7, limit=10)
        '{"success": true, "data": [...], "summary": {...}}'
    """
    try:
        # 处理日期参数
        if not start_date or not end_date:
            end_date_obj = datetime.now()
            start_date_obj = end_date_obj - timedelta(days=days)
            start_date = start_date_obj.strftime('%Y%m%d')
            end_date = end_date_obj.strftime('%Y%m%d')
        
        # 使用 Alpha Vantage 获取宏观新闻（支持历史日期过滤）
        av_provider = _get_alphavantage_provider()
        df = av_provider.get_macro_news(limit=limit or 10, start_date=start_date, end_date=end_date)
        
        if df is not None and not df.empty:
            # 转换为 Markdown 格式
            markdown = f"# 宏观市场全景简报\n\n"
            markdown += f"**更新时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            markdown += "---\n\n"
            
            # 格式化新闻数据
            markdown += f"## 📰 宏观新闻 ({len(df)}条)\n\n"
            for idx, (_, row) in enumerate(df.iterrows(), 1):
                title = row.get('title', '无标题')
                url = row.get('url', '')
                time_pub = row.get('time_published', '')
                summary = row.get('summary', '')
                source = row.get('source', '')
                sentiment = row.get('overall_sentiment_score', 0)
                
                markdown += f"### {idx}. "
                if url:
                    markdown += f"[{title}]({url})\n\n"
                else:
                    markdown += f"{title}\n\n"
            
                if time_pub:
                    markdown += f"- **时间**: {time_pub}\n"
                if source:
                    markdown += f"- **来源**: {source}\n"
                if sentiment:
                    markdown += f"- **情绪得分**: {sentiment}\n"
                if summary:
                    summary_short = summary[:150] + "..." if len(summary) > 150 else summary
                    markdown += f"- **摘要**: {summary_short}\n"
                markdown += "\n"
            
            markdown += f"*数据来源: Alpha Vantage*\n"
            
            result = {
                "success": True,
                "message": f"成功从 Alpha Vantage 获取宏观新闻",
                "format": "markdown",
                "content": markdown,
                "summary": {
                    "data_source": "alphavantage",
                    "date_range": {
                        "start": start_date,
                        "end": end_date
                    },
                    "total_records": len(df),
                    "note": "数据以 Markdown 格式返回，便于 LLM 理解和处理"
                }
            }
            
            return json.dumps(result, ensure_ascii=False, indent=2, default=str)
        else:
            return json.dumps({
                "success": False,
                "message": f"Alpha Vantage 返回空数据，可能暂无宏观新闻",
                "format": "markdown",
                "content": f"# 宏观市场全景简报\n\n## ⚠️ 暂无数据\n\n当前时间段内暂无宏观新闻数据。",
                "summary": {
                    "data_source": "alphavantage",
                    "date_range": {"start": start_date, "end": end_date},
                    "total_records": 0
                }
            }, ensure_ascii=False, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "message": f"获取宏观经济新闻时发生错误: {str(e)}",
            "data": [],
            "summary": {}
        }, ensure_ascii=False, indent=2)

