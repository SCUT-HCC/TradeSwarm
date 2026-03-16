"""基本面分析工具"""
import json
import re
from typing import Optional
import pandas as pd
from langchain_core.tools import tool
from datasources.data_sources.alphavantage_provider import AlphaVantageProvider
from utils.data_utils import normalize_stock_code
from utils.config_loader import load_config


def _df_to_preview(df, limit: int = 5):
    """
    将 DataFrame 转换为预览数据和元信息，限制返回条数。
    返回: (records, meta)
    """
    if df is None:
        return None, {"total_rows": 0, "columns": []}
    if hasattr(df, "empty") and df.empty:
        return None, {"total_rows": 0, "columns": list(df.columns)}
    # 截断
    preview_df = df.head(limit) if limit and hasattr(df, "head") else df
    try:
        records = preview_df.to_dict("records")
    except Exception:
        records = []
    meta = {
        "total_rows": len(df) if hasattr(df, "__len__") else 0,
        "preview_rows": len(preview_df) if hasattr(preview_df, "__len__") else 0,
        "columns": list(df.columns) if hasattr(df, "columns") else [],
    }
    return records, meta


def _get_latest_row(df):
    if df is None or getattr(df, "empty", True):
        return None
    try:
        return df.iloc[0].to_dict()
    except Exception:
        return None


def _pick_fields(row: dict, fields: list, alias: dict = None):
    if row is None:
        return None
    data = {}
    for k in fields:
        v = row.get(k)
        data[k] = v
    if alias:
        for old, new in alias.items():
            if old in row:
                data[new] = row.get(old)
    return data


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
def get_company_info(symbol: str) -> str:
    """
    获取公司基本信息（使用 Alpha Vantage API）
    
    此工具用于获取指定股票代码的公司基本信息，包括公司名称、所属行业、上市日期、股本等。
    
    Args:
        symbol: 股票代码，yfinance格式：
            - 美股：'AAPL', 'MSFT', 'GOOGL' 等
            - A股：'000001.SZ' (深圳), '600519.SS' (上海)
            示例：'AAPL' 或 '000001.SZ' 或 '600519.SS'
    
    Returns:
        JSON 格式的字符串，包含以下字段：
        - success: 是否成功
        - message: 提示信息
        - data: 公司基本信息字典，包含：
            - symbol: 股票代码
            - name: 公司名称
            - industry: 所属行业
            - list_date: 上市日期
            - 其他相关信息
        - summary: 数据摘要
    
    Examples:
        >>> get_company_info('000001')
        '{"success": true, "data": {...}, "summary": {...}}'
    """
    try:
        from datetime import datetime
        
        av_provider = _get_alphavantage_provider()
        result = av_provider.get_company_info(symbol)
        
        # 补充缺失字段，确保与 dev/zcx 格式兼容
        data = {
            "ts_code": result.get("symbol", symbol),  # 股票代码
            "name": result.get("name", ""),  # 公司名称
            "area": result.get("country", ""),  # 国家/地区（如果 API 提供）
            "industry": result.get("industry", result.get("sector", "")),  # 行业
            "market": result.get("exchange", ""),  # 交易所
            "list_date": result.get("latest_quarter", ""),  # 最新季度（作为上市日期近似值）
            "total_share": result.get("shares_outstanding", ""),  # 总股本（如果 API 提供）
            "float_share": result.get("shares_outstanding", ""),  # 流通股本（如果 API 提供）
            # 保留原有字段
            "symbol": result.get("symbol", symbol),
            "sector": result.get("sector", ""),
            "marketCap": result.get("marketCap", ""),
            "currency": result.get("currency", ""),
            "exchange": result.get("exchange", ""),
            "website": result.get("website", ""),
            "description": result.get("description", ""),
        }
        
        return json.dumps({
            "success": True,
            "message": f"成功从 Alpha Vantage 获取股票 {symbol} 的公司信息",
            "data": data,
            "summary": {
                "data_source": "alphavantage",
                "symbol": result.get("symbol", symbol),
                "update_time": datetime.now().strftime("%Y-%m-%d")  # 添加更新时间
            }
        }, ensure_ascii=False, indent=2, default=str)
    
    except Exception as e:
        return json.dumps({
            "success": False,
            "message": f"获取公司信息时发生错误: {str(e)}",
            "data": {},
            "summary": {}
        }, ensure_ascii=False, indent=2)


@tool
def get_financial_statements(
    symbol: str,
    report_type: Optional[str] = "annual",
    periods: Optional[int] = 4
) -> str:
    """
    获取三大财务报表（利润表、资产负债表、现金流量表）
    
    此工具用于获取指定股票的三大财务报表数据，支持年度和季度报告。
    使用 Alpha Vantage API 获取数据。
    
    Args:
        symbol: 股票代码，yfinance格式：
            - 美股：'AAPL', 'MSFT', 'GOOGL' 等
            - A股：'000001.SZ' (深圳), '600519.SS' (上海)
        report_type: 报告类型，可选值：
            - 'annual': 年度报告（默认）
            - 'quarter': 季度报告
        periods: 获取最近 N 期数据（默认 4 期）
    
    Returns:
        JSON 格式的字符串，包含以下字段：
        - success: 是否成功
        - message: 提示信息
        - data: 包含以下字段的字典：
            - income: 利润表数据（列表）
            - balance: 资产负债表数据（列表）
            - cashflow: 现金流量表数据（列表）
            - errors: 错误列表（如果有）
        - summary: 数据摘要
    
    Examples:
        >>> get_financial_statements('000001', report_type='annual', periods=4)
        '{"success": true, "data": {...}, "summary": {...}}'
    """
    try:
        # AK的财务报表接口有点问题
        if report_type not in ["annual", "quarter"]:
            report_type = "annual"

        av_provider = _get_alphavantage_provider()
        statements = av_provider.get_financial_statements(symbol, statement_type="all")
        
        income_df = statements.get('income', pd.DataFrame())
        balance_df = statements.get('balance', pd.DataFrame())
        cashflow_df = statements.get('cashflow', pd.DataFrame())

        # 核心字段提取（最新一条）
        income_row = _get_latest_row(income_df)
        balance_row = _get_latest_row(balance_df)
        cashflow_row = _get_latest_row(cashflow_df)

        # Alpha Vantage 字段名映射
        income_core = _pick_fields(
            income_row,
            [
                "fiscalDateEnding",
                "reportedCurrency",
                "totalRevenue",
                "grossProfit",
                "operatingIncome",
                "netIncome",
                "basicEPS",
            ],
        )

        balance_core = _pick_fields(
            balance_row,
            [
                "fiscalDateEnding",
                "reportedCurrency",
                "totalAssets",
                "totalLiabilities",
                "totalShareholderEquity",
                "cashAndCashEquivalentsAtCarryingValue",
                "shortTermInvestments",
            ],
        )

        cashflow_core = _pick_fields(
            cashflow_row,
            [
                "fiscalDateEnding",
                "reportedCurrency",
                "operatingCashflow",
                "cashflowFromInvestment",
                "cashflowFromFinancing",
                "changeInCashAndCashEquivalents",
            ],
        )

        income_preview, income_meta = _df_to_preview(income_df, limit=periods or 5)
        balance_preview, balance_meta = _df_to_preview(balance_df, limit=periods or 5)
        cashflow_preview, cashflow_meta = _df_to_preview(cashflow_df, limit=periods or 5)

        result = {
            "symbol": symbol,
            "report_type": report_type,
            "income": income_preview,
            "balance": balance_preview,
            "cashflow": cashflow_preview,
            "meta": {
                "income": income_meta,
                "balance": balance_meta,
                "cashflow": cashflow_meta,
            },
            "core": {
                "income": income_core,
                "balance": balance_core,
                "cashflow": cashflow_core,
            },
            "errors": []
        }
        
        if result["income"] is None:
            result["errors"].append("利润表数据为空")
        if result["balance"] is None:
            result["errors"].append("资产负债表数据为空")
        if result["cashflow"] is None:
            result["errors"].append("现金流量表数据为空")
        
        return json.dumps({
            "success": True,
            "message": f"成功从 Alpha Vantage 获取股票 {symbol} 的财务报表",
            "data": result,
            "summary": {
                "data_source": "alphavantage",
                "symbol": symbol,
                "report_type": report_type,
                "periods": periods
            }
        }, ensure_ascii=False, indent=2, default=str)

    except Exception as e:
        return json.dumps({
            "success": False,
            "message": f"获取财务报表时发生错误: {str(e)}",
            "data": {},
            "summary": {}
        }, ensure_ascii=False, indent=2)


@tool
def get_financial_indicators(
    symbol: str,
    report_type: Optional[str] = "annual",
    periods: Optional[int] = 4
) -> str:
    """
    获取财务指标（ROE、ROA、毛利率、净利率等）
    
    此工具用于获取指定股票的财务分析指标，包括盈利能力、成长能力、偿债能力等指标。
    使用 Alpha Vantage API 获取数据。
    
    Args:
        symbol: 股票代码，yfinance格式：
            - 美股：'AAPL', 'MSFT', 'GOOGL' 等
            - A股：'000001.SZ' (深圳), '600519.SS' (上海)
        report_type: 报告类型，可选值：
            - 'annual': 年度报告（默认）
            - 'quarter': 季度报告
        periods: 获取最近 N 期数据（默认 4 期）
    
    Returns:
        JSON 格式的字符串，包含以下字段：
        - success: 是否成功
        - message: 提示信息
        - data: 财务指标数据（列表）
        - summary: 数据摘要
    
    Examples:
        >>> get_financial_indicators('000001', report_type='annual', periods=4)
        '{"success": true, "data": {...}, "summary": {...}}'
    """
    try:
        if report_type not in ["annual", "quarter"]:
            report_type = "annual"

        av_provider = _get_alphavantage_provider()
        df = av_provider.get_financial_indicators(symbol)
        
        preview, meta = _df_to_preview(df, limit=periods or 5)

        latest = _get_latest_row(df)
        # Alpha Vantage 字段名
        core = _pick_fields(
            latest,
            [
                "symbol",
                "pe_ratio",
                "peg_ratio",
                "eps",
                "dividend_yield",
                "roe",
                "roa",
                "profit_margin",
            ],
        )

        if preview is None:
            return json.dumps({
                "success": False,
                "message": "Alpha Vantage 财务指标数据为空",
                "data": {},
                "summary": {"data_source": "alphavantage", "symbol": symbol}
            }, ensure_ascii=False, indent=2)

        return json.dumps({
            "success": True,
            "message": f"成功从 Alpha Vantage 获取股票 {symbol} 的财务指标",
            "data": {
                "symbol": symbol,
                "report_type": report_type,
                "data": preview,
                "meta": meta,
                "core": core
            },
            "summary": {
                "data_source": "alphavantage",
                "symbol": symbol,
                "report_type": report_type,
                "periods": periods
            }
        }, ensure_ascii=False, indent=2, default=str)
    
    except Exception as e:
        return json.dumps({
            "success": False,
            "message": f"获取财务指标时发生错误: {str(e)}",
            "data": {},
            "summary": {}
        }, ensure_ascii=False, indent=2)


@tool
def get_valuation_indicators(
    symbol: str,
    include_market_comparison: Optional[bool] = True
) -> str:
    """
    获取估值指标（PE、PB、PS、股息率等）
    
    此工具用于获取指定股票的估值指标，包括市盈率（PE）、市净率（PB）、市销率（PS）、股息率等。
    可选包含市场/行业对比数据。
    使用 Alpha Vantage API 获取数据。
    
    Args:
        symbol: 股票代码，yfinance格式：
            - 美股：'AAPL', 'MSFT', 'GOOGL' 等
            - A股：'000001.SZ' (深圳), '600519.SS' (上海)
        include_market_comparison: 是否包含市场/行业对比（默认 True）
    
    Returns:
        JSON 格式的字符串，包含以下字段：
        - success: 是否成功
        - message: 提示信息
        - data: 估值指标数据，包含：
            - pe_pb: PE/PB 数据
            - dividend: 分红数据
            - market_comparison: 市场对比数据（如果请求）
        - summary: 数据摘要
    
    Examples:
        >>> get_valuation_indicators('000001', include_market_comparison=True)
        '{"success": true, "data": {...}, "summary": {...}}'
    """
    try:
        av_provider = _get_alphavantage_provider()
        df = av_provider.get_valuation_metrics(symbol)
        
        preview, meta = _df_to_preview(df, limit=1)
        latest = _get_latest_row(df)
        
        # Alpha Vantage 字段名
        core = _pick_fields(
            latest,
            [
                "symbol",
                "market_cap",
                "pe_ratio",
                "peg_ratio",
                "price_to_book",
                "price_to_sales",
                "ev_to_ebitda",
                "dividend_yield",
            ],
        )

        if preview is None:
            return json.dumps({
                "success": False,
                "message": "Alpha Vantage 估值指标数据为空",
                "data": {},
                "summary": {"data_source": "alphavantage", "symbol": symbol}
            }, ensure_ascii=False, indent=2)
        
        return json.dumps({
            "success": True,
            "message": f"成功从 Alpha Vantage 获取股票 {symbol} 的估值指标",
            "data": {
                "symbol": symbol,
                "pe_pb": preview,
                "dividend": None,
                "market_comparison": None,
                "meta": meta,
                "core": core,
                "errors": []
            },
            "summary": {
                "data_source": "alphavantage",
                "symbol": symbol,
                "include_market_comparison": include_market_comparison
            }
        }, ensure_ascii=False, indent=2, default=str)
    
    except Exception as e:
        return json.dumps({
            "success": False,
            "message": f"获取估值指标时发生错误: {str(e)}",
            "data": {},
            "summary": {}
        }, ensure_ascii=False, indent=2)


@tool
def get_earnings_data(
    symbol: str,
    limit: Optional[int] = 10
) -> str:
    """
    获取业绩数据（年度和季度）
    
    此工具用于获取指定股票的年度和季度业绩数据。
    使用 Alpha Vantage EARNINGS 接口获取数据。
    
    Args:
        symbol: 股票代码，yfinance格式：
            - 美股：'AAPL', 'MSFT', 'GOOGL' 等
            - A股：'000001.SZ' (深圳), '600519.SS' (上海)
        limit: 返回最近 N 条记录（默认 10 条）
    
    Returns:
        JSON 格式的字符串，包含以下字段：
        - success: 是否成功
        - message: 提示信息
        - data: 业绩数据，包含：
            - forecast: 业绩预告数据（列表）
            - express: 业绩快报数据（列表）
            - errors: 错误列表（如果有）
        - summary: 数据摘要
    
    Examples:
        >>> get_earnings_data('000001', limit=10)
        '{"success": true, "data": {...}, "summary": {...}}'
    """
    try:
        av_provider = _get_alphavantage_provider()
        result = av_provider.get_earnings_data(symbol, limit)
        
        annual_earnings = result.get("annualEarnings", [])
        quarterly_earnings = result.get("quarterlyEarnings", [])
        
        # 核心字段压缩
        def _trim_earnings(items, is_annual: bool):
            if not items:
                return None
            trimmed = []
            for it in items[:limit]:
                trimmed.append({
                    "fiscalDateEnding": it.get("fiscalDateEnding"),
                    "reportedEPS": it.get("reportedEPS"),
                    "estimatedEPS": it.get("estimatedEPS"),
                    "surprise": it.get("surprise"),
                    "surprisePercentage": it.get("surprisePercentage"),
                })
            return trimmed

        annual_core = _trim_earnings(annual_earnings, is_annual=True)
        quarterly_core = _trim_earnings(quarterly_earnings, is_annual=False)

        return json.dumps({
            "success": True,
            "message": f"成功从 Alpha Vantage 获取股票 {symbol} 的业绩数据",
            "data": {
                "symbol": symbol,
                "annualEarnings": annual_earnings,
                "quarterlyEarnings": quarterly_earnings,
                "core": {
                    "annualEarnings": annual_core,
                    "quarterlyEarnings": quarterly_core
                },
                "errors": []
            },
            "summary": {
                "data_source": "alphavantage",
                "symbol": symbol,
                "limit": limit,
                "annual_count": len(annual_earnings) if annual_earnings else 0,
                "quarterly_count": len(quarterly_earnings) if quarterly_earnings else 0
            }
        }, ensure_ascii=False, indent=2, default=str)
    
    except Exception as e:
        return json.dumps({
            "success": False,
            "message": f"获取业绩数据时发生错误: {str(e)}",
            "data": {},
            "summary": {}
        }, ensure_ascii=False, indent=2)

