"""市场数据工具"""
import json
from typing import Optional
from langchain_core.tools import tool
from datasources.data_sources.yfinance_provider import YFinanceProvider
from utils.config_loader import load_config


# 全局 YFinance Provider 实例（懒加载）
_provider: Optional[YFinanceProvider] = None


def _get_provider() -> YFinanceProvider:
    """获取 YFinance Provider 实例（单例模式）"""
    global _provider
    if _provider is None:
        config = load_config()
        _provider = YFinanceProvider(config)
    return _provider


@tool
def get_stock_data(
    symbol: str,
    start_date: str,
    end_date: str
) -> str:
    """
    获取股票的日线行情数据（使用 yfinance，主要支持美股）
    
    此工具用于获取指定股票在指定日期范围内的日线行情数据，包括开盘价、收盘价、最高价、最低价、
    成交量等关键市场数据。
    
    Args:
        symbol: 股票代码，yfinance格式：
            - 美股：'AAPL', 'MSFT', 'GOOGL' 等
            - A股：'000001.SZ' (深圳), '600519.SS' (上海)
            示例：'AAPL' 或 '000001.SZ' 或 '600519.SS'
        start_date: 开始日期，格式为 'YYYYMMDD' 或 'YYYY-MM-DD'
            示例：'20250101' 或 '2025-01-01'
        end_date: 结束日期，格式为 'YYYYMMDD' 或 'YYYY-MM-DD'
            示例：'20251231' 或 '2025-12-31'
    
    Returns:
        JSON 格式的字符串，包含以下字段：
        - success: 是否成功
        - message: 提示信息
        - data: 股票数据列表，每个元素包含：
            - ts_code: 股票代码
            - Date: 交易日期
            - Open: 开盘价
            - High: 最高价
            - Low: 最低价
            - Close: 收盘价
            - Volume: 成交量
            - pre_close: 前收盘价（前一天的收盘价）
            - change: 涨跌额（当前收盘价 - 前收盘价）
            - pct_chg: 涨跌幅（百分比）
            - amount: 成交额（收盘价 × 成交量）
        - summary: 数据摘要（包含数据条数、日期范围、最新价格和涨跌幅等）
    
    Examples:
        >>> get_stock_data('AAPL', '20250101', '20250131')
        '{"success": true, "data": [...], "summary": {...}}'
    """
    try:
        provider = _get_provider()
        df = provider.get_daily(symbol, start_date, end_date)
        
        if df.empty:
            return json.dumps({
                "success": False,
                "message": f"未找到股票 {symbol} 在 {start_date} 至 {end_date} 期间的数据",
                "data": [],
                "summary": {}
            }, ensure_ascii=False, indent=2)
        
        # 重置索引，将日期作为列
        df_reset = df.reset_index()
        df_reset['Date'] = df_reset['Date'].dt.strftime('%Y-%m-%d')
        
        # 转换为字典列表并添加计算字段
        data_list = []
        prev_close = None
        
        for idx, row in df_reset.iterrows():
            record = {
                "ts_code": symbol,  # 股票代码标识
                "Date": row.get('Date'),
                "Open": row.get('Open'),
                "High": row.get('High'),
                "Low": row.get('Low'),
                "Close": row.get('Close'),
                "Volume": row.get('Volume'),
            }
            
            # 计算前收盘价、涨跌额、涨跌幅
            close_value = float(row.get('Close', 0)) if row.get('Close') is not None else None
            record["pre_close"] = prev_close if prev_close is not None else None
            
            if close_value is not None and prev_close is not None and prev_close != 0:
                change_value = close_value - prev_close
                pct_chg_value = (change_value / prev_close) * 100
                record["change"] = change_value
                record["pct_chg"] = pct_chg_value
            else:
                record["change"] = None
                record["pct_chg"] = None
            
            # 计算成交额（收盘价 × 成交量）
            volume_value = float(row.get('Volume', 0)) if row.get('Volume') is not None else None
            if close_value is not None and volume_value is not None:
                record["amount"] = close_value * volume_value
            else:
                record["amount"] = None
            
            data_list.append(record)
            prev_close = close_value
        
        # 计算摘要信息
        if data_list:
            latest = data_list[-1]
            first = data_list[0]
            summary = {
                "total_records": len(data_list),
                "date_range": {
                    "start": first.get('Date'),
                    "end": latest.get('Date')
                },
                "latest_price": {
                    "close": float(latest.get('Close', 0)) if latest.get('Close') else None,
                    "pct_chg": latest.get('pct_chg'),  # 添加涨跌幅
                }
            }
        else:
            summary = {
                "total_records": 0,
                "date_range": {"start": None, "end": None},
                "latest_price": None
            }
        
        # 返回 JSON 字符串
        result = {
            "success": True,
            "message": f"成功获取 {len(data_list)} 条数据",
            "data": data_list,
            "summary": summary
        }
        
        return json.dumps(result, ensure_ascii=False, indent=2, default=str)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "message": f"获取股票数据时发生错误: {str(e)}",
            "data": [],
            "summary": {}
        }, ensure_ascii=False, indent=2)

