#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据适配器

封装数据加载功能，提供统一的数据接口。
"""

import pandas as pd
from typing import Optional

from .data.loader import load_stock_data


class DataAdapter:
    """数据适配器"""
    
    def __init__(self, use_cache: bool = True):
        self.use_cache = use_cache
    
    def load_stock_data_until(self, symbol: str, date: str, start_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """加载截止到指定日期的股票数据"""
        try:
            if start_date is None:
                from datetime import datetime, timedelta
                date_obj = datetime.strptime(date, "%Y-%m-%d")
                start_date_obj = date_obj - timedelta(days=365)
                start_date = start_date_obj.strftime("%Y-%m-%d")
            
            df = load_stock_data(symbol, start_date, date, use_cache=self.use_cache)
            
            if df is None or len(df) == 0:
                return None
            
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            
            df = df[df.index <= pd.to_datetime(date)]
            return df
        except Exception as e:
            print(f"[DataAdapter] 加载 {symbol} 数据失败: {e}")
            return None
    
    def get_price(self, symbol: str, date: str, price_type: str = "open") -> Optional[float]:
        """获取指定日期的价格"""
        try:
            df = self.load_stock_data_until(symbol, date)
            if df is None or len(df) == 0:
                return None
            
            date_obj = pd.to_datetime(date)
            if date_obj not in df.index:
                df = df[df.index <= date_obj]
                if len(df) == 0:
                    return None
                date_obj = df.index[-1]
            
            if price_type == "open":
                return float(df.loc[date_obj, "Open"])
            elif price_type == "close":
                return float(df.loc[date_obj, "Close"])
            else:
                raise ValueError(f"不支持的价格类型: {price_type}")
        except Exception as e:
            print(f"[DataAdapter] 获取 {symbol} 在 {date} 的 {price_type} 价格失败: {e}")
            return None
    
    def get_next_trading_day(self, current_date: str, symbol: str = "SPY") -> Optional[str]:
        """获取下一个交易日"""
        try:
            df = self.load_stock_data_until(symbol, "2099-12-31", start_date=current_date)
            if df is None or len(df) == 0:
                return None
            
            current_date_obj = pd.to_datetime(current_date)
            future_dates = df[df.index > current_date_obj].index
            if len(future_dates) == 0:
                return None
            
            next_date = future_dates[0]
            return next_date.strftime("%Y-%m-%d")
        except Exception as e:
            print(f"[DataAdapter] 获取下一个交易日失败: {e}")
            return None

