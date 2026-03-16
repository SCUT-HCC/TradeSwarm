"""
YFinance 数据提供者
使用 yfinance 获取股票数据（主要支持美股，部分A股）
"""
import os
import pickle
import time
from typing import Any, Dict, Optional
import pandas as pd
import yfinance as yf
import requests

from .base_provider import BaseDataProvider


class YFinanceProvider(BaseDataProvider):
    """YFinance 数据提供者"""
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        初始化 YFinance Provider
        
        Args:
            config: 配置字典，需包含 data_sources 段
        """
        super().__init__(config)
        
        # 缓存设置
        self.cache_dir = self.data_sources_config.get('cache_dir', 'data_cache')
        self.use_cache = self.data_sources_config.get('use_cache', True)
        
        # 代理设置
        proxy_settings = self.data_sources_config.get('proxy_settings', {})
        self.use_proxy = proxy_settings.get('use_proxy', False)
        self.proxy_host = proxy_settings.get('host', '127.0.0.1')
        self.proxy_port = proxy_settings.get('port', 7890)
        self.proxy_type = proxy_settings.get('type', 'http')
        
        # 重试设置
        self.max_retries = self.data_sources_config.get('max_retries', 3)
        self.retry_delay = self.data_sources_config.get('retry_delay', 5)
        
        # 自动调整价格
        self.auto_adjust = self.data_sources_config.get('auto_adjust', True)
        
        # 设置代理
        if self.use_proxy:
            self._setup_proxy()
        
        # 确保缓存目录存在
        if self.use_cache:
            os.makedirs(self.cache_dir, exist_ok=True)
    
    def _setup_proxy(self) -> None:
        """设置代理环境变量"""
        if self.proxy_type == 'http':
            proxy_url = f'http://{self.proxy_host}:{self.proxy_port}'
        elif self.proxy_type == 'socks5':
            proxy_url = f'socks5://{self.proxy_host}:{self.proxy_port}'
        else:
            proxy_url = f'http://{self.proxy_host}:{self.proxy_port}'
        
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
        os.environ['http_proxy'] = proxy_url
        os.environ['https_proxy'] = proxy_url
    
    def _get_cache_path(self, symbol: str, start_date: str, end_date: str) -> str:
        """生成缓存文件路径"""
        start_clean = start_date.replace('-', '')
        end_clean = end_date.replace('-', '')
        filename = f"{symbol}_{start_clean}_{end_clean}.pkl"
        return os.path.join(self.cache_dir, filename)
    
    def _load_from_cache(self, cache_path: str) -> Optional[pd.DataFrame]:
        """从缓存加载数据"""
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'rb') as f:
                    df = pickle.load(f)
                return df
            except Exception as e:
                print(f"[WARN] 缓存加载失败: {e}")
                return None
        return None
    
    def _save_to_cache(self, df: pd.DataFrame, cache_path: str) -> None:
        """保存数据到缓存"""
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(df, f)
        except Exception as e:
            print(f"[WARN] 缓存保存失败: {e}")
    
    def _normalize_date(self, date_str: str) -> str:
        """
        标准化日期格式
        
        Args:
            date_str: 日期字符串（支持 YYYYMMDD 或 YYYY-MM-DD）
        
        Returns:
            YYYY-MM-DD 格式的日期字符串
        """
        if len(date_str) == 8 and date_str.isdigit():
            # YYYYMMDD 格式
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return date_str
    
    def get_daily(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        trade_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取股票日线行情数据
        
        Args:
            symbol: 股票代码（yfinance格式，如 'AAPL', '000001.SZ', '600519.SS'）
            start_date: 开始日期（格式：YYYY-MM-DD 或 YYYYMMDD）
            end_date: 结束日期（格式：YYYY-MM-DD 或 YYYYMMDD）
            trade_date: 可选，指定交易日期（格式：YYYY-MM-DD 或 YYYYMMDD）
        
        Returns:
            pandas.DataFrame，包含以下字段：
                - Open: 开盘价
                - High: 最高价
                - Low: 最低价
                - Close: 收盘价
                - Volume: 成交量
            Index: DatetimeIndex
        """
        # 标准化日期格式
        start_date = self._normalize_date(start_date)
        end_date = self._normalize_date(end_date)
        
        # 如果指定了交易日期，使用该日期作为开始和结束日期
        if trade_date:
            trade_date = self._normalize_date(trade_date)
            start_date = trade_date
            end_date = trade_date
        
        # 尝试从缓存加载
        if self.use_cache:
            cache_path = self._get_cache_path(symbol, start_date, end_date)
            cached_df = self._load_from_cache(cache_path)
            if cached_df is not None:
                return cached_df
        
        # 确保代理设置（如果启用）
        if self.use_proxy:
            self._setup_proxy()
        
        # 下载数据
        for attempt in range(self.max_retries):
            try:
                ticker = yf.Ticker(symbol)
                df = ticker.history(start=start_date, end=end_date, auto_adjust=self.auto_adjust)
                
                if df.empty:
                    # 尝试获取股票信息以提供更详细的错误信息
                    try:
                        info = ticker.info
                        long_name = info.get('longName', info.get('shortName', symbol))
                        raise ValueError(
                            f"无法获取 {symbol} ({long_name}) 的数据，"
                            f"该股票可能已退市或在该日期范围内无数据"
                        )
                    except:
                        raise ValueError(
                            f"无法获取 {symbol} 的数据，"
                            f"该股票可能已退市或在该日期范围内无数据"
                        )
                
                # 确保必要的列存在
                required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    raise ValueError(f"数据缺少必要的列: {missing_columns}")
                
                # 只保留必要的列
                df = df[required_columns].copy()
                
                # 确保索引是 DatetimeIndex
                if not isinstance(df.index, pd.DatetimeIndex):
                    df.index = pd.to_datetime(df.index)
                
                # 删除 NaN 值
                df = df.dropna()
                
                if len(df) == 0:
                    raise ValueError("数据清理后为空，请检查日期范围")
                
                # 排序
                df = df.sort_index()
                
                # 再次检查 NaN（以防万一）
                if df.isnull().any().any():
                    df = df.dropna()
                
                # 保存到缓存
                if self.use_cache:
                    self._save_to_cache(df, cache_path)
                
                return df
            
            except Exception as e:
                error_msg = str(e)
                
                if "Rate limited" in error_msg or "Too Many Requests" in error_msg:
                    if attempt < self.max_retries - 1:
                        wait_time = (attempt + 1) * self.retry_delay
                        print(f"[WARN] 速率限制，等待 {wait_time} 秒后重试... (尝试 {attempt + 1}/{self.max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise ValueError("达到最大重试次数，速率限制仍未解除")
                else:
                    if attempt == self.max_retries - 1:
                        raise
                    time.sleep(self.retry_delay)
        
        raise ValueError("数据获取失败")
    
    def get_company_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取公司基本信息
        
        Args:
            symbol: 股票代码
        
        Returns:
            包含公司信息的字典
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            # 提取关键信息
            company_info = {
                'symbol': symbol,
                'longName': info.get('longName', ''),
                'shortName': info.get('shortName', ''),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'marketCap': info.get('marketCap', 0),
                'currency': info.get('currency', 'USD'),
                'exchange': info.get('exchange', ''),
                'website': info.get('website', ''),
                'description': info.get('longBusinessSummary', ''),
            }
            
            return company_info
        except Exception as e:
            raise ValueError(f"获取公司信息失败: {e}")

