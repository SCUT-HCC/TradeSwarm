"""
数据提供者抽象基类
定义统一的数据提供者接口
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd


class BaseDataProvider(ABC):
    """数据提供者抽象基类"""
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        初始化数据提供者
        
        Args:
            config: 配置字典，包含数据源相关配置
        """
        self.config = config
        data_sources_config = config.get("data_sources", {})
        self.data_sources_config = data_sources_config
    
    @abstractmethod
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
            start_date: 开始日期（格式：YYYY-MM-DD）
            end_date: 结束日期（格式：YYYY-MM-DD）
            trade_date: 可选，指定交易日期（格式：YYYY-MM-DD）
        
        Returns:
            pandas.DataFrame，包含以下字段：
                - Open: 开盘价
                - High: 最高价
                - Low: 最低价
                - Close: 收盘价
                - Volume: 成交量
            Index: DatetimeIndex
        """
        pass
    
    def get_company_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取公司基本信息
        
        Args:
            symbol: 股票代码
        
        Returns:
            包含公司信息的字典
        """
        raise NotImplementedError("此方法需要子类实现")
    
    def get_financial_statements(
        self,
        symbol: str,
        statement_type: str = "all"
    ) -> Dict[str, pd.DataFrame]:
        """
        获取财务报表
        
        Args:
            symbol: 股票代码
            statement_type: 报表类型 ('income', 'balance', 'cashflow', 'all')
        
        Returns:
            包含财务报表的字典
        """
        raise NotImplementedError("此方法需要子类实现")
    
    def get_financial_indicators(self, symbol: str) -> pd.DataFrame:
        """
        获取财务指标
        
        Args:
            symbol: 股票代码
        
        Returns:
            包含财务指标的 DataFrame
        """
        raise NotImplementedError("此方法需要子类实现")
    
    def get_valuation_metrics(self, symbol: str) -> pd.DataFrame:
        """
        获取估值指标
        
        Args:
            symbol: 股票代码
        
        Returns:
            包含估值指标的 DataFrame
        """
        raise NotImplementedError("此方法需要子类实现")
    
    def get_news(self, symbol: str, limit: int = 10) -> pd.DataFrame:
        """
        获取股票相关新闻
        
        Args:
            symbol: 股票代码
            limit: 返回新闻数量限制
        
        Returns:
            包含新闻数据的 DataFrame
        """
        raise NotImplementedError("此方法需要子类实现")
    
    def get_macro_news(self, limit: int = 10) -> pd.DataFrame:
        """
        获取宏观经济新闻
        
        Args:
            limit: 返回新闻数量限制
        
        Returns:
            包含宏观新闻的 DataFrame
        """
        raise NotImplementedError("此方法需要子类实现")

