"""
Alpha Vantage 数据提供者
用于获取新闻和基本面数据
"""
import os
import time
from typing import Any, Dict, Optional, List
from datetime import datetime
import pandas as pd
import requests

from .base_provider import BaseDataProvider


class AlphaVantageProvider(BaseDataProvider):
    """Alpha Vantage"""
    
    BASE_URL = "https://www.alphavantage.co/query"
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        初始化 Alpha Vantage Provider
        
        Args:
            config: 配置字典，需包含 data_sources 段及 alpha_vantage_api_key
        """
        super().__init__(config)
        
        # 获取 API Key 列表（新方式：支持多个 API Key）
        api_keys_list = self.data_sources_config.get('alpha_vantage_api_keys', [])
        
        # 兼容旧配置方式（单个 API Key）
        if not api_keys_list:
            # 尝试从旧配置读取
            old_api_key = self.data_sources_config.get('alpha_vantage_api_key', '')
            if old_api_key:
                api_keys_list = [old_api_key]
                # 尝试读取备用 API Key
                backup_key = self.data_sources_config.get('alpha_vantage_api_key_backup', '')
                if backup_key:
                    api_keys_list.append(backup_key)
        
        # 验证 API Key 列表
        if not api_keys_list or not any(api_keys_list):
            raise ValueError(
                "Alpha Vantage API Key 未设置，请在 config/config.yaml 中设置 "
                "data_sources.alpha_vantage_api_keys（列表格式），或通过环境变量 ALPHA_VANTAGE_API_KEY 配置"
            )
        
        # 过滤空字符串
        self.api_keys = [key for key in api_keys_list if key and key.strip()]
        
        if not self.api_keys:
            raise ValueError("Alpha Vantage API Key 列表为空")
        
        # 当前使用的 API Key 索引
        self.current_api_key_index = 0
        
        # 记录每个 API Key 的使用状态（用于统计和调试）
        self.api_key_usage = {key: {'success': 0, 'rate_limited': 0, 'last_used_time': None} for key in self.api_keys}
        
        # 标记已用完的 API Key（达到每日限制的 Key）
        self.exhausted_api_keys = set()
        
        # API Key 轮换间隔（秒）- 避免短时间内重复使用同一个 Key
        self.key_rotation_interval = 1  # 至少间隔 1 秒才重复使用同一个 Key
        
        # 重试设置
        self.max_retries = self.data_sources_config.get('max_retries', 3)
        self.retry_delay = self.data_sources_config.get('retry_delay', 5)
        
        # 代理设置
        proxy_settings = self.data_sources_config.get('proxy_settings', {})
        self.use_proxy = proxy_settings.get('use_proxy', False)
        if self.use_proxy:
            self.proxy_host = proxy_settings.get('host', '127.0.0.1')
            self.proxy_port = proxy_settings.get('port', 7890)
            self.proxy_type = proxy_settings.get('type', 'http')
            self._setup_proxy()
        else:
            # 不使用代理时，设置 proxies 为 None
            self.proxies = None
    
    def _setup_proxy(self) -> None:
        """设置代理"""
        if self.proxy_type == 'http':
            proxy_url = f'http://{self.proxy_host}:{self.proxy_port}'
        elif self.proxy_type == 'socks5':
            proxy_url = f'socks5://{self.proxy_host}:{self.proxy_port}'
        else:
            proxy_url = f'http://{self.proxy_host}:{self.proxy_port}'
        
        self.proxies = {'http': proxy_url, 'https': proxy_url}
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
    
    def _get_current_api_key(self) -> Optional[str]:
        """
        获取当前使用的 API Key（优先选择最久未使用的 Key，避免短时间内重复使用）
        
        Returns:
            当前 API Key，如果没有可用的返回 None
        """
        # 如果当前 Key 已用完，尝试找下一个
        if 0 <= self.current_api_key_index < len(self.api_keys):
            current_key = self.api_keys[self.current_api_key_index]
            if current_key not in self.exhausted_api_keys:
                # 检查该 Key 是否在轮换间隔内被使用过
                last_used = self.api_key_usage[current_key]['last_used_time']
                if last_used is None:
                    # 从未使用过，可以使用
                    return current_key
                else:
                    # 检查是否在轮换间隔内
                    time_since_last_use = time.time() - last_used
                    if time_since_last_use >= self.key_rotation_interval:
                        # 已经过了轮换间隔，可以使用
                        return current_key
                    # 否则，需要找下一个可用的 Key
        
        # 尝试找下一个未用完的 Key（优先选择最久未使用的）
        return self._find_next_available_key()
    
    def _find_next_available_key(self) -> Optional[str]:
        """
        查找下一个可用的 API Key（优先选择最久未使用的 Key，避免短时间内重复使用）
        
        Returns:
            可用的 API Key，如果没有可用的返回 None
        """
        # 如果所有 Key 都已用完，返回 None
        if len(self.exhausted_api_keys) >= len(self.api_keys):
            return None
        
        # 获取所有可用的 Key（排除已用完的）
        available_keys = [key for key in self.api_keys if key not in self.exhausted_api_keys]
        if not available_keys:
            return None
        
        # 优先选择最久未使用的 Key
        current_time = time.time()
        best_key = None
        longest_time_since_use = -1
        
        for key in available_keys:
            last_used = self.api_key_usage[key]['last_used_time']
            if last_used is None:
                # 从未使用过，优先选择
                best_key = key
                longest_time_since_use = float('inf')
                break
            else:
                time_since_last_use = current_time - last_used
                # 如果已经过了轮换间隔，且是最久未使用的，则选择它
                if time_since_last_use >= self.key_rotation_interval:
                    if time_since_last_use > longest_time_since_use:
                        best_key = key
                        longest_time_since_use = time_since_last_use
        
        # 如果找到了合适的 Key，更新索引
        if best_key:
            self.current_api_key_index = self.api_keys.index(best_key)
            return best_key
        
        # 如果所有 Key 都在轮换间隔内，则按顺序选择下一个（但跳过已用完的）
        # 从当前索引开始，查找下一个未用完的 Key
        start_index = self.current_api_key_index
        for _ in range(len(self.api_keys)):
            self.current_api_key_index = (self.current_api_key_index + 1) % len(self.api_keys)
            key = self.api_keys[self.current_api_key_index]
            if key not in self.exhausted_api_keys:
                return key
        
        # 如果找不到，返回 None
        return None
    
    def _mark_api_key_exhausted(self, api_key: str) -> None:
        """
        标记 API Key 为已用完（达到每日限制）
        
        Args:
            api_key: 要标记的 API Key
        """
        if api_key not in self.exhausted_api_keys:
            self.exhausted_api_keys.add(api_key)
            print(f"[WARN] API Key {api_key[:8]}...{api_key[-4:]} 已达到每日限制，已标记为已用完")
    
    def _switch_to_next_api_key(self) -> bool:
        """
        切换到下一个可用的 API Key（跳过已用完的）
        
        Returns:
            是否成功切换到下一个 API Key
        """
        # 查找下一个可用的 Key
        next_key = self._find_next_available_key()
        if next_key:
            print(f"[INFO] 切换到下一个 Alpha Vantage API Key (索引: {self.current_api_key_index + 1}/{len(self.api_keys)}, 剩余可用: {len(self.api_keys) - len(self.exhausted_api_keys)})")
            return True
        else:
            print(f"[WARN] 所有 Alpha Vantage API Key 都已达到频率限制")
            return False
    
    def _rotate_to_next_api_key(self, used_key: str) -> None:
        """
        轮换到下一个 API Key（每次成功使用后调用，避免短时间内重复使用同一个 Key）
        
        Args:
            used_key: 刚刚使用的 API Key
        """
        # 记录使用的 Key 的时间戳
        if used_key in self.api_key_usage:
            self.api_key_usage[used_key]['last_used_time'] = time.time()
        
        # 切换到下一个可用的 Key（用于下次使用）
        self._switch_to_next_api_key()
    
    def _record_api_key_usage(self, success: bool = True, rate_limited: bool = False) -> None:
        """
        记录 API Key 使用情况
        
        Args:
            success: 是否成功
            rate_limited: 是否达到频率限制
        """
        current_key = self._get_current_api_key()
        if current_key in self.api_key_usage:
            if success:
                self.api_key_usage[current_key]['success'] += 1
            if rate_limited:
                self.api_key_usage[current_key]['rate_limited'] += 1
    
    def _format_datetime_for_api(self, date_str: str, is_start: bool = True) -> Optional[str]:
        """
        将日期字符串转换为 Alpha Vantage API 要求的格式 (YYYYMMDDTHHMM)
        
        Args:
            date_str: 日期字符串，格式可以是：
                - YYYYMMDD (例如: "20250125")
                - YYYY-MM-DD (例如: "2025-01-25")
                - YYYYMMDDTHHMM (例如: "20250125T0000")
            is_start: 是否为开始时间（True=00:00, False=23:59）
        
        Returns:
            API 格式的日期时间字符串 (YYYYMMDDTHHMM)，如果解析失败返回 None
        """
        try:
            # 如果已经是 API 格式，直接返回
            if 'T' in date_str and len(date_str) >= 13:
                return date_str[:13]  # 确保格式正确
            
            # 尝试解析 YYYYMMDD 格式
            if len(date_str) == 8 and date_str.isdigit():
                date_obj = datetime.strptime(date_str, '%Y%m%d')
            # 尝试解析 YYYY-MM-DD 格式
            elif len(date_str) == 10 and '-' in date_str:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            else:
                # 尝试其他常见格式
                date_obj = datetime.strptime(date_str[:10].replace('-', ''), '%Y%m%d')
            
            # 根据 is_start 设置时间
            if is_start:
                # 开始时间设为 00:00
                time_str = "0000"
            else:
                # 结束时间设为 23:59
                time_str = "2359"
            
            # 格式化为 YYYYMMDDTHHMM
            return date_obj.strftime('%Y%m%d') + 'T' + time_str
            
        except (ValueError, TypeError) as e:
            print(f"[WARN] 日期格式解析失败: {date_str}, 错误: {e}")
            return None
    
    def _setup_proxy(self) -> None:
        """设置代理"""
        if self.proxy_type == 'http':
            proxy_url = f'http://{self.proxy_host}:{self.proxy_port}'
        elif self.proxy_type == 'socks5':
            proxy_url = f'socks5://{self.proxy_host}:{self.proxy_port}'
        else:
            proxy_url = f'http://{self.proxy_host}:{self.proxy_port}'
        
        self.proxies = {'http': proxy_url, 'https': proxy_url}
        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
    
    def _make_request(self, params: Dict[str, str]) -> Dict[str, Any]:
        """
        发送 API 请求，支持自动轮询多个 API Key
        
        Args:
            params: API 参数字典
        
        Returns:
            API 响应数据
        """
        # 尝试使用所有可用的 API Key（轮询，跳过已用完的）
        api_attempts = 0
        max_attempts = len(self.api_keys) - len(self.exhausted_api_keys)  # 只尝试未用完的 Key
        
        while api_attempts < max_attempts:
            current_key = self._get_current_api_key()
            
            # 如果没有可用的 Key，直接报错
            if current_key is None:
                raise ValueError(f"Alpha Vantage API 频率限制：所有 {len(self.api_keys)} 个 API Key 都已达到每日限制")
            
            params['apikey'] = current_key
            
            for attempt in range(self.max_retries):
                try:
                    response = requests.get(
                        self.BASE_URL,
                        params=params,
                        proxies=self.proxies,
                        timeout=30
                    )
                    response.raise_for_status()
                    data = response.json()
                    
                    # 检查 API 错误
                    if 'Error Message' in data:
                        raise ValueError(f"Alpha Vantage API 错误: {data['Error Message']}")
                    
                    # 检查频率限制（Note 或 Information 字段）
                    is_rate_limited = False
                    rate_limit_message = None
                    
                    if 'Note' in data:
                        # API 调用频率限制 - 立即标记并切换，不重试
                        is_rate_limited = True
                        rate_limit_message = data.get('Note', '频率限制')
                        # 标记当前 Key 为已用完
                        self._mark_api_key_exhausted(current_key)
                        self._record_api_key_usage(success=False, rate_limited=True)
                        # 尝试切换到下一个可用的 API Key
                        if self._switch_to_next_api_key():
                            # 重置重试计数，使用下一个 API Key 重试
                            break
                        else:
                            # 所有 Key 都已达到限制
                            raise ValueError(f"Alpha Vantage API 频率限制: {rate_limit_message}（所有 {len(self.api_keys)} 个 API Key 都已达到限制）")
                    
                    # 检查 Information 字段（也是频率限制提示）
                    has_data = 'Symbol' in data or 'annualReports' in data or 'quarterlyReports' in data or \
                              'annualEarnings' in data or 'quarterlyEarnings' in data or 'feed' in data
                    if 'Information' in data and not has_data:
                        # 这是频率限制提示，不是实际数据 - 立即标记并切换，不重试
                        is_rate_limited = True
                        rate_limit_message = data.get('Information', '频率限制')
                        # 标记当前 Key 为已用完
                        self._mark_api_key_exhausted(current_key)
                        self._record_api_key_usage(success=False, rate_limited=True)
                        # 尝试切换到下一个可用的 API Key
                        if self._switch_to_next_api_key():
                            # 重置重试计数，使用下一个 API Key 重试
                            break
                        else:
                            # 所有 Key 都已达到限制
                            raise ValueError(f"Alpha Vantage API 频率限制: {rate_limit_message}（所有 {len(self.api_keys)} 个 API Key 都已达到限制）")
                    
                    # 成功获取数据，记录使用情况
                    self._record_api_key_usage(success=True, rate_limited=False)
                    # 立即轮换到下一个 API Key，避免短时间内重复使用同一个 Key
                    self._rotate_to_next_api_key(current_key)
                    return data
                
                except requests.exceptions.RequestException as e:
                    if attempt == self.max_retries - 1:
                        # 如果当前 API Key 失败，尝试切换到下一个
                        if self._switch_to_next_api_key():
                            break  # 使用下一个 API Key 重试
                        raise ValueError(f"API 请求失败: {e}")
                    time.sleep(self.retry_delay)
            
            # 如果当前 API Key 的所有重试都失败，尝试切换到下一个
            api_attempts += 1
            if api_attempts < len(self.api_keys):
                if not self._switch_to_next_api_key():
                    break  # 无法切换到下一个 Key
        
        raise ValueError(f"API 请求失败：已尝试所有 {len(self.api_keys)} 个 API Key")
    
    def get_daily(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        trade_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Alpha Vantage 不提供日线数据，返回空 DataFrame
        
        注意：Alpha Vantage 主要用于新闻和基本面数据，日线数据应使用 yfinance
        """
        return pd.DataFrame()
    
    def get_news(self, symbol: str, limit: int = 10, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """
        获取股票相关新闻
        
        Args:
            symbol: 股票代码（yfinance格式，如 'AAPL'）
            limit: 返回新闻数量限制（默认 10，最大 1000）
            start_date: 可选，开始日期（格式：YYYYMMDD 或 YYYYMMDDTHHMM）
            end_date: 可选，结束日期（格式：YYYYMMDD 或 YYYYMMDDTHHMM）
        
        Returns:
            包含新闻数据的 DataFrame
        """
        # 将 yfinance 格式转换为 Alpha Vantage 格式（去掉交易所后缀）
        av_symbol = symbol.split('.')[0] if '.' in symbol else symbol
        
        params = {
            'function': 'NEWS_SENTIMENT',
            'tickers': av_symbol,
            'limit': str(min(limit, 1000)),
            'sort': 'LATEST'  # 按最新排序
        }
        
        # 添加历史日期参数（如果提供）
        if start_date:
            # 转换日期格式为 YYYYMMDDTHHMM
            time_from = self._format_datetime_for_api(start_date, is_start=True)
            if time_from:
                params['time_from'] = time_from
        
        if end_date:
            # 转换日期格式为 YYYYMMDDTHHMM
            time_to = self._format_datetime_for_api(end_date, is_start=False)
            if time_to:
                params['time_to'] = time_to
        
        try:
            data = self._make_request(params)
            
            if 'feed' not in data or not data['feed']:
                return pd.DataFrame()
            
            news_list = []
            current_year = datetime.now().year
            
            for item in data['feed'][:limit]:
                time_published = item.get('time_published', '')
                
                # 验证年份（记录警告但不过滤）
                try:
                    if len(time_published) >= 4:
                        year = int(time_published[:4])
                        if year > current_year + 1:
                            print(f"[WARN] 新闻时间戳异常: {time_published} (当前年份: {current_year})")
                except (ValueError, TypeError):
                    pass
                
                news_list.append({
                    'title': item.get('title', ''),
                    'url': item.get('url', ''),
                    'time_published': time_published,
                    'summary': item.get('summary', ''),
                    'source': item.get('source', ''),
                    'overall_sentiment_score': item.get('overall_sentiment_score', 0),
                    'overall_sentiment_label': item.get('overall_sentiment_label', ''),
                })
            
            return pd.DataFrame(news_list)
        
        except Exception as e:
            raise ValueError(f"获取新闻数据失败: {e}")
    
    def get_company_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取公司基本信息（使用 OVERVIEW 接口）
        
        Args:
            symbol: 股票代码
        
        Returns:
            包含公司信息的字典
        """
        # 将 yfinance 格式转换为 Alpha Vantage 格式
        av_symbol = symbol.split('.')[0] if '.' in symbol else symbol
        
        params = {
            'function': 'OVERVIEW',
            'symbol': av_symbol
        }
        
        try:
            data = self._make_request(params)
            
            # 检查是否是频率限制响应
            if 'Information' in data and 'Symbol' not in data:
                raise ValueError(f"API 频率限制: {data['Information']}")
            
            if not data or 'Symbol' not in data:
                # 打印实际响应以便调试
                print(f"[DEBUG] OVERVIEW API 响应字段: {list(data.keys()) if data else 'None'}")
                raise ValueError(f"无法获取 {symbol} 的公司信息，响应中没有 'Symbol' 字段")
            
            return {
                'symbol': data.get('Symbol', symbol),
                'name': data.get('Name', ''),
                'sector': data.get('Sector', ''),
                'industry': data.get('Industry', ''),
                'marketCap': data.get('MarketCapitalization', '0'),
                'currency': data.get('Currency', 'USD'),
                'exchange': data.get('Exchange', ''),
                'website': data.get('Website', ''),
                'description': data.get('Description', ''),
            }
        
        except Exception as e:
            raise ValueError(f"获取公司信息失败: {e}")
    
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
        # 将 yfinance 格式转换为 Alpha Vantage 格式
        av_symbol = symbol.split('.')[0] if '.' in symbol else symbol
        
        result = {}
        
        if statement_type in ['income', 'all']:
            try:
                params = {'function': 'INCOME_STATEMENT', 'symbol': av_symbol}
                data = self._make_request(params)
                if 'annualReports' in data and data['annualReports']:
                    # 将年度报告转换为 DataFrame，按日期倒序排列
                    df = pd.DataFrame(data['annualReports'])
                    if 'fiscalDateEnding' in df.columns:
                        df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'])
                        df = df.sort_values('fiscalDateEnding', ascending=False)
                    result['income'] = df
                elif 'quarterlyReports' in data and data['quarterlyReports']:
                    # 如果没有年度报告，使用季度报告
                    df = pd.DataFrame(data['quarterlyReports'])
                    if 'fiscalDateEnding' in df.columns:
                        df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'])
                        df = df.sort_values('fiscalDateEnding', ascending=False)
                    result['income'] = df
            except Exception as e:
                print(f"[WARN] 获取利润表失败: {e}")
        
        if statement_type in ['balance', 'all']:
            try:
                params = {'function': 'BALANCE_SHEET', 'symbol': av_symbol}
                data = self._make_request(params)
                if 'annualReports' in data and data['annualReports']:
                    df = pd.DataFrame(data['annualReports'])
                    if 'fiscalDateEnding' in df.columns:
                        df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'])
                        df = df.sort_values('fiscalDateEnding', ascending=False)
                    result['balance'] = df
                elif 'quarterlyReports' in data and data['quarterlyReports']:
                    df = pd.DataFrame(data['quarterlyReports'])
                    if 'fiscalDateEnding' in df.columns:
                        df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'])
                        df = df.sort_values('fiscalDateEnding', ascending=False)
                    result['balance'] = df
            except Exception as e:
                print(f"[WARN] 获取资产负债表失败: {e}")
        
        if statement_type in ['cashflow', 'all']:
            try:
                params = {'function': 'CASH_FLOW', 'symbol': av_symbol}
                data = self._make_request(params)
                if 'annualReports' in data and data['annualReports']:
                    df = pd.DataFrame(data['annualReports'])
                    if 'fiscalDateEnding' in df.columns:
                        df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'])
                        df = df.sort_values('fiscalDateEnding', ascending=False)
                    result['cashflow'] = df
                elif 'quarterlyReports' in data and data['quarterlyReports']:
                    df = pd.DataFrame(data['quarterlyReports'])
                    if 'fiscalDateEnding' in df.columns:
                        df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'])
                        df = df.sort_values('fiscalDateEnding', ascending=False)
                    result['cashflow'] = df
            except Exception as e:
                print(f"[WARN] 获取现金流量表失败: {e}")
        
        return result
    
    def get_financial_indicators(self, symbol: str) -> pd.DataFrame:
        """
        获取财务指标
        
        Args:
            symbol: 股票代码
        
        Returns:
            包含财务指标的 DataFrame
        """
        # 将 yfinance 格式转换为 Alpha Vantage 格式
        av_symbol = symbol.split('.')[0] if '.' in symbol else symbol
        
        params = {
            'function': 'OVERVIEW',
            'symbol': av_symbol
        }
        
        try:
            data = self._make_request(params)
            
            if not data or 'Symbol' not in data:
                return pd.DataFrame()
            
            # 将字典转换为 DataFrame
            indicators = {
                'symbol': [data.get('Symbol', symbol)],
                'pe_ratio': [data.get('PERatio', '')],
                'peg_ratio': [data.get('PEGRatio', '')],
                'eps': [data.get('EPS', '')],
                'dividend_yield': [data.get('DividendYield', '')],
                'roe': [data.get('ReturnOnEquityTTM', '')],
                'roa': [data.get('ReturnOnAssetsTTM', '')],
                'profit_margin': [data.get('ProfitMargin', '')],
            }
            
            return pd.DataFrame(indicators)
        
        except Exception as e:
            raise ValueError(f"获取财务指标失败: {e}")
    
    def get_valuation_metrics(self, symbol: str) -> pd.DataFrame:
        """
        获取估值指标
        
        Args:
            symbol: 股票代码
        
        Returns:
            包含估值指标的 DataFrame
        """
        # 将 yfinance 格式转换为 Alpha Vantage 格式
        av_symbol = symbol.split('.')[0] if '.' in symbol else symbol
        
        params = {
            'function': 'OVERVIEW',
            'symbol': av_symbol
        }
        
        try:
            data = self._make_request(params)
            
            if not data or 'Symbol' not in data:
                return pd.DataFrame()
            
            # 即使某些字段为空，只要 Symbol 存在，就返回 DataFrame
            metrics = {
                'symbol': [data.get('Symbol', symbol)],
                'market_cap': [data.get('MarketCapitalization', '') or ''],
                'pe_ratio': [data.get('PERatio', '') or ''],
                'peg_ratio': [data.get('PEGRatio', '') or ''],
                'price_to_book': [data.get('PriceToBookRatio', '') or ''],
                'price_to_sales': [data.get('PriceToSalesRatioTTM', '') or ''],
                'ev_to_ebitda': [data.get('EVToRevenue', '') or ''],
                'dividend_yield': [data.get('DividendYield', '') or ''],
            }
            
            df = pd.DataFrame(metrics)
            # 即使某些字段为空，只要 Symbol 存在，就返回 DataFrame
            if 'Symbol' in data:
                return df
            return pd.DataFrame()
        
        except Exception as e:
            raise ValueError(f"获取估值指标失败: {e}")
    
    def get_macro_news(self, limit: int = 10, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """
        获取宏观经济新闻
        
        Args:
            limit: 返回新闻数量限制（默认 10，最大 1000）
            start_date: 可选，开始日期（格式：YYYYMMDD 或 YYYYMMDDTHHMM）
            end_date: 可选，结束日期（格式：YYYYMMDD 或 YYYYMMDDTHHMM）
        
        Returns:
            包含宏观新闻的 DataFrame
        """
        params = {
            'function': 'NEWS_SENTIMENT',
            'topics': 'economy',  # 宏观经济主题
            'limit': str(min(limit, 1000)),
            'sort': 'LATEST'  # 按最新排序
        }
        
        # 添加历史日期参数（如果提供）
        if start_date:
            # 转换日期格式为 YYYYMMDDTHHMM
            time_from = self._format_datetime_for_api(start_date, is_start=True)
            if time_from:
                params['time_from'] = time_from
        
        if end_date:
            # 转换日期格式为 YYYYMMDDTHHMM
            time_to = self._format_datetime_for_api(end_date, is_start=False)
            if time_to:
                params['time_to'] = time_to
        
        try:
            data = self._make_request(params)
            
            if 'feed' not in data or not data['feed']:
                return pd.DataFrame()
            
            news_list = []
            current_year = datetime.now().year
            
            for item in data['feed'][:limit]:
                time_published = item.get('time_published', '')
                
                # 验证年份（记录警告但不过滤）
                try:
                    if len(time_published) >= 4:
                        year = int(time_published[:4])
                        if year > current_year + 1:
                            print(f"[WARN] 新闻时间戳异常: {time_published} (当前年份: {current_year})")
                except (ValueError, TypeError):
                    pass
                
                news_list.append({
                    'title': item.get('title', ''),
                    'url': item.get('url', ''),
                    'time_published': time_published,
                    'summary': item.get('summary', ''),
                    'source': item.get('source', ''),
                    'overall_sentiment_score': item.get('overall_sentiment_score', 0.0),
                })
            
            if not news_list:
                return pd.DataFrame()
            
            df = pd.DataFrame(news_list)
            
            # 转换时间格式
            if 'time_published' in df.columns:
                df['time_published'] = pd.to_datetime(
                    df['time_published'],
                    format='%Y%m%dT%H%M%S',
                    errors='coerce'
                )
            
            return df
        
        except Exception as e:
            raise ValueError(f"获取宏观新闻失败: {e}")
    
    def get_earnings_data(self, symbol: str, limit: int = 10) -> Dict[str, Any]:
        """
        获取业绩数据（使用 EARNINGS 接口）
        
        Args:
            symbol: 股票代码
            limit: 返回最近 N 条记录
        
        Returns:
            包含业绩数据的字典
        """
        # 将 yfinance 格式转换为 Alpha Vantage 格式
        av_symbol = symbol.split('.')[0] if '.' in symbol else symbol
        
        params = {
            'function': 'EARNINGS',
            'symbol': av_symbol
        }
        
        try:
            data = self._make_request(params)
            
            if not data or 'Symbol' not in data:
                return {
                    'annualEarnings': [],
                    'quarterlyEarnings': [],
                }
            
            annual_earnings = data.get('annualEarnings', [])
            quarterly_earnings = data.get('quarterlyEarnings', [])
            
            return {
                'annualEarnings': annual_earnings[:limit] if annual_earnings else [],
                'quarterlyEarnings': quarterly_earnings[:limit] if quarterly_earnings else [],
            }
        
        except Exception as e:
            raise ValueError(f"获取业绩数据失败: {e}")

