"""OpenBB Provider"""
import re
from typing import Any, Optional, List, Dict
import pandas as pd
from datetime import datetime

# 尝试导入 OpenBB，如果未安装则标记
try:
    from openbb import obb
    OPENBB_AVAILABLE = True
except ImportError:
    obb = None
    OPENBB_AVAILABLE = False


class OpenBBProvider:
    """
    OpenBB 数据提供者封装 - 聚焦美股/全球市场数据

    主要功能：
    1. 获取美股个股新闻 (get_stock_news)
    2. 获取全球宏观新闻 (get_macro_news)
    3. 获取基本面数据 (get_company_info, get_financial_statements)

    设计理念：
    - 接口保持与 AkShareProvider 高度一致，便于上层业务切换
    - 优先使用免费数据源 (yfinance)，支持通过配置指定其他数据源
    - 输出格式标准化 (Markdown / Dict)
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """
        初始化 OpenBB Provider

        Args:
            config: 配置字典，可选。
                   可包含 'openbb_news_provider', 'openbb_fundamental_provider' 等配置。
        """
        if not OPENBB_AVAILABLE:
            print("Warning: OpenBB SDK not installed. OpenBBProvider will not function correctly.")

        self._config = config or {}

        # 提取配置
        data_sources_config = self._config.get("data_sources", {})

        # 默认数据源配置
        # 新闻数据源: yfinance (免费, 稳定), benzinga, tiingo, fmp, intrinio
        self._news_provider = data_sources_config.get("openbb_news_provider", "yfinance")

        # 基本面数据源: yfinance (免费), fmp, intrinio, polygon
        self._fundamental_provider = data_sources_config.get("openbb_fundamental_provider", "yfinance")

        # 默认限制
        self._default_limit = data_sources_config.get("openbb_default_limit", 20)

    # ==================== Public Methods ====================

    def get_stock_news(
        self,
        symbol: str,
        limit: int = 10
    ) -> str:
        """
        获取美股个股新闻

        Args:
            symbol: 股票代码 (如 'AAPL', 'TSLA')
            limit: 返回新闻数量限制

        Returns:
            Markdown 格式的新闻简报
        """
        if not OPENBB_AVAILABLE:
            return self._format_stock_news_error(symbol, "OpenBB SDK 未安装")

        try:
            # 清洗代码 (美股代码通常是字母)
            clean_symbol = symbol.strip().upper()

            # 调用 OpenBB 获取新闻
            # obb.news.company(symbol=..., provider=...)
            news_obj = obb.news.company(
                symbol=clean_symbol,
                limit=limit,
                provider=self._news_provider
            )

            # 转换为 DataFrame
            df = news_obj.to_df()

            if df is None or df.empty:
                return self._format_stock_news_empty(clean_symbol)

            # 格式化为 Markdown
            return self._format_stock_news_markdown(clean_symbol, df, limit)

        except Exception as e:
            return self._format_stock_news_error(symbol, str(e))

    def get_macro_news(
        self,
        limit: int = 20
    ) -> dict:
        """
        获取全球宏观新闻

        Args:
            limit: 新闻数量限制

        Returns:
            包含宏观新闻的字典：
            - data: pandas.DataFrame
            - errors: list
            - update_time: str
        """
        update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = {
            "data": pd.DataFrame(),
            "actual_sources": [],
            "errors": [],
            "update_time": update_time
        }

        if not OPENBB_AVAILABLE:
            result["errors"].append("OpenBB SDK 未安装")
            return result

        try:
            # 调用 OpenBB 获取全球新闻
            # obb.news.world(limit=..., provider=...)
            # 注意: yfinance 可能不支持 news.world，如果失败尝试其他或 tiingo (需key)
            # 这里先尝试配置的 provider，如果失败且是 yfinance，尝试 benzinga 或其他免费源(如果有)
            # 目前 yfinance 在 v4 中对 news.world 支持有限，通常返回空或报错
            # 如果配置的是 yfinance，我们尝试获取一些主要指数的新闻作为替代，或者直接尝试调用

            provider = self._news_provider

            try:
                news_obj = obb.news.world(limit=limit, provider=provider)
                df = news_obj.to_df()
            except Exception:
                # Fallback: 如果 news.world 失败，尝试获取 SPY (标普500) 的新闻作为宏观替代
                # 这在美股语境下常被视为市场新闻
                news_obj = obb.news.company(symbol="SPY", limit=limit, provider=provider)
                df = news_obj.to_df()

            if df is not None and not df.empty:
                # 统一列名
                df = self._format_news_dataframe(df, provider)
                result["data"] = df
                result["actual_sources"].append(provider)
            else:
                result["errors"].append(f"{provider} 数据源未返回宏观新闻")

        except Exception as e:
            result["errors"].append(f"宏观新闻获取失败: {str(e)}")

        return result

    def get_company_info(self, symbol: str) -> dict:
        """
        获取美股公司基本信息
        """
        if not OPENBB_AVAILABLE:
            return {"error": "OpenBB SDK 未安装"}

        try:
            clean_symbol = symbol.strip().upper()

            # obb.equity.profile(symbol=..., provider=...)
            profile_obj = obb.equity.profile(symbol=clean_symbol, provider=self._fundamental_provider)
            results = profile_obj.results

            if not results:
                return {"error": f"未找到 {clean_symbol} 的公司信息"}

            # 取第一条结果
            data = results[0].model_dump() if hasattr(results[0], 'model_dump') else results[0].dict()

            return {
                "symbol": clean_symbol,
                "name": data.get("name", "N/A"),
                "industry": data.get("industry", "N/A"),
                "sector": data.get("sector", "N/A"),
                "description": data.get("description", "N/A"),
                "data": data
            }

        except Exception as e:
            return {"error": f"获取公司信息失败: {str(e)}"}

    def get_financial_statements(
        self,
        symbol: str,
        report_type: str = "annual",
        periods: int = 4
    ) -> dict:
        """
        获取三大财务报表 (利润表, 资产负债表, 现金流量表)

        Args:
            symbol: 股票代码
            report_type: 'annual' (年度) 或 'quarter' (季度)
            periods: 期数
        """
        if not OPENBB_AVAILABLE:
            return {"error": "OpenBB SDK 未安装", "errors": ["OpenBB SDK 未安装"]}

        clean_symbol = symbol.strip().upper()
        provider = self._fundamental_provider

        result = {
            "symbol": clean_symbol,
            "report_type": report_type,
            "income": None,
            "balance": None,
            "cashflow": None,
            "errors": []
        }

        try:
            # 利润表
            try:
                income_obj = obb.equity.fundamental.income(
                    symbol=clean_symbol,
                    provider=provider,
                    period=report_type,
                    limit=periods
                )
                df = income_obj.to_df()
                if df is not None and not df.empty:
                    # 转为 dict list
                    # 注意：OpenBB 返回的 DataFrame 通常索引是日期或指标，需要确认格式
                    # 这里假设 to_dict('records') 可用，或者转置
                    # 通常基本面数据是以日期为索引，列为指标，或者反之
                    # 为了保持兼容性，我们直接转为 records
                    # 最好重置索引把日期放进去
                    df = df.reset_index()
                    result["income"] = df.to_dict('records')
            except Exception as e:
                result["errors"].append(f"利润表获取失败: {str(e)}")

            # 资产负债表
            try:
                balance_obj = obb.equity.fundamental.balance(
                    symbol=clean_symbol,
                    provider=provider,
                    period=report_type,
                    limit=periods
                )
                df = balance_obj.to_df()
                if df is not None and not df.empty:
                    df = df.reset_index()
                    result["balance"] = df.to_dict('records')
            except Exception as e:
                result["errors"].append(f"资产负债表获取失败: {str(e)}")

            # 现金流量表
            try:
                cash_obj = obb.equity.fundamental.cash(
                    symbol=clean_symbol,
                    provider=provider,
                    period=report_type,
                    limit=periods
                )
                df = cash_obj.to_df()
                if df is not None and not df.empty:
                    df = df.reset_index()
                    result["cashflow"] = df.to_dict('records')
            except Exception as e:
                result["errors"].append(f"现金流量表获取失败: {str(e)}")

        except Exception as e:
            result["errors"].append(f"财务报表获取总异常: {str(e)}")

        return result

    def get_global_indices_performance(self) -> dict:
        """
        获取美股核心指数表现 (替代 AkShare 的外围指数接口)
        """
        update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = {
            "data": pd.DataFrame(),
            "errors": [],
            "update_time": update_time
        }

        if not OPENBB_AVAILABLE:
            result["errors"].append("OpenBB SDK 未安装")
            return result

        try:
            # 获取 SPY, QQQ, DIA 作为核心指数代理
            indices = ["SPY", "QQQ", "DIA"]
            data_list = []

            for symbol in indices:
                try:
                    # 获取最新行情
                    # obb.equity.price.quote(symbol=...)
                    quote = obb.equity.price.quote(symbol=symbol, provider="yfinance")
                    res = quote.results[0]

                    # 提取数据
                    price = getattr(res, "last_price", getattr(res, "price", 0))
                    change = getattr(res, "change", 0)
                    change_percent = getattr(res, "change_percent", 0)

                    if price:
                        data_list.append({
                            "asset": symbol,
                            "code": symbol,
                            "price": price,
                            "change": f"{change_percent:.2f}%",
                            "change_pct": change_percent
                        })
                except Exception:
                    continue

            if data_list:
                result["data"] = pd.DataFrame(data_list)
            else:
                result["errors"].append("未能获取指数数据")

        except Exception as e:
            result["errors"].append(f"指数获取失败: {str(e)}")

        return result

    # ==================== Internal Helpers ====================

    def _format_stock_news_markdown(self, symbol: str, df: pd.DataFrame, limit: int) -> str:
        """格式化个股新闻为 Markdown"""
        update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        markdown = f"# 美股新闻简报 - {symbol}\n\n"
        markdown += f"**更新时间**: {update_time}\n\n"
        markdown += f"## 数据概览\n\n"
        markdown += f"- **股票代码**: {symbol}\n"
        markdown += f"- **新闻数量**: {len(df)} 条\n"
        markdown += f"- **数据来源**: OpenBB ({self._news_provider})\n\n"
        markdown += f"## 新闻列表\n\n"

        # 识别列名
        # OpenBB v4 news columns 常见: date, title, text/summary, url, source, images
        cols = df.columns
        title_col = next((c for c in cols if 'title' in c.lower()), 'title')
        date_col = next((c for c in cols if 'date' in c.lower()), 'date')
        url_col = next((c for c in cols if 'url' in c.lower() or 'link' in c.lower()), 'url')
        text_col = next((c for c in cols if 'text' in c.lower() or 'summary' in c.lower()), 'text')
        source_col = next((c for c in cols if 'source' in c.lower() or 'provider' in c.lower()), 'source')

        for idx, (_, row) in enumerate(df.iterrows(), 1):
            if idx > limit:
                break

            markdown += f"### {idx}. "

            # 标题
            title = str(row.get(title_col, '无标题')).strip()
            url = str(row.get(url_col, '')).strip()

            if url and url.lower() != 'nan':
                markdown += f"[{title}]({url})\n\n"
            else:
                markdown += f"{title}\n\n"

            # 详细信息
            date_val = row.get(date_col, '未知')
            markdown += f"- **发布时间**: {date_val}\n"

            source_val = row.get(source_col)
            if source_val and str(source_val).lower() != 'nan':
                markdown += f"- **来源**: {source_val}\n"

            # 摘要
            text_val = str(row.get(text_col, '')).strip()
            if text_val and text_val.lower() != 'nan':
                summary = text_val[:200] + "..." if len(text_val) > 200 else text_val
                # 清理换行
                summary = summary.replace('\n', ' ')
                markdown += f"- **摘要**: {summary}\n"

            markdown += "\n"

        markdown += f"*数据生成: TradeSwarm / OpenBB*\n"
        return markdown

    def _format_stock_news_empty(self, symbol: str) -> str:
        return f"# 美股新闻简报 - {symbol}\n\n## ⚠️ 数据为空\n\n未找到相关新闻。"

    def _format_stock_news_error(self, symbol: str, error: str) -> str:
        return f"# 美股新闻简报 - {symbol}\n\n## ❌ 获取失败\n\n错误信息: {error}"

    def _format_news_dataframe(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """标准化宏观新闻 DataFrame 列名"""
        if df is None or df.empty:
            return pd.DataFrame()

        # 简单映射，确保有 title, publish_time, url, content
        df = df.copy()

        # 寻找对应列并重命名
        cols = df.columns
        mapping = {}

        # Title
        if 'title' not in cols:
            found = next((c for c in cols if 'title' in c.lower()), None)
            if found: mapping[found] = 'title'

        # Date
        if 'publish_time' not in cols:
            found = next((c for c in cols if 'date' in c.lower()), None)
            if found: mapping[found] = 'publish_time'

        # URL
        if 'url' not in cols:
            found = next((c for c in cols if 'url' in c.lower() or 'link' in c.lower()), None)
            if found: mapping[found] = 'url'

        # Content
        if 'content' not in cols:
            found = next((c for c in cols if 'text' in c.lower() or 'summary' in c.lower()), None)
            if found: mapping[found] = 'content'

        df = df.rename(columns=mapping)

        # 添加源
        df['original_source'] = source

        return df
