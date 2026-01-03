"""
OpenBB Provider yfinance 数据源可用性测试

测试目标：验证 OpenBBProvider 使用默认 yfinance 数据源的基本功能
测试范围：
    - OpenBB SDK 可用性检查
    - 个股新闻获取
    - 宏观新闻获取
    - 公司基本信息获取
    - 财务报表获取
    - 指数表现获取

注意：所有测试依赖网络连接和 OpenBB SDK 正确安装
"""
import pytest
import pandas as pd
from typing import Dict, Any

from datasources.data_sources.openbb_provider import OpenBBProvider, OPENBB_AVAILABLE


# ==================== Fixtures ====================

@pytest.fixture
def provider_instance() -> OpenBBProvider:
    """
    创建使用默认配置（yfinance）的 OpenBBProvider 实例

    Returns:
        OpenBBProvider: 配置为使用 yfinance 数据源的提供者实例
    """
    return OpenBBProvider()


# ==================== 可用性测试 ====================

def test_openbb_availability() -> None:
    """
    测试 OpenBB SDK 是否已正确安装

    验证：OPENBB_AVAILABLE 标志为 True
    """
    assert OPENBB_AVAILABLE, "OpenBB SDK 未安装，请运行: pip install openbb"


@pytest.mark.skipif(not OPENBB_AVAILABLE, reason="OpenBB SDK 未安装")
class TestOpenBBProviderYfinance:
    """OpenBB Provider yfinance 数据源功能测试套件"""

    # ==================== 个股新闻测试 ====================

    @pytest.mark.network
    def test_get_stock_news_basic(self, provider_instance: OpenBBProvider) -> None:
        """
        测试获取美股个股新闻功能

        Args:
            provider_instance: OpenBBProvider 测试实例

        验证：
            - 返回值为字符串类型
            - 包含 Markdown 格式标题
            - 返回值包含股票代码
        """
        # 第一阶段：调用接口获取 AAPL 新闻
        result = provider_instance.get_stock_news(symbol="AAPL", limit=5)

        # 第二阶段：验证返回格式
        assert isinstance(result, str), "返回值应为字符串类型"
        assert "# 美股新闻简报" in result, "应包含 Markdown 标题"
        assert "AAPL" in result, "应包含股票代码"

        # 第三阶段：验证返回内容完整性
        # 注意：yfinance 的 news API 可能返回空数据或错误
        # 我们验证格式正确即可，允许数据为空或错误的情况
        assert len(result) > 50, "返回内容应该有基本长度"

    # ==================== 宏观新闻测试 ====================

    @pytest.mark.network
    def test_get_macro_news_basic(self, provider_instance: OpenBBProvider) -> None:
        """
        测试获取全球宏观新闻功能

        Args:
            provider_instance: OpenBBProvider 测试实例

        验证：
            - 返回值为字典类型
            - 包含必需的键
            - data 字段为 DataFrame 或空 DataFrame
        """
        # 第一阶段：调用接口
        result = provider_instance.get_macro_news(limit=10)

        # 第二阶段：验证返回结构
        assert isinstance(result, dict), "返回值应为字典类型"

        required_keys = {'data', 'actual_sources', 'errors', 'update_time'}
        assert required_keys.issubset(result.keys()), f"应包含必需键: {required_keys}"

        # 第三阶段：验证数据类型
        assert isinstance(result['data'], pd.DataFrame), "data 应为 DataFrame"
        assert isinstance(result['errors'], list), "errors 应为列表"
        assert isinstance(result['update_time'], str), "update_time 应为字符串"

        # 注意：yfinance 的 news.world 可能失败并 fallback 到 SPY 新闻
        # 因此只验证格式，不强制要求有数据

    # ==================== 公司信息测试 ====================

    @pytest.mark.network
    def test_get_company_info_basic(self, provider_instance: OpenBBProvider) -> None:
        """
        测试获取公司基本信息功能

        Args:
            provider_instance: OpenBBProvider 测试实例

        验证：
            - 返回值为字典类型
            - 成功时包含公司基本字段
            - 失败时包含 error 字段
        """
        # 第一阶段：调用接口获取 AAPL 公司信息
        result = provider_instance.get_company_info(symbol="AAPL")

        # 第二阶段：验证返回结构
        assert isinstance(result, dict), "返回值应为字典类型"

        # 第三阶段：验证成功或失败的数据完整性
        if 'error' not in result:
            # 成功情况：应包含基本字段
            expected_fields = {'symbol', 'name', 'industry', 'sector', 'description', 'data'}
            assert expected_fields.issubset(result.keys()), f"成功时应包含字段: {expected_fields}"
            assert result['symbol'] == 'AAPL', "symbol 应为 AAPL"
        else:
            # 失败情况：应有错误信息
            assert isinstance(result['error'], str), "error 应为字符串"
            assert len(result['error']) > 0, "错误信息不应为空"

    # ==================== 财务报表测试 ====================

    @pytest.mark.network
    def test_get_financial_statements_basic(self, provider_instance: OpenBBProvider) -> None:
        """
        测试获取财务报表功能

        Args:
            provider_instance: OpenBBProvider 测试实例

        验证：
            - 返回值为字典类型
            - 包含三大报表字段
            - 包含错误列表字段
        """
        # 第一阶段：调用接口获取 AAPL 年度财务报表
        result = provider_instance.get_financial_statements(
            symbol="AAPL",
            report_type="annual",
            periods=2
        )

        # 第二阶段：验证返回结构
        assert isinstance(result, dict), "返回值应为字典类型"

        expected_fields = {'symbol', 'report_type', 'income', 'balance', 'cashflow', 'errors'}
        assert expected_fields.issubset(result.keys()), f"应包含字段: {expected_fields}"

        # 第三阶段：验证字段类型
        assert result['symbol'] == 'AAPL', "symbol 应为 AAPL"
        assert result['report_type'] == 'annual', "report_type 应为 annual"
        assert isinstance(result['errors'], list), "errors 应为列表"

        # 第四阶段：验证至少一个报表成功获取（如果有数据）
        statements = [result['income'], result['balance'], result['cashflow']]
        successful_statements = [s for s in statements if s is not None]

        # 允许部分失败，但至少应尝试获取数据
        # 注意：yfinance 可能对某些报表支持不完整
        if len(result['errors']) == 0:
            assert len(successful_statements) > 0, "至少应有一个报表返回数据"

    # ==================== 指数表现测试 ====================

    @pytest.mark.network
    def test_get_global_indices_performance(self, provider_instance: OpenBBProvider) -> None:
        """
        测试获取全球指数表现功能

        Args:
            provider_instance: OpenBBProvider 测试实例

        验证：
            - 返回值为字典类型
            - 包含必需字段
            - data 为 DataFrame 且包含预期指数
        """
        # 第一阶段：调用接口
        result = provider_instance.get_global_indices_performance()

        # 第二阶段：验证返回结构
        assert isinstance(result, dict), "返回值应为字典类型"

        required_keys = {'data', 'errors', 'update_time'}
        assert required_keys.issubset(result.keys()), f"应包含必需键: {required_keys}"

        # 第三阶段：验证数据类型
        assert isinstance(result['data'], pd.DataFrame), "data 应为 DataFrame"
        assert isinstance(result['errors'], list), "errors 应为列表"
        assert isinstance(result['update_time'], str), "update_time 应为字符串"

        # 第四阶段：验证指数数据（如果成功获取）
        if not result['data'].empty:
            # 应包含 SPY, QQQ, DIA 中的至少一个
            expected_indices = {'SPY', 'QQQ', 'DIA'}
            actual_indices = set(result['data']['asset'].values)

            assert len(actual_indices.intersection(expected_indices)) > 0, \
                f"应包含至少一个预期指数: {expected_indices}"

            # 验证数据列
            expected_columns = {'asset', 'code', 'price', 'change', 'change_pct'}
            assert expected_columns.issubset(result['data'].columns), \
                f"DataFrame 应包含列: {expected_columns}"


# ==================== 配置验证测试 ====================

@pytest.mark.skipif(not OPENBB_AVAILABLE, reason="OpenBB SDK 未安装")
def test_provider_default_config() -> None:
    """
    测试 OpenBBProvider 默认配置

    验证：
        - 默认新闻数据源为 yfinance
        - 默认基本面数据源为 yfinance
    """
    provider = OpenBBProvider()

    assert provider._news_provider == "yfinance", "默认新闻数据源应为 yfinance"
    assert provider._fundamental_provider == "yfinance", "默认基本面数据源应为 yfinance"
    assert provider._default_limit == 20, "默认限制应为 20"


@pytest.mark.skipif(not OPENBB_AVAILABLE, reason="OpenBB SDK 未安装")
def test_provider_custom_config() -> None:
    """
    测试 OpenBBProvider 自定义配置

    验证：
        - 可通过配置指定数据源
        - 配置正确应用到实例
    """
    custom_config = {
        "data_sources": {
            "openbb_news_provider": "benzinga",
            "openbb_fundamental_provider": "fmp",
            "openbb_default_limit": 50
        }
    }

    provider = OpenBBProvider(config=custom_config)

    assert provider._news_provider == "benzinga", "新闻数据源应为配置值"
    assert provider._fundamental_provider == "fmp", "基本面数据源应为配置值"
    assert provider._default_limit == 50, "默认限制应为配置值"
