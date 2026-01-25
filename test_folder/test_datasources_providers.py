#!/usr/bin/env python3
"""
测试 datasources/data_sources 目录下两个 Provider 的所有公共接口

测试范围：
- AkshareProvider: 所有公共方法
- TushareProvider: 所有公共方法

使用方法：
    python test_datasources_providers.py
"""
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
import traceback
import pandas as pd

# 添加项目根目录到路径
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# 导入配置加载器
try:
    from utils.config_loader import load_config
    config = load_config()
except Exception as e:
    print(f"⚠️ 警告: 无法加载配置文件，使用默认配置: {e}")
    config = {
        "data_sources": {
            "tushare_token": os.getenv("TUSHARE_TOKEN", ""),
            "akshare_default_news_limit": 10,
            "akshare_request_timeout": 30
        }
    }

# 导入 Provider
from datasources.data_sources.akshare_provider import AkshareProvider
from datasources.data_sources.tushare_provider import TushareProvider


# 测试用的股票代码（贵州茅台，比较稳定）
TEST_SYMBOL = "600519"
TEST_TS_CODE = "600519.SH"

# 测试结果统计
test_results: List[Tuple[str, bool, str]] = []


def log_test(method_name: str, success: bool, message: str = ""):
    """记录测试结果"""
    test_results.append((method_name, success, message))
    status = "✅" if success else "❌"
    print(f"{status} {method_name}")
    if message:
        print(f"   {message}")


def print_section(title: str):
    """打印测试章节标题"""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def test_akshare_provider():
    """测试 AkshareProvider 的所有公共接口"""
    print_section("测试 AkshareProvider")
    
    try:
        # 初始化 Provider
        akshare_provider = AkshareProvider(config=config)
        print("✓ AkshareProvider 初始化成功")
    except Exception as e:
        print(f"❌ AkshareProvider 初始化失败: {e}")
        return
    
    # 1. 测试 get_macro_news
    try:
        print("\n[1] 测试 get_macro_news(source='all', limit=5)")
        result = akshare_provider.get_macro_news(source="all", limit=5)
        assert isinstance(result, dict), "返回值应为字典"
        assert "data" in result, "应包含 data 字段"
        assert "update_time" in result, "应包含 update_time 字段"
        print(f"   更新时间: {result.get('update_time')}")
        print(f"   实际数据源: {result.get('actual_sources', [])}")
        print(f"   新闻数量: {len(result.get('data', []))}")
        if result.get('errors'):
            print(f"   警告: {result.get('errors')}")
        log_test("AkshareProvider.get_macro_news", True)
    except Exception as e:
        log_test("AkshareProvider.get_macro_news", False, str(e))
        traceback.print_exc()
    
    # 2. 测试 get_northbound_money_flow
    try:
        print("\n[2] 测试 get_northbound_money_flow()")
        result = akshare_provider.get_northbound_money_flow()
        assert isinstance(result, dict), "返回值应为字典"
        assert "data" in result or "errors" in result, "应包含 data 或 errors 字段"
        print(f"   更新时间: {result.get('update_time')}")
        if result.get('data'):
            data = result['data']
            print(f"   状态: {data.get('flow_status', 'N/A')}")
            print(f"   金额: {data.get('value', 'N/A')}")
        if result.get('errors'):
            print(f"   错误: {result.get('errors')}")
        log_test("AkshareProvider.get_northbound_money_flow", True)
    except Exception as e:
        log_test("AkshareProvider.get_northbound_money_flow", False, str(e))
        traceback.print_exc()
    
    # 3. 测试 get_global_indices_performance
    try:
        print("\n[3] 测试 get_global_indices_performance()")
        result = akshare_provider.get_global_indices_performance()
        assert isinstance(result, dict), "返回值应为字典"
        assert "data" in result or "errors" in result, "应包含 data 或 errors 字段"
        print(f"   更新时间: {result.get('update_time')}")
        data = result.get('data', pd.DataFrame())
        if isinstance(data, pd.DataFrame) and not data.empty:
            print(f"   指数数量: {len(data)}")
            if len(data) > 0:
                print(f"   前3个指数: {data.head(3).to_dict('records')}")
        if result.get('errors'):
            print(f"   错误: {result.get('errors')}")
        log_test("AkshareProvider.get_global_indices_performance", True)
    except Exception as e:
        log_test("AkshareProvider.get_global_indices_performance", False, str(e))
        traceback.print_exc()
    
    # 4. 测试 get_currency_exchange_rate
    try:
        print("\n[4] 测试 get_currency_exchange_rate()")
        result = akshare_provider.get_currency_exchange_rate()
        assert isinstance(result, dict), "返回值应为字典"
        assert "data" in result or "errors" in result, "应包含 data 或 errors 字段"
        print(f"   更新时间: {result.get('update_time')}")
        if result.get('data'):
            data = result['data']
            print(f"   货币对: {data.get('currency_pair', 'N/A')}")
            print(f"   汇率: {data.get('price', 'N/A')}")
            print(f"   涨跌幅: {data.get('change', 'N/A')}")
        if result.get('errors'):
            print(f"   错误: {result.get('errors')}")
        log_test("AkshareProvider.get_currency_exchange_rate", True)
    except Exception as e:
        log_test("AkshareProvider.get_currency_exchange_rate", False, str(e))
        traceback.print_exc()
    
    # 5. 测试 get_company_info
    try:
        print("\n[5] 测试 get_company_info(symbol)")
        result = akshare_provider.get_company_info(TEST_SYMBOL)
        assert isinstance(result, dict), "返回值应为字典"
        if "error" not in result:
            print(f"   公司名称: {result.get('name', 'N/A')}")
            print(f"   所属行业: {result.get('industry', 'N/A')}")
        else:
            print(f"   错误: {result.get('error')}")
        log_test("AkshareProvider.get_company_info", "error" not in result, result.get('error', ''))
    except Exception as e:
        log_test("AkshareProvider.get_company_info", False, str(e))
        traceback.print_exc()
    
    # 6. 测试 get_profit_statement
    try:
        print("\n[6] 测试 get_profit_statement(symbol, report_type='annual', periods=2)")
        result = akshare_provider.get_profit_statement(TEST_SYMBOL, "annual", 2, "all")
        assert isinstance(result, dict), "返回值应为字典"
        assert "symbol" in result, "应包含 symbol 字段"
        if result.get('data'):
            print(f"   实际数据源: {result.get('actual_source', 'N/A')}")
            print(f"   数据条数: {len(result['data'])}")
        if result.get('errors'):
            print(f"   错误: {result.get('errors')}")
        log_test("AkshareProvider.get_profit_statement", result.get('data') is not None, 
                '; '.join(result.get('errors', [])) if not result.get('data') else '')
    except Exception as e:
        log_test("AkshareProvider.get_profit_statement", False, str(e))
        traceback.print_exc()
    
    # 7. 测试 get_balance_sheet
    try:
        print("\n[7] 测试 get_balance_sheet(symbol, report_type='annual', periods=2)")
        result = akshare_provider.get_balance_sheet(TEST_SYMBOL, "annual", 2, "all")
        assert isinstance(result, dict), "返回值应为字典"
        assert "symbol" in result, "应包含 symbol 字段"
        if result.get('data'):
            print(f"   实际数据源: {result.get('actual_source', 'N/A')}")
            print(f"   数据条数: {len(result['data'])}")
        if result.get('errors'):
            print(f"   错误: {result.get('errors')}")
        log_test("AkshareProvider.get_balance_sheet", result.get('data') is not None,
                '; '.join(result.get('errors', [])) if not result.get('data') else '')
    except Exception as e:
        log_test("AkshareProvider.get_balance_sheet", False, str(e))
        traceback.print_exc()
    
    # 8. 测试 get_cash_flow_statement
    try:
        print("\n[8] 测试 get_cash_flow_statement(symbol, report_type='annual', periods=2)")
        result = akshare_provider.get_cash_flow_statement(TEST_SYMBOL, "annual", 2, "all")
        assert isinstance(result, dict), "返回值应为字典"
        assert "symbol" in result, "应包含 symbol 字段"
        if result.get('data'):
            print(f"   实际数据源: {result.get('actual_source', 'N/A')}")
            print(f"   数据条数: {len(result['data'])}")
        if result.get('errors'):
            print(f"   错误: {result.get('errors')}")
        log_test("AkshareProvider.get_cash_flow_statement", result.get('data') is not None,
                '; '.join(result.get('errors', [])) if not result.get('data') else '')
    except Exception as e:
        log_test("AkshareProvider.get_cash_flow_statement", False, str(e))
        traceback.print_exc()
    
    # 9. 测试 get_financial_statements
    try:
        print("\n[9] 测试 get_financial_statements(symbol, report_type='annual', periods=2)")
        result = akshare_provider.get_financial_statements(TEST_SYMBOL, "annual", 2)
        assert isinstance(result, dict), "返回值应为字典"
        assert "symbol" in result, "应包含 symbol 字段"
        print(f"   利润表: {'有数据' if result.get('income') else '无数据'}")
        print(f"   资产负债表: {'有数据' if result.get('balance') else '无数据'}")
        print(f"   现金流量表: {'有数据' if result.get('cashflow') else '无数据'}")
        if result.get('errors'):
            print(f"   错误: {result.get('errors')}")
        has_data = any([result.get('income'), result.get('balance'), result.get('cashflow')])
        log_test("AkshareProvider.get_financial_statements", has_data,
                '; '.join(result.get('errors', [])) if not has_data else '')
    except Exception as e:
        log_test("AkshareProvider.get_financial_statements", False, str(e))
        traceback.print_exc()
    
    # 10. 测试 get_valuation_indicators
    try:
        print("\n[10] 测试 get_valuation_indicators(symbol)")
        result = akshare_provider.get_valuation_indicators(TEST_SYMBOL, include_market_comparison=False)
        assert isinstance(result, dict), "返回值应为字典"
        assert "symbol" in result, "应包含 symbol 字段"
        if result.get('pe_pb'):
            print(f"   PE/PB数据: 有")
        if result.get('dividend'):
            print(f"   分红数据: 有")
        if result.get('errors'):
            print(f"   错误: {result.get('errors')}")
        has_data = any([result.get('pe_pb'), result.get('dividend')])
        log_test("AkshareProvider.get_valuation_indicators", has_data or len(result.get('errors', [])) == 0,
                '; '.join(result.get('errors', [])) if not has_data else '')
    except Exception as e:
        log_test("AkshareProvider.get_valuation_indicators", False, str(e))
        traceback.print_exc()
    
    # 11. 测试 get_earnings_data
    try:
        print("\n[11] 测试 get_earnings_data(symbol, limit=5)")
        result = akshare_provider.get_earnings_data(TEST_SYMBOL, limit=5)
        assert isinstance(result, dict), "返回值应为字典"
        assert "symbol" in result, "应包含 symbol 字段"
        if result.get('forecast'):
            print(f"   业绩预告: {len(result['forecast'])} 条")
        if result.get('express'):
            print(f"   业绩快报: {len(result['express'])} 条")
        if result.get('errors'):
            print(f"   错误: {result.get('errors')}")
        # 业绩数据可能为空，这是正常的
        log_test("AkshareProvider.get_earnings_data", True, 
                '; '.join(result.get('errors', [])) if result.get('errors') else '')
    except Exception as e:
        log_test("AkshareProvider.get_earnings_data", False, str(e))
        traceback.print_exc()


def test_tushare_provider():
    """测试 TushareProvider 的所有公共接口"""
    print_section("测试 TushareProvider")
    
    # 检查 Tushare Token
    if not config.get("data_sources", {}).get("tushare_token"):
        print("⚠️ 警告: Tushare Token 未设置，部分测试可能失败")
        print("   请在 config/config.yaml 中设置 data_sources.tushare_token")
        print("   或通过环境变量 TUSHARE_TOKEN 设置")
    
    try:
        # 初始化 Provider
        tushare_provider = TushareProvider(config=config)
        print("✓ TushareProvider 初始化成功")
    except Exception as e:
        print(f"❌ TushareProvider 初始化失败: {e}")
        print("   请检查 config/config.yaml 中的 tushare_token 配置")
        return
    
    # 计算测试日期（最近30天）
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")
    
    # 1. 测试 get_daily
    try:
        print("\n[1] 测试 get_daily(ts_code, start_date, end_date)")
        result = tushare_provider.get_daily(TEST_TS_CODE, start_date_str, end_date_str)
        assert isinstance(result, pd.DataFrame), "返回值应为 DataFrame"
        if not result.empty:
            print(f"   数据条数: {len(result)}")
            print(f"   列名: {list(result.columns)[:5]}...")
        else:
            print("   数据为空（可能是非交易日）")
        log_test("TushareProvider.get_daily", True, "数据为空" if result.empty else "")
    except Exception as e:
        log_test("TushareProvider.get_daily", False, str(e))
        traceback.print_exc()
    
    # 2. 测试 get_stock_basic
    try:
        print("\n[2] 测试 get_stock_basic(ts_code)")
        result = tushare_provider.get_stock_basic(TEST_TS_CODE)
        assert isinstance(result, pd.DataFrame), "返回值应为 DataFrame"
        if not result.empty:
            row = result.iloc[0]
            print(f"   股票名称: {row.get('name', 'N/A')}")
            print(f"   所属行业: {row.get('industry', 'N/A')}")
        log_test("TushareProvider.get_stock_basic", True)
    except Exception as e:
        log_test("TushareProvider.get_stock_basic", False, str(e))
        traceback.print_exc()
    
    # 3. 测试 get_realtime_orderbook
    try:
        print("\n[3] 测试 get_realtime_orderbook(ts_code, return_format='dict')")
        result = tushare_provider.get_realtime_orderbook(TEST_TS_CODE, return_format="dict")
        assert isinstance(result, dict), "返回值应为字典"
        print(f"   股票名称: {result.get('name', 'N/A')}")
        print(f"   当前价格: {result.get('price', 'N/A')}")
        print(f"   数据来源: {result.get('data_source', 'N/A')}")
        if 'warning' in result:
            print(f"   警告: {result.get('warning')}")
        log_test("TushareProvider.get_realtime_orderbook", True)
    except Exception as e:
        log_test("TushareProvider.get_realtime_orderbook", False, str(e))
        traceback.print_exc()
    
    # 4. 测试 get_company_info
    try:
        print("\n[4] 测试 get_company_info(ts_code)")
        result = tushare_provider.get_company_info(TEST_TS_CODE)
        assert isinstance(result, dict), "返回值应为字典"
        if "error" not in result:
            print(f"   股票名称: {result.get('name', 'N/A')}")
            print(f"   所属行业: {result.get('industry', 'N/A')}")
        else:
            print(f"   错误: {result.get('error')}")
        log_test("TushareProvider.get_company_info", "error" not in result, result.get('error', ''))
    except Exception as e:
        log_test("TushareProvider.get_company_info", False, str(e))
        traceback.print_exc()
    
    # 5. 测试 get_income
    try:
        print("\n[5] 测试 get_income(ts_code)")
        result = tushare_provider.get_income(TEST_TS_CODE)
        assert isinstance(result, pd.DataFrame), "返回值应为 DataFrame"
        if not result.empty:
            print(f"   数据条数: {len(result)}")
            print(f"   列名: {list(result.columns)[:5]}...")
        log_test("TushareProvider.get_income", True, "数据为空" if result.empty else "")
    except Exception as e:
        log_test("TushareProvider.get_income", False, str(e))
        traceback.print_exc()
    
    # 6. 测试 get_balancesheet
    try:
        print("\n[6] 测试 get_balancesheet(ts_code)")
        result = tushare_provider.get_balancesheet(TEST_TS_CODE)
        assert isinstance(result, pd.DataFrame), "返回值应为 DataFrame"
        if not result.empty:
            print(f"   数据条数: {len(result)}")
            print(f"   列名: {list(result.columns)[:5]}...")
        log_test("TushareProvider.get_balancesheet", True, "数据为空" if result.empty else "")
    except Exception as e:
        log_test("TushareProvider.get_balancesheet", False, str(e))
        traceback.print_exc()
    
    # 7. 测试 get_cashflow
    try:
        print("\n[7] 测试 get_cashflow(ts_code)")
        result = tushare_provider.get_cashflow(TEST_TS_CODE)
        assert isinstance(result, pd.DataFrame), "返回值应为 DataFrame"
        if not result.empty:
            print(f"   数据条数: {len(result)}")
            print(f"   列名: {list(result.columns)[:5]}...")
        log_test("TushareProvider.get_cashflow", True, "数据为空" if result.empty else "")
    except Exception as e:
        log_test("TushareProvider.get_cashflow", False, str(e))
        traceback.print_exc()
    
    # 8. 测试 get_fina_indicator
    try:
        print("\n[8] 测试 get_fina_indicator(ts_code)")
        result = tushare_provider.get_fina_indicator(TEST_TS_CODE)
        assert isinstance(result, pd.DataFrame), "返回值应为 DataFrame"
        if not result.empty:
            print(f"   数据条数: {len(result)}")
            print(f"   列名: {list(result.columns)[:5]}...")
        log_test("TushareProvider.get_fina_indicator", True, "数据为空" if result.empty else "")
    except Exception as e:
        log_test("TushareProvider.get_fina_indicator", False, str(e))
        traceback.print_exc()
    
    # 9. 测试 get_daily_basic
    try:
        print("\n[9] 测试 get_daily_basic(ts_code)")
        result = tushare_provider.get_daily_basic(TEST_TS_CODE)
        assert isinstance(result, pd.DataFrame), "返回值应为 DataFrame"
        if not result.empty:
            print(f"   数据条数: {len(result)}")
            print(f"   列名: {list(result.columns)[:5]}...")
        log_test("TushareProvider.get_daily_basic", True, "数据为空" if result.empty else "")
    except Exception as e:
        log_test("TushareProvider.get_daily_basic", False, str(e))
        traceback.print_exc()
    
    # 10. 测试 get_forecast
    try:
        print("\n[10] 测试 get_forecast(ts_code, limit=5)")
        result = tushare_provider.get_forecast(TEST_TS_CODE, limit=5)
        assert isinstance(result, pd.DataFrame), "返回值应为 DataFrame"
        if not result.empty:
            print(f"   数据条数: {len(result)}")
        else:
            print("   数据为空（可能没有业绩预告）")
        log_test("TushareProvider.get_forecast", True, "数据为空" if result.empty else "")
    except Exception as e:
        log_test("TushareProvider.get_forecast", False, str(e))
        traceback.print_exc()
    
    # 11. 测试 get_express
    try:
        print("\n[11] 测试 get_express(ts_code, limit=5)")
        result = tushare_provider.get_express(TEST_TS_CODE, limit=5)
        assert isinstance(result, pd.DataFrame), "返回值应为 DataFrame"
        if not result.empty:
            print(f"   数据条数: {len(result)}")
        else:
            print("   数据为空（可能没有业绩快报）")
        log_test("TushareProvider.get_express", True, "数据为空" if result.empty else "")
    except Exception as e:
        log_test("TushareProvider.get_express", False, str(e))
        traceback.print_exc()


def print_summary():
    """打印测试总结"""
    print_section("测试总结")
    
    total = len(test_results)
    passed = sum(1 for _, success, _ in test_results if success)
    failed = total - passed
    
    print(f"总测试数: {total}")
    print(f"通过: {passed} ({passed/total*100:.1f}%)")
    print(f"失败: {failed} ({failed/total*100:.1f}%)")
    
    if failed > 0:
        print("\n失败的测试:")
        for method_name, success, message in test_results:
            if not success:
                print(f"  ❌ {method_name}")
                if message:
                    print(f"     原因: {message}")
    
    print("\n" + "=" * 80)


def main():
    """主函数"""
    print("=" * 80)
    print(" datasources/data_sources Provider 接口测试")
    print("=" * 80)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试股票: {TEST_SYMBOL} ({TEST_TS_CODE})")
    
    # 测试 AkshareProvider
    test_akshare_provider()
    
    # 测试 TushareProvider
    test_tushare_provider()
    
    # 打印总结
    print_summary()


if __name__ == "__main__":
    main()

