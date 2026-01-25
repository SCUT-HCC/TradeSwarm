#!/usr/bin/env python3
"""
测试 tradingagents/tools 目录下所有工具函数是否可用

测试范围：
- market_tools: get_stock_data
- technical_tools: get_indicators
- news_tools: get_news, get_global_news
- fundamentals_tools: get_company_info, get_financial_statements, get_financial_indicators, 
                       get_valuation_indicators, get_earnings_data

使用方法：
    python test_folder/test_all_tools.py
"""
import os
import sys
import json
from datetime import datetime, timedelta
from typing import List, Tuple

# 添加项目根目录到路径
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# 导入所有工具
from tradingagents.tool_nodes.utils.market_tools import get_stock_data
from tradingagents.tool_nodes.utils.technical_tools import get_indicators
from tradingagents.tool_nodes.utils.news_tools import get_news, get_global_news
from tradingagents.tool_nodes.utils.fundamentals_tools import (
    get_company_info,
    get_financial_statements,
    get_financial_indicators,
    get_valuation_indicators,
    get_earnings_data
)

# 测试用的股票代码（贵州茅台，比较稳定）
TEST_SYMBOL = "600519"
TEST_TS_CODE = "600519.SH"

# 测试结果统计
test_results: List[Tuple[str, bool, str]] = []


def log_test(tool_name: str, success: bool, message: str = ""):
    """记录测试结果"""
    test_results.append((tool_name, success, message))
    status = "✅" if success else "❌"
    print(f"{status} {tool_name}")
    if message:
        print(f"   {message}")


def print_section(title: str):
    """打印测试章节标题"""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def test_market_tools():
    """测试市场数据工具"""
    print_section("测试 Market Tools")
    
    # 1. 测试 get_stock_data
    try:
        print("\n[1] 测试 get_stock_data(ts_code, start_date, end_date)")
        # 获取最近30天的数据
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        
        result_str = get_stock_data.invoke({
            "ts_code": TEST_TS_CODE,
            "start_date": start_date_str,
            "end_date": end_date_str
        })
        result = json.loads(result_str)
        
        assert result.get("success") is not None, "返回结果应包含 success 字段"
        if result.get("success"):
            assert "data" in result, "成功时应包含 data 字段"
            print(f"   成功获取 {len(result.get('data', []))} 条数据")
            if result.get("summary"):
                print(f"   最新价格: {result['summary'].get('latest_price', {})}")
        else:
            print(f"   获取失败: {result.get('message', '')}")
        
        log_test("get_stock_data", result.get("success", False), 
                result.get("message", "")[:100] if not result.get("success") else "")
    except Exception as e:
        log_test("get_stock_data", False, str(e))
        import traceback
        traceback.print_exc()


def test_technical_tools():
    """测试技术指标工具"""
    print_section("测试 Technical Tools")
    
    # 1. 测试 get_indicators
    try:
        print("\n[1] 测试 get_indicators(ts_code, indicators, period=30)")
        result_str = get_indicators.invoke({
            "ts_code": TEST_TS_CODE,
            "indicators": "MA,RSI",
            "period": 30
        })
        result = json.loads(result_str)
        
        assert result.get("success") is not None, "返回结果应包含 success 字段"
        if result.get("success"):
            assert "data" in result, "成功时应包含 data 字段"
            assert "indicators" in result, "成功时应包含 indicators 字段"
            print(f"   成功计算 {len(result.get('indicators', []))} 个指标")
            print(f"   数据条数: {len(result.get('data', []))}")
            if result.get("summary", {}).get("latest_indicators"):
                print(f"   最新指标: {list(result['summary']['latest_indicators'].keys())[:3]}")
        else:
            print(f"   计算失败: {result.get('message', '')}")
        
        log_test("get_indicators", result.get("success", False),
                result.get("message", "")[:100] if not result.get("success") else "")
    except Exception as e:
        log_test("get_indicators", False, str(e))
        import traceback
        traceback.print_exc()


def test_news_tools():
    """测试新闻工具"""
    print_section("测试 News Tools")
    
    # 1. 测试 get_news
    try:
        print("\n[1] 测试 get_news(ts_code, days=7, limit=5)")
        result_str = get_news.invoke({
            "ts_code": TEST_SYMBOL,
            "days": 7,
            "limit": 5
        })
        result = json.loads(result_str)
        
        assert result.get("success") is not None, "返回结果应包含 success 字段"
        if result.get("success"):
            print(f"   数据格式: {result.get('format', 'json')}")
            if result.get("format") == "markdown":
                content = result.get("content", "")
                print(f"   Markdown 内容长度: {len(content)} 字符")
                print(f"   内容预览: {content[:100]}...")
            else:
                print(f"   数据条数: {len(result.get('data', []))}")
        else:
            print(f"   获取失败: {result.get('message', '')}")
        
        log_test("get_news", result.get("success", False),
                result.get("message", "")[:100] if not result.get("success") else "")
    except Exception as e:
        log_test("get_news", False, str(e))
        import traceback
        traceback.print_exc()
    
    # 2. 测试 get_global_news
    try:
        print("\n[2] 测试 get_global_news(days=7, limit=5)")
        result_str = get_global_news.invoke({
            "days": 7,
            "limit": 5
        })
        result = json.loads(result_str)
        
        assert result.get("success") is not None, "返回结果应包含 success 字段"
        if result.get("success"):
            print(f"   数据格式: {result.get('format', 'json')}")
            if result.get("format") == "markdown":
                content = result.get("content", "")
                print(f"   Markdown 内容长度: {len(content)} 字符")
                print(f"   内容预览: {content[:150]}...")
            else:
                print(f"   数据条数: {len(result.get('data', []))}")
        else:
            print(f"   获取失败: {result.get('message', '')}")
        
        log_test("get_global_news", result.get("success", False),
                result.get("message", "")[:100] if not result.get("success") else "")
    except Exception as e:
        log_test("get_global_news", False, str(e))
        import traceback
        traceback.print_exc()


def test_fundamentals_tools():
    """测试基本面分析工具"""
    print_section("测试 Fundamentals Tools")
    
    # 1. 测试 get_company_info
    try:
        print("\n[1] 测试 get_company_info(ts_code)")
        result_str = get_company_info.invoke({
            "ts_code": TEST_SYMBOL
        })
        result = json.loads(result_str)
        
        assert result.get("success") is not None, "返回结果应包含 success 字段"
        if result.get("success"):
            data = result.get("data", {})
            print(f"   公司名称: {data.get('name', 'N/A')}")
            print(f"   所属行业: {data.get('industry', 'N/A')}")
            print(f"   数据源: {result.get('summary', {}).get('data_source', 'N/A')}")
        else:
            print(f"   获取失败: {result.get('message', '')}")
        
        log_test("get_company_info", result.get("success", False),
                result.get("message", "")[:100] if not result.get("success") else "")
    except Exception as e:
        log_test("get_company_info", False, str(e))
        import traceback
        traceback.print_exc()
    
    # 2. 测试 get_financial_statements
    try:
        print("\n[2] 测试 get_financial_statements(ts_code, report_type='annual', periods=2)")
        result_str = get_financial_statements.invoke({
            "ts_code": TEST_SYMBOL,
            "report_type": "annual",
            "periods": 2
        })
        result = json.loads(result_str)
        
        assert result.get("success") is not None, "返回结果应包含 success 字段"
        if result.get("success"):
            data = result.get("data", {})
            print(f"   利润表: {'有数据' if data.get('income') else '无数据'}")
            print(f"   资产负债表: {'有数据' if data.get('balance') else '无数据'}")
            print(f"   现金流量表: {'有数据' if data.get('cashflow') else '无数据'}")
            if data.get("errors"):
                print(f"   警告: {data.get('errors')}")
        else:
            print(f"   获取失败: {result.get('message', '')}")
        
        log_test("get_financial_statements", result.get("success", False),
                result.get("message", "")[:100] if not result.get("success") else "")
    except Exception as e:
        log_test("get_financial_statements", False, str(e))
        import traceback
        traceback.print_exc()
    
    # 3. 测试 get_financial_indicators
    try:
        print("\n[3] 测试 get_financial_indicators(ts_code, report_type='annual', periods=2)")
        result_str = get_financial_indicators.invoke({
            "ts_code": TEST_SYMBOL,
            "report_type": "annual",
            "periods": 2
        })
        result = json.loads(result_str)
        
        assert result.get("success") is not None, "返回结果应包含 success 字段"
        if result.get("success"):
            data = result.get("data", {})
            print(f"   数据条数: {data.get('meta', {}).get('total_rows', 0)}")
            if data.get("core"):
                core = data["core"]
                print(f"   核心指标: ROE={core.get('roe', 'N/A')}, ROA={core.get('roe_dt', 'N/A')}")
        else:
            print(f"   获取失败: {result.get('message', '')}")
        
        log_test("get_financial_indicators", result.get("success", False),
                result.get("message", "")[:100] if not result.get("success") else "")
    except Exception as e:
        log_test("get_financial_indicators", False, str(e))
        import traceback
        traceback.print_exc()
    
    # 4. 测试 get_valuation_indicators
    try:
        print("\n[4] 测试 get_valuation_indicators(ts_code, include_market_comparison=False)")
        result_str = get_valuation_indicators.invoke({
            "ts_code": TEST_SYMBOL,
            "include_market_comparison": False
        })
        result = json.loads(result_str)
        
        assert result.get("success") is not None, "返回结果应包含 success 字段"
        if result.get("success"):
            data = result.get("data", {})
            if data.get("core"):
                core = data["core"]
                print(f"   PE: {core.get('pe', 'N/A')}, PB: {core.get('pb', 'N/A')}")
                print(f"   最新价格: {core.get('close', 'N/A')}")
        else:
            print(f"   获取失败: {result.get('message', '')}")
        
        log_test("get_valuation_indicators", result.get("success", False),
                result.get("message", "")[:100] if not result.get("success") else "")
    except Exception as e:
        log_test("get_valuation_indicators", False, str(e))
        import traceback
        traceback.print_exc()
    
    # 5. 测试 get_earnings_data
    try:
        print("\n[5] 测试 get_earnings_data(ts_code, limit=5)")
        result_str = get_earnings_data.invoke({
            "ts_code": TEST_SYMBOL,
            "limit": 5
        })
        result = json.loads(result_str)
        
        assert result.get("success") is not None, "返回结果应包含 success 字段"
        if result.get("success"):
            data = result.get("data", {})
            summary = result.get("summary", {})
            print(f"   业绩预告: {summary.get('forecast_count', 0)} 条")
            print(f"   业绩快报: {summary.get('express_count', 0)} 条")
            if data.get("errors"):
                print(f"   警告: {data.get('errors')}")
        else:
            print(f"   获取失败: {result.get('message', '')}")
        
        log_test("get_earnings_data", result.get("success", False),
                result.get("message", "")[:100] if not result.get("success") else "")
    except Exception as e:
        log_test("get_earnings_data", False, str(e))
        import traceback
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
        for tool_name, success, message in test_results:
            if not success:
                print(f"  ❌ {tool_name}")
                if message:
                    print(f"     原因: {message}")
    
    print("\n" + "=" * 80)


def main():
    """主函数"""
    print("=" * 80)
    print(" tradingagents/tools 工具函数测试")
    print("=" * 80)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试股票: {TEST_SYMBOL} ({TEST_TS_CODE})")
    print("\n注意: 部分工具可能需要网络连接和有效的 API Token")
    
    # 测试各个工具模块
    test_market_tools()
    test_technical_tools()
    test_news_tools()
    test_fundamentals_tools()
    
    # 打印总结
    print_summary()


if __name__ == "__main__":
    main()

