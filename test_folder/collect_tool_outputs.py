#!/usr/bin/env python3
"""
收集所有工具的样例返回，用于文档记录

运行此脚本会调用所有工具并保存返回结果到 tool_outputs.json
"""
import os
import sys
import json
from datetime import datetime, timedelta

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

# 测试用的股票代码
TEST_SYMBOL = "600519"  # 贵州茅台
TEST_TS_CODE = "600519.SH"

# 收集结果
outputs = {
    "collection_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "test_symbol": TEST_SYMBOL,
    "tools": {}
}


def collect_tool_output(tool_name, tool_func, invoke_params):
    """收集工具输出"""
    print(f"\n收集 {tool_name} 的输出...")
    try:
        result_str = tool_func.invoke(invoke_params)
        result_json = json.loads(result_str)
        
        outputs["tools"][tool_name] = {
            "success": result_json.get("success", False),
            "params": invoke_params,
            "output": result_json,
            "output_length": len(result_str),
            "note": "实际返回为 JSON 字符串，这里展示解析后的内容"
        }
        
        print(f"  [PASS] {tool_name}: 成功")
        if result_json.get("success"):
            print(f"    数据条数/内容: {len(result_json.get('data', [])) if isinstance(result_json.get('data'), list) else 'N/A'}")
        else:
            print(f"    错误: {result_json.get('message', '')[:100]}")
            
    except Exception as e:
        outputs["tools"][tool_name] = {
            "success": False,
            "params": invoke_params,
            "error": str(e),
            "note": "工具调用失败"
        }
        print(f"  [FAIL] {tool_name}: {str(e)}")


def main():
    """主函数"""
    print("=" * 80)
    print(" 收集工具输出样例")
    print("=" * 80)
    
    # 计算日期
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")
    
    # 1. 市场数据工具
    collect_tool_output(
        "get_stock_data",
        get_stock_data,
        {
            "ts_code": TEST_TS_CODE,
            "start_date": start_date_str,
            "end_date": end_date_str
        }
    )
    
    # 2. 技术分析工具
    collect_tool_output(
        "get_indicators",
        get_indicators,
        {
            "ts_code": TEST_TS_CODE,
            "indicators": "MA,RSI",
            "period": 30
        }
    )
    
    # 3. 新闻工具
    collect_tool_output(
        "get_news",
        get_news,
        {
            "ts_code": TEST_SYMBOL,
            "days": 7,
            "limit": 5
        }
    )
    
    collect_tool_output(
        "get_global_news",
        get_global_news,
        {
            "days": 7,
            "limit": 5
        }
    )
    
    # 4. 基本面分析工具
    collect_tool_output(
        "get_company_info",
        get_company_info,
        {
            "ts_code": TEST_SYMBOL
        }
    )
    
    collect_tool_output(
        "get_financial_statements",
        get_financial_statements,
        {
            "ts_code": TEST_SYMBOL,
            "report_type": "annual",
            "periods": 2
        }
    )
    
    collect_tool_output(
        "get_financial_indicators",
        get_financial_indicators,
        {
            "ts_code": TEST_SYMBOL,
            "report_type": "annual",
            "periods": 2
        }
    )
    
    collect_tool_output(
        "get_valuation_indicators",
        get_valuation_indicators,
        {
            "ts_code": TEST_SYMBOL,
            "include_market_comparison": False
        }
    )
    
    collect_tool_output(
        "get_earnings_data",
        get_earnings_data,
        {
            "ts_code": TEST_SYMBOL,
            "limit": 5
        }
    )
    
    # 保存结果
    output_file = os.path.join(ROOT, "test_folder", "tool_outputs.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(outputs, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\n{'=' * 80}")
    print(f" 收集完成！结果已保存到: {output_file}")
    print(f"{'=' * 80}")
    
    # 统计
    success_count = sum(1 for t in outputs["tools"].values() if t.get("success"))
    total_count = len(outputs["tools"])
    print(f"\n成功: {success_count}/{total_count}")


if __name__ == "__main__":
    main()

