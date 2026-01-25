"""测试 get_indicators 工具"""
import os
import sys
import json
from datetime import datetime, timedelta

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tradingagents.tool_nodes.utils.technical_tools import get_indicators


def save_json_result(data: dict, filename: str):
    """保存 JSON 结果到文件"""
    output_dir = "test_results"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    filepath = os.path.join(output_dir, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    print(f"   结果已保存到: {filepath}")


def test_get_indicators():
    """测试获取技术指标"""
    print("=" * 60)
    print("测试 get_indicators 工具")
    print("=" * 60)
    
    # 测试 1: 基本指标（MA, RSI）
    print("\n测试 1: 计算 MA 和 RSI 指标")
    print("-" * 60)
    result1 = get_indicators.invoke({
        "ts_code": "000001",
        "indicators": "MA,RSI",
        "period": 60
    })
    print("结果:")
    data1 = json.loads(result1)
    if data1.get('success'):
        print(f"✅ 成功计算 {len(data1.get('indicators', []))} 个指标")
        print(f"   数据条数: {data1.get('summary', {}).get('total_records', 0)}")
        if data1.get('summary', {}).get('latest_indicators'):
            latest = data1['summary']['latest_indicators']
            print(f"   最新指标值:")
            for key, value in list(latest.items())[:5]:  # 只显示前5个
                print(f"     {key}: {value:.2f}" if isinstance(value, float) else f"     {key}: {value}")
        # 保存 JSON 文件
        save_json_result(data1, "test1_ma_rsi.json")
    else:
        print(f"❌ 失败: {data1.get('message', '未知错误')}")
        save_json_result(data1, "test1_ma_rsi_error.json")
    
    # 测试 2: MACD 和 BOLL
    print("\n测试 2: 计算 MACD 和 BOLL 指标")
    print("-" * 60)
    result2 = get_indicators.invoke({
        "ts_code": "000001",
        "indicators": "MACD,BOLL",
        "start_date": "20250101",
        "end_date": "20250131"
    })
    data2 = json.loads(result2)
    if data2.get('success'):
        print(f"✅ 成功计算 {len(data2.get('indicators', []))} 个指标")
        print(f"   数据条数: {data2.get('summary', {}).get('total_records', 0)}")
        # 保存 JSON 文件
        save_json_result(data2, "test2_macd_boll.json")
    else:
        print(f"❌ 失败: {data2.get('message', '未知错误')}")
        save_json_result(data2, "test2_macd_boll_error.json")
    
    # 测试 3: 所有指标
    print("\n测试 3: 计算所有支持的指标")
    print("-" * 60)
    result3 = get_indicators.invoke({
        "ts_code": "000001",
        "indicators": "MA,EMA,RSI,MACD,BOLL,KDJ,OBV",
        "period": 30
    })
    data3 = json.loads(result3)
    if data3.get('success'):
        print(f"✅ 成功计算 {len(data3.get('indicators', []))} 个指标")
        print(f"   指标列表: {', '.join(data3.get('indicators', []))}")
        print(f"   数据条数: {data3.get('summary', {}).get('total_records', 0)}")
        # 保存 JSON 文件
        save_json_result(data3, "test3_all_indicators.json")
    else:
        print(f"❌ 失败: {data3.get('message', '未知错误')}")
        save_json_result(data3, "test3_all_indicators_error.json")
    
    print("\n" + "=" * 60)
    print("所有测试完成！JSON 文件已保存到 test_results/ 目录")
    print("=" * 60)


if __name__ == "__main__":
    test_get_indicators()

