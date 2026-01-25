"""测试 get_stock_data 工具"""
import os
import sys
from datetime import datetime, timedelta

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tradingagents.tool_nodes.utils.market_tools import get_stock_data


def test_get_stock_data():
    """测试获取股票数据"""
    print("=" * 60)
    print("测试 get_stock_data 工具")
    print("=" * 60)
    
    # 计算日期范围（最近30天）
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    
    print(f"\n测试参数:")
    print(f"  股票代码: 000001 (平安银行)")
    print(f"  开始日期: {start_str}")
    print(f"  结束日期: {end_str}")
    print()
    
    try:
        result = get_stock_data(
            ts_code="000001",
            start_date=start_str,
            end_date=end_str
        )
        
        print("工具返回结果:")
        print(result)
        print()
        
        # 解析 JSON 验证格式
        import json
        data = json.loads(result)
        
        if data.get('success'):
            print("✅ 测试成功！")
            print(f"   获取到 {data.get('summary', {}).get('total_records', 0)} 条数据")
            if data.get('summary', {}).get('latest_price'):
                latest = data['summary']['latest_price']
                print(f"   最新收盘价: {latest.get('close')}")
                print(f"   最新涨跌幅: {latest.get('pct_chg')}%")
        else:
            print("❌ 测试失败:")
            print(f"   错误信息: {data.get('message', '未知错误')}")
            
    except Exception as e:
        print(f"❌ 测试异常: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_get_stock_data()

