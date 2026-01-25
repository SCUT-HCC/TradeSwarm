"""
测试脚本：测试 _query_history_report 函数

验证从数据库查询历史报告的功能是否正常工作。
"""

import sqlite3
from tradingagents.agents.pre_open.summary.fundamentals_summary.node import _query_history_report

# 连接数据库
conn = sqlite3.connect("memory.db")

# 测试参数
symbol = "000001"
trade_date = "2024-12-15"
trading_session = "post_close"  # 或 "pre_open"

print("=" * 60)
print("测试 _query_history_report 函数")
print("=" * 60)
print(f"股票代码: {symbol}")
print(f"交易日期: {trade_date}")
print(f"交易时段: {trading_session}")
print("-" * 60)

# 调用函数
try:
    result = _query_history_report(conn, symbol, trade_date, trading_session)
    
    print("\n查询结果:")
    print("=" * 60)
    print(result)
    print("=" * 60)
    print(f"\n结果长度: {len(result)} 字符")
    
    # 验证结果格式
    if "Fundamentals History Summary" in result:
        print("✓ 包含标题 'Fundamentals History Summary'")
    else:
        print("✗ 缺少标题 'Fundamentals History Summary'")
    
    if "近期基本面趋势分析" in result:
        print("✓ 包含 '近期基本面趋势分析'")
    else:
        print("✗ 缺少 '近期基本面趋势分析'")
    
    if trading_session == "pre_open":
        if "开盘前" in result:
            print("✓ 包含 '开盘前' 描述")
        else:
            print("✗ 缺少 '开盘前' 描述")
    else:
        if "收盘后" in result:
            print("✓ 包含 '收盘后' 描述")
        else:
            print("✗ 缺少 '收盘后' 描述")
    
    # 检查是否包含报告内容
    if "ROE" in result or "盈利能力" in result:
        print("✓ 包含报告内容")
    else:
        print("✗ 缺少报告内容")
    
except Exception as e:
    print(f"\n❌ 查询失败: {e}")
    import traceback
    traceback.print_exc()

# 关闭连接
conn.close()

print("\n测试完成！")
