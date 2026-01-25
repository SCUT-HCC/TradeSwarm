"""
测试脚本：为 _query_history_report 插入测试数据

插入过去 7 天的 Market Analyst 报告数据到 analyst_reports 表。
"""

import sqlite3
from datetime import datetime, timedelta

# 连接数据库
conn = sqlite3.connect("memory.db")
cursor = conn.cursor()

# 确保表存在
create_table_sql = """
CREATE TABLE IF NOT EXISTS analyst_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analyst_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    report_content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
cursor.execute(create_table_sql)
conn.commit()

# 测试参数
symbol = "000001"
base_date = datetime(2026, 1, 18)  # 假设今天是 2026-01-18

# 准备过去 7 天的测试数据
# 每条记录包含单个报告内容（不是汇总的历史摘要）
test_reports = [
    {
        "date": base_date - timedelta(days=6),
        "content": """## 当前市场快照

**股票代码**: {symbol}
**交易日期**: {trade_date}
**最新价格**: 14.50 元
**涨跌幅**: +1.2%
**成交量**: 980,000 手

## 技术指标

- **MA5**: 14.35
- **MA10**: 14.20
- **RSI**: 52.3
- **MACD**: 中性信号

## 市场情绪

当前市场情绪平稳，成交量正常，技术指标显示震荡整理态势。""".format(symbol=symbol, trade_date=(base_date - timedelta(days=6)).strftime("%Y-%m-%d"))
    },
    {
        "date": base_date - timedelta(days=5),
        "content": """## 当前市场快照

**股票代码**: {symbol}
**交易日期**: {trade_date}
**最新价格**: 14.75 元
**涨跌幅**: +1.7%
**成交量**: 1,100,000 手

## 技术指标

- **MA5**: 14.50
- **MA10**: 14.30
- **RSI**: 55.2
- **MACD**: 金叉信号初现

## 市场情绪

当前市场情绪略有改善，成交量放大，技术指标显示上涨动能开始积累。""".format(symbol=symbol, trade_date=(base_date - timedelta(days=5)).strftime("%Y-%m-%d"))
    },
    {
        "date": base_date - timedelta(days=4),
        "content": """## 当前市场快照

**股票代码**: {symbol}
**交易日期**: {trade_date}
**最新价格**: 14.95 元
**涨跌幅**: +1.4%
**成交量**: 1,150,000 手

## 技术指标

- **MA5**: 14.65
- **MA10**: 14.45
- **RSI**: 57.8
- **MACD**: 金叉信号确认

## 市场情绪

当前市场情绪继续改善，成交量持续放大，技术指标显示上涨动能增强。""".format(symbol=symbol, trade_date=(base_date - timedelta(days=4)).strftime("%Y-%m-%d"))
    },
    {
        "date": base_date - timedelta(days=3),
        "content": """## 当前市场快照

**股票代码**: {symbol}
**交易日期**: {trade_date}
**最新价格**: 15.10 元
**涨跌幅**: +1.0%
**成交量**: 1,200,000 手

## 技术指标

- **MA5**: 14.80
- **MA10**: 14.60
- **RSI**: 59.5
- **MACD**: 金叉信号强化

## 市场情绪

当前市场情绪偏乐观，成交量放大，技术指标显示上涨动能持续。""".format(symbol=symbol, trade_date=(base_date - timedelta(days=3)).strftime("%Y-%m-%d"))
    },
    {
        "date": base_date - timedelta(days=2),
        "content": """## 当前市场快照

**股票代码**: {symbol}
**交易日期**: {trade_date}
**最新价格**: 15.18 元
**涨跌幅**: +0.5%
**成交量**: 1,180,000 手

## 技术指标

- **MA5**: 14.92
- **MA10**: 14.72
- **RSI**: 58.2
- **MACD**: 金叉信号维持

## 市场情绪

当前市场情绪偏乐观，成交量保持活跃，技术指标显示上涨动能稳定。""".format(symbol=symbol, trade_date=(base_date - timedelta(days=2)).strftime("%Y-%m-%d"))
    },
    {
        "date": base_date - timedelta(days=1),
        "content": """## 当前市场快照

**股票代码**: {symbol}
**交易日期**: {trade_date}
**最新价格**: 15.20 元
**涨跌幅**: +0.1%
**成交量**: 1,200,000 手

## 技术指标

- **MA5**: 14.98
- **MA10**: 14.75
- **RSI**: 58.5
- **MACD**: 金叉信号

## 市场情绪

当前市场情绪偏乐观，成交量放大，技术指标显示上涨动能。""".format(symbol=symbol, trade_date=(base_date - timedelta(days=1)).strftime("%Y-%m-%d"))
    },
    {
        "date": base_date,
        "content": """## 当前市场快照

**股票代码**: {symbol}
**交易日期**: {trade_date}
**最新价格**: 15.23 元
**涨跌幅**: +2.5%
**成交量**: 1,234,567 手

## 技术指标

- **MA5**: 14.98
- **MA10**: 14.75
- **RSI**: 58.5
- **MACD**: 金叉信号

## 市场情绪

当前市场情绪偏乐观，成交量放大，技术指标显示上涨动能。""".format(symbol=symbol, trade_date=base_date.strftime("%Y-%m-%d"))
    },
]

# 插入数据
insert_sql = """
INSERT INTO analyst_reports (analyst_type, symbol, trade_date, report_content)
VALUES (?, ?, ?, ?)
"""

print(f"开始插入测试数据到数据库...")
print(f"分析师类型: market")
print(f"股票代码: {symbol}")
print(f"基础日期: {base_date.strftime('%Y-%m-%d')}")
print(f"将插入 {len(test_reports)} 条记录\n")

for i, report in enumerate(test_reports, 1):
    trade_date = report["date"].strftime("%Y-%m-%d")
    content = report["content"]
    
    try:
        cursor.execute(
            insert_sql,
            ("market", symbol, trade_date, content)
        )
        print(f"[{i}/{len(test_reports)}] 成功插入: {trade_date}")
    except Exception as e:
        print(f"[{i}/{len(test_reports)}] 插入失败 {trade_date}: {e}")

# 提交事务
conn.commit()

# 验证插入的数据
print("\n验证插入的数据:")
print("-" * 60)
cursor.execute("""
    SELECT trade_date, LENGTH(report_content) as content_length
    FROM analyst_reports
    WHERE analyst_type='market' AND symbol=?
    ORDER BY trade_date ASC
""", (symbol,))

results = cursor.fetchall()
print(f"共查询到 {len(results)} 条记录:")
for row in results:
    print(f"  日期: {row[0]}, 报告长度: {row[1]} 字符")

# 关闭连接
cursor.close()
conn.close()

print("\n测试数据插入完成！")
print(f"\n提示: 可以使用以下 SQL 查询测试数据:")
print(f"  SELECT trade_date, report_content FROM analyst_reports")
print(f"  WHERE analyst_type='market' AND symbol='{symbol}'")
print(f"  AND trade_date <= '{base_date.strftime('%Y-%m-%d')}'")
print(f"  AND trade_date >= date('{base_date.strftime('%Y-%m-%d')}', '-7 days')")
print(f"  ORDER BY trade_date ASC;")
