"""
测试脚本：为 _query_history_report 插入测试数据

插入过去 7 天的 Sentiment Analyst 报告数据到 analyst_reports 表。
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
        "content": """## 今日情绪概览

**整体情绪**: 中性偏乐观 (Sentiment Score: 0.52)

### 情绪分布

- **正面情绪**: 52%
- **中性情绪**: 35%
- **负面情绪**: 13%

### 主要讨论话题

1. **市场表现**
   - 用户对近期股价表现反应中性
   - 部分用户关注市场波动

2. **公司动态**
   - 对公司运营情况讨论较少
   - 多数观点保持观望

3. **行业动态**
   - 对行业政策关注度一般
   - 讨论热度较低

## 情绪指标

- **情绪强度**: 中等
- **讨论热度**: 一般
- **意见分歧**: 较小（多数保持中性）"""
    },
    {
        "date": base_date - timedelta(days=5),
        "content": """## 今日情绪概览

**整体情绪**: 偏乐观 (Sentiment Score: 0.58)

### 情绪分布

- **正面情绪**: 58%
- **中性情绪**: 30%
- **负面情绪**: 12%

### 主要讨论话题

1. **市场表现**
   - 用户对近期股价上涨表示认可
   - 部分用户开始关注投资机会

2. **公司动态**
   - 对公司业务发展讨论增加
   - 多数观点偏向积极

3. **行业动态**
   - 对行业政策关注度提升
   - 讨论热度有所增加

## 情绪指标

- **情绪强度**: 中等偏高
- **讨论热度**: 较高
- **意见分歧**: 较小（多数偏乐观）"""
    },
    {
        "date": base_date - timedelta(days=4),
        "content": """## 今日情绪概览

**整体情绪**: 偏乐观 (Sentiment Score: 0.62)

### 情绪分布

- **正面情绪**: 62%
- **中性情绪**: 28%
- **负面情绪**: 10%

### 主要讨论话题

1. **业绩预期**
   - 用户对业绩预期反应积极
   - 多数观点认为公司前景良好

2. **市场表现**
   - 对近期股价上涨表示认可
   - 部分用户关注回调风险

3. **行业动态**
   - 对行业政策利好表示期待
   - 关注同行业公司表现

## 情绪指标

- **情绪强度**: 中等偏高
- **讨论热度**: 较高
- **意见分歧**: 较小（多数偏乐观）"""
    },
    {
        "date": base_date - timedelta(days=3),
        "content": """## 今日情绪概览

**整体情绪**: 偏乐观 (Sentiment Score: 0.64)

### 情绪分布

- **正面情绪**: 64%
- **中性情绪**: 26%
- **负面情绪**: 10%

### 主要讨论话题

1. **业绩预期**
   - 用户对业绩预告反应积极
   - 多数观点认为公司前景良好

2. **市场表现**
   - 对近期股价上涨表示认可
   - 部分用户关注回调风险

3. **行业动态**
   - 对行业政策利好表示期待
   - 关注同行业公司表现

## 情绪指标

- **情绪强度**: 中等偏高
- **讨论热度**: 较高
- **意见分歧**: 较小（多数偏乐观）"""
    },
    {
        "date": base_date - timedelta(days=2),
        "content": """## 今日情绪概览

**整体情绪**: 偏乐观 (Sentiment Score: 0.65)

### 情绪分布

- **正面情绪**: 65%
- **中性情绪**: 25%
- **负面情绪**: 10%

### 主要讨论话题

1. **业绩预期**
   - 用户对业绩预告反应积极
   - 多数观点认为公司前景良好

2. **市场表现**
   - 对近期股价上涨表示认可
   - 部分用户关注回调风险

3. **行业动态**
   - 对行业政策利好表示期待
   - 关注同行业公司表现

## 情绪指标

- **情绪强度**: 中等偏高
- **讨论热度**: 较高
- **意见分歧**: 较小（多数偏乐观）"""
    },
    {
        "date": base_date - timedelta(days=1),
        "content": """## 今日情绪概览

**整体情绪**: 偏乐观 (Sentiment Score: 0.65)

### 情绪分布

- **正面情绪**: 65%
- **中性情绪**: 25%
- **负面情绪**: 10%

### 主要讨论话题

1. **业绩预期**
   - 用户对业绩预告反应积极
   - 多数观点认为公司前景良好

2. **市场表现**
   - 对近期股价上涨表示认可
   - 部分用户关注回调风险

3. **行业动态**
   - 对行业政策利好表示期待
   - 关注同行业公司表现

## 情绪指标

- **情绪强度**: 中等偏高
- **讨论热度**: 较高
- **意见分歧**: 较小（多数偏乐观）"""
    },
    {
        "date": base_date,
        "content": """## 今日情绪概览

**整体情绪**: 偏乐观 (Sentiment Score: 0.65)

### 情绪分布

- **正面情绪**: 65%
- **中性情绪**: 25%
- **负面情绪**: 10%

### 主要讨论话题

1. **业绩预期**
   - 用户对业绩预告反应积极
   - 多数观点认为公司前景良好

2. **市场表现**
   - 对近期股价上涨表示认可
   - 部分用户关注回调风险

3. **行业动态**
   - 对行业政策利好表示期待
   - 关注同行业公司表现

## 情绪指标

- **情绪强度**: 中等偏高
- **讨论热度**: 较高
- **意见分歧**: 较小（多数偏乐观）"""
    },
]

# 插入数据
insert_sql = """
INSERT INTO analyst_reports (analyst_type, symbol, trade_date, report_content)
VALUES (?, ?, ?, ?)
"""

print(f"开始插入测试数据到数据库...")
print(f"分析师类型: sentiment")
print(f"股票代码: {symbol}")
print(f"基础日期: {base_date.strftime('%Y-%m-%d')}")
print(f"将插入 {len(test_reports)} 条记录\n")

for i, report in enumerate(test_reports, 1):
    trade_date = report["date"].strftime("%Y-%m-%d")
    content = report["content"]
    
    try:
        cursor.execute(
            insert_sql,
            ("sentiment", symbol, trade_date, content)
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
    WHERE analyst_type='sentiment' AND symbol=?
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
print(f"  WHERE analyst_type='sentiment' AND symbol='{symbol}'")
print(f"  AND trade_date <= '{base_date.strftime('%Y-%m-%d')}'")
print(f"  AND trade_date >= date('{base_date.strftime('%Y-%m-%d')}', '-7 days')")
print(f"  ORDER BY trade_date ASC;")
