"""
测试脚本：为 _query_history_report 插入测试数据

插入过去 7 天的 News Analyst 报告数据到 analyst_reports 表。
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
        "content": """## 今日新闻摘要

### 重要新闻

1. **行业政策动态**
   - 行业监管政策保持稳定
   - 市场预期政策环境良好

2. **公司动态**
   - 公司运营正常，无重大公告
   - 业务发展平稳

3. **市场动态**
   - 板块整体表现平稳
   - 市场关注度一般

## 新闻影响分析

**正面影响**: 
- 政策环境稳定有利于长期发展
- 公司运营稳健

**风险提示**:
- 需关注行业政策变化
- 注意市场情绪波动"""
    },
    {
        "date": base_date - timedelta(days=5),
        "content": """## 今日新闻摘要

### 重要新闻

1. **行业政策动态**
   - 行业政策环境持续稳定
   - 市场对政策预期积极

2. **公司动态**
   - 公司发布常规业务公告
   - 业务发展符合预期

3. **市场动态**
   - 板块整体表现良好
   - 市场关注度有所提升

## 新闻影响分析

**正面影响**: 
- 政策环境稳定带来信心
- 公司业务发展正常

**风险提示**:
- 需关注后续政策细节
- 注意市场情绪变化"""
    },
    {
        "date": base_date - timedelta(days=4),
        "content": """## 今日新闻摘要

### 重要新闻

1. **行业政策利好**
   - 相关部门发布支持政策信号
   - 市场预期政策将利好行业发展

2. **公司公告**
   - 公司发布业务进展公告
   - 业务发展稳步推进

3. **市场动态**
   - 同行业公司表现良好
   - 板块整体上涨，市场关注度提升

## 新闻影响分析

**正面影响**: 
- 政策支持信号带来积极预期
- 公司业务发展符合预期

**风险提示**:
- 需关注政策具体执行情况
- 注意市场情绪波动"""
    },
    {
        "date": base_date - timedelta(days=3),
        "content": """## 今日新闻摘要

### 重要新闻

1. **行业政策利好**
   - 相关部门发布支持政策，利好行业发展
   - 预计将推动相关公司业绩提升

2. **公司公告**
   - 公司发布业绩预告，预计净利润增长 15%
   - 业务发展获得市场关注

3. **市场动态**
   - 同行业公司表现强劲，板块整体上涨
   - 机构调研频繁，市场关注度提升

## 新闻影响分析

**正面影响**: 
- 政策支持带来长期利好
- 业绩预期改善市场信心

**风险提示**:
- 需关注政策执行细节
- 注意市场情绪波动"""
    },
    {
        "date": base_date - timedelta(days=2),
        "content": """## 今日新闻摘要

### 重要新闻

1. **行业政策利好**
   - 相关部门发布支持政策，利好行业发展
   - 预计将推动相关公司业绩提升

2. **公司公告**
   - 公司发布业绩预告，预计净利润增长 18%
   - 新产品发布获得市场关注

3. **市场动态**
   - 同行业公司表现强劲，板块整体上涨
   - 机构调研频繁，市场关注度提升

## 新闻影响分析

**正面影响**: 
- 政策支持带来长期利好
- 业绩预期改善市场信心

**风险提示**:
- 需关注政策执行细节
- 注意市场情绪波动"""
    },
    {
        "date": base_date - timedelta(days=1),
        "content": """## 今日新闻摘要

### 重要新闻

1. **行业政策利好**
   - 相关部门发布支持政策，利好行业发展
   - 预计将推动相关公司业绩提升

2. **公司公告**
   - 公司发布业绩预告，预计净利润增长 20%
   - 新产品发布获得市场关注

3. **市场动态**
   - 同行业公司表现强劲，板块整体上涨
   - 机构调研频繁，市场关注度提升

## 新闻影响分析

**正面影响**: 
- 政策支持带来长期利好
- 业绩预期改善市场信心

**风险提示**:
- 需关注政策执行细节
- 注意市场情绪波动"""
    },
    {
        "date": base_date,
        "content": """## 今日新闻摘要

### 重要新闻

1. **行业政策利好**
   - 相关部门发布支持政策，利好行业发展
   - 预计将推动相关公司业绩提升

2. **公司公告**
   - 公司发布业绩预告，预计净利润增长 20%
   - 新产品发布获得市场关注

3. **市场动态**
   - 同行业公司表现强劲，板块整体上涨
   - 机构调研频繁，市场关注度提升

## 新闻影响分析

**正面影响**: 
- 政策支持带来长期利好
- 业绩预期改善市场信心

**风险提示**:
- 需关注政策执行细节
- 注意市场情绪波动"""
    },
]

# 插入数据
insert_sql = """
INSERT INTO analyst_reports (analyst_type, symbol, trade_date, report_content)
VALUES (?, ?, ?, ?)
"""

print(f"开始插入测试数据到数据库...")
print(f"分析师类型: news")
print(f"股票代码: {symbol}")
print(f"基础日期: {base_date.strftime('%Y-%m-%d')}")
print(f"将插入 {len(test_reports)} 条记录\n")

for i, report in enumerate(test_reports, 1):
    trade_date = report["date"].strftime("%Y-%m-%d")
    content = report["content"]
    
    try:
        cursor.execute(
            insert_sql,
            ("news", symbol, trade_date, content)
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
    WHERE analyst_type='news' AND symbol=?
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
print(f"  WHERE analyst_type='news' AND symbol='{symbol}'")
print(f"  AND trade_date <= '{base_date.strftime('%Y-%m-%d')}'")
print(f"  AND trade_date >= date('{base_date.strftime('%Y-%m-%d')}', '-7 days')")
print(f"  ORDER BY trade_date ASC;")
