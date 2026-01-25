"""
测试脚本：为 _query_history_report 插入测试数据

插入过去 7 天的 Fundamentals Analyst 报告数据到 analyst_reports 表。
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
base_date = datetime(2026, 1, 18)  # 假设今天是 2024-12-15

# 准备过去 7 天的测试数据
# 每条记录包含单个报告内容（不是汇总的历史摘要）
test_reports = [
    {
        "date": base_date - timedelta(days=6),
        "content": """**分析周期**: 第 1 周

### 财务指标变化趋势

1. **盈利能力**: 开始改善
   - ROE 从 13.0% 提升至 13.5%
   - 净利润率开始回升

2. **成长性**: 保持稳定
   - 营收增长率维持在 14% 左右
   - 净利润增长平稳

3. **财务健康**: 保持稳定
   - 资产负债率控制在合理范围
   - 现金流状况良好

### 基本面评估

**核心优势**:
- 盈利能力开始改善
- 财务结构稳健
- 行业地位稳固

**关注点**:
- 需关注行业周期变化
- 估值水平需要进一步评估"""
    },
    {
        "date": base_date - timedelta(days=5),
        "content": """**分析周期**: 第 2 周

### 财务指标变化趋势

1. **盈利能力**: 持续改善
   - ROE 从 13.5% 提升至 14.0%
   - 净利润率稳步提升

2. **成长性**: 保持强劲
   - 营收增长率提升至 14.5%
   - 净利润增长加速

3. **财务健康**: 保持稳定
   - 资产负债率控制在合理范围
   - 现金流状况良好

### 基本面评估

**核心优势**:
- 盈利能力和成长性双重驱动
- 财务结构稳健，抗风险能力强
- 行业地位稳固

**关注点**:
- 需关注行业周期变化"""
    },
    {
        "date": base_date - timedelta(days=4),
        "content": """**分析周期**: 第 3 周

### 财务指标变化趋势

1. **盈利能力**: 持续改善
   - ROE 从 14.0% 提升至 14.5%
   - 净利润率稳步提升

2. **成长性**: 保持强劲
   - 营收增长率维持在 15% 以上
   - 净利润增长加速

3. **财务健康**: 保持稳定
   - 资产负债率控制在合理范围
   - 现金流状况良好

### 基本面评估

**核心优势**:
- 盈利能力和成长性双重驱动
- 财务结构稳健，抗风险能力强
- 行业地位稳固

**长期展望**:
- 基本面持续改善趋势明确"""
    },
    {
        "date": base_date - timedelta(days=3),
        "content": """**分析周期**: 第 4 周

### 财务指标变化趋势

1. **盈利能力**: 持续改善
   - ROE 从 14.5% 提升至 15.0%
   - 净利润率稳步提升

2. **成长性**: 保持强劲
   - 营收增长率维持在 15% 以上
   - 净利润增长加速

3. **财务健康**: 保持稳定
   - 资产负债率控制在合理范围
   - 现金流状况良好

### 基本面评估

**核心优势**:
- 盈利能力和成长性双重驱动
- 财务结构稳健，抗风险能力强
- 行业地位稳固

**长期展望**:
- 基本面持续改善趋势明确
- 估值水平合理，具备投资价值"""
    },
    {
        "date": base_date - timedelta(days=2),
        "content": """**分析周期**: 第 5 周

### 财务指标变化趋势

1. **盈利能力**: 持续改善
   - ROE 从 15.0% 提升至 15.2%
   - 净利润率稳步提升

2. **成长性**: 保持强劲
   - 营收增长率维持在 15% 以上
   - 净利润增长加速

3. **财务健康**: 保持稳定
   - 资产负债率控制在合理范围
   - 现金流状况良好

### 基本面评估

**核心优势**:
- 盈利能力和成长性双重驱动
- 财务结构稳健，抗风险能力强
- 行业地位稳固

**长期展望**:
- 基本面持续改善趋势明确
- 估值水平合理，具备投资价值
- 需关注行业周期变化"""
    },
    {
        "date": base_date - timedelta(days=1),
        "content": """**分析周期**: 第 6 周

### 财务指标变化趋势

1. **盈利能力**: 持续改善
   - ROE 从 15.0% 提升至 15.2%
   - 净利润率稳步提升

2. **成长性**: 保持强劲
   - 营收增长率维持在 15% 以上
   - 净利润增长加速

3. **财务健康**: 保持稳定
   - 资产负债率控制在合理范围
   - 现金流状况良好

### 基本面评估

**核心优势**:
- 盈利能力和成长性双重驱动
- 财务结构稳健，抗风险能力强
- 行业地位稳固

**长期展望**:
- 基本面持续改善趋势明确
- 估值水平合理，具备投资价值
- 需关注行业周期变化"""
    },
    {
        "date": base_date,
        "content": """**分析周期**: 第 7 周

### 财务指标变化趋势

1. **盈利能力**: 持续改善
   - ROE 从 13.5% 提升至 15.2%
   - 净利润率稳步提升

2. **成长性**: 保持强劲
   - 营收增长率维持在 15% 以上
   - 净利润增长加速

3. **财务健康**: 保持稳定
   - 资产负债率控制在合理范围
   - 现金流状况良好

### 基本面评估

**核心优势**:
- 盈利能力和成长性双重驱动
- 财务结构稳健，抗风险能力强
- 行业地位稳固

**长期展望**:
- 基本面持续改善趋势明确
- 估值水平合理，具备投资价值
- 需关注行业周期变化"""
    },
]

# 插入数据
insert_sql = """
INSERT INTO analyst_reports (analyst_type, symbol, trade_date, report_content)
VALUES (?, ?, ?, ?)
"""

print(f"开始插入测试数据到数据库...")
print(f"股票代码: {symbol}")
print(f"基础日期: {base_date.strftime('%Y-%m-%d')}")
print(f"将插入 {len(test_reports)} 条记录\n")

for i, report in enumerate(test_reports, 1):
    trade_date = report["date"].strftime("%Y-%m-%d")
    content = report["content"]
    
    try:
        cursor.execute(
            insert_sql,
            ("fundamentals", symbol, trade_date, content)
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
    WHERE analyst_type='fundamentals' AND symbol=?
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
print(f"  WHERE analyst_type='fundamentals' AND symbol='{symbol}'")
print(f"  AND trade_date <= '{base_date.strftime('%Y-%m-%d')}'")
print(f"  AND trade_date >= date('{base_date.strftime('%Y-%m-%d')}', '-7 days')")
print(f"  ORDER BY trade_date ASC;")
