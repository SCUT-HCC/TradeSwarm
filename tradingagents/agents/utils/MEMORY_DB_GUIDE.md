# Memory DB 交互指南

本指南说明如何与 `memory.db` 数据库交互，包括插入、查询、更新和删除分析师报告。

## 数据库结构

### 表：`analyst_reports`

```sql
CREATE TABLE analyst_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analyst_type TEXT NOT NULL,      -- 'market', 'news', 'sentiment', 'fundamentals'
    symbol TEXT NOT NULL,              -- 股票代码，如 '000001'
    trade_date TEXT NOT NULL,          -- 交易日期，如 '2024-01-15'
    report_content TEXT NOT NULL,      -- 报告内容（文本）
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

## 快速开始

### 方法 1：使用 MemoryDBHelper 类（推荐）

```python
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper

# 使用上下文管理器（自动关闭连接）
with MemoryDBHelper() as db:
    # 插入报告
    db.insert_report(
        analyst_type="market",
        symbol="000001",
        trade_date="2024-01-15",
        report_content="这是市场分析报告内容..."
    )
    
    # 查询今日报告
    report = db.query_today_report("market", "000001", "2024-01-15")
    if report:
        print(report)
    
    # 查询历史报告（过去 7 天）
    history = db.query_history_reports("market", "000001", "2024-01-15", lookback_days=7)
    for h in history:
        print(f"日期: {h['trade_date']}, 内容: {h['report_content'][:50]}...")
    
    # 获取统计信息
    stats = db.get_statistics(symbol="000001")
    print(f"总报告数: {stats['total_reports']}")
```

### 方法 2：使用便捷函数

```python
from tradingagents.agents.utils.memory_db_helper import (
    insert_report,
    query_today_report,
    query_history_reports,
)

# 插入报告
insert_report(
    analyst_type="news",
    symbol="000001",
    trade_date="2024-01-15",
    report_content="这是新闻分析报告..."
)

# 查询今日报告
report = query_today_report("news", "000001", "2024-01-15")

# 查询历史报告
history = query_history_reports("news", "000001", "2024-01-15", lookback_days=7)
```

### 方法 3：直接使用 init_db 的全局连接（不推荐，但兼容旧代码）

```python
from tradingagents.agents.init_db import conn, cursor

# 插入报告
cursor.execute(
    "INSERT INTO analyst_reports (analyst_type, symbol, trade_date, report_content) VALUES (?, ?, ?, ?)",
    ("market", "000001", "2024-01-15", "报告内容...")
)
conn.commit()

# 查询报告
cursor.execute(
    "SELECT report_content FROM analyst_reports WHERE analyst_type=? AND symbol=? AND trade_date=?",
    ("market", "000001", "2024-01-15")
)
result = cursor.fetchone()
if result:
    print(result[0])
```

## 完整 API 参考

### MemoryDBHelper 类

#### `__init__(db_path: str = "memory.db")`

初始化数据库连接。

```python
db = MemoryDBHelper("memory.db")
```

#### `insert_report(analyst_type, symbol, trade_date, report_content) -> bool`

插入分析师报告。

**参数：**
- `analyst_type`: 分析师类型（'market', 'news', 'sentiment', 'fundamentals'）
- `symbol`: 股票代码（如 '000001'）
- `trade_date`: 交易日期（如 '2024-01-15'）
- `report_content`: 报告内容（文本）

**返回：** 插入是否成功（bool）

#### `query_today_report(analyst_type, symbol, trade_date) -> Optional[str]`

查询今日报告（返回最新的一个）。

**参数：**
- `analyst_type`: 分析师类型
- `symbol`: 股票代码
- `trade_date`: 交易日期

**返回：** 报告内容（str），如果不存在返回 None

#### `query_history_reports(analyst_type, symbol, trade_date, lookback_days=7) -> List[Dict]`

查询历史报告。

**参数：**
- `analyst_type`: 分析师类型
- `symbol`: 股票代码
- `trade_date`: 交易日期（基准日期）
- `lookback_days`: 回溯天数，默认 7 天

**返回：** 报告列表，每个元素包含：
- `id`: 报告 ID
- `trade_date`: 交易日期
- `report_content`: 报告内容
- `created_at`: 创建时间

#### `query_all_reports(analyst_type=None, symbol=None, limit=None) -> List[Dict]`

查询所有报告（支持过滤）。

**参数：**
- `analyst_type`: 分析师类型（可选）
- `symbol`: 股票代码（可选）
- `limit`: 限制返回数量（可选）

**返回：** 报告列表

#### `update_report(report_id, report_content) -> bool`

更新报告内容。

**参数：**
- `report_id`: 报告 ID
- `report_content`: 新的报告内容

**返回：** 更新是否成功（bool）

#### `delete_report(report_id) -> bool`

删除报告。

**参数：**
- `report_id`: 报告 ID

**返回：** 删除是否成功（bool）

#### `get_statistics(symbol=None) -> Dict`

获取统计信息。

**参数：**
- `symbol`: 股票代码（可选，如果提供则只统计该股票）

**返回：** 统计信息字典，包含：
- `total_reports`: 总报告数
- `by_type`: 按类型分组的统计信息

#### `close()`

关闭数据库连接。

## 使用场景示例

### 场景 1：插入市场分析报告

```python
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper

with MemoryDBHelper() as db:
    market_report = """
    # 市场分析报告 - 000001
    
    ## 当前市场快照
    - 最新价格: 14.50 元
    - 涨跌幅: +1.2%
    - 成交量: 980,000 手
    
    ## 技术指标
    - MA5: 14.35
    - RSI: 52.3
    - MACD: 中性信号
    """
    
    db.insert_report(
        analyst_type="market",
        symbol="000001",
        trade_date="2024-01-15",
        report_content=market_report
    )
```

### 场景 2：批量插入历史报告

```python
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper
from datetime import datetime, timedelta

with MemoryDBHelper() as db:
    base_date = datetime(2024, 1, 15)
    
    for i in range(7):
        trade_date = (base_date - timedelta(days=i)).strftime("%Y-%m-%d")
        report_content = f"这是 {trade_date} 的报告内容..."
        
        db.insert_report(
            analyst_type="market",
            symbol="000001",
            trade_date=trade_date,
            report_content=report_content
        )
```

### 场景 3：查询并处理历史报告

```python
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper

with MemoryDBHelper() as db:
    # 查询过去 7 天的市场报告
    history = db.query_history_reports(
        analyst_type="market",
        symbol="000001",
        trade_date="2024-01-15",
        lookback_days=7
    )
    
    # 处理历史报告
    for report in history:
        print(f"日期: {report['trade_date']}")
        print(f"内容: {report['report_content'][:100]}...")
        print("-" * 80)
```

### 场景 4：获取统计信息

```python
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper

with MemoryDBHelper() as db:
    # 获取所有股票的统计信息
    all_stats = db.get_statistics()
    print(f"总报告数: {all_stats['total_reports']}")
    
    # 获取特定股票的统计信息
    symbol_stats = db.get_statistics(symbol="000001")
    print(f"000001 的报告数: {symbol_stats['total_reports']}")
    
    # 按类型查看
    for analyst_type, info in symbol_stats['by_type'].items():
        print(f"{analyst_type}: {info['count']} 条报告")
        print(f"  最早日期: {info['earliest_date']}")
        print(f"  最新日期: {info['latest_date']}")
```

### 场景 5：更新报告

```python
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper

with MemoryDBHelper() as db:
    # 先查询报告 ID
    reports = db.query_all_reports(
        analyst_type="market",
        symbol="000001",
        limit=1
    )
    
    if reports:
        report_id = reports[0]['id']
        new_content = "更新后的报告内容..."
        db.update_report(report_id, new_content)
```

### 场景 6：删除报告

```python
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper

with MemoryDBHelper() as db:
    # 查询要删除的报告
    reports = db.query_all_reports(
        analyst_type="market",
        symbol="000001"
    )
    
    # 删除所有报告
    for report in reports:
        db.delete_report(report['id'])
```

## 注意事项

1. **数据库文件位置**：默认数据库文件为项目根目录下的 `memory.db`
2. **连接管理**：推荐使用上下文管理器（`with` 语句）自动管理连接
3. **事务处理**：插入、更新、删除操作会自动提交，失败时会回滚
4. **日期格式**：交易日期使用 `YYYY-MM-DD` 格式（如 '2024-01-15'）
5. **分析师类型**：必须是以下之一：
   - `'market'`：市场分析师
   - `'news'`：新闻分析师
   - `'sentiment'`：情绪分析师
   - `'fundamentals'`：基本面分析师

## 与现有代码的集成

### 在 Summary 节点中使用

Summary 节点可以直接使用 `MemoryDBHelper` 替代直接 SQL 查询：

```python
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper

def create_market_summary_node(conn: Any) -> Callable[[AgentState], Dict[str, Any]]:
    def market_summary_node(state: AgentState) -> Dict[str, Any]:
        symbol = state["company_of_interest"]
        trade_date = state["trade_date"]
        
        # 使用 MemoryDBHelper
        with MemoryDBHelper() as db:
            today_report = db.query_today_report("market", symbol, trade_date) or ""
            history_reports = db.query_history_reports("market", symbol, trade_date, 7)
            history_report = "\n\n".join([r['report_content'] for r in history_reports])
        
        summary: AnalystMemorySummary = {
            "today_report": today_report,
            "history_report": history_report
        }
        
        return {"market_analyst_summary": summary}
    
    return market_summary_node
```

### 在测试脚本中使用

```python
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper

# 插入测试数据
with MemoryDBHelper() as db:
    db.insert_report(
        analyst_type="market",
        symbol="000001",
        trade_date="2024-01-15",
        report_content="测试报告内容"
    )
    
    # 验证插入
    report = db.query_today_report("market", "000001", "2024-01-15")
    assert report is not None, "报告插入失败"
```

## 故障排查

### 问题 1：数据库文件不存在

**错误：** `sqlite3.OperationalError: unable to open database file`

**解决：** `MemoryDBHelper` 会自动创建数据库文件和表，确保有写入权限。

### 问题 2：表不存在

**错误：** `sqlite3.OperationalError: no such table: analyst_reports`

**解决：** `MemoryDBHelper` 会自动创建表，如果仍有问题，检查数据库文件权限。

### 问题 3：插入失败但没有错误信息

**解决：** 检查参数是否正确，特别是 `analyst_type` 和日期格式。

## 相关文件

- **工具模块**：`tradingagents/agents/utils/memory_db_helper.py`
- **数据库初始化**：`tradingagents/agents/init_db.py`
- **报告解析**：`tradingagents/agents/utils/parse_report.py`
- **测试脚本**：`tradingagents/agents/utils/tests/db/`

