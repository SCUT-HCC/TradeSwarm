# -*- coding: utf-8 -*-
"""
导出 test.db 数据库内容到 JSON 和 TXT 文件
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any


def export_db_to_json(db_path: str = "test.db", output_json: str = "test_db_export.json", output_txt: str = "test_db_export.txt"):
    """
    导出数据库内容到 JSON 和 TXT 文件
    
    Args:
        db_path: 数据库路径
        output_json: JSON 输出文件路径
        output_txt: TXT 输出文件路径
    """
    if not Path(db_path).exists():
        print(f"[ERROR] 数据库文件不存在: {db_path}")
        return
    
    # 连接数据库
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # 使用 Row 工厂，可以按列名访问
    cursor = conn.cursor()
    
    # 查询所有报告
    cursor.execute("""
        SELECT 
            id,
            analyst_type,
            symbol,
            trade_date,
            report_content,
            created_at
        FROM analyst_reports
        ORDER BY trade_date DESC, created_at DESC
    """)
    
    rows = cursor.fetchall()
    
    # 转换为字典列表
    reports = []
    for row in rows:
        reports.append({
            "id": row["id"],
            "analyst_type": row["analyst_type"],
            "symbol": row["symbol"],
            "trade_date": row["trade_date"],
            "report_content": row["report_content"],
            "created_at": row["created_at"],
        })
    
    # 统计信息
    stats = {
        "total_reports": len(reports),
        "by_analyst_type": {},
        "by_symbol": {},
        "by_date": {},
    }
    
    for report in reports:
        # 按分析师类型统计
        analyst_type = report["analyst_type"]
        if analyst_type not in stats["by_analyst_type"]:
            stats["by_analyst_type"][analyst_type] = 0
        stats["by_analyst_type"][analyst_type] += 1
        
        # 按股票代码统计
        symbol = report["symbol"]
        if symbol not in stats["by_symbol"]:
            stats["by_symbol"][symbol] = 0
        stats["by_symbol"][symbol] += 1
        
        # 按日期统计
        trade_date = report["trade_date"]
        if trade_date not in stats["by_date"]:
            stats["by_date"][trade_date] = 0
        stats["by_date"][trade_date] += 1
    
    # 构建导出数据
    export_data = {
        "export_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "database_path": db_path,
        "statistics": stats,
        "reports": reports,
    }
    
    # 保存为 JSON
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(export_data, f, ensure_ascii=False, indent=2)
    print(f"[OK] JSON 文件已保存: {output_json}")
    
    # 保存为 TXT（可读格式）
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write("="*80 + "\n")
        f.write("数据库导出报告\n")
        f.write("="*80 + "\n\n")
        f.write(f"导出时间: {export_data['export_time']}\n")
        f.write(f"数据库路径: {db_path}\n\n")
        
        f.write("统计信息\n")
        f.write("-"*80 + "\n")
        f.write(f"总报告数: {stats['total_reports']}\n\n")
        
        f.write("按分析师类型统计:\n")
        for analyst_type, count in sorted(stats["by_analyst_type"].items()):
            f.write(f"  {analyst_type}: {count} 条\n")
        f.write("\n")
        
        f.write("按股票代码统计:\n")
        for symbol, count in sorted(stats["by_symbol"].items()):
            f.write(f"  {symbol}: {count} 条\n")
        f.write("\n")
        
        f.write("按日期统计:\n")
        for trade_date, count in sorted(stats["by_date"].items()):
            f.write(f"  {trade_date}: {count} 条\n")
        f.write("\n")
        
        f.write("="*80 + "\n")
        f.write("详细报告内容\n")
        f.write("="*80 + "\n\n")
        
        for i, report in enumerate(reports, 1):
            f.write(f"\n{'='*80}\n")
            f.write(f"报告 #{i} (ID: {report['id']})\n")
            f.write(f"{'='*80}\n")
            f.write(f"分析师类型: {report['analyst_type']}\n")
            f.write(f"股票代码: {report['symbol']}\n")
            f.write(f"交易日期: {report['trade_date']}\n")
            f.write(f"创建时间: {report['created_at']}\n")
            f.write(f"\n报告内容:\n")
            f.write("-"*80 + "\n")
            f.write(report['report_content'])
            f.write("\n" + "-"*80 + "\n\n")
    
    print(f"[OK] TXT 文件已保存: {output_txt}")
    
    # 打印摘要
    print(f"\n{'='*80}")
    print("数据库内容摘要")
    print(f"{'='*80}")
    print(f"总报告数: {stats['total_reports']}")
    print(f"\n按分析师类型:")
    for analyst_type, count in sorted(stats["by_analyst_type"].items()):
        print(f"  {analyst_type}: {count} 条")
    print(f"\n按股票代码:")
    for symbol, count in sorted(stats["by_symbol"].items()):
        print(f"  {symbol}: {count} 条")
    print(f"\n按日期:")
    for trade_date, count in sorted(stats["by_date"].items()):
        print(f"  {trade_date}: {count} 条")


if __name__ == "__main__":
    export_db_to_json()

