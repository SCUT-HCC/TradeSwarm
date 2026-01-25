from typing import Any
from pathlib import Path
import json
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.language_models import BaseChatModel
from tradingagents.agents.init_db import conn, cursor

def _parse_report_to_sql_structure(
    llm: BaseChatModel,
    fundamentals_report: str,
    symbol: str,
    trade_date: str
) -> dict[str, Any] | None:
    """
    使用 LLM 将 fundamentals_report 解析成符合 sql.json 结构的数据结构
    
    Args:
        llm: LangChain BaseChatModel 实例
        fundamentals_report: 基本面分析报告文本
        symbol: 股票代码
        trade_date: 交易日期
        
    Returns:
        符合 sql.json 结构的字典，如果解析失败返回 None
    """
    # 读取 sql.json 结构定义
    sql_json_path = Path(__file__).parent.parent / "summary" / "sql.json"
    with open(sql_json_path, "r", encoding="utf-8") as f:
        sql_schema = json.load(f)
    
    # 构建 prompt
    system_prompt = SystemMessage(
        "你是一个数据解析助手。请将基本面分析报告解析成符合数据库表结构的 JSON 格式。"
    )
    
    user_prompt = HumanMessage(
        f"""请将以下基本面分析报告解析成符合以下数据库表结构的 JSON 格式：

数据库表结构：
{json.dumps(sql_schema, ensure_ascii=False, indent=2)}

基本面分析报告：
{fundamentals_report}

股票代码：{symbol}
交易日期：{trade_date}

请返回一个 JSON 对象，包含以下字段：
- analyst_type: "fundamentals"
- symbol: "{symbol}"
- trade_date: "{trade_date}"
- report_content: 完整的报告内容（保持原格式）

注意：id 和 created_at 字段不需要在返回的 JSON 中，它们会在数据库插入时自动生成。

请只返回 JSON 对象，不要包含其他文字说明。"""
    )
    
    try:
        response = llm.invoke([system_prompt, user_prompt])
        response_content = response.content.strip()
        
        # 尝试提取 JSON（可能包含 markdown 代码块）
        if "```json" in response_content:
            response_content = response_content.split("```json")[1].split("```")[0].strip()
        elif "```" in response_content:
            response_content = response_content.split("```")[1].split("```")[0].strip()
        
        # 解析 JSON
        parse_result = json.loads(response_content)
        
        # 确保必要字段存在
        parse_result["analyst_type"] = "fundamentals"
        parse_result["symbol"] = symbol
        parse_result["trade_date"] = trade_date
        parse_result["report_content"] = fundamentals_report
        
        return parse_result
    except Exception as e:
        print(f"解析报告时发生错误: {e}")
        # 如果解析失败，返回一个基本结构
        return {
            "analyst_type": "fundamentals",
            "symbol": symbol,
            "trade_date": trade_date,
            "report_content": fundamentals_report
        }


def _insert_report_to_db(parse_result: dict[str, Any]) -> bool:
    """
    将解析后的报告数据插入到 analyst_reports 表中
    
    Args:
        parse_result: 符合 sql.json 结构的字典
        
    Returns:
        插入是否成功
    """
    try:
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
        
        # 插入数据
        insert_sql = """
        INSERT INTO analyst_reports (analyst_type, symbol, trade_date, report_content)
        VALUES (?, ?, ?, ?)
        """
        
        cursor.execute(
            insert_sql,
            (
                parse_result["analyst_type"],
                parse_result["symbol"],
                parse_result["trade_date"],
                parse_result["report_content"]
            )
        )
        conn.commit()
        
        print(f"成功插入基本面分析报告到数据库: {parse_result['symbol']} - {parse_result['trade_date']}")
        return True
        
    except Exception as e:
        print(f"插入报告到数据库时发生错误: {e}")
        conn.rollback()
        return False


def _query_today_report(symbol: str, trade_date: str) -> str:
    """
    从数据库查询 Fundamentals Analyst 的今日报告（参考 news_summary/node.py 的实现）
    
    Args:
        symbol: 股票代码（如 '600519'）
        trade_date: 交易日期（如 '2024-12-20'）
    
    Returns:
        今日基本面分析报告
    """
    try:
        # 执行查询：Fundamentals 按周更新
        sql = """
            SELECT report_content 
            FROM analyst_reports
            WHERE analyst_type='fundamentals' 
                AND symbol=? 
                AND trade_date=?
            ORDER BY created_at DESC 
            LIMIT 1
        """
        
        cursor.execute(sql, (symbol, trade_date))

        # 返回结果是一个元组
        result = cursor.fetchone()
        
        # 如果查询到结果，返回标题 + 报告内容
        if result and result[0]:
            return f"# Fundamentals Analysis Report - {symbol} ({trade_date})\n\n{result[0]}"
        else:
            # 如果没有查询到结果，返回标题 + 提示信息
            return f"# Fundamentals Analysis Report - {symbol} ({trade_date})\n\n未找到基本面分析报告数据。"
            
    except Exception as e:
        # 异常处理：如果查询失败，返回错误信息（或可以记录日志）
        print(f"查询基本面报告时发生错误: {e}")
        # 返回标题 + 错误信息
        return f"# Fundamentals Analysis Report - {symbol} ({trade_date})\n\n查询错误: {str(e)}"


def test_parse_and_insert():
    """
    测试函数：测试报告解析和数据库插入功能
    """
    # 导入 LLM
    from tradingagents.agents.init_llm import llm
    
    # 创建虚构的基本面分析报告
    fundamentals_report = """
# 贵州茅台 (600519) 基本面分析报告

## 一、公司概况
贵州茅台酒股份有限公司是中国白酒行业的龙头企业，主要从事茅台酒及系列酒的生产和销售。

## 二、财务指标分析

### 1. 盈利能力
- **营业收入**: 2024年实现营业收入约1,200亿元，同比增长15.2%
- **净利润**: 实现净利润约600亿元，同比增长18.5%
- **毛利率**: 保持在高位，约92%
- **净利率**: 约50%，盈利能力强劲

### 2. 成长性
- **营收增长率**: 近三年复合增长率约12%
- **净利润增长率**: 近三年复合增长率约15%
- **ROE**: 约30%，股东回报率优秀

### 3. 财务健康度
- **资产负债率**: 约20%，财务结构稳健
- **流动比率**: 约3.5，短期偿债能力强
- **现金及现金等价物**: 约800亿元，现金流充裕

### 4. 估值指标
- **PE (TTM)**: 约35倍
- **PB**: 约12倍
- **PEG**: 约2.3

## 三、投资建议
基于以上分析，贵州茅台基本面优秀，财务健康，但当前估值偏高。建议：
1. 长期投资者可关注回调机会
2. 短期投资者需注意估值风险
3. 建议在合理估值区间（PE < 30）时考虑买入

## 四、风险提示
1. 行业政策风险
2. 消费需求波动风险
3. 估值回调风险
"""
    
    # 测试参数
    symbol = "600519"
    trade_date = "2024-12-20"
    
    print("=" * 80)
    print("开始测试报告解析和数据库插入功能")
    print("=" * 80)
    
    # 第一步：测试解析功能
    print("\n[1/2] 测试报告解析功能...")
    print(f"股票代码: {symbol}")
    print(f"交易日期: {trade_date}")
    print(f"报告长度: {len(fundamentals_report)} 字符")
    
    try:
        parse_result = _parse_report_to_sql_structure(
            llm=llm,
            fundamentals_report=fundamentals_report,
            symbol=symbol,
            trade_date=trade_date
        )
        
        if parse_result:
            print("✓ 解析成功！")
            print("\n解析结果:")
            print("-" * 80)
            print(f"analyst_type: {parse_result.get('analyst_type')}")
            print(f"symbol: {parse_result.get('symbol')}")
            print(f"trade_date: {parse_result.get('trade_date')}")
            print(f"report_content 长度: {len(parse_result.get('report_content', ''))} 字符")
            print("-" * 80)
            
            # 第二步：测试数据库插入功能
            print("\n[2/2] 测试数据库插入功能...")
            success = _insert_report_to_db(parse_result)
            
            if success:
                print("✓ 数据库插入成功！")
                
                # 验证插入结果
                print("\n验证插入结果...")
                cursor.execute(
                    "SELECT id, analyst_type, symbol, trade_date, created_at FROM analyst_reports WHERE symbol=? AND trade_date=? ORDER BY created_at DESC LIMIT 1",
                    (symbol, trade_date)
                )
                result = cursor.fetchone()
                
                if result:
                    print("✓ 验证成功！插入的记录:")
                    print(f"  ID: {result[0]}")
                    print(f"  分析师类型: {result[1]}")
                    print(f"  股票代码: {result[2]}")
                    print(f"  交易日期: {result[3]}")
                    print(f"  创建时间: {result[4]}")
                    
                    # 第三步：测试查询功能（参考 _query_today_report）
                    print("\n[3/3] 测试查询功能...")
                    print("-" * 80)
                    query_result = _query_today_report(symbol, trade_date)
                    print("查询结果:")
                    print("-" * 80)
                    print(query_result)
                    print("-" * 80)
                else:
                    print("⚠ 警告: 未找到插入的记录")
            else:
                print("❌ 数据库插入失败")
        else:
            print("❌ 解析失败，返回 None")
            
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("测试完成")
    print("=" * 80)


if __name__ == "__main__":
    test_parse_and_insert()