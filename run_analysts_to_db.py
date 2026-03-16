# -*- coding: utf-8 -*-
"""
运行所有 Analyst 并保存到数据库

功能：
1. 运行所有 4 个 Analyst（market, news, fundamentals, sentiment）
2. 将报告保存到 test.db
"""

import sys
from pathlib import Path
from datetime import date

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from tradingagents.graph.utils import load_llm_from_config
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper
from tradingagents.agents.analysts.market_analyst.agent import create_market_analyst
from tradingagents.agents.analysts.news_analyst.agent import create_news_analyst
from tradingagents.agents.analysts.fundamentals_analyst.agent import create_fundamentals_analyst
from tradingagents.agents.analysts.social_media_analyst.agent import create_social_media_analyst


def run_analysts_and_save_to_db(
    symbol: str,
    trade_date: str,
    llm,
    db_path: str = "test.db"
) -> bool:
    """
    运行所有 Analyst 并保存报告到数据库
    
    Args:
        symbol: 股票代码
        trade_date: 交易日期
        llm: LLM 实例
        db_path: 数据库路径
        
    Returns:
        是否成功
    """
    print(f"\n{'='*80}")
    print(f"运行所有 Analyst 并保存到 {db_path}")
    print(f"{'='*80}\n")
    
    # 初始化数据库
    db_helper = MemoryDBHelper(db_path)
    
    # 创建 Analyst 节点
    market_analyst = create_market_analyst(llm)
    news_analyst = create_news_analyst(llm)
    fundamentals_analyst = create_fundamentals_analyst(llm)
    social_media_analyst = create_social_media_analyst(llm)
    
    analysts = [
        ("market", market_analyst, "market_report", {
            "company_of_interest": symbol,
            "trade_date": trade_date,
            "market_report": "",
            "messages": [],
        }),
        ("news", news_analyst, "news_report", {
            "company_of_interest": symbol,
            "trade_date": trade_date,
            "news_report": "",
            "messages": [],
        }),
        ("fundamentals", fundamentals_analyst, "fundamentals_report", {
            "company_of_interest": symbol,
            "trade_date": trade_date,
            "fundamentals_report": "",
            "messages": [],
        }),
        ("sentiment", social_media_analyst, "sentiment_report", {
            "company_of_interest": symbol,
            "trade_date": trade_date,
            "sentiment_report": "",
            "messages": [],
        }),
    ]
    
    success_count = 0
    for analyst_type, analyst_func, report_key, initial_state in analysts:
        print(f"\n[运行] {analyst_type.upper()} Analyst...")
        try:
            # 运行 Analyst
            result = analyst_func(initial_state)
            
            # 提取报告内容
            report_content = result.get(report_key, "")
            if not report_content:
                # 尝试从 messages 中提取
                messages = result.get("messages", [])
                for msg in reversed(messages):
                    if hasattr(msg, "content") and msg.content:
                        report_content = msg.content
                        break
            
            if report_content:
                # 保存到数据库
                success = db_helper.insert_report(
                    analyst_type=analyst_type,
                    symbol=symbol,
                    trade_date=trade_date,
                    report_content=report_content
                )
                if success:
                    success_count += 1
                    print(f"  [OK] {analyst_type.upper()} Analyst 报告已保存")
                else:
                    print(f"  [FAIL] {analyst_type.upper()} Analyst 保存失败")
            else:
                print(f"  [WARN] {analyst_type.upper()} Analyst 未生成报告内容")
                
        except Exception as e:
            print(f"  [ERROR] {analyst_type.upper()} Analyst 运行失败: {e}")
            import traceback
            traceback.print_exc()
    
    db_helper.close()
    
    print(f"\n[完成] 成功运行 {success_count}/{len(analysts)} 个 Analyst")
    return success_count == len(analysts)


def main():
    """主函数"""
    print("="*80)
    print("运行所有 Analyst 并保存到数据库")
    print("="*80)
    
    # 配置参数
    symbol = "AAPL"
    trade_date = "2026-02-05"  # 当日日期
    history_date = "2026-02-04"  # 历史日期
    db_path = "test.db"
    
    # 如果数据库已存在，删除旧数据
    if Path(db_path).exists():
        print(f"\n[清理] 删除旧的数据库: {db_path}")
        Path(db_path).unlink()
    
    try:
        # 加载 LLM
        print(f"[初始化] 加载 LLM...")
        llm = load_llm_from_config()
        print("[OK] LLM 加载成功")
        
        # 先为历史日期生成数据（用于历史报告）
        print(f"\n[步骤 1] 为历史日期 {history_date} 生成数据...")
        history_success = run_analysts_and_save_to_db(symbol, history_date, llm, db_path)
        
        # 再为当日日期生成数据
        print(f"\n[步骤 2] 为当日日期 {trade_date} 生成数据...")
        success = run_analysts_and_save_to_db(symbol, trade_date, llm, db_path)
        
        if success and history_success:
            print(f"\n[完成] 所有 Analyst 运行成功！")
            print(f"  数据库: {db_path}")
            print(f"  股票代码: {symbol}")
            print(f"  历史日期: {history_date}")
            print(f"  交易日期: {trade_date}")
        else:
            print(f"\n[WARN] 部分 Analyst 运行失败，请检查错误信息")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n[ERROR] 执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

