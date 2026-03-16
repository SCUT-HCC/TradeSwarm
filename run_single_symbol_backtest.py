#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
单标的多日回测驱动器

功能：
1. 按日期区间循环，每天依次调用：Pre-Open Graph → Market Open → Post Close
2. 固定一个 symbol，不做截面选股
3. 确保"多日动态仓位 + 资金曲线"跑通
4. 保存每日状态和最终报告

注意：
- Alpha Vantage API 有日访问限制，使用缓存数据
- 数据下载后会自动保存到缓存
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from tradingagents.graph.trading_graph import create_trading_graph
from tradingagents.graph.utils import load_llm_from_config
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper
from tradingagents.agents.utils.agentstate.agent_states import AgentState
from tradingagents.core.data_adapter import DataAdapter
from tradingagents.core.portfolio.portfolio_manager import PortfolioManager
from tradingagents.agents.market_open.node import create_market_open_executor
from tradingagents.agents.post_close.node import create_post_close_node
from tradingagents.agents.post_close.history_maintainer import create_history_maintainer_node
from tradingagents.agents.analysts.market_analyst.agent import create_market_analyst
from tradingagents.agents.analysts.news_analyst.agent import create_news_analyst
from tradingagents.agents.analysts.fundamentals_analyst.agent import create_fundamentals_analyst
from tradingagents.agents.analysts.social_media_analyst.agent import create_social_media_analyst


class DatabaseMemory:
    """从数据库读取历史经验的 Memory 类"""
    
    def __init__(self, db_path: str, symbol: str, limit: int = 10):
        self.db_helper = MemoryDBHelper(db_path)
        self.symbol = symbol
        self.limit = limit
    
    def get_memories(self, current_situation: str, n_matches: int = 2) -> List[Dict[str, Any]]:
        """从数据库查询历史报告"""
        try:
            conn = self.db_helper._get_connection()
            cursor = conn.cursor()
            
            sql = """
            SELECT analyst_type, trade_date, report_content
            FROM analyst_reports
            WHERE symbol = ?
                AND report_content IS NOT NULL
                AND report_content != ''
            ORDER BY trade_date DESC, created_at DESC
            LIMIT ?
            """
            
            cursor.execute(sql, (self.symbol, self.limit))
            results = cursor.fetchall()
            cursor.close()
            
            if not results:
                return []
            
            memories = []
            for row in results[:n_matches]:
                analyst_type = row[0]
                trade_date = row[1]
                report_content = row[2]
                
                situation = report_content[:200] + "..." if len(report_content) > 200 else report_content
                recommendation = "基于历史数据分析，建议谨慎操作，关注市场变化。"
                
                memories.append({
                    "matched_situation": situation,
                    "recommendation": recommendation,
                    "similarity_score": 0.7,
                })
            
            return memories
            
        except Exception as e:
            print(f"[WARN] 从数据库读取记忆失败: {e}")
            return []
    
    def close(self) -> None:
        """关闭数据库连接"""
        self.db_helper.close()


def get_trading_dates(start_date: str, end_date: str, data_adapter: DataAdapter) -> List[str]:
    """
    获取交易日历
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        data_adapter: 数据适配器
    
    Returns:
        交易日列表
    """
    # 使用 SPY 作为参考，获取交易日历
    df = data_adapter.load_stock_data_until("SPY", end_date, start_date=start_date)
    if df is None or len(df) == 0:
        print(f"[WARN] 无法加载 SPY 数据，尝试使用指定日期范围")
        # 如果无法加载 SPY，尝试生成日期范围（简单处理，不排除周末和节假日）
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        dates = []
        current = start
        while current <= end:
            # 简单排除周末
            if current.weekday() < 5:  # 0-4 是周一到周五
                dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return dates
    
    # 从 DataFrame 索引中提取交易日
    trading_dates = [date.strftime("%Y-%m-%d") for date in df.index if start_date <= date.strftime("%Y-%m-%d") <= end_date]
    return sorted(trading_dates)


def run_single_day(
    symbol: str,
    trade_date: str,
    llm: Any,
    memory: DatabaseMemory,
    db_helper: MemoryDBHelper,
    portfolio_manager: PortfolioManager,
    data_adapter: DataAdapter,
    output_dir: Path,
    previous_total_value: Optional[float] = None,
) -> Dict[str, Any]:
    """
    运行单日的完整流程：Pre-Open → Market Open → Post Close
    
    Args:
        symbol: 股票代码
        trade_date: 交易日期
        llm: LLM 实例
        memory: Memory 实例
        db_helper: 数据库助手
        portfolio_manager: 组合管理器
        data_adapter: 数据适配器
        output_dir: 输出目录
    
    Returns:
        当日执行结果
    """
    print(f"\n{'='*80}")
    print(f"交易日: {trade_date} ({symbol})")
    print(f"{'='*80}")
    
    day_result = {
        "date": trade_date,
        "symbol": symbol,
        "analyst_results": {},
        "pre_open": {},
        "market_open": {},
        "post_close": {},
        "portfolio_state": {},
        "errors": [],
    }
    
    try:
        # ========== 运行 Analyst 生成报告 ==========
        print(f"\n[Analyst] 开始生成分析师报告...")
        try:
            # 创建 Analyst 节点
            market_analyst = create_market_analyst(llm)
            news_analyst = create_news_analyst(llm)
            fundamentals_analyst = create_fundamentals_analyst(llm)
            social_media_analyst = create_social_media_analyst(llm)
            
            analysts = [
                ("market", market_analyst, "market_report"),
                ("news", news_analyst, "news_report"),
                ("fundamentals", fundamentals_analyst, "fundamentals_report"),
                ("sentiment", social_media_analyst, "sentiment_report"),
            ]
            
            analyst_results = {}
            for analyst_type, analyst_func, report_key in analysts:
                print(f"  [运行] {analyst_type.upper()} Analyst...")
                try:
                    # 准备初始状态
                    initial_state: AgentState = {
                        "company_of_interest": symbol,
                        "trade_date": trade_date,
                        report_key: "",
                        "messages": [],
                    }
                    
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
                            analyst_results[analyst_type] = "ok"
                            print(f"    [OK] {analyst_type.upper()} Analyst 报告已保存")
                        else:
                            analyst_results[analyst_type] = "save_failed"
                            print(f"    [FAIL] {analyst_type.upper()} Analyst 保存失败")
                    else:
                        analyst_results[analyst_type] = "no_content"
                        print(f"    [WARN] {analyst_type.upper()} Analyst 未生成报告内容")
                        
                except Exception as e:
                    analyst_results[analyst_type] = f"error: {str(e)}"
                    print(f"    [ERROR] {analyst_type.upper()} Analyst 运行失败: {e}")
                    import traceback
                    traceback.print_exc()
            
            day_result["analyst_results"] = analyst_results
            print(f"[Analyst] 完成")
            
        except Exception as e:
            error_msg = f"Analyst 执行失败: {e}"
            print(f"[ERROR] {error_msg}")
            day_result["errors"].append(error_msg)
            import traceback
            traceback.print_exc()
        
        # ========== Pre-Open 阶段 ==========
        print(f"\n[Pre-Open] 开始分析...")
        try:
            # 创建 Graph
            graph = create_trading_graph(llm, memory, db_helper)
            
            # 准备初始状态
            initial_state: AgentState = {
                "company_of_interest": symbol,
                "trade_date": trade_date,
                "trading_session": "pre_open",
                "messages": [],
                "current_position": portfolio_manager.get_position(symbol),
                "portfolio_state": portfolio_manager.get_portfolio_state(),
            }
            
            # 运行 Graph
            final_state = None
            for state in graph.stream(initial_state, stream_mode="values"):
                final_state = state
            
            if final_state:
                day_result["pre_open"] = {
                    "trader_investment_plan": final_state.get("trader_investment_plan"),
                    "strategy_selection": final_state.get("strategy_selection"),
                    "risk_summary": final_state.get("risk_summary"),
                }
                print(f"[Pre-Open] 分析完成")
            else:
                print(f"[Pre-Open] 未获取到最终状态")
                day_result["errors"].append("Pre-Open: 未获取到最终状态")
                
        except Exception as e:
            error_msg = f"Pre-Open 执行失败: {e}"
            print(f"[ERROR] {error_msg}")
            day_result["errors"].append(error_msg)
            import traceback
            traceback.print_exc()
        
        # ========== Market Open 阶段 ==========
        print(f"\n[Market Open] 开始执行交易...")
        try:
            # 创建 market_open 节点
            market_open_node = create_market_open_executor(portfolio_manager, data_adapter)
            
            # 准备状态（包含 Pre-Open 的结果）
            market_open_state: AgentState = {
                "company_of_interest": symbol,
                "trade_date": trade_date,
                "trading_session": "market_open",
                "messages": [],
                "current_position": portfolio_manager.get_position(symbol),
                "portfolio_state": portfolio_manager.get_portfolio_state(),
            }
            
            # 合并 Pre-Open 的结果
            if "pre_open" in day_result:
                market_open_state.update(day_result["pre_open"])
            
            # 执行 market_open 节点
            market_open_result = market_open_node(market_open_state)
            day_result["market_open"] = market_open_result
            
            # 更新 portfolio_manager 的状态（从 market_open_result 中获取）
            if market_open_result:
                # Market Open 节点已经通过 portfolio_manager 更新了状态
                # 这里只需要记录执行日志
                execution_log = market_open_result.get("execution_log", [])
                if execution_log:
                    log_entry = execution_log[0]
                    action = log_entry.get("action", "hold")
                    if action != "hold":
                        print(f"  [执行] {action.upper()}: {log_entry.get('volume', 0)} 股 @ ${log_entry.get('price', 0):.2f}")
                        print(f"  原因: {log_entry.get('reason', '')}")
                    else:
                        print(f"  [未执行] {log_entry.get('reason', '')}")
            
            print(f"[Market Open] 执行完成")
            
        except Exception as e:
            error_msg = f"Market Open 执行失败: {e}"
            print(f"[ERROR] {error_msg}")
            day_result["errors"].append(error_msg)
            import traceback
            traceback.print_exc()
        
        # ========== Post Close 阶段 ==========
        print(f"\n[Post Close] 开始更新收益...")
        try:
            # 创建 post_close 节点（传递前一天的 total_value）
            post_close_node = create_post_close_node(
                portfolio_manager, 
                data_adapter,
                previous_total_value=previous_total_value
            )
            
            # 准备状态
            post_close_state: AgentState = {
                "company_of_interest": symbol,
                "trade_date": trade_date,
                "trading_session": "post_close",
                "messages": [],
                "current_position": portfolio_manager.get_position(symbol),
                "portfolio_state": portfolio_manager.get_portfolio_state(),
            }
            
            # 执行 post_close 节点
            post_close_result = post_close_node(post_close_state)
            day_result["post_close"] = post_close_result
            
            # 更新组合状态
            day_result["portfolio_state"] = portfolio_manager.get_portfolio_state()
            
            # 获取单日收益率和最大回撤
            daily_return = post_close_result.get("daily_return", 0.0)
            max_drawdown = post_close_result.get("max_drawdown", 0.0)
            
            print(f"[Post Close] 更新完成")
            print(f"  总资产: ${portfolio_manager.total_value:,.2f}")
            print(f"  现金: ${portfolio_manager.cash:,.2f}")
            print(f"  持仓市值: ${portfolio_manager.positions_value:,.2f}")
            print(f"  总收益率: {portfolio_manager.total_return:.2f}%")
            print(f"  单日收益率: {daily_return:.2f}%")
            print(f"  最大回撤: {max_drawdown:.2f}%")
            
            # 更新前一天的 total_value（用于下一天的计算）
            previous_total_value = portfolio_manager.total_value
            
        except Exception as e:
            error_msg = f"Post Close 执行失败: {e}"
            print(f"[ERROR] {error_msg}")
            day_result["errors"].append(error_msg)
            import traceback
            traceback.print_exc()
        
        # ========== 保存 Daily Trading Summary ==========
        print(f"\n[Daily Summary] 开始保存日级交易摘要...")
        try:
            import json
            
            # 从 Pre-Open 结果中提取信息
            pre_open_result = day_result.get("pre_open", {})
            trader_plan_str = pre_open_result.get("trader_investment_plan", "{}")
            risk_summary = pre_open_result.get("risk_summary", {})
            final_decision_str = risk_summary.get("final_trade_decision", "{}")
            
            # 解析 JSON 字符串
            try:
                trader_plan = json.loads(trader_plan_str) if trader_plan_str else {}
            except json.JSONDecodeError:
                trader_plan = {}
            
            try:
                final_decision = json.loads(final_decision_str) if final_decision_str else {}
            except json.JSONDecodeError:
                final_decision = {}
            
            # 从 Post Close 结果中获取单日收益率和最大回撤
            post_close_result = day_result.get("post_close", {})
            actual_return = post_close_result.get("daily_return", 0.0)
            actual_max_drawdown = post_close_result.get("max_drawdown", 0.0)
            
            # 确定仓位状态
            portfolio_state = day_result.get("portfolio_state", {})
            positions = portfolio_state.get("positions", {})
            if symbol in positions:
                position = positions[symbol]
                if position.get("shares", 0) > 0:
                    positioning = "full" if portfolio_manager.cash < portfolio_manager.total_value * 0.1 else "partial"
                else:
                    positioning = "empty"
            else:
                positioning = "empty"
            
            # 从 Strategy Selector 输出中提取标准化字段（唯一来源）
            strategy_selection = pre_open_result.get("strategy_selection") or {}
            market_regime = strategy_selection.get("market_regime")
            # selected_strategy 直接用 strategy_type（例如 trend_following / mean_reversion）
            selected_strategy = strategy_selection.get("strategy_type")
            expected_behavior = strategy_selection.get("expected_behavior")
            
            # 组装 daily summary JSON
            daily_summary = {
                "date": trade_date,
                "market_regime": market_regime,
                "selected_strategy": selected_strategy,
                "expected_behavior": expected_behavior,
                "actual_outcome": {
                    "return": round(actual_return, 2),
                    "max_drawdown": round(actual_max_drawdown, 2)
                },
                "positioning": positioning,
                "anomaly": None,  # 可选：异常情况描述
                # 额外信息
                "trader_action": trader_plan.get("action", "UNKNOWN"),
                "risk_decision": final_decision.get("final_decision", "UNKNOWN"),
                "portfolio_value": round(portfolio_manager.total_value, 2),
                "cash": round(portfolio_manager.cash, 2),
                "positions_value": round(portfolio_manager.positions_value, 2),
            }
            
            # 保存到数据库
            summary_json_str = json.dumps(daily_summary, ensure_ascii=False, indent=2)
            success = db_helper.upsert_daily_trading_summary(
                date=trade_date,
                symbol=symbol,
                summary_json=summary_json_str,
                market_regime=daily_summary.get("market_regime"),
                selected_strategy=daily_summary.get("selected_strategy"),
                expected_behavior=daily_summary.get("expected_behavior"),
                actual_return=daily_summary["actual_outcome"]["return"],
                actual_max_drawdown=daily_summary["actual_outcome"]["max_drawdown"],
                positioning=daily_summary.get("positioning"),
                anomaly=daily_summary.get("anomaly"),
            )
            
            if success:
                print(f"[Daily Summary] 成功保存: {trade_date} - {symbol}")
                day_result["daily_summary"] = daily_summary
            else:
                print(f"[Daily Summary] 保存失败: {trade_date} - {symbol}")
                day_result["errors"].append("Daily Summary 保存失败")
            
        except Exception as e:
            error_msg = f"Daily Summary 保存失败: {e}"
            print(f"[ERROR] {error_msg}")
            day_result["errors"].append(error_msg)
            import traceback
            traceback.print_exc()
        
        # ========== History Maintainer 阶段 ==========
        print(f"\n[History Maintainer] 开始生成 summary...")
        try:
            # 创建 history_maintainer 节点
            history_maintainer_node = create_history_maintainer_node(llm, db_helper)
            
            # 准备状态
            history_state: AgentState = {
                "company_of_interest": symbol,
                "trade_date": trade_date,
                "trading_session": "post_close",
                "messages": [],
                "current_position": portfolio_manager.get_position(symbol),
                "portfolio_state": portfolio_manager.get_portfolio_state(),
            }
            
            # 执行 history_maintainer 节点
            history_result = history_maintainer_node(history_state)
            day_result["history_maintainer"] = history_result
            print(f"[History Maintainer] 完成")
            if "history_maintainer_log" in history_result:
                for log_entry in history_result["history_maintainer_log"]:
                    print(f"  {log_entry}")
            
        except Exception as e:
            error_msg = f"History Maintainer 执行失败: {e}"
            print(f"[ERROR] {error_msg}")
            day_result["errors"].append(error_msg)
            import traceback
            traceback.print_exc()
        
        # 保存当日结果
        day_output_file = output_dir / "daily_results" / f"{trade_date}.json"
        day_output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(day_output_file, "w", encoding="utf-8") as f:
            json.dump(day_result, f, ensure_ascii=False, indent=2, default=str)
        
    except Exception as e:
        error_msg = f"单日执行失败: {e}"
        print(f"[ERROR] {error_msg}")
        day_result["errors"].append(error_msg)
        import traceback
        traceback.print_exc()
    
    return day_result


def run_single_symbol_backtest(
    symbol: str,
    start_date: str,
    end_date: str,
    initial_cash: float = 100000.0,
    db_path: str = "memory.db",
    output_dir: str = "backtest_results",
) -> Dict[str, Any]:
    """
    运行单标的多日回测
    
    Args:
        symbol: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        initial_cash: 初始资金
        db_path: 数据库路径
        output_dir: 输出目录
    
    Returns:
        回测结果
    """
    print(f"\n{'='*80}")
    print(f"单标的多日回测驱动器")
    print(f"{'='*80}")
    print(f"股票代码: {symbol}")
    print(f"日期范围: {start_date} ~ {end_date}")
    print(f"初始资金: ${initial_cash:,.2f}")
    print(f"数据库: {db_path}")
    print(f"输出目录: {output_dir}")
    
    # 创建输出目录
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    (output_path / "daily_results").mkdir(parents=True, exist_ok=True)
    
    # 初始化组件
    print(f"\n[初始化] 初始化组件...")
    
    # 加载 LLM
    llm = load_llm_from_config()
    print(f"  [OK] LLM 加载成功")
    
    # 初始化数据库
    if not Path(db_path).exists():
        print(f"[WARN] 数据库 {db_path} 不存在，将创建新数据库")
    db_helper = MemoryDBHelper(db_path)
    print(f"  [OK] 数据库连接成功")
    
    # 创建 Memory
    memory = DatabaseMemory(db_path=db_path, symbol=symbol)
    print(f"  [OK] Memory 创建成功")
    
    # 初始化组合管理器
    portfolio_manager = PortfolioManager(initial_cash=initial_cash, max_positions=1)
    portfolio_manager.target_symbols = [symbol]  # 设置目标股票
    print(f"  [OK] 组合管理器初始化成功")
    
    # 初始化数据适配器（使用缓存）
    data_adapter = DataAdapter(use_cache=True)
    print(f"  [OK] 数据适配器初始化成功（使用缓存）")
    
    # 获取交易日历
    print(f"\n[交易日历] 获取交易日历...")
    trading_dates = get_trading_dates(start_date, end_date, data_adapter)
    print(f"  [OK] 找到 {len(trading_dates)} 个交易日")
    if len(trading_dates) == 0:
        print(f"[ERROR] 未找到交易日，退出")
        return {}
    
    # 运行每日流程
    print(f"\n[回测] 开始回测...")
    daily_results = []
    previous_total_value = initial_cash  # 初始值设为初始资金
    
    for i, trade_date in enumerate(trading_dates, 1):
        print(f"\n进度: {i}/{len(trading_dates)}")
        
        day_result = run_single_day(
            symbol=symbol,
            trade_date=trade_date,
            llm=llm,
            memory=memory,
            db_helper=db_helper,
            portfolio_manager=portfolio_manager,
            data_adapter=data_adapter,
            output_dir=output_path,
            previous_total_value=previous_total_value,
        )
        daily_results.append(day_result)
        
        # 更新前一天的 total_value（用于下一天的计算）
        previous_total_value = portfolio_manager.total_value
        
        # 简单进度显示
        if i % 5 == 0 or i == len(trading_dates):
            portfolio_state = portfolio_manager.get_portfolio_state()
            print(f"\n[进度] {i}/{len(trading_dates)} 完成")
            print(f"  当前总资产: ${portfolio_manager.total_value:,.2f}")
            print(f"  当前收益率: {portfolio_manager.total_return:.2f}%")
    
    # 生成最终报告
    print(f"\n[报告] 生成最终报告...")
    final_portfolio_state = portfolio_manager.get_portfolio_state()
    
    backtest_result = {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "initial_cash": initial_cash,
        "final_portfolio_state": final_portfolio_state,
        "total_return": portfolio_manager.total_return,
        "total_trading_days": len(trading_dates),
        "daily_results_count": len(daily_results),
        "summary": {
            "initial_cash": initial_cash,
            "final_value": portfolio_manager.total_value,
            "total_return": portfolio_manager.total_return,
            "cash": portfolio_manager.cash,
            "positions_value": portfolio_manager.positions_value,
            "total_trades": len(portfolio_manager.trades),
        },
    }
    
    # 保存最终报告
    report_file = output_path / "backtest_report.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(backtest_result, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"  [OK] 报告已保存到 {report_file}")
    
    # 打印摘要
    print(f"\n{'='*80}")
    print(f"回测完成")
    print(f"{'='*80}")
    print(f"股票代码: {symbol}")
    print(f"日期范围: {start_date} ~ {end_date}")
    print(f"交易日数: {len(trading_dates)}")
    print(f"初始资金: ${initial_cash:,.2f}")
    print(f"最终资产: ${portfolio_manager.total_value:,.2f}")
    print(f"总收益率: {portfolio_manager.total_return:.2f}%")
    print(f"现金: ${portfolio_manager.cash:,.2f}")
    print(f"持仓市值: ${portfolio_manager.positions_value:,.2f}")
    print(f"交易次数: {len(portfolio_manager.trades)}")
    print(f"输出目录: {output_path.absolute()}")
    
    # 清理
    memory.close()
    db_helper.close()
    
    return backtest_result


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="单标的多日回测驱动器")
    parser.add_argument("--symbol", type=str, default="AAPL", help="股票代码")
    parser.add_argument("--start", type=str, default="2024-01-02", help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default="2024-01-31", help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--cash", type=float, default=100000.0, help="初始资金")
    parser.add_argument("--db", type=str, default="memory.db", help="数据库路径")
    parser.add_argument("--output", type=str, default="backtest_results", help="输出目录")
    
    args = parser.parse_args()
    
    try:
        result = run_single_symbol_backtest(
            symbol=args.symbol,
            start_date=args.start,
            end_date=args.end,
            initial_cash=args.cash,
            db_path=args.db,
            output_dir=args.output,
        )
        
        if result:
            print(f"\n[完成] 回测成功完成！")
        else:
            print(f"\n[错误] 回测失败")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n[ERROR] 执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

