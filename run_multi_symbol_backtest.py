#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
多标的、多周期持续运行脚本

功能：
1. 支持按周/月定义周期
2. 每个周期开始时进行选股和再平衡
3. 周期内每个交易日执行：Pre-Open → Market Open → Post Close
4. 周期结束时运行 Reflector Agent
5. 包含异常处理、日志和监控

注意：
- Alpha Vantage API 有日访问限制，使用缓存数据
- 数据下载后会自动保存到缓存
"""

import json
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from calendar import monthrange

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
from tradingagents.agents.post_close.reflector import create_reflector_node
from tradingagents.agents.analysts.market_analyst.agent import create_market_analyst
from tradingagents.agents.analysts.news_analyst.agent import create_news_analyst
from tradingagents.agents.analysts.fundamentals_analyst.agent import create_fundamentals_analyst
from tradingagents.agents.analysts.social_media_analyst.agent import create_social_media_analyst
from tradingagents.core.selection.stock_selector import StockSelector
from tradingagents.core.selection.stock_pool import STOCK_POOL


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backtest_multi_symbol.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DatabaseMemory:
    """从数据库读取历史经验的 Memory 类"""
    
    def __init__(self, db_path: str, symbol: str, limit: int = 10):
        self.db_helper = MemoryDBHelper(db_path)
        self.symbol = symbol
        self.limit = limit
    
    def get_memories(self, current_situation: str, n_matches: int = 2) -> List[Dict[str, Any]]:
        """获取历史经验记忆"""
        # 从 cycle_reflections 表中读取最近的反思记录
        try:
            reflections = self.db_helper.query_cycle_reflection(
                cycle_type="weekly",
                symbol=self.symbol,
                limit=self.limit
            )
            if reflections:
                return [{"content": r.get("reflection_content", "")} for r in reflections]
        except Exception as e:
            logger.warning(f"获取历史记忆失败: {e}")
        return []
    
    def save_memory(self, content: str) -> bool:
        """保存记忆（由 Reflector 负责）"""
        return True


def get_trading_days(data_adapter: DataAdapter, start_date: str, end_date: str) -> List[str]:
    """获取交易日列表"""
    try:
        # 尝试从 SPY 数据获取交易日
        spy_df = data_adapter.load_stock_data_until("SPY", end_date, start_date=start_date)
        if spy_df is not None and len(spy_df) > 0:
            trading_days = [d.strftime("%Y-%m-%d") for d in spy_df.index if start_date <= d.strftime("%Y-%m-%d") <= end_date]
            return sorted(trading_days)
    except Exception as e:
        logger.warning(f"无法从 SPY 获取交易日: {e}")
    
    # 回退：生成日期范围（跳过周末）
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    trading_days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # 周一到周五
            trading_days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return trading_days


def get_cycle_dates(start_date: str, end_date: str, cycle_type: str = "weekly") -> List[Dict[str, str]]:
    """
    获取周期日期列表
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        cycle_type: 周期类型 ('weekly' 或 'monthly')
    
    Returns:
        周期列表，每个周期包含 start_date 和 end_date
    """
    cycles = []
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    
    if cycle_type == "weekly":
        while current <= end:
            cycle_end = min(current + timedelta(days=6), end)
            cycles.append({
                "start_date": current.strftime("%Y-%m-%d"),
                "end_date": cycle_end.strftime("%Y-%m-%d")
            })
            current = cycle_end + timedelta(days=1)
    elif cycle_type == "monthly":
        while current <= end:
            # 获取当前月份的最后一天
            year = current.year
            month = current.month
            last_day = monthrange(year, month)[1]
            cycle_end_date = datetime(year, month, last_day)
            cycle_end = min(cycle_end_date, end)
            cycles.append({
                "start_date": current.strftime("%Y-%m-%d"),
                "end_date": cycle_end.strftime("%Y-%m-%d")
            })
            # 移动到下个月的第一天
            if month == 12:
                current = datetime(year + 1, 1, 1)
            else:
                current = datetime(year, month + 1, 1)
    
    return cycles


def run_rebalance(
    portfolio: PortfolioManager,
    selector: StockSelector,
    data_adapter: DataAdapter,
    rebalance_date: str,
) -> List[str]:
    """
    执行再平衡：选股并更新组合
    
    Args:
        portfolio: 组合管理器
        selector: 选股器
        data_adapter: 数据适配器
        rebalance_date: 再平衡日期
    
    Returns:
        选中的股票列表
    """
    logger.info(f"[再平衡] 开始选股（日期: {rebalance_date}）...")
    
    try:
        # 执行选股
        selected_stocks = selector.select_stocks(rebalance_date)
        
        if not selected_stocks:
            logger.warning(f"[再平衡] 未选中任何股票，保持当前持仓")
            return portfolio.target_symbols if hasattr(portfolio, 'target_symbols') else []
        
        logger.info(f"[再平衡] 选中股票: {selected_stocks}")
        
        # 获取价格
        prices = {}
        for symbol in selected_stocks:
            price = data_adapter.get_price(symbol, rebalance_date, price_type="close")
            if price:
                prices[symbol] = price
            else:
                logger.warning(f"[再平衡] 无法获取 {symbol} 的价格，跳过")
        
        if not prices:
            logger.error(f"[再平衡] 无法获取任何股票价格，跳过再平衡")
            return portfolio.target_symbols if hasattr(portfolio, 'target_symbols') else []
        
        # 执行再平衡
        actions = portfolio.rebalance(selected_stocks, prices, rebalance_date)
        logger.info(f"[再平衡] 再平衡操作: {actions}")
        
        return selected_stocks
        
    except Exception as e:
        logger.error(f"[再平衡] 再平衡失败: {e}", exc_info=True)
        return portfolio.target_symbols if hasattr(portfolio, 'target_symbols') else []


def run_single_day(
    symbols: List[str],
    trade_date: str,
    llm: Any,
    memory: DatabaseMemory,
    db_helper: MemoryDBHelper,
    portfolio: PortfolioManager,
    data_adapter: DataAdapter,
    output_dir: Path,
    previous_total_value: Optional[float] = None,
) -> Dict[str, Any]:
    """
    运行单日流程（多标的版本）
    
    注意：对于多标的，我们为每个标的分别运行 Pre-Open 分析，
    但 Market Open 和 Post Close 是组合级别的
    """
    day_result = {
        "date": trade_date,
        "symbols": symbols,
        "pre_open": {},
        "market_open": {},
        "post_close": {},
        "errors": []
    }
    
    try:
        # ========== Pre-Open 阶段（为每个标的运行）==========
        logger.info(f"\n[Pre-Open] 开始分析（日期: {trade_date}）...")
        
        pre_open_results = {}
        for symbol in symbols:
            try:
                logger.info(f"  [Pre-Open] 分析 {symbol}...")
                
                # 创建 Pre-Open Graph
                graph = create_trading_graph(
                    llm=llm,
                    memory=memory,
                    db_helper=db_helper,
                    symbol=symbol,
                )
                
                # 准备初始状态
                initial_state: AgentState = {
                    "company_of_interest": symbol,
                    "trade_date": trade_date,
                }
                
                # 运行 Pre-Open Graph
                final_state = graph.invoke(initial_state)
                pre_open_results[symbol] = {
                    "strategy_selection": final_state.get("strategy_selection"),
                    "trader_investment_plan": final_state.get("trader_investment_plan"),
                    "risk_summary": final_state.get("risk_summary"),
                }
                
                logger.info(f"  [Pre-Open] {symbol} 分析完成")
                
            except Exception as e:
                logger.error(f"  [Pre-Open] {symbol} 分析失败: {e}", exc_info=True)
                day_result["errors"].append(f"Pre-Open {symbol}: {str(e)}")
        
        day_result["pre_open"] = pre_open_results
        
        # ========== Market Open 阶段（组合级别）==========
        logger.info(f"\n[Market Open] 开始执行（日期: {trade_date}）...")
        
        try:
            # 创建 market_open 节点
            market_open_node = create_market_open_executor(portfolio, data_adapter)
            
            # 准备状态（使用第一个标的的状态，或合并所有标的的状态）
            primary_symbol = symbols[0] if symbols else None
            market_open_state: AgentState = {
                "company_of_interest": primary_symbol,
                "trade_date": trade_date,
                "strategy_selection": pre_open_results.get(primary_symbol, {}).get("strategy_selection"),
                "trader_investment_plan": pre_open_results.get(primary_symbol, {}).get("trader_investment_plan"),
                "risk_summary": pre_open_results.get(primary_symbol, {}).get("risk_summary"),
            }
            
            market_open_result = market_open_node(market_open_state)
            day_result["market_open"] = market_open_result
            
            logger.info(f"[Market Open] 执行完成")
            
        except Exception as e:
            logger.error(f"[Market Open] 执行失败: {e}", exc_info=True)
            day_result["errors"].append(f"Market Open: {str(e)}")
        
        # ========== Post Close 阶段 ==========
        logger.info(f"\n[Post Close] 开始更新收益（日期: {trade_date}）...")
        
        try:
            post_close_node = create_post_close_node(
                portfolio,
                data_adapter,
                previous_total_value=previous_total_value
            )
            
            post_close_state: AgentState = {
                "company_of_interest": primary_symbol,
                "trade_date": trade_date,
            }
            
            post_close_result = post_close_node(post_close_state)
            day_result["post_close"] = post_close_result
            
            # 更新组合状态
            day_result["portfolio_state"] = portfolio.get_portfolio_state()
            
            # 获取单日收益率和最大回撤
            daily_return = post_close_result.get("daily_return", 0.0)
            max_drawdown = post_close_result.get("max_drawdown", 0.0)
            
            logger.info(f"[Post Close] 更新完成")
            logger.info(f"  总资产: ${portfolio.total_value:,.2f}")
            logger.info(f"  现金: ${portfolio.cash:,.2f}")
            logger.info(f"  持仓市值: ${portfolio.positions_value:,.2f}")
            logger.info(f"  总收益率: {portfolio.total_return:.2f}%")
            logger.info(f"  单日收益率: {daily_return:.2f}%")
            logger.info(f"  最大回撤: {max_drawdown:.2f}%")
            
        except Exception as e:
            logger.error(f"[Post Close] 更新失败: {e}", exc_info=True)
            day_result["errors"].append(f"Post Close: {str(e)}")
        
        # ========== History Maintainer ==========
        logger.info(f"\n[History Maintainer] 开始更新历史摘要...")
        
        try:
            history_maintainer_node = create_history_maintainer_node(llm, db_helper)
            
            for symbol in symbols:
                history_state: AgentState = {
                    "company_of_interest": symbol,
                    "trade_date": trade_date,
                }
                history_maintainer_node(history_state)
            
            logger.info(f"[History Maintainer] 更新完成")
            
        except Exception as e:
            logger.error(f"[History Maintainer] 更新失败: {e}", exc_info=True)
            day_result["errors"].append(f"History Maintainer: {str(e)}")
        
        # ========== 保存 daily summary ==========
        try:
            # 从 Pre-Open 结果中提取标准化字段（使用第一个标的）
            primary_result = pre_open_results.get(primary_symbol, {})
            strategy_selection_str = primary_result.get("strategy_selection", "")
            final_decision_str = primary_result.get("risk_summary", {}).get("final_trade_decision", "") if isinstance(primary_result.get("risk_summary"), dict) else ""
            
            # 解析 JSON
            from tradingagents.agents.utils.json_parser import extract_json_from_text
            
            strategy_selection = {}
            if strategy_selection_str:
                strategy_selection = extract_json_from_text(strategy_selection_str) or {}
            
            final_decision = {}
            if final_decision_str:
                final_decision = extract_json_from_text(final_decision_str) or {}
            
            # 从 Strategy Selector 或 Risk Manager 输出中提取标准化字段
            market_regime = strategy_selection.get("market_regime") or final_decision.get("market_regime")
            selected_strategy = strategy_selection.get("selected_strategy") or final_decision.get("selected_strategy")
            expected_behavior = strategy_selection.get("expected_behavior") or final_decision.get("expected_behavior")
            
            # 从 Post Close 结果中获取单日收益率和最大回撤
            post_close_result = day_result.get("post_close", {})
            actual_return = post_close_result.get("daily_return", 0.0)
            actual_max_drawdown = post_close_result.get("max_drawdown", 0.0)
            
            # 组装 daily summary JSON
            daily_summary = {
                "date": trade_date,
                "symbols": symbols,
                "market_regime": market_regime,
                "selected_strategy": selected_strategy,
                "expected_behavior": expected_behavior,
                "actual_return": actual_return,
                "actual_max_drawdown": actual_max_drawdown,
                "positioning": "full" if portfolio.positions_value > 0 else "empty",
                "anomaly": None,
            }
            
            # 保存到数据库（为每个标的保存一条记录）
            for symbol in symbols:
                db_helper.upsert_daily_trading_summary(
                    date=trade_date,
                    symbol=symbol,
                    market_regime=market_regime,
                    selected_strategy=selected_strategy,
                    expected_behavior=expected_behavior,
                    actual_return=actual_return,
                    actual_max_drawdown=actual_max_drawdown,
                    positioning=daily_summary["positioning"],
                    anomaly=daily_summary["anomaly"],
                    summary_json=json.dumps(daily_summary, ensure_ascii=False)
                )
            
        except Exception as e:
            logger.error(f"[Daily Summary] 保存失败: {e}", exc_info=True)
            day_result["errors"].append(f"Daily Summary: {str(e)}")
        
        # 保存每日结果
        output_file = output_dir / "daily_results" / f"{trade_date}.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(day_result, f, ensure_ascii=False, indent=2, default=str)
        
        return day_result
        
    except Exception as e:
        logger.error(f"[运行单日] 失败: {e}", exc_info=True)
        day_result["errors"].append(f"运行单日: {str(e)}")
        return day_result


def run_cycle_reflection(
    llm: Any,
    db_helper: MemoryDBHelper,
    cycle_type: str,
    cycle_start_date: str,
    cycle_end_date: str,
    symbols: List[str],
) -> Dict[str, Any]:
    """运行周期反思"""
    logger.info(f"\n[Reflector] 开始周期反思（{cycle_type}: {cycle_start_date} ~ {cycle_end_date}）...")
    
    try:
        reflector_node = create_reflector_node(llm=llm, db_helper=db_helper)
        
        # 为每个标的运行反思（或合并所有标的）
        for symbol in symbols:
            state: AgentState = {
                "cycle_type": cycle_type,
                "cycle_start_date": cycle_start_date,
                "cycle_end_date": cycle_end_date,
                "company_of_interest": symbol,
            }
            result = reflector_node(state)
            logger.info(f"  [Reflector] {symbol} 反思完成")
        
        logger.info(f"[Reflector] 周期反思完成")
        return {"status": "ok"}
        
    except Exception as e:
        logger.error(f"[Reflector] 周期反思失败: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


def run_backtest(
    start_date: str,
    end_date: str,
    initial_cash: float,
    cycle_type: str = "weekly",
    top_n: int = 5,
    rebalance_frequency: str = "cycle_start",  # 'cycle_start' 或 'monthly'
    db_path: str = "memory.db",
    output_dir: str = "backtest_results_multi",
) -> Dict[str, Any]:
    """
    运行多标的、多周期回测
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        initial_cash: 初始资金
        cycle_type: 周期类型 ('weekly' 或 'monthly')
        top_n: 每次选股数量
        rebalance_frequency: 再平衡频率 ('cycle_start' 或 'monthly')
        db_path: 数据库路径
        output_dir: 输出目录
    """
    logger.info("=" * 80)
    logger.info("多标的、多周期回测开始")
    logger.info(f"  日期范围: {start_date} ~ {end_date}")
    logger.info(f"  初始资金: ${initial_cash:,.2f}")
    logger.info(f"  周期类型: {cycle_type}")
    logger.info(f"  选股数量: {top_n}")
    logger.info("=" * 80)
    
    # 初始化
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    (output_path / "daily_results").mkdir(exist_ok=True)
    
    llm = load_llm_from_config()
    db_helper = MemoryDBHelper(db_path)
    data_adapter = DataAdapter(use_cache=True)
    
    portfolio = PortfolioManager(initial_cash=initial_cash, max_positions=top_n)
    
    # 创建选股器（使用较小的股票池进行测试）
    test_pool = STOCK_POOL[:30]  # 使用前30只股票
    selector = StockSelector(
        stock_pool=test_pool,
        top_n=top_n,
        data_adapter=data_adapter
    )
    
    # 获取周期列表
    cycles = get_cycle_dates(start_date, end_date, cycle_type)
    logger.info(f"\n共 {len(cycles)} 个周期")
    
    # 运行每个周期
    all_daily_results = []
    previous_total_value = initial_cash
    
    for cycle_idx, cycle in enumerate(cycles, 1):
        cycle_start = cycle["start_date"]
        cycle_end = cycle["end_date"]
        
        logger.info("\n" + "=" * 80)
        logger.info(f"周期 {cycle_idx}/{len(cycles)}: {cycle_start} ~ {cycle_end}")
        logger.info("=" * 80)
        
        # ========== 周期开始：再平衡 ==========
        if rebalance_frequency == "cycle_start" or (rebalance_frequency == "monthly" and cycle_type == "monthly"):
            selected_symbols = run_rebalance(
                portfolio=portfolio,
                selector=selector,
                data_adapter=data_adapter,
                rebalance_date=cycle_start,
            )
            if not selected_symbols:
                logger.warning(f"[周期 {cycle_idx}] 再平衡未选中股票，使用上一周期的持仓")
                selected_symbols = portfolio.target_symbols if hasattr(portfolio, 'target_symbols') and portfolio.target_symbols else ["AAPL"]  # 默认
        else:
            # 使用上一周期的持仓
            selected_symbols = portfolio.target_symbols if hasattr(portfolio, 'target_symbols') and portfolio.target_symbols else ["AAPL"]
        
        logger.info(f"[周期 {cycle_idx}] 当前持仓标的: {selected_symbols}")
        
        # ========== 周期内每日交易 ==========
        trading_days = get_trading_days(data_adapter, cycle_start, cycle_end)
        logger.info(f"[周期 {cycle_idx}] 交易日数: {len(trading_days)}")
        
        for day_idx, trade_date in enumerate(trading_days, 1):
            logger.info(f"\n[周期 {cycle_idx}] 交易日 {day_idx}/{len(trading_days)}: {trade_date}")
            
            try:
                # 为每个标的创建 Memory（简化：使用第一个标的的 Memory）
                primary_symbol = selected_symbols[0] if selected_symbols else "AAPL"
                memory = DatabaseMemory(db_path, primary_symbol, limit=10)
                
                day_result = run_single_day(
                    symbols=selected_symbols,
                    trade_date=trade_date,
                    llm=llm,
                    memory=memory,
                    db_helper=db_helper,
                    portfolio=portfolio,
                    data_adapter=data_adapter,
                    output_dir=output_path,
                    previous_total_value=previous_total_value,
                )
                
                all_daily_results.append(day_result)
                
                # 更新 previous_total_value
                if day_result.get("post_close"):
                    portfolio_state = day_result.get("portfolio_state", {})
                    previous_total_value = portfolio_state.get("total_value", previous_total_value)
                
            except Exception as e:
                logger.error(f"[周期 {cycle_idx}] 交易日 {trade_date} 失败: {e}", exc_info=True)
                continue
        
        # ========== 周期结束：Reflector ==========
        logger.info(f"\n[周期 {cycle_idx}] 周期结束，运行 Reflector...")
        reflection_result = run_cycle_reflection(
            llm=llm,
            db_helper=db_helper,
            cycle_type=cycle_type,
            cycle_start_date=cycle_start,
            cycle_end_date=cycle_end,
            symbols=selected_symbols,
        )
        logger.info(f"[周期 {cycle_idx}] Reflector 完成: {reflection_result.get('status')}")
    
    # 生成最终报告
    final_report = {
        "start_date": start_date,
        "end_date": end_date,
        "initial_cash": initial_cash,
        "final_total_value": portfolio.total_value,
        "total_return": portfolio.total_return,
        "total_days": len(all_daily_results),
        "total_cycles": len(cycles),
        "final_portfolio_state": portfolio.get_portfolio_state(),
    }
    
    report_file = output_path / "backtest_report.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(final_report, f, ensure_ascii=False, indent=2, default=str)
    
    logger.info("\n" + "=" * 80)
    logger.info("回测完成")
    logger.info(f"  初始资金: ${initial_cash:,.2f}")
    logger.info(f"  最终资产: ${portfolio.total_value:,.2f}")
    logger.info(f"  总收益率: {portfolio.total_return:.2f}%")
    logger.info("=" * 80)
    
    db_helper.close()
    
    return final_report


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="多标的、多周期回测")
    parser.add_argument("--start", type=str, default="2024-01-02", help="开始日期")
    parser.add_argument("--end", type=str, default="2024-01-31", help="结束日期")
    parser.add_argument("--cash", type=float, default=100000.0, help="初始资金")
    parser.add_argument("--cycle", type=str, default="weekly", choices=["weekly", "monthly"], help="周期类型")
    parser.add_argument("--top-n", type=int, default=5, help="选股数量")
    parser.add_argument("--rebalance", type=str, default="cycle_start", choices=["cycle_start", "monthly"], help="再平衡频率")
    parser.add_argument("--db", type=str, default="memory.db", help="数据库路径")
    parser.add_argument("--output", type=str, default="backtest_results_multi", help="输出目录")
    
    args = parser.parse_args()
    
    try:
        run_backtest(
            start_date=args.start,
            end_date=args.end,
            initial_cash=args.cash,
            cycle_type=args.cycle,
            top_n=args.top_n,
            rebalance_frequency=args.rebalance,
            db_path=args.db,
            output_dir=args.output,
        )
    except KeyboardInterrupt:
        logger.info("\n用户中断")
        sys.exit(0)
    except Exception as e:
        logger.error(f"回测失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

