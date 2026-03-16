#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
市场开盘交易执行节点

在 market_open 阶段执行交易，根据 pre_open 阶段的决策进行下单。
"""

from typing import Dict, Any, Callable, Optional
import json

from tradingagents.agents.utils.agentstate.agent_states import AgentState
from tradingagents.core.strategies.strategy_lib import execute_strategy, Signal
from tradingagents.core.data_adapter import DataAdapter
from tradingagents.core.portfolio.portfolio_manager import PortfolioManager
from tradingagents.agents.utils.json_parser import extract_json_from_text


def create_market_open_executor(
    portfolio_manager: PortfolioManager,
    data_adapter: DataAdapter,
) -> Callable[[AgentState], Dict[str, Any]]:
    """
    创建市场开盘交易执行节点
    
    Args:
        portfolio_manager: 组合管理器
        data_adapter: 数据适配器
    
    Returns:
        market_open_node: 市场开盘节点函数
    """
    
    def market_open_node(state: AgentState) -> Dict[str, Any]:
        """
        执行交易
        
        流程：
        1. 检查风险决策（如果被否定，不执行）
        2. 读取策略选择和交易计划
        3. 加载股票数据（截止到 T 日）
        4. 获取当前持仓状态
        5. 执行策略
        6. 根据信号和 Trader 建议决定是否下单
        7. 获取 T+1 日开盘价（实际执行价格）
        8. 执行交易（考虑目标仓位分配）
        9. 更新仓位状态
        """
        symbol = state.get("company_of_interest", "")
        trade_date = state.get("trade_date", "")
        
        if not symbol or not trade_date:
            print(f"[MarketOpen] 缺少必要参数: symbol={symbol}, trade_date={trade_date}")
            return {}
        
        # 1. 检查风险决策（解析 JSON 获取 final_decision）
        risk_summary = state.get("risk_summary")
        risk_decision = None
        if risk_summary:
            final_decision_str = risk_summary.get("final_trade_decision", "")
            if final_decision_str:
                try:
                    final_decision_json = extract_json_from_text(final_decision_str)
                    if final_decision_json:
                        risk_decision = final_decision_json.get("final_decision", "").upper()
                except:
                    # 如果解析失败，回退到字符串匹配
                    if "HOLD" in final_decision_str.upper():
                        risk_decision = "HOLD"
        
        if risk_decision == "HOLD":
            print(f"[MarketOpen] {symbol} 风险决策为 HOLD，不执行交易")
            # 即使不执行交易，也要返回当前仓位状态
            return {
                "current_position": portfolio_manager.get_position(symbol),
                "portfolio_state": portfolio_manager.get_portfolio_state(),
                "execution_log": [{
                    "timestamp": trade_date,
                    "action": "hold",
                    "price": 0.0,
                    "volume": 0,
                    "reason": "风险决策为 HOLD",
                }],
            }
        
        # 2. 读取策略选择和交易计划
        strategy_selection = state.get("strategy_selection")
        if not strategy_selection:
            print(f"[MarketOpen] {symbol} 没有策略选择结果")
            return {}
        
        strategy_type = strategy_selection.get("strategy_type")
        if not strategy_type:
            print(f"[MarketOpen] {symbol} 策略类型为空")
            return {}
        
        # 3. 获取当前持仓状态
        current_position = portfolio_manager.get_position(symbol)
        is_holding = current_position is not None
        
        # 4. 加载股票数据（截止到 T 日）
        df = data_adapter.load_stock_data_until(symbol, trade_date)
        if df is None or len(df) < 50:
            print(f"[MarketOpen] {symbol} 数据不足，无法执行策略")
            return {}
        
        # 5. 执行策略
        try:
            strategy_result = execute_strategy(strategy_type, df, is_holding=is_holding)
        except Exception as e:
            print(f"[MarketOpen] {symbol} 策略执行失败: {e}")
            return {}
        
        # 6. 获取 T+1 日开盘价（实际执行价格）
        next_trading_day = data_adapter.get_next_trading_day(trade_date)
        if not next_trading_day:
            print(f"[MarketOpen] {symbol} 无法获取下一个交易日")
            return {}
        
        execution_price = data_adapter.get_price(symbol, next_trading_day, "open")
        if execution_price is None:
            print(f"[MarketOpen] {symbol} 无法获取 {next_trading_day} 的开盘价")
            return {}
        
        # 7. 检查当天是否已经执行过交易（确保每天只执行一次）
        # 检查交易记录，如果当天已经有交易，则不执行
        today_trades = [t for t in portfolio_manager.trades if t.get("date") == trade_date]
        if today_trades:
            print(f"[MarketOpen] {symbol} 当天 ({trade_date}) 已执行过交易，跳过")
            return {
                "current_position": portfolio_manager.get_position(symbol),
                "portfolio_state": portfolio_manager.get_portfolio_state(),
                "execution_log": [{
                    "timestamp": trade_date,
                    "action": "hold",
                    "price": 0.0,
                    "volume": 0,
                    "reason": "当天已执行过交易",
                }],
            }
        
        # 8. 根据信号决定是否执行交易
        executed = False
        execution_reason = strategy_result.reason
        
        # 读取 Trader 的交易计划
        trader_plan_str = state.get("trader_investment_plan", "")
        trader_action = None
        if trader_plan_str:
            try:
                trader_json = extract_json_from_text(trader_plan_str)
                if trader_json:
                    trader_action = trader_json.get("action", "").upper()
            except:
                pass
        
        if strategy_result.signal == Signal.BUY and not is_holding:
            # 买入信号且未持仓
            if trader_action and trader_action == "HOLD":
                execution_reason = f"策略信号为 BUY，但 Trader 建议 HOLD，不执行"
            else:
                target_amount = portfolio_manager.get_target_amount(symbol)
                executed = portfolio_manager.execute_buy(
                    symbol=symbol,
                    price=execution_price,
                    amount=target_amount,
                    date=next_trading_day,
                    strategy_type=strategy_type,
                    stop_loss_price=strategy_result.stop_loss_price,
                    take_profit_price=strategy_result.take_profit_price,
                    reason=f"{strategy_type}: {strategy_result.reason}",
                )
                if executed:
                    execution_reason = f"执行买入: {strategy_result.reason}"
                    
        elif strategy_result.signal == Signal.SELL and is_holding:
            # 卖出信号且已持仓
            if trader_action and trader_action == "HOLD":
                execution_reason = f"策略信号为 SELL，但 Trader 建议 HOLD，不执行"
            else:
                executed = portfolio_manager.execute_sell(
                    symbol=symbol,
                    price=execution_price,
                    date=next_trading_day,
                    reason=f"{strategy_type}: {strategy_result.reason}",
                )
                if executed:
                    execution_reason = f"执行卖出: {strategy_result.reason}"
        
        # 9. 更新 AgentState 中的仓位信息
        updated_position = portfolio_manager.get_position(symbol)
        portfolio_state = portfolio_manager.get_portfolio_state()
        
        return {
            "current_position": updated_position,
            "portfolio_state": portfolio_state,
            "execution_log": [{
                "timestamp": next_trading_day,
                "action": "buy" if executed and strategy_result.signal == Signal.BUY else ("sell" if executed and strategy_result.signal == Signal.SELL else "hold"),
                "price": execution_price if executed else 0.0,
                "volume": updated_position["shares"] if updated_position and executed else 0,
                "reason": execution_reason,
            }],
        }
    
    return market_open_node

