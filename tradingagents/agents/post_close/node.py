#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
收盘后收益整理节点

在 post_close 阶段整理每日收益、更新仓位价格、记录交易日志等。
"""

from typing import Dict, Any, Callable, Optional

from tradingagents.agents.utils.agentstate.agent_states import AgentState
from tradingagents.core.data_adapter import DataAdapter
from tradingagents.core.portfolio.portfolio_manager import PortfolioManager


def create_post_close_node(
    portfolio_manager: PortfolioManager,
    data_adapter: DataAdapter,
    previous_total_value: Optional[float] = None,
) -> Callable[[AgentState], Dict[str, Any]]:
    """
    创建收盘后收益整理节点
    
    Args:
        portfolio_manager: 组合管理器
        data_adapter: 数据适配器
    
    Returns:
        post_close_node: 收盘后节点函数
    """
    
    def post_close_node(state: AgentState) -> Dict[str, Any]:
        """
        整理每日收益
        
        流程：
        1. 获取当前日期（收盘后）
        2. 更新所有持仓的当前价格（使用收盘价）
        3. 计算每日收益
        4. 更新组合状态
        5. 记录交易日志
        """
        trade_date = state.get("trade_date", "")
        
        if not trade_date:
            print(f"[PostClose] 缺少必要参数: trade_date={trade_date}")
            return {}
        
        # 1. 获取所有持仓
        portfolio_state = portfolio_manager.get_portfolio_state()
        all_positions = portfolio_state.get("positions", {})
        
        if not all_positions:
            print(f"[PostClose] 无持仓，跳过更新")
            return {
                "current_position": None,
                "portfolio_state": portfolio_state,
            }
        
        # 2. 更新所有持仓的当前价格（使用收盘价）
        updated_count = 0
        failed_symbols = []
        
        for symbol, position in all_positions.items():
            close_price = data_adapter.get_price(symbol, trade_date, "close")
            if close_price is None:
                failed_symbols.append(symbol)
                print(f"[PostClose] {symbol} 无法获取 {trade_date} 的收盘价")
                continue
            
            # 更新当前价格
            portfolio_manager.update_position(
                symbol=symbol,
                shares=position["shares"],
                entry_price=position["entry_price"],
                entry_date=position["entry_date"],
                current_price=close_price,
                strategy_type=position.get("strategy_type"),
                stop_loss_price=position.get("stop_loss_price"),
                take_profit_price=position.get("take_profit_price"),
            )
            updated_count += 1
        
        print(f"[PostClose] 更新了 {updated_count} 个持仓的价格，失败 {len(failed_symbols)} 个")
        if failed_symbols:
            print(f"[PostClose] 更新失败的股票: {', '.join(failed_symbols)}")
        
        # 3. 计算单日收益率（相对于前一天的总资产）
        daily_return = 0.0
        if previous_total_value is not None and previous_total_value > 0:
            current_total_value = portfolio_manager.total_value
            daily_return = ((current_total_value / previous_total_value) - 1) * 100
        
        # 4. 计算最大回撤（简化版：基于持仓的当前价格和建仓价格）
        max_drawdown = 0.0
        for symbol, position in all_positions.items():
            entry_price = position.get("entry_price", 0)
            current_price = position.get("current_price", 0)
            if entry_price > 0 and current_price < entry_price:
                drawdown = ((current_price / entry_price) - 1) * 100
                max_drawdown = min(max_drawdown, drawdown)  # 取最小值（最负的值）
        
        # 5. 更新组合状态
        updated_portfolio_state = portfolio_manager.get_portfolio_state()
        symbol = state.get("company_of_interest", "")
        updated_position = portfolio_manager.get_position(symbol) if symbol else None
        
        return {
            "current_position": updated_position,
            "portfolio_state": updated_portfolio_state,
            "daily_return": daily_return,
            "max_drawdown": max_drawdown,
        }
    
    return post_close_node

