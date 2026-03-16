#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
组合管理器

管理多股票投资组合，包括仓位分配、再平衡等。
参考 trading_sys/core/portfolio.py 的逻辑。
"""

from typing import Dict, List, Optional, Any
from datetime import datetime


class PortfolioManager:
    """
    组合管理器
    
    管理多股票投资组合的整体状态，包括：
    - 总资产、现金、持仓市值
    - 每个股票的仓位状态
    - 交易记录
    - 再平衡逻辑
    """
    
    def __init__(self, initial_cash: float, max_positions: int = 5):
        """
        初始化组合管理器
        
        Args:
            initial_cash: 初始资金
            max_positions: 最大持仓数量
        """
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.max_positions = max_positions
        
        # 每个股票的仓位状态 {symbol: {shares, entry_price, entry_date, current_price, pnl, pnl_pct}}
        self.positions: Dict[str, Dict[str, Any]] = {}
        
        # 目标股票列表（选股结果）
        self.target_symbols: List[str] = []
        
        # 交易记录
        self.trades: List[Dict[str, Any]] = []
    
    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取单个股票的仓位状态"""
        return self.positions.get(symbol)
    
    def get_target_allocation(self, symbol: str) -> float:
        """获取目标仓位分配（均分）"""
        if symbol not in self.target_symbols or len(self.target_symbols) == 0:
            return 0.0
        return 1.0 / len(self.target_symbols)
    
    def get_target_amount(self, symbol: str) -> float:
        """获取目标金额（基于总资产均分）"""
        allocation = self.get_target_allocation(symbol)
        return self.total_value * allocation
    
    @property
    def positions_value(self) -> float:
        """持仓市值总和"""
        return sum(p.get("market_value", 0.0) for p in self.positions.values())
    
    @property
    def total_value(self) -> float:
        """总资产（现金 + 持仓市值）"""
        return self.cash + self.positions_value
    
    @property
    def total_return(self) -> float:
        """总收益率"""
        if self.initial_cash == 0:
            return 0.0
        return (self.total_value / self.initial_cash - 1) * 100
    
    def update_position(
        self,
        symbol: str,
        shares: float,
        entry_price: float,
        entry_date: str,
        current_price: float,
        strategy_type: Optional[str] = None,
        stop_loss_price: Optional[float] = None,
        take_profit_price: Optional[float] = None,
    ):
        """更新单个股票的仓位状态"""
        market_value = shares * current_price
        cost_basis = shares * entry_price
        pnl = market_value - cost_basis
        pnl_pct = (current_price / entry_price - 1) * 100 if entry_price > 0 else 0.0
        
        self.positions[symbol] = {
            "symbol": symbol,
            "shares": shares,
            "entry_price": entry_price,
            "entry_date": entry_date,
            "current_price": current_price,
            "market_value": market_value,
            "cost_basis": cost_basis,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "strategy_type": strategy_type,
            "stop_loss_price": stop_loss_price,
            "take_profit_price": take_profit_price,
        }
    
    def execute_buy(
        self,
        symbol: str,
        price: float,
        amount: Optional[float] = None,
        shares: Optional[float] = None,
        date: Optional[str] = None,
        strategy_type: Optional[str] = None,
        stop_loss_price: Optional[float] = None,
        take_profit_price: Optional[float] = None,
        reason: str = "",
    ) -> bool:
        """执行买入"""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        if shares is None:
            if amount is None:
                amount = self.get_target_amount(symbol)
            shares = int(amount / price)
        
        if shares <= 0:
            return False
        
        cost = shares * price
        
        if cost > self.cash:
            shares = int(self.cash / price)
            cost = shares * price
        
        if shares <= 0:
            return False
        
        self.cash -= cost
        
        current_position = self.positions.get(symbol)
        if current_position:
            # 加仓
            old_shares = current_position["shares"]
            old_cost = current_position["cost_basis"]
            total_shares = old_shares + shares
            avg_price = (old_cost + cost) / total_shares
            
            self.update_position(
                symbol=symbol,
                shares=total_shares,
                entry_price=avg_price,
                entry_date=current_position["entry_date"],
                current_price=price,
                strategy_type=strategy_type or current_position.get("strategy_type"),
                stop_loss_price=stop_loss_price or current_position.get("stop_loss_price"),
                take_profit_price=take_profit_price or current_position.get("take_profit_price"),
            )
        else:
            # 新建仓
            self.update_position(
                symbol=symbol,
                shares=shares,
                entry_price=price,
                entry_date=date,
                current_price=price,
                strategy_type=strategy_type,
                stop_loss_price=stop_loss_price,
                take_profit_price=take_profit_price,
            )
        
        self.trades.append({
            "date": date,
            "symbol": symbol,
            "action": "buy",
            "shares": shares,
            "price": price,
            "amount": cost,
            "reason": reason,
        })
        
        return True
    
    def execute_sell(
        self,
        symbol: str,
        price: float,
        shares: Optional[float] = None,
        date: Optional[str] = None,
        reason: str = "",
    ) -> bool:
        """执行卖出"""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        if symbol not in self.positions:
            return False
        
        position = self.positions[symbol]
        
        if shares is None:
            shares = position["shares"]
        else:
            shares = min(shares, position["shares"])
        
        if shares <= 0:
            return False
        
        proceeds = shares * price
        self.cash += proceeds
        
        remaining_shares = position["shares"] - shares
        if remaining_shares <= 0:
            del self.positions[symbol]
        else:
            self.update_position(
                symbol=symbol,
                shares=remaining_shares,
                entry_price=position["entry_price"],
                entry_date=position["entry_date"],
                current_price=price,
                strategy_type=position.get("strategy_type"),
                stop_loss_price=position.get("stop_loss_price"),
                take_profit_price=position.get("take_profit_price"),
            )
        
        self.trades.append({
            "date": date,
            "symbol": symbol,
            "action": "sell",
            "shares": shares,
            "price": price,
            "amount": proceeds,
            "reason": reason,
        })
        
        return True
    
    def rebalance(self, target_symbols: List[str], prices: Dict[str, float], date: str) -> Dict[str, str]:
        """再平衡持仓"""
        self.target_symbols = target_symbols
        actions = {}
        
        # 卖出不在目标列表的股票
        current_symbols = list(self.positions.keys())
        for symbol in current_symbols:
            if symbol not in target_symbols:
                if symbol in prices:
                    self.execute_sell(symbol, prices[symbol], date=date, reason="再平衡-移出")
                    actions[symbol] = "sell"
        
        # 买入目标列表中未持有的股票
        target_amount = self.total_value / len(target_symbols) if len(target_symbols) > 0 else 0
        for symbol in target_symbols:
            if symbol not in self.positions and symbol in prices:
                self.execute_buy(symbol, prices[symbol], amount=target_amount, date=date, reason="再平衡-买入")
                actions[symbol] = "buy"
        
        return actions
    
    def get_recent_trades(self, days: int = 1) -> List[Dict[str, Any]]:
        """
        获取最近 N 天的交易记录
        
        Args:
            days: 最近天数，默认 1 天
            
        Returns:
            交易记录列表
        """
        from datetime import datetime, timedelta
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        return [t for t in self.trades if t.get("date", "") >= cutoff_date]
    
    def get_portfolio_state(self) -> Dict[str, Any]:
        """获取组合状态"""
        return {
            "total_value": self.total_value,
            "cash": self.cash,
            "positions_value": self.positions_value,
            "total_return": self.total_return,
            "positions": self.positions.copy(),
            "recent_trades": self.trades[-10:],
        }

