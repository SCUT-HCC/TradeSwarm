#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
策略库模块
实现5个语义正交的日频交易策略，供LLM Agent选择使用

策略特点：
1. 完全确定性（无随机性）
2. 参数固定（行业默认值）
3. 语义正交（不同市场假设）
4. 统一接口（可插拔）
"""

import pandas as pd
import numpy as np
from typing import Dict

from .types import Signal, StrategyResult


# ==================== 策略1: 趋势跟踪策略 ====================
# 市场假设：趋势会延续
# 核心逻辑：均线系统 + 趋势确认

def trend_following_strategy(df: pd.DataFrame, is_holding: bool = False) -> StrategyResult:
    """
    趋势跟踪策略
    
    参数（固定）：
    - MA20, MA50（行业标准）
    - ATR(14) 用于止损
    
    买入信号：价格 > MA20 > MA50，且MA20向上
    卖出信号：价格 < MA20 或 MA20 < MA50
    """
    if df is None or len(df) < 50:
        return StrategyResult(Signal.HOLD, 0.0, 0.0, 0.0, "数据不足")
    
    close = df['Close']
    high = df['High']
    low = df['Low']
    
    # 计算指标（固定参数）
    ma20 = close.rolling(20).mean()
    ma50 = close.rolling(50).mean()
    
    # ATR计算（用于止损）
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    
    current_price = close.iloc[-1]
    current_ma20 = ma20.iloc[-1]
    current_ma50 = ma50.iloc[-1]
    current_atr = atr.iloc[-1] if not atr.empty and not pd.isna(atr.iloc[-1]) else current_price * 0.02
    
    # 计算MA20斜率（判断趋势方向）
    ma20_slope = (ma20.iloc[-1] - ma20.iloc[-5]) / ma20.iloc[-5] if len(ma20) >= 5 else 0
    
    if is_holding:
        # 已持仓：寻找卖出信号
        if current_price < current_ma20 or current_ma20 < current_ma50:
            stop_loss = current_price - 2 * current_atr
            take_profit = current_price + 3 * current_atr
            return StrategyResult(
                Signal.SELL, 
                0.7, 
                stop_loss, 
                take_profit,
                f"趋势反转: 价格{current_price:.2f} < MA20{current_ma20:.2f} 或 MA20 < MA50"
            )
        else:
            # 持有，更新止损止盈
            stop_loss = current_price - 2 * current_atr
            take_profit = current_price + 3 * current_atr
            return StrategyResult(Signal.HOLD, 0.5, stop_loss, take_profit, "趋势延续")
    else:
        # 未持仓：寻找买入信号
        if current_price > current_ma20 > current_ma50 and ma20_slope > 0:
            stop_loss = current_price - 2 * current_atr
            take_profit = current_price + 3 * current_atr
            confidence = min(0.9, 0.5 + abs(ma20_slope) * 10)
            return StrategyResult(
                Signal.BUY,
                confidence,
                stop_loss,
                take_profit,
                f"趋势确认: 价格{current_price:.2f} > MA20{current_ma20:.2f} > MA50{current_ma50:.2f}, MA20向上"
            )
        else:
            return StrategyResult(Signal.HOLD, 0.3, 0.0, 0.0, "无趋势信号")


# ==================== 策略2: 均值回归策略 ====================
# 市场假设：价格会回归均值
# 核心逻辑：RSI + 布林带

def mean_reversion_strategy(df: pd.DataFrame, is_holding: bool = False) -> StrategyResult:
    """
    均值回归策略
    
    参数（固定）：
    - RSI(14) - 行业标准
    - 布林带(20, 1.5) - 放宽到1.5倍标准差，提高信号频率
    - 固定止损6%，止盈10%
    
    买入信号：RSI < 35 且 价格 < 布林带下轨（放宽条件）
    卖出信号：RSI > 65 或 价格 > 布林带上轨（放宽条件）
    """
    if df is None or len(df) < 20:
        return StrategyResult(Signal.HOLD, 0.0, 0.0, 0.0, "数据不足")
    
    close = df['Close']
    
    # 计算RSI（固定参数14）
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    current_rsi = rsi.iloc[-1] if not rsi.empty and not pd.isna(rsi.iloc[-1]) else 50
    
    # 计算布林带（固定参数20, 1.5）- 放宽到1.5倍标准差
    ma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    upper_band = ma20 + 1.5 * std20
    lower_band = ma20 - 1.5 * std20
    
    current_price = close.iloc[-1]
    current_upper = upper_band.iloc[-1] if not upper_band.empty else current_price * 1.1
    current_lower = lower_band.iloc[-1] if not lower_band.empty else current_price * 0.9
    
    # 固定止损止盈（调整）
    stop_loss_pct = 0.06  # 6%
    take_profit_pct = 0.10  # 10%
    
    if is_holding:
        # 已持仓：寻找卖出信号（放宽条件）
        if current_rsi > 65 or current_price > current_upper:
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = current_price * (1 + take_profit_pct)
            return StrategyResult(
                Signal.SELL,
                0.8,
                stop_loss,
                take_profit,
                f"超买回归: RSI{current_rsi:.1f} > 70 或 价格{current_price:.2f} > 上轨{current_upper:.2f}"
            )
        else:
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = current_price * (1 + take_profit_pct)
            return StrategyResult(Signal.HOLD, 0.5, stop_loss, take_profit, "等待回归")
    else:
        # 未持仓：寻找买入信号（放宽条件：RSI < 35）
        if current_rsi < 35 and current_price < current_lower:
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = current_price * (1 + take_profit_pct)
            confidence = 0.5 + (35 - current_rsi) / 35 * 0.4  # RSI越低，置信度越高
            return StrategyResult(
                Signal.BUY,
                confidence,
                stop_loss,
                take_profit,
                f"超卖反弹: RSI{current_rsi:.1f} < 35 且 价格{current_price:.2f} < 下轨{current_lower:.2f}"
            )
        else:
            return StrategyResult(Signal.HOLD, 0.3, 0.0, 0.0, "无超卖信号")


# ==================== 策略3: 动量突破策略 ====================
# 市场假设：强势突破会延续
# 核心逻辑：价格突破 + 成交量确认

def momentum_breakout_strategy(df: pd.DataFrame, is_holding: bool = False) -> StrategyResult:
    """
    动量突破策略
    
    参数（固定）：
    - 突破周期：20日
    - 成交量倍数：1.2倍（放宽条件）
    - 固定止损7%，止盈18%
    
    买入信号：价格突破20日高点 且 成交量 > 1.2倍20日均量（放宽条件）
    卖出信号：价格跌破10日低点
    """
    if df is None or len(df) < 20:
        return StrategyResult(Signal.HOLD, 0.0, 0.0, 0.0, "数据不足")
    
    close = df['Close']
    volume = df['Volume']
    
    # 计算突破位
    high_20d = close.rolling(20).max()
    low_10d = close.rolling(10).min()
    
    # 计算成交量
    avg_volume_20d = volume.rolling(20).mean()
    
    current_price = close.iloc[-1]
    current_high_20d = high_20d.iloc[-2] if len(high_20d) > 1 else current_price  # 前一日高点
    current_low_10d = low_10d.iloc[-1] if not low_10d.empty else current_price
    current_volume = volume.iloc[-1]
    current_avg_volume = avg_volume_20d.iloc[-1] if not avg_volume_20d.empty else current_volume
    
    # 固定止损止盈（调整）
    stop_loss_pct = 0.07  # 7%
    take_profit_pct = 0.18  # 18%
    
    if is_holding:
        # 已持仓：寻找卖出信号
        if current_price < current_low_10d:
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = current_price * (1 + take_profit_pct)
            return StrategyResult(
                Signal.SELL,
                0.7,
                stop_loss,
                take_profit,
                f"突破失败: 价格{current_price:.2f} < 10日低点{current_low_10d:.2f}"
            )
        else:
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = current_price * (1 + take_profit_pct)
            return StrategyResult(Signal.HOLD, 0.5, stop_loss, take_profit, "突破延续")
    else:
        # 未持仓：寻找买入信号（放宽成交量条件：1.2倍）
        volume_ratio = current_volume / current_avg_volume if current_avg_volume > 0 else 1
        if current_price > current_high_20d and volume_ratio > 1.2:
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = current_price * (1 + take_profit_pct)
            confidence = min(0.9, 0.5 + (volume_ratio - 1.2) / 1.2 * 0.4)  # 成交量越大，置信度越高
            return StrategyResult(
                Signal.BUY,
                confidence,
                stop_loss,
                take_profit,
                f"突破确认: 价格{current_price:.2f} > 20日高点{current_high_20d:.2f}, 成交量{volume_ratio:.2f}倍"
            )
        else:
            return StrategyResult(Signal.HOLD, 0.3, 0.0, 0.0, "无突破信号")


# ==================== 策略4: 反转策略 ====================
# 市场假设：极端价格会反转
# 核心逻辑：极端价格 + 成交量异常

def reversal_strategy(df: pd.DataFrame, is_holding: bool = False) -> StrategyResult:
    """
    反转策略（方案一：相对位置 + RSI）
    
    参数（固定）：
    - 相对位置：底部25%，顶部75%
    - RSI阈值：35（超卖）/ 65（超买）
    - 成交量倍数：1.2倍（放宽条件）
    - 固定止损6%，止盈12%
    
    买入信号：价格在20日区间底部25% 且 RSI < 35 且 成交量 > 1.2倍均量
    卖出信号：价格在20日区间顶部75% 且 RSI > 65
    """
    if df is None or len(df) < 20:
        return StrategyResult(Signal.HOLD, 0.0, 0.0, 0.0, "数据不足")
    
    close = df['Close']
    volume = df['Volume']
    
    # 计算20日区间
    low_20d = close.rolling(20).min()
    high_20d = close.rolling(20).max()
    range_20d = high_20d - low_20d
    avg_volume_20d = volume.rolling(20).mean()
    
    # 计算RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    current_price = close.iloc[-1]
    current_low_20d = low_20d.iloc[-1] if not low_20d.empty else current_price
    current_high_20d = high_20d.iloc[-1] if not high_20d.empty else current_price
    current_range = range_20d.iloc[-1] if not range_20d.empty else current_price * 0.1
    current_rsi = rsi.iloc[-1] if not rsi.empty and not pd.isna(rsi.iloc[-1]) else 50
    current_volume = volume.iloc[-1]
    current_avg_volume = avg_volume_20d.iloc[-1] if not avg_volume_20d.empty else current_volume
    
    # 计算价格相对位置（0-100%）
    if current_range > 0:
        price_position = (current_price - current_low_20d) / current_range * 100
    else:
        price_position = 50  # 如果区间为0，默认中间位置
    
    # 固定止损止盈
    stop_loss_pct = 0.06  # 6%
    take_profit_pct = 0.12  # 12%
    
    if is_holding:
        # 已持仓：寻找卖出信号（价格在顶部75% 且 RSI > 65）
        if price_position > 75 and current_rsi > 65:
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = current_price * (1 + take_profit_pct)
            return StrategyResult(
                Signal.SELL,
                0.8,
                stop_loss,
                take_profit,
                f"超买反转: 价格位置{price_position:.1f}% > 75%, RSI{current_rsi:.1f} > 65"
            )
        else:
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = current_price * (1 + take_profit_pct)
            return StrategyResult(Signal.HOLD, 0.5, stop_loss, take_profit, "等待反转")
    else:
        # 未持仓：寻找买入信号（价格在底部25% 且 RSI < 35 且 成交量 > 1.2倍）
        volume_ratio = current_volume / current_avg_volume if current_avg_volume > 0 else 1
        if price_position < 25 and current_rsi < 35 and volume_ratio > 1.2:
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = current_price * (1 + take_profit_pct)
            # 置信度：RSI越低、位置越低、成交量越大，置信度越高
            rsi_factor = (35 - current_rsi) / 35 * 0.3  # RSI贡献最多30%
            position_factor = (25 - price_position) / 25 * 0.2  # 位置贡献最多20%
            volume_factor = min(0.2, (volume_ratio - 1.2) / 1.2 * 0.2)  # 成交量贡献最多20%
            confidence = 0.5 + rsi_factor + position_factor + volume_factor
            confidence = min(0.9, confidence)  # 限制在0.9以内
            return StrategyResult(
                Signal.BUY,
                confidence,
                stop_loss,
                take_profit,
                f"超卖反弹: 价格位置{price_position:.1f}% < 25%, RSI{current_rsi:.1f} < 35, 成交量{volume_ratio:.2f}倍"
            )
        else:
            return StrategyResult(Signal.HOLD, 0.3, 0.0, 0.0, "无反转信号")


# ==================== 策略5: 震荡区间策略 ====================
# 市场假设：价格在区间内震荡
# 核心逻辑：支撑阻力 + 区间识别

def range_trading_strategy(df: pd.DataFrame, is_holding: bool = False) -> StrategyResult:
    """
    震荡区间策略
    
    参数（固定）：
    - 区间周期：20日
    - RSI阈值：35/65（放宽条件）
    - 固定止损5%，止盈8%（调整）
    
    买入信号：价格触及20日低点附近（下轨）且 RSI < 35（放宽条件）
    卖出信号：价格触及20日高点附近（上轨）或 RSI > 65（放宽条件）
    """
    if df is None or len(df) < 20:
        return StrategyResult(Signal.HOLD, 0.0, 0.0, 0.0, "数据不足")
    
    close = df['Close']
    
    # 计算区间
    high_20d = close.rolling(20).max()
    low_20d = close.rolling(20).min()
    range_size = high_20d - low_20d
    
    # 计算RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    current_rsi = rsi.iloc[-1] if not rsi.empty and not pd.isna(rsi.iloc[-1]) else 50
    
    current_price = close.iloc[-1]
    current_high_20d = high_20d.iloc[-1] if not high_20d.empty else current_price
    current_low_20d = low_20d.iloc[-1] if not low_20d.empty else current_price
    current_range = range_size.iloc[-1] if not range_size.empty else current_price * 0.1
    
    # 判断是否在震荡区间（区间大小 < 18%，放宽条件）
    is_ranging = current_range / current_price < 0.18
    
    # 固定止损止盈（调整）
    stop_loss_pct = 0.05  # 5%
    take_profit_pct = 0.08  # 8%
    
    if not is_ranging:
        return StrategyResult(Signal.HOLD, 0.2, 0.0, 0.0, "非震荡市场")
    
    if is_holding:
        # 已持仓：寻找卖出信号（放宽条件：RSI > 65）
        # 判断是否接近上轨（在区间75%以上，放宽条件）
        price_position = (current_price - current_low_20d) / current_range if current_range > 0 else 0.5
        if price_position > 0.75 or current_rsi > 65:
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = current_price * (1 + take_profit_pct)
            return StrategyResult(
                Signal.SELL,
                0.7,
                stop_loss,
                take_profit,
                f"触及上轨: 价格位置{price_position:.1%}, RSI{current_rsi:.1f}"
            )
        else:
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = current_price * (1 + take_profit_pct)
            return StrategyResult(Signal.HOLD, 0.5, stop_loss, take_profit, "区间内持有")
    else:
        # 未持仓：寻找买入信号（放宽条件：RSI < 35，价格位置 < 25%）
        # 判断是否接近下轨（在区间25%以下，放宽条件）
        price_position = (current_price - current_low_20d) / current_range if current_range > 0 else 0.5
        if price_position < 0.25 and current_rsi < 35:
            stop_loss = current_price * (1 - stop_loss_pct)
            take_profit = current_price * (1 + take_profit_pct)
            confidence = 0.5 + (35 - current_rsi) / 35 * 0.3  # RSI越低，置信度越高
            return StrategyResult(
                Signal.BUY,
                confidence,
                stop_loss,
                take_profit,
                f"触及下轨: 价格位置{price_position:.1%}, RSI{current_rsi:.1f}"
            )
        else:
            return StrategyResult(Signal.HOLD, 0.3, 0.0, 0.0, "未触及下轨")


# ==================== 策略6: 默认择时策略（包装现有MarketTimer） ====================
# 将现有的MarketTimer包装为统一接口
# 注意：此策略需要在run_portfolio.py中特殊处理，因为需要MarketTimer实例

def default_timing_strategy(df: pd.DataFrame, is_holding: bool = False) -> StrategyResult:
    """
    默认择时策略（包装现有MarketTimer）
    
    注意：此函数在run_portfolio.py中会被特殊处理，直接使用MarketTimer
    这里返回HOLD作为占位符
    """
    # 此策略在run_portfolio.py中直接使用MarketTimer，不通过此函数
    return StrategyResult(Signal.HOLD, 0.0, 0.0, 0.0, "使用MarketTimer")


# ==================== 策略映射 ====================

STRATEGY_MAPPING = {
    'trend_following': trend_following_strategy,
    'mean_reversion': mean_reversion_strategy,
    'momentum_breakout': momentum_breakout_strategy,
    'reversal': reversal_strategy,
    'range_trading': range_trading_strategy,
    'default_timing': default_timing_strategy,  # 现有策略
}

# ==================== 策略信息 ====================

STRATEGY_INFO = {
    'trend_following': {
        'name': '趋势跟踪策略',
        'market_hypothesis': '趋势会延续',
        'core_logic': '均线系统 + ATR追踪止损',
        'params': {'MA20': 20, 'MA50': 50, 'ATR': 14},
    },
    'mean_reversion': {
        'name': '均值回归策略',
        'market_hypothesis': '价格会回归均值',
        'core_logic': 'RSI + 布林带',
        'params': {'RSI': 14, 'BB': '20,1.5', 'RSI_threshold': '35/65', 'stop_loss': '6%', 'take_profit': '10%'},
    },
    'momentum_breakout': {
        'name': '动量突破策略',
        'market_hypothesis': '强势突破会延续',
        'core_logic': '价格突破 + 成交量确认',
        'params': {'breakout_period': 20, 'volume_ratio': 1.2, 'stop_loss': '7%', 'take_profit': '18%'},
    },
    'reversal': {
        'name': '反转策略',
        'market_hypothesis': '极端价格会反转',
        'core_logic': '相对位置 + RSI + 成交量确认',
        'params': {'position_bottom': '25%', 'position_top': '75%', 'RSI': '35/65', 'volume_ratio': 1.2, 'stop_loss': '6%', 'take_profit': '12%'},
    },
    'range_trading': {
        'name': '震荡区间策略',
        'market_hypothesis': '价格在区间内震荡',
        'core_logic': '支撑阻力 + RSI',
        'params': {'range_period': 20, 'RSI': 14, 'RSI_threshold': '35/65', 'stop_loss': '5%', 'take_profit': '8%'},
    },
}


def get_strategy(strategy_type: str):
    """
    获取策略函数
    
    Args:
        strategy_type: 策略类型（strategy_mapping中的key）
    
    Returns:
        策略函数
    """
    if strategy_type not in STRATEGY_MAPPING:
        raise ValueError(f"未知策略类型: {strategy_type}, 可用策略: {list(STRATEGY_MAPPING.keys())}")
    return STRATEGY_MAPPING[strategy_type]


def execute_strategy(strategy_type: str, df: pd.DataFrame, is_holding: bool = False) -> StrategyResult:
    """
    执行策略（统一接口）
    
    Args:
        strategy_type: 策略类型
        df: 股票数据（DataFrame with columns: Open, High, Low, Close, Volume）
        is_holding: 是否已持仓
    
    Returns:
        StrategyResult
    """
    strategy_func = get_strategy(strategy_type)
    return strategy_func(df, is_holding)

