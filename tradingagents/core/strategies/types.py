#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
策略类型定义
"""

from dataclasses import dataclass
from enum import Enum


class Signal(Enum):
    """交易信号"""
    BUY = 1
    SELL = -1
    HOLD = 0


@dataclass
class StrategyResult:
    """策略结果"""
    signal: Signal              # 交易信号
    confidence: float           # 置信度 0-1
    stop_loss_price: float     # 止损价
    take_profit_price: float   # 止盈价
    reason: str                # 信号原因

