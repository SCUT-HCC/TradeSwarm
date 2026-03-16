#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
市场状态枚举
"""

from enum import Enum


class MarketRegime(Enum):
    """市场状态"""
    BULL = "牛市"
    BEAR = "熊市"
    SIDEWAYS = "震荡市"

