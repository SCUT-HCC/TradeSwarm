#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Memory 管理模块。

主要职责：
- 提供基础的 MemoryManager 抽象（7 个交易日滚动窗口）
- 为不同 Analyst 类型（market/news/sentiment/fundamentals）提供专用 Manager

此模块只定义结构和接口，不直接耦合具体 Prompt。
"""

from .base_manager import BaseAnalystMemoryManager
from .market_memory import MarketMemoryManager
from .news_memory import NewsMemoryManager
from .sentiment_memory import SentimentMemoryManager
from .fundamentals_memory import FundamentalsMemoryManager

__all__ = [
    "BaseAnalystMemoryManager",
    "MarketMemoryManager",
    "NewsMemoryManager",
    "SentimentMemoryManager",
    "FundamentalsMemoryManager",
]


