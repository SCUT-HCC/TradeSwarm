#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
基础 Analyst MemoryManager 抽象。

主要职责：
- 提供 7 个交易日滚动窗口的通用逻辑接口
- 不关心具体 Prompt，只定义输入/输出契约
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol

from langchain_core.language_models import BaseChatModel

from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper


class DateResolver(Protocol):
    """用于将 trade_date 与“前 N 个交易日”映射的协议。

    说明：
        - 这里只定义协议接口，具体实现可以基于交易日历或价格数据
        - 目前为了保持结构简单，可以在后续实现时注入真正的 resolver
    """

    def get_past_trading_days(self, end_date: str, lookback: int) -> List[str]:
        """返回以 end_date 为结束的最近 lookback 个交易日（含 end_date）。"""


@dataclass
class SummaryContext:
    """生成 summary 所需的上下文信息。"""

    analyst_type: str
    symbol: str
    trade_date: str
    window_start_date: str
    window_end_date: str
    reports: List[Dict[str, Any]]  # 来自 analyst_reports 的原始报告列表


class BaseAnalystMemoryManager(ABC):
    """基础 Analyst MemoryManager。

    负责：
    - 从 analyst_reports 中读取过去 7 个交易日的原始报告
    - 调用具体子类提供的 Prompt / LLM 逻辑生成结构化 summary
    - 通过 MemoryDBHelper 将 summary 写入 analyst_summaries 表
    """

    def __init__(
        self,
        analyst_type: str,
        db_helper: MemoryDBHelper,
        date_resolver: Optional[DateResolver] = None,
    ) -> None:
        """
        初始化 MemoryManager。

        Args:
            analyst_type: 分析师类型（如 'market', 'news', 'sentiment', 'fundamentals'）
            db_helper: MemoryDBHelper 实例
            date_resolver: 交易日解析器（可选）
        """
        self.analyst_type: str = analyst_type
        self.db_helper: MemoryDBHelper = db_helper
        self.date_resolver: Optional[DateResolver] = date_resolver

    # ------------------------------------------------------------------
    # 对外主入口
    # ------------------------------------------------------------------

    def run_daily_update(
        self,
        llm: BaseChatModel,
        symbol: str,
        trade_date: str,
        window_size: int = 7,
    ) -> bool:
        """针对单个 symbol / trade_date 执行一次 7 日窗口 summary 更新。

        高层流程：
        1. 计算窗口期（交易日维度）
        2. 从 analyst_reports 中读取窗口内的原始报告
        3. 如果没有足够数据，可以选择：
           - 仍然生成（标记 source_reports_count < window_size）
           - 或直接跳过（由子类决定）
        4. 调用子类实现的 `build_summary_prompt` / `parse_summary_output`
        5. 将结果写入 analyst_summaries
        """
        # 1. 计算窗口日期（这里先简单使用日期减法，后续可接 DateResolver）
        window_start_date, window_end_date = self._resolve_window(trade_date, window_size)

        # 2. 读取窗口内原始报告
        reports = self._load_window_reports(
            symbol=symbol,
            window_end_date=trade_date,
            window_size=window_size,
        )

        source_reports_count = len(reports)
        if source_reports_count == 0:
            # 没有任何报告，直接跳过（返回 False 表示无更新）
            return False

        context = SummaryContext(
            analyst_type=self.analyst_type,
            symbol=symbol,
            trade_date=trade_date,
            window_start_date=window_start_date,
            window_end_date=window_end_date,
            reports=reports,
        )

        # 3. 由子类构建 Prompt 并调用 LLM（这里只定义结构，具体 Prompt 后续讨论）
        summary_content, llm_model, token_usage = self._generate_summary_with_llm(
            llm=llm,
            context=context,
        )

        # 4. 写入 analyst_summaries 表
        ok = self.db_helper.upsert_summary(
            analyst_type=self.analyst_type,
            symbol=symbol,
            trade_date=trade_date,
            summary_content=summary_content,
            window_start_date=window_start_date,
            window_end_date=window_end_date,
            source_reports_count=source_reports_count,
            llm_model=llm_model,
            token_usage=token_usage,
        )
        return ok

    # ------------------------------------------------------------------
    # 可复用的内部工具
    # ------------------------------------------------------------------

    def _resolve_window(self, trade_date: str, window_size: int) -> tuple[str, str]:
        """根据交易日期和窗口大小计算窗口起止日期。

        说明：
            - 当前简单采用自然日回溯 window_size-1 天
            - 后续可以通过 DateResolver 注入真实的“交易日历”
        """
        if self.date_resolver is not None:
            days = self.date_resolver.get_past_trading_days(
                end_date=trade_date,
                lookback=window_size,
            )
            if days:
                return days[0], days[-1]

        # Fallback：自然日回溯
        end_dt = datetime.strptime(trade_date, "%Y-%m-%d")
        start_dt = end_dt.replace()  # copy
        # 简单回溯 window_size-1 天（自然日）
        # 注意：这里只是兜底逻辑，实际推荐使用 DateResolver
        from datetime import timedelta

        start_dt = end_dt - timedelta(days=window_size - 1)
        return start_dt.strftime("%Y-%m-%d"), trade_date

    def _load_window_reports(
        self,
        symbol: str,
        window_end_date: str,
        window_size: int,
    ) -> List[Dict[str, Any]]:
        """加载窗口内的原始报告列表。"""
        # 这里仍然使用基于天数的回溯，后续可以根据 DateResolver 调整
        reports = self.db_helper.query_history_reports(
            analyst_type=self.analyst_type,
            symbol=symbol,
            trade_date=window_end_date,
            lookback_days=window_size * 2,  # 多给一些天数，避免周末/节假日导致数据不足
        )
        return reports

    # ------------------------------------------------------------------
    # 需要子类实现的抽象方法
    # ------------------------------------------------------------------

    @abstractmethod
    def _generate_summary_with_llm(
        self,
        llm: BaseChatModel,
        context: SummaryContext,
    ) -> tuple[str, Optional[str], Optional[int]]:
        """使用 LLM 生成结构化 summary。

        Args:
            llm: LLM 实例
            context: 包含窗口内原始报告等信息的上下文

        Returns:
            一个三元组：
                - summary_content: 结构化 summary（通常为 JSON 字符串）
                - llm_model: 使用的模型名称（可选）
                - token_usage: Token 用量（可选）

        注意：
            - 此方法只定义接口，具体 Prompt 和 JSON 结构由子类实现
        """


