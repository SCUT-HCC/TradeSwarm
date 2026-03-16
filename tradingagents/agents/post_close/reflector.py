#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Reflector Agent 节点

职责：
    - 在周期结束时（周/月）执行
    - 读取一个周期内的 daily_trading_summaries 与关键决策字段
    - 使用 LLM 生成结构化周期反思（错误模式、成功模式、策略适用条件、环境判断偏差等）
    - 将周期反思写入 cycle_reflections 表，供 past_memory_str 使用
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional
import json

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from tradingagents.agents.utils.agentstate.agent_states import AgentState
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper
from tradingagents.agents.utils.json_parser import extract_json_from_text


def create_reflector_node(
    llm: BaseChatModel,
    db_helper: MemoryDBHelper,
) -> Callable[[AgentState], Dict[str, Any]]:
    """
    创建 Reflector Agent 节点。

    Args:
        llm: 用于生成反思的 LLM 实例
        db_helper: MemoryDBHelper 实例

    Returns:
        一个符合 LangGraph 节点签名的函数：fn(state) -> updates
    """

    def reflector_node(state: AgentState) -> Dict[str, Any]:
        """
        Reflector Agent 节点执行函数。

        流程：
        1. 从 AgentState 中读取 cycle_type, cycle_start_date, cycle_end_date, symbol
        2. 查询周期内的 daily_trading_summaries
        3. 使用 LLM 生成结构化周期反思
        4. 将反思写入 cycle_reflections 表
        """
        cycle_type = state.get("cycle_type", "weekly")  # 'weekly' 或 'monthly'
        cycle_start_date = state.get("cycle_start_date")
        cycle_end_date = state.get("cycle_end_date")
        symbol = state.get("company_of_interest")

        if not cycle_start_date or not cycle_end_date:
            print(
                f"[Reflector] 缺少必要参数: cycle_start_date={cycle_start_date}, cycle_end_date={cycle_end_date}"
            )
            return {}

        if not symbol:
            print("[Reflector] 缺少 symbol 参数")
            return {}

        # 1. 查询周期内的 daily_trading_summaries
        summaries = db_helper.query_daily_trading_summaries_by_date_range(
            symbol=symbol,
            start_date=cycle_start_date,
            end_date=cycle_end_date,
        )

        if not summaries:
            print(f"[Reflector] 周期内 ({cycle_start_date} ~ {cycle_end_date}) 没有找到 daily summaries")
            return {}

        print(f"[Reflector] 找到 {len(summaries)} 条 daily summaries")

        # 2. 准备 LLM 输入：汇总周期内的关键信息
        summary_text = _format_summaries_for_reflection(summaries)

        # 3. 构建 Prompt
        system_prompt = _get_reflector_system_prompt()
        user_prompt = _get_reflector_user_prompt(
            cycle_type=cycle_type,
            cycle_start_date=cycle_start_date,
            cycle_end_date=cycle_end_date,
            symbol=symbol,
            summary_text=summary_text,
        )

        # 4. 调用 LLM 生成反思
        try:
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt),
            ]
            response = llm.invoke(messages)
            reflection_text = response.content if hasattr(response, "content") else str(response)

            # 5. 解析 JSON 输出
            reflection_json = extract_json_from_text(reflection_text)
            if not reflection_json:
                print("[Reflector] LLM 输出无法解析为 JSON")
                return {}

            # 6. 提取各个字段
            key_insights = reflection_json.get("key_insights", "")
            error_patterns = json.dumps(reflection_json.get("error_patterns", []), ensure_ascii=False)
            success_patterns = json.dumps(reflection_json.get("success_patterns", []), ensure_ascii=False)
            strategy_conditions = json.dumps(reflection_json.get("strategy_conditions", {}), ensure_ascii=False)
            environment_biases = json.dumps(reflection_json.get("environment_biases", []), ensure_ascii=False)
            reflection_content = json.dumps(reflection_json, ensure_ascii=False)

            # 7. 写入数据库
            success = db_helper.upsert_cycle_reflection(
                cycle_type=cycle_type,
                cycle_start_date=cycle_start_date,
                cycle_end_date=cycle_end_date,
                symbol=symbol,
                reflection_content=reflection_content,
                key_insights=key_insights,
                error_patterns=error_patterns,
                success_patterns=success_patterns,
                strategy_conditions=strategy_conditions,
                environment_biases=environment_biases,
            )

            if success:
                print(f"[Reflector] 成功写入周期反思: {cycle_type} ({cycle_start_date} ~ {cycle_end_date})")
            else:
                print(f"[Reflector] 写入周期反思失败")

            return {
                "reflector_log": {
                    "cycle_type": cycle_type,
                    "cycle_start_date": cycle_start_date,
                    "cycle_end_date": cycle_end_date,
                    "symbol": symbol,
                    "status": "ok" if success else "failed",
                    "summary_count": len(summaries),
                },
                "reflection_content": reflection_json,
            }

        except Exception as e:
            print(f"[Reflector] 生成周期反思失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                "reflector_log": {
                    "cycle_type": cycle_type,
                    "cycle_start_date": cycle_start_date,
                    "cycle_end_date": cycle_end_date,
                    "symbol": symbol,
                    "status": "error",
                    "error": str(e),
                },
            }

    return reflector_node


def _format_summaries_for_reflection(summaries: List[Dict[str, Any]]) -> str:
    """格式化 daily summaries 为文本，供 LLM 分析。"""
    lines = []
    for summary in summaries:
        date = summary.get("date", "")
        market_regime = summary.get("market_regime", "N/A")
        selected_strategy = summary.get("selected_strategy", "N/A")
        expected_behavior = summary.get("expected_behavior", "N/A")
        actual_return = summary.get("actual_return", 0.0)
        actual_max_drawdown = summary.get("actual_max_drawdown", 0.0)
        positioning = summary.get("positioning", "N/A")
        anomaly = summary.get("anomaly", "")

        lines.append(
            f"日期: {date}\n"
            f"  市场状态: {market_regime}\n"
            f"  选择策略: {selected_strategy}\n"
            f"  预期行为: {expected_behavior}\n"
            f"  实际收益: {actual_return:.2f}%\n"
            f"  最大回撤: {actual_max_drawdown:.2f}%\n"
            f"  仓位状态: {positioning}\n"
            f"  异常情况: {anomaly if anomaly else '无'}\n"
        )

    return "\n".join(lines)


def _get_reflector_system_prompt() -> str:
    """获取 Reflector Agent 的系统 Prompt。"""
    return """你是一位经验丰富的交易系统反思分析师。你的任务是分析一个交易周期（周/月）内的交易表现，识别错误模式、成功模式、策略适用条件以及环境判断偏差。

你需要输出一个结构化的 JSON 反思报告，包含以下字段：
- key_insights: 关键洞察（1-3 句话总结）
- error_patterns: 错误模式列表（每个模式包含：描述、发生频率、影响、改进建议）
- success_patterns: 成功模式列表（每个模式包含：描述、发生频率、收益贡献、适用条件）
- strategy_conditions: 策略适用条件（每个策略包含：适用市场状态、预期行为、实际表现、建议使用条件）
- environment_biases: 环境判断偏差（每个偏差包含：偏差类型、影响、修正建议）

请确保输出是有效的 JSON 格式。"""


def _get_reflector_user_prompt(
    cycle_type: str,
    cycle_start_date: str,
    cycle_end_date: str,
    symbol: str,
    summary_text: str,
) -> str:
    """获取 Reflector Agent 的用户 Prompt。"""
    return f"""请分析以下交易周期的表现：

周期类型: {cycle_type}
周期范围: {cycle_start_date} ~ {cycle_end_date}
标的: {symbol}

每日交易摘要：
{summary_text}

请基于以上信息，生成结构化的周期反思报告（JSON 格式）。重点关注：
1. 哪些决策导致了亏损？为什么？
2. 哪些决策带来了收益？成功的关键因素是什么？
3. 不同策略在什么市场状态下表现最好/最差？
4. 是否存在对市场状态的误判？如何改进？

请输出 JSON 格式的反思报告。"""

