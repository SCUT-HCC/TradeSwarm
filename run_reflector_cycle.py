#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
运行 Reflector Agent，对指定周期进行反思并写入 cycle_reflections。
"""

from __future__ import annotations

import argparse
from typing import Any

from tradingagents.agents.post_close.reflector import create_reflector_node
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper
from tradingagents.graph.utils import load_llm_from_config
from tradingagents.agents.utils.agentstate.agent_states import AgentState


def run_reflector(
    symbol: str,
    start_date: str,
    end_date: str,
    cycle_type: str,
    db_path: str,
) -> Any:
    """运行一次周期反思。"""
    print(f"[ReflectorRunner] symbol={symbol}, {start_date}~{end_date}, cycle_type={cycle_type}")

    llm = load_llm_from_config()
    db_helper = MemoryDBHelper(db_path)

    try:
        reflector_node = create_reflector_node(llm=llm, db_helper=db_helper)
        state: AgentState = {
            "cycle_type": cycle_type,
            "cycle_start_date": start_date,
            "cycle_end_date": end_date,
            "company_of_interest": symbol,
        }
        result = reflector_node(state)
        print("[ReflectorRunner] result keys:", list(result.keys()))
        print("[ReflectorRunner] reflector_log:", result.get("reflector_log"))
        return result
    finally:
        db_helper.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="运行 Reflector 周期反思")
    parser.add_argument("--symbol", type=str, default="AAPL")
    parser.add_argument("--start", type=str, default="2024-01-02")
    parser.add_argument("--end", type=str, default="2024-01-10")
    parser.add_argument("--cycle", type=str, default="weekly")
    parser.add_argument("--db", type=str, default="memory.db")
    args = parser.parse_args()

    run_reflector(
        symbol=args.symbol,
        start_date=args.start,
        end_date=args.end,
        cycle_type=args.cycle,
        db_path=args.db,
    )


if __name__ == "__main__":
    main()


