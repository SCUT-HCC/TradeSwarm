from __future__ import annotations

from typing import Any, Dict

from tradingagents.agents.utils.agentstate.agent_states import (
    AgentState,
    RiskDebateState,
    RiskSummary,
)
from tradingagents.agents.utils.state_helpers import build_curr_situation_from_summaries
from tradingagents.agents.utils.prompt_loader import load_prompt_template


def create_safe_debator(llm: Any):
    """创建 Safe/Conservative Risk Analyst LangGraph 节点工厂。

    - 输入：全局 AgentState（包含四个 analyst_summary 和 risk_summary）
    - 内部：从 risk_summary 中读取/更新 risk_debate_state
    - 输出：更新 risk_summary 中的 risk_debate_state
    """

    def safe_node(state: AgentState) -> Dict[str, Any]:
        """Safe/Conservative Risk Analyst 节点，参与风险辩论。"""
        # 1. 读取上一轮辩论状态（封装在 risk_summary 内）
        prev_summary: RiskSummary | None = state.get("risk_summary")  # type: ignore[assignment]
        prev_debate: RiskDebateState = (
            prev_summary.get("risk_debate_state")  # type: ignore[union-attr]
            if prev_summary is not None
            else {
                "risky_history": "",
                "safe_history": "",
                "neutral_history": "",
                "history": "",
                "latest_speaker": "",
                "current_risky_response": "",
                "current_safe_response": "",
                "current_neutral_response": "",
                "judge_decision": "",
                "count": 0,
            }
        )

        history = prev_debate.get("history", "")
        safe_history = prev_debate.get("safe_history", "")
        risky_history = prev_debate.get("risky_history", "")
        neutral_history = prev_debate.get("neutral_history", "")
        current_risky_response = prev_debate.get("current_risky_response", "")
        current_neutral_response = prev_debate.get("current_neutral_response", "")
        count = prev_debate.get("count", 0)
        
        # 计算当前轮次（每轮包含 risky、neutral、safe 各一次发言）
        round_number = (count // 3) + 1
        is_first_round = count < 3

        # 2. 从四个 Analyst 的 MemorySummary 中读取当日与 7 日脉络（无 memory，仅用已有 summary）
        market_summary = state["market_analyst_summary"]
        news_summary = state["news_analyst_summary"]
        sentiment_summary = state["sentiment_analyst_summary"]
        fundamentals_summary = state["fundamentals_analyst_summary"]

        # 当日报告
        market_research_report = market_summary["today_report"]
        news_report = news_summary["today_report"]
        sentiment_report = sentiment_summary["today_report"]
        fundamentals_report = fundamentals_summary["today_report"]

        # 最近 7 日 history 摘要
        market_history_summary = market_summary["history_report"]
        news_history_summary = news_summary["history_report"]
        sentiment_history_summary = sentiment_summary["history_report"]
        fundamentals_history_summary = fundamentals_summary["history_report"]

        # 3. 读取 research_summary 中的 investment_plan 作为 trader_decision
        research_summary: Dict[str, Any] | None = state.get("research_summary")  # type: ignore[assignment]
        trader_decision = (
            research_summary.get("investment_plan", "")  # type: ignore[union-attr]
            if research_summary is not None
            else state.get("investment_plan", "")
        )

        # 4. 加载并渲染 prompt 模板
        prompt = load_prompt_template(
            agent_type="risk_mgmt",
            agent_name="conservative_debator",
            context={
                "trader_decision": trader_decision,
                # 当日分析
                "market_research_report": market_research_report,
                "sentiment_report": sentiment_report,
                "news_report": news_report,
                "fundamentals_report": fundamentals_report,
                # 7 日 history 摘要
                "market_history_summary": market_history_summary,
                "news_history_summary": news_history_summary,
                "sentiment_history_summary": sentiment_history_summary,
                "fundamentals_history_summary": fundamentals_history_summary,
                # 风险辩论状态
                "history": history,
                "current_risky_response": current_risky_response,
                "current_neutral_response": current_neutral_response,
                "round_number": round_number,
                "is_first_round": is_first_round,
                "risky_history": risky_history,
                "neutral_history": neutral_history,
            },
        )

        # 5. 调用 LLM 生成论证
        response = llm.invoke(prompt)
        content: str = getattr(response, "content", str(response))

        argument = f"Safe Analyst: {content}"

        new_risk_debate_state: RiskDebateState = {
            "history": history + "\n" + argument,
            "risky_history": prev_debate.get("risky_history", ""),
            "safe_history": safe_history + "\n" + argument,
            "neutral_history": prev_debate.get("neutral_history", ""),
            "latest_speaker": "Safe",
            "current_risky_response": prev_debate.get("current_risky_response", ""),
            "current_safe_response": argument,
            "current_neutral_response": prev_debate.get("current_neutral_response", ""),
            "judge_decision": prev_debate.get("judge_decision", ""),
            "count": prev_debate.get("count", 0) + 1,
        }

        # 更新或创建 risk_summary
        new_summary: RiskSummary = {
            "risk_debate_state": new_risk_debate_state,
        }
        if prev_summary is not None:
            new_summary["final_trade_decision"] = prev_summary.get("final_trade_decision", "")
            new_summary["raw_response"] = prev_summary.get("raw_response", "")

        return {"risk_summary": new_summary}

    return safe_node


if __name__ == "__main__":
    # 保留原有的测试逻辑，但适配新的 AgentState 结构，便于后续回归验证。
    import os

    from dotenv import load_dotenv
    from langchain_openai import ChatOpenAI
    from openai import OpenAI
    
    load_dotenv()
    
    client = OpenAI(
        api_key=os.getenv("DASHSCOPE_API_KEY"),
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    )
    
    print("=" * 80)
    print("开始测试 conservative_debator_node (safe_debator)")
    print("=" * 80)
    
    print("\n[1/3] 初始化 LLM...")
    llm = ChatOpenAI(
        model="qwen-plus",
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        api_key=os.getenv("DASHSCOPE_API_KEY"),
    )
    print("✓ LLM 初始化完成")
    
    print("\n[2/3] 创建 safe_debator_node...")
    safe_debator_node = create_safe_debator(llm)
    print("✓ safe_debator_node 创建完成")
    
    print("\n[3/3] 准备测试用的 AgentState 并调用...")
    demo_summary: AnalystMemorySummary = {
        "today_report": "市场整体呈现上涨趋势，银行板块表现稳健。",
        "history_report": "Demo history report.",
    }

    agentstate: AgentState = {
        "company_of_interest": "000001",
        "trade_date": "2024-12-07",
        "trade_timestamp": "2024-12-07 10:00:00",
        "market_analyst_summary": demo_summary,
        "news_analyst_summary": {
            "today_report": "近期新闻显示，平安银行在零售业务转型方面取得积极进展。",
            "history_report": "Demo history report.",
        },
        "sentiment_analyst_summary": {
            "today_report": "社交媒体情绪对平安银行整体偏正面，投资者对银行数字化转型持乐观态度。",
            "history_report": "Demo history report.",
        },
        "fundamentals_analyst_summary": {
            "today_report": "平安银行财务基本面保持稳健，营收持续增长，资产质量持续改善。",
            "history_report": "Demo history report.",
        },
        "research_summary": {
            "investment_plan": "监控市场情绪变化，适时调整仓位。考虑到基本面强劲，可考虑适度增加仓位。",
        },
        "risk_summary": {
            "risk_debate_state": {
                "risky_history": "由于市场地位稳固，可承受较高风险。",
                "safe_history": "考虑到市场波动性，建议采取保守策略。",
                "neutral_history": "平衡风险和机会的视角。",
                "history": "风险评估正在进行中，存在多种观点。",
                "latest_speaker": "risk_analyst",
                "current_risky_response": "在计算风险的前提下，存在高回报潜力。",
                "current_safe_response": "",
                "current_neutral_response": "采取适度风险策略，定期监控。",
                "judge_decision": "等待最终风险评估。",
                "count": 1,
            },
        },
        "investment_plan": None,
        "trader_investment_plan": None,
        "final_trade_decision": None,
        "messages": [],
    }

    print("-" * 80)
    try:
        result = safe_debator_node(agentstate)
        
        print("\n" + "=" * 80)
        print("测试结果:")
        print("=" * 80)
        
        print("\n【Risk Debate State】")
        print("-" * 80)
        summary: RiskSummary = result.get("risk_summary", {})  # type: ignore[assignment]
        risk_debate_state = summary.get("risk_debate_state", {})
        print(f"Latest Speaker: {risk_debate_state.get('latest_speaker', 'N/A')}")
        print(f"Count: {risk_debate_state.get('count', 'N/A')}")
        print(f"\nCurrent Safe Response:")
        print(f"{risk_debate_state.get('current_safe_response', 'N/A')[:500]}...")
        print(f"\nSafe History:")
        print(f"{risk_debate_state.get('safe_history', 'N/A')[:300]}...")
        print(f"\nHistory:")
        print(f"{risk_debate_state.get('history', 'N/A')[:300]}...")
        
        print("\n" + "=" * 80)
        print("✓ 测试完成！")
        print("=" * 80)
        
    except Exception as e:  # pragma: no cover - 手工调试用
        print(f"\n❌ 测试失败: {str(e)}")
        import traceback

        traceback.print_exc()
