"""
测试交易图

运行完整的交易决策流程，验证所有节点和子图是否正常工作。
"""

from typing import Any, Dict
from datetime import datetime

from tradingagents.agents.utils.agentstate.agent_states import AgentState, AnalystMemorySummary        
from tradingagents.graph.pre_open.utils import MockMemory, load_llm_from_config
from tradingagents.graph.pre_open.trading_graph import create_trading_graph
from tradingagents.agents.utils.init.init_db import conn


def create_initial_state(
    company_code: str = "000001",
    trade_date: str = "2026-01-18",
) -> Dict[str, Any]:
    """
    创建初始 AgentState。
    
    Args:
        company_code: 股票代码
        trade_date: 交易日期
        
    Returns:
        初始状态的字典
    """
    # 创建空的 AnalystMemorySummary（Summary 节点会填充）
    empty_summary: AnalystMemorySummary = {
        "today_report": "",
        "history_report": "",
    }
    
    initial_state: AgentState = {
        "company_of_interest": company_code,
        "trade_date": trade_date,
        "trade_timestamp": datetime.now().isoformat(),
        "market_analyst_summary": empty_summary,
        "news_analyst_summary": empty_summary,
        "sentiment_analyst_summary": empty_summary,
        "fundamentals_analyst_summary": empty_summary,
        "research_summary": None,
        "investment_plan": None,
        "risk_summary": None,
        "final_trade_decision": None,
        "trader_investment_plan": None,
        "messages": [],  # MessagesState 需要
    }
    
    return initial_state


def main():
    """主测试函数。"""
    print("=" * 80)
    print("开始测试交易图")
    print("=" * 80)
    
    # 1. 加载 LLM
    print("\n[1/5] 加载 LLM 配置...")
    try:
        llm = load_llm_from_config("config/config.yaml")
        print(f"✓ LLM 加载成功: {llm.model_name}")
    except Exception as e:
        print(f"✗ LLM 加载失败: {e}")
        return
    
    # 2. 创建 MockMemory
    print("\n[2/5] 创建 MockMemory...")
    memory = MockMemory()
    print("✓ MockMemory 创建成功")
    
    # 3. 创建交易图
    print("\n[3/5] 创建交易图...")
    try:

        graph = create_trading_graph(
            llm=llm,
            memory=memory,
            data_manager=conn, 
        )
        print("✓ 交易图创建成功")
    except Exception as e:
        print(f"✗ 交易图创建失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 4. 创建初始状态
    print("\n[4/5] 创建初始状态...")
    initial_state = create_initial_state(
        company_code="000001",
        trade_date="2026-01-18",
    )
    print(f"✓ 初始状态创建成功: 股票代码={initial_state['company_of_interest']}, 日期={initial_state['trade_date']}")
    
    # 5. 运行图
    print("\n[5/5] 运行交易图...")
    print("-" * 80)
    try:
        final_state = graph.invoke(initial_state)
        print("-" * 80)
        print("\n✓ 交易图运行成功！")
        
        # 打印关键结果
        print("\n" + "=" * 80)
        print("最终结果摘要")
        print("=" * 80)
        
        if final_state.get("research_summary"):
            research = final_state["research_summary"]
            print(f"\n[Research Summary]")
            print(f"  投资计划: {research.get('investment_plan', 'N/A')[:200]}...")
            if research.get("investment_debate_state"):
                debate = research["investment_debate_state"]
                print(f"  辩论轮次: {debate.get('count', 0)}")
        
        if final_state.get("trader_investment_plan"):
            print(f"\n[Trader Plan]")
            print(f"  {final_state['trader_investment_plan'][:200]}...")
        
        if final_state.get("risk_summary"):
            risk = final_state["risk_summary"]
            print(f"\n[Risk Summary]")
            print(f"  最终决策: {risk.get('final_trade_decision', 'N/A')[:200]}...")
            if risk.get("risk_debate_state"):
                debate = risk["risk_debate_state"]
                print(f"  辩论轮次: {debate.get('count', 0)}")
        
        print("\n" + "=" * 80)
        print("测试完成！")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n✗ 交易图运行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

