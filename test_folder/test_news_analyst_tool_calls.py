"""测试 news_analyst 的工具调用详情"""
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tradingagents.agents.utils.agentstate.agent_states import AgentState
from tradingagents.agents.init_llm import llm
from langchain.agents import create_agent
from tradingagents.tool_nodes.utils.news_tools import get_news, get_global_news
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult


class ToolCallTracker(BaseCallbackHandler):
    """追踪工具调用的回调处理器"""
    
    def __init__(self):
        self.tool_calls = []
        self.tool_results = []
        self.llm_calls = []
    
    def on_tool_start(self, serialized, input_str, **kwargs):
        """工具开始调用时"""
        tool_name = serialized.get("name", "unknown")
        print(f"\n{'='*80}")
        print(f"[工具调用开始] {tool_name}")
        print(f"{'='*80}")
        print(f"输入参数: {input_str[:500]}...")  # 只显示前500字符
        
        self.tool_calls.append({
            "name": tool_name,
            "input": input_str,
            "timestamp": kwargs.get("run_id", "")
        })
    
    def on_tool_end(self, output, **kwargs):
        """工具调用结束时"""
        tool_name = kwargs.get("name", "unknown")
        print(f"\n[工具调用结束] {tool_name}")
        
        # 格式化输出
        output_str = str(output)
        if len(output_str) > 1000:
            print(f"输出结果（前1000字符）:\n{output_str[:1000]}...")
            print(f"\n... (共 {len(output_str)} 字符)")
        else:
            print(f"输出结果:\n{output_str}")
        
        self.tool_results.append({
            "name": tool_name,
            "output": output_str,
            "timestamp": kwargs.get("run_id", "")
        })
        print(f"{'='*80}\n")
    
    def on_tool_error(self, error, **kwargs):
        """工具调用出错时"""
        tool_name = kwargs.get("name", "unknown")
        print(f"\n{'='*80}")
        print(f"[工具调用错误] {tool_name}")
        print(f"错误信息: {str(error)}")
        print(f"{'='*80}\n")
    
    def on_llm_start(self, serialized, prompts, **kwargs):
        """LLM 开始调用时"""
        print(f"\n[LLM 调用] 提示词长度: {len(str(prompts))} 字符")
        self.llm_calls.append({
            "prompts": str(prompts)[:200],  # 只保存前200字符
            "timestamp": kwargs.get("run_id", "")
        })
    
    def get_summary(self):
        """获取调用摘要"""
        return {
            "tool_calls": len(self.tool_calls),
            "tool_results": len(self.tool_results),
            "llm_calls": len(self.llm_calls),
            "tools_used": list(set([t["name"] for t in self.tool_calls]))
        }


def test_news_analyst_with_tracking():
    """测试 news_analyst 并追踪工具调用"""
    
    # 创建回调追踪器
    tracker = ToolCallTracker()
    
    # 准备测试数据
    ticker = "000001"
    current_date = "2025-12-11"
    
    # 工具列表
    tools = [get_news, get_global_news]
    
    # system prompt
    system_prom = (
        "你是一位专业的新闻和宏观经济研究员，负责分析最近7天内的全球和公司特定事件。"
        "你的目标是撰写一份详细、细致的报告，总结相关的宏观经济新闻、市场信号和关键事件，"
        "这些信息可能影响所选股票的交易和投资决策。"
        "确保同时分析宏观全球新闻和与 {ticker} 相关的特定公司新闻。"
        "你是一个有用的 AI 助手，与其他助手协作。"
        "使用提供的工具来推进问题的解答。"
        "**重要：工作流程和停止条件**"
        "1. 首先调用 get_news 工具获取股票相关新闻（最多调用1次）。"
        "2. 然后调用 get_global_news 工具获取宏观经济新闻（最多调用1次）。"
        "3. 获取数据后，立即基于获取的数据撰写分析报告。"
        "4. 撰写完报告后，必须立即停止，不要再调用任何工具。"
        "5. 如果工具调用失败或返回空数据，也要基于已有信息撰写报告并停止，不要重复尝试。"
        "6. 不要重复调用相同的工具，每个工具最多调用1次。"
        "如果你无法完全回答，其他助手会继续。"
        "如果你确定最终交易建议：**买入/持有/卖出**，请在输出前加上相应前缀。"
        "分析提示："
        "1. 重点关注可能影响股价的重大新闻和事件（如政策变化、业绩公告、行业动态等）。"
        "2. 分析新闻对股票可能产生的正面或负面影响。"
        "3. 区分短期影响和长期影响。"
        "4. 在描述'当前'、'最新'等状态时，使用最新日期的新闻信息。"
        "5. 可以使用历史新闻进行趋势分析，但描述当前状态时使用最新信息。"
        "你可用的工具包括：{tool_names}。"
        "当前日期是 {current_date}。"
        "当前要分析的股票代码是 {ticker}。"
        "请使用中文进行分析和报告。"
    ).format(
        tool_names=", ".join([tool.name for tool in tools]),
        current_date=current_date,
        ticker=ticker
    )
    
    # 创建 agent
    agent = create_agent(
        model=llm,
        tools=tools,
        system_prompt=system_prom
    )
    
    # 准备输入消息
    input_message = {"role": "user", "content": f"分析股票 {ticker} 的新闻和宏观经济信息"}
    
    print("\n" + "="*80)
    print("开始测试 News Analyst 工具调用")
    print("="*80)
    print(f"股票代码: {ticker}")
    print(f"交易日期: {current_date}")
    print(f"可用工具: {', '.join([tool.name for tool in tools])}")
    print("="*80 + "\n")
    
    # 调用 agent（带回调）
    try:
        result = agent.invoke(
            input=input_message,
            config={
                "recursion_limit": 50,
                "callbacks": [tracker]  # 添加回调追踪
            }
        )
        
        # 打印摘要
        print("\n" + "="*80)
        print("工具调用摘要")
        print("="*80)
        summary = tracker.get_summary()
        print(f"工具调用次数: {summary['tool_calls']}")
        print(f"工具执行结果数: {summary['tool_results']}")
        print(f"LLM 调用次数: {summary['llm_calls']}")
        print(f"使用的工具: {', '.join(summary['tools_used'])}")
        print("="*80)
        
        # 打印最终结果
        print("\n" + "="*80)
        print("最终分析报告")
        print("="*80)
        
        # 提取最后一条 AI 消息
        final_report = ""
        for msg in reversed(result["messages"]):
            if hasattr(msg, 'content') and msg.content:
                has_tool_calls = False
                if hasattr(msg, 'tool_calls') and msg.tool_calls:
                    has_tool_calls = True
                if not has_tool_calls:
                    final_report = msg.content
                    break
        
        if final_report:
            print(final_report)
        else:
            print("未找到最终报告")
        
        print("="*80 + "\n")
        
        # 保存详细日志到文件
        log_data = {
            "summary": summary,
            "tool_calls": tracker.tool_calls,
            "tool_results": [{"name": r["name"], "output_length": len(r["output"])} for r in tracker.tool_results],
            "final_report_length": len(final_report)
        }
        
        with open("news_analyst_tool_calls_log.json", "w", encoding="utf-8") as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2, default=str)
        
        print("详细日志已保存到: news_analyst_tool_calls_log.json")
        
    except Exception as e:
        print(f"\n错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_news_analyst_with_tracking()

