from typing import Callable, Any
from pathlib import Path
from jinja2 import Template
from langchain.agents import create_agent
from langchain_core.language_models import BaseChatModel
import json

from .state import FundamentalsAnalystState
from tradingagents.tool_nodes.utils import (
    get_company_info,
    get_financial_statements,
    get_financial_indicators,
    get_valuation_indicators,
    get_earnings_data
)
from tradingagents.agents.utils.json_parser import parse_analyst_output, validate_analyst_json


def create_fundamentals_analyst(llm: BaseChatModel) -> Callable[[FundamentalsAnalystState], dict[str, Any]]:
    """
    创建 Fundamentals Analyst agent 节点函数
    
    该函数返回一个符合 LangGraph 节点规范的函数，用于执行公司基本面和估值分析。
    agent 会调用多个财务数据工具（公司信息、财务报表、财务指标、估值指标、业绩数据），
    按照明确的工作流程获取数据并生成详细的分析报告。
    
    Args:
        llm: LangChain BaseChatModel 实例，用于驱动 agent 的推理和决策
        
    Returns:
        fundamentals_analyst_node: 一个接受 FundamentalsAnalystState 并返回更新字典的函数
        
    节点函数返回值:
        dict 包含以下键:
            - messages: 更新后的消息历史（append 策略）
            - fundamentals_report: 生成的基本面分析报告文本
    
    实现细节:
        - 从 prompt.j2 加载 Jinja2 模板并渲染 system prompt
        - 使用 LangChain 的 create_agent 创建 agent 实例
        - 配置递归限制以支持多次工具调用
        - 从后往前查找最后一条非工具调用的 AI 消息作为最终报告
        - 工具调用顺序：公司信息 -> 财务报表 -> 财务指标 -> 估值指标 -> 业绩数据（可选）
    """
    
    def fundamentals_analyst_node(state: FundamentalsAnalystState) -> dict[str, Any]:
        """
        Fundamentals Analyst 节点的执行函数
        
        Args:
            state: 当前的 FundamentalsAnalystState
            
        Returns:
            包含 messages 和 fundamentals_report 的更新字典
        """
        # 第一阶段：准备参数
        current_date = state["trade_date"]
        ticker = state["company_of_interest"]
        
        # 工具列表：基本面分析工具（按调用顺序）
        tools = [
            get_company_info,
            get_financial_statements,
            get_financial_indicators,
            get_valuation_indicators,
            get_earnings_data,
        ]
        
        # 第二阶段：加载并渲染 prompt 模板
        prompt_path = Path(__file__).parent / "prompt.j2"
        with open(prompt_path, "r", encoding="utf-8") as f:
            template = Template(f.read())
        
        system_prompt = template.render(
            tool_names=", ".join([tool.name for tool in tools]),
            current_date=current_date,
            ticker=ticker
        )
        
        # 第三阶段：创建并调用 agent
        agent = create_agent(
            model=llm,
            tools=tools,
            system_prompt=system_prompt
        )
        
        # 准备输入消息
        if not state["messages"]:
            # 如果没有消息，使用初始消息
            last_message = {"role": "user", "content": f"分析股票 {ticker} 的基本面和估值情况"}
        else:
            last_message = state["messages"][-1]
        
        result = agent.invoke(
            input=last_message,
            config={"recursion_limit": 50}  # 增加递归限制以支持多次工具调用
        )
        
        # 第四阶段：提取最终报告
        # 从后往前查找最后一条非工具调用的 AI 消息
        fundamentals_report = ""
        structured_data = None
        metadata = None
        
        for msg in reversed(result["messages"]):
            # 检查是否是 AI 消息且有内容
            if hasattr(msg, 'content') and msg.content:
                # 检查是否有 tool_calls（如果有，说明是工具调用请求，不是最终回复）
                has_tool_calls = False
                if hasattr(msg, 'tool_calls'):
                    if msg.tool_calls and len(msg.tool_calls) > 0:
                        has_tool_calls = True
                
                # 如果没有工具调用，这就是最终的 AI 回复
                if not has_tool_calls:
                    # 解析 JSON 输出
                    report_content, structured_data, metadata = parse_analyst_output(
                        msg.content, "fundamentals"
                    )
                    fundamentals_report = report_content
                    break
        
        # 如果没有找到，使用最后一条消息的内容（作为兜底）
        if not fundamentals_report and result["messages"]:
            last_msg = result["messages"][-1]
            if hasattr(last_msg, 'content') and last_msg.content:
                report_content, structured_data, metadata = parse_analyst_output(
                    last_msg.content, "fundamentals"
                )
                fundamentals_report = report_content
        
        # 验证结构化数据（如果存在）
        if structured_data:
            is_valid, error_msg = validate_analyst_json(structured_data, "fundamentals")
            if not is_valid:
                print(f"[WARN] Fundamentals Analyst JSON 验证失败: {error_msg}")
        
        return {
            "messages": result["messages"],
            "fundamentals_report": fundamentals_report,
            "fundamentals_structured_data": json.dumps(structured_data, ensure_ascii=False) if structured_data else None,
            "fundamentals_metadata": json.dumps(metadata, ensure_ascii=False) if metadata else None,
        }
    
    return fundamentals_analyst_node
