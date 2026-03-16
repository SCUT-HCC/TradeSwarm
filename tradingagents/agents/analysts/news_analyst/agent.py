from typing import Callable, Any
from pathlib import Path
from jinja2 import Template
from langchain.agents import create_agent
from langchain_core.language_models import BaseChatModel
import json

from .state import NewsAnalystState
from tradingagents.tool_nodes.utils import get_news, get_global_news
from tradingagents.agents.utils.json_parser import parse_analyst_output, validate_analyst_json


def create_news_analyst(llm: BaseChatModel) -> Callable[[NewsAnalystState], dict[str, Any]]:
    """
    创建 News Analyst agent 节点函数
    
    该函数返回一个符合 LangGraph 节点规范的函数，用于执行新闻和宏观经济分析。
    agent 会分析过去7天的全球新闻和公司特定事件，生成详细的新闻分析报告。
    
    Args:
        llm: LangChain BaseChatModel 实例，用于驱动 agent 的推理和决策
        
    Returns:
        news_analyst_node: 一个接受 NewsAnalystState 并返回更新字典的函数
        
    节点函数返回值:
        dict 包含以下键:
            - messages: 更新后的消息历史（append 策略）
            - news_report: 生成的新闻分析报告文本
    
    实现细节:
        - 从 prompt.j2 加载 Jinja2 模板并渲染 system prompt
        - 使用 LangChain 的 create_agent 创建 agent 实例
        - 仅当 agent 未调用工具时，才提取最终报告内容
    """
    
    def news_analyst_node(state: NewsAnalystState) -> dict[str, Any]:
        """
        News Analyst 节点的执行函数
        
        Args:
            state: 当前的 NewsAnalystState
            
        Returns:
            包含 messages 和 news_report 的更新字典
        """
        # 第一阶段：准备参数
        current_date = state["trade_date"]
        ticker = state["company_of_interest"]
        
        # 工具列表：新闻和宏观新闻分析工具
        tools = [
            get_news,
            get_global_news,
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
        
        result = agent.invoke({
            "messages": state["messages"]
        })
        
        # 第四阶段：提取结果
        # 从后往前查找最后一条非工具调用的 AI 消息
        news_report = ""
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
                        msg.content, "news"
                    )
                    news_report = report_content
                    break
        
        # 如果没有找到，使用最后一条消息的内容（作为兜底）
        if not news_report and result["messages"]:
            last_msg = result["messages"][-1]
            if hasattr(last_msg, 'content') and last_msg.content:
                report_content, structured_data, metadata = parse_analyst_output(
                    last_msg.content, "news"
                )
                news_report = report_content
        
        # 验证结构化数据（如果存在）
        if structured_data:
            is_valid, error_msg = validate_analyst_json(structured_data, "news")
            if not is_valid:
                print(f"[WARN] News Analyst JSON 验证失败: {error_msg}")
        
        return {
            "messages": result["messages"],
            "news_report": news_report,
            "news_structured_data": json.dumps(structured_data, ensure_ascii=False) if structured_data else None,
            "news_metadata": json.dumps(metadata, ensure_ascii=False) if metadata else None,
        }
    
    return news_analyst_node
