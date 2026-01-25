"""
Graph 工具函数

包含 MockMemory、LLM 初始化等工具函数。
"""

from typing import Any, List, Dict
from pathlib import Path
import yaml
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
import os

load_dotenv()


class MockMemory:
    """
    Mock Memory 类，用于测试阶段。
    
    get_memories() 返回写死的示例数据，模拟 FinancialSituationMemory 的行为。
    """
    
    def get_memories(self, current_situation: str, n_matches: int = 2) -> List[Dict[str, Any]]:
        """
        返回写死的示例记忆数据。
        
        Args:
            current_situation: 当前情境描述（未使用，仅保持接口一致）
            n_matches: 返回的匹配数量（未使用，仅保持接口一致）
            
        Returns:
            包含示例记忆数据的列表
        """
        return [
            {
                "matched_situation": "市场出现技术性回调，但基本面依然强劲，成交量放大",
                "recommendation": "建议在回调时逐步建仓，设置止损位，关注关键支撑位。",
                "similarity_score": 0.85,
            },
            {
                "matched_situation": "行业政策利好，公司业绩超预期，但短期涨幅较大",
                "recommendation": "建议分批买入，避免追高，关注回调机会。",
                "similarity_score": 0.78,
            },
        ]


def load_llm_from_config(config_path: str = "config/config.yaml") -> ChatOpenAI:
    """
    从 config.yaml 读取 LLM 配置并初始化 ChatOpenAI 实例。
    
    Args:
        config_path: 配置文件路径，默认为 "config/config.yaml"
        
    Returns:
        初始化好的 ChatOpenAI 实例
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    with open(config_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    llm_config = config.get("llm", {})
    
    # 优先使用环境变量，如果没有则使用配置文件中的值
    api_key = os.getenv("DASHSCOPE_API_KEY") or llm_config.get("api_key")
    if not api_key:
        raise ValueError("未找到 API Key，请设置环境变量 DASHSCOPE_API_KEY 或在配置文件中设置")
    
    base_url = llm_config.get("base_url", "https://dashscope.aliyuncs.com/compatible-mode/v1")
    model_name = llm_config.get("model_name", "qwen-plus")
    temperature = llm_config.get("temperature", 0.1)
    
    return ChatOpenAI(
        api_key=api_key,
        base_url=base_url,
        model=model_name,
        temperature=temperature,
    )

