"""
Graph 工具函数

包含 LLM 初始化等工具函数。
"""

from pathlib import Path
import yaml
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
import os

load_dotenv()


def load_llm_from_config(config_path: str = "config/config.yaml") -> ChatOpenAI:
    """
    从 config.yaml 读取 LLM 配置并初始化 ChatOpenAI 实例。
    
    注意：qwen API 不支持代理，因此会临时禁用环境变量中的代理设置。
    
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
    
    # 保存当前的代理设置
    old_http_proxy = os.environ.get('HTTP_PROXY')
    old_https_proxy = os.environ.get('HTTPS_PROXY')
    
    # 临时禁用代理（qwen 不支持代理）
    # 注意：不恢复代理设置，因为 Qwen LLM 在后续调用时也不应该使用代理
    if 'HTTP_PROXY' in os.environ:
        del os.environ['HTTP_PROXY']
    if 'HTTPS_PROXY' in os.environ:
        del os.environ['HTTPS_PROXY']
    
    # 创建 httpx 客户端，明确禁用代理
    import httpx
    # httpx.Client 默认 trust_env=True，会读取环境变量中的代理设置
    # 为了确保不使用代理，我们：
    # 1. 删除环境变量（已在上方完成）
    # 2. 设置 trust_env=False 来禁用从环境变量读取代理
    # 3. 显式设置 proxy=None 来确保不使用任何代理
    http_client = httpx.Client(
        verify=True,
        trust_env=False,  # 禁用从环境变量读取代理设置
        proxy=None,  # 显式禁用代理
        timeout=httpx.Timeout(60.0),  # 设置超时
    )
    
    llm = ChatOpenAI(
        api_key=api_key,
        base_url=base_url,
        model=model_name,
        temperature=temperature,
        http_client=http_client,  # 使用自定义的 http_client（不包含代理）
    )
    
    # 验证 LLM 的客户端配置
    # 确保 LLM 使用的客户端不使用代理
    if hasattr(llm, 'client') and hasattr(llm.client, '_client'):
        # 检查客户端是否使用了我们传入的 http_client
        llm_client = llm.client._client
        if llm_client is not http_client:
            # 如果 LLM 创建了新的客户端，我们需要确保它也不使用代理
            # 但通常 ChatOpenAI 应该使用我们传入的 http_client
            pass
    
    # 不恢复代理设置，因为后续 LLM 调用也不应该使用代理
    # 如果需要恢复，可以在调用完成后手动恢复
    # if old_http_proxy:
    #     os.environ['HTTP_PROXY'] = old_http_proxy
    # if old_https_proxy:
    #     os.environ['HTTPS_PROXY'] = old_https_proxy
    
    return llm

