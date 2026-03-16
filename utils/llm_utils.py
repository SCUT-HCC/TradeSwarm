from langchain_openai import ChatOpenAI
from langchain.messages import HumanMessage, AIMessage, SystemMessage
import os

def init_llm(config: dict) -> ChatOpenAI:
    """
    Initialize the LLM from the provided configuration.
    
    注意：qwen API 不支持代理，因此会临时禁用环境变量中的代理设置。
    
    Args:
        config (dict): The configuration dictionary containing an 'llm' section.
        
    Returns:
        ChatOpenAI: The initialized LangChain ChatOpenAI object.
    """
    llm_config = config.get('llm', {})
    
    # 保存当前的代理设置
    old_http_proxy = os.environ.get('HTTP_PROXY')
    old_https_proxy = os.environ.get('HTTPS_PROXY')
    
    # 临时禁用代理（qwen 不支持代理）
    if 'HTTP_PROXY' in os.environ:
        del os.environ['HTTP_PROXY']
    if 'HTTPS_PROXY' in os.environ:
        del os.environ['HTTPS_PROXY']
    
    # 创建 httpx 客户端，明确禁用代理
    import httpx
    # httpx.Client 不接受 proxies 参数，我们需要通过环境变量控制
    # 由于已经删除了环境变量，直接创建客户端即可
    http_client = httpx.Client(
        verify=True,
    )
    
    llm = ChatOpenAI(
        api_key=llm_config.get('api_key'),
        base_url=llm_config.get('base_url'),
        model=llm_config.get('model_name'),
        temperature=llm_config.get('temperature', 0.1),
        http_client=http_client,  # 使用自定义的 http_client（不包含代理）
    )
    
    # 不恢复代理设置，因为后续 LLM 调用也不应该使用代理
    # 如果需要恢复，可以在调用完成后手动恢复
    # if old_http_proxy:
    #     os.environ['HTTP_PROXY'] = old_http_proxy
    # if old_https_proxy:
    #     os.environ['HTTPS_PROXY'] = old_https_proxy
    
    return llm
