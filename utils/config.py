"""
环境变量配置管理模块

本模块负责读取.env文件中的环境变量，并提供统一的配置访问接口。
使用单例模式确保配置在整个应用中保持一致。
"""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv


class Config:
    """
    环境变量配置类

    负责加载和管理所有环境变量配置，包括：
    - 阿里百联API配置
    - 多模型配置
    - 向量数据库配置
    - 日志配置
    - 实验配置
    - SWE-bench配置

    使用方式:
        from utils.config import get_config
        config = get_config()
        api_key = config.alibaba_api_key
    """

    _instance: Optional['Config'] = None

    def __new__(cls):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """初始化配置，加载环境变量"""
        if self._initialized:
            return

        # 加载.env文件
        env_path = Path(__file__).parent.parent / '.env'
        if env_path.exists():
            load_dotenv(env_path)

        # 第一阶段：阿里百联API配置
        self.alibaba_api_key = os.getenv('ALIBABA_API_KEY', '')
        self.alibaba_api_base = os.getenv('ALIBABA_API_BASE', 'https://dashscope.aliyuncs.com/compatible-mode/v1')
        self.alibaba_model_name = os.getenv('ALIBABA_MODEL_NAME', 'qwen-max')

        # 第二阶段：多模型配置
        self.alibaba_model_cheap = os.getenv('ALIBABA_MODEL_CHEAP', 'qwen-turbo')
        self.alibaba_model_powerful = os.getenv('ALIBABA_MODEL_POWERFUL', 'qwen-max')

        # 第三阶段：向量数据库配置
        self.chroma_persist_dir = os.getenv('CHROMA_PERSIST_DIR', './data/chromadb')

        # 第四阶段：日志配置
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.log_dir = os.getenv('LOG_DIR', './logs')

        # 第五阶段：实验配置
        self.random_seed = int(os.getenv('RANDOM_SEED', '42'))
        self.max_concurrent_agents = int(os.getenv('MAX_CONCURRENT_AGENTS', '5'))
        self.agent_timeout = int(os.getenv('AGENT_TIMEOUT', '300'))

        self._initialized = True

    def validate(self) -> tuple[bool, str]:
        """
        验证必要的配置是否已设置

        Returns:
            tuple[bool, str]: (是否验证通过, 错误信息)
        """
        if not self.alibaba_api_key:
            return False, "ALIBABA_API_KEY未设置，请在.env文件中配置"

        if self.alibaba_api_key == 'your_api_key_here':
            return False, "请将ALIBABA_API_KEY替换为实际的API密钥"

        return True, ""

    def __repr__(self) -> str:
        """字符串表示（隐藏敏感信息）"""
        return (
            f"Config(\n"
            f"  alibaba_api_key={'*' * 20 if self.alibaba_api_key else 'NOT_SET'},\n"
            f"  alibaba_api_base={self.alibaba_api_base},\n"
            f"  alibaba_model_name={self.alibaba_model_name},\n"
            f"  log_level={self.log_level},\n"
            f"  random_seed={self.random_seed}\n"
            f")"
        )


# 全局单例访问函数
_config_instance: Optional[Config] = None


def get_config() -> Config:
    """
    获取配置单例实例

    Returns:
        Config: 配置对象

    示例:
        >>> from utils.config import get_config
        >>> config = get_config()
        >>> print(config.alibaba_api_key)
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance


def reload_config() -> Config:
    """
    重新加载配置（主要用于测试或动态配置场景）

    Returns:
        Config: 新的配置对象
    """
    global _config_instance
    Config._instance = None
    _config_instance = None
    return get_config()