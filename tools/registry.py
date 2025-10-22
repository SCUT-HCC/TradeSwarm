"""
工具注册表模块

提供全局工具注册和管理机制，采用单例模式
"""

from typing import Dict, List, Any
from camel.toolkits import FunctionTool


class ToolRegistry:
    """
    工具注册表（单例模式）

    负责工具的注册、管理和根据配置获取启用的工具列表
    """

    # 类变量：全局工具注册表
    _registry: Dict[str, Dict[str, FunctionTool]] = {}

    # 单例实例
    _instance = None

    def __new__(cls):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def register(cls, category: str, name: str, tool: FunctionTool) -> None:
        """
        注册工具

        参数:
            category: 工具类别 ("file_operation" | "information_retrieval")
            name: 工具名称
            tool: FunctionTool实例
        """
        if category not in cls._registry:
            cls._registry[category] = {}

        cls._registry[category][name] = tool

    @classmethod
    def get_enabled_tools(cls, tool_configs: List[Dict[str, Any]]) -> List[FunctionTool]:
        """
        根据配置获取启用的工具列表

        参数:
            tool_configs: 工具配置列表，每个元素包含:
                - category: 工具类别
                - name: 工具名称
                - enabled: 是否启用
                - params: 可选的默认参数

        返回:
            启用的FunctionTool实例列表

        异常:
            ValueError: 如果请求的工具未注册
        """
        enabled_tools: List[FunctionTool] = []

        for tool_cfg in tool_configs:
            # 跳过未启用的工具
            if not tool_cfg.get("enabled", False):
                continue

            category = tool_cfg["category"]
            name = tool_cfg["name"]

            # 检查工具是否已注册
            if category not in cls._registry:
                raise ValueError(
                    f"工具类别 '{category}' 未注册"
                )

            if name not in cls._registry[category]:
                raise ValueError(
                    f"工具 '{name}' 在类别 '{category}' 中未注册. "
                    f"已注册的工具: {list(cls._registry[category].keys())}"
                )

            tool = cls._registry[category][name]
            enabled_tools.append(tool)

        return enabled_tools

    @classmethod
    def list_registered_tools(cls) -> Dict[str, List[str]]:
        """
        列出所有已注册的工具

        返回:
            字典，键为类别，值为该类别下的工具名称列表
        """
        result = {}
        for category, tools in cls._registry.items():
            result[category] = list(tools.keys())
        return result

    @classmethod
    def get_tool(cls, category: str, name: str) -> FunctionTool:
        """
        获取指定工具

        参数:
            category: 工具类别
            name: 工具名称

        返回:
            FunctionTool实例

        异常:
            ValueError: 如果工具不存在
        """
        if category not in cls._registry:
            raise ValueError(f"工具类别 '{category}' 未注册")

        if name not in cls._registry[category]:
            raise ValueError(
                f"工具 '{name}' 在类别 '{category}' 中未注册"
            )

        return cls._registry[category][name]

    @classmethod
    def clear(cls) -> None:
        """清空注册表（主要用于测试）"""
        cls._registry.clear()
