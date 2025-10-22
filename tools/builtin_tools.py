"""
内置工具配置模块

使用CAMEL框架提供的内置工具包，无需自己实现基础功能
"""

from camel.toolkits import (
    SearchToolkit,
    FileWriteToolkit,
)


def get_file_operation_tools():
    """
    获取文件操作工具

    使用CAMEL的FileWriteToolkit提供文件写入功能

    返回:
        工具列表
    """
    toolkit = FileWriteToolkit()
    return toolkit.get_tools()


def get_information_retrieval_tools():
    """
    获取信息检索工具

    使用CAMEL的SearchToolkit提供网络搜索功能

    返回:
        工具列表
    """
    toolkit = SearchToolkit()
    return toolkit.get_tools()


# 注册内置工具到ToolRegistry
def register_builtin_tools():
    """将CAMEL内置工具注册到ToolRegistry"""
    from tools.registry import ToolRegistry

    # 注册文件操作工具
    file_tools = get_file_operation_tools()
    for tool in file_tools:
        ToolRegistry.register("file_operation", tool.func.__name__, tool)

    # 注册信息检索工具
    search_tools = get_information_retrieval_tools()
    for tool in search_tools:
        ToolRegistry.register("information_retrieval", tool.func.__name__, tool)
