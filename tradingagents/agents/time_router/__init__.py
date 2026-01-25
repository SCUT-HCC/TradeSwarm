"""
Time Router 模块

根据当前时间自动设置 trading_session 字段。
"""

from .node import create_time_router_node

__all__ = ["create_time_router_node"]
