"""
Time Router Node

根据当前时间判断交易时段，并设置 trading_session 字段。

时间划分规则：
- 9:30 以前：pre_open（盘前）
- 9:30：market_open（开盘）
- 9:30-15:00：intraday（盘中）
- 15:00 以后：post_close（盘后）
"""

from typing import Callable, Dict, Any, Literal
from datetime import datetime, time
from tradingagents.agents.utils.agentstate.agent_states import AgentState


def create_time_router_node() -> Callable[[AgentState], Dict[str, Any]]:
    """
    创建 Time Router 节点函数
    
    该函数返回一个符合 LangGraph 节点规范的函数，用于根据当前时间
    自动判断交易时段并设置 trading_session 字段。
    
    Returns:
        time_router_node: 一个接受 AgentState 并返回更新字典的函数
        
    节点函数返回值:
        dict 包含以下键:
            - trading_session: 交易时段（pre_open / market_open / intraday / post_close）
    
    实现细节:
        - 获取当前系统时间
        - 根据时间判断交易时段
        - 9:30 以前：pre_open
        - 9:30：market_open
        - 9:30-15:00：intraday
        - 15:00 以后：post_close
    """
    
    def time_router_node(state: AgentState) -> Dict[str, Any]:
        """
        Time Router 节点的执行函数
        
        Args:
            state: 当前的 AgentState
            
        Returns:
            包含 trading_session 的更新字典
        """
        # 第一阶段：获取当前时间
        current_time = datetime.now().time()
        
        # 第二阶段：判断交易时段
        trading_session = _determine_trading_session(current_time)
        
        # 第三阶段：返回更新字典
        return {
            "trading_session": trading_session
        }
    
    return time_router_node


def _determine_trading_session(current_time: time) -> Literal["pre_open", "market_open", "intraday", "post_close"]:
    """
    根据当前时间判断交易时段
    
    Args:
        current_time: 当前时间（time 对象）
        
    Returns:
        交易时段字符串
        - pre_open: 9:30 以前
        - market_open: 9:30（精确到分钟，即 9:30:00-9:30:59）
        - intraday: 9:30:01-14:59:59
        - post_close: 15:00 以后
    """
    # 定义关键时间点
    market_open_time = time(9, 30, 0)  # 9:30:00
    market_open_end = time(9, 30, 59)  # 9:30:59
    market_close_time = time(15, 0, 0)  # 15:00:00
    
    # 判断交易时段
    if current_time < market_open_time:
        # 9:30 以前：盘前
        return "pre_open"
    elif market_open_time <= current_time <= market_open_end:
        # 9:30（精确到分钟）：开盘
        return "market_open"
    elif market_open_end < current_time < market_close_time:
        # 9:30:01-14:59:59：盘中
        return "intraday"
    else:
        # 15:00 以后：盘后
        return "post_close"
