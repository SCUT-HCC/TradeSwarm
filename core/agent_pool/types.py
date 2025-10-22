"""
Agent Pool 数据类型定义

定义系统中流转的核心数据结构
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class TaskInfo:
    """任务信息"""
    task_id: str
    description: str
    priority: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class DispatchPackage:
    """派发给Agent的输入包"""
    task: TaskInfo
    contexts: Dict[str, Any]
    reflection: Optional[str] = None
    agent_id: str = ""


@dataclass
class AgentResult:
    """单个Agent的执行结果"""
    agent_id: str
    task_id: str
    output: str
    execution_time: float
    contexts_used: Dict[str, Any]
    success: bool = True
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class AggregatedResult:
    """聚合后的集体结果"""
    task_id: str
    aggregated_output: str
    strategy_used: str
    participating_agents: List[str]
    individual_results: Dict[str, AgentResult]
    confidence: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class Reflection:
    """Agent的反思结果"""
    agent_id: str
    task_id: str
    iteration: int

    # 三层认知模型
    what_differences: str  # 第一层: 事实层差异
    why_differences: str   # 第二层: 认知层原因
    how_to_improve: str    # 第三层: 元认知层改进计划

    # 可选的结构化字段
    unique_contributions: List[str] = field(default_factory=list)
    missed_points: List[str] = field(default_factory=list)

    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IterationResult:
    """单轮迭代的执行结果统计"""
    iteration_number: int
    total_agents: int
    successful_agents: int
    failed_agents: int
    average_execution_time: float
    total_time: float
    aggregation_strategy: str
    reflections_generated: int
    timestamp: datetime = field(default_factory=datetime.now)
