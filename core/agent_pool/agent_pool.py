"""
Agent池核心实现

负责管理多个异构Agent的并行执行和API限流控制
"""

import asyncio
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from tqdm import tqdm

from core.agent.base_agent import BaseAgent
from .types import AgentResult


class RateLimiter:
    """
    基于令牌桶算法的API速率限制器

    工作原理:
        - 维护一个虚拟令牌桶，容量为max_tokens
        - 令牌以rate速率补充（每秒补充rate个）
        - 每次API调用消耗1个令牌
        - 令牌不足时等待补充

    特点:
        - 允许短时burst（桶中有余量时）
        - 保证长期平均速率不超过限制
    """

    def __init__(self, rate: float, max_tokens: int):
        """
        初始化速率限制器

        参数:
            rate: 令牌补充速率（每秒生成的令牌数）
            max_tokens: 令牌桶容量
        """
        self.rate = rate
        self.max_tokens = max_tokens
        self.tokens = max_tokens
        self.last_update = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """获取一个令牌，如果令牌不足则等待"""
        async with self._lock:
            while self.tokens < 1:
                # 计算需要等待的时间
                wait_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self._refill()

            self.tokens -= 1
            self._refill()

    def _refill(self):
        """补充令牌"""
        now = time.time()
        elapsed = now - self.last_update

        # 根据经过的时间补充令牌
        new_tokens = elapsed * self.rate
        self.tokens = min(self.tokens + new_tokens, self.max_tokens)
        self.last_update = now


class AgentPool:
    """
    Agent池：管理大量异构Agent的并行执行

    核心功能:
        1. 注册和管理多个BaseAgent实例
        2. 异步并发执行（基于asyncio）
        3. 双层限流控制（Semaphore + RateLimiter）
        4. 优雅降级的异常处理

    设计特点:
        - 支持异构Agent（不同配置、不同角色）
        - 伪并发：逻辑并发 + 限流自动调度
        - 配置驱动：通过JSON配置创建Agent
    """

    def __init__(
        self,
        max_concurrent: int = 20,
        rate_limit: float = 10.0,
        rate_limit_capacity: int = 60
    ):
        """
        初始化Agent池

        参数:
            max_concurrent: 最大并发数（Semaphore槽位数）
            rate_limit: API调用速率限制（每秒请求数）
            rate_limit_capacity: 令牌桶容量
        """
        self.agents: Dict[str, BaseAgent] = {}
        self.max_concurrent = max_concurrent

        # 第一层限流：Semaphore控制并发槽位
        self._semaphore = asyncio.Semaphore(max_concurrent)

        # 第二层限流：RateLimiter控制API速率
        self._rate_limiter = RateLimiter(rate_limit, rate_limit_capacity)

        self.logger = logging.getLogger("AgentPool")

    def register_agent(self, agent_id: str, agent: BaseAgent):
        """
        注册Agent到池中

        参数:
            agent_id: Agent唯一标识
            agent: BaseAgent实例
        """
        if agent_id in self.agents:
            self.logger.warning(f"Agent {agent_id} 已存在，将被覆盖")

        self.agents[agent_id] = agent
        self.logger.info(f"Agent {agent_id} 已注册到池中")

    def register_agents_from_configs(
        self,
        configs: List[Dict[str, Any]],
        id_prefix: str = "agent",
        show_progress: bool = True
    ) -> List[str]:
        """
        从配置列表批量创建和注册Agent

        参数:
            configs: Agent配置字典列表
            id_prefix: Agent ID前缀
            show_progress: 是否显示进度条（默认True）

        返回:
            注册的Agent ID列表
        """
        agent_ids = []

        # 创建进度条
        iterator = tqdm(
            enumerate(configs),
            total=len(configs),
            desc=f"注册Agent",
            unit="agent",
            disable=not show_progress,
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
        ) if show_progress else enumerate(configs)

        for i, config in iterator:
            agent_id = f"{id_prefix}_{i+1:03d}"
            agent = BaseAgent(config)
            self.register_agent(agent_id, agent)
            agent_ids.append(agent_id)

            # 更新进度条描述（显示当前注册的Agent ID）
            if show_progress and hasattr(iterator, 'set_postfix'):
                iterator.set_postfix(current=agent_id)

        self.logger.info(f"批量注册了 {len(agent_ids)} 个Agent")
        return agent_ids

    async def execute_agent(
        self,
        agent_id: str,
        input_text: str
    ) -> AgentResult:
        """
        执行单个Agent（带限流控制）

        参数:
            agent_id: Agent标识
            input_text: 输入文本

        返回:
            AgentResult执行结果
        """
        agent = self.agents.get(agent_id)
        if agent is None:
            error_msg = f"Agent {agent_id} 不存在"
            self.logger.error(error_msg)
            return AgentResult(
                agent_id=agent_id,
                task_id="",
                output="",
                execution_time=0.0,
                contexts_used={},
                success=False,
                error=error_msg
            )

        # 第一层限流：获取并发槽位
        async with self._semaphore:
            # 第二层限流：获取API调用令牌
            await self._rate_limiter.acquire()

            # 执行Agent
            start_time = time.time()
            try:
                # 调用BaseAgent的异步执行方法
                output = await agent.async_run(input_text)
                execution_time = time.time() - start_time

                return AgentResult(
                    agent_id=agent_id,
                    task_id="",
                    output=output,
                    execution_time=execution_time,
                    contexts_used={},
                    success=True
                )

            except Exception as e:
                execution_time = time.time() - start_time
                self.logger.error(
                    f"Agent {agent_id} 执行失败: {str(e)}",
                    exc_info=True
                )

                return AgentResult(
                    agent_id=agent_id,
                    task_id="",
                    output="",
                    execution_time=execution_time,
                    contexts_used={},
                    success=False,
                    error=str(e)
                )

    async def execute_all(
        self,
        input_text: str,
        agent_ids: Optional[List[str]] = None
    ) -> Dict[str, AgentResult]:
        """
        并行执行所有Agent（或指定的Agent子集）

        参数:
            input_text: 所有Agent的输入文本
            agent_ids: 可选的Agent ID列表，None表示执行所有Agent

        返回:
            {agent_id: AgentResult} 字典
        """
        # 确定要执行的Agent列表
        if agent_ids is None:
            agent_ids = list(self.agents.keys())

        if not agent_ids:
            self.logger.warning("没有可执行的Agent")
            return {}

        self.logger.info(
            f"开始并行执行 {len(agent_ids)} 个Agent "
            f"(并发限制: {self.max_concurrent}, "
            f"速率限制: {self._rate_limiter.rate} req/s)"
        )

        # 使用asyncio.gather并发执行所有Agent
        # return_exceptions=True 确保单个Agent失败不影响其他Agent
        tasks = [
            self.execute_agent(agent_id, input_text)
            for agent_id in agent_ids
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 整理结果
        results_dict = {}
        success_count = 0

        for agent_id, result in zip(agent_ids, results):
            if isinstance(result, Exception):
                # 处理异常情况
                self.logger.error(
                    f"Agent {agent_id} 返回异常: {str(result)}"
                )
                results_dict[agent_id] = AgentResult(
                    agent_id=agent_id,
                    task_id="",
                    output="",
                    execution_time=0.0,
                    contexts_used={},
                    success=False,
                    error=str(result)
                )
            else:
                results_dict[agent_id] = result
                if result.success:
                    success_count += 1

        self.logger.info(
            f"执行完成: {success_count}/{len(agent_ids)} 个Agent成功"
        )

        return results_dict

    def get_agent_count(self) -> int:
        """获取已注册的Agent数量"""
        return len(self.agents)

    def get_agent_ids(self) -> List[str]:
        """获取所有已注册的Agent ID"""
        return list(self.agents.keys())

    def remove_agent(self, agent_id: str) -> bool:
        """
        从池中移除Agent

        参数:
            agent_id: Agent标识

        返回:
            是否成功移除
        """
        if agent_id in self.agents:
            del self.agents[agent_id]
            self.logger.info(f"Agent {agent_id} 已从池中移除")
            return True

        self.logger.warning(f"Agent {agent_id} 不存在，无法移除")
        return False

    def clear(self):
        """清空Agent池"""
        count = len(self.agents)
        self.agents.clear()
        self.logger.info(f"Agent池已清空，移除了 {count} 个Agent")
