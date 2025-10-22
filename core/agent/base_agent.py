"""
BaseAgent核心类模块

配置驱动的Agent基础类，通过组合CAMEL的ChatAgent实现LLM交互，
提供三阶段执行框架（observing → planning → acting）和配置管理。
"""

from typing import Dict, Any, List, Optional
import time
import logging
import asyncio

from camel.agents import ChatAgent
from camel.messages import BaseMessage
from camel.types import ModelType
from camel.models import ModelFactory

from core.agent.config_validator import ConfigValidator, ValidationError
from core.agent.prompt_manager import PromptManager
from tools.registry import ToolRegistry


class BaseAgent:
    """
    配置驱动的Agent基础类

    通过组合CAMEL的ChatAgent实现LLM交互，
    提供三阶段执行框架和配置管理，同时支持同步和异步执行。

    架构:
        用户输入 → observing() → planning() → acting() → 返回结果

    三阶段流程:
        1. observing: 观察与理解阶段，分析用户输入，可能调用工具获取上下文
        2. planning: 推理与规划阶段，基于observation进行CoT推理
        3. acting: 执行与反馈阶段，根据plan执行工具调用并整合结果

    执行模式:
        - 同步执行: run() / observing() / planning() / acting()
        - 异步执行: async_run() / async_observing() / async_planning() / async_acting()

    使用场景:
        - 单Agent执行: 使用同步方法 run()
        - 多Agent并行执行: 使用异步方法 async_run() 配合 asyncio.gather()
    """

    def __init__(self, config: Dict[str, Any]):
        """
        初始化BaseAgent

        参数:
            config: 配置字典（已加载的JSON配置）

        流程:
            1. 验证配置 (ConfigValidator)
            2. 初始化模型 (ModelBackend)
            3. 注册工具 (ToolRegistry)
            4. 创建ChatAgent (组合)
            5. 初始化PromptManager

        异常:
            ValidationError: 配置验证失败
        """
        # 第一阶段: 验证配置
        validator = ConfigValidator()
        validator.validate(config)

        self.config = config
        self._setup_logging()

        # 第二阶段: 初始化模型
        self._init_model()

        # 第三阶段: 注册和获取工具
        self._init_tools()

        # 第四阶段: 创建ChatAgent（组合模式）
        self._init_chat_agent()

        # 第五阶段: 初始化PromptManager
        self._prompt_manager = PromptManager(config["execution_flow"])

        self.logger.info(
            f"BaseAgent初始化成功: {config['agent_profile']['name']}"
        )

    def _setup_logging(self) -> None:
        """配置日志系统"""
        log_level = self.config["error_handling"].get("log_level", "INFO")
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(
            self.config["agent_profile"]["name"]
        )

    def _init_model(self) -> None:
        """初始化模型配置"""
        import os
        from camel.models import ModelFactory
        from camel.types import ModelPlatformType
        from camel.configs import QwenConfig
        
        model_cfg = self.config["model_config"]
        self._model_type = model_cfg["model_type"]
        self._model_config_dict = model_cfg.get("model_config_dict", {})
        self._system_prompt = model_cfg.get("system_prompt", "")
        
        # 获取API密钥,支持ALIBABA_API_KEY或QWEN_API_KEY
        api_key = os.getenv("ALIBABA_API_KEY") or os.getenv("QWEN_API_KEY")
        if not api_key:
            raise ValueError(
                "需要设置环境变量 ALIBABA_API_KEY 或 QWEN_API_KEY\n"
                "请在 .env 文件中配置: ALIBABA_API_KEY=your_api_key_here"
            )
        
        # 获取API base URL
        api_base = os.getenv("ALIBABA_API_BASE") or "https://dashscope.aliyuncs.com/compatible-mode/v1"
        
        # 使用ModelFactory创建带配置的模型实例
        self._model = ModelFactory.create(
            model_platform=ModelPlatformType.QWEN,
            model_type=self._model_type,
            model_config_dict=QwenConfig(**self._model_config_dict).as_dict(),
            api_key=api_key,
            url=api_base
        )
        
        self.logger.info(f"模型已初始化: {self._model_type}, 配置: {self._model_config_dict}")

    def _init_tools(self) -> None:
        """初始化工具系统"""
        tool_configs = self.config["tool_config"]

        # 注册内置工具（如果还未注册）
        from tools.builtin_tools import register_builtin_tools
        try:
            register_builtin_tools()
        except Exception as e:
            self.logger.warning(f"工具注册警告: {e}")

        # 获取启用的工具列表
        self._tools = ToolRegistry.get_enabled_tools(tool_configs)
        self.logger.info(f"已加载 {len(self._tools)} 个工具")

    def _init_chat_agent(self) -> None:
        """初始化ChatAgent(组合模式)"""
        # 创建系统消息
        system_message = BaseMessage.make_assistant_message(
            role_name=self.config["agent_profile"]["role"],
            content=self._system_prompt
        )

        # 创建ChatAgent,使用预先配置的模型实例
        self._chat_agent = ChatAgent(
            system_message=system_message,
            model=self._model,  # 使用ModelFactory创建的模型实例
            tools=self._tools if self._tools else None
        )

        self.logger.info(f"ChatAgent已创建,模型: {self._model_type}")

    def run(self, user_input: str) -> str:
        """
        主执行入口：三阶段循环

        参数:
            user_input: 用户输入文本

        返回:
            执行结果文本

        流程:
            user_input → observing() → planning() → acting() → result
        """
        self.logger.info(f"开始执行任务: {user_input[:50]}...")

        try:
            # 第一阶段: 观察与理解
            observation = self.observing(user_input)
            self.logger.debug(f"Observation: {observation[:100]}...")

            # 第二阶段: 推理与规划
            plan = self.planning(observation)
            self.logger.debug(f"Plan: {plan[:100]}...")

            # 第三阶段: 执行与反馈
            result = self.acting(plan)
            self.logger.info("任务执行完成")

            return result

        except Exception as e:
            self.logger.error(f"执行失败: {str(e)}", exc_info=True)
            return f"执行过程中发生错误: {str(e)}"

    def observing(self, input_text: str) -> str:
        """
        第一阶段: 观察与理解

        分析用户输入，理解需求，可能调用工具获取必要的上下文信息

        参数:
            input_text: 用户输入文本

        返回:
            观察结果（字符串）
        """
        # 渲染observe阶段的prompt
        observe_prompt = self._prompt_manager.render(
            "observe",
            input=input_text
        )

        # 调用ChatAgent执行观察
        user_msg = BaseMessage.make_user_message(
            role_name="User",
            content=observe_prompt
        )

        response = self._chat_agent.step(user_msg)
        observation = response.msg.content

        return observation

    def planning(self, observation: str) -> str:
        """
        第二阶段: 推理与规划

        基于observation进行Chain-of-Thought推理，制定执行计划

        参数:
            observation: 第一阶段的观察结果

        返回:
            执行计划（字符串）
        """
        # 渲染planning阶段的prompt
        planning_prompt = self._prompt_manager.render(
            "planning",
            observation=observation
        )

        # 调用ChatAgent执行规划
        user_msg = BaseMessage.make_user_message(
            role_name="User",
            content=planning_prompt
        )

        response = self._chat_agent.step(user_msg)
        plan = response.msg.content

        return plan

    def acting(self, plan: str) -> str:
        """
        第三阶段: 执行与反馈

        根据plan执行具体操作，调用工具，整合结果

        参数:
            plan: 第二阶段的执行计划

        返回:
            执行结果（字符串）
        """
        # 构造action prompt
        action_prompt = f"""根据以下计划执行任务：

{plan}

请根据计划调用必要的工具完成任务，并返回最终结果。"""

        # 调用ChatAgent执行动作（带重试机制）
        max_retries = self.config["execution_flow"]["action"].get("max_retries", 2)
        retry_delay = self.config["execution_flow"]["action"].get("retry_delay", 1.0)

        for attempt in range(max_retries + 1):
            try:
                user_msg = BaseMessage.make_user_message(
                    role_name="User",
                    content=action_prompt
                )

                response = self._chat_agent.step(user_msg)
                result = response.msg.content

                return result

            except Exception as e:
                self.logger.warning(
                    f"执行失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}"
                )

                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    # 超过重试次数，返回错误信息
                    return f"执行失败，已重试{max_retries}次: {str(e)}"

    def reset(self) -> None:
        """
        重置Agent状态

        清空对话历史，重置为初始状态
        """
        self._chat_agent.reset()
        self.logger.info("Agent状态已重置")

    def get_conversation_history(self) -> List[BaseMessage]:
        """
        获取对话历史

        返回:
            对话消息列表
        """
        return self._chat_agent.memory.get_context()

    # ==================== 异步执行方法 ====================

    async def async_run(self, user_input: str) -> str:
        """
        异步执行入口：三阶段循环

        参数:
            user_input: 用户输入文本

        返回:
            执行结果文本

        流程:
            user_input → async_observing() → async_planning() → async_acting() → result

        说明:
            适用于需要并行执行多个Agent的场景,使用asyncio调度
        """
        self.logger.info(f"开始异步执行任务: {user_input[:50]}...")

        try:
            # 第一阶段: 观察与理解
            observation = await self.async_observing(user_input)
            self.logger.debug(f"Observation: {observation[:100]}...")

            # 第二阶段: 推理与规划
            plan = await self.async_planning(observation)
            self.logger.debug(f"Plan: {plan[:100]}...")

            # 第三阶段: 执行与反馈
            result = await self.async_acting(plan)
            self.logger.info("异步任务执行完成")

            return result

        except Exception as e:
            self.logger.error(f"异步执行失败: {str(e)}", exc_info=True)
            return f"执行过程中发生错误: {str(e)}"

    async def async_observing(self, input_text: str) -> str:
        """
        第一阶段: 异步观察与理解

        分析用户输入,理解需求,可能调用工具获取必要的上下文信息

        参数:
            input_text: 用户输入文本

        返回:
            观察结果（字符串）

        实现:
            使用run_in_executor在线程池中执行同步的_chat_agent.step调用
        """
        # 渲染observe阶段的prompt
        observe_prompt = self._prompt_manager.render(
            "observe",
            input=input_text
        )

        # 调用ChatAgent执行观察
        user_msg = BaseMessage.make_user_message(
            role_name="User",
            content=observe_prompt
        )

        # 使用run_in_executor在线程池中执行同步的step调用
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,  # 使用默认线程池
            self._chat_agent.step,
            user_msg
        )
        observation = response.msg.content

        return observation

    async def async_planning(self, observation: str) -> str:
        """
        第二阶段: 异步推理与规划

        基于observation进行Chain-of-Thought推理,制定执行计划

        参数:
            observation: 第一阶段的观察结果

        返回:
            执行计划（字符串）

        实现:
            使用run_in_executor在线程池中执行同步的_chat_agent.step调用
        """
        # 渲染planning阶段的prompt
        planning_prompt = self._prompt_manager.render(
            "planning",
            observation=observation
        )

        # 调用ChatAgent执行规划
        user_msg = BaseMessage.make_user_message(
            role_name="User",
            content=planning_prompt
        )

        # 使用run_in_executor在线程池中执行同步的step调用
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            self._chat_agent.step,
            user_msg
        )
        plan = response.msg.content

        return plan

    async def async_acting(self, plan: str) -> str:
        """
        第三阶段: 异步执行与反馈

        根据plan执行具体操作,调用工具,整合结果

        参数:
            plan: 第二阶段的执行计划

        返回:
            执行结果（字符串）

        实现:
            使用run_in_executor在线程池中执行同步的_chat_agent.step调用,
            重试延迟使用asyncio.sleep实现异步等待
        """
        # 构造action prompt
        action_prompt = f"""根据以下计划执行任务：

{plan}

请根据计划调用必要的工具完成任务，并返回最终结果。"""

        # 调用ChatAgent执行动作（带重试机制）
        max_retries = self.config["execution_flow"]["action"].get("max_retries", 2)
        retry_delay = self.config["execution_flow"]["action"].get("retry_delay", 1.0)

        for attempt in range(max_retries + 1):
            try:
                user_msg = BaseMessage.make_user_message(
                    role_name="User",
                    content=action_prompt
                )

                # 使用run_in_executor在线程池中执行同步的step调用
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    self._chat_agent.step,
                    user_msg
                )
                result = response.msg.content

                return result

            except Exception as e:
                self.logger.warning(
                    f"异步执行失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}"
                )

                if attempt < max_retries:
                    await asyncio.sleep(retry_delay)  # 使用异步sleep
                else:
                    # 超过重试次数,返回错误信息
                    return f"执行失败，已重试{max_retries}次: {str(e)}"
