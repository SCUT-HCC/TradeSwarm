"""
配置验证器模块

提供三层配置验证机制:
1. Schema验证: JSON Schema格式检查
2. 逻辑验证: prompt模板占位符、工具名称、模型类型验证
3. 资源验证: 长期记忆路径、API密钥验证
"""

from typing import Dict, Any
import os
import re


class ValidationError(Exception):
    """配置验证失败异常"""
    pass


class ConfigValidator:
    """
    配置验证器

    负责验证Agent配置的完整性和有效性，在BaseAgent初始化时调用。
    验证失败时立即抛出异常，防止运行时错误。
    """

    def __init__(self):
        """初始化验证器"""
        self._required_sections = [
            "agent_profile",
            "model_config",
            "execution_flow",
            "tool_config",
            "memory_config",
            "error_handling"
        ]

        self._supported_models = [
            # OpenAI models
            "gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-4",
            # Claude models
            "claude-3-opus", "claude-3-sonnet", "claude-3-haiku",
            "claude-3-5-sonnet",
            # Alibaba Qwen models (via DashScope API)
            "qwen-max", "qwen-turbo", "qwen-plus", "qwen-long"
        ]

    def validate(self, config: Dict[str, Any]) -> None:
        """
        验证配置有效性

        参数:
            config: 配置字典

        异常:
            ValidationError: 验证失败时抛出
        """
        self._validate_schema(config)
        self._validate_logic(config)
        self._validate_resources(config)

    def _validate_schema(self, config: Dict[str, Any]) -> None:
        """
        第一层: JSON Schema验证

        验证配置结构的完整性和必填字段
        """
        # 验证顶层必需节点
        for section in self._required_sections:
            if section not in config:
                raise ValidationError(f"缺少必需配置节: {section}")

        # 验证agent_profile
        profile = config["agent_profile"]
        if "name" not in profile:
            raise ValidationError("agent_profile缺少必需字段: name")
        if "role" not in profile:
            raise ValidationError("agent_profile缺少必需字段: role")

        # 验证model_config
        model_cfg = config["model_config"]
        if "model_type" not in model_cfg:
            raise ValidationError("model_config缺少必需字段: model_type")
        if "model_config_dict" not in model_cfg:
            raise ValidationError("model_config缺少必需字段: model_config_dict")

        # 验证execution_flow三阶段配置
        exec_flow = config["execution_flow"]
        for stage in ["observe", "planning", "action"]:
            if stage not in exec_flow:
                raise ValidationError(f"execution_flow缺少必需阶段: {stage}")

        # 验证observe阶段配置
        if "prompt_template" not in exec_flow["observe"]:
            raise ValidationError("execution_flow.observe缺少必需字段: prompt_template")

        # 验证planning阶段配置
        if "prompt_template" not in exec_flow["planning"]:
            raise ValidationError("execution_flow.planning缺少必需字段: prompt_template")

        # 验证tool_config是数组
        if not isinstance(config["tool_config"], list):
            raise ValidationError("tool_config必须是数组")

        # 验证memory_config结构
        mem_cfg = config["memory_config"]
        if "short_term" not in mem_cfg:
            raise ValidationError("memory_config缺少必需字段: short_term")
        if "long_term" not in mem_cfg:
            raise ValidationError("memory_config缺少必需字段: long_term")

    def _validate_logic(self, config: Dict[str, Any]) -> None:
        """
        第二层: 逻辑验证

        验证配置的业务规则和逻辑约束
        """
        # 验证模型类型
        model_type = config["model_config"]["model_type"]
        if model_type not in self._supported_models:
            raise ValidationError(
                f"不支持的模型类型: {model_type}. "
                f"支持的模型: {', '.join(self._supported_models)}"
            )

        # 验证prompt模板占位符
        exec_flow = config["execution_flow"]

        # observe阶段必须包含{input}占位符
        observe_template = exec_flow["observe"]["prompt_template"]
        if "{input}" not in observe_template:
            raise ValidationError(
                "execution_flow.observe.prompt_template必须包含{input}占位符"
            )

        # planning阶段必须包含{observation}占位符
        planning_template = exec_flow["planning"]["prompt_template"]
        if "{observation}" not in planning_template:
            raise ValidationError(
                "execution_flow.planning.prompt_template必须包含{observation}占位符"
            )

        # 验证工具配置
        for tool in config["tool_config"]:
            if "category" not in tool:
                raise ValidationError("工具配置缺少必需字段: category")
            if "name" not in tool:
                raise ValidationError("工具配置缺少必需字段: name")
            if "enabled" not in tool:
                raise ValidationError("工具配置缺少必需字段: enabled")

            # 验证category有效性
            valid_categories = ["file_operation", "information_retrieval"]
            if tool["category"] not in valid_categories:
                raise ValidationError(
                    f"无效的工具类别: {tool['category']}. "
                    f"有效类别: {', '.join(valid_categories)}"
                )

        # 验证记忆配置逻辑
        mem_cfg = config["memory_config"]
        if mem_cfg["short_term"]["enabled"]:
            max_msgs = mem_cfg["short_term"].get("max_messages", 10)
            if not isinstance(max_msgs, int) or max_msgs < 1:
                raise ValidationError("short_term.max_messages必须是正整数")

        # 验证错误处理配置
        err_cfg = config["error_handling"]
        if "llm_failure_retry" in err_cfg:
            retry = err_cfg["llm_failure_retry"]
            if not isinstance(retry, int) or retry < 0:
                raise ValidationError("error_handling.llm_failure_retry必须是非负整数")

        if "retry_delay" in err_cfg:
            delay = err_cfg["retry_delay"]
            if not isinstance(delay, (int, float)) or delay < 0:
                raise ValidationError("error_handling.retry_delay必须是非负数")

    def _validate_resources(self, config: Dict[str, Any]) -> None:
        """
        第三层: 资源验证

        验证外部资源的可用性
        """
        # 验证API密钥
        model_type = config["model_config"]["model_type"]

        if model_type.startswith("gpt"):
            if not os.getenv("OPENAI_API_KEY"):
                raise ValidationError(
                    f"模型 {model_type} 需要设置环境变量: OPENAI_API_KEY"
                )
        elif model_type.startswith("claude"):
            if not os.getenv("ANTHROPIC_API_KEY"):
                raise ValidationError(
                    f"模型 {model_type} 需要设置环境变量: ANTHROPIC_API_KEY"
                )
        elif model_type.startswith("qwen"):
            if not os.getenv("ALIBABA_API_KEY"):
                raise ValidationError(
                    f"模型 {model_type} 需要设置环境变量: ALIBABA_API_KEY"
                )

        # 验证长期记忆路径（如果启用）
        mem_cfg = config["memory_config"]
        if mem_cfg["long_term"]["enabled"]:
            if "storage_path" in mem_cfg["long_term"]:
                path = mem_cfg["long_term"]["storage_path"]
                # 检查父目录是否存在
                parent_dir = os.path.dirname(path)
                if parent_dir and not os.path.exists(parent_dir):
                    raise ValidationError(
                        f"长期记忆存储路径的父目录不存在: {parent_dir}"
                    )
