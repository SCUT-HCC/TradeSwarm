"""
提示词管理器模块

管理各执行阶段的提示词模板渲染和变量替换
"""

from typing import Dict, Any


class PromptManager:
    """
    提示词模板管理器

    负责管理observe和planning阶段的prompt模板，
    提供模板变量渲染功能
    """

    def __init__(self, execution_flow_config: Dict[str, Any]):
        """
        初始化提示词管理器

        参数:
            execution_flow_config: execution_flow配置字典
        """
        self._templates: Dict[str, str] = {}

        # 提取observe阶段的prompt模板
        if "observe" in execution_flow_config:
            observe_cfg = execution_flow_config["observe"]
            self._templates["observe"] = observe_cfg.get("prompt_template", "")

        # 提取planning阶段的prompt模板
        if "planning" in execution_flow_config:
            planning_cfg = execution_flow_config["planning"]
            self._templates["planning"] = planning_cfg.get("prompt_template", "")

        # 存储完整配置用于获取其他参数
        self._config = execution_flow_config

    def render(self, stage: str, **kwargs) -> str:
        """
        渲染指定阶段的prompt

        参数:
            stage: 阶段名称 ("observe" | "planning")
            **kwargs: 模板变量，例如 input="...", observation="..."

        返回:
            渲染后的prompt文本

        异常:
            ValueError: 如果stage不存在或缺少必需的模板变量
        """
        if stage not in self._templates:
            raise ValueError(f"未知的执行阶段: {stage}")

        template = self._templates[stage]

        # 渲染模板变量
        try:
            rendered = template.format(**kwargs)
        except KeyError as e:
            raise ValueError(
                f"阶段 {stage} 的prompt模板缺少必需变量: {e}"
            )

        return rendered

    def get_stage_config(self, stage: str) -> Dict[str, Any]:
        """
        获取指定阶段的完整配置

        参数:
            stage: 阶段名称

        返回:
            阶段配置字典
        """
        if stage not in self._config:
            raise ValueError(f"未知的执行阶段: {stage}")

        return self._config[stage]

    def should_include_memory(self, stage: str = "observe") -> bool:
        """
        判断指定阶段是否需要包含记忆

        参数:
            stage: 阶段名称，默认"observe"

        返回:
            是否包含记忆
        """
        stage_cfg = self.get_stage_config(stage)
        return stage_cfg.get("include_memory", False)

    def get_memory_window(self, stage: str = "observe") -> int:
        """
        获取指定阶段的记忆窗口大小

        参数:
            stage: 阶段名称，默认"observe"

        返回:
            记忆窗口大小（历史消息数量）
        """
        stage_cfg = self.get_stage_config(stage)
        return stage_cfg.get("memory_window", 5)

    def get_output_format(self, stage: str = "planning") -> str:
        """
        获取指定阶段的输出格式

        参数:
            stage: 阶段名称，默认"planning"

        返回:
            输出格式 ("text" | "json")
        """
        stage_cfg = self.get_stage_config(stage)
        return stage_cfg.get("output_format", "text")
