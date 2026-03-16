#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Market Analyst 的 MemoryManager。

说明：
    - 继承自 BaseAnalystMemoryManager
    - 使用 market_summary.j2 Prompt 模板生成 7 日窗口的结构化 summary
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Optional

from jinja2 import Template
from langchain_core.language_models import BaseChatModel

from .base_manager import BaseAnalystMemoryManager, SummaryContext


class MarketMemoryManager(BaseAnalystMemoryManager):
    """Market Analyst 的 MemoryManager。"""

    def __init__(self, db_helper: Any, date_resolver: Optional[Any] = None) -> None:
        super().__init__(analyst_type="market", db_helper=db_helper, date_resolver=date_resolver)

    def _generate_summary_with_llm(
        self,
        llm: BaseChatModel,
        context: SummaryContext,
    ) -> tuple[str, Optional[str], Optional[int]]:
        """使用 LLM 生成 7 日窗口的结构化 Market summary。

        Args:
            llm: LLM 实例
            context: 包含窗口内原始报告等信息的上下文

        Returns:
            一个三元组：
                - summary_content: 结构化 summary（JSON 字符串）
                - llm_model: 使用的模型名称（可选）
                - token_usage: Token 用量（可选）
        """
        # 1. 加载 Prompt 模板
        prompt_path = Path(__file__).parent / "prompts" / "market_summary.j2"
        if not prompt_path.exists():
            raise FileNotFoundError(f"Prompt 模板不存在: {prompt_path}")

        with open(prompt_path, "r", encoding="utf-8") as f:
            template_content = f.read()
        template = Template(template_content)

        # 2. 渲染 Prompt
        prompt = template.render(
            analyst_type=context.analyst_type,
            symbol=context.symbol,
            trade_date=context.trade_date,
            window_start_date=context.window_start_date,
            window_end_date=context.window_end_date,
            reports=context.reports,
        )

        # 3. 调用 LLM
        messages = [
            {
                "role": "user",
                "content": prompt,
            },
        ]
        response = llm.invoke(messages)
        content: str = getattr(response, "content", str(response))

        # 4. 提取 JSON（可能被 markdown code blocks 包裹）
        json_content = self._extract_json(content)

        # 5. 验证 JSON 格式
        try:
            parsed_json = json.loads(json_content)
            # 验证关键字段是否为空
            if not parsed_json.get("symbol") or not parsed_json.get("trend"):
                print(f"[WARN] JSON 关键字段为空，原始内容长度: {len(content)}")
        except json.JSONDecodeError as e:
            raise ValueError(f"LLM 返回的 JSON 格式无效: {e}\n原始内容: {content[:500]}")

        # 6. 获取模型名称和 token 用量
        llm_model = getattr(llm, "model_name", None) or getattr(llm, "model", None)
        token_usage = self._extract_token_usage(response)

        return json_content, llm_model, token_usage

    def _extract_json(self, content: str) -> str:
        """从 LLM 返回的内容中提取 JSON。

        处理情况：
            - 直接返回 JSON
            - 被 ```json ... ``` 包裹
            - 被 ``` ... ``` 包裹
            - 包含前置说明文字
        """
        # 清理内容：移除可能的 BOM 和前后空白
        content = content.strip()
        if content.startswith('\ufeff'):
            content = content[1:]
        
        # 方法1: 尝试提取 markdown code block 中的 JSON（支持 json 标签）
        json_pattern = r"```(?:json)?\s*(\{.*?\})\s*```"
        match = re.search(json_pattern, content, re.DOTALL)
        if match:
            extracted = match.group(1).strip()
            # 验证是否是有效的 JSON
            try:
                json.loads(extracted)
                return extracted
            except json.JSONDecodeError:
                pass
        
        # 方法2: 尝试提取 markdown code block（无标签）
        json_pattern = r"```\s*(\{.*?\})\s*```"
        match = re.search(json_pattern, content, re.DOTALL)
        if match:
            extracted = match.group(1).strip()
            try:
                json.loads(extracted)
                return extracted
            except json.JSONDecodeError:
                pass
        
        # 方法3: 查找第一个 { 到最后一个 } 之间的内容（更精确的匹配）
        # 使用平衡括号匹配
        start_idx = content.find('{')
        if start_idx != -1:
            # 从第一个 { 开始，找到匹配的 }
            brace_count = 0
            for i in range(start_idx, len(content)):
                if content[i] == '{':
                    brace_count += 1
                elif content[i] == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        extracted = content[start_idx:i+1].strip()
                        try:
                            json.loads(extracted)
                            return extracted
                        except json.JSONDecodeError:
                            break
        
        # 方法4: 尝试直接提取 JSON 对象（贪婪匹配，但可能匹配到多个）
        json_pattern = r"(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})"
        matches = re.findall(json_pattern, content, re.DOTALL)
        for match in matches:
            try:
                json.loads(match)
                return match.strip()
            except json.JSONDecodeError:
                continue
        
        # 如果都找不到，返回原始内容（让 JSON 解析器报错）
        return content.strip()

    def _extract_token_usage(self, response: Any) -> Optional[int]:
        """从 LLM 响应中提取 token 用量。"""
        # 尝试从 response 对象中获取 token 用量
        if hasattr(response, "response_metadata"):
            metadata = response.response_metadata
            if isinstance(metadata, dict):
                usage = metadata.get("token_usage", {})
                if isinstance(usage, dict):
                    total = usage.get("total_tokens")
                    if total is not None:
                        return int(total)
        return None


