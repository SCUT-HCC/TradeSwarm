#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
JSON 输出解析工具

用于从 LLM 输出中提取和解析 JSON 格式的结构化数据。
"""
from __future__ import annotations

import json
import re
from typing import Dict, Any, Optional, Tuple


def extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    """
    从文本中提取 JSON 对象。
    
    支持以下格式：
    1. 纯 JSON 对象（以 { 开头，以 } 结尾）
    2. Markdown 代码块中的 JSON（```json ... ``` 或 ``` ... ```）
    3. 文本中包含的 JSON 对象
    
    Args:
        text: 包含 JSON 的文本
        
    Returns:
        解析后的 JSON 字典，如果解析失败则返回 None
        
    Examples:
        >>> text = '```json\n{"key": "value"}\n```'
        >>> extract_json_from_text(text)
        {'key': 'value'}
        
        >>> text = 'Some text {"key": "value"} more text'
        >>> extract_json_from_text(text)
        {'key': 'value'}
    """
    if not text or not isinstance(text, str):
        return None
    
    # 方法1: 尝试提取 Markdown 代码块中的 JSON
    json_block_pattern = r'```(?:json)?\s*(\{.*?\})\s*```'
    match = re.search(json_block_pattern, text, re.DOTALL)
    if match:
        json_str = match.group(1)
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            pass
    
    # 方法2: 查找第一个完整的 JSON 对象（从 { 到匹配的 }）
    brace_count = 0
    start_idx = -1
    
    for i, char in enumerate(text):
        if char == '{':
            if start_idx == -1:
                start_idx = i
            brace_count += 1
        elif char == '}':
            brace_count -= 1
            if brace_count == 0 and start_idx != -1:
                json_str = text[start_idx:i+1]
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    # 继续尝试下一个 JSON 对象
                    start_idx = -1
                    brace_count = 0
    
    # 方法3: 尝试直接解析整个文本（如果是纯 JSON）
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        pass
    
    return None


def parse_analyst_output(
    content: str,
    analyst_type: str
) -> Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    解析 Analyst 的输出，提取报告内容、结构化数据和元数据。
    
    Args:
        content: LLM 输出的原始内容
        analyst_type: Analyst 类型（"market", "news", "fundamentals", "sentiment"）
        
    Returns:
        Tuple[report_content, structured_data, metadata]:
            - report_content: 原始报告内容（Markdown 格式）
            - structured_data: 解析出的结构化数据（JSON 字典）
            - metadata: 元数据（JSON 字典）
            
    Examples:
        >>> content = '{"role": "Market Analyst", "detailed_report": "..."}'
        >>> report, structured, metadata = parse_analyst_output(content, "market")
        >>> structured["role"]
        'Market Analyst'
    """
    # 尝试提取 JSON
    json_data = extract_json_from_text(content)
    
    if json_data is None:
        # 如果没有 JSON，返回原始内容作为报告
        return content, None, None
    
    # 提取详细报告（如果存在）
    report_content = json_data.get("detailed_report", "")
    if not report_content:
        # 如果没有 detailed_report，使用整个 JSON 的字符串表示作为报告
        report_content = json.dumps(json_data, ensure_ascii=False, indent=2)
    
    # 提取结构化数据（排除 detailed_report 和 metadata）
    structured_data = {k: v for k, v in json_data.items() 
                      if k not in ["detailed_report", "metadata"]}
    
    # 提取元数据
    metadata = json_data.get("metadata")
    
    # 如果 metadata 是字符串，尝试解析
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            pass
    
    return report_content, structured_data, metadata


def validate_analyst_json(json_data: Dict[str, Any], analyst_type: str) -> Tuple[bool, Optional[str]]:
    """
    验证 Analyst JSON 输出的必需字段。
    
    根据新的 prompt 设计，每个 analyst 都有特定的必需字段：
    - market: role, symbol, analysis_date, summary, detailed_report
    - news: role, symbol, analysis_date, summary, detailed_report
    - fundamentals: role, symbol, analysis_date, summary, detailed_report
    - sentiment: role, symbol, analysis_date, summary, detailed_report
    
    Args:
        json_data: 解析后的 JSON 数据
        analyst_type: Analyst 类型（"market", "news", "fundamentals", "sentiment"）
        
    Returns:
        Tuple[is_valid, error_message]:
            - is_valid: 是否有效
            - error_message: 错误消息（如果无效）
    """
    # 所有 analyst 的共同必需字段
    common_required_fields = ["role", "symbol", "analysis_date", "summary", "detailed_report"]
    
    # 特定 analyst 的额外必需字段
    type_specific_fields = {
        "market": ["indicators_analyzed", "key_findings", "market_assessment"],
        "news": ["macro_news_summary", "company_news_summary", "key_events"],
        "fundamentals": ["profitability", "growth", "financial_health", "valuation"],
        "sentiment": ["sentiment_overview", "key_themes"],
    }
    
    # 检查共同必需字段
    missing_common = [field for field in common_required_fields if field not in json_data]
    if missing_common:
        return False, f"缺少共同必需字段: {', '.join(missing_common)}"
    
    # 检查特定类型的必需字段（如果定义了）
    specific_fields = type_specific_fields.get(analyst_type, [])
    missing_specific = [field for field in specific_fields if field not in json_data]
    if missing_specific:
        return False, f"缺少 {analyst_type} 特定必需字段: {', '.join(missing_specific)}"
    
    return True, None

