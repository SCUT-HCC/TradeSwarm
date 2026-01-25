#!/usr/bin/env python3
"""
详细测试新闻工具：get_news 和 get_global_news

提供更详细的测试输出和调试信息，便于排查问题。
"""
import os
import sys
import json
from datetime import datetime, timedelta

# 添加项目根目录到路径
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# 导入工具
from tradingagents.tool_nodes.utils.news_tools import get_news, get_global_news

# 测试用的股票代码
TEST_SYMBOL = "600519"  # 贵州茅台
TEST_TS_CODE = "600519.SH"


def print_section(title: str):
    """打印章节标题"""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def test_get_news_detailed():
    """详细测试 get_news"""
    print_section("详细测试 get_news")
    
    print(f"\n测试参数:")
    print(f"  股票代码: {TEST_SYMBOL}")
    print(f"  天数: 7")
    print(f"  限制: 5")
    
    try:
        print("\n[步骤1] 调用 get_news.invoke()...")
        result_str = get_news.invoke({
            "ts_code": TEST_SYMBOL,
            "days": 7,
            "limit": 5
        })
        
        print(f"   返回类型: {type(result_str)}")
        print(f"   返回长度: {len(result_str) if isinstance(result_str, str) else 'N/A'}")
        
        print("\n[步骤2] 解析 JSON...")
        result = json.loads(result_str)
        
        print(f"   解析成功")
        print(f"   JSON 键: {list(result.keys())}")
        
        print("\n[步骤3] 分析返回结果...")
        print(f"   success: {result.get('success')}")
        print(f"   message: {result.get('message', '')[:200]}")
        print(f"   format: {result.get('format', 'N/A')}")
        
        if result.get("success"):
            if result.get("format") == "markdown":
                content = result.get("content", "")
                print(f"\n   Markdown 内容:")
                print(f"   长度: {len(content)} 字符")
                print(f"   前500字符:")
                print(f"   {'-' * 60}")
                print(f"   {content[:500]}")
                print(f"   {'-' * 60}")
                
                # 检查 Markdown 结构
                if "# 个股新闻简报" in content:
                    print(f"   ✓ 包含标题: 个股新闻简报")
                if "## 数据概览" in content:
                    print(f"   ✓ 包含数据概览")
                if "## 新闻列表" in content:
                    print(f"   ✓ 包含新闻列表")
            else:
                data = result.get("data", [])
                print(f"\n   JSON 数据:")
                print(f"   数据条数: {len(data)}")
                if data:
                    print(f"   第一条数据键: {list(data[0].keys())[:5]}")
                    print(f"   第一条数据示例:")
                    print(f"   {json.dumps(data[0], ensure_ascii=False, indent=2)[:300]}")
            
            summary = result.get("summary", {})
            if summary:
                print(f"\n   摘要信息:")
                print(f"   {json.dumps(summary, ensure_ascii=False, indent=2)}")
        else:
            print(f"\n   ❌ 获取失败")
            print(f"   错误信息: {result.get('message', '')}")
        
        print("\n[步骤4] 完整返回结果（JSON格式）:")
        print(json.dumps(result, ensure_ascii=False, indent=2)[:1000])
        if len(json.dumps(result, ensure_ascii=False, indent=2)) > 1000:
            print("   ... (已截断)")
        
        print("\n✅ get_news 测试完成")
        
    except json.JSONDecodeError as e:
        print(f"\n❌ JSON 解析失败: {e}")
        print(f"   原始返回内容:")
        print(f"   {result_str[:500]}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


def test_get_global_news_detailed():
    """详细测试 get_global_news"""
    print_section("详细测试 get_global_news")
    
    print(f"\n测试参数:")
    print(f"  天数: 7")
    print(f"  限制: 5")
    
    try:
        print("\n[步骤1] 调用 get_global_news.invoke()...")
        result_str = get_global_news.invoke({
            "days": 7,
            "limit": 5
        })
        
        print(f"   返回类型: {type(result_str)}")
        print(f"   返回长度: {len(result_str) if isinstance(result_str, str) else 'N/A'}")
        
        print("\n[步骤2] 解析 JSON...")
        result = json.loads(result_str)
        
        print(f"   解析成功")
        print(f"   JSON 键: {list(result.keys())}")
        
        print("\n[步骤3] 分析返回结果...")
        print(f"   success: {result.get('success')}")
        print(f"   message: {result.get('message', '')[:200]}")
        print(f"   format: {result.get('format', 'N/A')}")
        
        if result.get("success"):
            if result.get("format") == "markdown":
                content = result.get("content", "")
                print(f"\n   Markdown 内容:")
                print(f"   长度: {len(content)} 字符")
                print(f"   前500字符:")
                print(f"   {'-' * 60}")
                print(f"   {content[:500]}")
                print(f"   {'-' * 60}")
                
                # 检查 Markdown 结构
                sections = []
                if "# 宏观市场全景简报" in content:
                    sections.append("✓ 包含标题: 宏观市场全景简报")
                if "## 📰 宏观新闻" in content:
                    sections.append("✓ 包含宏观新闻")
                if "## 💰 北向资金流向" in content:
                    sections.append("✓ 包含北向资金")
                if "## 📊 核心指数表现" in content:
                    sections.append("✓ 包含核心指数")
                if "## 💱 汇率信息" in content:
                    sections.append("✓ 包含汇率信息")
                
                if sections:
                    print(f"\n   Markdown 结构检查:")
                    for section in sections:
                        print(f"   {section}")
            else:
                data = result.get("data", [])
                print(f"\n   JSON 数据:")
                print(f"   数据条数: {len(data)}")
                if data:
                    print(f"   第一条数据键: {list(data[0].keys())[:5]}")
            
            summary = result.get("summary", {})
            if summary:
                print(f"\n   摘要信息:")
                print(f"   {json.dumps(summary, ensure_ascii=False, indent=2)}")
        else:
            print(f"\n   ❌ 获取失败")
            print(f"   错误信息: {result.get('message', '')}")
        
        print("\n[步骤4] 完整返回结果（JSON格式）:")
        print(json.dumps(result, ensure_ascii=False, indent=2)[:1000])
        if len(json.dumps(result, ensure_ascii=False, indent=2)) > 1000:
            print("   ... (已截断)")
        
        print("\n✅ get_global_news 测试完成")
        
    except json.JSONDecodeError as e:
        print(f"\n❌ JSON 解析失败: {e}")
        print(f"   原始返回内容:")
        print(f"   {result_str[:500]}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


def main():
    """主函数"""
    print("=" * 80)
    print(" 新闻工具详细测试")
    print("=" * 80)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试股票: {TEST_SYMBOL}")
    
    # 测试 get_news
    test_get_news_detailed()
    
    # 测试 get_global_news
    test_get_global_news_detailed()
    
    print("\n" + "=" * 80)
    print(" 测试完成")
    print("=" * 80)


if __name__ == "__main__":
    main()

