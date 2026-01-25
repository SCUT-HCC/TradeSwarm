"""测试新闻工具"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tradingagents.tool_nodes.utils.news_tools import get_news, get_global_news


def test_get_news():
    """测试 get_news 工具"""
    print("=" * 60)
    print("测试 get_news 工具")
    print("=" * 60)
    
    try:
        result = get_news.invoke({
            "ts_code": "000001",
            "days": 7,
            "limit": 10
        })
        
        print("\n工具返回结果（前500字符）:")
        print(result[:500])
        print("...")
        
        import json
        data = json.loads(result)
        if data.get('success'):
            print(f"\n✅ 成功获取 {data.get('summary', {}).get('total_records', 0)} 条新闻")
            print(f"   数据源: {data.get('summary', {}).get('data_source', 'unknown')}")
        else:
            print(f"\n❌ 失败: {data.get('message', '未知错误')}")
            
    except Exception as e:
        print(f"❌ 测试异常: {str(e)}")
        import traceback
        traceback.print_exc()


def test_get_global_news():
    """测试 get_global_news 工具"""
    print("\n" + "=" * 60)
    print("测试 get_global_news 工具")
    print("=" * 60)
    
    try:
        result = get_global_news.invoke({
            "days": 7,
            "limit": 10
        })
        
        print("\n工具返回结果（前500字符）:")
        print(result[:500])
        print("...")
        
        import json
        data = json.loads(result)
        if data.get('success'):
            print(f"\n✅ 成功")
            print(f"   消息: {data.get('message', '')[:100]}...")
        else:
            print(f"\n❌ 失败: {data.get('message', '未知错误')}")
            
    except Exception as e:
        print(f"❌ 测试异常: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_get_news()
    test_get_global_news()

