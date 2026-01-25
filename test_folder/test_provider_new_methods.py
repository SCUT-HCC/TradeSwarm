"""测试 Provider 新添加的方法"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tradingagents.data_sources.tushare_provider import TushareProvider
from tradingagents.data_sources.akshare_provider import AkshareProvider
import json


def test_tushare_realtime_orderbook():
    """测试 TushareProvider.get_realtime_orderbook()"""
    print("=" * 80)
    print("测试 TushareProvider.get_realtime_orderbook()")
    print("=" * 80)
    
    try:
        provider = TushareProvider()
        
        # 测试 dict 格式
        print("\n1. 测试 dict 格式返回:")
        result_dict = provider.get_realtime_orderbook("000001", return_format="dict")
        print(f"   返回类型: {type(result_dict)}")
        print(f"   股票名称: {result_dict.get('name', 'N/A')}")
        print(f"   当前价格: {result_dict.get('price', 'N/A')}")
        print(f"   数据来源: {result_dict.get('data_source', 'N/A')}")
        if 'warning' in result_dict:
            print(f"   警告: {result_dict.get('warning', '')}")
        
        # 测试 markdown 格式
        print("\n2. 测试 markdown 格式返回:")
        result_md = provider.get_realtime_orderbook("000001", return_format="markdown")
        print(f"   返回类型: {type(result_md)}")
        print(f"   内容预览（前200字符）:")
        print(f"   {result_md[:200]}...")
        
        print("\n✅ TushareProvider.get_realtime_orderbook() 测试通过")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()


def test_akshare_smart_money_flow():
    """测试 AkshareProvider.get_smart_money_flow()"""
    print("\n" + "=" * 80)
    print("测试 AkshareProvider.get_smart_money_flow()")
    print("=" * 80)
    
    try:
        provider = AkshareProvider()
        result = provider.get_smart_money_flow()
        
        print(f"\n返回结果:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        if 'error' not in result:
            print(f"\n✅ 成功获取北向资金数据")
            print(f"   {result.get('title', '')}: {result.get('value', '')}")
        else:
            print(f"\n⚠️ 获取失败: {result.get('error', '')}")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()


def test_akshare_global_indices():
    """测试 AkshareProvider.get_global_indices_summary()"""
    print("\n" + "=" * 80)
    print("测试 AkshareProvider.get_global_indices_summary()")
    print("=" * 80)
    
    try:
        provider = AkshareProvider()
        result = provider.get_global_indices_summary()
        
        print(f"\n返回结果（共 {len(result)} 条）:")
        for item in result:
            print(f"   {item.get('asset', 'N/A')}: {item.get('price', 'N/A')} ({item.get('change', 'N/A')})")
        
        if result:
            print(f"\n✅ 成功获取外围指数数据")
        else:
            print(f"\n⚠️ 未获取到数据")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()


def test_akshare_currency_rate():
    """测试 AkshareProvider.get_currency_rate()"""
    print("\n" + "=" * 80)
    print("测试 AkshareProvider.get_currency_rate()")
    print("=" * 80)
    
    try:
        provider = AkshareProvider()
        result = provider.get_currency_rate()
        
        print(f"\n返回结果:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        if result.get('price') is not None:
            print(f"\n✅ 成功获取汇率数据")
            print(f"   {result.get('description', '')}")
        else:
            print(f"\n⚠️ 未获取到汇率数据")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()


def test_akshare_global_news():
    """测试 AkshareProvider.get_global_news()"""
    print("\n" + "=" * 80)
    print("测试 AkshareProvider.get_global_news()")
    print("=" * 80)
    
    try:
        provider = AkshareProvider()
        df = provider.get_global_news(limit=5)
        
        if df is not None and not df.empty:
            print(f"\n返回结果（共 {len(df)} 条）:")
            print(f"列名: {df.columns.tolist()}")
            
            # 尝试提取标题和日期（支持不同的列名）
            for i, (_, row) in enumerate(df.head(5).iterrows(), 1):
                # 尝试不同的列名
                title = None
                for col in ['title', '新闻标题', '标题', 'event']:
                    if col in row:
                        title = str(row[col])
                        break
                
                date = None
                for col in ['date', '发布时间', '时间', '日期']:
                    if col in row:
                        date = str(row[col])
                        break
                
                print(f"\n   [{i}] {title or 'N/A'}")
                if date:
                    print(f"       日期: {date}")
            
            print(f"\n✅ 成功获取宏观经济新闻")
        else:
            print(f"\n⚠️ 未获取到新闻")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # 测试 Tushare Provider
    test_tushare_realtime_orderbook()
    
    # 测试 AkShare Provider
    test_akshare_smart_money_flow()
    test_akshare_global_indices()
    test_akshare_currency_rate()
    test_akshare_global_news()
    
    print("\n" + "=" * 80)
    print("所有测试完成")
    print("=" * 80)

