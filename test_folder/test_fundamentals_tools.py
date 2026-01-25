"""测试基本面分析工具"""
import json
import sys
from tradingagents.data_sources.akshare_provider import AkshareProvider
from tradingagents.data_sources.tushare_provider import TushareProvider

# 设置输出编码（Windows 控制台）
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass


def test_provider_method(method_name, provider, method_func, *args, **kwargs):
    """测试 Provider 方法"""
    print("=" * 80)
    print(f"【测试】{method_name}")
    print("=" * 80)
    print(f"Provider: {provider.__class__.__name__}")
    print(f"参数: args={args}, kwargs={kwargs}")
    print("-" * 80)
    
    try:
        result = method_func(*args, **kwargs)
        
        if isinstance(result, dict):
            if "error" in result:
                print(f"[失败] {result['error']}")
            else:
                print(f"[成功] 测试通过")
                # 显示关键字段
                for key in ["symbol", "name", "report_type", "income", "balance", "cashflow", 
                           "pe_pb", "dividend", "forecast", "express", "data"]:
                    if key in result:
                        value = result[key]
                        if value is None:
                            print(f"  {key}: None")
                        elif isinstance(value, list):
                            print(f"  {key}: 列表，共 {len(value)} 条记录")
                            if len(value) > 0:
                                print(f"    示例: {str(value[0])[:150]}...")
                        elif isinstance(value, dict):
                            print(f"  {key}: 字典，包含 {len(value)} 个字段")
                            # 显示字典的前几个键
                            keys = list(value.keys())[:5]
                            for k in keys:
                                print(f"    {k}: {str(value[k])[:50]}...")
                        else:
                            print(f"  {key}: {str(value)[:100]}")
                
                # 显示 errors（如果有）
                if "errors" in result and result["errors"]:
                    print(f"  警告: {len(result['errors'])} 个错误")
                    for err in result["errors"][:3]:
                        print(f"    - {err[:100]}")
        else:
            print(f"[成功] 返回类型: {type(result).__name__}")
            if hasattr(result, '__len__'):
                print(f"  长度: {len(result)}")
        
        print(f"\n完整结果（前800字符）:")
        result_str = json.dumps(result, ensure_ascii=False, indent=2, default=str)
        print(result_str[:800])
        if len(result_str) > 800:
            print("...")
        
    except Exception as e:
        print(f"[异常] {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("\n")


def main():
    """主测试函数"""
    print("\n" + "=" * 80)
    print("基本面分析工具测试")
    print("=" * 80 + "\n")
    
    # 测试股票代码
    test_code = "000001"  # 平安银行
    
    # 初始化 Provider
    print("初始化 Provider...")
    try:
        ak_provider = AkshareProvider()
        print("  AkshareProvider: 成功")
    except Exception as e:
        print(f"  AkshareProvider: 失败 - {str(e)}")
        ak_provider = None
    
    try:
        ts_provider = TushareProvider()
        print("  TushareProvider: 成功")
    except Exception as e:
        print(f"  TushareProvider: 失败 - {str(e)}")
        ts_provider = None
    
    print("\n")
    
    # 1. 测试 get_company_info (AkShare)
    if ak_provider:
        test_provider_method("get_company_info (AkShare)", 
                            ak_provider, 
                            ak_provider.get_company_info, 
                            test_code)
    
    # 2. 测试 get_earnings_data (AkShare) — Ak 可用的保留接口
    if ak_provider:
        test_provider_method("get_earnings_data (AkShare)", 
                            ak_provider, 
                            ak_provider.get_earnings_data, 
                            test_code, 
                            limit=5)
    
    # 7. 测试 Tushare fallback 方法（含三大报表、PE/PB、业绩）
    if ts_provider:
        print("=" * 80)
        print("测试 Tushare Fallback 方法（含三大报表、PE/PB、业绩）")
        print("=" * 80 + "\n")
        
        test_provider_method("get_company_info (Tushare)", 
                            ts_provider, 
                            ts_provider.get_company_info, 
                            test_code)
        # 三大报表
        test_provider_method("get_income (Tushare)", 
                            ts_provider, 
                            ts_provider.get_income, 
                            test_code)
        test_provider_method("get_balancesheet (Tushare)", 
                            ts_provider, 
                            ts_provider.get_balancesheet, 
                            test_code)
        test_provider_method("get_cashflow (Tushare)", 
                            ts_provider, 
                            ts_provider.get_cashflow, 
                            test_code)
        # 财务指标
        test_provider_method("get_fina_indicator (Tushare)", 
                            ts_provider, 
                            ts_provider.get_fina_indicator, 
                            test_code)
        # 日度估值/PE/PB
        test_provider_method("get_daily_basic (Tushare)", 
                            ts_provider, 
                            ts_provider.get_daily_basic, 
                            test_code)
        # 业绩预告/快报
        test_provider_method("get_forecast (Tushare)", 
                            ts_provider, 
                            ts_provider.get_forecast, 
                            test_code, 
                            limit=5)
        test_provider_method("get_express (Tushare)", 
                            ts_provider, 
                            ts_provider.get_express, 
                            test_code, 
                            limit=5)
    
    print("=" * 80)
    print("测试完成")
    print("=" * 80)


if __name__ == "__main__":
    main()

