import tushare as ts
import time
import re


def extract_stock_code_number(stock_code: str) -> str:
    """提取股票代码的纯数字部分"""
    stock_code = stock_code.strip()
    numbers = re.sub(r"\D", "", stock_code)
    if len(numbers) == 6 and numbers.isdigit():
        return numbers
    if len(numbers) >= 6:
        return numbers[:6]
    return stock_code


def get_realtime_orderbook(ts_code: str):
    """
    获取实时五档盘口数据（使用爬虫接口）
    """
    clean_symbol = extract_stock_code_number(ts_code)
    
    df = ts.get_realtime_quotes(clean_symbol)
    
    if df is None or df.empty:
        raise Exception(f"找不到股票 {clean_symbol} 的实时行情")
    
    row = df.iloc[0]
    name = str(row['name'])
    price = float(row['price'])
    pre_close = float(row['pre_close'])
    change_pct = (price - pre_close) / pre_close * 100 if pre_close > 0 else 0
    
    # 提取五档盘口数据
    ask_prices = []
    ask_volumes = []
    bid_prices = []
    bid_volumes = []
    
    # 卖盘 (卖5 -> 卖1)
    for i in range(5, 0, -1):
        try:
            ask_prices.append(float(row[f'a{i}_p']))
            ask_volumes.append(int(row[f'a{i}_v']))
        except (KeyError, ValueError):
            ask_prices.append(0.0)
            ask_volumes.append(0)
    
    # 买盘 (买1 -> 买5)
    for i in range(1, 6):
        try:
            bid_prices.append(float(row[f'b{i}_p']))
            bid_volumes.append(int(row[f'b{i}_v']))
        except (KeyError, ValueError):
            bid_prices.append(0.0)
            bid_volumes.append(0)
    
    result = {
        "name": name,
        "code": clean_symbol,
        "price": price,
        "pre_close": pre_close,
        "change_pct": change_pct,
        "ask_prices": ask_prices,
        "ask_volumes": ask_volumes,
        "bid_prices": bid_prices,
        "bid_volumes": bid_volumes,
        "data_source": "legacy_crawler"
    }
    return result


def test_latency(func, *args, **kwargs):
    """测试函数执行延迟"""
    start_time = time.time()
    try:
        result = func(*args, **kwargs)
        end_time = time.time()
        latency = (end_time - start_time) * 1000  # 转换为毫秒
        return latency, result, None
    except Exception as e:
        end_time = time.time()
        latency = (end_time - start_time) * 1000
        return latency, None, str(e)


def print_orderbook(result: dict):
    """打印五档盘口数据"""
    print(f"  股票名称: {result.get('name', 'N/A')}")
    print(f"  当前价格: {result.get('price', 0):.2f}")
    print(f"  昨收价: {result.get('pre_close', 0):.2f}")
    print(f"  涨跌幅: {result.get('change_pct', 0):+.2f}%")
    print(f"\n  五档盘口:")
    print(f"  {'档位':<8} {'价格':<12} {'挂单量':<12}")
    print(f"  {'-' * 32}")
    
    # 卖盘 (卖5 -> 卖1)
    ask_prices = result.get('ask_prices', [])
    ask_volumes = result.get('ask_volumes', [])
    for i in range(len(ask_prices) - 1, -1, -1):
        level = 5 - i
        p = ask_prices[i]
        v = ask_volumes[i] if i < len(ask_volumes) else 0
        if p > 0:
            print(f"  卖{level:<6} {p:<12.2f} {v:<12}")
    
    # 买盘 (买1 -> 买5)
    bid_prices = result.get('bid_prices', [])
    bid_volumes = result.get('bid_volumes', [])
    for i in range(len(bid_prices)):
        level = i + 1
        p = bid_prices[i]
        v = bid_volumes[i] if i < len(bid_volumes) else 0
        if p > 0:
            print(f"  买{level:<6} {p:<12.2f} {v:<12}")


# ==================== 测试代码 ====================

print("=" * 80)
print("Tushare 爬虫接口延迟测试（五档盘口）")
print("=" * 80)

# 测试股票代码
test_codes = ['000681.SH']

# 1. 单次测试
print("\n【测试1】get_realtime_orderbook (爬虫接口)")
print("-" * 80)
for code in test_codes:
    latency, result, error = test_latency(get_realtime_orderbook, code)
    if error:
        print(f"{code}: ❌ 失败 - {error} (延迟: {latency:.2f}ms)")
    else:
        print(f"{code}: ✅ 成功 (延迟: {latency:.2f}ms)")
        if result:
            print_orderbook(result)

# 2. 多次测试取平均值
print("\n【测试2】get_realtime_orderbook 多次测试（取平均值）")
print("-" * 80)
test_code = '000681.SH'
latencies = []
for i in range(5):
    latency, result, error = test_latency(get_realtime_orderbook, test_code)
    if not error:
        latencies.append(latency)
        print(f"第 {i+1} 次: {latency:.2f}ms")
        if i == 0:  # 第一次显示五档数据
            print_orderbook(result)
    else:
        print(f"第 {i+1} 次: ❌ 失败 - {error}")

if latencies:
    avg_latency = sum(latencies) / len(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)
    print(f"\n统计结果:")
    print(f"  平均延迟: {avg_latency:.2f}ms")
    print(f"  最小延迟: {min_latency:.2f}ms")
    print(f"  最大延迟: {max_latency:.2f}ms")

print("\n" + "=" * 80)
print("测试完成")
print("=" * 80)