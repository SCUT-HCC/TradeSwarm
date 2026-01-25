import akshare as ak
import pandas as pd
from datetime import datetime


def get_smart_money_flow():
    """
    获取北向资金实时净流入情况
    """
    try:
        # 获取沪股通、深股通的实时数据
        df = ak.stock_hsgt_north_net_flow_in_em(symbol="全部")
        
        # 这个接口返回的是一个单行 DataFrame，包含最新的资金数据
        # 格式通常包含: date, value(亿元)
        item = df.iloc[0]
        
        # 资金单位转换（接口返回可能是万元或亿元，需确认，通常 AkShare 此时返回的是元或万元）
        # 这里假设需要根据列名判断，通常东方财富返回的是【万元】
        money = item['value'] 
        
        # 简单转换成便于阅读的字符串
        flow_status = "净流入" if money > 0 else "净流出"
        amount_yi = money / 10000  # 转换为亿元
        
        return {
            "title": "北向资金(Smart Money)",
            "value": f"{flow_status} {amount_yi:.2f} 亿元",
            "date": str(item['date']),
            "source": "EastMoney HSGT"
        }
    except Exception as e:
        return {"error": f"北向资金获取失败: {str(e)}"}

def get_global_indices_summary():
    """
    获取关键外围指数涨跌幅 (美股, 恒生, A50)
    """
    # 定义关注的代码 (AkShare 的东方财富源代码)
    # 注意：这里需要使用 global 相关的接口，或者期货接口
    # 简单起见，我们抓取几个核心期货/指数的实时数据
    
    summary = []
    
    # 1. 富时中国 A50 (反映外资对 A 股预期)
    try:
        # 使用新浪源的全球指数/期货接口比较快
        # 也可以用 stock_hsgt_index_spot_em 获取恒生指数等
        
        # 这里演示获取外盘期货（A50, 纳指期货, 黄金, 原油）
        # 这是一个非常实用的接口: futures_foreign_commodity_realtime (外盘期货实时)
        df = ak.futures_foreign_commodity_realtime(subscribe_list=["富时A50", "道琼斯", "纳斯达克", "布伦特原油", "伦敦金"])
        
        for _, row in df.iterrows():
            name = row['名称']
            price = row['最新价']
            change_pct = row['涨跌幅']
            
            summary.append({
                "asset": name,
                "price": price,
                "change": f"{change_pct}%"
            })
            
    except Exception as e:
        print(f"外围数据获取失败: {e}")
        
    return summary

def get_currency_rate():
    """
    获取美元/人民币离岸汇率
    """
    try:
        # 获取外汇实时报价
        df = ak.fx_spot_quote()
        
        # 筛选美元/人民币
        # 名称通常是 "美元/人民币"
        usd_cny = df[df['名称'] == '美元/人民币']
        
        if not usd_cny.empty:
            price = usd_cny.iloc[0]['最新价']
            change = usd_cny.iloc[0]['涨跌幅']
            return f"USD/CNY: {price} ({change}%)"
        return "USD/CNY: N/A"
        
    except Exception as e:
        return f"汇率获取失败: {e}"

        

def get_macro_news(limit=10):
    """
    获取宏观经济新闻
    策略：
    1. 优先使用 stock_news_em 查询 "上证指数" (000001)，这是查看大盘宏观消息的黑客技巧。
    2. 如果需要，也可以扩展使用 news_economic_baidu。
    """
    print("🔄 正在获取宏观市场资讯...")
    
    news_results = []
    
    # --- 策略 A: 上证指数新闻 (利用 stock_news_em) ---
    # 000001 是上证指数代码，这里的新闻即为宏观/大盘新闻
    try:
        # 注意：指数代码通常不需要后缀，直接用 000001
        df = ak.stock_news_em(symbol="000001")
        
        if df is not None and not df.empty:
            # 标准化列名 (处理可能的中文列名)
            rename_map = {
                '新闻标题': 'title', '标题': 'title',
                '新闻内容': 'snippet', '内容': 'snippet',
                '发布时间': 'date', '时间': 'date',
                '文章链接': 'link', 'url': 'link'
            }
            df = df.rename(columns=rename_map)
            
            # 确保有 snippet 列，如果没有内容列，用标题代替
            if 'snippet' not in df.columns:
                df['snippet'] = df['title']
                
            # 转换时间
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
            # 排序并截取
            df = df.sort_values(by='date', ascending=False).head(limit)
            
            for _, row in df.iterrows():
                news_results.append({
                    "title": str(row.get('title', '')),
                    "snippet": str(row.get('snippet', ''))[:100], # 截取摘要
                    "date": str(row['date']),
                    "link": str(row.get('link', '')),
                    "source": "EastMoney (Macro/Index)"
                })
                
            return news_results

    except Exception as e:
        print(f"策略 A (上证指数) 失败: {e}")

    # --- 策略 B: 百度财经新闻 (news_economic_baidu) ---
    # 如果策略 A 没数据或想作为补充，可以使用这个
    try:
        print("尝试切换至百度财经源...")
        df_baidu = ak.news_economic_baidu()
        
        if df_baidu is not None and not df_baidu.empty:
            # 百度返回的列通常是 ['日期', '时间', '事件'] 或类似的
            # 这里的字段可能需要根据实际返回 print(df.columns) 调整，常见的是 'event' 或 'title'
            
            # 假设列名包含 'title' 或 'event'
            title_col = 'event' if 'event' in df_baidu.columns else 'title'
            time_col = 'date' if 'date' in df_baidu.columns else '时间'
            
            if title_col in df_baidu.columns:
                for _, row in df_baidu.head(limit).iterrows():
                    news_results.append({
                        "title": str(row.get(title_col, '')),
                        "snippet": "Baidu Economic News",
                        "date": str(row.get(time_col, datetime.now().strftime("%Y-%m-%d"))),
                        "link": "", # 百度这个接口可能不带链接
                        "source": "Baidu Economic"
                    })
                return news_results
                
    except Exception as e:
        print(f"策略 B (百度财经) 失败: {e}")

    return news_results

# --- 测试 ---
if __name__ == "__main__":
    news = get_macro_news(limit=5)
    print(f"\n✅ 获取到 {len(news)} 条宏观新闻：")
    for n in news:
        print(f"[{n['date']}] {n['title']}")