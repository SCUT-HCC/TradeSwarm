import tushare as ts
import pandas as pd

def get_realtime_orderbook(symbol: str):
    """
    获取实时五档盘口 (免费版，基于新浪源)
    symbol: 股票代码 (只需数字，如 '600519')
    """
    # 清洗代码：tushare 旧版只需要数字代码
    clean_symbol = symbol.split('.')[0] if '.' in symbol else symbol
    
    try:
        # 调用 get_realtime_quotes
        df = ts.get_realtime_quotes(clean_symbol)
        
        if df is None or df.empty:
            return f"找不到股票 {clean_symbol} 的行情"
            
        row = df.iloc[0]
        name = row['name']
        price = float(row['price'])
        pre_close = float(row['pre_close'])
        change_pct = (price - pre_close) / pre_close * 100
        
        # 构建 Markdown 格式的盘口，方便 Agent 阅读
        md = f"### 📊 {name} ({clean_symbol}) 实时盘口\n"
        md += f"**现价**: {price:.2f} ({change_pct:+.2f}%)\n\n"
        
        md += "| 档位 | 价格 | 挂单量 |\n"
        md += "| :--- | :--- | :--- |\n"
        
        # 卖盘 (卖5 -> 卖1)
        # 注意：旧版接口列名是 a1_p, a1_v (ask 1 price/volume)
        for i in range(5, 0, -1):
            p = float(row[f'a{i}_p'])
            v = int(row[f'a{i}_v'])
            md += f"| 🟢 卖{i} | {p:.2f} | {v} |\n"
            
        # 买盘 (买1 -> 买5)
        for i in range(1, 6):
            p = float(row[f'b{i}_p'])
            v = int(row[f'b{i}_v'])
            md += f"| 🔴 买{i} | {p:.2f} | {v} |\n"
            
        return md

    except Exception as e:
        return f"获取实时行情失败: {str(e)}"

# --- 测试代码 ---
if __name__ == "__main__":
    # 查茅台
    print(get_realtime_orderbook("300655"))