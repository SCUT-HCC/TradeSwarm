# import akshare as ak
# import pandas as pd
# import re
# from datetime import datetime

# def get_stock_news(symbol: str, start_date: str = None, end_date: str = None) -> str:
#     """
#     获取指定股票的新闻，并支持时间范围筛选。
    
#     Args:
#         symbol: 股票代码 (如 "600519")
#         start_date: 开始日期 "YYYY-MM-DD" (可选)
#         end_date: 结束日期 "YYYY-MM-DD" (可选)
#     """
#     # 1. 清洗代码
#     clean_symbol = re.sub(r"\D", "", symbol)
    
#     try:
#         # 2. 调用接口 (默认拉取最近的新闻)
#         df = ak.stock_news_em(symbol=clean_symbol)
#         import pdb; pdb.set_trace()
#         if df is None or df.empty:
#             return f"未找到股票 {symbol} 的相关新闻。"

#         # 3. 【关键】将 '发布时间' 转为 datetime 对象以便比较
#         # 东方财富的格式通常是 "2023-10-27 15:30:00"
#         df['发布时间'] = pd.to_datetime(df['发布时间'])

#         # 4. 执行日期筛选
#         if start_date:
#             # 将输入字符串转为 datetime (默认时间为 00:00:00)
#             s_dt = pd.to_datetime(start_date)
#             df = df[df['发布时间'] >= s_dt]
        
#         if end_date:
#             # 将输入字符串转为 datetime，并设为当天的 23:59:59 以包含当天所有新闻
#             e_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
#             df = df[df['发布时间'] <= e_dt]

#         if df.empty:
#             return f"在 {start_date} 到 {end_date} 期间未找到相关新闻。"

#         # 5. 选择展示列 (中文列名)
#         target_cols = ['发布时间', '新闻标题', '文章链接']
#         # 确保列名存在
#         cols = [c for c in target_cols if c in df.columns]
        
#         # 6. 按时间倒序排列并返回
#         result_df = df[cols].sort_values(by='发布时间', ascending=False)
        
#         return result_df.to_markdown(index=False)

#     except Exception as e:
#         return f"获取新闻出错: {str(e)}"

# # --- 测试 ---
# if __name__ == "__main__":
#     # 示例：查询 2024年1月1日 到 2025年1月1日 之间的新闻
#     print(get_stock_news("600519", start_date="2024-01-01", end_date="2025-12-10"))

import akshare as ak
import pandas as pd
import re
from datetime import datetime, timedelta

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    【列名兜底】标准化列名，防止数据源列名变动导致 KeyError
    将中文列名统一映射为英文内部变量
    """
    # 定义可能的列名映射表 (根据历史变动经验)
    col_mapping = {
        '发布时间': 'public_time', '时间': 'public_time', 'time': 'public_time',
        '新闻标题': 'title', '标题': 'title',
        '文章链接': 'url', '链接': 'url', 'url': 'url',
        '文章来源': 'source', '来源': 'source'
    }
    
    # 重命名列
    df = df.rename(columns=col_mapping)
    
    # 检查必要列是否存在
    required_cols = ['public_time', 'title']
    for col in required_cols:
        if col not in df.columns:
            # 如果找不到标准列，尝试在现有列中模糊搜索
            found = False
            for existing_col in df.columns:
                if col == 'public_time' and ('时间' in str(existing_col) or 'time' in str(existing_col)):
                    df = df.rename(columns={existing_col: 'public_time'})
                    found = True
                    break
                if col == 'title' and ('标题' in str(existing_col) or 'title' in str(existing_col)):
                    df = df.rename(columns={existing_col: 'title'})
                    found = True
                    break
            if not found:
                raise ValueError(f"缺失关键列: {col}, 当前列名: {df.columns.tolist()}")
    
    return df

def get_stock_news_robust(symbol: str, start_date: str = None, end_date: str = None) -> str:
    """
    获取股票新闻（带 Fallback 机制）
    
    策略：
    1. 尝试获取数据。
    2. 尝试清洗列名。
    3. 尝试按日期过滤。
    4. [Fallback] 如果日期过滤后为空，返回最近的 5 条新闻作为兜底，而不是返回空字符串。
    """
    # 1. 股票代码清洗 (600519.SH -> 600519)
    clean_symbol = re.sub(r"\D", "", symbol)
    
    print(f"🔄 正在查询 {clean_symbol} 的新闻...")

    try:
        # --- API 调用 ---
        # 东方财富个股新闻接口
        df = ak.stock_news_em(symbol=clean_symbol)
        
        if df is None or df.empty:
            return f"⚠️ 未找到股票 {symbol} 的任何数据（接口返回为空）。"

        # --- 数据标准化 ---
        try:
            df = normalize_columns(df)
        except ValueError as ve:
            return f"❌ 数据解析失败（列名变更）: {str(ve)}"

        # 转换时间格式
        df['public_time'] = pd.to_datetime(df['public_time'], errors='coerce')
        # 删除无法解析时间的脏数据
        df = df.dropna(subset=['public_time'])

        # --- 准备返回的列 ---
        display_cols = ['public_time', 'title', 'url']
        # 确保 url 列存在，不存在则补空
        if 'url' not in df.columns:
            df['url'] = ''
        
        # 基础排序：最新的在前面
        df = df.sort_values(by='public_time', ascending=False)
        
        # 保存一份全量数据的副本，用于 Fallback
        full_df = df.copy()

        # --- 日期过滤逻辑 ---
        filter_msg = ""
        is_filtered = False
        
        if start_date:
            s_dt = pd.to_datetime(start_date)
            df = df[df['public_time'] >= s_dt]
            is_filtered = True
        
        if end_date:
            # 结束日期包含当天全天
            e_dt = pd.to_datetime(end_date) + timedelta(days=1) - timedelta(seconds=1)
            df = df[df['public_time'] <= e_dt]
            is_filtered = True

        # --- [关键 Fallback] 结果判断 ---
        
        # 情况 A: 过滤后有数据 -> 正常返回
        if not df.empty:
            result_txt = df[display_cols].to_markdown(index=False)
            return f"✅ 找到 {len(df)} 条符合日期范围的新闻：\n\n{result_txt}"

        # 情况 B: 过滤后没数据，但 API 有返回数据 -> 触发逻辑 Fallback
        elif is_filtered and df.empty:
            # 获取最近的 5 条作为建议
            fallback_data = full_df[display_cols].head(5)
            fallback_txt = fallback_data.to_markdown(index=False)
            
            # 计算数据源的实际时间范围
            min_date = full_df['public_time'].min().strftime('%Y-%m-%d')
            max_date = full_df['public_time'].max().strftime('%Y-%m-%d')
            
            return (
                f"⚠️ **未找到指定时间段 ({start_date} ~ {end_date}) 的新闻。**\n"
                f"数据源可用时间范围为: {min_date} 到 {max_date}。\n\n"
                f"👇 **为您展示最近的 5 条新闻作为参考：**\n\n"
                f"{fallback_txt}"
            )
            
        else:
            return "⚠️ 该股票近期无新闻。"

    except Exception as e:
        # 系统级 Fallback: 捕获所有未知错误
        return f"❌ 接口调用发生系统错误: {str(e)}"

# ==========================================
# 测试用例
# ==========================================
if __name__ == "__main__":
    # 测试 1: 正常查询（最近几天）
    print("--- 测试 1: 正常查询 ---")
    print(get_stock_news_robust("000001", start_date="2024-01-01"))
    
    # 测试 2: 触发 Fallback (查询未来的时间，或者很久以前的时间)
    print("\n--- 测试 2: 触发无数据 Fallback ---")
    # 假设我们查一个肯定没有新闻的日期
    print(get_stock_news_robust("600519", start_date="2030-01-01", end_date="2030-02-01"))
    
    # 测试 3: 测试带后缀的代码
    print("\n--- 测试 3: 代码清洗测试 ---")
    print(get_stock_news_robust("000001.SZ", start_date="2024-01-01"))