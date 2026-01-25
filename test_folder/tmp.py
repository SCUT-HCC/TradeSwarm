import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
import traceback
import pandas as pd

# 添加项目根目录到路径
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# 导入配置加载器
try:
    from utils.config_loader import load_config
    config = load_config()
except Exception as e:
    print(f"⚠️ 警告: 无法加载配置文件，使用默认配置: {e}")
    config = {
        "data_sources": {
            "tushare_token": os.getenv("TUSHARE_TOKEN", ""),
            "akshare_default_news_limit": 10,
            "akshare_request_timeout": 30
        }
    }


from datasources.data_sources.akshare_provider import AkshareProvider
from datasources.data_sources.tushare_provider import TushareProvider


# 测试用的股票代码（贵州茅台，比较稳定）
TEST_SYMBOL = "002050"
TEST_TS_CODE = "600519.SH"
test_results: List[Tuple[str, bool, str]] = []
akshare_provider = AkshareProvider(config=config)
result = akshare_provider.get_currency_exchange_rate()
print(result)