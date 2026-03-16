"""
股票池配置
"""

SECTOR_STOCKS = {
    # 科技 (Technology)
    'Technology': [
        'AAPL',   # Apple
        'MSFT',   # Microsoft
        'GOOGL',  # Alphabet
        'NVDA',   # NVIDIA
        'META',   # Meta
        'AVGO',   # Broadcom
        'ORCL',   # Oracle
        'CRM',    # Salesforce
        'AMD',    # AMD
        'ADBE',   # Adobe
    ],
    
    # 金融 (Financials)
    'Financials': [
        'JPM',    # JPMorgan
        'BAC',    # Bank of America
        'WFC',    # Wells Fargo
        'GS',     # Goldman Sachs
        'MS',     # Morgan Stanley
        'BLK',    # BlackRock
        'C',      # Citigroup
        'SCHW',   # Charles Schwab
        'AXP',    # American Express
        'USB',    # US Bancorp
    ],
    
    # 医疗健康 (Healthcare)
    'Healthcare': [
        'UNH',    # UnitedHealth
        'JNJ',    # Johnson & Johnson
        'PFE',    # Pfizer
        'ABBV',   # AbbVie
        'MRK',    # Merck
        'LLY',    # Eli Lilly
        'TMO',    # Thermo Fisher
        'ABT',    # Abbott
        'DHR',    # Danaher
        'BMY',    # Bristol-Myers
    ],
    
    # 消费品 (Consumer Discretionary)
    'Consumer': [
        'AMZN',   # Amazon
        'TSLA',   # Tesla
        'HD',     # Home Depot
        'MCD',    # McDonald's
        'NKE',    # Nike
        'LOW',    # Lowe's
        'SBUX',   # Starbucks
        'TJX',    # TJX Companies
        'BKNG',   # Booking
        'CMG',    # Chipotle
    ],
    
    # 通信服务 (Communication Services)
    'Communication': [
        'GOOG',   # Alphabet Class C
        'DIS',    # Disney
        'NFLX',   # Netflix
        'CMCSA',  # Comcast
        'VZ',     # Verizon
        'T',      # AT&T
        'TMUS',   # T-Mobile
        'CHTR',   # Charter
        'EA',     # Electronic Arts
    ],
    
    # 工业 (Industrials)
    'Industrials': [
        'CAT',    # Caterpillar
        'UNP',    # Union Pacific
        'HON',    # Honeywell
        'BA',     # Boeing
        'RTX',    # Raytheon
        'DE',     # John Deere
        'LMT',    # Lockheed Martin
        'GE',     # GE Aerospace
        'UPS',    # UPS
        'MMM',    # 3M
    ],
    
    # 能源 (Energy)
    'Energy': [
        'XOM',    # Exxon Mobil
        'CVX',    # Chevron
        'COP',    # ConocoPhillips
        'SLB',    # Schlumberger
        'EOG',    # EOG Resources
        'MPC',    # Marathon Petroleum
        'PSX',    # Phillips 66
        'VLO',    # Valero
        'OXY',    # Occidental
        'HAL',    # Halliburton
    ],
    
    # 必需消费品 (Consumer Staples)
    'Staples': [
        'PG',     # Procter & Gamble
        'KO',     # Coca-Cola
        'PEP',    # PepsiCo
        'COST',   # Costco
        'WMT',    # Walmart
        'PM',     # Philip Morris
        'MO',     # Altria
        'CL',     # Colgate-Palmolive
        'MDLZ',   # Mondelez
        'KHC',    # Kraft Heinz
    ],
    
    # 公用事业 (Utilities)
    'Utilities': [
        'NEE',    # NextEra Energy
        'DUK',    # Duke Energy
        'SO',     # Southern Company
        'D',      # Dominion Energy
        'AEP',    # American Electric
        'SRE',    # Sempra
        'EXC',    # Exelon
        'XEL',    # Xcel Energy
        'PEG',    # PSEG
        'ED',     # Consolidated Edison
    ],
    
    # 房地产 (Real Estate)
    'RealEstate': [
        'AMT',    # American Tower
        'PLD',    # Prologis
        'CCI',    # Crown Castle
        'EQIX',   # Equinix
        'SPG',    # Simon Property
        'PSA',    # Public Storage
        'O',      # Realty Income
        'WELL',   # Welltower
        'DLR',    # Digital Realty
        'AVB',    # AvalonBay
    ],
    
    # 材料 (Materials)
    'Materials': [
        'LIN',    # Linde
        'APD',    # Air Products
        'SHW',    # Sherwin-Williams
        'FCX',    # Freeport-McMoRan
        'ECL',    # Ecolab
        'NEM',    # Newmont
        'NUE',    # Nucor
        'DOW',    # Dow Inc
        'DD',     # DuPont
        'VMC',    # Vulcan Materials
    ],
}

# 市场指数（用于大盘判断）
MARKET_INDICES = {
    'SPY': 'S&P 500 ETF',
    'QQQ': 'Nasdaq 100 ETF',
    'IWM': 'Russell 2000 ETF',
    'VTI': 'Total Stock Market ETF',
}

# 汇总所有股票
STOCK_POOL = []
for sector, stocks in SECTOR_STOCKS.items():
    STOCK_POOL.extend(stocks)

# 去重
STOCK_POOL = list(set(STOCK_POOL))


def get_all_symbols() -> list:
    """获取所有股票代码"""
    return STOCK_POOL.copy()


def get_sector_symbols(sector: str) -> list:
    """获取指定行业的股票"""
    return SECTOR_STOCKS.get(sector, [])


def get_sectors() -> list:
    """获取所有行业名称"""
    return list(SECTOR_STOCKS.keys())


if __name__ == '__main__':
    print(f"总股票数: {len(STOCK_POOL)}")
    for sector, stocks in SECTOR_STOCKS.items():
        print(f"  {sector}: {len(stocks)} 只")

