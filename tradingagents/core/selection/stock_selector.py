#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
截面选股器
基于因子打分进行股票排名选择
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime, timedelta

from .market_regime import MarketRegime
from ..data_adapter import DataAdapter


class StockSelector:
    """
    截面选股器
    
    使用多因子模型对股票进行排名，选出 Top N
    """
    
    def __init__(self, stock_pool: List[str], top_n: int = 5, data_adapter: Optional[DataAdapter] = None):
        """
        初始化选股器
        
        Args:
            stock_pool: 股票池列表
            top_n: 每次选择的股票数量
            data_adapter: 数据适配器（如果为None，则创建新的）
        """
        self.stock_pool = stock_pool
        self.top_n = top_n
        self.data_adapter = data_adapter or DataAdapter(use_cache=True)
        
        # 默认因子权重（震荡市配置）
        self.default_weights = {
            'momentum_20d': 0.25,       # 20日动量
            'momentum_60d': 0.15,       # 60日动量
            'volatility': -0.15,        # 波动率（负权重，越低越好）
            'rsi_score': 0.15,          # RSI 得分
            'volume_ratio': 0.10,       # 成交量比率
            'trend_strength': 0.20,     # 趋势强度
        }
        
        # 方案二：基于市场状态的权重配置
        # 牛市权重配置
        self.bull_weights = {
            'momentum_20d': 0.30,       # 动量因子权重高（追涨）
            'momentum_60d': 0.20,      # 长期动量也重要
            'volatility': -0.10,       # 波动率权重低（牛市不怕波动）
            'rsi_score': 0.10,         # RSI权重中等
            'volume_ratio': 0.15,     # 成交量重要（资金流入）
            'trend_strength': 0.15,   # 趋势强度重要
        }
        
        # 熊市权重配置
        self.bear_weights = {
            'momentum_20d': 0.10,      # 动量权重低（避免追跌）
            'momentum_60d': 0.05,     # 长期动量权重更低
            'volatility': -0.30,       # 波动率权重高（重视风险控制）
            'rsi_score': 0.20,        # RSI权重高（寻找超卖反弹）
            'volume_ratio': 0.15,     # 成交量中等
            'trend_strength': 0.20,   # 趋势强度重要（避免逆势）
        }
        
        # 震荡市权重配置
        self.sideways_weights = {
            'momentum_20d': 0.15,      # 动量权重中等
            'momentum_60d': 0.10,     # 长期动量权重中等
            'volatility': -0.20,      # 波动率权重较高（震荡市波动大）
            'rsi_score': 0.25,       # RSI权重最高（震荡市看超买超卖）
            'volume_ratio': 0.20,   # 成交量权重高（资金流向重要）
            'trend_strength': 0.10,  # 趋势强度权重低（震荡市无明确趋势）
        }
        
        # 当前使用的权重（初始为默认权重）
        self.factor_weights = self.default_weights.copy()
        
        # 方案一：基于IC的动态权重配置
        self.use_ic_weights = True  # 是否使用IC权重（方案一）
        self.ic_window_days = 90  # IC计算窗口（过去90天，约3个月）
        self.future_returns_days = 5  # 未来收益天数（计算5日后的收益）
        self.min_ic_samples = 20  # 最小样本数（至少需要20个数据点）
        self.ic_history = {}  # 存储历史IC值 {factor_name: [ic1, ic2, ...]}
        
        # 市场状态平滑（方案二）
        self._regime_history = []  # 记录最近N次的市场状态
        self._regime_smooth_window = 3  # 需要连续3次相同状态才切换
        
        # 数据缓存
        self._data_cache = {}
    
    def set_factor_weights(self, weights: Dict[str, float]):
        """设置因子权重"""
        self.factor_weights = weights
    
    def calculate_ic(self, factor_values: pd.Series, future_returns: pd.Series) -> float:
        """
        计算信息系数（IC）：因子值与未来收益的相关系数
        
        Args:
            factor_values: 因子值序列
            future_returns: 未来收益序列
        
        Returns:
            IC值（-1到1之间）
        """
        if len(factor_values) != len(future_returns):
            return 0.0
        
        # 去除NaN值
        valid_mask = ~(factor_values.isna() | future_returns.isna())
        if valid_mask.sum() < self.min_ic_samples:
            return 0.0
        
        factor_clean = factor_values[valid_mask]
        returns_clean = future_returns[valid_mask]
        
        if len(factor_clean) < self.min_ic_samples:
            return 0.0
        
        # 计算相关系数（Pearson相关系数）
        ic = factor_clean.corr(returns_clean)
        
        # 如果计算失败，返回0
        if pd.isna(ic):
            return 0.0
        
        return ic
    
    def calculate_factor_ics(self, date: str) -> Dict[str, float]:
        """
        计算所有因子的IC值（方案一）
        使用最近几个历史时点的截面IC，然后取平均
        
        注意：为了避免信息泄露，我们只使用date之前足够远的历史数据
        对于每个历史时点hist_date，我们使用hist_date之前的数据来计算IC
        
        Args:
            date: 计算日期
        
        Returns:
            各因子的IC值字典
        """
        factor_ics = {}
        
        # 使用最近4个时点（每月一个，共4个月）
        # 但确保这些时点都在date之前足够远，以便计算未来收益时不泄露信息
        historical_dates = []
        current_dt = datetime.strptime(date, '%Y-%m-%d')
        for i in range(1, 5):  # 从1开始，确保有足够的时间计算未来收益
            # 往前推i个月，再往前推future_returns_days天，确保未来收益在date之前
            month_dt = current_dt - timedelta(days=30 * i + self.future_returns_days)
            date_str = month_dt.strftime('%Y-%m-%d')
            historical_dates.append(date_str)
        
        # 计算每个因子的IC
        for factor_name in self.default_weights.keys():
            ic_values = []  # 存储每个时点的IC值
            
            # 遍历历史时点，计算截面IC
            for hist_date in historical_dates:
                # 确保hist_date + future_returns_days <= date，避免信息泄露
                hist_dt = datetime.strptime(hist_date, '%Y-%m-%d')
                future_dt = hist_dt + timedelta(days=self.future_returns_days)
                current_dt = datetime.strptime(date, '%Y-%m-%d')
                
                if future_dt > current_dt:
                    # 如果未来收益日期超过了当前日期，跳过这个时点
                    continue
                
                factor_values_list = []
                future_returns_list = []
                
                # 在此时点，计算所有股票的因子值和未来收益
                for symbol in self.stock_pool:
                    # 加载数据（需要包含未来收益的数据）
                    # 但只加载到hist_date + future_returns_days，不超过date
                    future_date_dt = hist_dt + timedelta(days=self.future_returns_days)
                    future_date = future_date_dt.strftime('%Y-%m-%d')
                    
                    # 确保future_date <= date
                    if future_date_dt > current_dt:
                        continue
                    
                    df = self.load_data(symbol, future_date, lookback_days=120)
                    
                    if df is None or len(df) < 60:
                        continue
                    
                    # 只使用到hist_date的数据计算因子
                    df_factor = df[df.index <= hist_date]
                    if len(df_factor) < 60:
                        continue
                    
                    try:
                        # 计算因子值（使用到hist_date的数据）
                        factors = self.calculate_factors(df_factor)
                        if factor_name not in factors or pd.isna(factors[factor_name]):
                            continue
                        
                        factor_value = factors[factor_name]
                        
                        # 计算未来收益（使用hist_date和future_date的价格）
                        current_price = df_factor['Close'].iloc[-1]
                        
                        # 获取未来价格（使用future_date的数据）
                        df_future = df[df.index <= future_date]
                        if len(df_future) == 0:
                            continue
                        
                        future_price = df_future['Close'].iloc[-1]
                        future_return = (future_price / current_price - 1) if current_price > 0 else 0
                        
                        factor_values_list.append(factor_value)
                        future_returns_list.append(future_return)
                    except Exception as e:
                        continue
                
                # 计算此时点的截面IC
                if len(factor_values_list) >= 10:  # 至少需要10只股票
                    factor_series = pd.Series(factor_values_list)
                    returns_series = pd.Series(future_returns_list)
                    ic = self.calculate_ic(factor_series, returns_series)
                    if not pd.isna(ic) and abs(ic) > 0.001:  # 过滤掉过小的IC
                        ic_values.append(ic)
            
            # 计算平均IC
            if len(ic_values) >= 2:  # 至少需要2个时点的IC
                avg_ic = np.mean(ic_values)
                factor_ics[factor_name] = avg_ic
            else:
                # 如果IC数据不足，使用默认权重对应的IC（假设为0.1）
                factor_ics[factor_name] = 0.0
        
        return factor_ics
    
    def update_weights_by_ic(self, factor_ics: Dict[str, float]):
        """
        根据IC值更新权重（方案一）
        
        Args:
            factor_ics: 各因子的IC值字典
        """
        if not factor_ics:
            return
        
        # 计算IC绝对值之和（用于归一化）
        ic_abs_sum = sum(abs(ic) for ic in factor_ics.values())
        
        if ic_abs_sum == 0:
            # 如果所有IC都为0，使用默认权重
            self.factor_weights = self.default_weights.copy()
            return
        
        # 根据IC绝对值分配权重
        new_weights = {}
        for factor_name, ic in factor_ics.items():
            # 使用IC绝对值，并归一化
            ic_abs = abs(ic)
            weight = ic_abs / ic_abs_sum
            
            # 对于负权重因子（如volatility），保持符号
            if factor_name == 'volatility':
                # 波动率是负权重因子，IC为负时应该增加权重（绝对值）
                new_weights[factor_name] = -weight
            else:
                new_weights[factor_name] = weight
        
        # 平滑权重变化（避免剧烈波动）
        if hasattr(self, 'factor_weights') and self.factor_weights:
            # 使用70%旧权重 + 30%新权重进行平滑
            for factor_name in new_weights.keys():
                old_weight = self.factor_weights.get(factor_name, 0)
                new_weight = new_weights[factor_name]
                self.factor_weights[factor_name] = 0.7 * old_weight + 0.3 * new_weight
        else:
            self.factor_weights = new_weights
        
        # 输出IC和权重信息
        print(f"  [IC权重] 因子IC值:")
        for factor_name, ic in factor_ics.items():
            print(f"    {factor_name}: {ic:.3f} -> 权重: {self.factor_weights.get(factor_name, 0):.3f}")
    
    def _identify_market_regime(self, market_df: pd.DataFrame) -> MarketRegime:
        """
        识别市场状态（方案二）
        
        Args:
            market_df: 市场指数数据（如SPY）
        
        Returns:
            市场状态
        """
        if market_df is None or len(market_df) < 200:
            return MarketRegime.SIDEWAYS
        
        close = market_df['Close']
        
        # 计算均线
        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ma200 = close.rolling(200).mean()
        
        current_price = close.iloc[-1]
        current_ma20 = ma20.iloc[-1]
        current_ma50 = ma50.iloc[-1]
        current_ma200 = ma200.iloc[-1]
        
        # 计算收益率
        returns_20d = (close.iloc[-1] / close.iloc[-20] - 1) if len(close) >= 20 else 0
        returns_60d = (close.iloc[-1] / close.iloc[-60] - 1) if len(close) >= 60 else 0
        
        # 计算波动率
        volatility = close.pct_change().tail(20).std() * np.sqrt(252) if len(close) >= 20 else 0
        
        # 评分系统
        bull_score = 0
        bear_score = 0
        
        # 1. 趋势评分（价格与均线关系）
        if current_price > current_ma20 > current_ma50 > current_ma200:
            bull_score += 2
        elif current_price < current_ma20 < current_ma50 < current_ma200:
            bear_score += 2
        elif current_price > current_ma20 and current_ma20 > current_ma50:
            bull_score += 1
        elif current_price < current_ma20 and current_ma20 < current_ma50:
            bear_score += 1
        
        # 2. 动量评分
        if returns_20d > 0.02 and returns_60d > 0.05:  # 20日>2%, 60日>5%
            bull_score += 1
        elif returns_20d < -0.02 and returns_60d < -0.05:
            bear_score += 1
        
        # 3. 波动率评分（高波动率倾向于震荡或熊市）
        if volatility > 0.25:  # 年化波动率>25%
            bear_score += 0.5
        elif volatility < 0.15:  # 年化波动率<15%
            bull_score += 0.5
        
        # 判断状态
        if bull_score >= 2:
            return MarketRegime.BULL
        elif bear_score >= 2:
            return MarketRegime.BEAR
        else:
            return MarketRegime.SIDEWAYS
    
    def _update_weights_by_regime(self, regime: MarketRegime):
        """
        根据市场状态更新权重（带平滑机制）
        
        Args:
            regime: 当前市场状态
        """
        # 记录状态历史
        self._regime_history.append(regime)
        if len(self._regime_history) > self._regime_smooth_window:
            self._regime_history.pop(0)
        
        # 平滑机制：需要连续N次相同状态才切换
        if len(self._regime_history) >= self._regime_smooth_window:
            # 检查最近N次是否都是同一状态
            if all(r == regime for r in self._regime_history[-self._regime_smooth_window:]):
                # 确定目标权重
                if regime == MarketRegime.BULL:
                    target_weights = self.bull_weights
                elif regime == MarketRegime.BEAR:
                    target_weights = self.bear_weights
                else:
                    target_weights = self.sideways_weights
                
                # 平滑切换权重（避免剧烈变化）
                for factor in self.factor_weights.keys():
                    old_weight = self.factor_weights.get(factor, 0)
                    new_weight = target_weights.get(factor, 0)
                    # 使用70%旧权重 + 30%新权重进行平滑
                    self.factor_weights[factor] = 0.7 * old_weight + 0.3 * new_weight
                
                print(f"  [市场状态] {regime.value} - 权重已切换")
            else:
                # 状态不稳定，保持当前权重
                pass
        else:
            # 历史不足，直接使用目标权重
            if regime == MarketRegime.BULL:
                self.factor_weights = self.bull_weights.copy()
            elif regime == MarketRegime.BEAR:
                self.factor_weights = self.bear_weights.copy()
            else:
                self.factor_weights = self.sideways_weights.copy()
    
    def load_data(self, symbol: str, end_date: str, lookback_days: int = 120) -> Optional[pd.DataFrame]:
        """
        加载股票数据
        
        Args:
            symbol: 股票代码
            end_date: 截止日期
            lookback_days: 回看天数（用于计算因子）
        """
        cache_key = f"{symbol}_{end_date}"
        if cache_key in self._data_cache:
            return self._data_cache[cache_key]
        
        try:
            # 使用 DataAdapter 加载数据
            df = self.data_adapter.load_stock_data_until(symbol, end_date)
            
            if df is not None and len(df) > 20:
                # 确保有足够的历史数据
                if len(df) >= lookback_days:
                    self._data_cache[cache_key] = df
                    return df
            return None
        except Exception as e:
            print(f"  [WARN] 加载 {symbol} 失败: {e}")
            return None
    
    def calculate_factors(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        计算单只股票的所有因子值
        
        Args:
            df: 股票数据 DataFrame
        
        Returns:
            因子值字典
        """
        close = df['Close']
        volume = df['Volume']
        high = df['High']
        low = df['Low']
        
        factors = {}
        
        # 1. 动量因子
        factors['momentum_20d'] = (close.iloc[-1] / close.iloc[-20] - 1) if len(close) >= 20 else 0
        factors['momentum_60d'] = (close.iloc[-1] / close.iloc[-60] - 1) if len(close) >= 60 else 0
        
        # 2. 波动率（20日标准差/均值）
        if len(close) >= 20:
            returns = close.pct_change().dropna()
            factors['volatility'] = returns.tail(20).std() * np.sqrt(252)  # 年化波动率
        else:
            factors['volatility'] = 0
        
        # 3. RSI 得分（RSI 在 40-60 之间得分最高）
        rsi = self._calculate_rsi(close, 14)
        if not np.isnan(rsi):
            # RSI 50 附近得分最高，过高或过低扣分
            factors['rsi_score'] = 1 - abs(rsi - 50) / 50
        else:
            factors['rsi_score'] = 0
        
        # 4. 成交量比率（近5日成交量 / 20日均量）
        if len(volume) >= 20:
            recent_vol = volume.tail(5).mean()
            avg_vol = volume.tail(20).mean()
            factors['volume_ratio'] = recent_vol / avg_vol if avg_vol > 0 else 1
        else:
            factors['volume_ratio'] = 1
        
        # 5. 趋势强度（价格在均线上方的程度）
        if len(close) >= 50:
            ma20 = close.tail(20).mean()
            ma50 = close.tail(50).mean()
            current = close.iloc[-1]
            
            # 价格相对于均线的位置
            above_ma20 = (current - ma20) / ma20 if ma20 > 0 else 0
            above_ma50 = (current - ma50) / ma50 if ma50 > 0 else 0
            ma_trend = (ma20 - ma50) / ma50 if ma50 > 0 else 0
            
            factors['trend_strength'] = above_ma20 * 0.4 + above_ma50 * 0.3 + ma_trend * 0.3
        else:
            factors['trend_strength'] = 0
        
        return factors
    
    def _calculate_rsi(self, close: pd.Series, period: int = 14) -> float:
        """计算 RSI"""
        if len(close) < period + 1:
            return np.nan
        
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi.iloc[-1]
    
    def calculate_composite_score(self, factors: Dict[str, float]) -> float:
        """
        计算综合得分
        
        Args:
            factors: 因子值字典
        
        Returns:
            综合得分
        """
        score = 0.0
        for factor_name, weight in self.factor_weights.items():
            if factor_name in factors:
                score += factors[factor_name] * weight
        return score
    
    def rank_stocks(self, date: str, min_data_days: int = 60) -> pd.DataFrame:
        """
        对股票池进行排名
        
        Args:
            date: 排名日期
            min_data_days: 最少需要的数据天数
        
        Returns:
            排名结果 DataFrame
        """
        # 方案一：基于IC的动态权重
        if self.use_ic_weights:
            try:
                factor_ics = self.calculate_factor_ics(date)
                if factor_ics:
                    self.update_weights_by_ic(factor_ics)
                else:
                    print(f"  [IC权重] 无法计算IC，使用默认权重")
            except Exception as e:
                print(f"  [IC权重] 计算IC失败: {e}，使用默认权重")
                self.factor_weights = self.default_weights.copy()
        
        # 方案二：识别市场状态并切换权重（保留但默认不使用）
        # 如果需要使用方案二，可以设置 self.use_ic_weights = False
        if not self.use_ic_weights:
            spy_df = self.load_data('SPY', date)
            if spy_df is not None:
                spy_df = spy_df[spy_df.index <= date]
                if len(spy_df) >= 200:
                    market_regime = self._identify_market_regime(spy_df)
                    self._update_weights_by_regime(market_regime)
                    print(f"  [市场状态] {market_regime.value}")
                else:
                    print(f"  [市场状态] 数据不足，使用默认权重")
            else:
                print(f"  [市场状态] 无法加载SPY数据，使用默认权重")
        
        results = []
        
        print(f"\n计算 {date} 的因子得分...")
        
        for symbol in self.stock_pool:
            df = self.load_data(symbol, date)
            
            if df is None or len(df) < min_data_days:
                continue
            
            # 截取到指定日期
            df = df[df.index <= date]
            
            if len(df) < min_data_days:
                continue
            
            try:
                factors = self.calculate_factors(df)
                score = self.calculate_composite_score(factors)
                
                results.append({
                    'symbol': symbol,
                    'score': score,
                    **factors
                })
            except Exception as e:
                print(f"  [WARN] 计算 {symbol} 因子失败: {e}")
                continue
        
        if not results:
            print("  [WARN] 没有有效的股票数据")
            return pd.DataFrame()
        
        df_results = pd.DataFrame(results)
        
        # 对每个因子进行标准化（z-score）
        for col in self.factor_weights.keys():
            if col in df_results.columns:
                mean = df_results[col].mean()
                std = df_results[col].std()
                if std > 0:
                    df_results[f'{col}_zscore'] = (df_results[col] - mean) / std
        
        # 重新计算标准化后的综合得分
        df_results['zscore_total'] = 0.0
        for factor_name, weight in self.factor_weights.items():
            zscore_col = f'{factor_name}_zscore'
            if zscore_col in df_results.columns:
                df_results['zscore_total'] += df_results[zscore_col] * weight
        
        # 排名
        df_results['rank'] = df_results['zscore_total'].rank(ascending=False)
        df_results = df_results.sort_values('rank')
        
        return df_results
    
    def select_stocks(self, date: str) -> List[str]:
        """
        选出 Top N 股票
        
        Args:
            date: 选股日期
        
        Returns:
            选中的股票列表
        """
        df_ranked = self.rank_stocks(date)
        
        if df_ranked.empty:
            return []
        
        selected = df_ranked.head(self.top_n)['symbol'].tolist()
        
        print(f"\n选中股票 ({date}):")
        for i, row in df_ranked.head(self.top_n).iterrows():
            print(f"  {int(row['rank'])}. {row['symbol']}: 得分={row['zscore_total']:.3f}")
        
        return selected
    
    def clear_cache(self):
        """清除数据缓存"""
        self._data_cache.clear()


def get_monthly_rebalance_dates(start_date: str, end_date: str) -> List[str]:
    """
    获取每月第一个交易日作为再平衡日
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
    
    Returns:
        再平衡日期列表
    """
    dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # Month Start
    return [d.strftime('%Y-%m-%d') for d in dates]
