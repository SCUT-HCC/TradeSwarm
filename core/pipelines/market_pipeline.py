"""
Market Pipeline

处理实时市场数据、价格走势、成交量等技术指标分析。
"""

import asyncio
from typing import Dict, Any
from datetime import datetime

from .base_pipeline import DataCollectionPipeline


class MarketPipeline(DataCollectionPipeline):
    """市场数据Pipeline"""
    
    def __init__(self, db_manager, session_id: str):
        """
        初始化Market Pipeline
        
        参数:
            db_manager: 数据库管理器
            session_id: 会话ID
        """
        super().__init__(db_manager, session_id, "market_pipeline")
    
    def get_output_type(self) -> str:
        """获取输出类型"""
        return "market_analysis"
    
    async def collect_data(self) -> Dict[str, Any]:
        """
        采集市场数据
        
        返回:
            Dict[str, Any]: 市场分析数据
        """
        self.logger.info("开始采集市场数据...")
        
        # 模拟市场数据采集（实际实现需要对接真实API）
        await asyncio.sleep(1.0)  # 模拟网络请求延迟
        
        # 模拟市场数据
        market_data = {
            "timestamp": datetime.now().isoformat(),
            "symbol": "AAPL",
            "price": 175.50,
            "change": 2.30,
            "change_percent": 1.33,
            "volume": 45000000,
            "market_cap": 2800000000000,
            "technical_indicators": {
                "rsi": 65.2,
                "macd": 1.25,
                "bollinger_upper": 180.5,
                "bollinger_lower": 170.2,
                "moving_average_20": 175.8,
                "moving_average_50": 172.1
            },
            "price_trend": {
                "short_term": "bullish",
                "medium_term": "neutral", 
                "long_term": "bullish"
            },
            "support_resistance": {
                "support_levels": [170.0, 165.0, 160.0],
                "resistance_levels": [180.0, 185.0, 190.0]
            },
            "volatility": {
                "current_volatility": 0.15,
                "historical_volatility": 0.18,
                "volatility_rank": "medium"
            }
        }
        
        # 生成市场分析报告
        analysis = self._analyze_market_data(market_data)
        
        self.logger.info("市场数据采集完成")
        return analysis
    
    def _analyze_market_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        分析市场数据
        
        参数:
            data: 原始市场数据
            
        返回:
            Dict[str, Any]: 分析结果
        """
        # 技术指标分析
        technical_score = self._calculate_technical_score(data["technical_indicators"])
        
        # 趋势分析
        trend_analysis = self._analyze_trend(data["price_trend"])
        
        # 支撑阻力分析
        support_resistance = self._analyze_support_resistance(
            data["price"],
            data["support_resistance"]
        )
        
        # 波动率分析
        volatility_analysis = self._analyze_volatility(data["volatility"])
        
        # 综合评分
        overall_score = (
            technical_score * 0.4 +
            trend_analysis["score"] * 0.3 +
            support_resistance["score"] * 0.2 +
            volatility_analysis["score"] * 0.1
        )
        
        return {
            "timestamp": data["timestamp"],
            "symbol": data["symbol"],
            "current_price": data["price"],
            "price_change": {
                "absolute": data["change"],
                "percentage": data["change_percent"]
            },
            "volume": data["volume"],
            "market_cap": data["market_cap"],
            "technical_analysis": {
                "indicators": data["technical_indicators"],
                "score": technical_score,
                "signal": "buy" if technical_score > 0.6 else "sell" if technical_score < 0.4 else "hold"
            },
            "trend_analysis": trend_analysis,
            "support_resistance_analysis": support_resistance,
            "volatility_analysis": volatility_analysis,
            "overall_score": overall_score,
            "recommendation": self._generate_recommendation(overall_score),
            "confidence": min(abs(overall_score - 0.5) * 2, 1.0)
        }
    
    def _calculate_technical_score(self, indicators: Dict[str, float]) -> float:
        """计算技术指标评分"""
        score = 0.0
        
        # RSI分析
        rsi = indicators["rsi"]
        if rsi > 70:
            score += 0.2  # 超买
        elif rsi < 30:
            score += 0.8  # 超卖
        else:
            score += 0.5  # 中性
        
        # MACD分析
        macd = indicators["macd"]
        if macd > 0:
            score += 0.3  # 看涨
        else:
            score += 0.2  # 看跌
        
        # 布林带分析
        price = 175.50  # 假设当前价格
        bb_upper = indicators["bollinger_upper"]
        bb_lower = indicators["bollinger_lower"]
        
        if price > bb_upper:
            score += 0.1  # 接近上轨
        elif price < bb_lower:
            score += 0.3  # 接近下轨
        
        return min(score, 1.0)
    
    def _analyze_trend(self, trend_data: Dict[str, str]) -> Dict[str, Any]:
        """分析价格趋势"""
        trend_scores = {
            "bullish": 0.8,
            "neutral": 0.5,
            "bearish": 0.2
        }
        
        short_score = trend_scores.get(trend_data["short_term"], 0.5)
        medium_score = trend_scores.get(trend_data["medium_term"], 0.5)
        long_score = trend_scores.get(trend_data["long_term"], 0.5)
        
        overall_score = (short_score * 0.5 + medium_score * 0.3 + long_score * 0.2)
        
        return {
            "short_term": trend_data["short_term"],
            "medium_term": trend_data["medium_term"],
            "long_term": trend_data["long_term"],
            "score": overall_score,
            "direction": "bullish" if overall_score > 0.6 else "bearish" if overall_score < 0.4 else "neutral"
        }
    
    def _analyze_support_resistance(
        self,
        current_price: float,
        levels: Dict[str, list]
    ) -> Dict[str, Any]:
        """分析支撑阻力位"""
        support_levels = levels["support_levels"]
        resistance_levels = levels["resistance_levels"]
        
        # 找到最近的支撑和阻力位
        nearest_support = max([s for s in support_levels if s < current_price], default=0)
        nearest_resistance = min([r for r in resistance_levels if r > current_price], default=float('inf'))
        
        # 计算距离支撑和阻力的百分比
        support_distance = (current_price - nearest_support) / current_price if nearest_support > 0 else 1.0
        resistance_distance = (nearest_resistance - current_price) / current_price if nearest_resistance != float('inf') else 1.0
        
        # 评分：距离支撑越近，看涨信号越强
        score = 0.5 + (0.5 - support_distance) * 0.5
        
        return {
            "nearest_support": nearest_support,
            "nearest_resistance": nearest_resistance,
            "support_distance": support_distance,
            "resistance_distance": resistance_distance,
            "score": score,
            "signal": "bullish" if support_distance < 0.05 else "bearish" if resistance_distance < 0.05 else "neutral"
        }
    
    def _analyze_volatility(self, volatility_data: Dict[str, Any]) -> Dict[str, Any]:
        """分析波动率"""
        current_vol = volatility_data["current_volatility"]
        historical_vol = volatility_data["historical_volatility"]
        
        # 波动率比较
        vol_ratio = current_vol / historical_vol if historical_vol > 0 else 1.0
        
        # 评分：适中的波动率较好
        if vol_ratio < 0.8:
            score = 0.6  # 低波动率
        elif vol_ratio > 1.2:
            score = 0.4  # 高波动率
        else:
            score = 0.7  # 适中波动率
        
        return {
            "current_volatility": current_vol,
            "historical_volatility": historical_vol,
            "volatility_ratio": vol_ratio,
            "score": score,
            "level": "low" if vol_ratio < 0.8 else "high" if vol_ratio > 1.2 else "normal"
        }
    
    def _generate_recommendation(self, overall_score: float) -> str:
        """生成投资建议"""
        if overall_score > 0.7:
            return "强烈买入"
        elif overall_score > 0.6:
            return "买入"
        elif overall_score > 0.4:
            return "持有"
        elif overall_score > 0.3:
            return "卖出"
        else:
            return "强烈卖出"
