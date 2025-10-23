"""
News Pipeline

分析财经新闻、行业动态、政策变化。
"""

import asyncio
from typing import Dict, Any, List
from datetime import datetime

from .base_pipeline import DataCollectionPipeline


class NewsPipeline(DataCollectionPipeline):
    """新闻分析Pipeline"""
    
    def __init__(self, db_manager, session_id: str):
        """
        初始化News Pipeline
        
        参数:
            db_manager: 数据库管理器
            session_id: 会话ID
        """
        super().__init__(db_manager, session_id, "news_pipeline")
    
    def get_output_type(self) -> str:
        """获取输出类型"""
        return "news_analysis"
    
    async def collect_data(self) -> Dict[str, Any]:
        """
        采集新闻数据
        
        返回:
            Dict[str, Any]: 新闻分析数据
        """
        self.logger.info("开始采集新闻数据...")
        
        # 模拟新闻数据采集（实际实现需要对接财经新闻API）
        await asyncio.sleep(0.8)  # 模拟网络请求延迟
        
        # 模拟新闻数据
        news_data = {
            "timestamp": datetime.now().isoformat(),
            "sources": {
                "reuters": {
                    "articles": 15,
                    "sentiment_score": 0.68,
                    "key_headlines": [
                        "Apple reports strong Q4 earnings, beats expectations",
                        "iPhone 15 sales exceed projections in key markets"
                    ]
                },
                "bloomberg": {
                    "articles": 12,
                    "sentiment_score": 0.72,
                    "key_headlines": [
                        "Apple stock rises on positive analyst upgrades",
                        "Tech sector shows resilience amid market volatility"
                    ]
                },
                "cnbc": {
                    "articles": 8,
                    "sentiment_score": 0.65,
                    "key_headlines": [
                        "Apple's services revenue growth accelerates",
                        "Analysts raise price targets for Apple stock"
                    ]
                }
            },
            "topics": {
                "earnings": {
                    "count": 8,
                    "sentiment": 0.75,
                    "impact_score": 0.8
                },
                "product_launch": {
                    "count": 5,
                    "sentiment": 0.68,
                    "impact_score": 0.6
                },
                "market_analysis": {
                    "count": 12,
                    "sentiment": 0.62,
                    "impact_score": 0.7
                },
                "regulatory": {
                    "count": 3,
                    "sentiment": 0.45,
                    "impact_score": 0.9
                }
            },
            "key_events": [
                {
                    "title": "Apple Q4 Earnings Beat Expectations",
                    "impact": "positive",
                    "confidence": 0.85,
                    "timeframe": "immediate"
                },
                {
                    "title": "iPhone 15 Sales Exceed Projections",
                    "impact": "positive", 
                    "confidence": 0.78,
                    "timeframe": "short_term"
                },
                {
                    "title": "Regulatory Concerns Over App Store",
                    "impact": "negative",
                    "confidence": 0.65,
                    "timeframe": "medium_term"
                }
            ],
            "sentiment_breakdown": {
                "very_positive": 0.20,
                "positive": 0.45,
                "neutral": 0.25,
                "negative": 0.08,
                "very_negative": 0.02
            }
        }
        
        # 生成新闻分析报告
        analysis = self._analyze_news_data(news_data)
        
        self.logger.info("新闻数据采集完成")
        return analysis
    
    def _analyze_news_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        分析新闻数据
        
        参数:
            data: 原始新闻数据
            
        返回:
            Dict[str, Any]: 分析结果
        """
        # 计算整体新闻情绪
        overall_sentiment = self._calculate_overall_sentiment(data)
        
        # 分析新闻源
        source_analysis = self._analyze_sources(data["sources"])
        
        # 分析话题影响
        topic_analysis = self._analyze_topics(data["topics"])
        
        # 分析关键事件
        event_analysis = self._analyze_events(data["key_events"])
        
        # 新闻影响评估
        impact_assessment = self._assess_news_impact(data)
        
        # 生成新闻摘要
        news_summary = self._generate_news_summary(data)
        
        return {
            "timestamp": data["timestamp"],
            "overall_sentiment": {
                "score": overall_sentiment,
                "level": self._get_sentiment_level(overall_sentiment),
                "confidence": self._calculate_confidence(data)
            },
            "source_analysis": source_analysis,
            "topic_analysis": topic_analysis,
            "event_analysis": event_analysis,
            "impact_assessment": impact_assessment,
            "news_summary": news_summary,
            "key_insights": self._generate_key_insights(data, overall_sentiment),
            "recommendation": self._generate_news_recommendation(overall_sentiment, impact_assessment)
        }
    
    def _calculate_overall_sentiment(self, data: Dict[str, Any]) -> float:
        """计算整体新闻情绪"""
        sources = data["sources"]
        weights = {"reuters": 0.4, "bloomberg": 0.35, "cnbc": 0.25}
        
        weighted_sentiment = 0.0
        total_weight = 0.0
        
        for source, weight in weights.items():
            if source in sources:
                sentiment = sources[source]["sentiment_score"]
                weighted_sentiment += sentiment * weight
                total_weight += weight
        
        return weighted_sentiment / total_weight if total_weight > 0 else 0.5
    
    def _analyze_sources(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        """分析新闻源"""
        analysis = {}
        
        for source, data in sources.items():
            sentiment = data["sentiment_score"]
            article_count = data["articles"]
            
            analysis[source] = {
                "sentiment_score": sentiment,
                "sentiment_level": self._get_sentiment_level(sentiment),
                "article_count": article_count,
                "activity_level": self._get_activity_level(article_count),
                "reliability_score": self._get_reliability_score(source),
                "key_headlines": data.get("key_headlines", [])
            }
        
        return analysis
    
    def _analyze_topics(self, topics: Dict[str, Any]) -> Dict[str, Any]:
        """分析话题影响"""
        analysis = {}
        
        for topic, data in topics.items():
            sentiment = data["sentiment"]
            impact = data["impact_score"]
            count = data["count"]
            
            # 计算话题重要性
            importance = (impact * 0.6 + count / 20 * 0.4)  # 标准化重要性
            
            analysis[topic] = {
                "sentiment_score": sentiment,
                "impact_score": impact,
                "importance": min(importance, 1.0),
                "article_count": count,
                "trend": self._get_topic_trend(sentiment, impact)
            }
        
        return analysis
    
    def _analyze_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """分析关键事件"""
        if not events:
            return {"event_count": 0, "overall_impact": 0.5, "risk_level": "medium"}
        
        # 计算加权影响
        total_impact = 0.0
        total_weight = 0.0
        risk_events = 0
        
        for event in events:
            impact_score = 0.8 if event["impact"] == "positive" else 0.2
            confidence = event["confidence"]
            weight = confidence * impact_score
            
            total_impact += weight
            total_weight += confidence
            
            if event["impact"] == "negative":
                risk_events += 1
        
        overall_impact = total_impact / total_weight if total_weight > 0 else 0.5
        risk_level = "high" if risk_events > len(events) * 0.5 else "medium" if risk_events > 0 else "low"
        
        return {
            "event_count": len(events),
            "overall_impact": overall_impact,
            "risk_level": risk_level,
            "positive_events": len([e for e in events if e["impact"] == "positive"]),
            "negative_events": len([e for e in events if e["impact"] == "negative"]),
            "immediate_events": len([e for e in events if e["timeframe"] == "immediate"])
        }
    
    def _assess_news_impact(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """评估新闻影响"""
        # 基于情绪和事件分析评估影响
        sentiment = self._calculate_overall_sentiment(data)
        events = data["key_events"]
        
        # 计算短期影响
        short_term_impact = sentiment * 0.6
        if events:
            event_impact = sum(
                event["confidence"] * (0.8 if event["impact"] == "positive" else 0.2)
                for event in events
            ) / len(events)
            short_term_impact += event_impact * 0.4
        
        # 计算长期影响
        long_term_impact = sentiment * 0.4
        regulatory_events = [e for e in events if "regulatory" in e.get("title", "").lower()]
        if regulatory_events:
            long_term_impact -= 0.2  # 监管事件通常有长期负面影响
        
        return {
            "short_term_impact": min(short_term_impact, 1.0),
            "long_term_impact": min(long_term_impact, 1.0),
            "overall_impact": (short_term_impact + long_term_impact) / 2,
            "market_reaction": self._predict_market_reaction(sentiment, events),
            "volatility_impact": self._assess_volatility_impact(events)
        }
    
    def _generate_news_summary(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """生成新闻摘要"""
        sources = data["sources"]
        topics = data["topics"]
        events = data["key_events"]
        
        # 提取关键信息
        total_articles = sum(source["articles"] for source in sources.values())
        top_topics = sorted(topics.items(), key=lambda x: x[1]["count"], reverse=True)[:3]
        recent_events = [e for e in events if e["timeframe"] in ["immediate", "short_term"]]
        
        return {
            "total_articles": total_articles,
            "top_topics": [{"topic": topic, "count": data["count"]} for topic, data in top_topics],
            "recent_events": recent_events,
            "sentiment_summary": self._get_sentiment_summary(data["sentiment_breakdown"]),
            "key_headlines": self._extract_key_headlines(sources)
        }
    
    def _get_sentiment_level(self, sentiment: float) -> str:
        """获取情绪等级"""
        if sentiment > 0.7:
            return "very_positive"
        elif sentiment > 0.6:
            return "positive"
        elif sentiment > 0.4:
            return "neutral"
        elif sentiment > 0.3:
            return "negative"
        else:
            return "very_negative"
    
    def _get_activity_level(self, article_count: int) -> str:
        """获取活动等级"""
        if article_count > 20:
            return "high"
        elif article_count > 10:
            return "medium"
        else:
            return "low"
    
    def _get_reliability_score(self, source: str) -> float:
        """获取新闻源可靠性评分"""
        reliability_scores = {
            "reuters": 0.9,
            "bloomberg": 0.85,
            "cnbc": 0.8,
            "wsj": 0.9,
            "ft": 0.85
        }
        return reliability_scores.get(source, 0.7)
    
    def _get_topic_trend(self, sentiment: float, impact: float) -> str:
        """获取话题趋势"""
        if sentiment > 0.6 and impact > 0.7:
            return "bullish"
        elif sentiment < 0.4 and impact > 0.7:
            return "bearish"
        else:
            return "neutral"
    
    def _predict_market_reaction(self, sentiment: float, events: List[Dict[str, Any]]) -> str:
        """预测市场反应"""
        if sentiment > 0.7:
            return "positive"
        elif sentiment < 0.3:
            return "negative"
        else:
            # 考虑事件影响
            positive_events = len([e for e in events if e["impact"] == "positive"])
            negative_events = len([e for e in events if e["impact"] == "negative"])
            
            if positive_events > negative_events:
                return "positive"
            elif negative_events > positive_events:
                return "negative"
            else:
                return "neutral"
    
    def _assess_volatility_impact(self, events: List[Dict[str, Any]]) -> str:
        """评估波动率影响"""
        high_impact_events = [e for e in events if e.get("confidence", 0) > 0.8]
        regulatory_events = [e for e in events if "regulatory" in e.get("title", "").lower()]
        
        if len(high_impact_events) > 2 or len(regulatory_events) > 0:
            return "high"
        elif len(high_impact_events) > 0:
            return "medium"
        else:
            return "low"
    
    def _get_sentiment_summary(self, sentiment_breakdown: Dict[str, float]) -> str:
        """获取情绪摘要"""
        positive = sentiment_breakdown["very_positive"] + sentiment_breakdown["positive"]
        negative = sentiment_breakdown["very_negative"] + sentiment_breakdown["negative"]
        
        if positive > 0.6:
            return "新闻情绪整体积极"
        elif negative > 0.4:
            return "新闻情绪整体消极"
        else:
            return "新闻情绪相对中性"
    
    def _extract_key_headlines(self, sources: Dict[str, Any]) -> List[str]:
        """提取关键标题"""
        headlines = []
        for source_data in sources.values():
            headlines.extend(source_data.get("key_headlines", []))
        return headlines[:5]  # 返回前5个标题
    
    def _calculate_confidence(self, data: Dict[str, Any]) -> float:
        """计算分析置信度"""
        # 基于新闻源数量和文章数量计算置信度
        source_count = len(data["sources"])
        total_articles = sum(source["articles"] for source in data["sources"].values())
        
        source_score = min(source_count / 3, 1.0)
        article_score = min(total_articles / 50, 1.0)
        
        return (source_score * 0.4 + article_score * 0.6)
    
    def _generate_key_insights(self, data: Dict[str, Any], sentiment: float) -> List[str]:
        """生成关键洞察"""
        insights = []
        
        # 情绪洞察
        if sentiment > 0.7:
            insights.append("新闻情绪极度乐观，媒体对股票前景看好")
        elif sentiment < 0.3:
            insights.append("新闻情绪极度悲观，媒体对股票前景担忧")
        
        # 事件洞察
        events = data["key_events"]
        positive_events = [e for e in events if e["impact"] == "positive"]
        negative_events = [e for e in events if e["impact"] == "negative"]
        
        if len(positive_events) > len(negative_events):
            insights.append("正面事件占主导，有利于股价上涨")
        elif len(negative_events) > len(positive_events):
            insights.append("负面事件较多，可能对股价造成压力")
        
        # 监管洞察
        regulatory_events = [e for e in events if "regulatory" in e.get("title", "").lower()]
        if regulatory_events:
            insights.append("存在监管相关新闻，需要关注政策风险")
        
        return insights
    
    def _generate_news_recommendation(self, sentiment: float, impact: Dict[str, Any]) -> str:
        """生成新闻建议"""
        overall_impact = impact["overall_impact"]
        
        if sentiment > 0.7 and overall_impact > 0.7:
            return "新闻情绪积极且影响重大，建议关注后续发展"
        elif sentiment < 0.3 and overall_impact > 0.7:
            return "新闻情绪消极且影响重大，建议谨慎投资"
        elif sentiment > 0.6:
            return "新闻情绪相对积极，市场情绪支撑较强"
        elif sentiment < 0.4:
            return "新闻情绪相对消极，市场情绪压力较大"
        else:
            return "新闻情绪中性，市场情绪相对稳定"
