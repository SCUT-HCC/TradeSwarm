"""
Research Pipeline

实现双Agent迭代优化Workflow，包含数据等待、迭代分析和收敛判断逻辑。
"""

import asyncio
from typing import Dict, Any, List
from datetime import datetime

from .base_pipeline import AnalysisPipeline
from ..storage import PipelineOutput


class ResearchPipeline(AnalysisPipeline):
    """研究分析Pipeline"""
    
    def __init__(self, db_manager, session_id: str):
        """
        初始化Research Pipeline
        
        参数:
            db_manager: 数据库管理器
            session_id: 会话ID
        """
        super().__init__(
            db_manager=db_manager,
            session_id=session_id,
            pipeline_name="research_pipeline",
            required_inputs=["market_analysis", "social_analysis", "news_analysis", "fundamentals_analysis"]
        )
        self.max_iterations = 3
        self.convergence_threshold = 0.1
    
    def get_output_type(self) -> str:
        """获取输出类型"""
        return "research_report"
    
    async def analyze_data(self, inputs: Dict[str, PipelineOutput]) -> Dict[str, Any]:
        """
        执行研究分析（双Agent迭代优化）
        
        参数:
            inputs: 输入数据映射
            
        返回:
            Dict[str, Any]: 研究分析结果
        """
        self.logger.info("开始研究分析...")
        
        # 提取输入数据
        market_data = inputs["market_analysis"].data
        social_data = inputs["social_analysis"].data
        news_data = inputs["news_analysis"].data
        fundamentals_data = inputs["fundamentals_analysis"].data
        
        # 初始化迭代分析
        bullish_analysis = None
        bearish_analysis = None
        iteration = 0
        convergence_achieved = False
        
        self.logger.info("开始迭代分析...")
        
        while iteration < self.max_iterations and not convergence_achieved:
            iteration += 1
            self.logger.info(f"第{iteration}轮迭代分析...")
            
            # Bullish Agent分析
            bullish_analysis = await self._bullish_agent_analysis(
                market_data, social_data, news_data, fundamentals_data, 
                bearish_analysis, iteration
            )
            
            # Bearish Agent分析
            bearish_analysis = await self._bearish_agent_analysis(
                market_data, social_data, news_data, fundamentals_data,
                bullish_analysis, iteration
            )
            
            # 检查收敛性
            if iteration > 1:
                convergence_achieved = self._check_convergence(
                    bullish_analysis, bearish_analysis
                )
                
                if convergence_achieved:
                    self.logger.info(f"第{iteration}轮迭代后达到收敛")
                else:
                    self.logger.info(f"第{iteration}轮迭代未收敛，继续分析")
            
            # 短暂延迟，模拟Agent思考时间
            await asyncio.sleep(0.5)
        
        # 生成最终研究报告
        final_report = self._generate_final_report(
            bullish_analysis, bearish_analysis, iteration, convergence_achieved
        )
        
        self.logger.info("研究分析完成")
        return final_report
    
    async def _bullish_agent_analysis(
        self,
        market_data: Dict[str, Any],
        social_data: Dict[str, Any],
        news_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any],
        bearish_analysis: Dict[str, Any],
        iteration: int
    ) -> Dict[str, Any]:
        """Bullish Agent分析"""
        self.logger.info(f"Bullish Agent第{iteration}轮分析...")
        
        # 分析看涨信号
        bullish_signals = self._identify_bullish_signals(
            market_data, social_data, news_data, fundamentals_data
        )
        
        # 分析上涨潜力
        upside_potential = self._analyze_upside_potential(
            market_data, fundamentals_data, bullish_signals
        )
        
        # 分析支撑位
        support_levels = self._analyze_support_levels(market_data, fundamentals_data)
        
        # 分析催化剂
        catalysts = self._identify_catalysts(news_data, social_data, fundamentals_data)
        
        # 考虑Bearish Agent的反驳（如果有）
        counter_arguments = self._address_bearish_concerns(
            bearish_analysis, bullish_signals, iteration
        )
        
        # 计算看涨信心度
        confidence = self._calculate_bullish_confidence(
            bullish_signals, upside_potential, catalysts, counter_arguments
        )
        
        return {
            "iteration": iteration,
            "bullish_signals": bullish_signals,
            "upside_potential": upside_potential,
            "support_levels": support_levels,
            "catalysts": catalysts,
            "counter_arguments": counter_arguments,
            "confidence": confidence,
            "recommendation": self._generate_bullish_recommendation(confidence),
            "key_points": self._extract_bullish_key_points(
                bullish_signals, upside_potential, catalysts
            )
        }
    
    async def _bearish_agent_analysis(
        self,
        market_data: Dict[str, Any],
        social_data: Dict[str, Any],
        news_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any],
        bullish_analysis: Dict[str, Any],
        iteration: int
    ) -> Dict[str, Any]:
        """Bearish Agent分析"""
        self.logger.info(f"Bearish Agent第{iteration}轮分析...")
        
        # 分析看跌信号
        bearish_signals = self._identify_bearish_signals(
            market_data, social_data, news_data, fundamentals_data
        )
        
        # 分析下跌风险
        downside_risk = self._analyze_downside_risk(
            market_data, fundamentals_data, bearish_signals
        )
        
        # 分析阻力位
        resistance_levels = self._analyze_resistance_levels(market_data, fundamentals_data)
        
        # 分析风险因素
        risk_factors = self._identify_risk_factors(news_data, social_data, fundamentals_data)
        
        # 考虑Bullish Agent的观点（如果有）
        counter_arguments = self._address_bullish_optimism(
            bullish_analysis, bearish_signals, iteration
        )
        
        # 计算看跌信心度
        confidence = self._calculate_bearish_confidence(
            bearish_signals, downside_risk, risk_factors, counter_arguments
        )
        
        return {
            "iteration": iteration,
            "bearish_signals": bearish_signals,
            "downside_risk": downside_risk,
            "resistance_levels": resistance_levels,
            "risk_factors": risk_factors,
            "counter_arguments": counter_arguments,
            "confidence": confidence,
            "recommendation": self._generate_bearish_recommendation(confidence),
            "key_points": self._extract_bearish_key_points(
                bearish_signals, downside_risk, risk_factors
            )
        }
    
    def _identify_bullish_signals(
        self,
        market_data: Dict[str, Any],
        social_data: Dict[str, Any],
        news_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """识别看涨信号"""
        signals = []
        signal_strength = 0.0
        
        # 技术指标看涨信号
        if market_data.get("technical_analysis", {}).get("signal") == "buy":
            signals.append("技术指标显示买入信号")
            signal_strength += 0.3
        
        # 社交媒体情绪积极
        social_sentiment = social_data.get("overall_sentiment", {}).get("score", 0.5)
        if social_sentiment > 0.6:
            signals.append("社交媒体情绪积极")
            signal_strength += 0.2
        
        # 新闻情绪积极
        news_sentiment = news_data.get("overall_sentiment", {}).get("score", 0.5)
        if news_sentiment > 0.6:
            signals.append("新闻情绪积极")
            signal_strength += 0.2
        
        # 基本面强劲
        fundamentals_score = fundamentals_data.get("overall_score", 0.5)
        if fundamentals_score > 0.7:
            signals.append("基本面强劲")
            signal_strength += 0.3
        
        return {
            "signals": signals,
            "signal_strength": min(signal_strength, 1.0),
            "signal_count": len(signals)
        }
    
    def _identify_bearish_signals(
        self,
        market_data: Dict[str, Any],
        social_data: Dict[str, Any],
        news_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """识别看跌信号"""
        signals = []
        signal_strength = 0.0
        
        # 技术指标看跌信号
        if market_data.get("technical_analysis", {}).get("signal") == "sell":
            signals.append("技术指标显示卖出信号")
            signal_strength += 0.3
        
        # 社交媒体情绪消极
        social_sentiment = social_data.get("overall_sentiment", {}).get("score", 0.5)
        if social_sentiment < 0.4:
            signals.append("社交媒体情绪消极")
            signal_strength += 0.2
        
        # 新闻情绪消极
        news_sentiment = news_data.get("overall_sentiment", {}).get("score", 0.5)
        if news_sentiment < 0.4:
            signals.append("新闻情绪消极")
            signal_strength += 0.2
        
        # 基本面偏弱
        fundamentals_score = fundamentals_data.get("overall_score", 0.5)
        if fundamentals_score < 0.4:
            signals.append("基本面偏弱")
            signal_strength += 0.3
        
        return {
            "signals": signals,
            "signal_strength": min(signal_strength, 1.0),
            "signal_count": len(signals)
        }
    
    def _analyze_upside_potential(
        self,
        market_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any],
        bullish_signals: Dict[str, Any]
    ) -> Dict[str, Any]:
        """分析上涨潜力"""
        # 基于技术分析
        technical_score = market_data.get("overall_score", 0.5)
        
        # 基于基本面
        fundamentals_score = fundamentals_data.get("overall_score", 0.5)
        
        # 基于分析师目标价
        analyst_target = fundamentals_data.get("analyst_analysis", {}).get("upside_potential", 0.1)
        
        # 综合上涨潜力
        upside_potential = (
            technical_score * 0.3 + 
            fundamentals_score * 0.4 + 
            analyst_target * 0.3
        )
        
        return {
            "technical_upside": technical_score,
            "fundamentals_upside": fundamentals_score,
            "analyst_upside": analyst_target,
            "overall_upside": upside_potential,
            "upside_level": "high" if upside_potential > 0.7 else "medium" if upside_potential > 0.5 else "low"
        }
    
    def _analyze_downside_risk(
        self,
        market_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any],
        bearish_signals: Dict[str, Any]
    ) -> Dict[str, Any]:
        """分析下跌风险"""
        # 基于技术分析
        technical_score = 1 - market_data.get("overall_score", 0.5)
        
        # 基于基本面
        fundamentals_score = 1 - fundamentals_data.get("overall_score", 0.5)
        
        # 基于风险因素
        risk_factors = fundamentals_data.get("key_risks", [])
        risk_score = min(len(risk_factors) / 5, 1.0)
        
        # 综合下跌风险
        downside_risk = (
            technical_score * 0.3 + 
            fundamentals_score * 0.4 + 
            risk_score * 0.3
        )
        
        return {
            "technical_risk": technical_score,
            "fundamentals_risk": fundamentals_score,
            "risk_factors": risk_score,
            "overall_risk": downside_risk,
            "risk_level": "high" if downside_risk > 0.7 else "medium" if downside_risk > 0.5 else "low"
        }
    
    def _analyze_support_levels(
        self,
        market_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """分析支撑位"""
        support_analysis = market_data.get("support_resistance_analysis", {})
        support_levels = support_analysis.get("nearest_support", 0)
        
        # 基于基本面估值
        valuation = fundamentals_data.get("valuation_analysis", {})
        fair_value = support_levels * 1.1  # 假设公允价值比支撑位高10%
        
        return {
            "technical_support": support_levels,
            "fundamental_support": fair_value,
            "support_strength": "strong" if support_levels > 0 else "weak"
        }
    
    def _analyze_resistance_levels(
        self,
        market_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """分析阻力位"""
        resistance_analysis = market_data.get("support_resistance_analysis", {})
        resistance_levels = resistance_analysis.get("nearest_resistance", float('inf'))
        
        # 基于分析师目标价
        analyst_target = fundamentals_data.get("analyst_analysis", {}).get("target_price", 0)
        
        return {
            "technical_resistance": resistance_levels,
            "analyst_resistance": analyst_target,
            "resistance_strength": "strong" if resistance_levels < float('inf') else "weak"
        }
    
    def _identify_catalysts(
        self,
        news_data: Dict[str, Any],
        social_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """识别催化剂"""
        catalysts = []
        
        # 新闻催化剂
        key_events = news_data.get("key_events", [])
        for event in key_events:
            if event.get("impact") == "positive":
                catalysts.append({
                    "type": "news",
                    "description": event.get("title", ""),
                    "impact": "positive",
                    "timeframe": event.get("timeframe", "unknown")
                })
        
        # 社交媒体催化剂
        trending_topics = social_data.get("trending_analysis", {}).get("top_topics", [])
        for topic in trending_topics[:2]:  # 前2个热门话题
            if topic.get("sentiment", 0.5) > 0.6:
                catalysts.append({
                    "type": "social",
                    "description": f"热门话题: {topic.get('tag', '')}",
                    "impact": "positive",
                    "timeframe": "short_term"
                })
        
        # 基本面催化剂
        strengths = fundamentals_data.get("key_strengths", [])
        for strength in strengths[:2]:  # 前2个优势
            catalysts.append({
                "type": "fundamental",
                "description": strength,
                "impact": "positive",
                "timeframe": "long_term"
            })
        
        return catalysts
    
    def _identify_risk_factors(
        self,
        news_data: Dict[str, Any],
        social_data: Dict[str, Any],
        fundamentals_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """识别风险因素"""
        risk_factors = []
        
        # 新闻风险因素
        key_events = news_data.get("key_events", [])
        for event in key_events:
            if event.get("impact") == "negative":
                risk_factors.append({
                    "type": "news",
                    "description": event.get("title", ""),
                    "impact": "negative",
                    "timeframe": event.get("timeframe", "unknown")
                })
        
        # 社交媒体风险因素
        social_sentiment = social_data.get("overall_sentiment", {}).get("score", 0.5)
        if social_sentiment < 0.4:
            risk_factors.append({
                "type": "social",
                "description": "社交媒体情绪消极",
                "impact": "negative",
                "timeframe": "short_term"
            })
        
        # 基本面风险因素
        risks = fundamentals_data.get("key_risks", [])
        for risk in risks:
            risk_factors.append({
                "type": "fundamental",
                "description": risk,
                "impact": "negative",
                "timeframe": "medium_term"
            })
        
        return risk_factors
    
    def _address_bearish_concerns(
        self,
        bearish_analysis: Dict[str, Any],
        bullish_signals: Dict[str, Any],
        iteration: int
    ) -> Dict[str, Any]:
        """回应看跌担忧"""
        if not bearish_analysis or iteration == 1:
            return {"addressed": False, "response": "无看跌担忧需要回应"}
        
        bearish_signals = bearish_analysis.get("bearish_signals", {}).get("signals", [])
        counter_arguments = []
        
        for signal in bearish_signals:
            if "技术指标" in signal:
                counter_arguments.append("技术指标可能滞后，基本面支撑仍然强劲")
            elif "社交媒体" in signal:
                counter_arguments.append("社交媒体情绪波动较大，长期趋势更重要")
            elif "新闻" in signal:
                counter_arguments.append("负面新闻影响通常是短期的")
            elif "基本面" in signal:
                counter_arguments.append("基本面指标需要综合评估，单一指标可能误导")
        
        return {
            "addressed": len(counter_arguments) > 0,
            "response": "已回应看跌担忧",
            "counter_arguments": counter_arguments
        }
    
    def _address_bullish_optimism(
        self,
        bullish_analysis: Dict[str, Any],
        bearish_signals: Dict[str, Any],
        iteration: int
    ) -> Dict[str, Any]:
        """回应看涨乐观"""
        if not bullish_analysis or iteration == 1:
            return {"addressed": False, "response": "无看涨乐观需要回应"}
        
        bullish_signals = bullish_analysis.get("bullish_signals", {}).get("signals", [])
        counter_arguments = []
        
        for signal in bullish_signals:
            if "技术指标" in signal:
                counter_arguments.append("技术指标可能过于乐观，需要谨慎对待")
            elif "社交媒体" in signal:
                counter_arguments.append("社交媒体情绪可能过于狂热，存在泡沫风险")
            elif "新闻" in signal:
                counter_arguments.append("正面新闻可能已经充分反映在价格中")
            elif "基本面" in signal:
                counter_arguments.append("基本面优势可能被高估")
        
        return {
            "addressed": len(counter_arguments) > 0,
            "response": "已回应看涨乐观",
            "counter_arguments": counter_arguments
        }
    
    def _calculate_bullish_confidence(
        self,
        bullish_signals: Dict[str, Any],
        upside_potential: Dict[str, Any],
        catalysts: List[Dict[str, Any]],
        counter_arguments: Dict[str, Any]
    ) -> float:
        """计算看涨信心度"""
        signal_strength = bullish_signals.get("signal_strength", 0.0)
        upside_score = upside_potential.get("overall_upside", 0.0)
        catalyst_score = min(len(catalysts) / 3, 1.0)
        
        # 考虑反驳意见的影响
        counter_impact = 0.1 if counter_arguments.get("addressed", False) else 0.0
        
        confidence = (signal_strength * 0.4 + upside_score * 0.4 + catalyst_score * 0.2) - counter_impact
        
        return max(0.0, min(1.0, confidence))
    
    def _calculate_bearish_confidence(
        self,
        bearish_signals: Dict[str, Any],
        downside_risk: Dict[str, Any],
        risk_factors: List[Dict[str, Any]],
        counter_arguments: Dict[str, Any]
    ) -> float:
        """计算看跌信心度"""
        signal_strength = bearish_signals.get("signal_strength", 0.0)
        risk_score = downside_risk.get("overall_risk", 0.0)
        risk_factor_score = min(len(risk_factors) / 3, 1.0)
        
        # 考虑反驳意见的影响
        counter_impact = 0.1 if counter_arguments.get("addressed", False) else 0.0
        
        confidence = (signal_strength * 0.4 + risk_score * 0.4 + risk_factor_score * 0.2) - counter_impact
        
        return max(0.0, min(1.0, confidence))
    
    def _check_convergence(
        self,
        bullish_analysis: Dict[str, Any],
        bearish_analysis: Dict[str, Any]
    ) -> bool:
        """检查收敛性"""
        if not bullish_analysis or not bearish_analysis:
            return False
        
        bullish_confidence = bullish_analysis.get("confidence", 0.0)
        bearish_confidence = bearish_analysis.get("confidence", 0.0)
        
        # 计算信心度差异
        confidence_diff = abs(bullish_confidence - bearish_confidence)
        
        # 如果差异小于阈值，认为收敛
        return confidence_diff < self.convergence_threshold
    
    def _generate_final_report(
        self,
        bullish_analysis: Dict[str, Any],
        bearish_analysis: Dict[str, Any],
        iteration: int,
        convergence_achieved: bool
    ) -> Dict[str, Any]:
        """生成最终研究报告"""
        if not bullish_analysis or not bearish_analysis:
            return {"error": "分析数据不完整"}
        
        bullish_confidence = bullish_analysis.get("confidence", 0.0)
        bearish_confidence = bearish_analysis.get("confidence", 0.0)
        
        # 确定最终建议
        if bullish_confidence > bearish_confidence + 0.2:
            final_recommendation = "买入"
            confidence_level = bullish_confidence
        elif bearish_confidence > bullish_confidence + 0.2:
            final_recommendation = "卖出"
            confidence_level = bearish_confidence
        else:
            final_recommendation = "持有"
            confidence_level = (bullish_confidence + bearish_confidence) / 2
        
        return {
            "timestamp": datetime.now().isoformat(),
            "iteration_count": iteration,
            "convergence_achieved": convergence_achieved,
            "bullish_analysis": bullish_analysis,
            "bearish_analysis": bearish_analysis,
            "final_recommendation": final_recommendation,
            "confidence_level": confidence_level,
            "consensus": self._generate_consensus(bullish_analysis, bearish_analysis),
            "key_insights": self._generate_key_insights(bullish_analysis, bearish_analysis),
            "risk_assessment": self._generate_risk_assessment(bullish_analysis, bearish_analysis)
        }
    
    def _generate_consensus(
        self,
        bullish_analysis: Dict[str, Any],
        bearish_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """生成共识观点"""
        bullish_confidence = bullish_analysis.get("confidence", 0.0)
        bearish_confidence = bearish_analysis.get("confidence", 0.0)
        
        if bullish_confidence > bearish_confidence:
            consensus = "看涨"
            consensus_strength = bullish_confidence
        elif bearish_confidence > bullish_confidence:
            consensus = "看跌"
            consensus_strength = bearish_confidence
        else:
            consensus = "中性"
            consensus_strength = 0.5
        
        return {
            "consensus": consensus,
            "consensus_strength": consensus_strength,
            "disagreement_level": abs(bullish_confidence - bearish_confidence)
        }
    
    def _generate_key_insights(
        self,
        bullish_analysis: Dict[str, Any],
        bearish_analysis: Dict[str, Any]
    ) -> List[str]:
        """生成关键洞察"""
        insights = []
        
        # 看涨洞察
        bullish_signals = bullish_analysis.get("bullish_signals", {}).get("signals", [])
        if bullish_signals:
            insights.append(f"看涨信号: {', '.join(bullish_signals[:2])}")
        
        # 看跌洞察
        bearish_signals = bearish_analysis.get("bearish_signals", {}).get("signals", [])
        if bearish_signals:
            insights.append(f"看跌信号: {', '.join(bearish_signals[:2])}")
        
        # 催化剂洞察
        catalysts = bullish_analysis.get("catalysts", [])
        if catalysts:
            insights.append(f"关键催化剂: {catalysts[0].get('description', '')}")
        
        # 风险洞察
        risk_factors = bearish_analysis.get("risk_factors", [])
        if risk_factors:
            insights.append(f"主要风险: {risk_factors[0].get('description', '')}")
        
        return insights
    
    def _generate_risk_assessment(
        self,
        bullish_analysis: Dict[str, Any],
        bearish_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """生成风险评估"""
        bullish_confidence = bullish_analysis.get("confidence", 0.0)
        bearish_confidence = bearish_analysis.get("confidence", 0.0)
        
        # 风险等级
        if bearish_confidence > 0.7:
            risk_level = "高"
        elif bearish_confidence > 0.5:
            risk_level = "中"
        else:
            risk_level = "低"
        
        return {
            "risk_level": risk_level,
            "bullish_confidence": bullish_confidence,
            "bearish_confidence": bearish_confidence,
            "risk_balance": bearish_confidence - bullish_confidence,
            "recommendation": "谨慎投资" if bearish_confidence > 0.6 else "可考虑投资"
        }
    
    def _generate_bullish_recommendation(self, confidence: float) -> str:
        """生成看涨建议"""
        if confidence > 0.8:
            return "强烈买入"
        elif confidence > 0.6:
            return "买入"
        elif confidence > 0.4:
            return "谨慎买入"
        else:
            return "观望"
    
    def _generate_bearish_recommendation(self, confidence: float) -> str:
        """生成看跌建议"""
        if confidence > 0.8:
            return "强烈卖出"
        elif confidence > 0.6:
            return "卖出"
        elif confidence > 0.4:
            return "谨慎卖出"
        else:
            return "观望"
    
    def _extract_bullish_key_points(
        self,
        bullish_signals: Dict[str, Any],
        upside_potential: Dict[str, Any],
        catalysts: List[Dict[str, Any]]
    ) -> List[str]:
        """提取看涨关键点"""
        points = []
        
        if bullish_signals.get("signal_strength", 0) > 0.5:
            points.append("技术指标和基本面信号积极")
        
        if upside_potential.get("overall_upside", 0) > 0.6:
            points.append("上涨潜力较大")
        
        if catalysts:
            points.append(f"存在{len(catalysts)}个催化剂")
        
        return points
    
    def _extract_bearish_key_points(
        self,
        bearish_signals: Dict[str, Any],
        downside_risk: Dict[str, Any],
        risk_factors: List[Dict[str, Any]]
    ) -> List[str]:
        """提取看跌关键点"""
        points = []
        
        if bearish_signals.get("signal_strength", 0) > 0.5:
            points.append("技术指标和基本面信号消极")
        
        if downside_risk.get("overall_risk", 0) > 0.6:
            points.append("下跌风险较大")
        
        if risk_factors:
            points.append(f"存在{len(risk_factors)}个风险因素")
        
        return points
