"""
Trading Pipeline

实现三阶段决策Workflow，包含数据等待和顺序决策流程。
"""

import asyncio
from typing import Dict, Any, List
from datetime import datetime

from .base_pipeline import AnalysisPipeline
from ..storage import PipelineOutput


class TradingPipeline(AnalysisPipeline):
    """交易决策Pipeline"""
    
    def __init__(self, db_manager, session_id: str):
        """
        初始化Trading Pipeline
        
        参数:
            db_manager: 数据库管理器
            session_id: 会话ID
        """
        super().__init__(
            db_manager=db_manager,
            session_id=session_id,
            pipeline_name="trading_pipeline",
            required_inputs=["research_report"]
        )
    
    def get_output_type(self) -> str:
        """获取输出类型"""
        return "trading_decision"
    
    async def analyze_data(self, inputs: Dict[str, PipelineOutput]) -> Dict[str, Any]:
        """
        执行交易决策分析（三阶段决策流程）
        
        参数:
            inputs: 输入数据映射
            
        返回:
            Dict[str, Any]: 交易决策结果
        """
        self.logger.info("开始交易决策分析...")
        
        # 提取研究分析数据
        research_data = inputs["research_report"].data
        
        # 三阶段决策流程
        self.logger.info("第一阶段：Trader Agent制定交易策略...")
        trader_decision = await self._trader_agent_analysis(research_data)
        
        self.logger.info("第二阶段：Risk Officer Agent评估风险...")
        risk_assessment = await self._risk_officer_analysis(research_data, trader_decision)
        
        self.logger.info("第三阶段：Manager Agent最终决策...")
        final_decision = await self._manager_agent_analysis(
            research_data, trader_decision, risk_assessment
        )
        
        self.logger.info("交易决策分析完成")
        return final_decision
    
    async def _trader_agent_analysis(self, research_data: Dict[str, Any]) -> Dict[str, Any]:
        """Trader Agent分析"""
        self.logger.info("Trader Agent分析中...")
        
        # 分析研究建议
        recommendation = research_data.get("final_recommendation", "持有")
        confidence = research_data.get("confidence_level", 0.5)
        
        # 制定交易策略
        trading_strategy = self._develop_trading_strategy(research_data)
        
        # 生成交易指令草案
        trade_instructions = self._generate_trade_instructions(
            recommendation, confidence, trading_strategy
        )
        
        # 计算预期收益
        expected_return = self._calculate_expected_return(research_data, trading_strategy)
        
        # 确定仓位建议
        position_suggestion = self._determine_position_size(
            confidence, expected_return, research_data
        )
        
        return {
            "timestamp": datetime.now().isoformat(),
            "agent": "trader",
            "recommendation": recommendation,
            "confidence": confidence,
            "trading_strategy": trading_strategy,
            "trade_instructions": trade_instructions,
            "expected_return": expected_return,
            "position_suggestion": position_suggestion,
            "key_considerations": self._extract_trader_considerations(research_data)
        }
    
    async def _risk_officer_analysis(
        self,
        research_data: Dict[str, Any],
        trader_decision: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Risk Officer Agent分析"""
        self.logger.info("Risk Officer Agent分析中...")
        
        # 评估风险敞口
        risk_exposure = self._assess_risk_exposure(trader_decision, research_data)
        
        # 资金管理建议
        capital_management = self._evaluate_capital_management(trader_decision, research_data)
        
        # 止损止盈方案
        stop_loss_profit = self._design_stop_loss_profit(trader_decision, research_data)
        
        # 风险评估
        risk_assessment = self._comprehensive_risk_assessment(
            research_data, trader_decision, risk_exposure
        )
        
        # 风险控制建议
        risk_controls = self._recommend_risk_controls(risk_assessment, trader_decision)
        
        return {
            "timestamp": datetime.now().isoformat(),
            "agent": "risk_officer",
            "risk_exposure": risk_exposure,
            "capital_management": capital_management,
            "stop_loss_profit": stop_loss_profit,
            "risk_assessment": risk_assessment,
            "risk_controls": risk_controls,
            "risk_recommendation": self._generate_risk_recommendation(risk_assessment),
            "key_risks": self._identify_key_risks(research_data, trader_decision)
        }
    
    async def _manager_agent_analysis(
        self,
        research_data: Dict[str, Any],
        trader_decision: Dict[str, Any],
        risk_assessment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Manager Agent最终决策"""
        self.logger.info("Manager Agent最终决策中...")
        
        # 整合所有意见
        integrated_analysis = self._integrate_all_opinions(
            research_data, trader_decision, risk_assessment
        )
        
        # 做出最终决策
        final_decision = self._make_final_decision(integrated_analysis)
        
        # 生成执行计划
        execution_plan = self._create_execution_plan(final_decision, risk_assessment)
        
        # 设置监控指标
        monitoring_metrics = self._setup_monitoring_metrics(final_decision, risk_assessment)
        
        return {
            "timestamp": datetime.now().isoformat(),
            "agent": "manager",
            "final_decision": final_decision,
            "execution_plan": execution_plan,
            "monitoring_metrics": monitoring_metrics,
            "decision_rationale": self._generate_decision_rationale(
                research_data, trader_decision, risk_assessment, final_decision
            ),
            "success_metrics": self._define_success_metrics(final_decision),
            "contingency_plans": self._create_contingency_plans(final_decision, risk_assessment)
        }
    
    def _develop_trading_strategy(self, research_data: Dict[str, Any]) -> Dict[str, Any]:
        """制定交易策略"""
        recommendation = research_data.get("final_recommendation", "持有")
        confidence = research_data.get("confidence_level", 0.5)
        
        # 基于建议确定策略类型
        if recommendation == "买入":
            strategy_type = "long_position"
            strategy_description = "建立多头仓位"
        elif recommendation == "卖出":
            strategy_type = "short_position"
            strategy_description = "建立空头仓位"
        else:
            strategy_type = "hold_position"
            strategy_description = "维持当前仓位"
        
        # 确定策略参数
        strategy_params = {
            "strategy_type": strategy_type,
            "strategy_description": strategy_description,
            "confidence_level": confidence,
            "time_horizon": self._determine_time_horizon(confidence),
            "entry_conditions": self._define_entry_conditions(research_data),
            "exit_conditions": self._define_exit_conditions(research_data)
        }
        
        return strategy_params
    
    def _generate_trade_instructions(
        self,
        recommendation: str,
        confidence: float,
        strategy: Dict[str, Any]
    ) -> Dict[str, Any]:
        """生成交易指令"""
        if recommendation == "买入":
            action = "BUY"
            quantity = self._calculate_trade_quantity(confidence, "buy")
        elif recommendation == "卖出":
            action = "SELL"
            quantity = self._calculate_trade_quantity(confidence, "sell")
        else:
            action = "HOLD"
            quantity = 0
        
        return {
            "action": action,
            "quantity": quantity,
            "order_type": "MARKET" if confidence > 0.7 else "LIMIT",
            "urgency": "HIGH" if confidence > 0.8 else "MEDIUM" if confidence > 0.6 else "LOW",
            "time_in_force": "DAY",
            "strategy_reference": strategy.get("strategy_type", "unknown")
        }
    
    def _calculate_expected_return(self, research_data: Dict[str, Any], strategy: Dict[str, Any]) -> Dict[str, Any]:
        """计算预期收益"""
        # 基于研究分析的信心度
        confidence = research_data.get("confidence_level", 0.5)
        
        # 基于历史波动率估算收益范围
        base_return = 0.05  # 基础预期收益5%
        confidence_multiplier = confidence * 2  # 信心度越高，预期收益越高
        
        expected_return = base_return * confidence_multiplier
        
        return {
            "expected_return": expected_return,
            "return_range": {
                "optimistic": expected_return * 1.5,
                "realistic": expected_return,
                "pessimistic": expected_return * 0.5
            },
            "probability": confidence,
            "time_horizon": strategy.get("time_horizon", "medium_term")
        }
    
    def _determine_position_size(
        self,
        confidence: float,
        expected_return: Dict[str, Any],
        research_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """确定仓位大小"""
        # 基于信心度确定仓位比例
        base_position = 0.1  # 基础仓位10%
        confidence_multiplier = confidence * 2  # 信心度越高，仓位越大
        
        position_size = min(base_position * confidence_multiplier, 0.3)  # 最大仓位30%
        
        return {
            "position_size": position_size,
            "position_type": "long" if research_data.get("final_recommendation") == "买入" else "short",
            "max_position": 0.3,
            "risk_tolerance": "high" if confidence > 0.7 else "medium" if confidence > 0.5 else "low"
        }
    
    def _assess_risk_exposure(
        self,
        trader_decision: Dict[str, Any],
        research_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """评估风险敞口"""
        position_size = trader_decision.get("position_suggestion", {}).get("position_size", 0)
        confidence = trader_decision.get("confidence", 0.5)
        
        # 计算风险敞口
        risk_exposure = position_size * (1 - confidence)  # 信心度越低，风险敞口越大
        
        return {
            "risk_exposure": risk_exposure,
            "exposure_level": "high" if risk_exposure > 0.2 else "medium" if risk_exposure > 0.1 else "low",
            "max_loss_potential": risk_exposure * 0.2,  # 假设最大损失20%
            "risk_factors": self._identify_risk_factors(research_data)
        }
    
    def _evaluate_capital_management(
        self,
        trader_decision: Dict[str, Any],
        research_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """评估资金管理"""
        position_size = trader_decision.get("position_suggestion", {}).get("position_size", 0)
        
        # 资金分配建议
        capital_allocation = {
            "trading_capital": position_size,
            "reserve_capital": 1 - position_size,
            "risk_capital": position_size * 0.1  # 风险资本为交易资本的10%
        }
        
        return {
            "capital_allocation": capital_allocation,
            "leverage_ratio": 1.0,  # 无杠杆
            "margin_requirements": position_size * 0.1,  # 保证金要求
            "liquidity_requirements": position_size * 0.05  # 流动性要求
        }
    
    def _design_stop_loss_profit(
        self,
        trader_decision: Dict[str, Any],
        research_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """设计止损止盈方案"""
        confidence = trader_decision.get("confidence", 0.5)
        expected_return = trader_decision.get("expected_return", {}).get("expected_return", 0.05)
        
        # 止损点设置
        stop_loss = expected_return * 0.5  # 止损点为预期收益的50%
        
        # 止盈点设置
        take_profit = expected_return * 1.5  # 止盈点为预期收益的150%
        
        return {
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "stop_loss_percentage": stop_loss * 100,
            "take_profit_percentage": take_profit * 100,
            "risk_reward_ratio": take_profit / stop_loss if stop_loss > 0 else 0
        }
    
    def _comprehensive_risk_assessment(
        self,
        research_data: Dict[str, Any],
        trader_decision: Dict[str, Any],
        risk_exposure: Dict[str, Any]
    ) -> Dict[str, Any]:
        """综合风险评估"""
        # 市场风险
        market_risk = self._assess_market_risk(research_data)
        
        # 流动性风险
        liquidity_risk = self._assess_liquidity_risk(trader_decision)
        
        # 操作风险
        operational_risk = self._assess_operational_risk(trader_decision)
        
        # 综合风险评分
        overall_risk = (market_risk + liquidity_risk + operational_risk) / 3
        
        return {
            "market_risk": market_risk,
            "liquidity_risk": liquidity_risk,
            "operational_risk": operational_risk,
            "overall_risk": overall_risk,
            "risk_level": "high" if overall_risk > 0.7 else "medium" if overall_risk > 0.4 else "low",
            "risk_tolerance": self._determine_risk_tolerance(overall_risk)
        }
    
    def _recommend_risk_controls(
        self,
        risk_assessment: Dict[str, Any],
        trader_decision: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """推荐风险控制措施"""
        controls = []
        
        overall_risk = risk_assessment.get("overall_risk", 0.5)
        
        if overall_risk > 0.7:
            controls.append({
                "type": "position_limit",
                "description": "限制仓位大小",
                "implementation": "将仓位限制在10%以下"
            })
            controls.append({
                "type": "stop_loss",
                "description": "设置严格止损",
                "implementation": "止损点设置为5%"
            })
        elif overall_risk > 0.4:
            controls.append({
                "type": "monitoring",
                "description": "加强监控",
                "implementation": "每日监控仓位变化"
            })
        
        return controls
    
    def _integrate_all_opinions(
        self,
        research_data: Dict[str, Any],
        trader_decision: Dict[str, Any],
        risk_assessment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """整合所有意见"""
        # 研究分析权重
        research_weight = 0.4
        trader_weight = 0.3
        risk_weight = 0.3
        
        # 综合评分
        research_score = research_data.get("confidence_level", 0.5)
        trader_score = trader_decision.get("confidence", 0.5)
        risk_score = 1 - risk_assessment.get("overall_risk", 0.5)  # 风险越低，评分越高
        
        integrated_score = (
            research_score * research_weight +
            trader_score * trader_weight +
            risk_score * risk_weight
        )
        
        return {
            "integrated_score": integrated_score,
            "research_contribution": research_score * research_weight,
            "trader_contribution": trader_score * trader_weight,
            "risk_contribution": risk_score * risk_weight,
            "consensus": self._determine_consensus(research_score, trader_score, risk_score)
        }
    
    def _make_final_decision(self, integrated_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """做出最终决策"""
        integrated_score = integrated_analysis.get("integrated_score", 0.5)
        
        if integrated_score > 0.7:
            decision = "执行交易"
            urgency = "HIGH"
        elif integrated_score > 0.5:
            decision = "谨慎执行"
            urgency = "MEDIUM"
        else:
            decision = "暂缓执行"
            urgency = "LOW"
        
        return {
            "decision": decision,
            "urgency": urgency,
            "confidence": integrated_score,
            "execution_priority": "immediate" if urgency == "HIGH" else "scheduled",
            "approval_required": integrated_score < 0.6
        }
    
    def _create_execution_plan(
        self,
        final_decision: Dict[str, Any],
        risk_assessment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """创建执行计划"""
        if final_decision.get("decision") == "执行交易":
            execution_plan = {
                "phase": "immediate",
                "steps": [
                    "确认市场条件",
                    "设置止损止盈",
                    "执行交易指令",
                    "监控仓位变化"
                ],
                "timeline": "立即执行",
                "success_criteria": "交易成功执行且风险可控"
            }
        elif final_decision.get("decision") == "谨慎执行":
            execution_plan = {
                "phase": "scheduled",
                "steps": [
                    "等待更好时机",
                    "重新评估市场条件",
                    "调整交易参数",
                    "分阶段执行"
                ],
                "timeline": "24小时内",
                "success_criteria": "在风险可控前提下完成交易"
            }
        else:
            execution_plan = {
                "phase": "cancelled",
                "steps": [
                    "取消交易计划",
                    "重新分析市场",
                    "等待新信号"
                ],
                "timeline": "无",
                "success_criteria": "避免不必要的风险"
            }
        
        return execution_plan
    
    def _setup_monitoring_metrics(
        self,
        final_decision: Dict[str, Any],
        risk_assessment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """设置监控指标"""
        return {
            "price_monitoring": {
                "frequency": "real_time",
                "alerts": ["价格突破止损点", "价格达到止盈点"]
            },
            "risk_monitoring": {
                "frequency": "daily",
                "metrics": ["风险敞口", "最大回撤", "夏普比率"]
            },
            "performance_monitoring": {
                "frequency": "weekly",
                "metrics": ["收益率", "胜率", "风险调整收益"]
            }
        }
    
    def _generate_decision_rationale(
        self,
        research_data: Dict[str, Any],
        trader_decision: Dict[str, Any],
        risk_assessment: Dict[str, Any],
        final_decision: Dict[str, Any]
    ) -> str:
        """生成决策理由"""
        rationale_parts = []
        
        # 研究分析理由
        research_recommendation = research_data.get("final_recommendation", "持有")
        rationale_parts.append(f"研究分析建议: {research_recommendation}")
        
        # 交易策略理由
        trader_confidence = trader_decision.get("confidence", 0.5)
        rationale_parts.append(f"交易策略信心度: {trader_confidence:.2f}")
        
        # 风险评估理由
        risk_level = risk_assessment.get("risk_level", "medium")
        rationale_parts.append(f"风险等级: {risk_level}")
        
        # 最终决策理由
        decision = final_decision.get("decision", "暂缓执行")
        rationale_parts.append(f"最终决策: {decision}")
        
        return " | ".join(rationale_parts)
    
    def _define_success_metrics(self, final_decision: Dict[str, Any]) -> Dict[str, Any]:
        """定义成功指标"""
        return {
            "primary_metrics": [
                "交易执行成功率",
                "风险控制有效性",
                "收益目标达成率"
            ],
            "secondary_metrics": [
                "决策准确性",
                "风险调整收益",
                "最大回撤控制"
            ],
            "success_thresholds": {
                "execution_success_rate": 0.9,
                "risk_control_effectiveness": 0.8,
                "return_target_achievement": 0.7
            }
        }
    
    def _create_contingency_plans(
        self,
        final_decision: Dict[str, Any],
        risk_assessment: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """创建应急计划"""
        plans = []
        
        # 市场风险应急计划
        if risk_assessment.get("market_risk", 0) > 0.7:
            plans.append({
                "scenario": "市场大幅波动",
                "action": "立即平仓",
                "trigger": "价格波动超过5%",
                "execution": "自动执行"
            })
        
        # 流动性风险应急计划
        if risk_assessment.get("liquidity_risk", 0) > 0.7:
            plans.append({
                "scenario": "流动性不足",
                "action": "分批平仓",
                "trigger": "成交量下降50%",
                "execution": "手动执行"
            })
        
        return plans
    
    # 辅助方法
    def _determine_time_horizon(self, confidence: float) -> str:
        """确定时间范围"""
        if confidence > 0.8:
            return "long_term"
        elif confidence > 0.6:
            return "medium_term"
        else:
            return "short_term"
    
    def _define_entry_conditions(self, research_data: Dict[str, Any]) -> List[str]:
        """定义入场条件"""
        return [
            "技术指标确认",
            "基本面支撑",
            "市场情绪配合"
        ]
    
    def _define_exit_conditions(self, research_data: Dict[str, Any]) -> List[str]:
        """定义出场条件"""
        return [
            "达到止盈目标",
            "触发止损条件",
            "基本面恶化"
        ]
    
    def _calculate_trade_quantity(self, confidence: float, action: str) -> float:
        """计算交易数量"""
        base_quantity = 1000  # 基础数量
        confidence_multiplier = confidence * 2
        return int(base_quantity * confidence_multiplier)
    
    def _assess_market_risk(self, research_data: Dict[str, Any]) -> float:
        """评估市场风险"""
        # 基于研究分析的风险指标
        risk_assessment = research_data.get("risk_assessment", {})
        return risk_assessment.get("bearish_confidence", 0.5)
    
    def _assess_liquidity_risk(self, trader_decision: Dict[str, Any]) -> float:
        """评估流动性风险"""
        # 基于仓位大小的流动性风险
        position_size = trader_decision.get("position_suggestion", {}).get("position_size", 0)
        return min(position_size * 2, 1.0)
    
    def _assess_operational_risk(self, trader_decision: Dict[str, Any]) -> float:
        """评估操作风险"""
        # 基于交易复杂度的操作风险
        confidence = trader_decision.get("confidence", 0.5)
        return 1 - confidence
    
    def _determine_risk_tolerance(self, overall_risk: float) -> str:
        """确定风险承受度"""
        if overall_risk > 0.7:
            return "low"
        elif overall_risk > 0.4:
            return "medium"
        else:
            return "high"
    
    def _determine_consensus(self, research_score: float, trader_score: float, risk_score: float) -> str:
        """确定共识"""
        scores = [research_score, trader_score, risk_score]
        avg_score = sum(scores) / len(scores)
        
        if avg_score > 0.7:
            return "strong_consensus"
        elif avg_score > 0.5:
            return "moderate_consensus"
        else:
            return "weak_consensus"
    
    def _identify_risk_factors(self, research_data: Dict[str, Any]) -> List[str]:
        """识别风险因素"""
        risk_assessment = research_data.get("risk_assessment", {})
        key_risks = risk_assessment.get("key_risks", [])
        return key_risks[:3]  # 返回前3个风险因素
    
    def _extract_trader_considerations(self, research_data: Dict[str, Any]) -> List[str]:
        """提取交易员考虑因素"""
        return [
            "市场趋势分析",
            "技术指标信号",
            "基本面支撑",
            "风险收益比"
        ]
    
    def _generate_risk_recommendation(self, risk_assessment: Dict[str, Any]) -> str:
        """生成风险建议"""
        overall_risk = risk_assessment.get("overall_risk", 0.5)
        
        if overall_risk > 0.7:
            return "风险过高，建议暂缓交易"
        elif overall_risk > 0.4:
            return "风险中等，建议谨慎交易"
        else:
            return "风险可控，可以执行交易"
