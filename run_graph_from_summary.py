# -*- coding: utf-8 -*-
"""
从 Summary 节点开始运行完整 Graph 流程

功能：
1. 从 test.db 读取 Analyst 报告
2. 从 Summary 节点开始运行完整 Graph
3. 保存每个 Agent 的中间输出
"""

import json
import sys
from pathlib import Path
from datetime import date
from typing import Any, Dict

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from tradingagents.graph.trading_graph import create_trading_graph
from tradingagents.graph.utils import load_llm_from_config
from tradingagents.agents.utils.memory_db_helper import MemoryDBHelper
from tradingagents.agents.utils.agentstate.agent_states import AgentState
from typing import Any, List, Dict


class DatabaseMemory:
    """
    从数据库读取历史经验的 Memory 类
    
    从 SQLite 数据库中查询历史分析师报告，提取关键信息作为记忆。
    """
    
    def __init__(self, db_path: str, symbol: str, limit: int = 10):
        """
        初始化 DatabaseMemory
        
        Args:
            db_path: 数据库文件路径
            symbol: 股票代码
            limit: 查询的历史报告数量限制
        """
        self.db_helper = MemoryDBHelper(db_path)
        self.symbol = symbol
        self.limit = limit
    
    def get_memories(self, current_situation: str, n_matches: int = 2) -> List[Dict[str, Any]]:
        """
        从数据库查询历史报告，提取相似情况和建议
        
        Args:
            current_situation: 当前情境描述
            n_matches: 返回的匹配数量
            
        Returns:
            包含历史记忆的列表，格式: [{"matched_situation": ..., "recommendation": ..., "similarity_score": ...}]
        """
        try:
            # 直接查询数据库获取历史报告内容
            # 查询所有类型的历史报告，按日期倒序排列
            conn = self.db_helper._get_connection()
            cursor = conn.cursor()
            
            sql = """
            SELECT analyst_type, trade_date, report_content
            FROM analyst_reports
            WHERE symbol = ?
                AND report_content IS NOT NULL
                AND report_content != ''
            ORDER BY trade_date DESC, created_at DESC
            LIMIT ?
            """
            
            cursor.execute(sql, (self.symbol, self.limit))
            results = cursor.fetchall()
            cursor.close()
            
            if not results:
                return []
            
            # 从历史报告中提取关键信息
            memories = []
            for row in results[:n_matches]:
                analyst_type = row[0]
                trade_date = row[1]
                report_content = row[2]
                
                # 简单提取：使用报告的前200个字符作为情况描述
                situation = report_content[:200] + "..." if len(report_content) > 200 else report_content
                
                # 尝试从报告中提取建议（如果有的话）
                # 这里简化处理，实际可以解析报告内容提取建议
                recommendation = "基于历史数据分析，建议谨慎操作，关注市场变化。"
                
                memories.append({
                    "matched_situation": situation,
                    "recommendation": recommendation,
                    "similarity_score": 0.7,  # 简化处理，实际可以使用相似度计算
                })
            
            return memories
            
        except Exception as e:
            print(f"[WARN] 从数据库读取记忆失败: {e}")
            return []
    
    def close(self) -> None:
        """关闭数据库连接"""
        self.db_helper.close()


def save_node_output(
    node_name: str,
    state: Dict[str, Any],
    output_dir: Path
) -> None:
    """
    保存节点输出到文件
    
    Args:
        node_name: 节点名称
        state: 节点执行后的状态
        output_dir: 输出目录
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 保存 JSON 格式
    json_file = output_dir / f"{node_name}_output.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, default=str)
    
    # 保存文本格式（简化版）
    txt_file = output_dir / f"{node_name}_output.txt"
    with open(txt_file, "w", encoding="utf-8") as f:
        f.write(f"节点: {node_name}\n")
        f.write(f"时间: {date.today().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*80 + "\n\n")
        
        # 提取关键字段（完整内容，不截断）
        if "messages" in state:
            messages = state["messages"]
            if messages:
                last_msg = messages[-1]
                if hasattr(last_msg, "content"):
                    f.write("最后一条消息:\n")
                    f.write(str(last_msg.content) + "\n\n")
        
        # 保存各个 summary（完整内容，不截断）
        for key in ["market_analyst_summary", "news_analyst_summary", 
                    "sentiment_analyst_summary", "fundamentals_analyst_summary"]:
            if key in state and state[key]:
                summary = state[key]
                f.write(f"\n{key}:\n")
                if isinstance(summary, dict):
                    if "today_report" in summary:
                        f.write(f"今日报告: {str(summary['today_report'])}\n\n")
                    if "history_report" in summary:
                        f.write(f"历史报告: {str(summary['history_report'])}\n\n")
        
        # 保存 research_summary 的完整内容（包括 raw output）
        if "research_summary" in state and state["research_summary"]:
            f.write(f"\nresearch_summary:\n")
            f.write("="*80 + "\n")
            research_summary = state["research_summary"]
            if isinstance(research_summary, dict):
                # 保存 investment_debate_state 的完整内容（包括所有 raw output）
                if "investment_debate_state" in research_summary:
                    debate_state = research_summary["investment_debate_state"]
                    f.write(f"\ninvestment_debate_state (完整辩论历史):\n")
                    f.write("-"*80 + "\n")
                    if isinstance(debate_state, dict):
                        # 保存所有字段，包括 raw_response, current_response, history, bull_history, bear_history 等
                        for k, v in debate_state.items():
                            f.write(f"\n{k}:\n")
                            f.write("-"*40 + "\n")
                            if isinstance(v, str):
                                # 如果是 JSON 字符串，尝试格式化
                                try:
                                    parsed = json.loads(v)
                                    f.write(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n")
                                except:
                                    # 如果不是 JSON，直接写入（可能是多行文本）
                                    f.write(str(v) + "\n")
                            else:
                                f.write(str(v) + "\n")
                # 保存其他字段（包括 raw_response, investment_plan 等）
                for k, v in research_summary.items():
                    if k != "investment_debate_state":
                        f.write(f"\n{k}:\n")
                        f.write("-"*80 + "\n")
                        if isinstance(v, str):
                            try:
                                parsed = json.loads(v)
                                f.write(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n")
                            except:
                                f.write(str(v) + "\n")
                        else:
                            f.write(str(v) + "\n")
        
        # 保存 risk_subgraph 的完整内容（包括 raw output）
        if "risk_summary" in state and state["risk_summary"]:
            f.write(f"\nrisk_summary (完整风险辩论历史):\n")
            f.write("="*80 + "\n")
            risk_summary = state["risk_summary"]
            if isinstance(risk_summary, dict):
                # 保存所有字段，包括 raw_response, risk_debate_state 等
                for k, v in risk_summary.items():
                    f.write(f"\n{k}:\n")
                    f.write("-"*80 + "\n")
                    if isinstance(v, str):
                        try:
                            parsed = json.loads(v)
                            f.write(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n")
                        except:
                            # 如果不是 JSON，直接写入（可能是多行文本）
                            f.write(str(v) + "\n")
                    else:
                        f.write(str(v) + "\n")
        
        # 保存 trader 的输出（完整内容）
        if "trader_investment_plan" in state and state["trader_investment_plan"]:
            f.write(f"\ntrader_investment_plan:\n")
            f.write("="*80 + "\n")
            trader_output = state["trader_investment_plan"]
            if isinstance(trader_output, str):
                try:
                    parsed = json.loads(trader_output)
                    f.write(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n")
                except:
                    f.write(str(trader_output) + "\n")
            else:
                f.write(str(trader_output) + "\n")
        
        # 保存 strategy_selection 的输出（完整内容）
        if "strategy_selection" in state and state["strategy_selection"]:
            f.write(f"\nstrategy_selection:\n")
            f.write("="*80 + "\n")
            strategy_selection = state["strategy_selection"]
            if isinstance(strategy_selection, dict):
                for k, v in strategy_selection.items():
                    f.write(f"\n{k}:\n")
                    f.write("-"*40 + "\n")
                    if isinstance(v, str):
                        try:
                            parsed = json.loads(v)
                            f.write(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n")
                        except:
                            f.write(str(v) + "\n")
                    else:
                        f.write(str(v) + "\n")
            else:
                f.write(str(strategy_selection) + "\n")
        
        # 保存其他关键字段（完整内容，不截断）
        for key in ["research_result", "investment_plan", "final_trade_decision", 
                    "trading_strategy", "trading_strategy_status"]:
            if key in state and state[key]:
                f.write(f"\n{key}:\n")
                f.write("="*80 + "\n")
                value = state[key]
                if isinstance(value, str):
                    # 如果是 JSON 字符串，尝试格式化
                    try:
                        parsed = json.loads(value)
                        f.write(json.dumps(parsed, ensure_ascii=False, indent=2) + "\n")
                    except:
                        f.write(str(value) + "\n")
                else:
                    f.write(str(value) + "\n")


def run_full_graph(
    symbol: str,
    trade_date: str,
    llm: Any,
    memory: Any,
    db_path: str = "test.db",
    output_dir: str = "graph_outputs"
) -> Dict[str, Any]:
    """
    运行完整的 Graph 流程（从 Summary 节点开始）
    
    Args:
        symbol: 股票代码
        trade_date: 交易日期
        llm: LLM 实例
        memory: Memory 实例
        db_path: 数据库路径
        output_dir: 输出目录
        
    Returns:
        最终状态
    """
    print(f"\n{'='*80}")
    print(f"运行完整 Graph 流程（从 Summary 节点开始）")
    print(f"{'='*80}\n")
    
    # 检查数据库是否存在
    if not Path(db_path).exists():
        print(f"[ERROR] 数据库 {db_path} 不存在！")
        print(f"  请先运行 run_analysts_to_db.py 生成 Analyst 报告")
        raise FileNotFoundError(f"数据库不存在: {db_path}")
    
    # 创建输出目录
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # 初始化数据库连接
    db_helper = MemoryDBHelper(db_path)
    
    # 创建 Graph
    print("[构建] 创建交易决策图...")
    graph = create_trading_graph(llm, memory, db_helper)
    print("[OK] Graph 创建成功")
    
    # 准备初始状态
    initial_state: AgentState = {
        "company_of_interest": symbol,
        "trade_date": trade_date,
        "trading_session": "pre_open",
        "messages": [],
    }
    
    # 运行 Graph
    print(f"\n[运行] 开始执行 Graph...")
    print(f"  股票代码: {symbol}")
    print(f"  交易日期: {trade_date}")
    print(f"  交易时段: pre_open")
    print(f"  数据库: {db_path}\n")
    
    executed_nodes = set()
    current_state = initial_state
    
    try:
        # 使用 stream_mode="updates" 来捕获每个节点的更新
        # 注意：子图内部节点（如 bull_researcher, bear_researcher）不会单独触发事件
        # 但它们的输出会保存在子图的 state 中（如 research_summary.investment_debate_state）
        for event in graph.stream(initial_state, stream_mode="updates"):
            for node_name, node_state in event.items():
                if node_name not in executed_nodes:
                    executed_nodes.add(node_name)
                    print(f"  [执行] {node_name}...")
                    
                    # 更新当前状态
                    current_state.update(node_state)
                    
                    # 保存节点输出（包括子图内部节点的原始输出，它们保存在 state 中）
                    save_node_output(node_name, node_state, output_path)
                    print(f"    [OK] 输出已保存到 {output_path / f'{node_name}_output.json'}")
        
        # 获取最终完整状态
        print(f"\n[获取] 最终状态...")
        final_state = None
        for state in graph.stream(initial_state, stream_mode="values"):
            final_state = state
        
        if final_state:
            current_state.update(final_state)
            save_node_output("final_state", final_state, output_path)
            print(f"  [OK] 最终状态已保存")
        
    except Exception as e:
        print(f"\n[ERROR] Graph 执行失败: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        db_helper.close()
    
    print(f"\n[完成] Graph 执行完成")
    print(f"  执行节点数: {len(executed_nodes)}")
    print(f"  输出目录: {output_path.absolute()}")
    
    return current_state


def main():
    """主函数"""
    print("="*80)
    print("从 Summary 节点开始运行完整 Graph 流程")
    print("="*80)
    
    # 配置参数
    symbol = "AAPL"
    trade_date = "2026-02-05"  # 使用指定的交易日期
    db_path = "demo_data.db"  # 使用 demo_data.db
    output_dir = "graph_outputs"
    
    # 删除旧的输出目录
    if Path(output_dir).exists():
        print(f"\n[清理] 删除旧的输出目录: {output_dir}")
        import shutil
        shutil.rmtree(output_dir)
    
    try:
        # 加载 LLM
        print(f"\n[初始化] 加载 LLM...")
        llm = load_llm_from_config()
        print("[OK] LLM 加载成功")
        
        # 创建 Memory（从数据库读取）
        memory = DatabaseMemory(db_path=db_path, symbol=symbol)
        
        # 运行完整 Graph
        final_state = run_full_graph(
            symbol=symbol,
            trade_date=trade_date,
            llm=llm,
            memory=memory,
            db_path=db_path,
            output_dir=output_dir
        )
        
        # 打印摘要
        print(f"\n{'='*80}")
        print("执行摘要")
        print(f"{'='*80}")
        print(f"股票代码: {symbol}")
        print(f"交易日期: {trade_date}")
        print(f"数据库: {db_path}")
        print(f"输出目录: {output_dir}")
        print(f"\n[完成] Graph 执行完成！")
        
        # 关闭 Memory 数据库连接
        if 'memory' in locals():
            memory.close()
        
    except Exception as e:
        print(f"\n[ERROR] 执行失败: {e}")
        import traceback
        traceback.print_exc()
        # 确保关闭数据库连接
        if 'memory' in locals():
            memory.close()
        sys.exit(1)


if __name__ == "__main__":
    main()

