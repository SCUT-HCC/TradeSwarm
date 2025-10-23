"""
TradeSwarm主程序入口

实现完整的Pipeline并行执行框架
"""

import asyncio
import logging
from datetime import datetime

from core.storage import db_manager
from core.pipelines import (
    MarketPipeline,
    SocialPipeline, 
    NewsPipeline,
    FundamentalsPipeline,
    ResearchPipeline,
    TradingPipeline
)


async def main():
    """
    主函数：执行6个Pipeline并行运行
    
    实现完整的TradeSwarm系统：
    - 6个Pipeline完全并行运行
    - Pipeline间通过SQLite数据库解耦通信
    - Pipeline内部是顺序Workflow（Agent串行执行）
    """
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    print("=" * 80)
    print("TradeSwarm - 多智能体量化交易系统")
    print("=" * 80)
    print()
    print("系统架构:")
    print("  - 6个Pipeline完全并行运行")
    print("  - Pipeline间通过SQLite数据库解耦通信")
    print("  - Pipeline内部是顺序Workflow（Agent串行执行）")
    print()
    print("=" * 80)
    print()
    
    try:
        # 初始化数据库管理器
        logger.info("初始化数据库管理器...")
        await db_manager.initialize()
        
        # 创建会话
        session_id = await db_manager.create_session()
        logger.info(f"创建会话: {session_id}")
        
        print(f"会话ID: {session_id}")
        print("启动所有Pipeline...")
        print()
        
        # 启动6个Pipeline并行执行
        start_time = datetime.now()
        
        results = await asyncio.gather(
            MarketPipeline(db_manager, session_id).run(),
            SocialPipeline(db_manager, session_id).run(),
            NewsPipeline(db_manager, session_id).run(),
            FundamentalsPipeline(db_manager, session_id).run(),
            ResearchPipeline(db_manager, session_id).run(),
            TradingPipeline(db_manager, session_id).run(),
            return_exceptions=True
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print()
        print("=" * 80)
        print("所有Pipeline执行完成")
        print("=" * 80)
        print()
        
        # 显示执行结果
        pipeline_names = [
            "Market Pipeline",
            "Social Pipeline", 
            "News Pipeline",
            "Fundamentals Pipeline",
            "Research Pipeline",
            "Trading Pipeline"
        ]
        
        print("执行结果:")
        for i, (name, result) in enumerate(zip(pipeline_names, results)):
            if isinstance(result, Exception):
                print(f"  ❌ {name}: 执行失败 - {result}")
            else:
                print(f"  ✅ {name}: 执行成功")
        
        print()
        print(f"总执行时间: {duration:.2f}秒")
        print()
        
        # 显示最终交易决策
        try:
            trading_output = await db_manager.get_pipeline_output(
                session_id=session_id,
                output_type="trading_decision",
                timeout=5.0
            )
            
            if trading_output:
                final_decision = trading_output.data.get("final_decision", {})
                decision = final_decision.get("decision", "未知")
                confidence = final_decision.get("confidence", 0.0)
                
                print("=" * 80)
                print("最终交易决策")
                print("=" * 80)
                print(f"决策: {decision}")
                print(f"信心度: {confidence:.2f}")
                print("=" * 80)
            else:
                print("⚠️  未获取到最终交易决策")
                
        except Exception as e:
            logger.warning(f"获取交易决策失败: {e}")
        
        # 完成会话
        await db_manager.complete_session(session_id)
        logger.info(f"会话 {session_id} 已完成")
        
    except Exception as e:
        logger.error(f"系统执行失败: {e}")
        print(f"❌ 系统执行失败: {e}")
        
    finally:
        # 关闭数据库管理器
        await db_manager.close()
        logger.info("数据库管理器已关闭")


if __name__ == "__main__":
    print("\n🚀 启动TradeSwarm系统\n")
    asyncio.run(main())
