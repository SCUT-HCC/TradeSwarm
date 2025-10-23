"""
TradeSwarm系统测试脚本

测试数据库并发安全和Pipeline解耦机制
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


async def test_database_manager():
    """测试数据库管理器"""
    print("=" * 60)
    print("测试数据库管理器")
    print("=" * 60)
    
    try:
        # 初始化数据库
        await db_manager.initialize()
        print("✅ 数据库初始化成功")
        
        # 创建会话
        session_id = await db_manager.create_session()
        print(f"✅ 创建会话成功: {session_id}")
        
        # 测试保存Pipeline输出
        from core.storage import PipelineOutput
        
        test_output = PipelineOutput(
            session_id=session_id,
            pipeline_name="test_pipeline",
            output_type="test_analysis",
            data={"test": "data", "timestamp": datetime.now().isoformat()}
        )
        
        await db_manager.save_pipeline_output(test_output)
        print("✅ 保存Pipeline输出成功")
        
        # 测试获取Pipeline输出
        retrieved_output = await db_manager.get_pipeline_output(
            session_id=session_id,
            output_type="test_analysis",
            timeout=5.0
        )
        
        if retrieved_output:
            print("✅ 获取Pipeline输出成功")
            print(f"   数据: {retrieved_output.data}")
        else:
            print("❌ 获取Pipeline输出失败")
        
        # 完成会话
        await db_manager.complete_session(session_id)
        print("✅ 完成会话成功")
        
        return True
        
    except Exception as e:
        print(f"❌ 数据库管理器测试失败: {e}")
        return False


async def test_single_pipeline():
    """测试单个Pipeline"""
    print("=" * 60)
    print("测试单个Pipeline")
    print("=" * 60)
    
    try:
        # 创建会话
        session_id = await db_manager.create_session()
        print(f"会话ID: {session_id}")
        
        # 测试Market Pipeline
        print("测试Market Pipeline...")
        market_pipeline = MarketPipeline(db_manager, session_id)
        market_result = await market_pipeline.run()
        
        if market_result:
            print("✅ Market Pipeline执行成功")
            print(f"   输出类型: {market_pipeline.get_output_type()}")
        else:
            print("❌ Market Pipeline执行失败")
            return False
        
        # 验证输出是否保存到数据库
        market_output = await db_manager.get_pipeline_output(
            session_id=session_id,
            output_type="market_analysis",
            timeout=5.0
        )
        
        if market_output:
            print("✅ Market Pipeline输出已保存到数据库")
        else:
            print("❌ Market Pipeline输出未保存到数据库")
            return False
        
        # 完成会话
        await db_manager.complete_session(session_id)
        print("✅ 单个Pipeline测试完成")
        
        return True
        
    except Exception as e:
        print(f"❌ 单个Pipeline测试失败: {e}")
        return False


async def test_pipeline_dependencies():
    """测试Pipeline依赖关系"""
    print("=" * 60)
    print("测试Pipeline依赖关系")
    print("=" * 60)
    
    try:
        # 创建会话
        session_id = await db_manager.create_session()
        print(f"会话ID: {session_id}")
        
        # 先运行数据采集Pipeline
        print("运行数据采集Pipeline...")
        data_collection_results = await asyncio.gather(
            MarketPipeline(db_manager, session_id).run(),
            SocialPipeline(db_manager, session_id).run(),
            NewsPipeline(db_manager, session_id).run(),
            FundamentalsPipeline(db_manager, session_id).run(),
            return_exceptions=True
        )
        
        data_pipeline_names = ["Market", "Social", "News", "Fundamentals"]
        for name, result in zip(data_pipeline_names, data_collection_results):
            if isinstance(result, Exception):
                print(f"❌ {name} Pipeline执行失败: {result}")
                return False
            else:
                print(f"✅ {name} Pipeline执行成功")
        
        # 等待数据就绪
        print("等待数据就绪...")
        await asyncio.sleep(1.0)
        
        # 运行Research Pipeline（依赖数据采集Pipeline的输出）
        print("运行Research Pipeline...")
        research_pipeline = ResearchPipeline(db_manager, session_id)
        research_result = await research_pipeline.run()
        
        if research_result:
            print("✅ Research Pipeline执行成功")
        else:
            print("❌ Research Pipeline执行失败")
            return False
        
        # 运行Trading Pipeline（依赖Research Pipeline的输出）
        print("运行Trading Pipeline...")
        trading_pipeline = TradingPipeline(db_manager, session_id)
        trading_result = await trading_pipeline.run()
        
        if trading_result:
            print("✅ Trading Pipeline执行成功")
        else:
            print("❌ Trading Pipeline执行失败")
            return False
        
        # 完成会话
        await db_manager.complete_session(session_id)
        print("✅ Pipeline依赖关系测试完成")
        
        return True
        
    except Exception as e:
        print(f"❌ Pipeline依赖关系测试失败: {e}")
        return False


async def test_concurrent_pipelines():
    """测试并发Pipeline执行"""
    print("=" * 60)
    print("测试并发Pipeline执行")
    print("=" * 60)
    
    try:
        # 创建会话
        session_id = await db_manager.create_session()
        print(f"会话ID: {session_id}")
        
        # 并发执行所有Pipeline
        print("并发执行所有Pipeline...")
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
        
        # 检查结果
        pipeline_names = ["Market", "Social", "News", "Fundamentals", "Research", "Trading"]
        success_count = 0
        
        for name, result in zip(pipeline_names, results):
            if isinstance(result, Exception):
                print(f"❌ {name} Pipeline执行失败: {result}")
            else:
                print(f"✅ {name} Pipeline执行成功")
                success_count += 1
        
        print(f"执行时间: {duration:.2f}秒")
        print(f"成功率: {success_count}/{len(pipeline_names)}")
        
        # 完成会话
        await db_manager.complete_session(session_id)
        
        return success_count == len(pipeline_names)
        
    except Exception as e:
        print(f"❌ 并发Pipeline测试失败: {e}")
        return False


async def main():
    """主测试函数"""
    print("🧪 TradeSwarm系统测试")
    print("=" * 80)
    print()
    
    # 配置日志
    logging.basicConfig(
        level=logging.WARNING,  # 减少日志输出
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    test_results = []
    
    # 测试1: 数据库管理器
    print("测试1: 数据库管理器")
    result1 = await test_database_manager()
    test_results.append(("数据库管理器", result1))
    print()
    
    # 测试2: 单个Pipeline
    print("测试2: 单个Pipeline")
    result2 = await test_single_pipeline()
    test_results.append(("单个Pipeline", result2))
    print()
    
    # 测试3: Pipeline依赖关系
    print("测试3: Pipeline依赖关系")
    result3 = await test_pipeline_dependencies()
    test_results.append(("Pipeline依赖关系", result3))
    print()
    
    # 测试4: 并发Pipeline执行
    print("测试4: 并发Pipeline执行")
    result4 = await test_concurrent_pipelines()
    test_results.append(("并发Pipeline执行", result4))
    print()
    
    # 关闭数据库管理器
    await db_manager.close()
    
    # 显示测试结果
    print("=" * 80)
    print("测试结果汇总")
    print("=" * 80)
    
    passed = 0
    total = len(test_results)
    
    for test_name, result in test_results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print()
    print(f"总计: {passed}/{total} 测试通过")
    
    if passed == total:
        print("🎉 所有测试通过！系统运行正常。")
    else:
        print("⚠️  部分测试失败，请检查系统配置。")


if __name__ == "__main__":
    asyncio.run(main())
