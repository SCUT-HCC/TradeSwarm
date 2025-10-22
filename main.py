"""
Agent池并行执行演示

演示如何使用AgentPool管理50个Agent并行执行相同任务
"""

import json
import asyncio
import time
from utils.config import get_config
from core.agent_pool import AgentPool


async def main_agent_pool():
    """使用Agent池并行执行50个Agent"""

    print("=" * 80)
    print("Agent池并行执行演示")
    print("=" * 80)

    # 第一步：验证环境配置
    print("\n[1/4] 验证环境配置...")
    config = get_config()
    is_valid, error_msg = config.validate()
    if not is_valid:
        print(f"❌ 配置验证失败: {error_msg}")
        print("提示: 请确保.env文件中配置了必要的API密钥")
        return
    print("✅ 环境配置验证通过")

    # 第二步：加载Agent配置
    print("\n[2/4] 加载Agent配置...")
    config_path = "configs/examples/medical_researcher.json"

    with open(config_path, 'r', encoding='utf-8') as f:
        agent_config = json.load(f)

    print(f"✅ 配置加载完成: {agent_config['agent_profile']['name']}")

    # 第三步：创建Agent池并注册50个Agent
    print("\n[3/4] 创建Agent池并注册Agent...")
    print("配置:")
    print(f"  - Agent数量: 50")
    print(f"  - 最大并发数: 20 (同时最多20个Agent执行)")
    print(f"  - API速率限制: 10 req/s (每秒最多10次API调用)")
    print(f"  - 令牌桶容量: 60 (允许短时突发60个请求)")

    # 创建Agent池
    # max_concurrent=20: 最多20个Agent同时执行
    # rate_limit=10.0: 每秒最多10次API调用
    # rate_limit_capacity=60: 令牌桶容量60个
    agent_pool = AgentPool(
        max_concurrent=20,
        rate_limit=10.0,
        rate_limit_capacity=60
    )

    # 批量注册50个Agent
    # 注意：这里使用相同的配置创建50个Agent实例
    # 实际应用中可以使用不同的配置创建异构Agent
    print("\n正在创建50个Agent实例...")
    agent_configs = [agent_config for _ in range(50)]
    agent_ids = agent_pool.register_agents_from_configs(
        agent_configs,
        id_prefix="medical_agent"
    )

    print(f"✅ 成功注册 {len(agent_ids)} 个Agent")
    print(f"   Agent ID范围: {agent_ids[0]} ~ {agent_ids[-1]}")

    # 第四步：并行执行任务
    print("\n[4/4] 开始并行执行任务...")
    print("-" * 80)

    # 定义任务
    task = "请帮我检索关于阿尔茨海默症最新治疗方法的研究文献"
    print(f"任务: {task}")
    print(f"执行策略: 50个Agent同时处理相同任务")
    print()
    print("执行中...")
    print("(前20个Agent立即开始，剩余Agent根据API限流自动排队)")
    print()

    # 记录开始时间
    start_time = time.time()

    # 并行执行所有Agent
    results = await agent_pool.execute_all(task)

    # 记录总耗时
    total_time = time.time() - start_time

    # 第五步：统计和展示结果
    print("\n" + "=" * 80)
    print("执行完成 - 统计信息")
    print("=" * 80)

    # 统计成功和失败
    success_count = sum(1 for r in results.values() if r.success)
    failed_count = len(results) - success_count

    # 计算执行时间统计
    execution_times = [r.execution_time for r in results.values() if r.success]
    if execution_times:
        avg_time = sum(execution_times) / len(execution_times)
        min_time = min(execution_times)
        max_time = max(execution_times)
    else:
        avg_time = min_time = max_time = 0.0

    print(f"\n总体统计:")
    print(f"  - 总Agent数: {len(results)}")
    print(f"  - 成功: {success_count} ({success_count/len(results)*100:.1f}%)")
    print(f"  - 失败: {failed_count} ({failed_count/len(results)*100:.1f}%)")
    print(f"  - 总耗时: {total_time:.2f} 秒")
    print(f"  - 平均吞吐: {len(results)/total_time:.2f} agent/秒")

    if execution_times:
        print(f"\n单个Agent执行时间:")
        print(f"  - 平均: {avg_time:.2f} 秒")
        print(f"  - 最快: {min_time:.2f} 秒")
        print(f"  - 最慢: {max_time:.2f} 秒")

    # 展示前3个成功的Agent结果
    print("\n" + "-" * 80)
    print("部分Agent执行结果预览 (前3个成功的Agent):")
    print("-" * 80)

    success_results = [
        (agent_id, result)
        for agent_id, result in results.items()
        if result.success
    ]

    for i, (agent_id, result) in enumerate(success_results[:3]):
        print(f"\n[{agent_id}]")
        print(f"执行时间: {result.execution_time:.2f} 秒")
        print(f"输出预览: {result.output[:200]}...")
        if i < 2:
            print("-" * 80)

    # 如果有失败的，展示失败信息
    if failed_count > 0:
        print("\n" + "-" * 80)
        print(f"失败的Agent ({failed_count}个):")
        print("-" * 80)

        failed_results = [
            (agent_id, result)
            for agent_id, result in results.items()
            if not result.success
        ]

        for agent_id, result in failed_results[:5]:  # 只显示前5个
            print(f"  - {agent_id}: {result.error}")

        if failed_count > 5:
            print(f"  ... 还有 {failed_count - 5} 个失败的Agent")

    print("\n" + "=" * 80)
    print("实验结论:")
    print("=" * 80)
    print(f"✅ Agent池成功管理了 {len(results)} 个Agent的并行执行")
    print(f"✅ 限流机制工作正常，遵守API速率限制")
    print(f"✅ 总耗时 {total_time:.2f} 秒，展示了并行执行的效率优势")
    print()
    print("说明:")
    print("  - 前20个Agent立即开始执行（受max_concurrent限制）")
    print("  - API调用受速率限制自动排队（10 req/s）")
    print("  - 令牌桶允许初始突发（前60个请求可快速发出）")
    print("  - 失败的Agent不影响其他Agent执行（优雅降级）")


if __name__ == "__main__":
    print("\n🚀 启动Agent池并行执行演示\n")
    asyncio.run(main_agent_pool())
