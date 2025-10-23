"""
DatabaseManager数据库管理器

实现异步SQLite数据库管理，支持Pipeline间解耦通信。
采用写队列模式避免并发写冲突，支持WAL模式提升并发性能。
"""

import asyncio
import aiosqlite
import json
import uuid
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import logging

from .schema import DatabaseSchema, PipelineOutput, Session


class DatabaseManager:
    """数据库管理器（单例模式）"""
    
    _instance: Optional["DatabaseManager"] = None
    _initialized: bool = False
    
    def __new__(cls) -> "DatabaseManager":
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, db_path: str = "tradeswarm.db"):
        """
        初始化数据库管理器
        
        参数:
            db_path: 数据库文件路径
        """
        if not self._initialized:
            self.db_path = db_path
            self.write_queue: asyncio.Queue = asyncio.Queue()
            self.write_worker_task: Optional[asyncio.Task] = None
            self.logger = logging.getLogger(__name__)
            self._initialized = True
    
    async def initialize(self) -> None:
        """
        初始化数据库连接和表结构
        
        注意:
            必须在首次使用前调用此方法
        """
        if self.write_worker_task is None:
            # 启动写队列工作协程
            self.write_worker_task = asyncio.create_task(self._write_worker())
            
            # 创建数据库表和索引
            async with aiosqlite.connect(self.db_path) as db:
                # 启用WAL模式
                await db.execute("PRAGMA journal_mode=WAL")
                await db.execute("PRAGMA synchronous=NORMAL")
                await db.execute("PRAGMA cache_size=10000")
                await db.execute("PRAGMA temp_store=MEMORY")
                
                # 创建表
                for table_name, create_sql in DatabaseSchema.get_create_tables_sql().items():
                    await db.execute(create_sql)
                    self.logger.info(f"创建表: {table_name}")
                
                # 创建索引
                for index_sql in DatabaseSchema.get_create_indexes_sql():
                    await db.execute(index_sql)
                
                await db.commit()
                self.logger.info("数据库初始化完成")
    
    async def _write_worker(self) -> None:
        """
        写队列工作协程
        
        处理所有数据库写操作，避免并发写冲突
        """
        while True:
            try:
                # 从队列获取写操作
                write_operation = await self.write_queue.get()
                
                if write_operation is None:  # 停止信号
                    break
                
                # 执行写操作
                async with aiosqlite.connect(self.db_path) as db:
                    await write_operation(db)
                    await db.commit()
                
                self.write_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"写队列工作协程错误: {e}")
                await asyncio.sleep(1)  # 错误后等待1秒再继续
    
    async def create_session(self, session_id: Optional[str] = None) -> str:
        """
        创建新会话
        
        参数:
            session_id: 可选的会话ID，如果为None则自动生成
            
        返回:
            str: 会话ID
        """
        if session_id is None:
            session_id = str(uuid.uuid4())
        
        session = Session(session_id=session_id)
        
        # 通过写队列添加会话
        async def write_session(db: aiosqlite.Connection):
            await db.execute(
                f"INSERT INTO {DatabaseSchema.SESSIONS_TABLE} (session_id, status, created_at) VALUES (?, ?, ?)",
                (session.session_id, session.status, session.created_at)
            )
        
        await self.write_queue.put(write_session)
        self.logger.info(f"创建会话: {session_id}")
        
        return session_id
    
    async def save_pipeline_output(self, output: PipelineOutput) -> None:
        """
        保存Pipeline输出到数据库
        
        参数:
            output: Pipeline输出对象
        """
        # 通过写队列保存输出
        async def write_output(db: aiosqlite.Connection):
            await db.execute(
                f"INSERT INTO {DatabaseSchema.PIPELINE_OUTPUTS_TABLE} "
                f"(session_id, pipeline_name, output_type, data, status, created_at) "
                f"VALUES (?, ?, ?, ?, ?, ?)",
                (
                    output.session_id,
                    output.pipeline_name,
                    output.output_type,
                    json.dumps(output.data, ensure_ascii=False),
                    output.status,
                    output.created_at
                )
            )
        
        await self.write_queue.put(write_output)
        self.logger.info(f"保存Pipeline输出: {output.pipeline_name} -> {output.output_type}")
    
    async def get_pipeline_output(
        self,
        session_id: str,
        output_type: str,
        timeout: float = 30.0
    ) -> Optional[PipelineOutput]:
        """
        获取指定类型的Pipeline输出（带轮询等待）
        
        参数:
            session_id: 会话ID
            output_type: 输出类型
            timeout: 超时时间（秒）
            
        返回:
            Optional[PipelineOutput]: Pipeline输出对象，如果超时则返回None
        """
        start_time = asyncio.get_event_loop().time()
        
        while True:
            # 检查超时
            if asyncio.get_event_loop().time() - start_time > timeout:
                self.logger.warning(f"获取输出超时: {output_type}")
                return None
            
            # 查询数据库
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute(
                    f"SELECT * FROM {DatabaseSchema.PIPELINE_OUTPUTS_TABLE} "
                    f"WHERE session_id = ? AND output_type = ? AND status = 'completed' "
                    f"ORDER BY created_at DESC LIMIT 1",
                    (session_id, output_type)
                ) as cursor:
                    row = await cursor.fetchone()
                    
                    if row:
                        # 转换为PipelineOutput对象
                        data = {
                            "session_id": row[1],
                            "pipeline_name": row[2],
                            "output_type": row[3],
                            "data": row[4],
                            "status": row[5],
                            "created_at": row[6]
                        }
                        return PipelineOutput.from_dict(data)
            
            # 等待一段时间后重试
            await asyncio.sleep(0.5)
    
    async def get_pipeline_outputs_by_session(
        self,
        session_id: str,
        output_types: Optional[List[str]] = None
    ) -> List[PipelineOutput]:
        """
        获取会话的所有Pipeline输出
        
        参数:
            session_id: 会话ID
            output_types: 可选的输出类型过滤列表
            
        返回:
            List[PipelineOutput]: Pipeline输出列表
        """
        async with aiosqlite.connect(self.db_path) as db:
            if output_types:
                # 按类型过滤
                placeholders = ",".join("?" * len(output_types))
                query = (
                    f"SELECT * FROM {DatabaseSchema.PIPELINE_OUTPUTS_TABLE} "
                    f"WHERE session_id = ? AND output_type IN ({placeholders}) "
                    f"AND status = 'completed' ORDER BY created_at"
                )
                params = [session_id] + output_types
            else:
                # 获取所有输出
                query = (
                    f"SELECT * FROM {DatabaseSchema.PIPELINE_OUTPUTS_TABLE} "
                    f"WHERE session_id = ? AND status = 'completed' ORDER BY created_at"
                )
                params = [session_id]
            
            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                
                outputs = []
                for row in rows:
                    data = {
                        "session_id": row[1],
                        "pipeline_name": row[2],
                        "output_type": row[3],
                        "data": row[4],
                        "status": row[5],
                        "created_at": row[6]
                    }
                    outputs.append(PipelineOutput.from_dict(data))
                
                return outputs
    
    async def wait_for_outputs(
        self,
        session_id: str,
        required_types: List[str],
        timeout: float = 60.0
    ) -> Dict[str, PipelineOutput]:
        """
        等待多个输出类型就绪
        
        参数:
            session_id: 会话ID
            required_types: 需要的输出类型列表
            timeout: 超时时间（秒）
            
        返回:
            Dict[str, PipelineOutput]: 输出类型到PipelineOutput的映射
        """
        start_time = asyncio.get_event_loop().time()
        results: Dict[str, PipelineOutput] = {}
        
        while len(results) < len(required_types):
            # 检查超时
            if asyncio.get_event_loop().time() - start_time > timeout:
                self.logger.warning(f"等待输出超时: {required_types}")
                break
            
            # 查询所有需要的输出
            async with aiosqlite.connect(self.db_path) as db:
                placeholders = ",".join("?" * len(required_types))
                query = (
                    f"SELECT * FROM {DatabaseSchema.PIPELINE_OUTPUTS_TABLE} "
                    f"WHERE session_id = ? AND output_type IN ({placeholders}) "
                    f"AND status = 'completed' ORDER BY created_at"
                )
                params = [session_id] + required_types
                
                async with db.execute(query, params) as cursor:
                    rows = await cursor.fetchall()
                    
                    for row in rows:
                        output_type = row[3]
                        if output_type not in results:
                            data = {
                                "session_id": row[1],
                                "pipeline_name": row[2],
                                "output_type": row[3],
                                "data": row[4],
                                "status": row[5],
                                "created_at": row[6]
                            }
                            results[output_type] = PipelineOutput.from_dict(data)
            
            # 如果还有未获取的输出，等待后重试
            if len(results) < len(required_types):
                await asyncio.sleep(0.5)
        
        return results
    
    async def complete_session(self, session_id: str) -> None:
        """
        完成会话
        
        参数:
            session_id: 会话ID
        """
        async def update_session(db: aiosqlite.Connection):
            await db.execute(
                f"UPDATE {DatabaseSchema.SESSIONS_TABLE} "
                f"SET status = 'completed', completed_at = ? WHERE session_id = ?",
                (datetime.now(), session_id)
            )
        
        await self.write_queue.put(update_session)
        self.logger.info(f"完成会话: {session_id}")
    
    async def cleanup_old_sessions(self, days: int = 7) -> None:
        """
        清理旧会话数据
        
        参数:
            days: 保留天数
        """
        cutoff_date = datetime.now().timestamp() - (days * 24 * 3600)
        
        async def cleanup(db: aiosqlite.Connection):
            # 清理旧会话
            await db.execute(
                f"DELETE FROM {DatabaseSchema.SESSIONS_TABLE} "
                f"WHERE created_at < ?",
                (cutoff_date,)
            )
            
            # 清理旧输出
            await db.execute(
                f"DELETE FROM {DatabaseSchema.PIPELINE_OUTPUTS_TABLE} "
                f"WHERE created_at < ?",
                (cutoff_date,)
            )
        
        await self.write_queue.put(cleanup)
        self.logger.info(f"清理{days}天前的旧数据")
    
    async def close(self) -> None:
        """关闭数据库管理器"""
        if self.write_worker_task:
            # 发送停止信号
            await self.write_queue.put(None)
            await self.write_worker_task
            self.write_worker_task = None
        
        self.logger.info("数据库管理器已关闭")


# 全局数据库管理器实例
db_manager = DatabaseManager()
