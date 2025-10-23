"""
数据库Schema定义

定义TradeSwarm系统的数据库表结构，支持Pipeline间解耦通信。
"""

from typing import Dict, Any, Optional
from datetime import datetime
import json


class DatabaseSchema:
    """数据库表结构定义"""
    
    # 数据库表名常量
    PIPELINE_OUTPUTS_TABLE = "pipeline_outputs"
    SESSIONS_TABLE = "sessions"
    
    @staticmethod
    def get_create_tables_sql() -> Dict[str, str]:
        """
        获取创建数据库表的SQL语句
        
        返回:
            Dict[str, str]: 表名到创建SQL的映射
        """
        return {
            DatabaseSchema.PIPELINE_OUTPUTS_TABLE: f"""
                CREATE TABLE IF NOT EXISTS {DatabaseSchema.PIPELINE_OUTPUTS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    pipeline_name TEXT NOT NULL,
                    output_type TEXT NOT NULL,
                    data TEXT NOT NULL,
                    status TEXT DEFAULT 'completed',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            DatabaseSchema.SESSIONS_TABLE: f"""
                CREATE TABLE IF NOT EXISTS {DatabaseSchema.SESSIONS_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT UNIQUE NOT NULL,
                    status TEXT DEFAULT 'running',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP NULL
                )
            """
        }
    
    @staticmethod
    def get_create_indexes_sql() -> list[str]:
        """
        获取创建数据库索引的SQL语句
        
        返回:
            list[str]: 索引创建SQL列表
        """
        return [
            f"CREATE INDEX IF NOT EXISTS idx_session_pipeline ON {DatabaseSchema.PIPELINE_OUTPUTS_TABLE}(session_id, pipeline_name)",
            f"CREATE INDEX IF NOT EXISTS idx_session_type ON {DatabaseSchema.PIPELINE_OUTPUTS_TABLE}(session_id, output_type)",
            f"CREATE INDEX IF NOT EXISTS idx_session_status ON {DatabaseSchema.PIPELINE_OUTPUTS_TABLE}(session_id, status)",
            f"CREATE INDEX IF NOT EXISTS idx_session_id ON {DatabaseSchema.SESSIONS_TABLE}(session_id)"
        ]
    
    @staticmethod
    def get_pipeline_output_types() -> Dict[str, str]:
        """
        获取Pipeline输出类型定义
        
        返回:
            Dict[str, str]: 输出类型到描述的映射
        """
        return {
            "market_analysis": "市场数据分析报告",
            "social_analysis": "社交媒体情绪分析报告", 
            "news_analysis": "新闻分析报告",
            "fundamentals_analysis": "基本面分析报告",
            "research_report": "研究分析综合报告",
            "trading_decision": "交易决策报告"
        }
    
    @staticmethod
    def get_pipeline_names() -> list[str]:
        """
        获取所有Pipeline名称
        
        返回:
            list[str]: Pipeline名称列表
        """
        return [
            "market_pipeline",
            "social_pipeline", 
            "news_pipeline",
            "fundamentals_pipeline",
            "research_pipeline",
            "trading_pipeline"
        ]


class PipelineOutput:
    """Pipeline输出数据模型"""
    
    def __init__(
        self,
        session_id: str,
        pipeline_name: str,
        output_type: str,
        data: Dict[str, Any],
        status: str = "completed"
    ):
        """
        初始化Pipeline输出
        
        参数:
            session_id: 会话ID
            pipeline_name: Pipeline名称
            output_type: 输出类型
            data: 输出数据
            status: 状态
        """
        self.session_id = session_id
        self.pipeline_name = pipeline_name
        self.output_type = output_type
        self.data = data
        self.status = status
        self.created_at = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典格式
        
        返回:
            Dict[str, Any]: 字典格式的数据
        """
        return {
            "session_id": self.session_id,
            "pipeline_name": self.pipeline_name,
            "output_type": self.output_type,
            "data": json.dumps(self.data, ensure_ascii=False),
            "status": self.status,
            "created_at": self.created_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipelineOutput":
        """
        从字典创建PipelineOutput实例
        
        参数:
            data: 字典数据
            
        返回:
            PipelineOutput: PipelineOutput实例
        """
        return cls(
            session_id=data["session_id"],
            pipeline_name=data["pipeline_name"],
            output_type=data["output_type"],
            data=json.loads(data["data"]),
            status=data.get("status", "completed")
        )


class Session:
    """会话数据模型"""
    
    def __init__(
        self,
        session_id: str,
        status: str = "running"
    ):
        """
        初始化会话
        
        参数:
            session_id: 会话ID
            status: 状态
        """
        self.session_id = session_id
        self.status = status
        self.created_at = datetime.now()
        self.completed_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典格式
        
        返回:
            Dict[str, Any]: 字典格式的数据
        """
        return {
            "session_id": self.session_id,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Session":
        """
        从字典创建Session实例
        
        参数:
            data: 字典数据
            
        返回:
            Session: Session实例
        """
        session = cls(
            session_id=data["session_id"],
            status=data.get("status", "running")
        )
        if data.get("completed_at"):
            session.completed_at = datetime.fromisoformat(data["completed_at"])
        return session
