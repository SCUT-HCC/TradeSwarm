"""
BasePipeline基类

实现Pipeline生命周期管理和数据库轮询等待机制。
为所有Pipeline提供统一的接口和错误处理。
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime

from ..storage import DatabaseManager, PipelineOutput


class BasePipeline(ABC):
    """Pipeline基类"""
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        session_id: str,
        pipeline_name: str
    ):
        """
        初始化Pipeline
        
        参数:
            db_manager: 数据库管理器
            session_id: 会话ID
            pipeline_name: Pipeline名称
        """
        self.db_manager = db_manager
        self.session_id = session_id
        self.pipeline_name = pipeline_name
        self.logger = logging.getLogger(f"{__name__}.{pipeline_name}")
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    async def run(self) -> Dict[str, Any]:
        """
        运行Pipeline
        
        返回:
            Dict[str, Any]: Pipeline执行结果
        """
        self.start_time = datetime.now()
        self.logger.info(f"Pipeline {self.pipeline_name} 开始执行")
        
        try:
            # 执行Pipeline逻辑
            result = await self.execute()
            
            # 保存输出到数据库
            if result:
                output = PipelineOutput(
                    session_id=self.session_id,
                    pipeline_name=self.pipeline_name,
                    output_type=self.get_output_type(),
                    data=result
                )
                await self.db_manager.save_pipeline_output(output)
                self.logger.info(f"Pipeline {self.pipeline_name} 输出已保存")
            
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            self.logger.info(f"Pipeline {self.pipeline_name} 执行完成，耗时: {duration:.2f}秒")
            
            return result or {}
            
        except Exception as e:
            self.logger.error(f"Pipeline {self.pipeline_name} 执行失败: {e}")
            
            # 保存错误信息
            error_output = PipelineOutput(
                session_id=self.session_id,
                pipeline_name=self.pipeline_name,
                output_type=self.get_output_type(),
                data={"error": str(e), "status": "failed"},
                status="failed"
            )
            await self.db_manager.save_pipeline_output(error_output)
            
            raise
    
    @abstractmethod
    async def execute(self) -> Dict[str, Any]:
        """
        执行Pipeline逻辑（子类必须实现）
        
        返回:
            Dict[str, Any]: Pipeline执行结果
        """
        pass
    
    @abstractmethod
    def get_output_type(self) -> str:
        """
        获取Pipeline输出类型（子类必须实现）
        
        返回:
            str: 输出类型
        """
        pass
    
    async def wait_for_inputs(
        self,
        required_types: List[str],
        timeout: float = 60.0
    ) -> Dict[str, PipelineOutput]:
        """
        等待输入数据就绪
        
        参数:
            required_types: 需要的输入类型列表
            timeout: 超时时间（秒）
            
        返回:
            Dict[str, PipelineOutput]: 输入类型到PipelineOutput的映射
        """
        self.logger.info(f"等待输入数据: {required_types}")
        
        inputs = await self.db_manager.wait_for_outputs(
            session_id=self.session_id,
            required_types=required_types,
            timeout=timeout
        )
        
        if len(inputs) < len(required_types):
            missing_types = set(required_types) - set(inputs.keys())
            self.logger.warning(f"部分输入数据缺失: {missing_types}")
        
        self.logger.info(f"获取到输入数据: {list(inputs.keys())}")
        return inputs
    
    async def wait_for_input(
        self,
        output_type: str,
        timeout: float = 30.0
    ) -> Optional[PipelineOutput]:
        """
        等待单个输入数据就绪
        
        参数:
            output_type: 需要的输出类型
            timeout: 超时时间（秒）
            
        返回:
            Optional[PipelineOutput]: Pipeline输出对象，如果超时则返回None
        """
        self.logger.info(f"等待输入数据: {output_type}")
        
        output = await self.db_manager.get_pipeline_output(
            session_id=self.session_id,
            output_type=output_type,
            timeout=timeout
        )
        
        if output:
            self.logger.info(f"获取到输入数据: {output_type}")
        else:
            self.logger.warning(f"输入数据超时: {output_type}")
        
        return output
    
    def get_duration(self) -> Optional[float]:
        """
        获取Pipeline执行时长
        
        返回:
            Optional[float]: 执行时长（秒），如果未完成则返回None
        """
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def is_completed(self) -> bool:
        """
        检查Pipeline是否已完成
        
        返回:
            bool: 是否已完成
        """
        return self.end_time is not None


class DataCollectionPipeline(BasePipeline):
    """数据采集Pipeline基类"""
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        session_id: str,
        pipeline_name: str
    ):
        """
        初始化数据采集Pipeline
        
        参数:
            db_manager: 数据库管理器
            session_id: 会话ID
            pipeline_name: Pipeline名称
        """
        super().__init__(db_manager, session_id, pipeline_name)
    
    async def execute(self) -> Dict[str, Any]:
        """
        执行数据采集逻辑
        
        返回:
            Dict[str, Any]: 采集到的数据
        """
        # 子类实现具体的数据采集逻辑
        return await self.collect_data()
    
    @abstractmethod
    async def collect_data(self) -> Dict[str, Any]:
        """
        采集数据（子类必须实现）
        
        返回:
            Dict[str, Any]: 采集到的数据
        """
        pass


class AnalysisPipeline(BasePipeline):
    """分析Pipeline基类"""
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        session_id: str,
        pipeline_name: str,
        required_inputs: List[str]
    ):
        """
        初始化分析Pipeline
        
        参数:
            db_manager: 数据库管理器
            session_id: 会话ID
            pipeline_name: Pipeline名称
            required_inputs: 需要的输入类型列表
        """
        super().__init__(db_manager, session_id, pipeline_name)
        self.required_inputs = required_inputs
    
    async def execute(self) -> Dict[str, Any]:
        """
        执行分析逻辑
        
        返回:
            Dict[str, Any]: 分析结果
        """
        # 等待输入数据
        inputs = await self.wait_for_inputs(
            required_types=self.required_inputs,
            timeout=60.0
        )
        
        # 执行分析
        return await self.analyze_data(inputs)
    
    @abstractmethod
    async def analyze_data(self, inputs: Dict[str, PipelineOutput]) -> Dict[str, Any]:
        """
        分析数据（子类必须实现）
        
        参数:
            inputs: 输入数据映射
            
        返回:
            Dict[str, Any]: 分析结果
        """
        pass
