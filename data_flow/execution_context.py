import json
import logging
from logging import Logger
from typing import Dict, Any, Optional

from pydantic import BaseModel, ConfigDict, field_validator

from utils.log_system import get_logger

__all__ = ['ExecutionContext']


class ExecutionContext(BaseModel):
    """执行上下文（传递全局参数，如超时时间、日志器等）"""
    global_vars: Dict[str, Any] = {}  # 全局变量（可被所有节点访问）

    model_config = ConfigDict(
        json_encoders={
            logging.Logger: lambda logger: {
                'name': logger.name,
                'log_level': logger.level,
                'to_console': getattr(logger, 'to_console', True),  # 使用getattr避免属性不存在
                'log_file': getattr(logger, 'log_file', None),
                'colorful_log': getattr(logger, 'colorful_log', True)
            }
        }
    )

    @classmethod
    @field_validator('global_vars', mode='before')
    def validate_global_vars(cls, v: Any) -> Dict[str, Any]:
        """验证并处理global_vars，确保日志器正确反序列化"""
        if not isinstance(v, dict):
            raise ValueError("global_vars must be a dictionary")

        if 'logger' in v:
            logger_data = v['logger']

            # 如果是字典形式的日志器配置，重建日志器
            if isinstance(logger_data, dict):
                try:
                    v['logger'] = get_logger(
                        name=logger_data.get('name', 'default'),
                        level=logger_data.get('log_level', logging.INFO),
                        to_console=logger_data.get('to_console', True),
                        log_file=logger_data.get('log_file'),
                        colorful_log=logger_data.get('colorful_log', True)
                    )
                except Exception as e:
                    raise ValueError(f"Failed to create logger: {str(e)}")
            # 如果已经是Logger实例，无需处理
            elif not isinstance(logger_data, logging.Logger):
                raise ValueError(f"Invalid logger type: {type(logger_data)}")

        return v

    def __init__(self, timeout: int = 30, debug: bool = False,
                 logger: Optional[Logger] = None, **kwargs):
        super().__init__(global_vars={
            "timeout": timeout,
            "debug": debug,
            "logger": logger or get_logger(self.__class__.__name__, 'info'),
            **kwargs
        })

    def get_context(self, key: str, default: Any = None) -> Any:
        """获取全局变量（如超时、日志器等）"""
        return self.global_vars.get(key, default)

    def set_context(self, key: str, value: Any):
        """设置全局变量（如超时、日志器等）"""
        self.global_vars[key] = value

    def __getattr__(self, item: str) -> Any:
        return self.get_context(item, None)

    def __setattr__(self, key: str, value: Any):
        self.set_context(key, value)
