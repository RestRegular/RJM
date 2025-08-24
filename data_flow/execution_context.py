from logging import Logger
from typing import Dict, Any, Optional

from pydantic import BaseModel

from utils.data_visitor import DataVisitor

__all__ = ['ExecutionContext']

from utils.log_system import get_logger


class ExecutionContext(BaseModel):
    """执行上下文（传递全局参数，如超时时间、日志器等）"""
    global_vars: Dict[str, Any] = {}  # 全局变量（可被所有节点访问）

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
