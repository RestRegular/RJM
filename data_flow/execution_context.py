from typing import Dict, Any

from pydantic import BaseModel

from utils.data_visitor import DataVisitor

__all__ = ['ExecutionContext']


class ExecutionContext(BaseModel):
    """执行上下文（传递全局参数，如超时时间、日志器等）"""
    timeout: int = 30  # 节点执行超时时间（秒）
    debug: bool = False  # 是否开启调试模式
    global_vars: Dict[str, Any] = {}  # 全局变量（可被所有节点访问）

    def __init__(self, timeout: int = 30, debug: bool = False, **kwargs):
        super().__init__(timeout=timeout, debug=debug, **kwargs)
        self.global_vars['data']: Dict[str, Any] = kwargs or {}

    def get_context(self, key: str, default: Any = None) -> Any:
        """获取全局变量（如超时、日志器等）"""
        return self.global_vars['data'].get(key, default)

    def set_context(self, key: str, value: Any):
        """设置全局变量（如超时、日志器等）"""
        self.global_vars['data'][key] = value
