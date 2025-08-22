from typing import Dict, Any

from pydantic import BaseModel

from utils.data_visitor import DataVisitor

__all__ = ['ExecutionContext']


class ExecutionContext(BaseModel):
    """执行上下文（传递全局参数，如超时时间、日志器等）"""
    timeout: int = 30  # 节点执行超时时间（秒）
    debug: bool = False  # 是否开启调试模式
    global_vars: Dict = {}  # 全局变量（可被所有节点访问）

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.global_vars['context'] = self
        self.global_vars['data']: Dict[str, Any] = kwargs or {}

    def get_data(self, key: str) -> Any:
        """获取全局变量（如超时、日志器等）"""
        return self.global_vars['data'].get(key, None)

    def set_data(self, key: str, value: Any):
        """设置全局变量（如超时、日志器等）"""
        self.global_vars['data'][key] = value
