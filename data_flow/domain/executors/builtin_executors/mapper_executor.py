import logging
from typing import Any, Callable

from data_flow.domain.result import ExecutionResult
from data_flow.utils.log_system import get_logger
from data_flow.domain.node_config import NodeConfig
from data_flow.domain.enum_data import BuiltinNodeType
from data_flow.domain.node_executor import NodeExecutor
from data_flow.domain.execution_context import ExecutionContext
from data_flow.domain.node_executor_factory import NodeExecutorFactory

__all__ = [
    "MapperNodeConfig",
    "MapperNodeExecutor"
]

logger = get_logger(__name__)


class MapperNodeConfig(NodeConfig):
    """映射节点配置"""
    # 映射处理函数，参数为该节点的输入端口数据字典 {port_id: port_value}
    map_handler: Callable[[Any], Any]


@NodeExecutorFactory.register_executor
class MapperNodeExecutor(NodeExecutor):
    """映射节点执行器"""

    def __init__(self, node, context, map_handler: Callable[[Any], Any] = None):
        super().__init__(node, context)
        self.map_handler = map_handler

    def execute(self, **kwargs) -> ExecutionResult:
        self.process_args(**kwargs)
        input_data = self.get_input_data()
        self.map_handler = self.map_handler or self.node.get_config("map_handler")

        if not input_data:
            error = ValueError(f"转换节点 {self.node.id} 缺少必要的输入数据")
            self.log_validation_failed(error, f"缺少必要的输入数据")
            raise

        # 将多输入数据传递给映射处理器
        self.log_handle_start()
        try:
            # 映射处理器现在需要处理多个输入
            result_data = self.map_handler(
                input_data,
                context=self.context,
                node=self.node
            )
        except Exception as e:
            self.log_handle_failed(e, str(e))
            raise ValueError(f"转换节点执行失败: {str(e)}") from e

        return self.generate_default_execute_result(result_data)

    @classmethod
    def get_node_type(cls) -> str | BuiltinNodeType:
        return BuiltinNodeType.MAPPER

    @classmethod
    def get_node_config(cls, context: ExecutionContext) -> NodeConfig:
        return None

    def get_logger(self) -> logging.Logger:
        return logger
