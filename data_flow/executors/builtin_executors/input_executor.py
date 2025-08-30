import logging
from typing import Dict, Any, Callable

from data_flow.node import Node
from data_flow.node_config import NodeConfig
from data_flow.enum_data import BuiltinNodeType
from data_flow.node_executor import NodeExecutor
from data_flow.node_executor_factory import NodeExecutorFactory
from data_flow.execution_context import ExecutionContext
from data_flow.result import ExecuteResult, DefaultExecuteResult

__all__ = [
    "InputNodeConfig",
    "InputNodeExecutor"
]

from utils.log_system import get_logger

logger = get_logger(__name__)


class InputNodeConfig(NodeConfig):
    data_provider: Callable


# 输入节点执行器
@NodeExecutorFactory.register_executor
class InputNodeExecutor(NodeExecutor):
    """
    输入节点执行器
    负责产生或接收外部输入数据
    """

    def __init__(self, node, context, data_provider: Callable = None):
        super().__init__(node, context)
        self.data_provider = data_provider

    def execute(self, **kwargs) -> ExecuteResult:
        self.process_args(**kwargs)
        self.data_provider = self.data_provider or self.node.get_config("data_provider") or self._default_data_provider
        input_data = self.get_input_data()
        # 输入节点通常不需要输入数据
        if input_data:
            logger.warning(f"输入节点 {self.node} 接收到未预期的数据：{str(input_data)[:50] + ('...' if len(input_data) > 50 else '')}\n"
                           f"输入节点通常不需要输入端口和输入数据")

        self.log_handle_start()
        data = self.data_provider(context=self.context, node=self.node)

        output_port_ids = [port.id for port in self.node.outputs]
        if not output_port_ids:
            error = ValueError(f"输入节点 {self.node.id} 没有定义输出端口")
            self.log_execution_failed(error, "没有定义输出端口")
            raise error

        return self.generate_default_execute_result(result_data=data)

    def _default_data_provider(self, **kwargs) -> Any:
        """默认数据提供器，可被外部提供的函数覆盖"""
        return kwargs.get("context").initial_data

    def _validate_node(self) -> None:
        """验证输入节点的特殊要求：不应有输入端口"""
        if self.node and self.node.inputs:
            error = ValueError(f"输入节点 {self.node.id} 不应有输入端口")
            self.log_validation_failed(error, f"输入节点不应有输入端口")
            raise error

    @classmethod
    def get_node_type(cls) -> str | BuiltinNodeType:
        return BuiltinNodeType.INPUT

    @classmethod
    def get_node_config(cls, context: ExecutionContext) -> InputNodeConfig:
        return InputNodeConfig(data_provider=lambda **kwargs: context.get_context("initial_data"))

    def get_logger(self) -> logging.Logger:
        return logger
