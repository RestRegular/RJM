from typing import Dict, Any, Callable

from data_flow.node import Node
from data_flow.node_config import NodeConfig
from data_flow.enum_data import BuiltinNodeType
from data_flow.node_executor import NodeExecutor
from data_flow.node_executor_factory import NodeExecutorFactory
from data_flow.execution_context import ExecutionContext
from data_flow.result import ExecuteResult, DefaultExecuteResult


__all__ = [
    "OutputNodeConfig",
    "OutputNodeExecutor"
]


class OutputNodeConfig(NodeConfig):
    data_processor: Callable


@NodeExecutorFactory.register_executor
class OutputNodeExecutor(NodeExecutor):
    """
    输出节点执行器
    负责处理并输出数据
    """

    def __init__(self, node, context, data_processor: Callable = None):
        super().__init__(node, context)
        self.data_processor = data_processor

    def execute(self, **kwargs) -> ExecuteResult:
        self.process_args(**kwargs)
        self.data_processor = self.data_processor or self.node.get_config("data_processor", None) or self._default_data_processor

        input_data = self.get_input_data()

        if not input_data:
            raise ValueError(f"输出节点 {self.node.id} 没有获取到输入数据")

        result_data = self.data_processor(input_data, node=self.node, context=self.context)

        return self.generate_default_execute_result(result_data=result_data)

    @classmethod
    def get_node_type(cls) -> str | BuiltinNodeType:
        return BuiltinNodeType.OUTPUT

    @staticmethod
    def _default_data_processor(input_data: Any, **kwargs) -> Any:
        return input_data

    @classmethod
    def get_node_config(cls, context: ExecutionContext) -> NodeConfig:
        return OutputNodeConfig(data_processor=lambda input_data, **kwargs: input_data)
