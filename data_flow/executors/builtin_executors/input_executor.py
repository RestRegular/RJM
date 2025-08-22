from typing import Dict, Any, Callable

from data_flow import NodeConfig
from data_flow.node_executor import NodeExecutor
from data_flow.node_executor_factory import NodeExecutorFactory
from data_flow.node import Node
from data_flow.execution_context import ExecutionContext
from data_flow.enum_data import BuiltinNodeType
from data_flow.result import ExecuteResult, DefaultExecuteResult

__all__ = [
    "InputNodeConfig",
    "InputNodeExecutor"
]


class InputNodeConfig(NodeConfig):
    data_provider: Callable


# 输入节点执行器
@NodeExecutorFactory.register_executor
class InputNodeExecutor(NodeExecutor):
    """输入节点执行器
    负责产生或接收外部输入数据
    """

    def __init__(self, node: Node = None, context: ExecutionContext = None, data_provider: Callable = None):
        super().__init__(node, context)
        self.data_provider = data_provider

    def execute(self, **kwargs) -> ExecuteResult:
        """执行输入节点逻辑"""
        self.process_args(**kwargs)
        self.data_provider = self.data_provider or self.node.get_config("data_provider") or self._default_data_provider
        input_data = self.node.get_config("data.input")
        # 输入节点通常不需要输入数据
        if input_data:
            self.context.global_vars.get("logger", print)(
                f"警告: 输入节点 {self.node.id} 收到未预期的输入数据"
            )

        # 获取数据
        data = self.data_provider()

        # 确保输出格式符合节点定义
        output_port_ids = [port.id for port in self.node.outputs]
        if not output_port_ids:
            raise ValueError(f"输入节点 {self.node.id} 没有定义输出端口")

        return DefaultExecuteResult(
            node_id=self.node.id,
            output_data={output_port_ids[0]: data},
            success=True
        )

    def _default_data_provider(self) -> Any:
        """默认数据提供器，可被外部提供的函数覆盖"""
        return []

    def _validate_node(self) -> None:
        """验证输入节点的特殊要求：不应有输入端口"""
        if self.node and self.node.inputs:
            raise ValueError(f"输入节点 {self.node.id} 不应有输入端口")

    @classmethod
    def get_node_type(cls) -> str | BuiltinNodeType:
        return BuiltinNodeType.INPUT
