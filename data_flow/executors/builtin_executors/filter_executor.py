from typing import Dict, Any, Callable

from data_flow.node import Node
from data_flow.node_config import NodeConfig
from data_flow.enum_data import BuiltinNodeType
from data_flow.execution_context import ExecutionContext
from data_flow.result import ExecuteResult, DefaultExecuteResult
from data_flow.node_executor import NodeExecutor
from data_flow.node_executor_factory import NodeExecutorFactory

__all__ = ["FilterNodeConfig", "FilterNodeExecutor"]


class FilterNodeConfig(NodeConfig):
    filter_handler: Callable[[Any], Any]


# 过滤节点执行器
@NodeExecutorFactory.register_executor
class FilterNodeExecutor(NodeExecutor):
    """
    过滤节点执行器
    负责根据条件过滤输入数据
    """

    def __init__(self, node: Node = None, context: ExecutionContext = None, filter_handler: Callable = None):
        super().__init__(node, context)
        self.filter_handler = filter_handler

    def execute(self, **kwargs) -> ExecuteResult:
        """执行过滤逻辑"""
        self.process_args(validate=False, **kwargs)
        self.filter_handler = self.filter_handler or self.node.get_config("filter_handler")
        self._validate_node()
        input_data = self.node.get_config("data.input")
        # 获取输入数据
        input_port_id = next(
            (port.id for port in self.node.inputs if port.required),
            self.node.inputs[0].id if self.node.inputs else None
        )

        if not input_port_id or input_port_id not in input_data:
            raise ValueError(f"过滤节点 {self.node.id} 缺少必要的输入数据")

        data = input_data[input_port_id]
        if not isinstance(data, list):
            data = [data]  # 确保数据是可迭代的

        # 执行过滤
        filtered_data = self._apply_filter(data)

        # 准备输出
        output_port_id = self.node.outputs[0].id if self.node.outputs else "output"
        return DefaultExecuteResult(
            node_id=self.node.id,
            output_data={output_port_id: filtered_data},
            success=True
        )

    def _apply_filter(self, data: list) -> list:
        """应用过滤表达式"""
        try:
            filtered = []
            for item in data:
                # 限制eval只能访问item变量，提高安全性
                if self.filter_handler(item):
                    filtered.append(item)
            return filtered
        except Exception as e:
            raise ValueError(f"过滤表达式执行失败: {str(e)}") from e

    def _validate_node(self) -> None:
        """验证过滤节点的特殊要求"""
        if not self.filter_handler:
            raise ValueError(f"过滤节点 {self.node.id} 必须配置过滤处理器")
        if self.node:
            if not self.node.inputs:
                raise ValueError(f"过滤节点 {self.node.id} 必须至少有一个输入端口")

    @classmethod
    def get_node_type(cls) -> str | BuiltinNodeType:
        return BuiltinNodeType.FILTER
