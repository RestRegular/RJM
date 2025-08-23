from collections.abc import Iterable
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

    def __init__(self, node, context, filter_handler: Callable = None):
        super().__init__(node, context)
        self.filter_handler = filter_handler

    def execute(self, **kwargs) -> ExecuteResult:
        """执行过滤逻辑"""
        self.process_args(validate=False, **kwargs)
        self.filter_handler = self.filter_handler or self.node.get_config("filter_handler")
        self._validate_node()
        input_data = self.get_input_data()

        if not input_data:
            raise ValueError(f"过滤节点 {self.node.id} 缺少必要的输入数据")

        for data in input_data.values():
            if not isinstance(data, Iterable) or isinstance(data, str):
                raise ValueError(f"过滤节点 {self.node.id} 的输入端口 {self.node.inputs[0].id} 的数据不是可迭代对象")

        # 执行过滤
        filtered_data = [self._apply_filter(data) for data in input_data.values()]

        return self.generate_default_execute_result(
            result_data=filtered_data
        )

    def _apply_filter(self, data: Iterable[Any]) -> list:
        """应用过滤表达式"""
        try:
            if isinstance(data, dict):
                return {k: v for k, v in data.items() if self.filter_handler({k: v}, context=self.context, node=self.node)}
            else:
                return [item for item in data if self.filter_handler(item, context=self.context, node=self.node)]
        except Exception as e:
            raise ValueError(f"过滤节点执行失败: {str(e)}") from e

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

    @classmethod
    def get_node_config(cls, context: ExecutionContext) -> NodeConfig:
        return FilterNodeConfig(filter_handler=lambda item: item['value'] > 5)
