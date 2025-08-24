import logging
from collections.abc import Iterable
from typing import Dict, Any, Callable

from data_flow.node import Node
from data_flow.node_config import NodeConfig
from data_flow.enum_data import BuiltinNodeType
from data_flow.execution_context import ExecutionContext
from data_flow.result import ExecuteResult, DefaultExecuteResult
from data_flow.node_executor import NodeExecutor
from data_flow.node_executor_factory import NodeExecutorFactory
from utils.log_system import get_logger

__all__ = ["FilterNodeConfig", "FilterNodeExecutor"]

logger = get_logger(__name__)


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

        if self.context.debug:
            logger.debug(f"过滤节点 {self.node} 输入原始数据：{str(input_data)[:50] + ('...' if len(str(input_data)) > 50 else '')}")

        if not input_data:
            logger.error(f"过滤节点 {self.node} 缺少输入数据")
            raise ValueError(f"过滤节点 {self.node.base_info()} 缺少输入数据")

        for port_id, data in input_data.items():
            if not isinstance(data, Iterable) or isinstance(data, str):
                error = ValueError(f"过滤节点 {self.node.id} 的输入端口 {port_id} 的数据 {str(data) + ('...' if len(str(data)) > 50 else '')} 不是可迭代对象")
                logger.error(error, f"过滤节点 {self.node} 的输入端口 {port_id} 的输入数据 {str(data) + ('...' if len(str(data)) > 50 else '')} 不是可迭代对象")
                raise error

        # 执行过滤
        try:
            self.log_handle_start()
            filtered_data = self.filter_handler(input_data, context=self.context, node=self.node)
        except Exception as e:
            self.log_handle_failed(e, str(e))
            raise ValueError(f"过滤节点执行失败: {str(e)}") from e

        return self.generate_default_execute_result(
            result_data=filtered_data
        )

    def _validate_node(self) -> None:
        """验证过滤节点的特殊要求"""
        if not self.filter_handler:
            error = ValueError(f"过滤节点 {self.node.id} 必须配置过滤处理器")
            self.log_validation_failed(error, "缺少过滤处理器")
            raise error
        if self.node:
            if not self.node.inputs:
                error = ValueError(f"过滤节点 {self.node.id} 必须至少有一个输入端口")
                self.log_validation_failed(error, "输入端口为空")
                raise error

    @classmethod
    def get_node_type(cls) -> str | BuiltinNodeType:
        return BuiltinNodeType.FILTER

    @classmethod
    def get_node_config(cls, context: ExecutionContext) -> NodeConfig:
        return FilterNodeConfig(filter_handler=lambda item, **kwargs: item['value'] > 5)

    def get_logger(self) -> logging.Logger:
        return logger


