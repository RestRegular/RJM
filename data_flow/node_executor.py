from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

from pydantic import BaseModel

from data_flow.result import ExecuteResult
from data_flow.node import Node, NodeConfig
from data_flow.enum_data import BuiltinNodeType
from data_flow.execution_context import ExecutionContext


class NodeExecutor(ABC):
    """节点执行器抽象基类，所有具体节点执行器需继承此类并实现抽象方法"""
    def __init__(self, node: Node, context: ExecutionContext):
        """
        初始化节点执行器
        :param node: 要执行的节点
        :param context: 执行上下文
        """
        self.node = node
        self.context = context
        NodeExecutor._validate_node(self)  # 验证节点是否符合执行器要求

    def _validate_node(self) -> None:
        """验证节点是否符合执行器要求"""
        # 基础验证：检查节点是否有必要的端口
        if self.node:
            required_inputs = [port.id for port in self.node.inputs if port.required]
            if not required_inputs:
                return
            pass

    def process_args(self, validate: bool = True, **kwargs):
        if not self.node:
            self.node = kwargs.get('node', None)
        if not self.context:
            self.context = kwargs.get('context', None)
        if not self.node or not self.context:
            raise ValueError("节点执行器初始化失败，缺少必要参数")
        if validate:
            self._validate_node()
        else:
            NodeExecutor._validate_node(self)

    @abstractmethod
    def execute(self, **kwargs) -> ExecuteResult:
        """执行节点逻辑"""
        raise NotImplementedError("子类必须实现 execute 方法")

    @classmethod
    @abstractmethod
    def get_node_type(cls) -> str | BuiltinNodeType:
        """获取节点类型"""
        raise NotImplementedError("子类必须实现 get_node_type 方法")
