from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union

from pydantic import BaseModel

from data_flow.result import ExecuteResult, DefaultExecuteResult
from data_flow.node import Node, NodeConfig
from data_flow.enum_data import BuiltinNodeType, DataType
from data_flow.execution_context import ExecutionContext


class NodeExecutor(ABC):
    """节点执行器抽象基类，所有具体节点执行器需继承此类并实现抽象方法"""
    def __init__(self, node, context):
        """
        初始化节点执行器
        :param node: 要执行的节点
        :param context: 执行上下文
        """
        self.node: Node = node
        self.context: ExecutionContext = context
        if self.node and not self.node.config:
            self.node.config = self.get_node_config(self.context)
        NodeExecutor._validate_node(self)  # 验证节点是否符合执行器要求

    def _validate_node(self):
        """验证节点是否符合执行器要求"""
        # 基础验证：检查节点是否有必要的端口
        if self.node:
            if not isinstance(self.node, Node):
                raise ValueError("节点执行器初始化失败，节点参数必须是 Node 实例")
            if not self.node.config:
                raise ValueError(f"节点执行器初始化失败，节点 {self.node.id} 缺少配置参数")
        if self.context:
            if not isinstance(self.context, ExecutionContext):
                raise ValueError("节点执行器初始化失败，执行上下文参数必须是 ExecutionContext 实例")

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

    @classmethod
    @abstractmethod
    def get_node_config(cls, context: ExecutionContext) -> NodeConfig:
        """获取节点配置"""
        raise NotImplementedError("子类必须实现 get_node_config 方法")

    def get_input_data(self, port_id: Union[List[str], str] = None):
        """处理并获取输入数据，指定端口ID获取指定的端口数据，否则获取全部数据"""
        # 获取所有输入数据配置
        input_data = self.node.get_config("data.input")

        # 如果未指定端口ID，返回所有端口的数据
        if port_id is None:
            # 验证所有端口数据类型并返回有效数据
            valid_data = {}
            for port in self.node.inputs:
                port_id = port.id
                port_data_type = port.data_type

                if port_id in input_data:
                    data = input_data[port_id]
                    # 验证数据类型
                    if port_data_type:
                        if isinstance(port_data_type, DataType):
                            if port_data_type.validate(data):
                                valid_data[port_id] = data
                        elif isinstance(port_data_type, str):
                            if DataType.from_str(port_data_type).validate(data):
                                valid_data[port_id] = data
                    else:
                        # 没有指定数据类型，直接返回
                        valid_data[port_id] = data

            return valid_data if valid_data else None
        else:
            # 如果指定了端口ID，找到对应的端口类型
            # 处理端口ID为列表的情况，取第一个有效ID
            if isinstance(port_id, list):
                port_id = port_id[0] if port_id else None

            # 查找指定端口
            target_port = next(
                (port for port in self.node.inputs if port.id == port_id),
                None
            )
            port_data_type = target_port.data_type if target_port else None

            # 获取指定端口的数据
            if port_id and port_id in input_data:
                data = input_data[port_id]
                # 验证数据类型
                if port_data_type:
                    if isinstance(port_data_type, DataType):
                        return data if port_data_type.validate(data) else None
                    elif isinstance(port_data_type, str):
                        return data if DataType.from_str(port_data_type).validate(data) else None
                else:
                    # 没有指定数据类型，直接返回
                    return data

        return None  # 未找到有效数据

    def generate_default_execute_result(self, result_data: Any = None, success: bool = True, **kwargs) -> DefaultExecuteResult:
        """将执行结果分发给每个输出端口"""
        output_port_ids = [port.id for port in self.node.outputs] if len(self.node.outputs) > 0 else ["output"]
        return DefaultExecuteResult(
            node_id=self.node.id,
            output_data={port_id: result_data for port_id in output_port_ids},
            success=success,
            **kwargs
        )
