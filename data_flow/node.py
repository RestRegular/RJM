import uuid
from typing import Optional, List, Dict, Any, Type, Union
from abc import ABC

from pydantic import BaseModel, Field

from data_flow.port import Port
from data_flow.result import Result
from utils.data_visitor import DataVisitor
from data_flow.node_config import NodeConfig
from data_flow.enum_data import NodeStatus, BuiltinNodeType


class Node(BaseModel):
    id: str = Field(default_factory=lambda :str(uuid.uuid4()))  # 节点唯一标识
    name: str  # 节点名称（如"数据过滤"、"格式转换"）
    type: Union[str, BuiltinNodeType]  # 节点类型（如"filter"、"transform"、"aggregator"）
    description: Optional[str] = None  # 节点描述

    # TODO: 节点属性
    is_start: bool = False  # 是否为起始节点
    is_end: bool = False  # 是否为结束节点

    # 输入/输出端口定义
    inputs: List[Port] = []  # 输入端口（接收上游节点数据）
    outputs: List[Port] = []  # 输出端口（向下游节点传递数据）

    config: NodeConfig = NodeConfig()  # 节点配置参数，以及后续执行数据存储
    status: Union[NodeStatus, str] = NodeStatus.PENDING  # 节点运行状态

    # 运行时数据（执行后填充）
    result: Optional[Result] = None  # 节点执行结果（键为输出端口id）
    error: Optional[str] = None  # 错误信息（状态为FAILED时填充）

    executor: Optional[Any] = None  # 节点执行器实例

    def get_input_port(self, port_id: str) -> Optional[Port]:
        """根据端口ID获取输入端口"""
        for port in self.inputs:
            if port.id == port_id:
                return port
        return None

    def get_output_port(self, port_id: str) -> Optional[Port]:
        """根据端口ID获取输出端口"""
        for port in self.outputs:
            if port.id == port_id:
                return port
        return None

    def set_config(self, key: str, value: Any):
        self.config.set_config(key, value)

    def get_config(self, key: str, default: Any = None) -> Any:
        return self.config.get_config(key, default)

    def base_info(self) -> str:
        return f"<Node[{self.id}] - ({self.type}) ({self.name})>"

    def status_info(self) -> str:
        return f"({self.status})"

    def inputs_info(self) -> str:
        return f"Inputs: {self.inputs}" if self.inputs else ""

    def outputs_info(self) -> str:
        return f"Outputs: {self.outputs}" if self.outputs else ""

    def __str__(self):
        # 基础信息部分
        base_info = self.base_info()
        # 状态信息，使用括号突出显示
        status_info = self.status_info()
        # 输入输出信息，仅在有值时显示
        inputs_info = self.inputs_info()
        outputs_info = self.outputs_info()

        # 组合所有信息，过滤空字符串
        parts = [base_info, status_info]
        if inputs_info:
            parts.append(inputs_info)
        if outputs_info:
            parts.append(outputs_info)

        return f'[{" | ".join(parts)}]'

    def __repr__(self):
        return self.__str__()
