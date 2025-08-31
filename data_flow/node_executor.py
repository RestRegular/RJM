import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union, Callable

from pydantic import BaseModel

from data_flow.node import Node, NodeConfig
from data_flow.execution_context import ExecutionContext
from data_flow.enum_data import BuiltinNodeType, DataType
from data_flow.result import ExecuteResult, DefaultExecuteResult


class NodeExecutor(ABC):
    """节点执行器抽象基类，所有具体节点执行器需继承此类并实现抽象方法"""

    def __init__(self, node, context,
                 input_processor: Optional[Callable] = None):
        """
        初始化节点执行器

        :param node: 要执行的节点
        :param context: 执行上下文
        :param input_processor: 输入数据预处理器
        """
        self.node: Node = node
        self.context: ExecutionContext = context
        if self.node and not self.node.config:
            self.node.config = self.get_node_config(self.context)
        self.input_processor = input_processor or (
            self.node.get_config("input_processor") if self.node.config else None) \
                               or self.default_input_processor
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

    def process_args(self, validate: bool = True, log: bool = True, **kwargs):
        """
        处理额外参数

        :param validate: 用于指定采用的验证逻辑，True为自定义验证逻辑，False为默认验证逻辑
        :param log: 用于指定是否记录开始执行执行器的日志
        :param kwargs: 其他参数：node(Node)、context(ExecutionContext)
        """
        if not self.node:
            self.node = kwargs.get('node', None)
        if not self.context:
            self.context = kwargs.get('context', None)
        if self.context:
            self.adjust_logger()
        if not self.node or not self.context:
            raise ValueError("节点执行器初始化失败，缺少必要参数")
        if validate:
            self._validate_node()
        else:
            NodeExecutor._validate_node(self)

        if log:
            self.log_execution_start()

    @abstractmethod
    def execute(self, **kwargs) -> ExecuteResult:
        """执行节点逻辑"""
        raise NotImplementedError("子类必须实现 execute 方法")

    @abstractmethod
    def get_logger(self) -> logging.Logger:
        """获取日志记录器"""
        raise NotImplementedError("子类必须实现 get_logger 方法")

    def adjust_logger(self):
        self.get_logger().setLevel(self.context.get_context("log_level", logging.INFO))

    def log_execution_start(self):
        """记录节点开始执行"""
        self.get_logger().info(f"[{self.get_node_type()}]节点 {self.node.base_info()} 开始执行，数据输入端口：{[port.id for port in self.node.inputs]}")

    def log_handle_start(self):
        """记录节点开始执行处理逻辑"""
        self.get_logger().info(f"[{self.get_node_type()}]节点 {self.node.base_info()} 开始执行数据处理逻辑")

    def log_execution_failed(self, error: Exception, msg: str = None):
        """记录节点执行失败"""
        self.get_logger().error(f"[{self.get_node_type()}]节点 {self.node} 执行失败{(': ' + msg) if msg else ''}", exc_info=error if self.context.debug else False)

    def log_handle_failed(self, error: Exception, msg: str = None):
        """记录节点数据处理逻辑失败"""
        self.get_logger().error(f"[{self.get_node_type()}]节点 {self.node} 处理逻辑失败{(': ' + msg) if msg else ''}", exc_info=error if self.context.debug else False)

    def log_validation_failed(self, error: Exception, msg: str = None):
        """记录节点数据验证失败"""
        self.get_logger().error(f"[{self.get_node_type()}]节点 {self.node} 数据验证失败{(': ' + msg) if msg else ''}", exc_info=error if self.context.debug else False)

    def log_info(self, msg: str):
        """记录节点信息"""
        self.get_logger().info(f"[{self.get_node_type()}]节点 {self.node.base_info()}: {msg}")

    def log_warning(self, msg: str):
        """记录节点警告信息"""
        self.get_logger().warning(f"[{self.get_node_type()}]节点 {self.node}: {msg}")

    def log_error(self, error: Exception, msg: str):
        """记录节点错误信息"""
        self.get_logger().error(f"[{self.get_node_type()}]节点 {self.node}: {msg}", exc_info=error if self.context.debug else None)

    def log_execution_succeed(self):
        """记录节点执行成功信息"""
        self.get_logger().info(f"[{self.get_node_type()}]节点 {self.node.base_info()} 执行成功")

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
        """处理并获取输入数据，指定端口ID获取指定的端口数据，否则获取全部数据，并预处理输入数据"""
        return self.process_input_data(self._get_input_data(port_id))

    def _get_input_data(self, port_id: Union[List[str], str] = None):
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
        if success:
            self.log_execution_succeed()
        return DefaultExecuteResult(
            node_id=self.node.id,
            output_data={port_id: result_data for port_id in output_port_ids},
            success=success,
            **kwargs
        )

    def process_input_data(self, input_port_datas, **kwargs):
        """处理输入数据"""
        return self.input_processor(input_port_datas, self=self, **kwargs)

    @staticmethod
    def default_input_processor(input_port_datas, **kwargs):
        return input_port_datas
