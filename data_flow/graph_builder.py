from typing import Dict, List, Any, Callable, Optional, Union

from pylint.checkers.utils import node_type

from data_flow import *
from data_flow.graph import Graph
from data_flow.node import Node
from data_flow.port import Port
from data_flow.edge import Edge


class GraphBuilder:
    """图构建器，提供简洁的API来创建数据流图"""

    def __init__(self, name: str, description: str = ""):
        """
        初始化图构建器

        Args:
            name: 图名称
            description: 图描述
        """
        self.graph = Graph(name=name, description=description)
        self.ports: Dict[str, Port] = {}
        self.nodes: Dict[str, Node] = {}
        self.port_counter = 0
        self.node_counter = 0

    def port(self, port_id: Optional[str] = None, name: str = "",
             data_type: DataType = DataType.ANY, required: bool = True) -> Port:
        """
        创建并注册一个端口

        Args:
            port_id: 端口ID，如果为None则自动生成
            name: 端口名称
            data_type: 数据类型
            required: 是否必需

        Returns:
            Port: 创建的端口对象
        """
        if port_id is None:
            port_id = f"port_{self.port_counter}"
            self.port_counter += 1

        port = Port(id=port_id, name=name, data_type=data_type, required=required)
        self.ports[port_id] = port
        return port

    def input_node(self, name: str, outputs: List[Port],
                    description: Optional[str] = None,
                    is_start: bool = False,
                    is_end: bool = False) -> Node:
        """
        创建输入节点

        Args:
            name: 节点名称
            outputs: 输出端口列表
            description: 节点描述
            is_start: 是否为起始节点
            is_end: 是否为终止节点

        Returns:
            Node: 创建的输入节点
        """
        node_id = f"node_{self.node_counter}"
        self.node_counter += 1

        return self.node(name, BuiltinNodeType.INPUT,[], outputs,
                         None, description, is_start, is_end)

    def mapper_node(self, name: str, inputs: List[Port], outputs: List[Port],
                    handler: Callable,
                    description: Optional[str] = None,
                    is_start: bool = False,
                    is_end: bool = False) -> Node:
        """
        创建映射节点

        Args:
            name: 节点名称
            inputs: 输入端口列表
            outputs: 输出端口列表
            handler: 数据处理函数
            description: 节点描述
            is_start: 是否为起始节点
            is_end: 是否为终止节点

        Returns:
            Node: 创建的映射节点
        """
        return self.node(name, BuiltinNodeType.MAPPER,
                         inputs, outputs, MapperNodeConfig(map_handler=handler),
                         description, is_start, is_end)

    def db_write_node(self, name: str, inputs: List[Port],
                      db_config: DBConnectionConfig,
                      table_mapping: Dict[str, str],
                      default_table: str = "graph",
                      description: Optional[str] = None,
                      is_start: bool = False,
                      is_end: bool = False) -> Node:
        """
        创建数据库写入节点

        Args:
            name: 节点名称
            inputs: 输入端口列表
            db_config: 数据库连接配置
            table_mapping: 端口到表的映射
            default_table: 默认表名
            description: 节点描述
            is_start: 是否为起始节点
            is_end: 是否为终止节点

        Returns:
            Node: 创建的数据库写入节点
        """
        result_port = self.port(name=f"{name}_result", data_type=DataType.ANY)

        return self.node(name, BuiltinNodeType.DB_WRITE,
                         inputs, [result_port],
                         DBWriteConfig(
                             default_table=default_table,
                             db_conn=db_config,
                             port_table_mapping=table_mapping),
                         description=description, is_start=is_start, is_end=is_end)

    def db_read_node(self, name: str, outputs: List[Port],
                     db_conn: DBConnectionConfig,
                     port_query_mapping: Dict[str, str],
                     batch_size: int = 1000, description: str = "",
                     is_start: bool = False, is_end: bool = False) -> Node:
        return self.node(name, BuiltinNodeType.DB_READ, [], outputs,
                         DBReadConfig(
                            batch_size=batch_size,
                            db_conn=db_conn,
                            port_query_mapping=port_query_mapping),
                         description=description, is_start=is_start, is_end=is_end)

    def filter_node(self, name: str, inputs: List[Port],
                    outputs: List[Port], handler: Callable,
                    description: Optional[str] = None,
                    is_start: bool = False,
                    is_end: bool = False) -> Node:
        """
        创建数据库写入节点

        Args:
            name: 节点名称
            inputs: 输入端口列表
            outputs: 输出端口列表
            handler: 筛选处理器
            description: 节点描述
            is_start: 是否为起始节点
            is_end: 是否为终止节点

        Returns:
            Node: 创建的数据库写入节点
        """
        return self.node(name, BuiltinNodeType.FILTER, inputs,
                         outputs, FilterNodeConfig(filter_handler=handler),
                         description=description, is_start=is_start, is_end=is_end)

    def output_node(self, name: str, inputs: List[str], outputs: List[str],
                    processor: Callable = None, description: str = "",
                    is_start: bool = False, is_end: bool = False) -> Node:
        """
        创建输出节点

        Args:
            name: 节点名称
            inputs: 输入端口列表
            outputs: 输出端口列表
            processor: 结果处理器
            description: 节点描述
            is_start: 是否为起始节点
            is_end: 是否为终止节点
        """
        return self.node(name, BuiltinNodeType.OUTPUT, inputs,
                         outputs,
                         OutputNodeConfig(data_processor=processor)
                         if processor else None,
                         description, is_start, is_end)

    def node(self, name: str,
             ntype: Union[str, BuiltinNodeType],
             inputs: List[Port], outputs: List[Port],
             config: Optional[NodeConfig] = None,
             description: Optional[str] = None,
             is_start: bool = False,
             is_end: bool = False) -> Node:
        node = Node(
            name=name,
            type=ntype,
            inputs=inputs,
            outputs=outputs,
            config=config,
            description=description,
            is_start=is_start,
            is_end=is_end
        )
        self.nodes[node.id] = node
        self.graph.add_node(node)
        return node

    def connect(self, source_node: Node, source_port: Port,
                target_node: Node, target_port: Port) -> Edge:
        """
        连接两个节点

        Args:
            source_node: 源节点
            source_port: 源端口
            target_node: 目标节点
            target_port: 目标端口

        Returns:
            Edge: 创建的边
        """
        edge = Edge(
            source_node_id=source_node.id,
            source_port_id=source_port.id,
            target_node_id=target_node.id,
            target_port_id=target_port.id
        )

        self.graph.add_edge(edge)
        return edge

    def auto_connect(self, source_node: Node, target_node: Node,
                     port_name_suffix: str = "") -> List[Edge]:
        """
        自动连接两个节点（基于端口名称匹配）

        Args:
            source_node: 源节点
            target_node: 目标节点
            port_name_suffix: 端口名称后缀，用于区分同名端口

        Returns:
            List[Edge]: 创建的边列表
        """
        edges = []

        for source_port in source_node.outputs:
            for target_port in target_node.inputs:
                source_port_name = source_port.name + port_name_suffix
                target_port_name = target_port.name + port_name_suffix

                if source_port_name == target_port_name:
                    edge = self.connect(source_node, source_port, target_node, target_port)
                    edges.append(edge)

        return edges

    def build(self) -> Graph:
        """
        构建最终的图

        Returns:
            Graph: 构建完成的图
        """
        return self.graph
