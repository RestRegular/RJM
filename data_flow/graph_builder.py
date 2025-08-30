from typing import Dict, List, Any, Callable, Optional, Union
import logging

from data_flow import *
from data_flow.graph import Graph
from data_flow.node import Node
from data_flow.port import Port
from data_flow.edge import Edge
from utils.log_system import get_logger

logger = get_logger(__name__)


class GraphBuilder:
    """图构建器，提供简洁的API来创建数据流图"""

    def __init__(self, name: str, description: str = "", log_level: int = logging.NOTSET):
        """
        初始化图构建器

        Args:
            name: 图名称
            description: 图描述
        """
        logger.setLevel(log_level)
        logger.info(f"初始化图构建器，名称: {name}，描述: {description}")
        self.graph = Graph(name=name, description=description)
        self.ports: Dict[str, Port] = {}
        self.nodes: Dict[str, Node] = {}
        self.port_counter = 0
        self.node_counter = 0
        logger.debug(f"图构建器初始化完成，当前端口数: {len(self.ports)}，节点数: {len(self.nodes)}")

    def port(self, port_id: Optional[str] = None, name: str = "",
             data_type: Union[DataType, str] = DataType.ANY, required: bool = True) -> Port:
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
        logger.debug(f"开始创建端口，名称: {name}，数据类型: {data_type}，必需: {required}")

        if port_id is None:
            port_id = f"port_{self.port_counter}"
            self.port_counter += 1
            logger.debug(f"自动生成端口ID: {port_id}")

        port = Port(id=port_id, name=name, data_type=data_type, required=required)
        self.ports[port_id] = port
        logger.info(f"端口创建成功: {str(port)}")
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
        logger.info(f"开始创建输入节点: {name}")
        logger.debug(f"输出端口数量: {len(outputs)}，是否为起始节点: {is_start}，是否为终止节点: {is_end}")

        node_id = f"node_{self.node_counter}"
        self.node_counter += 1
        logger.debug(f"分配节点ID: {node_id}")

        node = self.node(name, BuiltinNodeType.INPUT, [], outputs,
                         None, description, is_start, is_end)
        logger.info(f"输入节点创建完成: {str(node)}")
        return node

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
        logger.info(f"开始创建映射节点: {name}")
        logger.debug(f"输入端口数量: {len(inputs)}，输出端口数量: {len(outputs)}，处理器: {handler.__name__ if hasattr(handler, '__name__') else '匿名函数'}")

        node = self.node(name, BuiltinNodeType.MAPPER,
                         inputs, outputs, MapperNodeConfig(map_handler=handler),
                         description, is_start, is_end)
        logger.info(f"映射节点创建完成: {str(node)}")
        return node

    def db_write_node(self, name: str, inputs: List[Port],
                      db_config: DBConnectionConfig,
                      port_table_mapping: Dict[str, str],
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
            port_table_mapping: 端口到表的映射
            default_table: 默认表名
            description: 节点描述
            is_start: 是否为起始节点
            is_end: 是否为终止节点

        Returns:
            Node: 创建的数据库写入节点
        """
        logger.info(f"开始创建数据库写入节点: {name}")
        logger.debug(f"输入端口数量: {len(inputs)}，数据库配置: {db_config}，端口表映射: {port_table_mapping}")

        result_port = self.port(name=f"{name}_result", data_type=DataType.ANY)
        logger.debug(f"创建结果端口: {str(result_port)}")

        node = self.node(name, BuiltinNodeType.DB_WRITE,
                         inputs, [result_port],
                         DBWriteConfig(
                             default_table=default_table,
                             db_conn=db_config,
                             port_table_mapping=port_table_mapping),
                         description=description, is_start=is_start, is_end=is_end)
        logger.info(f"数据库写入节点创建完成: {str(node)}")
        return node

    def db_read_node(self,
                     name: str,
                     inputs: List[Port],
                     outputs: List[Port],
                     db_conn: DBConnectionConfig,
                     port_query_mapping: Dict[str, str],
                     batch_size: int = 1000, description: str = "",
                     is_start: bool = False, is_end: bool = False,
                     **more_configs) -> Node:
        logger.info(f"开始创建数据库读取节点: {name}")
        logger.debug(f"输入端口数量: {len(inputs)}，输出端口数量: {len(outputs)}，数据库配置: {db_conn}，批次大小: {batch_size}")

        node = self.node(
            name,
            BuiltinNodeType.DB_READ,
            inputs, outputs,
            DBReadConfig(
                batch_size=batch_size,
                db_conn=db_conn,
                port_query_mapping=port_query_mapping,
                **more_configs
            ),
            description=description, is_start=is_start, is_end=is_end)
        logger.info(f"数据库读取节点创建完成: {str(node)}")
        return node

    def filter_node(self, name: str, inputs: List[Port],
                    outputs: List[Port], handler: Callable,
                    description: Optional[str] = None,
                    is_start: bool = False,
                    is_end: bool = False) -> Node:
        """
        创建筛选节点

        Args:
            name: 节点名称
            inputs: 输入端口列表
            outputs: 输出端口列表
            handler: 筛选处理器
            description: 节点描述
            is_start: 是否为起始节点
            is_end: 是否为终止节点

        Returns:
            Node: 创建的筛选节点
        """
        logger.info(f"开始创建筛选节点: {name}")
        logger.debug(f"输入端口数量: {len(inputs)}，输出端口数量: {len(outputs)}，筛选处理器: {handler.__name__ if hasattr(handler, '__name__') else '匿名函数'}")

        node = self.node(name, BuiltinNodeType.FILTER, inputs,
                         outputs, FilterNodeConfig(filter_handler=handler),
                         description=description, is_start=is_start, is_end=is_end)
        logger.info(f"筛选节点创建完成: {str(node)}")
        return node

    def output_node(self, name: str, inputs: List[Port], outputs: List[Port],
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
        logger.info(f"开始创建输出节点: {name}")
        logger.debug(f"输入端口数量: {len(inputs)}，输出端口数量: {len(outputs)}，处理器: {processor.__name__ if processor and hasattr(processor, '__name__') else '无'}")

        node = self.node(name, BuiltinNodeType.OUTPUT, inputs,
                         outputs,
                         OutputNodeConfig(data_processor=processor)
                         if processor else None,
                         description, is_start, is_end)
        logger.info(f"输出节点创建完成: {str(node)}")
        return node

    def node(self, name: str,
             ntype: Union[str, BuiltinNodeType],
             inputs: List[Port], outputs: List[Port],
             config: Optional[NodeConfig] = None,
             description: Optional[str] = None,
             is_start: bool = False,
             is_end: bool = False) -> Node:
        logger.debug(f"创建通用节点: {name}，类型: {ntype}")
        logger.debug(f"输入端口: {[port.id for port in inputs]}，输出端口: {[port.id for port in outputs]}")

        node = Node(
            name=name,
            type=ntype,
            inputs=inputs,
            outputs=outputs,
            config=config or NodeConfig(),
            description=description,
            is_start=is_start,
            is_end=is_end
        )
        self.nodes[node.id] = node
        self.graph.add_node(node)
        logger.debug(f"节点已添加到图和内部字典: {node.id}")
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
        logger.info(f"开始连接节点: {source_node.id}:{source_port.id} -> {target_node.id}:{target_port.id}")

        edge = Edge(
            source_node_id=source_node.id,
            source_port_id=source_port.id,
            target_node_id=target_node.id,
            target_port_id=target_port.id
        )

        self.graph.add_edge(edge)
        logger.info(f"边创建成功并添加到图: {str(edge)}")
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
        logger.info(f"开始自动连接节点: {source_node.id} -> {target_node.id}，端口后缀: '{port_name_suffix}'")
        logger.debug(f"源节点输出端口: {[port.name for port in source_node.outputs]}")
        logger.debug(f"目标节点输入端口: {[port.name for port in target_node.inputs]}")

        edges = []
        matched_count = 0

        for source_port in source_node.outputs:
            for target_port in target_node.inputs:
                source_port_name = source_port.name + port_name_suffix
                target_port_name = target_port.name + port_name_suffix

                if source_port_name == target_port_name:
                    edge = self.connect(source_node, source_port, target_node, target_port)
                    edges.append(edge)
                    matched_count += 1
                    logger.debug(f"端口匹配成功: {source_port_name} -> {target_port_name}")

        logger.info(f"自动连接完成，共创建 {matched_count} 条边，总边数: {len(edges)}")
        return edges

    def build(self) -> Graph:
        """
        构建最终的图

        Returns:
            Graph: 构建完成的图
        """
        logger.info("开始构建最终图")
        logger.debug(f"图统计 - 节点数: {len(self.graph.nodes)}，边数: {len(self.graph.edges)}，端口数: {len(self.ports)}")

        # 检查图的基本完整性
        if len(self.graph.nodes) == 0:
            logger.warning("图中没有节点")
        if len(self.graph.edges) == 0:
            logger.warning("图中没有边")

        # 检查起始节点
        start_nodes = [node for node in self.graph.nodes.values() if node.is_start]
        if len(start_nodes) == 0:
            logger.warning("图中没有设置起始节点")
        else:
            logger.info(f"图中包含 {len(start_nodes)} 个起始节点")

        # 检查终止节点
        end_nodes = [node for node in self.graph.nodes.values() if node.is_end]
        if len(end_nodes) == 0:
            logger.warning("图中没有设置终止节点")
        else:
            logger.info(f"图中包含 {len(end_nodes)} 个终止节点")

        logger.info(f"图构建完成: {str(self.graph)}")
        return self.graph
