"""
这幅流转图用于将流转图序列化并存储于 mysql 数据库中
"""
import json

from data_flow import *
from data_flow.graph import Graph
from data_flow.enum_data import DataType
from data_flow.node_config import NodeConfig
from data_flow.graph_builder import GraphBuilder


def build_graph_for_storage_graph() -> Graph:
    """
    使用构建器模式创建存储流转图数据流转图

    Returns:
        Graph: 配置好的用于存储流转图的流转图
    """
    builder = GraphBuilder(
        name="存储流转图",
        description="将流转图数据（节点、边、配置）保存到MySQL数据库"
    )

    # 定义所有端口
    graph_raw_data = builder.port("graph_raw_data", "图数据", DataType.LIST)
    graph_base_info = builder.port("graph_base_info", "图基本信息", DataType.LIST)
    graph_node_data = builder.port("graph_node_data", "图节点数据", DataType.LIST)
    graph_node_info = builder.port("graph_node_info", "图节点信息", DataType.LIST)
    graph_edge_info = builder.port("graph_edge_info", "图边信息", DataType.LIST)
    graph_node_config_info = builder.port("graph_node_config_info", "图节点配置信息", DataType.LIST)

    # 创建节点
    input_node = builder.input_node(
        name="图数据输入",
        outputs=[graph_raw_data],
        description="接收待导出的原始图数据，作为整个导出流程的数据源起点",
        is_start=True
    )

    # 提取图基本信息
    extract_base_info = builder.mapper_node(
        name="图基本数据提取",
        inputs=[graph_raw_data],
        outputs=[graph_base_info],
        handler=lambda port_datas, **kwargs: [
            {
                "id": graph.id,
                "name": graph.name,
                "description": graph.description,
                "status": str(graph.status)
            }
            for port_data in port_datas.values()
            for graph in port_data
        ],
        description="从原始图数据中提取图的基础属性，包括ID、名称、描述和状态"
    )

    # 提取图节点数据
    extract_node_data = builder.mapper_node(
        name="图节点数据提取",
        inputs=[graph_raw_data],
        outputs=[graph_node_data],
        handler=lambda port_datas, **kwargs: [
            (node, graph.id)
            for port_data in port_datas.values()
            for graph in port_data
            for node in graph.nodes.values()
        ],
        description="从原始图数据中提取所有节点对象，为后续节点信息解析提供数据"
    )

    # 提取图节点信息
    extract_node_info = builder.mapper_node(
        name="图节点信息提取",
        inputs=[graph_node_data],
        outputs=[graph_node_info],
        handler=lambda port_datas, **kwargs: [
            {
                "id": node.id,
                "graph_id": graph_id,
                "name": node.name,
                "type": str(node.type),
                "description": node.description,
                "is_start": node.is_start,
                "is_end": node.is_end,
                "inputs": json.dumps([port.model_dump() for port in node.inputs]),
                "outputs": json.dumps([port.model_dump() for port in node.outputs]),
                "status": node.status,
                "error": node.error
            }
            for node_datas in port_datas.values()
            for node, graph_id in node_datas
        ],
        description="从节点对象中提取详细信息，包括节点ID、所属图ID、类型等元数据"
    )

    # 提取图边信息
    extract_edge_info = builder.mapper_node(
        name="图边数据提取",
        inputs=[graph_raw_data],
        outputs=[graph_edge_info],
        handler=lambda port_datas, **kwargs: [
            {
                "id": edge.id,
                "graph_id": graph.id,
                "source_node_id": edge.source_node_id,
                "source_port_id": edge.source_port_id,
                "target_node_id": edge.target_node_id,
                "target_port_id": edge.target_port_id,
                "enabled": edge.enabled
            }
            for port_data in port_datas.values()
            for graph in port_data
            for edge in graph.edges
        ],
        description="从原始图数据中提取所有边的连接信息，包括源节点、目标节点及端口映射关系"
    )

    # 提取节点配置信息
    extract_config_info = builder.mapper_node(
        name="提取节点配置信息",
        inputs=[graph_node_data],
        outputs=[graph_node_config_info],
        handler=lambda port_datas, **kwargs: [
            {
                "node_id": node.id,
                "config_key": key,
                "config_value": '{"config": null}'
            }
            for node_datas in port_datas.values()
            for node, graph_id in node_datas
            for key, config in node.config.config.items()
        ],
        description="从节点对象中提取配置信息，包括配置项的键值对，用于数据库存储"
    )

    # 创建数据库写入节点
    db_write_node = builder.db_write_node(
        name="图数据入库",
        inputs=[graph_base_info, graph_node_info, graph_node_config_info, graph_edge_info],
        db_config=DBConnectionConfig(
            password="197346285",
            dbname="data_flow",
            user="root"
        ),
        port_table_mapping={
            graph_base_info.id: "graph",
            graph_node_info.id: "graph_node",
            graph_node_config_info.id: "graph_node_config",
            graph_edge_info.id: "graph_edge"
        },
        description="将提取的图基本信息、节点信息、配置信息和边信息分别写入对应的数据表",
        is_end=True
    )

    # 连接所有节点
    builder.connect(input_node, graph_raw_data, extract_base_info, graph_raw_data)
    builder.connect(input_node, graph_raw_data, extract_node_data, graph_raw_data)
    builder.connect(input_node, graph_raw_data, extract_edge_info, graph_raw_data)
    builder.connect(extract_node_data, graph_node_data, extract_node_info, graph_node_data)
    builder.connect(extract_node_data, graph_node_data, extract_config_info, graph_node_data)
    builder.connect(extract_base_info, graph_base_info, db_write_node, graph_base_info)
    builder.connect(extract_node_info, graph_node_info, db_write_node, graph_node_info)
    builder.connect(extract_config_info, graph_node_config_info, db_write_node, graph_node_config_info)
    builder.connect(extract_edge_info, graph_edge_info, db_write_node, graph_edge_info)

    return builder.build()
