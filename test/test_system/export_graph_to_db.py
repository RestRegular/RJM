import json
from typing import Dict, List, Any

from data_flow import *
from data_flow.graph import Graph
from data_flow.graph_executor import GraphExecutor
from data_flow.node import Node
from data_flow.port import Port
from data_flow.edge import Edge
from data_flow.enum_data import BuiltinNodeType, DataType
from data_flow.node_config import NodeConfig
from data_flow.execution_context import ExecutionContext
from data_flow.graph_builder import GraphBuilder


def create_export_flow_graph() -> Graph:
    """
    创建用于导出图数据到MySQL的内置流程图

    该流程图包含以下处理步骤：
    1. 输入图数据
    2. 提取图基本信息
    3. 提取图节点数据
    4. 提取图边数据
    5. 提取节点配置信息
    6. 将数据写入MySQL数据库

    Returns:
        Graph: 配置好的图数据导出流程图
    """
    # 1. 创建图实例
    export_graph = Graph(
        name="图数据导出流程",
        description="将流程图数据（节点、边、配置）保存到MySQL数据库"
    )

    # 端口定义 - 定义数据流转的接口
    graph_raw_data_port = Port(id="graph_raw_data", name="图数据", data_type=DataType.LIST, required=True)
    graph_base_info_port = Port(id="graph_base_info", name="图基本信息", data_type=DataType.LIST, required=True)
    graph_node_data_port = Port(id="graph_node_data", name="图节点数据", data_type=DataType.LIST, required=True)
    graph_node_info_port = Port(id="graph_node_info", name="图节点信息", data_type=DataType.LIST, required=True)
    graph_edge_info_port = Port(id="graph_edge_info", name="图边信息", data_type=DataType.LIST, required=True)
    graph_node_config_info_port = Port(id="graph_node_config_info", name="图节点配置信息", data_type=DataType.LIST,
                                       required=True)
    write_result_port = Port(id="write_result", name="写入结果", data_type=DataType.ANY)

    # 2. 创建节点
    # 2.1 输入节点：提供待导出的图数据
    input_node = Node(
        name="图数据输入",
        type=BuiltinNodeType.INPUT,
        is_start=True,
        inputs=[],
        outputs=[graph_raw_data_port]
    )

    # 2.2 转换节点：将Graph对象转换为数据库存储格式

    # 提取图基本信息（ID、名称、描述、状态）
    extract_graph_base_info_node = Node(
        name="图基本数据提取",
        type=BuiltinNodeType.MAPPER,
        inputs=[graph_raw_data_port],
        outputs=[graph_base_info_port],
        config=MapperNodeConfig(
            map_handler=lambda port_datas, **kwargs: [
                {
                    "id": graph.id,
                    "name": graph.name,
                    "description": graph.description,
                    "status": str(graph.status)
                }
                for port_data in port_datas.values()
                for graph in port_data
            ]
        )
    )

    # 提取图中的所有节点对象
    extract_graph_node_data_node = Node(
        name="图节点数据提取",
        type=BuiltinNodeType.MAPPER,
        inputs=[graph_raw_data_port],
        outputs=[graph_node_data_port],
        config=MapperNodeConfig(
            map_handler=lambda port_datas, **kwargs: [
                node
                for port_data in port_datas.values()
                for graph in port_data
                for node in graph.nodes.values()
            ]
        )
    )

    # 从节点对象中提取节点信息（ID、图ID、名称、类型等）
    extract_graph_node_info_node = Node(
        name="图节点信息提取",
        type=BuiltinNodeType.MAPPER,
        inputs=[graph_node_data_port],
        outputs=[graph_node_info_port],
        config=MapperNodeConfig(
            map_handler=lambda port_datas, **kwargs: [
                {
                    "id": node.id,
                    "graph_id": kwargs.get("context", None).get_context("processing_graph", None).id,
                    "name": node.name,
                    "type": str(node.type),
                    "description": node.description,
                    "is_start": node.is_start,
                    "is_end": node.is_end,
                    "status": node.status,
                    "error": node.error
                }
                for nodes in port_datas.values()
                for node in nodes
            ]
        )
    )

    # 提取图中的所有边信息
    extract_graph_edge_info_node = Node(
        name="图边数据提取",
        type=BuiltinNodeType.MAPPER,
        inputs=[graph_raw_data_port],
        outputs=[graph_edge_info_port],
        config=MapperNodeConfig(
            map_handler=lambda port_datas, **kwargs: [
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
                for graph in port_data  # type: graph: Graph
                for edge in graph.edges  # type: edge: Edge
            ]
        )
    )

    # 提取节点配置信息
    extract_node_config_info_node = Node(
        name="提取节点配置信息",
        type=BuiltinNodeType.MAPPER,
        inputs=[graph_node_data_port],
        outputs=[graph_node_config_info_port],
        config=MapperNodeConfig(
            map_handler=lambda port_datas, **kwargs: [
                {
                    "node_id": node.id,
                    "config_key": key,
                    "config_value": '{"config": null}'
                }
                for nodes in port_datas.values()
                for node in nodes
                for key, config in node.config.config.items()
            ]
        )
    )

    # 2.3 数据库写入节点：将转换后的数据写入MySQL
    db_write_node = Node(
        name="图数据入库",
        type=BuiltinNodeType.DB_WRITE,
        is_end=True,
        inputs=[
            graph_base_info_port,
            graph_node_info_port,
            graph_node_config_info_port,
            graph_edge_info_port
        ],
        outputs=[write_result_port],
        config=DBWriteConfig(
            default_table="graph",
            db_conn=DBConnectionConfig(
                password="197346285",
                dbname="data_flow",
                user="root"
            ),
            port_table_mapping={
                graph_base_info_port.id: "graph",
                graph_node_info_port.id: "graph_node",
                graph_node_config_info_port.id: "graph_node_config",
                graph_edge_info_port.id: "graph_edge"
            }
        )
    )

    # 3. 添加节点到图
    export_graph.add_nodes(
        input_node,
        extract_graph_edge_info_node,
        extract_graph_base_info_node,
        extract_node_config_info_node,
        extract_graph_node_info_node,
        extract_graph_node_data_node,
        db_write_node
    )

    # 4. 创建边（定义数据流转关系）
    # 输入节点到各提取节点的边
    input_to_base_info_edge = Edge(
        source_node_id=input_node.id,
        source_port_id=graph_raw_data_port.id,
        target_node_id=extract_graph_base_info_node.id,
        target_port_id=graph_raw_data_port.id
    )
    input_to_node_data_edge = Edge(
        source_node_id=input_node.id,
        source_port_id=graph_raw_data_port.id,
        target_node_id=extract_graph_node_data_node.id,
        target_port_id=graph_raw_data_port.id
    )
    input_to_edge_info_edge = Edge(
        source_node_id=input_node.id,
        source_port_id=graph_raw_data_port.id,
        target_node_id=extract_graph_edge_info_node.id,
        target_port_id=graph_raw_data_port.id
    )

    # 节点数据到节点信息和配置信息的边
    node_data_to_node_info_edge = Edge(
        source_node_id=extract_graph_node_data_node.id,
        source_port_id=graph_node_data_port.id,
        target_node_id=extract_graph_node_info_node.id,
        target_port_id=graph_node_data_port.id
    )
    node_data_to_node_config_info_edge = Edge(
        source_node_id=extract_graph_node_data_node.id,
        source_port_id=graph_node_data_port.id,
        target_node_id=extract_node_config_info_node.id,
        target_port_id=graph_node_data_port.id
    )

    # 各提取节点到数据库写入节点的边
    base_info_to_db_edge = Edge(
        source_node_id=extract_graph_base_info_node.id,
        source_port_id=graph_base_info_port.id,
        target_node_id=db_write_node.id,
        target_port_id=graph_base_info_port.id
    )
    node_info_to_db_edge = Edge(
        source_node_id=extract_graph_node_info_node.id,
        source_port_id=graph_node_info_port.id,
        target_node_id=db_write_node.id,
        target_port_id=graph_node_info_port.id
    )
    node_config_info_to_db_edge = Edge(
        source_node_id=extract_node_config_info_node.id,
        source_port_id=graph_node_config_info_port.id,
        target_node_id=db_write_node.id,
        target_port_id=graph_node_config_info_port.id
    )
    edge_info_to_db_edge = Edge(
        source_node_id=extract_graph_edge_info_node.id,
        source_port_id=graph_edge_info_port.id,
        target_node_id=db_write_node.id,
        target_port_id=graph_edge_info_port.id
    )

    # 添加所有边到图
    export_graph.add_edges(
        input_to_base_info_edge,
        input_to_edge_info_edge,
        input_to_node_data_edge,
        node_data_to_node_info_edge,
        node_data_to_node_config_info_edge,
        base_info_to_db_edge,
        node_info_to_db_edge,
        node_config_info_to_db_edge,
        edge_info_to_db_edge
    )

    return export_graph


def create_export_flow_graph_by_graph_builder() -> Graph:
    """
    使用构建器模式创建图数据导出流程图

    Returns:
        Graph: 配置好的图数据导出流程图
    """
    builder = GraphBuilder(
        name="图数据导出流程",
        description="将流程图数据（节点、边、配置）保存到MySQL数据库"
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
            node
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
                "graph_id": kwargs.get("context", None).get_context("processing_graph", None).id,
                "name": node.name,
                "type": str(node.type),
                "description": node.description,
                "is_start": node.is_start,
                "is_end": node.is_end,
                "status": node.status,
                "error": node.error
            }
            for nodes in port_datas.values()
            for node in nodes
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
            for nodes in port_datas.values()
            for node in nodes
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
        table_mapping={
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


async def test_export_graph() -> tuple[GraphExecutor, Graph]:
    """
    测试图数据导出图功能

    Returns:
        tuple: (GraphExecutor, Graph) 执行器和执行后的图
    """
    # 创建导出流程图
    # graph = create_export_flow_graph()
    graph = create_export_flow_graph_by_graph_builder()

    # 创建执行上下文，传入待导出的图数据
    context = ExecutionContext(initial_data=[graph], processing_graph=graph)

    # 创建执行器并执行
    executor = GraphExecutor(graph, context)
    executed_graph = await executor.run()

    return executor, executed_graph


def main():
    """
    主函数：执行图数据导出测试并输出错误信息
    """
    import asyncio

    try:
        # 执行测试
        executor, graph = asyncio.run(test_export_graph())

        # 输出执行过程中的错误
        if graph.errors:
            print("执行过程中发现错误：")
            for error in graph.errors:
                print(f"错误: {error.error}")
        else:
            print("图数据导出流程执行成功，无错误")
            print(executor.get_node_results())

    except Exception as e:
        print(f"执行过程中发生异常: {e}")


if __name__ == '__main__':
    main()
