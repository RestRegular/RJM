import json
import logging
from typing import Dict, List, Any

from data_flow import *
from data_flow.graph import Graph
from data_flow.graph_executor import GraphExecutor
from data_flow.node import Node
from data_flow.port import Port
from data_flow.edge import Edge
from data_flow.enum_data import BuiltinNodeType, DataType
from data_flow.execution_context import ExecutionContext
from data_flow.graph_builder import GraphBuilder
from utils.data_visitor import DataVisitor


def create_import_flow_graph() -> Graph:
    """
    创建用于从MySQL导入图数据的内置流程图

    该流程图包含以下处理步骤：
    1. 从MySQL数据库读取图数据（基本信息、节点、边、配置）
    2. 转换节点数据为Node对象
    3. 转换边数据为Edge对象
    4. 组装完整的Graph对象
    5. 输出导入的图数据

    Returns:
        Graph: 配置好的图数据导入流程图
    """
    # 1. 创建图实例
    import_graph = Graph(
        name="图数据导入流程",
        description="从MySQL数据库读取并重建流程图数据（节点、边、配置）"
    )

    # 端口定义 - 定义数据流转的接口
    graph_base_info_port = Port(id="graph_base_info", name="图基本信息", data_type=DataType.LIST, required=True)
    graph_node_info_port = Port(id="graph_node_info", name="图节点信息", data_type=DataType.LIST, required=True)
    graph_edge_info_port = Port(id="graph_edge_info", name="图边信息", data_type=DataType.LIST, required=True)
    graph_node_config_info_port = Port(id="graph_node_config_info", name="图节点配置信息", data_type=DataType.LIST,
                                       required=True)

    node_objects_port = Port(id="node_objects", name="节点对象列表", data_type=DataType.LIST, required=True)
    edge_objects_port = Port(id="edge_objects", name="边对象列表", data_type=DataType.LIST, required=True)
    assembled_graph_port = Port(id="assembled_graph", name="组装后的图对象", data_type=DataType.OBJECT, required=True)
    import_result_port = Port(id="import_result", name="导入结果", data_type=DataType.ANY)

    # 2. 创建节点
    # 2.1 数据库读取节点：从MySQL读取图数据
    db_read_node = Node(
        name="图数据读取",
        type=BuiltinNodeType.DB_READ,
        is_start=True,
        inputs=[],
        outputs=[
            graph_base_info_port,
            graph_node_info_port,
            graph_edge_info_port,
            graph_node_config_info_port
        ],
        config=DBReadConfig(
            db_conn=DBConnectionConfig(
                password="197346285",
                dbname="data_flow",
                user="root"
            ),
            port_query_mapping={
                graph_base_info_port.id: "SELECT * FROM graph",
                graph_node_info_port.id: "SELECT * FROM graph_node",
                graph_edge_info_port.id: "SELECT * FROM graph_edge",
                graph_node_config_info_port.id: "SELECT * FROM graph_node_config"
            }
        )
    )

    # 2.2 转换节点：将数据库记录转换为对象
    convert_nodes_node = Node(
        name="节点数据转换",
        type=BuiltinNodeType.MAPPER,
        inputs=[graph_node_info_port, graph_node_config_info_port],
        outputs=[node_objects_port],
        config=MapperNodeConfig(
            map_handler=lambda port_datas, **kwargs: [
                Node(
                    id=node_info["id"],
                    name=node_info["name"],
                    type=node_info["type"],
                    is_start=node_info["is_start"],
                    is_end=node_info["is_end"],
                    inputs=[],  # 简化处理，实际可能需要更复杂的端口重建逻辑
                    outputs=[],
                    description=node_info["description"],
                    status=node_info["status"],
                    error=node_info["error"],
                    config=NodeConfig(
                        **{
                            cfg["config_key"]: cfg["config_value"]
                            for cfg in port_datas[graph_node_config_info_port.id]
                            if cfg["node_id"] == node_info["id"]
                        }
                    )
                )
                for node_info in port_datas[graph_node_info_port.id]
            ]
        )
    )

    # 转换边数据为Edge对象
    convert_edges_node = Node(
        name="边数据转换",
        type=BuiltinNodeType.MAPPER,
        inputs=[graph_edge_info_port],
        outputs=[edge_objects_port],
        config=MapperNodeConfig(
            map_handler=lambda port_datas, **kwargs: [
                Edge(
                    id=edge_info["id"],
                    source_node_id=edge_info["source_node_id"],
                    source_port_id=edge_info["source_port_id"],
                    target_node_id=edge_info["target_node_id"],
                    target_port_id=edge_info["target_port_id"],
                    enabled=edge_info["enabled"]
                )
                for edge_info in port_datas[graph_edge_info_port.id]
            ]
        )
    )

    # 组装完整的Graph对象
    assemble_graph_node = Node(
        name="图对象组装",
        type=BuiltinNodeType.MAPPER,
        inputs=[graph_base_info_port, node_objects_port, edge_objects_port],
        outputs=[assembled_graph_port],
        config=MapperNodeConfig(
            map_handler=lambda port_datas, **kwargs: [
                Graph(
                    id=base_info["id"],
                    name=base_info["name"],
                    description=base_info["description"],
                    status=base_info["status"]
                ).add_nodes(*port_datas[node_objects_port.id]).add_edges(*port_datas[edge_objects_port.id])
                for base_info in port_datas[graph_base_info_port.id]
            ]
        )
    )

    # 2.3 输出节点：输出导入的图数据
    output_node = Node(
        name="图数据输出",
        type=BuiltinNodeType.OUTPUT,
        is_end=True,
        inputs=[assembled_graph_port],
        outputs=[import_result_port]
    )

    # 3. 添加节点到图
    import_graph.add_nodes(
        db_read_node,
        convert_nodes_node,
        convert_edges_node,
        assemble_graph_node,
        output_node
    )

    # 4. 创建边（定义数据流转关系）
    # 读取节点到转换节点的边
    read_to_nodes_edge = Edge(
        source_node_id=db_read_node.id,
        source_port_id=graph_node_info_port.id,
        target_node_id=convert_nodes_node.id,
        target_port_id=graph_node_info_port.id
    )
    read_to_node_configs_edge = Edge(
        source_node_id=db_read_node.id,
        source_port_id=graph_node_config_info_port.id,
        target_node_id=convert_nodes_node.id,
        target_port_id=graph_node_config_info_port.id
    )
    read_to_edges_edge = Edge(
        source_node_id=db_read_node.id,
        source_port_id=graph_edge_info_port.id,
        target_node_id=convert_edges_node.id,
        target_port_id=graph_edge_info_port.id
    )
    read_to_base_info_edge = Edge(
        source_node_id=db_read_node.id,
        source_port_id=graph_base_info_port.id,
        target_node_id=assemble_graph_node.id,
        target_port_id=graph_base_info_port.id
    )

    # 转换节点到组装节点的边
    nodes_to_assemble_edge = Edge(
        source_node_id=convert_nodes_node.id,
        source_port_id=node_objects_port.id,
        target_node_id=assemble_graph_node.id,
        target_port_id=node_objects_port.id
    )
    edges_to_assemble_edge = Edge(
        source_node_id=convert_edges_node.id,
        source_port_id=edge_objects_port.id,
        target_node_id=assemble_graph_node.id,
        target_port_id=edge_objects_port.id
    )

    # 组装节点到输出节点的边
    assemble_to_output_edge = Edge(
        source_node_id=assemble_graph_node.id,
        source_port_id=assembled_graph_port.id,
        target_node_id=output_node.id,
        target_port_id=assembled_graph_port.id
    )

    # 添加所有边到图
    import_graph.add_edges(
        read_to_nodes_edge,
        read_to_node_configs_edge,
        read_to_edges_edge,
        read_to_base_info_edge,
        nodes_to_assemble_edge,
        edges_to_assemble_edge,
        assemble_to_output_edge
    )

    return import_graph


def create_import_flow_graph_by_graph_builder() -> Graph:
    """
    使用构建器模式创建图数据导入流程图

    Returns:
        Graph: 配置好的图数据导入流程图
    """
    builder = GraphBuilder(
        name="图数据导入流程",
        description="从MySQL数据库读取并重建流程图数据（节点、边、配置）",
        log_level=logging.DEBUG
    )

    # 定义所有端口
    graph_target_id = builder.port("graph_target_id", "指定的流转图数据", DataType.LIST)
    graph_base_info = builder.port("graph_base_info", "图基本信息", DataType.LIST)
    graph_node_info = builder.port("graph_node_info", "图节点信息", DataType.LIST)
    graph_edge_info = builder.port("graph_edge_info", "图边信息", DataType.LIST)
    graph_node_config_info = builder.port("graph_node_config_info", "图节点配置信息", DataType.LIST)

    node_objects = builder.port("node_objects", "节点对象列表", DataType.LIST)
    edge_objects = builder.port("edge_objects", "边对象列表", DataType.LIST)
    assembled_graph = builder.port("assembled_graph", "组装后的图对象", DataType.ANY)
    import_result = builder.port("import_result", "导入结果", DataType.ANY)

    # 创建节点
    # 获取指定流转图ID数据节点
    input_node = builder.input_node(
        name="指定的流转图ID数据输入节点",
        outputs=[graph_target_id],
        description="获取指定流转图ID数据: List[str]",
        is_start=True
    )
    # 数据库读取节点
    def db_read_input_processor(port_datas, self: DBReadExecutor, **kwargs):
        # 需要根据输入的指定流转图ID数据生成查询语句
        target_id_str = ', '.join([
            f"'{graph_id}'"
            for port_data in port_datas.values()
            for graph_id in port_data
        ])
        graph_base_info_sql = "SELECT * FROM graph WHERE id IN (" + target_id_str + ")"
        graph_node_info_sql = "SELECT * FROM graph_node WHERE graph_id IN (" + target_id_str + ")"
        graph_edge_info_sql = "SELECT * FROM graph_edge WHERE graph_id IN (" + target_id_str + ")"
        graph_node_config_info_sql = "SELECT * FROM graph_node_config"

        port_query_mapping = {
            graph_base_info.id: graph_base_info_sql,
            graph_node_info.id: graph_node_info_sql,
            graph_edge_info.id: graph_edge_info_sql,
            graph_node_config_info.id: graph_node_config_info_sql
        }
        self.node.set_config("port_query_mapping", port_query_mapping)
        return port_datas

    db_read_node = builder.db_read_node(
        name="图数据读取",
        inputs=[graph_target_id],
        outputs=[graph_base_info, graph_node_info, graph_edge_info, graph_node_config_info],
        db_conn=DBConnectionConfig(
            password="197346285",
            dbname="data_flow",
            user="root"
        ),
        port_query_mapping={},
        description="从MySQL数据库读取图的基本信息、节点、边和配置数据",
        is_start=True,
        input_processor=db_read_input_processor
    )

    # 节点数据转换
    convert_nodes = builder.mapper_node(
        name="节点数据转换",
        inputs=[graph_node_info, graph_node_config_info],
        outputs=[node_objects],
        handler=lambda port_datas, **kwargs: [
            Node(
                id=node_info["id"],
                name=node_info["name"],
                type=node_info["type"],
                is_start=node_info["is_start"],
                is_end=node_info["is_end"],
                inputs=[Port.model_validate(port) for port in json.loads(node_info["inputs"])],
                outputs=[Port.model_validate(port) for port in json.loads(node_info["outputs"])],
                description=node_info["description"],
                status=node_info["status"],
                error=node_info["error"],
                config=NodeConfig(
                    **{
                        cfg["config_key"]: cfg["config_value"]
                        for cfg in port_datas[graph_node_config_info.id]
                        if cfg["node_id"] == node_info["id"]
                    }
                )
            )
            for node_info in port_datas[graph_node_info.id]
        ],
        description="将数据库中的节点记录转换为Node对象，并关联配置信息"
    )

    # 边数据转换
    convert_edges = builder.mapper_node(
        name="边数据转换",
        inputs=[graph_edge_info],
        outputs=[edge_objects],
        handler=lambda port_datas, **kwargs: [
            Edge(
                id=edge_info["id"],
                source_node_id=edge_info["source_node_id"],
                source_port_id=edge_info["source_port_id"],
                target_node_id=edge_info["target_node_id"],
                target_port_id=edge_info["target_port_id"],
                enabled=edge_info["enabled"]
            )
            for edge_info in port_datas[graph_edge_info.id]
        ],
        description="将数据库中的边记录转换为Edge对象"
    )

    # 图对象组装
    assemble_graph = builder.mapper_node(
        name="图对象组装",
        inputs=[graph_base_info, node_objects, edge_objects],
        outputs=[assembled_graph],
        handler=lambda port_datas, **kwargs: [
            Graph(
                id=base_info["id"],
                name=base_info["name"],
                description=base_info["description"],
                status=base_info["status"]
            ).add_nodes(*port_datas[node_objects.id]) \
                .add_edge_list(port_datas[edge_objects.id])
            for base_info in port_datas[graph_base_info.id]
        ],
        description="将图基本信息、节点对象和边对象组装成完整的Graph对象"
    )

    # 输出节点
    output_node = builder.output_node(
        name="图数据输出",
        inputs=[assembled_graph],
        outputs=[import_result],
        description="输出导入成功的图对象，作为整个导入流程的结果",
        is_end=True
    )

    # 连接所有节点
    builder.connect(input_node, graph_target_id, db_read_node, graph_target_id)
    builder.connect(db_read_node, graph_node_info, convert_nodes, graph_node_info)
    builder.connect(db_read_node, graph_node_config_info, convert_nodes, graph_node_config_info)
    builder.connect(db_read_node, graph_edge_info, convert_edges, graph_edge_info)
    builder.connect(db_read_node, graph_base_info, assemble_graph, graph_base_info)
    builder.connect(convert_nodes, node_objects, assemble_graph, node_objects)
    builder.connect(convert_edges, edge_objects, assemble_graph, edge_objects)
    builder.connect(assemble_graph, assembled_graph, output_node, assembled_graph)

    return builder.build()


async def test_import_graph() -> tuple[GraphExecutor, Graph, List[Graph]]:
    """
    测试图数据导入图功能

    Returns:
        tuple: (GraphExecutor, Graph) 执行器和执行后的图
    """
    # 创建导入流程图
    # graph = create_import_flow_graph()
    graph = create_import_flow_graph_by_graph_builder()

    # 创建执行上下文
    context = ExecutionContext(
        initial_data=[
            # "893e98f1-2c28-4f92-a1aa-9506652b611d",
            "9b36d621-22b5-40c5-8c93-4adef31c0e24"
        ],
        debug=True,
        log_level=logging.DEBUG
    )

    # 创建执行器并执行
    executor = GraphExecutor(graph, context)
    executed_graph = await executor.run()

    results = DataVisitor(executed_graph.search_node_by_name("图数据输出").result.get_result_data())["import_result.assembled_graph"]  # type: List[Graph]
    for result in results:
        result.status = GraphStatus.PENDING
    return executor, executed_graph, results


def main():
    """
    主函数：执行图数据导入测试并输出结果
    """
    import asyncio

    try:
        # 执行测试
        executor, graph, result_graphs = asyncio.run(test_import_graph())

        # 输出执行过程中的错误
        if graph.errors:
            print("执行过程中发现错误：")
            for error in graph.errors:
                print(f"错误: {error.error}")
        else:
            print("图数据导入流程执行成功，无错误")
            print("导入结果：")
            for result_graph in result_graphs:
                print(result_graph)

    except Exception as e:
        print(f"执行过程中发生异常: {e}")


if __name__ == '__main__':
    main()
