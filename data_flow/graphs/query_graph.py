"""
这幅流转图用于从 Mysql 数据库中查询指定 id 的流转图
"""
import json

from data_flow import *
from data_flow.graph import Graph
from data_flow.graph_builder import GraphBuilder


def build_graph_for_query_graph() -> Graph:
    """
    使用构建器模式创建查询流转图

    Returns:
        Graph: 配置好的查询流转图
    """
    builder = GraphBuilder(
        name="查询流转图",
        description="从MySQL数据库读取并重建流转图数据（节点、边、配置）",
        log_level=logging.DEBUG
    )

    # 定义所有端口
    graph_base_info = builder.port("graph_base_info", "图基本信息", DataType.LIST)
    graph_node_info = builder.port("graph_node_info", "图节点信息", DataType.LIST)
    graph_edge_info = builder.port("graph_edge_info", "图边信息", DataType.LIST)
    graph_node_config_info = builder.port("graph_node_config_info", "图节点配置信息", DataType.LIST)

    node_objects = builder.port("node_objects", "节点对象列表", DataType.LIST)
    edge_objects = builder.port("edge_objects", "边对象列表", DataType.LIST)
    assembled_graph = builder.port("assembled_graph", "组装后的图对象", DataType.ANY)
    import_result = builder.port("import_result", "导入结果", DataType.ANY)

    # 创建节点
    # 数据库读取节点
    db_read_node = builder.db_read_node(
        name="图数据读取",
        outputs=[graph_base_info, graph_node_info, graph_edge_info, graph_node_config_info],
        db_conn=DBConnectionConfig(
            password="197346285",
            dbname="data_flow",
            user="root"
        ),
        port_query_mapping={
            graph_base_info.id: "SELECT * FROM graph",
            graph_node_info.id: "SELECT * FROM graph_node",
            graph_edge_info.id: "SELECT * FROM graph_edge",
            graph_node_config_info.id: "SELECT * FROM graph_node_config"
        },
        description="从MySQL数据库读取图的基本信息、节点、边和配置数据",
        is_start=True
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
    builder.connect(db_read_node, graph_node_info, convert_nodes, graph_node_info)
    builder.connect(db_read_node, graph_node_config_info, convert_nodes, graph_node_config_info)
    builder.connect(db_read_node, graph_edge_info, convert_edges, graph_edge_info)
    builder.connect(db_read_node, graph_base_info, assemble_graph, graph_base_info)
    builder.connect(convert_nodes, node_objects, assemble_graph, node_objects)
    builder.connect(convert_edges, edge_objects, assemble_graph, edge_objects)
    builder.connect(assemble_graph, assembled_graph, output_node, assembled_graph)

    return builder.build()
