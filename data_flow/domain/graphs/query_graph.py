"""
这幅流转图用于从 Mysql 数据库中查询指定 id 的流转图
"""
import json
import logging
from typing import Optional

from data_flow.domain import *
from data_flow.domain.node import Node
from data_flow.domain.port import Port
from data_flow.domain.edge import Edge
from data_flow.domain.graph import Graph
from data_flow.domain.node_config import NodeConfig
from data_flow.domain.graph_builder import GraphBuilder


query_graph: Optional[Graph] = None


def build_graph_for_query_graph() -> Graph:
    """
    使用构建器模式创建图数据导入流程图

    Returns:
        Graph: 配置好的图数据导入流程图
    """
    # global query_graph
    #
    # if query_graph:
    #     query_graph.reset_status()
    #     return query_graph

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
        self.node.set_config("db_conn", self.context.get_context(
            "db_conn_config", self.node.get_config("db_conn")))
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
    def convert_nodes_handler(port_datas, **kwargs):
        node_objs = {}
        for node_info in port_datas[graph_node_info.id]:
            node = Node(
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
            if node_info["graph_id"] not in node_objs:
                node_objs[node_info["graph_id"]] = []
            node_objs[node_info["graph_id"]].append(node)
        return [{graph_id: nodes} for graph_id, nodes in node_objs.items()]

    convert_nodes = builder.mapper_node(
        name="节点数据转换",
        inputs=[graph_node_info, graph_node_config_info],
        outputs=[node_objects],
        handler=convert_nodes_handler,
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
    def assemble_graph_handler(port_datas, **kwargs):
        node_objs = {
            k: v
            for node_obj in port_datas[node_objects.id]
            for k, v in node_obj.items()
        }
        graphs = {}
        for base_info in port_datas[graph_base_info.id]:
            graph_nodes = node_objs[base_info["id"]]
            graph_node_ids = {node.id for node in graph_nodes}
            graph = Graph(
                id=base_info["id"],
                name=base_info["name"],
                description=base_info["description"],
                status=base_info["status"]
            ).add_node_list(graph_nodes) \
                .add_edge_list([edge
                                for edge in port_datas[edge_objects.id]
                                if edge.source_node_id in graph_node_ids and \
                                edge.target_node_id in graph_node_ids])
            graphs[base_info["id"]] = graph
        return [graph for graph in graphs.values()]

    assemble_graph = builder.mapper_node(
        name="图对象组装",
        inputs=[graph_base_info, node_objects, edge_objects],
        outputs=[assembled_graph],
        handler=assemble_graph_handler,
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

    # query_graph = builder.build()
    return builder.build()
