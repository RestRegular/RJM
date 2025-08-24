import asyncio
from typing import Optional

from data_flow import *
from data_flow.node import Node
from data_flow.port import Port
from data_flow.enum_data import DataType
from data_flow.graph import Graph
from data_flow.edge import Edge
from data_flow.execution_context import ExecutionContext
from data_flow.graph_executor import GraphExecutor


async def test_db_flow():
    # 1. 创建数据库读取节点
    db_read_node = Node(
        name="读取AI数据",
        type=BuiltinNodeType.DB_READ,
        inputs=[],
        outputs=[Port(id="messages", name="AI数据", data_type=DataType.LIST)],
        config=DBReadConfig(
            db_conn={
                "host": "localhost",
                "user": "root",
                "password": "197346285",
                "dbname": "ai_chat"
            },
            port_query_mapping={
                "messages": "select * from messages"
            }
        )
    )

    # 2. 创建过滤节点（过滤留下ai_config_id == 9）
    filter_node = Node(
        name="过滤ID == 9",
        type=BuiltinNodeType.FILTER,
        inputs=[Port(id="messages", name="输入数据", data_type=DataType.LIST, required=True)],
        outputs=[Port(id="filtered", name="过滤后数据", data_type=DataType.LIST)],
        config=FilterNodeConfig(filter_handler=lambda port_datas, **kwargs: [
            ai_config
            for port_data in port_datas.values()
            for ai_config in port_data
            if ai_config['ai_config_id'] == 9
        ])
    )

    # 3. 创建数据库写入节点
    db_write_node = Node(
        name="写入AI config",
        type="db_write",
        inputs=[Port(id="filtered", name="待写入数据", data_type=DataType.LIST, required=True)],
        outputs=[Port(id="result", name="写入结果", data_type=DataType.NUMBER)],
        config=DBWriteConfig(
            db_conn={
                "host": "localhost",
                "user": "root",
                "password": "197346285",
                "dbname": "ai_chat"
            },
            port_table_mapping={
                "filtered": "ai_9_messages"
            },
            upsert_key="id"
        )
    )

    # 4. 构建流程图
    graph = Graph(name="数据库数据处理流程")
    graph.add_node(db_read_node)
    graph.add_node(filter_node)
    graph.add_node(db_write_node)

    # 添加边
    graph.add_edge(Edge(
        source_node_id=db_read_node.id,
        source_port_id="messages",
        target_node_id=filter_node.id,
        target_port_id="messages"
    ))
    graph.add_edge(Edge(
        source_node_id=filter_node.id,
        source_port_id="filtered",
        target_node_id=db_write_node.id,
        target_port_id="filtered"
    ))

    # 5. 执行流程
    extension_context = ExecutionContext(debug=True)
    executor = GraphExecutor(graph, extension_context)

    result_graph = await executor.run(start_node_ids=[db_read_node.id])
    return executor, result_graph


def main():
    executor, result = asyncio.run(test_db_flow())
    for error in result.errors:
        print(f"{result.get_node_by_id(error.node_id).name}: {error.error}")
    print(executor.get_node_results())
    pass

if __name__ == '__main__':
    main()
