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
        name="读取用户数据",
        type=BuiltinNodeType.DB_READ,
        inputs=[],
        outputs=[Port(id="users", name="用户数据", data_type=DataType.JSON)],
        config=DBReadConfig(
            query="SELECT * FROM messages",
            db_conn={
                "host": "localhost",
                "user": "root",
                "password": "197346285",
                "dbname": "ai_chat"
            }
        )
    )

    # 2. 创建过滤节点（过滤留下ai_config_id == 9）
    filter_node = Node(
        name="过滤成年用户",
        type=BuiltinNodeType.FILTER,
        inputs=[Port(id="input", name="输入数据", data_type=DataType.JSON, required=True)],
        outputs=[Port(id="filtered", name="过滤后数据", data_type=DataType.JSON)],
        config=FilterNodeConfig(filter_handler=lambda item: item['ai_config_id'] == 9)
    )

    # 3. 创建数据库写入节点
    db_write_node = Node(
        name="写入成年用户表",
        type="db_write",
        inputs=[Port(id="data", name="待写入数据", data_type=DataType.JSON, required=True)],
        outputs=[Port(id="result", name="写入结果", data_type=DataType.NUMBER)],
        config=DBWriteConfig(
            table="ai_9_messages",
            db_conn={
                "host": "localhost",
                "user": "root",
                "password": "197346285",
                "dbname": "ai_chat"
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
        source_port_id="users",
        target_node_id=filter_node.id,
        target_port_id="input"
    ))
    graph.add_edge(Edge(
        source_node_id=filter_node.id,
        source_port_id="filtered",
        target_node_id=db_write_node.id,
        target_port_id="data"
    ))

    # 5. 执行流程
    extension_context = ExecutionContext(debug=True)
    executor = GraphExecutor(graph, extension_context)

    result_graph = await executor.run(start_node_ids=[db_read_node.id])
    return result_graph.get_node_by_id(db_write_node.id).result


def main():
    result = asyncio.run(test_db_flow())
    print(result.get_result_data())
    pass

if __name__ == '__main__':
    main()
