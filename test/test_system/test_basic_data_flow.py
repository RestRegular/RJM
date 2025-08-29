import asyncio
import logging
from typing import Dict

from data_flow.node import Node, NodeConfig
from data_flow.edge import Edge
from data_flow.port import Port, DataType
from data_flow.graph_executor import GraphExecutor, Graph, ExecutionContext
from data_flow import *


# 1. 定义节点
input_node = Node(
    name="数据输入",
    type=BuiltinNodeType.INPUT,
    inputs=[],
    outputs=[Port(id="output", name="输出数据", data_type=DataType.LIST)]
)

filter_node = Node(
    name="数据过滤",
    type=BuiltinNodeType.FILTER,
    inputs=[Port(id="input", name="输入数据", data_type=DataType.LIST, required=True)],
    outputs=[Port(id="passed", name="通过数据", data_type=DataType.LIST)],
    config=FilterNodeConfig(
        filter_handler=lambda port_datas, **kwargs: [
            item
            for port_data in port_datas.values()
            for item in port_data
            if item['value'] > 5
        ]
    )
)

# 3. 执行图
async def test():
    graph = Graph(name="简单数据处理流程")
    graph.add_node(input_node)
    graph.add_node(filter_node)
    graph.add_edge(Edge(
        source_node_id=input_node.id,
        source_port_id="output",
        target_node_id=filter_node.id,
        target_port_id="input",
        condition=lambda source_port_datas: len(datas) > 5
    ))

    context = ExecutionContext(
        debug=True,
        initial_data=[{"value": 1}, {"value": 3}, {"value": 5}, {"value": 7}, {"value": 9}]
    )
    executor = GraphExecutor(graph, context)
    result_graph = await executor.run(start_node_ids=[input_node.id])

    # 4. 查看结果
    return executor, result_graph


def main():
    from pprint import pprint

    executor, result_graph = asyncio.run(test())

    pprint(executor.get_node_results())


if __name__ == '__main__':
    main()
