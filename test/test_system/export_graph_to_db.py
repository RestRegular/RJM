import json
import logging
from typing import Dict, List, Any

from data_flow import *
from data_flow.node import Node
from data_flow.port import Port
from data_flow.edge import Edge
from data_flow.graph import Graph
from data_flow.node_config import NodeConfig
from data_flow.graph_builder import GraphBuilder
from data_flow.graph_executor import GraphExecutor
from data_flow.execution_context import ExecutionContext
from data_flow.enum_data import BuiltinNodeType, DataType
from data_flow.graphs.storage_graph import build_graph_for_storage_graph


async def test_export_graph() -> tuple[GraphExecutor, Graph]:
    """
    测试图数据导出图功能

    Returns:
        tuple: (GraphExecutor, Graph) 执行器和执行后的图
    """
    # 创建导出流程图
    graph = build_graph_for_storage_graph()

    # 创建执行上下文，传入待导出的图数据
    context = ExecutionContext(initial_data=[graph], debug=True, log_level=logging.INFO)

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

    except Exception as e:
        print(f"执行过程中发生异常: {e}")


if __name__ == '__main__':
    main()
