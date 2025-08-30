from typing import Union, List

from data_flow import *
from data_flow.graph import Graph
from utils.log_system import get_logger
from data_flow.graph_executor import GraphExecutor
from data_flow.execution_context import ExecutionContext
from data_flow.graphs.storage_graph import build_graph_for_storage_graph


async def storage_graph(graphs: Union[List[Graph], Graph]):
    """
    用于存储流转图到 mysql 数据库

    :param graphs: 待存储的流转图
    """
    if not isinstance(graphs, list) and not isinstance(graphs, Graph):
        raise TypeError("参数 graphs 必须为 list 或 Graph 类型")
    graphs = [graphs] if not isinstance(graphs, list) else graphs
    logger = get_logger(__name__)
    work_graph = build_graph_for_storage_graph()
    context = ExecutionContext(logger=logger, debug=True, initial_data=graphs)
    executor = GraphExecutor(work_graph, context)
    await executor.run()
    if len(work_graph.errors) > 0:
        raise Exception('\n'.join(work_graph.errors))
