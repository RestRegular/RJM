from typing import Union, List

from data_flow.domain.graph import Graph
from data_flow.domain.enum_data import GraphStatus
from data_flow.utils.data_visitor import DataVisitor
from data_flow.domain.graph_executor import GraphExecutor
from data_flow.domain.execution_context import ExecutionContext
from data_flow.domain.graphs.query_graph import build_graph_for_query_graph
from data_flow.domain.executors.builtin_executors.db_executor import DBConnectionConfig


async def query_graphs(specific_id: List[str]):
    """
    使用查询流转图的流转图查询指定 ID 的流转图
    """
    print(specific_id)
    graph = build_graph_for_query_graph()
    context = ExecutionContext(
        initial_data=specific_id,
        db_conn_config=DBConnectionConfig.from_env()
    )
    executor = GraphExecutor(graph, context)
    executed_graph = await executor.run()
    results = DataVisitor(executed_graph.search_node_by_name("图数据输出")
                          .result.get_result_data())["import_result.assembled_graph"]  # type: List[Graph]
    print(results)
    for result in results:
        result.status = GraphStatus.PENDING
    return results
