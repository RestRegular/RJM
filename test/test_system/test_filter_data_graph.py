import asyncio
import os
import sys

sys.path.append(os.path.abspath("D:\\repositories\\RJM\\"))


from data_flow.domain import *
from data_flow.domain.graph_executor import GraphExecutor
from data_flow.domain.graphs.graph_19ebc647_d9bb_4fdc_b2f9_f2430adef53c import build_graphs


if __name__ == '__main__':
    graph = build_graphs()[0]
    context = ExecutionContext(initial_data=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    executor = GraphExecutor(graph, context)
    graph = asyncio.run(executor.run())
    print(executor.get_node_results())
