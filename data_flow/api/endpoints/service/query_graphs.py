from typing import List

from data_flow.domain.graph import Graph
from data_flow.api.endpoints.service.graph_manager import GraphManager


def query_graphs(specific_ids: List[str]) -> List[Graph]:
    graphs = []
    for graph_id in specific_ids:
        if not GraphManager.has_graph(graph_id):
            raise ValueError(f"Graph {graph_id} not found")
        gs = GraphManager.get_graph(graph_id)
        graphs.append(gs)
    return graphs
