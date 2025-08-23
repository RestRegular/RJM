from fastapi import APIRouter, HTTPException
from typing import List

from data_flow.edge import Edge
from data_flow.graph import Graph
from api.results import EdgeResult
from api.endpoints.graph import graph_store
from api.schemas import EdgeCreate, EdgeResponse

router = APIRouter()


@router.post("/graph/add/{graph_id}", response_model=EdgeResponse)
async def add_edge_to_graph(graph_id: str, edge: EdgeCreate):
    """向流程图添加边"""
    if graph_id not in graph_store:
        return EdgeResponse(code=404, message=f"流程图 {graph_id} 不存在", succeed=False)

    graph: Graph = graph_store[graph_id]
    new_edge = Edge(
        source_node_id=edge.source_node_id,
        source_port_id=edge.source_port_id,
        target_node_id=edge.target_node_id,
        target_port_id=edge.target_port_id,
        enabled=edge.enabled,
        condition=edge.condition
    )

    try:
        graph.add_edge(new_edge)
        return EdgeResponse.from_edge(new_edge)
    except ValueError as e:
        return EdgeResponse(code=400, message=str(e), succeed=False)


@router.get("/graph/get/{graph_id}", response_model=EdgeResponse)
async def get_edges_in_graph(graph_id: str):
    """获取流程图中的所有边"""
    if graph_id not in graph_store:
        return EdgeResponse(code=404, message=f"流程图 {graph_id} 不存在", succeed=False)
    return EdgeResponse(result=[EdgeResult.from_edge(e) for e in graph_store[graph_id].edges])


@router.post("/graph/delete/{graph_id}/{edge_id}")
async def remove_edge_from_graph(graph_id: str, edge_id: str):
    """从流程图中删除边"""
    if graph_id not in graph_store:
        return EdgeResponse(code=404, message=f"流程图 {graph_id} 不存在", succeed=False)

    graph = graph_store[graph_id]
    graph.edges = [e for e in graph.edges if e.id != edge_id]
    return EdgeResponse(message=f"边 {edge_id} 已从流程图 {graph_id} 中删除")
