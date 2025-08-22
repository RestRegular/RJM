from fastapi import APIRouter, HTTPException
from typing import List

from api.results import NodeResult
from data_flow.node import Node
from data_flow.graph import Graph
from api.schemas import NodeCreate, NodeResponse
from api.endpoints.graph import graph_store

router = APIRouter()


@router.post("/graph/add/{graph_id}", response_model=NodeResponse)
async def add_node_to_graph(graph_id: str, node: NodeCreate):
    """向指定流程图添加节点"""
    if graph_id not in graph_store:
        return NodeResponse(code=404, message=f"流程图 {graph_id} 不存在", succeed=False)

    graph: Graph = graph_store[graph_id]
    new_node = Node(
        name=node.name,
        type=node.type,
        description=node.description,
        inputs=node.inputs,
        outputs=node.outputs,
        config=node.config
    )

    try:
        graph.add_node(new_node)
        return NodeResponse.from_node(new_node)
    except ValueError as e:
        return NodeResponse(code=400, message=str(e), succeed=False)


@router.get("/graph/get/{graph_id}", response_model=NodeResponse)
async def get_nodes_in_graph(graph_id: str):
    """获取指定流程图中的所有节点"""
    if graph_id not in graph_store:
        return NodeResponse(code=404, message=f"流程图 {graph_id} 不存在", succeed=False)
    return NodeResponse(result=[NodeResult.from_node(node) for node in graph_store[graph_id].nodes.values()])


@router.post("/graph/delete/{graph_id}/{node_id}")
async def remove_node_from_graph(graph_id: str, node_id: str):
    """从流程图中删除节点"""
    if graph_id not in graph_store:
        return NodeResponse(code=404, message=f"流程图 {graph_id} 不存在", succeed=False)

    graph = graph_store[graph_id]
    graph.remove_node(node_id)
    return NodeResponse(code=200, message=f"节点 {node_id} 已从流程图 {graph_id} 中删除")
