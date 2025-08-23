from fastapi import APIRouter, HTTPException, status
from typing import List, Optional

from data_flow.graph import Graph
from api.results import GraphResult
from api.schemas import GraphCreate, GraphUpdate, GraphResponse

router = APIRouter()


# TODO: 暂时使用内存管理流程图，测试成功之后添加内置的流程图提供数据库访问和存储流程图的功能
graph_store: dict[str, Graph] = {}


@router.post("/create/", response_model=GraphResponse, status_code=status.HTTP_201_CREATED)
async def create_graph(graph: GraphCreate):
    """创建新的流程图"""
    if graph.name in [g.name for g in graph_store.values()]:
        return GraphResponse(
            code=400,
            message=f"流程图 {graph.name} 已存在",
            succeed=False
        )
    new_graph = Graph(
        name=graph.name,
        description=graph.description
    )
    graph_store[new_graph.id] = new_graph
    return GraphResponse.from_graph(new_graph)


@router.get("/query/", response_model=GraphResponse)
async def list_graphs(name: Optional[str] = None):
    """查询流程图列表（支持按名称筛选）"""
    graphs = [GraphResult.from_graph(g) for g in graph_store.values() if name and name in g.name]
    return GraphResponse(result=graphs)


@router.get("/get/{graph_id}", response_model=GraphResponse)
async def get_graph(graph_id: str):
    """获取单个流程图详情"""
    if graph_id not in graph_store:
        return GraphResponse(
            code=404,
            message=f"流程图 {graph_id} 不存在",
            succeed=False
        )
    return GraphResponse.from_graph(graph_store[graph_id])


@router.put("/update/{graph_id}", response_model=GraphResponse)
async def update_graph(graph_id: str, graph_update: GraphUpdate):
    """更新流程图信息"""
    if graph_id not in graph_store:
        return GraphResponse(
            code=404,
            message=f"流程图 {graph_id} 不存在",
            succeed=False
        )
    graph = graph_store[graph_id]
    update_data = graph_update.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(graph, key, value)
    return GraphResponse.from_graph(graph)


@router.post("/delete/{graph_id}", response_model=GraphResponse, status_code=status.HTTP_200_OK)
async def delete_graph(graph_id: str):
    """删除流程图"""
    if graph_id not in graph_store:
        return GraphResponse(
            code=404,
            message=f"流程图 {graph_id} 不存在",
            succeed=False
        )
    del graph_store[graph_id]
    return GraphResponse(message=f"删除流程图 {graph_id} 成功")
