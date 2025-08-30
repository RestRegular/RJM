import asyncio
from typing import List, Optional
from fastapi import APIRouter, HTTPException, status, UploadFile, File

from data_flow.graph import Graph
from api.results import GraphResult
from api.endpoints.service.gg_parse import process_gg_parse
from api.schemas import GraphCreate, GraphUpdate, GraphResponse
from api.endpoints.service.storage_graph import storage_graph

router = APIRouter()


@router.post("/upload_graph/", response_model=GraphResponse, status_code=status.HTTP_200_OK)
async def upload_graph(file: UploadFile = File(...)):
    """
    上传 GG 文件，服务器将其解析为 Graph 并进行存储
    返回上传的流转图的基本数据：id、name、description、node_count、edge_count
    """
    # 读取上传的文件内容
    content = await file.read()
    try:
        # 处理流程图
        graphs = process_gg_parse(content.decode("utf-8"))
        # 储存流程图
        await storage_graph(graphs)
        return GraphResponse(code=200, message="上传成功", result=[
            GraphResult.from_graph(graph)
            for graph in graphs
        ])
    except Exception as e:
        return GraphResponse(
            code=400,
            message=str(e),
            succeed=False
        )


@router.get("/get_graph/{graph_id}", response_model=GraphResponse, status_code=status.HTTP_200_OK)
async def get_graph(graph_id: str):
    """
    获取指定流程图
    """
    pass
