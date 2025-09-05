from typing import Union, List

from fastapi import APIRouter, status, UploadFile, File

from data_flow.api.endpoints.service.gg_parse import process_gg_parse
from data_flow.api.endpoints.service.query_graphs import query_graphs
from data_flow.api.endpoints.service.storage_graph import storage_graph
from data_flow.api.results import GraphResult
from data_flow.api.schemas import GraphResponse

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


@router.post("/query_graph/", response_model=GraphResponse, status_code=status.HTTP_200_OK)
async def query_graph(graph_ids: Union[str, List[str]]):
    """
    查询指定 ID 的流转图
    """
    graphs = await query_graphs(graph_ids if isinstance(graph_ids, List) else [graph_ids])
    return GraphResponse(
        code=200,
        message="查询成功",
        result=[
            GraphResult.from_graph(graph)
            for graph in graphs
        ]
    )
