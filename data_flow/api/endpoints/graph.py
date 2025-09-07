import os.path
from typing import Union, List, Optional, Dict, Any

from fastapi import APIRouter, status, UploadFile, File

from data_flow import CACHE_DIR_PATH
from data_flow.api.endpoints.service.gg_parse import process_gg_parse
from data_flow.api.endpoints.service.query_graphs import query_graphs
from data_flow.api.results import GraphResult
from data_flow.api.schemas import GraphResponse
from data_flow.api.endpoints.service.graph_manager import GraphManager
from data_flow.utils.file_manager import delete_cache_files
from data_flow.utils.log_system import get_logger

router = APIRouter()


@router.post("/upload_gg_graph/", response_model=GraphResponse, status_code=status.HTTP_200_OK)
async def upload_gg_graph(file: UploadFile = File(...)):
    """
    上传 GG 文件，服务器将其解析为 Graph 并进行存储
    返回上传的流转图的基本数据：id、name、description、node_count、edge_count
    """
    # 读取上传的文件内容
    content = await file.read()
    try:
        # 处理流程图
        graph = process_gg_parse(content.decode("utf-8"))
        return GraphResponse(
            code=200,
            message="上传成功",
            result=GraphResult.from_graph(graph)
        )
    except Exception as e:
        return GraphResponse(
            code=400,
            message=str(e),
            succeed=False
        )


@router.post("/upload_py_graph/", response_model=GraphResponse, status_code=status.HTTP_200_OK)
async def upload_py_graph(file: UploadFile = File(...)):
    """
    上传 Python 文件，服务器将其解析为 Graph 并进行存储
    返回上传的流转图的基本数据：id、name、description、node_count、edge_count
    """
    # 读取上传的文件内容
    content = await file.read()
    try:
        # 将文件保存到缓存中
        cache_path = os.path.join(CACHE_DIR_PATH, f"{file.filename}")
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(content.decode("utf-8"))
        graph = GraphManager.dynamic_import_graph_from_cache(file.filename[:-3], cache_path)
        return GraphResponse(
            code=200,
            message="上传成功",
            result=GraphResult.from_graph(graph)
        )
    except Exception as e:
        get_logger("GraphManager").error("failed to upload py graph", exc_info=e)
        return GraphResponse(
            code=400,
            message=str(e),
            succeed=False
        )
    finally:
        delete_cache_files(file.filename)


@router.post("/query_graph/", response_model=GraphResponse, status_code=status.HTTP_200_OK)
async def query_graph(graph_ids: Optional[Union[str, List[str]]] = None):
    """
    查询指定 ID 的流转图，**若未导入则自动导入**

    若不指定 ID 则查询所有已导入的流转图

    若想查询所有流程图信息，请使用 `graphs/graph_info`
    """
    try:
        graphs = query_graphs(graph_ids if isinstance(graph_ids, List) else [graph_ids]) \
            if graph_ids else GraphManager.get_storage().values()
        return GraphResponse(
            code=200,
            message="查询成功",
            result=[
                GraphResult.from_graph(graph)
                for graph in graphs
            ]
        )
    except ValueError as e:
        return GraphResponse(
            code=400,
            message=str(e),
            succeed=False
        )

@router.post("/search_graph/", response_model=GraphResponse, status_code=status.HTTP_200_OK)
def search_graph(prompts: Union[str, List[str]]):
    """
    模糊搜索指定名称或 ID 的 **已导入的** 流转图

    若要搜索未导入的流转图，请使用 `graphs/search_graph_and_import`
    """
    prompts = prompts if isinstance(prompts, List) else [prompts]
    querying_graphs = []
    for graph_name in prompts:
        ids = GraphManager.search_graph(graph_name)
        for _id in ids:
            g = GraphManager.get_storage().get(_id)
            if g:
                querying_graphs.append(g)
    return GraphResponse(
        message="搜索成功",
        result=[
            GraphResult.from_graph(graph)
            for graph in querying_graphs
        ]
    )


@router.post("/search_graph_and_import/", response_model=GraphResponse, status_code=status.HTTP_200_OK)
def search_graph(prompts: Union[str, List[str]]):
    """
    模糊搜索指定名称或 ID 的流转图，**若未导入则自动导入**
    """
    prompts = prompts if isinstance(prompts, List) else [prompts]
    querying_graphs = []
    for graph_name in prompts:
        ids = GraphManager.search_graph(graph_name)
        for _id in ids:
            gs = GraphManager.get_graph(_id)
            querying_graphs.append(gs)
    return GraphResponse(
        message="搜索成功",
        result=[
            GraphResult.from_graph(graph)
            for graph in querying_graphs
        ]
    )


@router.get("/graph_info/", response_model=Dict[str, Any], status_code=status.HTTP_200_OK)
def graph_info():
    """
    查询所有流转图信息
    """
    return GraphManager.get_all_graph_info()


@router.post("/delete_graph/", response_model=GraphResponse, status_code=status.HTTP_200_OK)
def delete_graph(graph_ids: Union[str, List[str]]):
    """
    删除指定 ID 的流转图
    """
    graph_ids = graph_ids if isinstance(graph_ids, List) else [graph_ids]
    with GraphManager.deleting_graphs() as gm:
        for _id in graph_ids:
            gm.delete_graph(_id)
    return GraphResponse(
        message="删除成功"
    )