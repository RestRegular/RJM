from typing import List, Dict

from fastapi import APIRouter, HTTPException, BackgroundTasks

from data_flow.enum_data import *
from api.results import ExecutionResult
from api.endpoints.graph import graph_store
from data_flow.graph_executor import GraphExecutor
from data_flow.execution_context import ExecutionContext
from api.schemas import ExecutionRequest, ExecutionResponse

router = APIRouter()

# 存储执行结果
execution_results: Dict[str, ExecutionResult] = {}


@router.post("/execute/{graph_id}", response_model=ExecutionResponse)
async def execute_graph(
        graph_id: str,
        request: ExecutionRequest,
        background_tasks: BackgroundTasks):
    """执行指定流程图"""
    if graph_id not in graph_store:
        return ExecutionResponse(code=404, message=f"流程图 {graph_id} 不存在", succeed=False)

    graph = graph_store[graph_id]
    context = request.context_params

    # 同步执行或异步后台执行
    if request.run_async:
        # 后台异步执行
        execution_id = f"exec_{graph_id}_{id(context)}"
        background_tasks.add_task(
            run_graph_background,
            graph_id=graph_id,
            start_nodes=request.start_node_ids,
            context=context,
            execution_id=execution_id
        )
        return ExecutionResponse(result=ExecutionResult(
            execution_id=execution_id,
            status=NodeStatus.RUNNING,
            node_results=None
        ))
    else:
        # 同步执行
        executor = GraphExecutor(graph, context)
        result_graph = await executor.run(request.start_node_ids)
        return ExecutionResponse(result=ExecutionResult(
            execution_id=f"sync_{graph_id}",
            status=result_graph.status,
            node_results=executor.get_node_results()
        ))


async def run_graph_background(
        graph_id: str,
        start_nodes: List[str],
        context: ExecutionContext,
        execution_id: str):
    """后台执行流程图的任务函数"""
    graph = graph_store[graph_id]
    executor = GraphExecutor(graph, context)
    result_graph = await executor.run(start_nodes)

    execution_results[execution_id] = ExecutionResult(
        execution_id=execution_id,
        status=result_graph.status,
        node_results=executor.get_node_results()
    )


@router.get("/result/{execution_id}")
async def get_execution_result(execution_id: str):
    """查询执行结果"""
    if execution_id not in execution_results:
        raise HTTPException(404, detail="执行记录不存在")
    return execution_results[execution_id]
