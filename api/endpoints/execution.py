from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import List, Dict
from data_flow.graph_executor import GraphExecutor
from data_flow.execution_context import ExecutionContext
from api.schemas import ExecutionRequest, ExecutionResult
from api.endpoints.graph import graph_store

router = APIRouter()

# 存储执行结果（实际项目建议用数据库）
execution_results: Dict[str, ExecutionResult] = {}


@router.post("/graph/{graph_id}", response_model=ExecutionResult)
async def execute_graph(
        graph_id: str,
        request: ExecutionRequest,
        background_tasks: BackgroundTasks
):
    """执行指定流程图"""
    if graph_id not in graph_store:
        raise HTTPException(404, detail="流程图不存在")

    graph = graph_store[graph_id]
    context = ExecutionContext(**request.context_params)

    # 同步执行（简单场景）或异步后台执行（复杂场景）
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
        return {"execution_id": execution_id, "status": "running"}
    else:
        # 同步执行
        executor = GraphExecutor(graph, context)
        result_graph = await executor.run(request.start_node_ids)
        return {
            "execution_id": f"sync_{graph_id}",
            "status": "completed",
            "node_results": {
                node_id: {"status": node.status, "error": node.error}
                for node_id, node in result_graph.nodes.items()
            }
        }


async def run_graph_background(graph_id: str, start_nodes: List[str], context: ExecutionContext, execution_id: str):
    """后台执行流程图的任务函数"""
    graph = graph_store[graph_id]
    executor = GraphExecutor(graph, context)
    result_graph = await executor.run(start_nodes)

    execution_results[execution_id] = {
        "execution_id": execution_id,
        "status": "completed",
        "node_results": {
            node_id: {"status": node.status, "error": node.error}
            for node_id, node in result_graph.nodes.items()
        }
    }


@router.get("/{execution_id}")
async def get_execution_result(execution_id: str):
    """查询执行结果"""
    if execution_id not in execution_results:
        raise HTTPException(404, detail="执行记录不存在")
    return execution_results[execution_id]