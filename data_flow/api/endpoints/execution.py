from typing import List, Dict

from fastapi import APIRouter, HTTPException, BackgroundTasks

from data_flow.domain.enum_data import *
from data_flow.api.results import ExecutionResult
from data_flow.domain.graph_executor import GraphExecutor
from data_flow.domain.execution_context import ExecutionContext
from data_flow.api.schemas import ExecutionRequest, ExecutionResponse
from data_flow.api.endpoints.service.query_graphs import query_graphs
from data_flow.utils.data_visitor import DataVisitor

router = APIRouter()


@router.post("/execute/", response_model=ExecutionResponse)
async def execute(request: ExecutionRequest):
    """
    执行指定 ID 的流转图，**若未导入则自动导入**
    """
    executing_graph_ids = request.executing_graph_ids
    executing_graphs = query_graphs(executing_graph_ids)
    executing_context = ExecutionContext(**request.context_params)
    results = {}
    for graph in executing_graphs:
        executor = GraphExecutor(graph, executing_context)
        executed_graph = await executor.run()
        results[graph.id] = ExecutionResult(
            execution_id=graph.id,
            status=executed_graph.status,
            results=executor.get_node_results(mode='python')
        )
    return ExecutionResponse(
        result=results
    )
