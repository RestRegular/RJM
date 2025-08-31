from typing import Optional, List, Dict, Any, Union

from pydantic import BaseModel, Field

from data_flow.port import Port
from data_flow.graph import Graph
from data_flow.edge import Edge
from data_flow.node import Node
from data_flow.execution_context import ExecutionContext
from data_flow.enum_data import BuiltinNodeType, DataType, NodeStatus
from api.results import Result, GraphResult, NodeResult, EdgeResult, ExecutionResult


# 响应模型
class Response(BaseModel):
    code: int = 200
    succeed: bool = True
    message: str = "OK"


# 流程图模型
class GraphCreate(BaseModel):
    name: str
    description: Optional[str] = None


class GraphUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


class GraphResponse(Response):
    result: Union[GraphResult, List[GraphResult], None] = None

    @staticmethod
    def from_graph(graph: Graph) -> "GraphResponse":
        return GraphResponse(
            result=GraphResult.from_graph(graph)
        )


# 节点模型
class NodeCreate(BaseModel):
    name: str
    type: Union[str, BuiltinNodeType]
    description: Optional[str] = None
    inputs: List[Port] = []
    outputs: List[Port] = []
    config: Dict[str, Any] = {}

class NodeResponse(Response):
    result: Union[NodeResult, List[NodeResult], None] = None

    @staticmethod
    def from_node(node: Node) -> "NodeResponse":
        return NodeResponse(
            result=NodeResult.from_node(node)
        )

# 边模型
class EdgeCreate(BaseModel):
    source_node_id: str
    source_port_id: str
    target_node_id: str
    target_port_id: str
    enabled: bool = True
    condition: Optional[str] = None

class EdgeResponse(Response):
    result: Union[EdgeResult, List[EdgeResult], None] = None

    @staticmethod
    def from_edge(edge: Edge) -> "EdgeResponse":
        return EdgeResponse(
            result=EdgeResult.from_edge(edge)
        )


# 执行模型
class ExecutionRequest(BaseModel):
    start_node_ids: List[str]
    run_async: bool = False
    context_params: ExecutionContext = ExecutionContext(timeout=30, debug=False)  # 传递给ExecutionContext的参数


class ExecutionResponse(Response):
    result: Union[ExecutionResult, List[ExecutionResult], None] = None

