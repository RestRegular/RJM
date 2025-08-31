from typing import Optional, List, Dict, Union, Any

from pydantic import BaseModel, Field

from data_flow.enum_data import *
from data_flow.graph import Graph
from data_flow.node import Node
from data_flow.port import Port
from data_flow.edge import Edge


# 结果模型
class Result(BaseModel):
    pass


# 流程图模型
class GraphResult(Result):
    id: str
    name: str
    description: Optional[str] = None
    node_count: int = 0
    edge_count: int = 0

    @staticmethod
    def from_graph(graph: Graph) -> 'GraphResult':
        return GraphResult(
            id=graph.id,
            name=graph.name,
            description=graph.description,
            node_count=len(graph.nodes),
            edge_count=len(graph.edges)
        )

    class Config:
        from_attributes = True
        json_encoders = {dict: lambda v: len(v)}  # 计算节点数量


# 节点模型
class NodeResult(Result):
    id: str
    name: str
    type: Union[BuiltinNodeType, str]
    description: Optional[str] = None
    inputs: List[Port]
    outputs: List[Port]

    @staticmethod
    def from_node(node: Node) -> 'NodeResult':
        return NodeResult(
            id=node.id,
            name=node.name,
            type=node.type,
            description=node.description,
            inputs=node.inputs,
            outputs=node.outputs
        )

    class Config:
        from_attributes = True


# 边模型
class EdgeResult(Result):
    id: str
    source_node_id: str
    source_port_id: str
    target_node_id: str
    target_port_id: str
    enabled: bool
    condition: Optional[str]

    @staticmethod
    def from_edge(edge: Edge) -> 'EdgeResult':
        return EdgeResult(
            id=edge.id,
            source_node_id=edge.source_node_id,
            source_port_id=edge.source_port_id,
            target_node_id=edge.target_node_id,
            target_port_id=edge.target_port_id,
            enabled=edge.enabled,
            condition=edge.condition
        )

    class Config:
        from_attributes = True


class ExecutionResult(Result):
    execution_id: str
    status: Union[GraphStatus, str]
    node_results: Optional[Dict[str, Dict[str, Any]]] = None
