import uuid
from typing import Optional, List, Dict

from pydantic import BaseModel, Field

from data_flow.edge import Edge
from data_flow.node import Node
from data_flow.enum_data import GraphStatus
from data_flow.result import Result


class GraphError(BaseModel):
    node_id: str  # 错误节点ID
    error: str  # 错误信息


class Graph(BaseModel):
    id: str = Field(default_factory=lambda :str(uuid.uuid4()))  # 图唯一标识
    name: str  # 图名称（如"用户行为数据处理流程"）
    description: Optional[str] = None  # 图描述
    nodes: Dict[str, Node] = {}  # 节点集合（键为节点ID）
    starts: List[str] = []       # 起点集合
    ends: List[str] = []         # 终点集合
    edges: List[Edge] = []       # 边集合
    status: GraphStatus = GraphStatus.PENDING
    errors: List[GraphError] = []  # 错误信息

    def __str__(self):
        return (f"[Graph({self.name!r}): id={self.id[:8] + ('...' if len(self.id) > 8 else self.id)}, "
                f"nodes={len(self.nodes)}, edges={len(self.edges)}, "
                f"status={self.status}]")

    def __repr__(self):
        return self.__str__()

    def add_node(self, node: Node) -> 'Graph':
        """添加节点"""
        if node.id in self.nodes:
            raise ValueError(f"节点ID {node.id} 已存在")
        if node.is_start:
            self.starts.append(node.id)
        if node.is_end:
            self.ends.append(node.id)
        self.nodes[node.id] = node
        return self  # 返回当前图对象，支持链式调用

    def add_node_list(self, nodes: List[Node]) -> 'Graph':
        """批量添加节点"""
        for node in nodes:
            self.add_node(node)
        return self

    def add_nodes(self, *nodes) -> 'Graph':
        """批量添加节点"""
        return self.add_node_list(nodes)

    def get_node_by_id(self, node_id: str) -> Optional[Node]:
        """根据节点ID获取节点"""
        return self.nodes.get(node_id, None)

    def search_node_by_name(self, node_name: str) -> Optional[Node]:
        """根据节点名称搜索节点"""
        return next((node for node in self.nodes.values() if node.name == node_name), None)

    def remove_node(self, node_id: str) -> 'Graph':
        """删除节点及关联的边"""
        if node_id not in self.nodes:
            return self
        del self.nodes[node_id]
        # 移除与该节点相关的所有边
        self.edges = [
            edge for edge in self.edges
            if edge.source_node_id != node_id and edge.target_node_id != node_id
        ]
        return self

    def add_edge(self, edge: Edge) -> 'Graph':
        """添加边（自动校验节点和端口是否存在）"""
        # 校验上游节点和端口
        if edge.source_node_id not in self.nodes:
            raise ValueError(f"上游节点 {edge.source_node_id} 不存在")
        if not self.nodes[edge.source_node_id].get_output_port(edge.source_port_id):
            raise ValueError(f"上游节点 {edge.source_node_id} 不存在输出端口 {edge.source_port_id}")
        # 校验下游节点和端口
        if edge.target_node_id not in self.nodes:
            raise ValueError(f"下游节点 {edge.target_node_id} 不存在")
        if not self.nodes[edge.target_node_id].get_input_port(edge.target_port_id):
            raise ValueError(f"下游节点 {edge.target_node_id} 不存在输入端口 {edge.target_port_id}")
        # 避免重复边
        if edge in self.edges:
            raise ValueError("边已存在")
        self.edges.append(edge)
        return self

    def add_edge_list(self, edges: List[Edge]) -> 'Graph':
        """批量添加边"""
        for edge in edges:
            self.add_edge(edge)
        return self

    def add_edges(self, *edges) -> 'Graph':
        """添加多个边"""
        return self.add_edge_list(edges)

    def get_downstream_edges(self, node_id: str) -> List[Edge]:
        """获取节点的下游边（从该节点出发的边）"""
        return [edge for edge in self.edges if edge.source_node_id == node_id and edge.enabled]

    def get_upstream_edges(self, node_id: str) -> List[Edge]:
        """获取节点的上游边（指向该节点的边）"""
        return [edge for edge in self.edges if edge.target_node_id == node_id and edge.enabled]

    def to_dict(self):
        return self.model_dump()

    def get_node_result(self, node_id: str) -> Result:
        return self.nodes[node_id].result
