import uuid
from typing import Optional, List, Dict

from pydantic import BaseModel, Field

from data_flow.edge import Edge
from data_flow.node import Node


class Graph(BaseModel):
    id: str = Field(default_factory=lambda :str(uuid.uuid4()))  # 图唯一标识
    name: str  # 图名称（如"用户行为数据处理流程"）
    description: Optional[str] = None  # 图描述
    nodes: Dict[str, Node] = {}  # 节点集合（键为节点ID）
    edges: List[Edge] = []       # 边集合

    def add_node(self, node: Node):
        """添加节点"""
        if node.id in self.nodes:
            raise ValueError(f"节点ID {node.id} 已存在")
        self.nodes[node.id] = node

    def get_node_by_id(self, node_id: str) -> Optional[Node]:
        """根据节点ID获取节点"""
        return self.nodes.get(node_id)

    def remove_node(self, node_id: str):
        """删除节点及关联的边"""
        if node_id not in self.nodes:
            return
        del self.nodes[node_id]
        # 移除与该节点相关的所有边
        self.edges = [
            edge for edge in self.edges
            if edge.source_node_id != node_id and edge.target_node_id != node_id
        ]

    def add_edge(self, edge: Edge):
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

    def get_downstream_edges(self, node_id: str) -> List[Edge]:
        """获取节点的下游边（从该节点出发的边）"""
        return [edge for edge in self.edges if edge.source_node_id == node_id and edge.enabled]

    def get_upstream_edges(self, node_id: str) -> List[Edge]:
        """获取节点的上游边（指向该节点的边）"""
        return [edge for edge in self.edges if edge.target_node_id == node_id and edge.enabled]