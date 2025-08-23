import uuid
from pydantic import BaseModel, Field
from typing import Optional, Callable, Any


class Edge(BaseModel):
    id: str = Field(default_factory=lambda :str(uuid.uuid4()))  # 边唯一标识
    source_node_id: str  # 上游节点ID
    source_port_id: str  # 上游节点输出端口ID
    target_node_id: str  # 下游节点ID
    target_port_id: str  # 下游节点输入端口ID
    enabled: bool = True  # 边是否启用（禁用后数据不流转）
    condition: Callable[[Any], bool] = None  # 条件判断器（满足时才流转）

    def __hash__(self):
        """用于边的去重判断"""
        return hash(f"{self.source_node_id}_{self.source_port_id}_{self.target_node_id}_{self.target_port_id}")

    def __repr__(self):
        return (f"[Edge: "
                f"({self.source_port_id[:12]}"
                f"{'...' if len(self.source_port_id) > 12 else ''})"
                f"{self.source_node_id[:6]}... -> "
                f"({self.target_port_id[:12]}"
                f"{'...' if len(self.target_port_id) > 12 else ''})"
                f"{self.target_node_id[:6]}...]")

    def __str__(self):
        return (f"[Edge: "
                f"({self.source_port_id}){self.source_node_id} -> "
                f"({self.target_port_id}){self.target_node_id}]")
