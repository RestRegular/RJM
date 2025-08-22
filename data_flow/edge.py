import uuid
from pydantic import BaseModel
from typing import Optional, Callable, Any


class Edge(BaseModel):
    id: str = str(uuid.uuid4())  # 边唯一标识
    source_node_id: str  # 上游节点ID
    source_port_id: str  # 上游节点输出端口ID
    target_node_id: str  # 下游节点ID
    target_port_id: str  # 下游节点输入端口ID
    enabled: bool = True  # 边是否启用（禁用后数据不流转）
    condition: Callable[[Any], bool] = None  # 条件判断器（满足时才流转）

    def __hash__(self):
        """用于边的去重判断"""
        return hash(f"{self.source_node_id}_{self.source_port_id}_{self.target_node_id}_{self.target_port_id}")
