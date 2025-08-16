from typing import List

from pydantic import BaseModel

from app.service.data_flow.domain.edge import Edge
from app.service.data_flow.domain.node import Node


class DagGraph(BaseModel):
    nodes: List[Node]
    edges: List[Edge]