from typing import Optional, List

from pydantic import BaseModel

from app.service.data_flow.domain.inst import Inst
from app.service.data_flow.domain.port_to_port_edges import PortToPortEdges
from app.service.plugin.domain.register import NodeEvents, RunOnce
from app.service.plugin.runner import ActionRunner


class Graph(BaseModel):
    in_edges: PortToPortEdges = PortToPortEdges()
    out_edges: PortToPortEdges = PortToPortEdges()


class Node(Inst):
    name: str = None
    start: Optional[bool] = False
    debug: bool = False
    inputs: Optional[List[str]] = []
    outputs: Optional[List[str]] = []
    class_name: str
    module: str
    init: Optional[dict] = {}
    skip: bool = False
    run_once: Optional[RunOnce] = RunOnce()
    node_events: Optional[NodeEvents] = None
    block_flow: bool = False
    on_error_continue: bool = False
    run_in_background: bool = False
    on_connection_error_repeat: int = 1
    append_input_payload: bool = False
    join_input_payload: bool = False
    object: Optional[ActionRunner] = None