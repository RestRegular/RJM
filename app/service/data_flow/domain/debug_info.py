from typing import List, Dict, Optional
from time import time

from pydantic import BaseModel

from app.service.data_flow import CONST_UNKNOWN
from app.service.data_flow.domain.debug_call_info import DebugCallInfo, Profiler, DebugInput
from app.service.data_flow.domain.input_params import InputParams
from app.service.data_flow.domain.inst import Inst
from app.service.data_flow.domain.error_debug_info import ErrorDebugInfo
from app.service.data_flow.domain.node import Node


class FlowDebugInfo(Inst):
    name: str = CONST_UNKNOWN
    error: List[ErrorDebugInfo] = []

    def has_errors(self) -> bool:
        return len(self.error) > 0

    def add_error(self, error: ErrorDebugInfo):
        self.error.append(error)


class DebugNodeInfo(BaseModel):
    id: str
    name: str = None
    sequence_number: int = None
    execution_number: Optional[int] = None
    errors: int = 0
    warnings: int = 0
    calls: List[DebugCallInfo] = []
    # profiler: Profiler

    def has_errors(self) -> bool:
        for call in self.calls:
            if call.has_errors():
                return True
        return False

    @staticmethod
    def _get_input_params(input_port, input_params):
        if input_port:
            return InputParams(port=input_port, value=input_params)
        return None

    def append_call_info(self, flow_start_time, task_start_time,
                         node: Node,
                         input_edge: Inst,
                         input_params: Optional[InputParams],
                         output_edge: Optional[Inst],
                         output_params: Optional[InputParams],
                         active: bool,
                         error=None,
                         errors: int = 0,
                         warnings: int = 0):
        debug_start_time = task_start_time - flow_start_time
        debug_end_time = time() - flow_start_time
        debug_run_time = debug_end_time - debug_start_time

        call_debug_info = DebugCallInfo(
            run=active,
            profiler=Profiler(
                start_time=debug_start_time,
                end_time=debug_end_time,
                run_time=debug_run_time
            ),
            input=DebugInput(
                edge=input_edge,
                params=input_params
            ),
            output=DebugOutput(
                edge=output_edge,  # TODO: this is always None
                result=output_params
            ),
            init=node.init,
            error=error,
            errors=errors,
            warnings=warnings
        )

        self.warnings += warnings
        self.errors += errors
        self.calls.append(call_debug_info)


class DebugEdgeInfo(BaseModel):
    active: List[bool] = []


class DebugInfo(BaseModel):
    timestamp: float
    flow_debug_info: FlowDebugInfo
    event: Inst
    nodes: Dict[str, DebugNodeInfo] = {}
    edges: Dict[str, DebugEdgeInfo] = {}

    def _add_debug_edge_info(self, input_edge_id: str, active: bool):
        if input_edge_id not in self.edges:
            self.edges[input_edge_id] = DebugEdgeInfo(
                active=[active]
            )
        else:
            self.edges[input_edge_id].active.append(active)

    def add_node_info(self, info: DebugNodeInfo):
        self.nodes[info.id] = info

    def has_nodes(self) -> bool:
        return len(self.nodes) > 0

    def has_errors(self) -> bool:
        if self.flow_debug_info.has_errors():
            return True
        for _, node in self.nodes.items():
            if node.has_errors():
                return True
        return False
