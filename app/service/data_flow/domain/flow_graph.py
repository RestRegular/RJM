from typing import Optional

from pydantic import PrivateAttr

from app.service.data_flow.domain.flow_graph_data import FlowGraphData
from app.service.data_flow.domain.flow_response import FlowResponse
from app.service.data_flow.domain.name_inst import NamedInst
from app.service.data_flow import CONST_RESPONSE

from app.utils.field_update_logger import FieldUpdateLogger


class FlowGraph(NamedInst):
    desc: Optional[str] = None
    flow_graph: Optional[FlowGraphData] = None
    response: Optional[FlowResponse] = {}

    _update_in_workflow: dict = PrivateAttr({})

    def __init__(self, **data: Any):
        if CONST_RESPONSE in data and isinstance(data[CONST_RESPONSE], dict):
            data[CONST_RESPONSE] = FlowResponse(data[CONST_RESPONSE])
        super().__init__(**data)
        self._field_changes = FieldUpdateLogger()

    def record_change(self, field, value, old_value):
        self._field_changes.add(field, value, old_value)

    def get_changed_field(self) -> FieldUpdateLogger:
        return self._field_changes
