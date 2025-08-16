from typing import List, Optional

from pydantic import BaseModel

from app.service.data_flow.domain.input_params import InputParams
from app.service.data_flow.domain.inst import Inst
from app.service.plugin.domain.result import Result


class DebugInput(BaseModel):
    edge: Optional[Inst] = None
    params: Optional[InputParams] = None


class DebugOutput(BaseModel):
    edge: Optional[Inst] = None
    result: Optional[List[Result]] = None


class Profiler(BaseModel):
    start_time: float
    run_time: float
    end_time: float


class DebugCallInfo(BaseModel):
    profiler: Profiler
    input: DebugInput
    output: DebugOutput
    init: Optional[dict] = None
    # profile: Optional[dict] = None
    # event: Optional[dict] = None
    # session: Optional[dict] = None
    error: Optional[str] = None
    run: bool = False
    errors: int = 0
    warnings: int = 0

    def has_error(self) -> bool:
        return self.error is not None
