from typing import List, Optional

from pydantic import BaseModel

from app.domain import CONST_EVENT_STATUS_COLLECTED
from app.domain.inst import Inst
from app.utils.time import EventTime, Time


class EventProcessors(BaseModel):
    rules: List[str] = []
    flows: List[str] = []
    third_party: List[str] = []


class EventMetadata(BaseModel):
    aux: Optional[dict] = {}
    time: EventTime
    ip: Optional[str] = None
    status: Optional[str] = CONST_EVENT_STATUS_COLLECTED
    channel: Optional[str] = None
    processor: EventProcessors = EventProcessors()
    no_profile: Optional[bool] = False
    debug: Optional[bool] = False
    valid: Optional[bool] = True
    error: Optional[bool] = False
    warning: Optional[bool] = False
    merge: Optional[bool] = False
    instance: Optional[Inst] = None


class EventPayloadMetaData(BaseModel):
    time: Time
    ip: Optional[str] = None
    status: Optional[str] = None