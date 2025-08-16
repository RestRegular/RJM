from typing import Optional

from datetime import datetime

from app.domain.inst import Inst
from app.utils.date import now_in_utc


class EventSession(Inst):
    start: datetime = now_in_utc()
    duration: float = 0
    tz: Optional[str] = None
