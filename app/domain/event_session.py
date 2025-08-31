from typing import Optional

from datetime import datetime

from app.domain.entity import Entity
from app.service.utils.date import now_in_utc


class EventSession(Entity):
    start: datetime = now_in_utc()
    duration: float = 0
    tz: Optional[str] = None