from typing import Optional, Dict

from app.domain.entity import Entity

class Device(Entity):
    name: Optional[str] = None
    type: Optional[str] = None
    properties: Optional[Dict[str, str]] = None