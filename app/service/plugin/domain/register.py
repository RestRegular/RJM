from typing import Optional

from pydantic import BaseModel


class NodeEvents(BaseModel):
    on_create: Optional[str] = None
    on_remove: Optional[str] = None