from typing import Any, Optional

from app.domain.inst import Inst


class NamedInst(Inst):
    name: str

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.id = self.id.lower()

    def is_empty(self) -> bool:
        return self.id == '' or self.id is None or \
            self.name == '' or self.name is None


class NamedInstInContext(NamedInst):
    production: Optional[bool] = False
    running: Optional[bool] = False
