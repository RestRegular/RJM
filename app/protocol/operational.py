from typing import Protocol, runtime_checkable

from app.utils.data_structure.operation import RecordFlag


@runtime_checkable
class Operational(Protocol):
    operation: RecordFlag
