from typing import Optional, Any
from datetime import datetime
from uuid import uuid4

from pydantic import BaseModel, PrivateAttr, ConfigDict
from sqlalchemy.testing.suite.test_reflection import metadata
from user_agents import parse

from app.domain import CONST_DURATION, CONST_CONTEXT, CONST_SESSION, CONST_ENDED, CONST_OPERATION
from app.domain.inst import Inst
from app.utils.data_structure.operation import Operation
from app.utils.data_structure.storage_info import StorageInfo
from app.utils.data_visitor import DataVisitor
from app.utils.date import now_in_utc
from app.utils.time import Time


class SessionTime(Time):
    timestamp: Optional[float] = 0
    duration: float = 0
    weekday: Optional[int] = None

    def __init__(self, **data: Any):
        if CONST_DURATION not in data:
            data[CONST_DURATION] = 0
        super().__init__(**data)
        self.weekday = self.insert.weekday()

        if self.timestamp == 0:
            self.timestamp = datetime.timestamp(now_in_utc())

    @staticmethod
    def new() -> 'SessionTime':
        return SessionTime()


class SessionMetadata(BaseModel):
    time: SessionTime = SessionTime()
    channel: Optional[str] = None
    aux: Optional[dict] = {}
    status: Optional[str] = None

    @staticmethod
    def new() -> 'SessionMetadata':
        return SessionMetadata(time=SessionTime.new())


class SessionContext(dict):
    def __init__(self, **data):
        super().__init__(**data)
        self.visitor = DataVisitor(self)

    def get_time_zone(self) -> Optional[str]:
        return self.visitor['time.tz']

    def get_platform(self):
        return self.visitor['browser.local.device.platform']

    def get_browser_name(self):
        return self.visitor['browser.local.browser.name']


class Session(Inst):
    metadata: SessionMetadata
    operation: Operation = Operation()
    # profile: Optional[Entity] = None
    #
    # device: Optional[Device] = Device()
    # os: Optional[OS] = OS()
    # app: Optional[Application] = Application()
    #
    # utm: Optional[UTM] = UTM()
    context: Optional[SessionContext] = SessionContext()
    properties: Optional[dict] = {}
    # traits: Optional[dict] = {}
    aux: Optional[dict] = {}

    _updated_in_workflow: bool = PrivateAttr(False)

    model_config = ConfigDict(arbitrary_types_allowed=True,
                              frozen=False)

    def __init__(self, **data: Any):
        if CONST_CONTEXT in data and not isinstance(data[CONST_CONTEXT], SessionContext):
            data[CONST_CONTEXT] = SessionContext(**(data[CONST_CONTEXT] if isinstance(data[CONST_CONTEXT], dict) else {}))
        super().__init__(**data)
        self._is_frozen = False

    def freeze(self):
        self._is_frozen = True

    def unfreeze(self):
        self._is_frozen = False

    def __setattr__(self, key, value):
        if getattr(self, '_is_frozen', False) and key != '_is_frozen':
            raise TypeError(f"Cannot modify frozen instance: attribute '{key}' is read-only")
        super().__setattr__(key, value)

    def is_new(self) -> bool:
        return self.operation.new

    def set_new(self, flag: bool = True):
        self.operation.new = flag

    def set_updated(self, flag: bool = True):
        self.operation.update = flag

    def is_updated(self) -> bool:
        return self.operation.update

    def set_updated_in_workflow(self, state: bool = True):
        self._updated_in_workflow = state

    def is_updated_in_workflow(self) -> bool:
        return self._updated_in_workflow

    def fill_meta_data(self):
        self._fill_meta_data(CONST_SESSION)

    def replace(self, session):
        if isinstance(session, Session):
            self.unfreeze()
            self.id = session.id
            self.metadata = session.metadata
            self.operation = session.operation
            self.context = session.context
            self.properties = session.properties
            self.aux = session.aux
            self.freeze()

    def is_reopened(self) -> bool:
        return self.operation.new or self.metadata.status == CONST_ENDED

    def has_not_saved_changes(self) -> bool:
        return self.operation.new or self.operation.needs_update()

    def get_user_agent(self) -> Optional[str]:
        user_agent_str = DataVisitor.get("browser.local.browser.userAgent", self.context, None)
        return (parse(user_agent_str)
                if user_agent_str
                else None)

    @staticmethod
    def storage_info() -> StorageInfo:
        return StorageInfo(
            CONST_SESSION,
            Session,
            exclude={
                CONST_OPERATION: ...
            },
            multi=True
        )

    @staticmethod
    def new(id_: Optional[str] = None):
        session = Session(
            id=str(uuid4()) if not id_ else id_,
            metadata=SessionMetadata.new()
        )
        session.fill_meta_data()
        session.set_new()
        return session


class FrozenSession(Session):
    class Config:
        frozen: bool = True
