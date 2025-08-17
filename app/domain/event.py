import json
from uuid import uuid4
from typing import Optional, Tuple, Union, List, Any

from pydantic import BaseModel, ConfigDict, model_validator

from app.domain import CONST_MISSING_EVENT_TYPE, CONST_SAVE, CONST_ID
from app.domain.event_metadata import EventMetadata
from app.domain.event_session import EventSession
from app.domain.flat_event import FlatEvent
from app.domain.inst import Inst
from app.domain.named_inst import NamedInst
from app.utils.data_structure.operation import RecordFlag
from app.utils.data_structure.storage_info import StorageInfo
from app.utils.string_manager import capitalize_event_type_id
from app.utils.data_structure.request import Request


class Tags(BaseModel):
    values: Tuple['str', ...] = ()
    count: int = 0
    model_config = ConfigDict(validate_assignment=True)

    @classmethod
    @model_validator(mode='before')
    def total_tags(cls, values: dict) -> dict:
        values['count'] = len(values.get('values', ()))
        return values

    def add(self, tag: Union[str, List[str]]):
        if isinstance(tag, list):
            tag = tuple(tag)
            self.values += tag
        else:
            self.values += (tag,)
        self.count = len(self.values)


class EventJourney(BaseModel):
    status: Optional[str] = None

    def is_empty(self) -> bool:
        return self.status is None or self.status == ""


class Event(NamedInst):
    metadata: EventMetadata
    type: str

    properties: Optional[dict] = {}
    operation: RecordFlag = RecordFlag()

    # read only properties
    source: Inst
    session: Optional[EventSession] = None

    context: Optional[dict] = {}
    request: Optional[dict] = {}
    config: Optional[dict] = {}
    tags: Tags = Tags()
    aux: dict = {}

    journey: EventJourney = EventJourney.model_construct()
    data: Optional[dict] = {}

    def __init__(self, **data: Any):
        if 'type' in data and isinstance(data['type'], str):
            data['type'] = data.get('type', CONST_MISSING_EVENT_TYPE).lower().replace(' ', '-')
        if 'name' not in data:
            data['name'] = capitalize_event_type_id(data.get('type', ''))
        super().__init__(**data)

    def replace(self, event: 'Event'):
        if isinstance(event, Event):
            self.id = event.id
            self.name = event.name
            self.type = event.type
            self.metadata = event.metadata
            self.properties = event.properties
            self.operation = event.operation
            self.context = event.context
            self.request = event.request
            self.config = event.config
            self.tags = event.tags
            self.aux = event.aux
            self.journey = event.journey
            self.data = event.data

    def get_ip(self):
        return Request(self.request).get_ip()

    def is_persistent(self) -> bool:
        if CONST_SAVE in self.config and isinstance(self.config[CONST_SAVE], bool):
            return self.config[CONST_SAVE]
        else:
            return False

    def has_session(self) -> bool:
        return self.session is not None

    @staticmethod
    def new(data: dict) -> 'Event':
        data[CONST_ID] = str(uuid4())
        return Event(**data)

    @staticmethod
    def storage_info() -> StorageInfo:
        return StorageInfo(
            'event',
            Event,
            multi=True
        )

    @staticmethod
    def dictionary(id_: str = None,
                   type_: str = None,
                   session_id: str = None,
                   properties: dict = None,
                   context = None) -> dict:
        if context is None:
            context = {}
        if properties is None:
            properties = {}
        return {
            "id": id_,
            "type": type_,
            "name": capitalize_event_type_id(type_),
            "metadata": {
                "aux": {},
                "time": {
                    "insert": None,
                    "create": None,
                    "update": None,
                    "process_time": 0,
                },
                "ip": None,
                "status": None,
                "channel": None,
                "processor": {
                    "flows": [],
                    "rules": [],
                    "third_party": []
                },
                "no_profile": False,
                "debug": False,
                "valid": True,
                "warning": False,
                "error": False,
                "instance": {
                    "id": None
                },
            },
            "properties": properties,
            "operation": {
                "new": False,
                "update": False
            },
            "source": {
                "id": None,
                "type": [],
                "bridge": {
                    "id": None,
                    "name": None
                },
                "timestamp": None,
                "name": None,
                "description": None,
                "channel": None,
                "enabled": True,
                "transitional": False,
                "tags": [],
                "groups": [],
                "returns_profile": False,
                "permanent_profile_id": False,
                "requires_consent": False,
                "manual": None,
                "locked": False,
                "synchronize_profiles": True,
                "config": None
            },
            "session": {
                "id": session_id,
                "start": None,
                "duration": 0,
                "tz": "utc"
            },
            "context": context,
            "request": {},
            "config": {},
            "tags": {
                "values": (),
                "count": 0
            },
            "aux": {},
            "data": {},
            "journey": {
                "state": None
            }
        }


class DottyEncoder(json.JSONEncoder):
    """Helper class for encoding of nested Dotty dicts into standard dict
    """

    def default(self, obj):
        """Return dict data of Dotty when possible or encode with standard format

        :param obj: Input object
        :return: Serializable data
        """
        try:
            if hasattr(obj, '_data'):
                return obj._data
            elif isinstance(obj, datetime):
                # Convert datetime to an ISO formatted string
                return obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                return json.JSONEncoder.default(self, obj)
        except TypeError:
            return str(obj)


def flat_events_to_event(flat_events: List[FlatEvent]) -> List[Event]:
    return [Event(**flat_event.to_dict()) for flat_event in flat_events]
