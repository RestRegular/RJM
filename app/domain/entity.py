from contextlib import contextmanager

from datetime import datetime

import json

from dotty_dict import Dotty
from typing import Optional, TypeVar, Type, Set, List, Union
from uuid import uuid4
from pydantic import BaseModel, PrivateAttr

from app.domain import ExtraInfo
from app.domain.storage_record import RecordMetadata, StorageRecord
from app.domain.time import Time
from app.domain.value_object.storage_info import StorageInfo
from app.exceptions.log_handler import get_logger
from app.protocol.operational import Operational
from app.service.change_monitoring.field_update_logger import FieldUpdateLogger
from app.service.dot_notation_converter import dotter
from app.service.storage.index import Resource

logger = get_logger(__name__)

T = TypeVar("T")


class Creatable(BaseModel):

    @classmethod
    def create(cls: Type[T], record: Optional[StorageRecord]) -> Optional[T]:
        if not record:
            return None

        obj = cls(**dict(record))

        if hasattr(obj, 'set_meta_data'):
            obj.set_meta_data(record.get_meta_data())
        return obj


class NullableEntity(Creatable):
    id: Optional[str] = None


class NullablePrimaryEntity(NullableEntity):
    primary_id: Optional[str] = None


class Entity(Creatable):
    id: str
    _metadata: Optional[RecordMetadata] = PrivateAttr(None)

    def set_meta_data(self, metadata: RecordMetadata = None) -> 'Entity':
        self._metadata = metadata
        return self

    def get_meta_data(self) -> Optional[RecordMetadata]:
        return self._metadata if isinstance(self._metadata, RecordMetadata) else None

    def _fill_meta_data(self, index_type: str):
        """
        Used to fill metadata with default current index and id.
        """
        if not self.has_meta_data():
            resource = Resource()
            self.set_meta_data(RecordMetadata(id=self.id, index=resource[index_type].get_write_index()))

    def dump_meta_data(self) -> Optional[dict]:
        return self._metadata.model_dump(mode='json') if isinstance(self._metadata, RecordMetadata) else None

    def has_meta_data(self) -> bool:
        return self._metadata is not None

    def to_storage_record(self, exclude=None, exclude_unset: bool = False) -> StorageRecord:
        record = StorageRecord(**self.model_dump(exclude=exclude, exclude_unset=exclude_unset))

        # Storage records must have ES _id

        if 'id' in record:
            record['_id'] = record['id']

        if self._metadata:
            record.set_meta_data(self._metadata)
        else:
            # Does not have metadata
            storage_info = self.storage_info()  # type: Optional[StorageInfo]
            if storage_info and storage_info.multi is True:
                if isinstance(self, Operational):
                    if self.operation.new is False:
                        # This is critical error of the system. It should be reported to the vendor.
                        logger.error(
                            f"Entity {type(self)} does not have index set. And it is not new.",
                            extra=ExtraInfo.build(object=self, origin="storage", error_number="S0001")
                        )
                else:
                    # This is critical warning of the system. It should be reported to the vendor.
                    logger.warning(
                        f"Entity {type(self)} converts to index-less storage record.",
                        extra=ExtraInfo.build(object=self, origin="storage", error_number="S0002")
                    )
        return record

    @staticmethod
    def new() -> 'Entity':
        return Entity(id=str(uuid4()))

    @staticmethod
    def storage_info() -> Optional[StorageInfo]:
        return None

    def get_dotted_properties(self) -> Set[str]:
        return dotter(self.model_dump())

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id if isinstance(other, Entity) else False


class DefaultEntity(Entity):
    metadata: Optional[Time] = None


class PrimaryEntity(Entity):
    primary_id: Optional[str] = None
    metadata: Optional[Time] = None
    ids: Optional[List[str]] = None


class DottyEncoder(json.JSONEncoder):
    """Helper class for encoding of nested Dotty dicts into standard dict
    """

    def default(self, obj):
        """Return dict data of Dotty when possible or encode with standard format

        :param object: Input object
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


class FlatEntity(Dotty):

    def __init__(self, dictionary):
        self._changes: Optional[FieldUpdateLogger] = None
        super().__init__(dictionary)
        self._metadata = None
        # Keeps current changes
        self._changes: Optional[FieldUpdateLogger] = None

    def monitor_changes(self, flag: bool) -> 'FlatEntity':
        if flag:
            self._changes = FieldUpdateLogger()
        else:
            self._changes = None
        return self

    def __getstate__(self):
        # Here, you should retrieve the state, not set it.
        state = super().__getstate__()
        state['_metadata'] = self._metadata
        state['_changes'] = self._changes
        return state

    def __setstate__(self, state):
        # Here, you should call the base class' setstate, not getstate.
        super().__setstate__(state)
        self._metadata = state.get('_metadata', None)
        self._changes = state.get('_changes', None)

    def __setitem__(self, key, value):
        if self._changes:
            old_value = self.get(key, None)
            self._changes.add(key, value, old_value, ignore=('metadata.fields', 'operation'))
        super().__setitem__(key, value)

    def set(self, key, value, session_id: Optional[str] = None, event_type: Optional[str] = None):
        if self._changes:
            old_value = self.get(key, None)
            self._changes.add(key,
                              value,
                              old_value,
                              session_id,
                              event_type,
                              ignore=('metadata.fields', 'operation')
                              )
        super().__setitem__(key, value)

    def override(self, key, value):
        # This one does not record changes or checks for PCP
        super().__setitem__(key, value)

    def get_change_logger(self) -> FieldUpdateLogger:
        return self._changes if self._changes is not None else FieldUpdateLogger()

    def has_changes(self) -> bool:
        return bool(self._changes) and self._changes.has_changes()

    def clear_changes(self):
        self._changes = FieldUpdateLogger()

    def to_json(self):
        """Return wrapped dictionary as json string.
        This method does not copy wrapped dictionary.
        :return str: Wrapped dictionary as json string
        """
        return json.dumps(self._data, cls=DottyEncoder)

    def instanceof(self, field: str, instance: Union[type, tuple]) -> bool:
        if field not in self:
            return False
        return isinstance(self.get(field, None), instance)

    @property
    def id(self) -> Optional[str]:
        return self.get('id', None)

    @id.setter
    def id(self, value: str):
        """Setter method"""
        if not isinstance(value, str):
            raise ValueError("ID value must be a string.")

        self['id'] = value

    def get_meta_data(self) -> Optional[RecordMetadata]:
        return self._metadata if isinstance(self._metadata, RecordMetadata) else None

    def has_meta_data(self) -> bool:
        return self._metadata is not None

    def set_meta_data(self, metadata: RecordMetadata = None) -> 'FlatEntity':
        self._metadata = metadata
        return self

    def to_storage_record(self) -> StorageRecord:

        as_dict = self.to_dict()
        if 'operation' in as_dict:
            del as_dict['operation']

        record = StorageRecord(**as_dict)

        # Storage records must have ES _id

        if 'id' in record:
            record['_id'] = record['id']

        if self._metadata:
            record.set_meta_data(self._metadata)

        return record


@contextmanager
def change_monitor(flat_entity: FlatEntity):
    flat_entity.monitor_changes(True)
    yield
    flat_entity.monitor_changes(False)
