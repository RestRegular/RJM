from datetime import datetime
from pydantic import BaseModel
from typing import Optional, Union

from app.domain.entity import Entity, NullableEntity, PrimaryEntity, NullablePrimaryEntity
from app.domain.time import Time
from app.domain.value_object.storage_info import StorageInfo


class EntityRecordTime(Time):
    due: Optional[datetime] = None
    expire: Optional[datetime] = None


class EntityRecordMetadata(BaseModel):
    time: EntityRecordTime = EntityRecordTime()


class EntityRecord(Entity):
    profile: Union[Entity, NullableEntity]
    metadata: Optional[EntityRecordMetadata] = EntityRecordMetadata()
    type: str
    properties: Optional[dict] = {}
    traits: Optional[dict] = {}

    @staticmethod
    def storage_info() -> StorageInfo:
        return StorageInfo(
            'entity',
            EntityRecord
        )
