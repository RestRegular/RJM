from typing import Optional

from app.domain.entity import Entity
from app.domain.entity_record import EntityRecordMetadata


class RemoteSystemIntegrationId(Entity):
    profile: Entity
    metadata: Optional[EntityRecordMetadata] = EntityRecordMetadata()
    traits: Optional[dict] = {}

    def get_first_id(self) -> Optional[str]:
        return self.traits.get('id', None)