from typing import Optional, List

from pydantic import BaseModel

from app.domain.named_entity import NamedEntity, NamedEntityInContext
from app.domain.ref_value import RefValue


class IdentificationField(BaseModel):
    profile_trait: RefValue
    event_property: RefValue


class IdentificationPoint(NamedEntityInContext):
    description: Optional[str] = ""
    source: NamedEntity
    event_type: NamedEntity
    fields: Optional[List[IdentificationField]] = None
    enabled: bool = False
    settings: Optional[dict] = {}
