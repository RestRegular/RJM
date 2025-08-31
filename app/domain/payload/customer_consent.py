from typing import Optional

from pydantic import BaseModel

from app.domain.entity import Entity


class CustomerConsent(BaseModel):
    source: Entity
    session: Entity
    profile: Entity
    consents: Optional[dict] = {}
