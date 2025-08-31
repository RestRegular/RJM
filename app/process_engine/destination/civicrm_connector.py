from typing import Optional, List

from app.process_engine.destination.destination_interface import DestinationInterface
from app.process_engine.action.v1.connectors.civi_crm.client import CiviCRMClient, CiviClientCredentials
from app.domain.flat_profile import FlatProfile
from app.domain.flat_event import FlatEvent


class CiviCRMConnector(DestinationInterface):

    async def _dispatch(self, data):
        if "id" not in data or "email" not in data:
            raise ValueError("Given fields mapping must contain \"id\" and \"email\" keys.")

        credentials = self.resource.credentials.test if self.debug else self.resource.credentials.production

        client = CiviCRMClient(**CiviClientCredentials(**credentials).model_dump())

        await client.add_contact(data)

    async def dispatch_profile(self, data, flat_profile: Optional[FlatProfile],
                               changed_fields: List[dict] = None, metadata=None):
        await self._dispatch(data)

    async def dispatch_event(self, data, flat_event: FlatEvent, metadata=None, profile_id: Optional[str] = None, session_id: Optional[str] = None):
        await self._dispatch(data)
