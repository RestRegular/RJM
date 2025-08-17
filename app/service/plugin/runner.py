from typing import Optional, Dict, final
from pydantic import BaseModel

from app.service.plugin.domain.result import Result
from app.utils.console import Console
from app.domain.event import Event
from app.utils.data_visitor import DataVisitor


class ReshapeTemplate(BaseModel):
    template: str = ""
    default: bool = True


class JoinSettings(BaseModel):
    merge: bool = False
    reshape: Dict[str, ReshapeTemplate] = None
    type: str = 'dict'

    def has_reshape_templates(self) -> bool:
        return self.reshape is not None

    def get_reshape_template(self, port) -> ReshapeTemplate:
        return self.reshape[port]


class ActionRunner:
    id = None
    debug = True
    event: Event = None
    session = None
    flow: BaseModel = None  # Flow
    flow_history = None
    console: Console = None
    node = None
    memory: dict = None
    execution_graph = None  # GraphInvoker
    ux: list = None
    join: JoinSettings = None

    @final
    def __init__(self):
        pass

    async def set_up(self, init):
        pass

    async def run(self, payload: dict, in_edge=None):
        pass

    async def close(self):
        pass

    async def on_error(self, e):
        pass

    def _get_dot_accessor(self, payload) -> DataVisitor:
        return DataVisitor.new(profile=self.profile, session=self.session, payload=payload,
                               event=self.event, flow=self.flow, memory=self.memory)

    def update_profile(self):
        if isinstance(self.profile, Profile):
            self.profile.mark_for_update()
            self.profile.data.compute_anonymous_field()
            # Removed not needed to always hash IDS. Hash only on update.
            # self.profile.hash_all_allowed_pii_as_ids()
        else:
            if self.event.metadata.profile_less is True:
                self.console.warning("Can not update profile when processing profile less events.")
            else:
                self.console.error("Can not update profile. Profile is empty.")

    def discard_profile_update(self):
        if isinstance(self.profile, Profile):
            self.profile.set_updated(False)
        else:
            if self.event.metadata.profile_less is True:
                self.console.warning("Can not update profile when processing profile less events.")
            else:
                self.console.error("Can not update profile. Profile is empty.")

    def set_tracker_option(self, key, value):
        if isinstance(self.tracker_payload, TrackerPayload):
            self.tracker_payload.options[key] = value

    def join_output(self) -> bool:
        return isinstance(self.join, JoinSettings) and self.join.merge is True

    def get_error_result(self, message, port) -> Result:
        self.console.error(message)
        return Result(port=port, value={
            "message": message
        })
