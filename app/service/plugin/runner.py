from typing import Optional

from pydantic import BaseModel

from app.service.plugin.domain.console import Console


class ActionRunner:
    id = None
    debug = True
    session = None
    flow: BaseModel = None  # Flow
    flow_history = None
    console: Console = None
    node = None
    memory: dict = None
    execution_graph = None  # GraphInvoker
    ux: list = None
    join = None

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

    def _get_dot_accessor(self, payload) -> DotAccessor:
        return DotAccessor(self.profile, self.session, payload, self.event, self.flow, self.memory)

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