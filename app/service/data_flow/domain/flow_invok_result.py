from typing import Optional, List


from app.domain.event import Event
from app.domain.session import Session

from app.utils.console import Log
from app.utils.logging.logger import get_logger
from app.service.data_flow.domain.debug_info import DebugInfo
from app.service.data_flow.domain.flow_graph import FlowGraph

logger = get_logger(__name__)


class FlowInvokerResult:
    def __init__(self, debug_info: DebugInfo, log_list: List[Log], flow: FlowGraph, event: Event,
                 session: Optional[Session] = None):
        self.debug_info = debug_info
        self.log_list: List[Log] = log_list
        self.event = event
        self.session = session
        self.flow = flow

    def __repr__(self):
        return (f"[FlowInvokeResult:\n"
                f"\t[session: {self.session}]\n"
                f"\t[event: {self.event}]]")

    def register_logs_in_logger(self):
        for log in self.log_list:
            if log.is_error():
                logger.error(
                    log.message,
                    extra=log.to_extra()
                )
            elif log.is_waring():
                logger.warning(
                    log.message,
                    extra=log.to_extra()
                )
            elif log.is_debug():
                logger.debug(
                    log.message,
                    extra=log.to_extra()
                )
