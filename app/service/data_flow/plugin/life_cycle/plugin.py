import importlib
from typing import Optional

from sqlalchemy.util import await_only

from app.domain.event import Event
from app.domain.session import Session
from app.service.data_flow.domain.flow_history import FlowHistory
from app.service.data_flow.domain.node import Node
from app.service.plugin.domain.result import Result
from app.service.plugin.runner import ActionRunner
from app.domain.flow import Flow
from app.utils.console import Console
from app.utils.getters import get_inst_id


async def create_instance(node: Node) -> ActionRunner:
    module = importlib.import_module(node.module)
    plugin_class = getattr(module, node.class_name)

    action = plugin_class()

    if not isinstance(action, ActionRunner):
        raise TypeError(f"Class {module}.{node.class_name} is not of type {type(ActionRunner)}.")

    return action


def set_context(node: Node,
                event: Optional[Event],
                session: Optional[Session],
                flow: Optional[Flow],
                flow_history: Optional[FlowHistory],
                metics,
                memory,
                ux,
                # tracker_payload,
                execution_graph,
                debug: bool) -> ActionRunner:
    data = node.model_dump(exclude={
        "object": ...
    }, mode='json')
    node.object.node = Node(**data)
    node.object.debug = debug
    node.object.event = event
    node.object.session = session
    node.object.flow = flow
    node.object.flow_history = flow_history
    node.object.metrics = metics
    node.object.memory = memory
    node.object.ux = ux
    node.object.console = Console(node.class_name, node.module, get_inst_id(flow), None, node.id)
    node.object.execution_graph = execution_graph
    node.object.id = node.id
    return node.object


async def execute(node: Node, params: dict) -> Optional[Result]:
    if node.init is not None:
        init = {**node.init, "__debug__": node.debug}
    else:
        init = {"__debug__": node.debug}
    await node.object.set_up(init)

    # params has payload and in_edge
    return await node.object.run(**params)
