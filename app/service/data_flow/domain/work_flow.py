from time import time

from app.utils.log_system.logger import get_logger
from app.service.data_flow.domain.inst import Inst as DFInst
from app.domain.event import Event
from app.domain.flow import Flow
from app.service.data_flow.domain.flow_invok_result import FlowInvokeResult
from app.service.data_flow.domain.debug_info import DebugInfo, FlowDebugInfo
from app.service.data_flow.domain.flow_history import FlowHistory
from app.service.data_flow.domain.graph_invoker import GraphInvoker
from app.utils.dag_error import DagGraphError
from app.utils.dag_processor import DagProcessor
from app.utils.flow_graph_converter import FlowGraphConverter

logger = get_logger(__name__)

class WorkFlow:

    def __init__(self, flow_history: FlowHistory):
        self.flow_history = flow_history

    def _make_dag(self, flow: Flow, debug: bool) -> GraphInvoker:
        # Convert Editor graph to exec graph
        converter = FlowGraphConverter(flow.flowGraph.model_dump())
        dag_graph = converter.convert_to_dag_graph()
        dag = DagProcessor(dag_graph)

        try:
            # If scheduled event find node with defined id
            if self.scheduled_event_config is not None and self.scheduled_event_config.is_scheduled():
                # It must be equal to scheduled node id
                node_id = self.scheduled_event_config.node_id
                return dag.make_execution_dag(start_nodes=dag.find_scheduled_nodes(node_ids=[node_id]), debug=debug)
            return dag.make_execution_dag(start_nodes=dag.find_start_nodes(), debug=debug)
        except DagGraphError as e:
            message = "Flow `{}` returned the following error: `{}`".format(flow.id, str(e))
            logger.error(message)
            raise DagGraphError(message)

    async def _run(self, exec_dag: GraphInvoker, flow: Flow, event: Event, session, ux: list) -> FlowInvokeResult:
        flow_start_time = time()
        debug_info = DebugInfo(
            timestamp=flow_start_time,
            flow_debug_info=FlowDebugInfo(id=flow.id, name=flow.name),
            event=WfEntity(id=event.id)
        )
        log_list = []

        # Init and run with event
        debug_info = await exec_dag.init(
            debug_info,
            log_list,
            flow,
            self.flow_history,
            event,
            session,
            ux)

        if not debug_info.has_errors():
            debug_info, log_list, session, event = await exec_dag.run(
                payload={},
                event=event,
                session=session,
                debug_info=debug_info,
                log_list=log_list
            )

        await exec_dag.close()

        return FlowInvokeResult(debug_info, log_list, flow, event, session)

    async def invoke(self, flow: Flow, event: Event, session, ux: list, debug) -> FlowInvokeResult:

        """
        Invokes workflow and returns DebugInfo and list of saved Logs.
        """

        if event is None:
            raise DagGraphError(
                "Flow `{}` has no context event defined.".format(
                    flow.id))

        if not flow.flowGraph:
            raise DagGraphError("Flow {} is empty".format(flow.id))

        if self.flow_history.is_acyclic(flow.id):
            exec_dag = self._make_dag(flow, debug=debug)
            result = await self._run(exec_dag, flow, event, session, ux)
            return result

        raise RuntimeError("Workflow has circular reference.")
