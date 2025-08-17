import uuid
from datetime import datetime
from typing import Optional, List, Any

from pydantic import BaseModel

from app.domain.named_inst import NamedInstInContext
from app.service.data_flow.domain.flow_graph_data import FlowGraphData, EdgeBundle
from app.service.data_flow.domain.flow_graph import FlowGraph
from app.service.plugin.domain.register import MetaData, Plugin, Core, NodeEvents
from app.config import rjm
from app.utils.log_system.logger import get_logger
from app.utils.secrets import decrypt, encrypt, b64_encoder, b64_decoder
from app.utils.date import now_in_utc

logger = get_logger(__name__)


class FlowSchema(BaseModel):
    version: str = rjm.version.version
    url: str = 'http://...'  # TODO
    server_version: str = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.server_version = rjm.version.version


class Flow(FlowGraph):
    tags: Optional[List[str]] = ["General"]
    lock: bool = False
    type: str
    timestamp: Optional[datetime] = None
    deploy_timestamp: Optional[datetime] = None
    file_name: Optional[str] = None
    df_schema: FlowSchema = FlowSchema()

    def arrange_nodes(self):
        if self.flow_graph is not None:
            targets = {edge.tgt for edge in self.flow_graph.edges}
            starting_nodes = [node for node in self.flow_graph.nodes if node.id not in targets]
            start_at = [0, 0]
            for starting_node in starting_nodes:
                node_to_distance_map = self.flow_graph.traverse_graph_for_distance(start_id=starting_node.id)
                for node_id in node_to_distance_map:
                    node = self.flow_graph.get_node_by_id(node_id)
                    node.position.y = start_at[1] + 150 * node_to_distance_map[node_id]
                distance_to_nodes_map = {}
                for node_id in node_to_distance_map:
                    if node_to_distance_map[node_id] not in distance_to_nodes_map:
                        distance_to_nodes_map[node_to_distance_map[node_id]] = []
                    distance_to_nodes_map[node_to_distance_map[node_id]].append(node_id)
                for node_ids in distance_to_nodes_map.values():
                    nodes = [self.flow_graph.get_node_by_id(node_id) for node_id in node_ids]
                    row_center = start_at[0] - 200 * len(nodes) + 250
                    for node in nodes:
                        node.position.x = row_center - node.data.metadata.width // 2
                        row_center += node.data.metadata.width
                start_at[0] += len(max(distance_to_nodes_map.values(), key=len)) * 200

    def get_empty_workflow_record(self, type_: str) -> 'FlowRecord':
        return FlowRecord(
            id=self.id,
            timestamp=now_in_utc(),
            desc=self.desc,
            name=self.name,
            tags=self.tags,
            lock=self.lock,
            type=type_
        )

    @staticmethod
    def from_workflow_record(record: 'FlowRecord') -> Optional['Flow']:
        if 'type' not in record.draft:
            record.draft['type'] = record.type
        flow = Flow(**record.draft)
        flow.deploy_timestamp = record.deploy_timestamp
        flow.timestamp = record.timestamp
        flow.file_name = record.file_name
        flow.id = record.id
        if not flow.timestamp:
            flow.timestamp = now_in_utc()
        return flow

    @staticmethod
    def new(id_: str = None) -> 'Flow':
        return Flow(
            id=str(uuid.uuid4()) if id_ is None else id_,
            timestamp=now_in_utc(),
            name="Empty",
            df_schema=FlowSchema(version=str(rjm.version)),
            flow_graph=FlowGraphData(nodes=[], edges=[]),
            type='collection'
        )

    @staticmethod
    def build(name: str, desc: str = None, id_: str = None,
              lock: bool = False, tags: List[str] = None,
              type_: str = 'collection') -> 'Flow':
        if tags is None:
            tags = ["General"]
        return Flow(
            id=str(uuid.uuid4()) if id_ is None else id_,
            timestamp=now_in_utc(),
            name=name,
            df_schema=FlowSchema(version=str(rjm.version)),
            desc=desc,
            tags=tags,
            lock=lock,
            flow_graph=FlowGraphData(nodes=[], edges=[]),
            type=type_
        )

    def __add__(self, edge_bundle: EdgeBundle):
        if edge_bundle.src not in self.flow_graph.nodes:
            self.flow_graph.nodes.append(edge_bundle.src)
        if edge_bundle.tgt not in self.flow_graph.nodes:
            self.flow_graph.nodes.append(edge_bundle.tgt)
        if edge_bundle.edg not in self.flow_graph.edges:
            self.flow_graph.edges.append(edge_bundle.edg)
        else:
            logger.warning(f"Edge {edge_bundle.edg} already exists.")
        return self


class CoreRecord(BaseModel):
    id: str
    classname: str
    module: str
    inputs: Optional[List[str]] = []
    outputs: Optional[List[str]] = []
    init: Optional[str] = ""
    node: Optional[NodeEvents] = None
    # form: Optional[str] = None
    manual: Optional[str] = None
    author: Optional[str] = None
    license: Optional[str] = "MIT"
    version: Optional[str] = "0.0.1"

    @staticmethod
    def encode(core: Core) -> 'CoreRecord':
        return CoreRecord(
            id=core.id,
            classname=core.classname,
            module=core.module,
            inputs=core.inputs,
            outputs=core.outputs,
            init=encrypt(core.init),
            node=core.node,
            manual=core.manual,
            author=core.author,
            license=core.license,
            version=core.version
        )

    def decode(self) -> Core:
        return Core(
            id=self.id,
            classname=self.classname,
            module=self.module,
            inputs=self.inputs,
            outputs=self.outputs,
            init=decrypt(self.init),
            node=self.node,
            manual=self.manual,
            author=self.author,
            license=self.license,
        )


class MetaDataRecord(BaseModel):
    name: str
    desc: Optional[str] = ""
    keywords: Optional[List[str]] = []
    type: str = 'flowNode'
    width: int = 300
    height: int = 100
    icon: str = 'plugin'
    documentation: Optional[str] = ""
    group: Optional[List[str]] = ["General"]
    tags: List[str] = []
    pro: bool = False
    commercial: bool = False
    remote: bool = False
    frontend: bool = False
    emits_event: Optional[str] = ""
    purpose: List[str] = ['collection']

    @staticmethod
    def encode(metadata: MetaData) -> 'MetaDataRecord':
        return MetaDataRecord(
            name=metadata.name,
            desc=metadata.desc,
            keywords=metadata.keywords,
            type=metadata.type,
            width=metadata.width,
            height=metadata.height,
            icon=metadata.icon,
            documentation=b64_encoder(metadata.documentation),
            group=metadata.group,
            tags=metadata.tags,
            pro=metadata.pro,
            commercial=metadata.commercial,
            remote=metadata.remote,
            frontend=metadata.frontend,
            emits_event=b64_encoder(metadata.emit_events),
            purpose=metadata.purpose
        )

    def decode(self) -> MetaData:
        return MetaData(
            name=self.name,
            desc=self.desc,
            group=self.group,
            tags=self.tags,
            pro=self.pro,
            commercial=self.commercial,
            remote=self.remote,
            frontend=self.frontend,
            emit_events=b64_decoder(self.emits_event),
            purpose=self.purpose,
            type=self.type,
            height=self.height,
            width=self.width,
            icon=self.icon,
            keywords=self.keywords,
            documentation=b64_decoder(self.documentation),
        )


class PluginRecord(BaseModel):
    start: bool = False
    debug: bool = False
    core: CoreRecord
    metadata: MetaDataRecord

    @staticmethod
    def encode(plugin: Plugin) -> 'PluginRecord':
        return PluginRecord(
            start=plugin.start,
            debug=plugin.debug,
            core=CoreRecord.encode(plugin.core),
            metadata=MetaDataRecord.encode(plugin.metadata)
        )

    def decode(self) -> Plugin:
        data = {
            "start": self.start,
            "debug": self.debug,
            "core": self.core.decode(),
            "metadata": self.metadata.decode()
        }
        return Plugin.model_construct(_fields_set=self.model_fields_set, **data)


class FlowRecord(NamedInstInContext):
    timestamp: Optional[datetime] = None
    deploy_timestamp: Optional[datetime] = None
    desc: Optional[str] = None
    tags: Optional[List[str]] = ["General"]
    file_name: Optional[str] = None
    draft: Optional[dict] = {}
    lock: bool = False
    type: str

    def get_empty_workflow(self, id_) -> Flow:
        return Flow.build(
            id_=id_,
            name=self.name,
            desc=self.desc,
            tags=self.tags,
            lock=self.lock
        )
