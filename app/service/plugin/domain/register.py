import hashlib
from typing import Optional, Any, Dict, List

from pydantic import BaseModel


class RunOnce(BaseModel):
    value: Optional[str] = ""
    ttl: int = 0
    type: str = "value"
    enable: bool = False


class NodeEvents(BaseModel):
    on_create: Optional[str] = None
    on_remove: Optional[str] = None


class Core(BaseModel):
    id: Optional[str] = None
    classname: str
    module: str
    inputs: Optional[List[str]] = []
    outputs: Optional[List[str]] = []
    init: Optional[dict] = None
    # microservice: Optional[MicroserviceConfig] = MicroserviceConfig.create()
    skip: bool = False
    block_flow: bool = False
    run_in_background: bool = False
    on_error_continue: bool = False
    on_connection_error_repeat: int = 0
    append_input_payload: bool = False
    join_input_payload: bool = False
    # form: Optional[Form] = None
    manual: Optional[str] = None
    author: Optional[str] = None
    license: Optional[str] = "MIT"
    version: Optional[str] = "0.0.1"
    run_once: Optional[RunOnce] = RunOnce()
    node: Optional[NodeEvents] = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.id = self.get_id()

    # def has_microservice(self) -> bool:
    #     pass

    def get_id(self) -> str:
        action_id = f"{self.module}@{self.classname}#{self.version}"
        # If defined resource for microservice plugin action add that to the id.
        # You can create one microservice per remote server.
        # if self.has_microservice() and self.microservice.has_server_resource():
        #     action_id += self.microservice.server.resource.id
        return hashlib.md5(action_id.encode()).hexdigest()


class Desc(BaseModel):
    desc: str


class Documentation(BaseModel):
    details: Optional[Desc]
    inputs: Dict[str, Desc]
    outputs: Dict[str, Desc]


class MetaData(BaseModel):
    name: str
    project: Optional[str] = "Resume-JobMatcher"
    desc: Optional[Desc] = Desc(desc="")
    keywords: Optional[List[str]] = []
    type: str = "flow_node"
    width: int = 300
    height: int = 100
    icon: str = "plugin"
    documentation: Optional[Documentation] = None
    group: Optional[List[str]] = ["General"]
    tags: List[str] = []
    pro: bool = False
    commercial: bool = False
    remote: bool = False
    frontend: bool = False
    emit_events: Optional[Dict[str, str]] = {}
    purpose: List[str] = ["data flow"]


class Plugin(BaseModel):
    start: bool = False
    debug: bool = False
    core: Core
    metadata: MetaData