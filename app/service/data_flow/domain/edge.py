from pydantic import BaseModel
from app.service.data_flow.domain.connection import Connection

from app.service.data_flow.domain.flow_graph_data import EdgeData


class Edge(BaseModel):
    id: str
    source: Connection
    target: Connection
    enabled: bool = True
    data: EdgeData = EdgeData()

    def is_valid(self, nodes):
        # TODO
        pass

    def has_name(self):
        # TODO
        pass

    @property
    def name(self) -> str:
        # TODO
        pass

    def __key(self):
        # TODO
        pass

    def __hash__(self):
        return hash(self.__key())
