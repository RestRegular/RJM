from pydantic import BaseModel
from connection import Connection


class Edge(BaseModel):
    id: str
    source: Connection
    target: Connection
    enabled: bool = True
    # TODO: data: EdgeData = EdgeData()

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
