import hashlib
from random import randint
from uuid import uuid4
from typing import Dict, List

from pydantic import BaseModel

from app.service.plugin.domain.register import Plugin


class Position(BaseModel):
    x: float
    y: float


class EdgeBundle:
    def __init__(self, src, edg, tgt):
        self.src = src
        self.edg = edg
        self.tgt = tgt

    def __repr__(self):
        return f"[EdgeBundle: (src: {self.src}) > (edg: {self.edg}) > (tgt: {self.tgt})]"


class EdgeData(BaseModel):
    name: str = ""


class Edge(BaseModel):
    src: str
    src_handler: str
    tgt: str
    tgt_handler: str
    id: str
    type: str
    data: EdgeData = EdgeData()

    def __eq__(self, other: 'Edge'):
        return (other.src == self.src and
                other.tgt == self.tgt and
                other.type == self.type and
                other.src_handler == self.src_handler and
                other.tgt_handler == self.tgt_handler)


class Node(BaseModel):
    id: str
    type: str
    position: Position
    data: Plugin

    def __call__(self, *args, **kwargs):
        if len(args) == 0:
            raise ValueError("Missing port argument.")
        if len(args) != 1:
            raise ValueError("Only one port argument is allowed.")
        return

    def __rshift__(self, other):
        raise ValueError(
            "You can not connect with edge nodes. Only node ports can be connected. Use parentis to indicate node.")


class NodePort:
    def __init__(self, port: str, node: Node):
        self.port = port
        self.node = node

    def __rshift__(self, other: 'NodePort') -> EdgeBundle:
        if self.port not in self.node.data.core.outputs:
            raise ValueError(
                f"Could not find port {self.port} in defined output ports of {self.node.data.core.classname}."
                f"Allowed ports are as follows: {', '.join(self.node.data.core.outputs)}")
        if other.port not in other.node.data.core.inputs:
            raise ValueError(
                f"Could not find port {other.port} in defined input ports of {other.node.data.core.classname}."
                f"Allowed ports are as follows: {', '.join(other.node.data.core.inputs)}")
        other.node.position.y += randint(0, 500)
        other.node.position.x += randint(0, 500)

        edge = Edge(
            id=str(uuid4()),
            type="default",
            src=self.node.id,
            tgt=other.node.id,
            src_handler=self.port,
            tgt_handler=other.port
        )
        return EdgeBundle(
            src=self.node,
            edg=edge,
            tgt=other.node
        )


class FlowGraphData(BaseModel):
    nodes: List[Node]
    edges: List[Edge]

    _id_length_limit = 32

    def __add__(self, other: Node):
        self.nodes.append(other)

    def shorten_edge_ids(self):
        for edge in self.edges:
            if len(edge.id) > self._id_length_limit:
                edge.id = hashlib.md5(edge.id.encode()).hexdigest()

    def get_node_by_id(self, id_: str) -> Node:
        for node in self.nodes:
            if node.id == id_:
                return node
        return None

    def get_nodes_out_edges(self, node_id: str) -> List[Edge]:
        for edge in self.edges:
            if edge.src == node_id:
                yield edge

    def get_nodes_in_edges(self, node_id: str) -> List[Edge]:
        for edge in self.edges:
            if edge.tgt == node_id:
                yield edge

    def remove_out_edge_on_port(self, port):
        def _remove():
            for edge in self.edges:
                if edge.src_handler != port:
                    yield edge

        self.edges = list(_remove())

    def traverse_graph_for_distance(self, start_id: str, distance_map: Dict[str, int] = None, cur_distance: int = 0,
                                    path: List = None) -> Dict:
        """ DFS """
        if distance_map is None:
            distance_map = {}
        if path is None:
            path = []
        if distance_map.get(start_id, -1) < cur_distance:
            distance_map[start_id] = cur_distance

        children = [edge.tgt for edge in self.get_nodes_out_edges(start_id) if edge.tgt not in path]
        for child in children:
            self.traverse_graph_for_distance(child, distance_map, cur_distance + 1, [*path, child])

        return distance_map
