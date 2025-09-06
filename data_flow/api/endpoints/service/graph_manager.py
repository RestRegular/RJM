import json
import os
from types import ModuleType
import importlib
from typing import Union, List, Dict, Any, Optional
from contextlib import contextmanager

from data_flow.domain.graph import Graph

GRAPH_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "domain", "graphs")
GRAPH_INFO_PATH = os.path.join(GRAPH_DIR, "graph_info.json")
os.makedirs(GRAPH_DIR, exist_ok=True)

GRAPH_INFO: Optional[Dict[str, Any]] = None
if os.path.exists(GRAPH_INFO_PATH):
    with open(GRAPH_INFO_PATH, "r", encoding="utf-8") as _f:
        GRAPH_INFO = json.load(_f)
if GRAPH_INFO is None:
    with open(GRAPH_INFO_PATH, "w", encoding="utf-8"):
        GRAPH_INFO = {}


class GraphManager:
    _graph_storage: Dict[str, Graph] = {}

    def __init__(self):
        pass

    @classmethod
    def get_storage(cls) -> Dict[str, Graph]:
        return cls._graph_storage

    @classmethod
    def get_graph(cls, graph_id: str) -> Graph:
        return cls._graph_storage.get(graph_id, None) or cls.dynamic_import_graph(graph_id)

    @classmethod
    def has_graph(cls, graph_id: str) -> bool:
        return graph_id in cls._graph_storage or graph_id in GRAPH_INFO \
            or os.path.exists(os.path.join(GRAPH_DIR, f"{graph_id}.py"))

    @staticmethod
    def _build_graph(module: ModuleType) -> Graph:
        if hasattr(module, "build_graphs"):
            graph = module.build_graphs()[0]
        elif hasattr(module, "generate_graphs"):
            graph = module.generate_graphs()[0]
        else:
            raise ValueError("Invalid module")
        return graph

    @classmethod
    def storage_graphs(cls, graph: Graph, filepath: str):
        with open(GRAPH_INFO_PATH, "w", encoding="utf-8") as f:
            cls._graph_storage[graph.id] = graph
            with open(os.path.join(GRAPH_DIR, f"{graph.id}.py"), "w", encoding="utf-8") as tf, \
                    open(filepath, "r", encoding="utf-8") as sf:
                tf.write(sf.read())
            GRAPH_INFO[graph.id] = graph.to_dict_info()
            json.dump(GRAPH_INFO, f, indent=2)

    @classmethod
    def dynamic_import_graph_from_cache(cls, graph_id: str, filepath: str) -> Graph:
        if cls.has_graph(graph_id):
            return cls.get_graph(graph_id)

        graph = cls._build_graph(importlib.import_module(f"data_flow.api.endpoints.service.cache.{graph_id}"))

        cls.storage_graphs(graph, filepath)
        return graph

    @classmethod
    def dynamic_import_graph(cls, graph_id: str) -> Graph:
        if graph_id in cls._graph_storage:
            return cls.get_graph(graph_id)
        if not os.path.exists(os.path.join(GRAPH_DIR, f"{graph_id}.py")):
            raise FileNotFoundError(f"Graph {graph_id} not found")
        graph = cls._build_graph(importlib.import_module(f"data_flow.domain.graphs.{graph_id}"))
        graph.id = graph_id
        cls._graph_storage[graph_id] = graph
        return graph

    @staticmethod
    def get_graph_info(specific_ids: Union[List[str], str]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        return [GRAPH_INFO[gid] for gid in specific_ids if gid in GRAPH_INFO] if isinstance(specific_ids, list)\
            else (GRAPH_INFO[specific_ids] if specific_ids in GRAPH_INFO else None)

    @staticmethod
    def get_all_graph_info() -> Dict[str, Any]:
        return GRAPH_INFO

    @staticmethod
    def search_graph(prompt: str) -> List[str]:
        if not prompt or not GRAPH_INFO:
            return []
        prompt_lower = prompt.lower()
        matched_ids = []
        for _id, graph_info in GRAPH_INFO.items():
            if (prompt_lower in _id.lower()) or (prompt_lower in graph_info['name'].lower()):
                matched_ids.append(_id)
        return matched_ids

    @classmethod
    def delete_graph(cls, graph_id: str):
        if cls.has_graph(graph_id):
            if os.path.exists(os.path.join(GRAPH_DIR, f"{graph_id}.py")):
                os.remove(os.path.join(GRAPH_DIR, f"{graph_id}.py"))
                cls._graph_storage.pop(graph_id)
                GRAPH_INFO.pop(graph_id)

    @classmethod
    @contextmanager
    def deleting_graphs(cls):
        try:
            yield cls
        finally:
            with open(GRAPH_INFO_PATH, "w", encoding="utf-8") as f:
                json.dump(GRAPH_INFO, f, indent=2)
