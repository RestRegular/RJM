import os
import importlib


def dynamic_import_graph(filename: str):
    module = importlib.import_module(f"api.endpoints.service.cache.{filename}")

    if hasattr(module, "build_graphs"):
        return module.build_graphs()
    elif hasattr(module, "generate_graphs"):
        return module.generate_graphs()
    else:
        raise ValueError("Invalid module")
