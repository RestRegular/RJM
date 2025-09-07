import os

__all__ = [
    "CACHE_DIR_PATH"
]

CACHE_DIR_PATH = os.path.join(os.path.dirname(__file__), "api", "endpoints", "service", "cache")
