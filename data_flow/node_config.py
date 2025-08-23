from typing import Dict, Any, Optional
from pydantic import BaseModel


class NodeConfig(BaseModel):
    """节点配置参数（根据节点类型动态定义）"""
    config: Dict[str, Any]

    def __init__(self, /, **kwargs):
        super().__init__(config=kwargs, **kwargs)

    def get_config(self, key: str, default: Any = None):
        return self.config.get(key, default)

    def set_config(self, key: str, value: Any):
        self.config[key] = value
