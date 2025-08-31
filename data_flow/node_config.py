from typing import Dict, Any, Optional, Callable

from pydantic import BaseModel


class NodeConfig(BaseModel):
    """节点配置参数（根据节点类型动态定义）"""
    config: Dict[str, Any]

    def __init__(self, /, input_processor: Optional[Callable] = None, **kwargs):
        super().__init__(config={
            "input_processor": input_processor,
            **kwargs
        }, **kwargs)

    def get_config(self, key: str, default: Any = None):
        return self.config.get(key, default)

    def set_config(self, key: str, value: Any):
        self.config[key] = value

    def __getattr__(self, item: str) -> Any:
        return self.get_config(item)

    def __setattr__(self, key: str, value: Any) -> str:
        self.set_config(key, value)
