import json
from typing import Dict, List

from pydantic.v1.utils import deep_update
from pydantic_core import core_schema


class FlowResponse(Dict[str, Dict]):
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

    def __setitem__(self, key, value: dict):
        if not isinstance(value, dict):
            raise TypeError("FlowResponse value must be a dict")
        dict.__setitem__(self, key, value)

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        """为Pydantic提供类型验证规则"""

        # 验证输入是否为FlowResponse实例或可转换为它的字典
        def validate(value):
            if isinstance(value, cls):
                return value
            elif isinstance(value, dict):
                # 确保字典的键是字符串，值是字典
                for k, v in value.items():
                    if not isinstance(k, str) or not isinstance(v, dict):
                        raise ValueError("FlowResponse requires str keys and dict values")
                return cls(value)
            else:
                raise TypeError(f"Expected FlowResponse or dict, got {type(value)}")

        return core_schema.no_info_plain_validator_function(validate)


class FlowResponses:
    def __init__(self, responses: List[dict]):
        self.responses = responses

    def merge(self):
        merged_response = {}
        for response in self.responses:
            if response:
                merged_response = deep_update(merged_response, response)
        return json.loads(json.dumps(merged_response, default=str))
