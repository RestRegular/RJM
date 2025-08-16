import json
from typing import Dict, List

from pydantic.v1.utils import deep_update


class FlowResponse(Dict[str, Dict]):
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

    def __setitem__(self, key, value: dict):
        if not isinstance(value, dict):
            raise TypeError("FlowResponse value must be a dict")
        dict.__setitem__(self, key, value)


class FlowResponses:
    def __init__(self, responses: List[dict]):
        self.responses = responses

    def merge(self):
        merged_response = {}
        for response in self.responses:
            if response:
                merged_response = deep_update(merged_response, response)
        return json.loads(json.dumps(merged_response, default=str))
