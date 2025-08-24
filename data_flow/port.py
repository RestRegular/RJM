from typing import List, Dict, Optional, Union, AsyncIterable
import uuid

from pydantic import BaseModel, Field

from data_flow.enum_data import DataType


# 端口定义（节点的输入/输出端口，用于数据流转的接口）
class Port(BaseModel):
    id: str = Field(default_factory=lambda :str(uuid.uuid4()))  # 端口唯一标识
    name: str  # 端口名称
    data_type: Union[DataType, str]  # 端口支持的数据类型
    required: bool = False  # 是否为必填端口（输入端口有效）

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"<Port{'(R)' if self.required else ''}: ({self.data_type})({self.name})({self.id})>"
