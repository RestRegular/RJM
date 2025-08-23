from typing import List, Dict, Optional, Union, AsyncIterable
import uuid

from pydantic import BaseModel

from data_flow.enum_data import DataType


# 端口定义（节点的输入/输出端口，用于数据流转的接口）
class Port(BaseModel):
    id: str  # 端口唯一标识
    name: str  # 端口名称
    data_type: Union[DataType, str]  # 端口支持的数据类型
    required: bool = False  # 是否为必填端口（输入端口有效）
