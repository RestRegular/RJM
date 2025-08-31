import json
from enum import Enum
from typing import Any


# 数据类型枚举（定义节点输入/输出数据的格式）
class DataType(str, Enum):
    JSON = "json"       # 通用JSON结构
    TEXT = "text"       # 文本数据
    NUMBER = "number"   # 数值型
    DATE = "date"       # 日期型
    BOOLEAN = "boolean" # 布尔型
    BINARY = "binary"   # 二进制数据
    LIST = "list"       # 列表数据
    DICT = "dict"       # 字典数据
    ANY = "any"         # 任意数据
    NONE = "none"       # 空数据

    def __str__(self):
        return self.value

    def validate(self, data: Any):
        if self == DataType.JSON:
            try:
                json.loads(data)
            except json.JSONDecodeError:
                return False
        elif self == DataType.TEXT:
            return isinstance(data, str)
        elif self == DataType.NUMBER:
            return isinstance(data, (int, float))
        elif self == DataType.DATE:
            from datetime import datetime
            return isinstance(data, datetime)
        elif self == DataType.BOOLEAN:
            return isinstance(data, bool)
        elif self == DataType.LIST:
            return isinstance(data, list)
        elif self == DataType.DICT:
            return isinstance(data, dict)
        elif self == DataType.BINARY:
            return isinstance(data, bytes)
        elif self == DataType.ANY:
            return True
        elif self == DataType.NONE:
            return data is None
        else:
            raise ValueError(f"不支持的数据类型: {self}")

    @staticmethod
    def from_str(dtype: str) -> "DataType":
        for k, t in DataType.__dict__.items():
            t = getattr(DataType, k)
            if k.lower() == dtype.lower() and isinstance(t, DataType):
                return t
        raise ValueError(f"不支持的数据类型: {dtype}")


class NodeStatus(str, Enum):
    PENDING = "pending"  # 待执行
    RUNNING = "running"  # 执行中
    SUCCESS = "success"  # 执行成功
    FAILED = "failed"  # 执行失败

    def __str__(self):
        return self.value


class GraphStatus(str, Enum):
    PENDING = "pending"  # 待执行
    RUNNING = "running"  # 执行中
    COMPLETED = "completed"  # 执行完成
    FAILED = "failed"  # 执行失败

    def __str__(self):
        return self.value


class ResultStatus(str, Enum):
    SUCCESS = "success"  # 执行成功
    FAILED = "failed"  # 执行失败

    def __str__(self):
        return self.value


class BuiltinNodeType(str, Enum):
    INPUT = "input"             # 输入节点
    OUTPUT = "output"           # 输出节点
    FILTER = "filter"           # 过滤节点
    MAPPER = "mapper"           # 映射节点
    REDUCE = "reduce"           # 聚合节点
    SORT = "sort"               # 排序节点
    AGGREGATE = "aggregate"     # 聚合节点
    JOIN = "join"               # 连接节点
    SPLIT = "split"             # 分割节点
    DB_READ = "db_read"         # 数据库读取节点
    DB_WRITE = "db_write"       # 数据库写入节点

    def __str__(self):
        return self.value


__all__ = [
    "DataType",
    "NodeStatus",
    "ResultStatus",
    "BuiltinNodeType",
    "GraphStatus"
]
