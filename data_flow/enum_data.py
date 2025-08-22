from enum import Enum


# 数据类型枚举（定义节点输入/输出数据的格式）
class DataType(str, Enum):
    JSON = "json"  # 通用JSON结构
    TEXT = "text"  # 文本数据
    NUMBER = "number"  # 数值型
    BOOLEAN = "boolean"  # 布尔型
    BINARY = "binary"  # 二进制数据


class NodeStatus(str, Enum):
    PENDING = "pending"  # 待执行
    RUNNING = "running"  # 执行中
    SUCCESS = "success"  # 执行成功
    FAILED = "failed"  # 执行失败


class ResultStatus(str, Enum):
    SUCCESS = "success"  # 执行成功
    FAILED = "failed"  # 执行失败


class BuiltinNodeType(str, Enum):
    INPUT = "input"             # 输入节点
    OUTPUT = "output"           # 输出节点
    FILTER = "filter"           # 过滤节点
    MAP = "map"                 # 映射节点
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
    "BuiltinNodeType"
]
