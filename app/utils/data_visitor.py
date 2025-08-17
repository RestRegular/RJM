"""
数据访问工具模块

该模块提供了灵活的数据访问和操作功能，支持多种数据类型：
    - 基本字典和嵌套字典
    - Pydantic模型
    - Dataclass
    - Namedtuple
    - 普通类实例
    - 列表/元组/集合
    - 基本数据类型
    - 自定义类型(通过注册转换器)

主要类:
    1. Notation - 处理点分路径表示法
    2. DataVisitor - 核心数据访问器
    3. BaseData - 抽象基类(自定义数据类可继承)
"""

import json
import re
from typing import Union, Any, List, overload
from enum import Enum
from abc import ABC, abstractmethod
from dataclasses import is_dataclass, asdict
from collections import namedtuple

from dotty_dict import dotty, Dotty
from pydantic import BaseModel

# 匹配点分路径表示法的正则表达式
# 支持格式: a.b.c 或 a.b.c... (通配符)
_notation_match_regex = re.compile(
    r"^(?:[a-zA-Z0-9_]+\.)*(?:\.\.$|[a-zA-Z0-9_]+$)")


class NotationType(Enum):
    """路径表示法类型枚举"""
    INVALID = "invalid"  # 无效路径
    SPECIFIC = "specific"  # 具体路径
    WILDCARD = "wildcard"  # 通配路径(以...结尾)


class Notation:
    """路径表示法处理类

    属性:
    - content: 原始路径字符串
    - type: 路径类型(NotationType)

    方法:
    - validate: 验证路径格式是否合法
    - _get_type: 确定路径类型
    """

    def __init__(self, notation: str, noexcept: bool = False):
        """初始化路径表示法

        :param notation: 路径字符串
        :param noexcept: 是否不抛出异常(返回INVALID类型而不是报错)
        """
        self.content = notation
        if self.validate(notation):
            self.type = self._get_type()
            return
        if not noexcept:
            raise ValueError(f"Invalid notation: '{notation}'")
        self.type = NotationType.INVALID

    @staticmethod
    def validate(notation: str) -> bool:
        """验证路径格式是否合法"""
        return _notation_match_regex.match(notation) is not None

    def _get_type(self) -> NotationType:
        """确定路径类型"""
        if self.content.endswith("..."):
            return NotationType.WILDCARD
        return NotationType.SPECIFIC

    def __repr__(self):
        return self.content


class InvalidNotation:
    """无效路径表示法容器类"""

    def __init__(self, notation: Union[str, Notation]):
        self.notation = notation

    def __repr__(self):
        return f"[InvalidNotation: '{self.notation}']"


class BaseData(ABC):
    """抽象基类，自定义数据类可继承此类实现转换方法"""

    @abstractmethod
    def convert_to_dict(self) -> dict:
        """将数据转换为字典形式"""
        pass


class DataVisitor:
    """核心数据访问器类

    功能:
    - 支持多种数据类型的自动转换
    - 支持点分路径表示法访问嵌套数据
    - 支持数据修改
    - 支持自定义类型转换器

    属性:
    - storage: 内部数据存储(Dotty字典)

    类方法:
    - register_converter: 注册自定义类型转换器
    - unregister_converter: 移除自定义类型转换器
    - get/set: 静态方法快捷访问
    """

    _converters = {}  # 类型转换器注册表

    def __init__(self, data: Union[dict, Dotty, BaseModel, str, Any]):
        """初始化数据访问器

        :param data: 支持多种数据类型，自动转换为Dotty字典
        """
        self.storage: Dotty = DataVisitor._convert(data)

    @staticmethod
    def new(**data: Any):
        """创建一个新的数据访问器实例

        :param data: 支持多种数据类型，自动转换为Dotty字典
        :return: 新的数据访问器实例
        """
        return DataVisitor(data)

    @staticmethod
    def _convert(data: Union[dict, Dotty, BaseModel, BaseData, Any]) -> Dotty:
        """将各种数据类型转换为Dotty字典

        转换优先级:
        1. 自定义转换器
        2. 内置支持类型(dict/Dotty/Pydantic模型等)
        3. 其他类型自动包装

        :param data: 输入数据
        :return: Dotty字典
        """
        if data is None:
            return dotty({})

        # 1. 优先使用自定义转换器
        for type_cls, converter in DataVisitor._converters.items():
            if isinstance(data, type_cls):
                return DataVisitor._convert(converter(data))

        # 2. 处理内置支持类型
        if isinstance(data, (dict, Dotty)):
            return dotty(data) if isinstance(data, dict) else data

        if isinstance(data, BaseModel):
            return dotty(data.model_dump(mode="json"))

        if isinstance(data, BaseData):
            return dotty(data.convert_to_dict())

        if isinstance(data, str):
            try:
                return dotty(json.loads(data))
            except Exception:
                # 不是JSON字符串，包装为单个值
                return dotty({"_value": data})

        # 3. 支持dataclass
        if is_dataclass(data) and not isinstance(data, type):
            return dotty(asdict(data))

        # 4. 支持namedtuple
        if isinstance(data, tuple) and hasattr(data, "_fields"):
            return dotty(dict(zip(data._fields, data)))

        # 5. 支持普通类实例(通过__dict__)
        if hasattr(data, "__dict__"):
            return dotty(data.__dict__)

        # 6. 支持列表和其他可迭代对象
        if isinstance(data, (list, tuple, set)):
            return dotty({"_items": list(data)})

        # 7. 基础类型直接包装
        return dotty({"_value": data})

    @classmethod
    def register_converter(cls, type_cls, converter):
        """注册自定义类型转换器

        :param type_cls: 要转换的类型
        :param converter: 转换函数
        """
        cls._converters[type_cls] = converter

    @classmethod
    def unregister_converter(cls, type_cls):
        """移除自定义类型转换器"""
        if type_cls in cls._converters:
            del cls._converters[type_cls]

    @staticmethod
    def _get_notation(notation: Union[str, Notation], noexcept: bool = False) -> Notation:
        """获取Notation对象"""
        return Notation(notation, noexcept) if isinstance(notation, str) else notation

    @classmethod
    def convert_to_dict(cls, object_: Union[dict, Dotty]) -> dict:
        """将对象转换为普通字典"""
        if isinstance(object_, dict):
            return object_
        elif isinstance(object_, Dotty):
            return object_.to_dict()
        else:
            result = cls._convert(object_)
            try:
                return result.to_dict()
            except TypeError:
                return result

    @staticmethod
    def track(notation: Notation) -> List[str]:
        """将路径表示法拆分为路径组件列表"""
        if notation.type == NotationType.SPECIFIC:
            return notation.content.split('.')
        else:
            return notation.content[:-3].split('.')

    def get_all(self, notation: Union[str, Notation]) -> dict:
        """获取通配路径下的所有数据"""
        notation = self._get_notation(notation)
        if notation.type != NotationType.WILDCARD:
            return None
        tracks = self.track(notation)
        data = self.storage
        for track in tracks:
            if track not in data:
                return None
            data = data.get(track)
        try:
            return self.convert_to_dict(data)
        except ValueError:
            return None

    def get_data(self, notation: Union[str, Notation]):
        """获取路径对应的数据"""
        notation = self._get_notation(notation, True)
        if notation.type == NotationType.INVALID:
            return InvalidNotation(notation)
        if notation.type == NotationType.SPECIFIC:
            tracks = self.track(notation)
            data = self.storage
            for track in tracks:
                if track.isnumeric():
                    index = int(track)
                    if "_items" in data:
                        data = data.get("_items")
                    elif "_value" in data:
                        data = data.get("_value")
                    if not isinstance(data, list):
                        return None
                    if index >= len(data):
                        return None
                    data = self.convert_to_dict(data[index])
                elif track not in data:
                    return None
                else:
                    data = self.convert_to_dict(data.get(track))
            return data
        return self.get_all(notation.content)

    def set_data(self, notation: str, data):
        """设置路径对应的数据"""
        notation = self._get_notation(notation)
        if notation.type != NotationType.INVALID:
            tracks = self.track(notation)
            data_ = self.storage
            for track in tracks[:-1]:
                if track not in data_:
                    data_[track] = {}
                data_ = data_.get(track)
            value = self._convert(data)
            key = tracks[-1]
            if is_dataclass(data_):
                if hasattr(data_, key):
                    setattr(data_, key, data)
                else:
                    raise AttributeError(f"Error notation: {notation}\n"
                                         f"Attribute '{key}' does not exist in '{data_.__class__}'.")
            else:
                data_[key] = value
            return

    def __delitem__(self, key):
        """删除路径对应的数据"""
        notation = self._get_notation(key)
        tracks = self.track(notation)
        data_ = self.storage
        for track in tracks[:-1]:
            if track not in data_:
                raise KeyError(f"Error notation: {notation}\n"
                               f"Key '{track}' does not exist.")
            data_ = data_.get(track)
        if tracks[-1] in data_:
            del data_[tracks[-1]]
            return
        raise KeyError(f"Error notation: {notation}\n"
                        f"Key '{tracks[-1]}' does not exist.")

    def __setitem__(self, key, value):
        """设置路径对应的数据(使用[]语法)"""
        notation = self._get_notation(key)
        self.set_data(notation, value)

    def __getitem__(self, item):
        """获取路径对应的数据(使用[]语法)"""
        notation = self._get_notation(item)
        value = self.get_data(notation)
        if not value: return None
        if "_items" in value:
            return value["_items"]
        if "_value" in value:
            return value["_value"]
        return value

    def __contains__(self, item):
        """检查路径是否存在(使用in语法)"""
        notation = self._get_notation(item)
        return self.get_data(notation) is not None

    @staticmethod
    def get(notation: str, payload, default=None):
        """静态方法快捷获取数据"""
        notation = DataVisitor._get_notation(notation)
        result = DataVisitor(payload)[notation]
        return default if result is None else result

    @staticmethod
    def set(notation: str, payload, value):
        """静态方法快捷设置数据"""
        notation = DataVisitor._get_notation(notation)
        DataVisitor(payload)[notation] = value

    @classmethod
    def cast(cls, value):
        """类型转换辅助方法(字符串转bool/int/float等)"""
        if isinstance(value, str):
            if value.lower() == "true":
                return True
            elif value.lower() == "false":
                return False
            elif value.lower() in {"none", "null"}:
                return None
            elif value.replace('.', '').isnumeric() and value.count(".") == 1:
                try:
                    return float(value)
                except ValueError:
                    return value
            elif value.isnumeric():
                try:
                    return int(value)
                except ValueError:
                    return value
        return value


def test_extended_features():
    """测试扩展功能"""
    from dataclasses import dataclass
    from datetime import datetime
    from pydantic import BaseModel as PydanticModel

    print("\n=== 测试扩展功能 ===")

    # 1. 测试dataclass
    @dataclass
    class User:
        id: int
        name: str
        hobby: List[str]
        address: Any = None

    # 2. 测试namedtuple
    Address = namedtuple('Address', ['city', 'street'])

    # 3. 测试普通类
    class Product:
        def __init__(self, id_: int, name: str, price: float):
            self.id = id_
            self.name = name
            self.price = price

        def __repr__(self):
            return f"Product(id={self.id}, name={self.name}, price={self.price})"

    # 4. 测试Pydantic模型
    class Order(PydanticModel):
        order_id: str
        amount: float
        created_at: datetime

    # 5. 注册自定义转换器(处理datetime)
    DataVisitor.register_converter(datetime, lambda dt: dt.isoformat())

    # 准备测试数据
    test_data = {
        "user": User(1, "Alice", ["reading", "swimming"],
                    Address("Beijing", "Main St")),
        "products": [
            Product(100, "Laptop", 9999.99),
            Product(101, "Phone", 5999.99)
        ],
        "order": Order(
            order_id="ORD123",
            amount=15999.98,
            created_at=datetime(2023, 1, 1, 12, 0, 0)
        ),
        "simple_value": "Just a string",
        "numeric_value": "123.45"
    }

    # 创建访问器
    visitor = DataVisitor(test_data)

    # 测试各种访问方式
    print("\n--- 基本访问测试 ---")
    print("用户姓名:", visitor["user.name"])  # Alice
    print("用户爱好:", visitor["user.hobby"])  # ['reading', 'swimming']
    print("用户城市:", visitor["user.address.city"])  # Beijing
    print("第一个产品:", visitor["products.0.name"])  # Laptop
    print("订单金额:", visitor["order.amount"])  # 15999.98
    print("订单时间:", visitor["order.created_at"])  # 2023-01-01T12:00:00
    print("简单值:", visitor["simple_value"])  # Just a string
    print("数字字符串:", visitor["numeric_value"])  # 123.45

    # 测试修改数据
    print("\n--- 修改测试 ---")
    print("修改前的爱好:", visitor["user.hobby"])
    visitor["user.hobby"] = ["programming", "music"]
    print("修改后的爱好:", visitor["user.hobby"])

    # 测试静态方法
    print("\n--- 静态方法测试 ---")
    print("静态获取:", DataVisitor.get("user.name", test_data))  # Alice
    DataVisitor.set("user.name", test_data, "Bob")
    print("静态修改后:", DataVisitor.get("user.name", test_data))  # Bob

    # 测试类型转换
    print("\n--- 类型转换测试 ---")
    print("字符串转bool:", DataVisitor.cast("true"))  # True
    print("字符串转int:", DataVisitor.cast("123"))  # 123
    print("字符串转float:", DataVisitor.cast("123.45"))  # 123.45
    print("字符串转None:", DataVisitor.cast("null"))  # None

    # 测试包含检查
    print("\n--- 包含检查测试 ---")
    print("检查存在路径:", "user.name" in visitor)  # True
    print("检查不存在路径:", "user.age" in visitor)  # False

    # 测试删除
    print("\n--- 删除测试 ---")
    print("删除前:", "simple_value" in visitor)
    del visitor["simple_value"]
    print("删除后:", "simple_value" in visitor)


def test_basic_features():
    """测试基本功能"""
    print("\n=== 测试基本功能 ===")

    # 1. 测试字典访问
    print("\n--- 字典测试 ---")
    data = {
        "user": {
            "name": "Alice",
            "age": 30,
            "address": {
                "city": "Beijing",
                "street": "Main St"
            }
        },
        "items": ["apple", "banana", "orange"]
    }

    visitor = DataVisitor(data)
    print("用户名:", visitor["user.name"])  # Alice
    print("年龄:", visitor["user.age"])  # 30
    print("城市:", visitor["user.address.city"])  # Beijing
    print("第一个物品:", visitor["items"][0])  # apple

    # 测试修改
    visitor["user.age"] = 31
    print("修改后的年龄:", visitor["user.age"])  # 31

    # 2. 测试JSON字符串
    print("\n--- JSON测试 ---")
    json_str = '{"system": {"version": "1.0.0", "settings": {"debug": true}}}'
    json_visitor = DataVisitor(json_str)
    print("版本:", json_visitor["system.version"])  # 1.0.0
    print("调试模式:", json_visitor["system.settings.debug"])  # True

    # 3. 测试基本类型
    print("\n--- 基本类型测试 ---")
    simple_visitor = DataVisitor("Hello")
    print("简单值:", simple_visitor["_value"])  # Hello


if __name__ == '__main__':
    test_basic_features()
    test_extended_features()
