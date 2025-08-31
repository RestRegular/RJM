import logging
from abc import ABC, abstractmethod
from typing import Dict, Type

from data_flow.node import Node
from data_flow.node_executor import NodeExecutor
from data_flow.execution_context import ExecutionContext
from utils.log_system import get_logger

__all__ = ['NodeExecutorFactory']

logger = get_logger(__name__)


class NodeExecutorFactory:
    """节点执行器工厂，负责根据节点类型创建相应的执行器实例"""

    _executor_module_map: Dict = None
    _executor_map: Dict = {}

    def __init__(self):
        if not self._executor_module_map:
            self._executor_module_map = self.scan_executors_modules()

    @classmethod
    def scan_executors_modules(cls) -> Dict:
        """扫描 executors 目录下的 py 文件并动态导入"""
        import importlib
        import os
        import sys

        current_dir = os.path.join(os.path.dirname(__file__), 'executors')

        result = {}
        if current_dir not in sys.path:
            sys.path.append(current_dir)

        for filename in os.listdir(current_dir):
            if filename.endswith(".py") and filename != "__init__.py":
                module_name = filename[:-3]
                try:
                    module = importlib.import_module(module_name)
                    members = {}
                    import_list = getattr(module, "__all__", None) or getattr(module, "export", [])
                    # 只导入__all__中指定的成员
                    for member_name in import_list:
                        if hasattr(module, member_name):
                            member = getattr(module, member_name)
                            members[member_name] = member
                            if issubclass(member, NodeExecutor):
                                cls.register_executor(member)
                        else:
                            print(f"警告: 模块 {module_name} 的 __all__ 中包含未定义成员 {member_name}")
                    result[module_name] = members
                except Exception as e:
                    print(f"Error importing {module_name}: {e}")
        return result

    @classmethod
    def register_executor(cls, executor_class: type):
        """注册新的节点执行器类型"""
        if not issubclass(executor_class, NodeExecutor):
            raise ValueError(f"执行器类必须继承自 NodeExecutor")
        cls._executor_map[str(executor_class.get_node_type())] = executor_class
        return executor_class

    @classmethod
    def create_executor(cls, node: Node, context: ExecutionContext, **kwargs) -> NodeExecutor:
        """
        创建节点执行器实例
        :param node: 节点对象
        :param context: 执行上下文
        :param kwargs: 传递给执行器的额外参数
        :return: 节点执行器实例
        """
        executor_class = cls._executor_map.get(str(node.type))
        if not executor_class:
            logger.setLevel(context.log_level)
            logger.error(f"未找到节点类型 {node.type} 的执行器", exc_info=context.log_level == logging.DEBUG)
            raise ValueError(f"未找到节点类型 {node.type} 的执行器")

        return executor_class(node, context, **kwargs)


def main():
    factory = NodeExecutorFactory()
    pass

if __name__ == '__main__':
    main()
