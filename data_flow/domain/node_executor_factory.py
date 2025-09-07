import os
import sys
import logging
import importlib
from typing import Dict, Optional, Union, List, Type, Any

from data_flow.domain.execution_context import ExecutionContext
from data_flow.domain.node import Node
from data_flow.domain.node_executor import NodeExecutor
from data_flow.utils.log_system import get_logger

__all__ = [
    'NodeExecutorFactory'
]

logger = get_logger(__name__)

EXECUTOR_DIR_PATH = os.path.join(os.path.dirname(__file__), "executors")

_has_initialized = False
_executor_module_map: Optional[Dict[str, Dict[str, Any]]] = None
_executor_map: Dict[str, Type[NodeExecutor]] = {}

class NodeExecutorFactory:
    """节点执行器工厂，负责根据节点类型创建相应的执行器实例"""

    @classmethod
    def initialize(cls):
        global _executor_module_map, _has_initialized
        if not _has_initialized:
            _has_initialized = True
            _executor_module_map = cls.scan_executors_modules()

    @classmethod
    def scan_executors_modules(cls) -> Dict[str, Dict[str, Any]]:
        """扫描 executors 目录下的 py 文件并动态导入"""
        current_dir = os.path.join(os.path.dirname(__file__), 'executors')

        result = {}
        if current_dir not in sys.path:
            sys.path.append(current_dir)

        for filename in os.listdir(current_dir):
            if filename.endswith(".py") and filename != "__init__.py":
                module_name = filename[:-3]
                node_type = None
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
                                node_type = member.get_node_type()
                        else:
                            logger.warning(f"模块 {module_name} 的 __all__ 中包含未定义成员 {member_name}")
                    result[node_type] = members
                except Exception as e:
                    logger.error(f"Error importing {module_name}: {e}", exc_info=e)
        return result

    @classmethod
    def register_executor(cls, executor_class: type):
        """注册新的节点执行器类型"""
        global _executor_map
        if not issubclass(executor_class, NodeExecutor):
            raise ValueError(f"执行器类必须继承自 NodeExecutor")
        _executor_map[str(executor_class.get_node_type())] = executor_class
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
        global _executor_map
        executor_class = _executor_map.get(str(node.type))
        if not executor_class:
            logger.setLevel(context.log_level)
            logger.error(f"未找到节点类型 {node.type} 的执行器", exc_info=context.log_level == logging.DEBUG)
            raise ValueError(f"未找到节点类型 {node.type} 的执行器")

        return executor_class(node, context, **kwargs)

    @classmethod
    def get_executor(cls, node_type: Optional[Union[str, List[str]]] = None)\
            -> Union[Dict[str, Type[NodeExecutor]], Dict[str, Type[NodeExecutor]]]:
        global _executor_map
        if isinstance(node_type, str):
            return _executor_map.get(node_type)
        elif isinstance(node_type, list):
            return {node_type: _executor_map.get(node_type) for node_type in node_type}
        else:
            return _executor_map

    @staticmethod
    def get_module_member(node_type: str, member_name: str) -> Any:
        """从缓存中获取模块成员"""
        global _executor_module_map
        module = _executor_module_map.get(node_type, None)
        if not module:
            raise ValueError(f"未找到模块 {node_type}")
        if member_name not in module:
            raise ValueError(f"未找到模块 {node_type} 的成员 '{member_name}'")
        return module.get(member_name)

    @classmethod
    def dynamic_import_executor_from_cache(cls, filename: str, filepath: str) -> Type[NodeExecutor]:
        global _executor_module_map
        module_name = filename[:-3]
        module = importlib.import_module(f"data_flow.api.endpoints.service.cache.{module_name}")
        import_list = getattr(module, "__all__", None) or getattr(module, "export", [])
        members = {}
        executor_cls = None
        for member_name in import_list:
            if hasattr(module, member_name):
                member = getattr(module, member_name)
                members[member_name] = member
                if issubclass(member, NodeExecutor) and not executor_cls:
                    cls.register_executor(member)
                    executor_cls = member
            else:
                logger.warning(f"模块 {module_name} 的 __all__ 中包含未定义成员 {member_name}")
        _executor_module_map[executor_cls.get_node_type()] = members
        with open(os.path.join(EXECUTOR_DIR_PATH, filename), "w", encoding="utf-8") as tf, \
                open(filepath, "r", encoding="utf-8") as sf:
            tf.write(sf.read())
        return executor_cls


NodeExecutorFactory.initialize()
