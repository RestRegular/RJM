import json
import inspect
from typing import Optional, List, Dict, Any, Literal, Union
import logging

from pydantic import BaseModel

from data_flow import *
from data_flow.node import Node
from data_flow.result import Result
from utils.log_system import get_logger
from data_flow.result import ExecuteResult
from data_flow.graph import Graph, GraphError
from data_flow.execution_context import ExecutionContext

logger = get_logger(__name__)


class GraphExecutor:
    def __init__(self, graph: Graph, context: ExecutionContext):
        self.graph = graph
        self.context = context
        logger.setLevel(self.context.log_level or logging.INFO)

    async def _get_upstream_data(self, node_id: str) -> Dict[str, Any]:
        """获取上游节点传递到当前节点的数据（按输入端口分组）"""
        logger.debug(f"开始获取节点 {node_id} 的上游数据")

        target_node = self.graph.get_node_by_id(node_id)

        upstream_edges = self.graph.get_upstream_edges(node_id)
        logger.debug(f"节点 {node_id} 的上游边数量: {len(upstream_edges)}")

        input_data = {}
        for edge in upstream_edges:
            logger.debug(f"处理边: {str(edge)}")

            source_node = self.graph.nodes.get(edge.source_node_id)
            if not source_node:
                logger.warning(f"上游节点 {edge.source_node_id} 不存在，跳过")
                continue

            if source_node.status != NodeStatus.SUCCESS:
                logger.debug(f"上游节点 {edge.source_node_id} 状态为 {source_node.status}，跳过")
                continue  # 上游节点未执行或失败，跳过

            # 从上游节点的输出结果中获取对应端口的数据
            source_port_data = source_node.result.get_data(edge.source_port_id)
            logger.debug(f"从端口 {edge.source_port_id} 获取到数据: {type(source_port_data)}")

            # 进行条件判断
            if edge.condition:
                condition_result = edge.condition(
                    source_port_data,
                    source_node=source_node,
                    target_node=target_node,
                    source_port=edge.source_port_id,
                    target_port=edge.target_port_id,
                    edge=edge
                )
                logger.debug(f"边条件检查结果: {condition_result}")
                if not condition_result:
                    logger.debug(f"边条件不满足，跳过该边")
                    continue

            if source_port_data is not None:
                input_data[edge.target_port_id] = source_port_data
                logger.debug(f"将数据添加到输入端口 {edge.target_port_id}")

        logger.debug(f"节点 {node_id} 最终输入数据: {list(input_data.keys())}")
        return input_data

    async def _execute_node(self, node: Node) -> Node:
        """执行单个节点"""
        logger.info(f"开始执行节点: {str(node)}")
        node.status = NodeStatus.RUNNING

        try:
            # 通过工厂创建合适的执行器
            if not node.executor:
                logger.debug(f"为节点 {node.id} 创建执行器")
                node.executor = NodeExecutorFactory.create_executor(
                    node=node, context=self.context)
                logger.debug(f"执行器创建完成: {type(node.executor).__name__}")

            # 获取上游数据
            logger.debug(f"获取节点 {node.id} 的上游数据")
            input_data = await self._get_upstream_data(node.id)
            logger.debug(f"节点 {node.id} 接收到输入数据: {len(input_data)} 个端口")

            node.set_config("data.input", input_data)

            # 执行节点逻辑
            logger.info(f"执行节点 {node.id} 的业务逻辑")
            result = await self._run_executor(node.executor, node=node, context=self.context)

            node.result = result
            node.status = NodeStatus.SUCCESS if result.success else NodeStatus.FAILED

            if result.success:
                logger.info(f"节点 {node.id} 执行成功")
            else:
                logger.error(f"节点 {node.id} 执行失败: {result.error}")
                node.error = result.error

        except Exception as e:
            node.status = NodeStatus.FAILED
            node.error = str(e)
            logger.error(f"节点 {node.id} 执行过程中发生异常: {str(e)}",
                         exc_info=self.context.log_level == logging.DEBUG)

        logger.info(f"节点 {node.id} 执行完成，状态: {node.status}")
        return node

    @staticmethod
    async def _run_executor(executor: NodeExecutor, **kwargs) -> ExecuteResult:
        """
        运行执行器，支持同步和异步执行器
        如果执行器的execute方法是异步的，使用await；否则直接调用
        """
        node = kwargs.get('node')
        logger.debug(f"开始运行执行器，节点: {node.id if node else '未知'}")

        try:
            if inspect.iscoroutinefunction(executor.execute):
                # 异步执行
                logger.debug("检测到异步执行器")
                result = await executor.execute(**kwargs)
            else:
                # 同步执行
                logger.debug("检测到同步执行器")
                result = executor.execute(**kwargs)

            logger.debug(f"执行器运行完成，结果状态: {'成功' if result.success else '失败'}")
            return result

        except Exception as e:
            logger.error(f"执行器运行异常: {str(e)}",
                         exc_info=kwargs.get('context', {}).log_level == logging.DEBUG)
            raise

    async def run(self, start_node_ids: Optional[List[str]] = None) -> Graph:
        """执行图"""
        logger.info(f"开始执行流程图: {str(self.graph)}")

        start_node_ids = start_node_ids or self.graph.starts
        logger.debug(f"起始节点: {start_node_ids}")

        self.graph.status = GraphStatus.RUNNING

        # 拓扑排序（确保节点按依赖顺序执行）
        logger.debug("开始拓扑排序")
        sorted_node_ids = self._topological_sort()
        logger.debug(f"拓扑排序结果: {sorted_node_ids}")

        # 过滤出需要执行的节点（从起始节点可达的节点）
        logger.debug("开始筛选可达节点")
        executable_nodes = self._get_reachable_nodes(start_node_ids, sorted_node_ids)
        logger.info(f"需要执行的节点数量: {len(executable_nodes)}，节点列表: {executable_nodes}")

        # 依次执行节点
        for index, node_id in enumerate(executable_nodes, 1):
            node = self.graph.get_node_by_id(node_id)

            logger.info(f"执行进度: {index}/{len(executable_nodes)} - 节点: {node.base_info()}")

            if not node:
                logger.warning(f"节点 {node_id} 不存在，跳过")
                continue

            await self._execute_node(node)

            if node.status == NodeStatus.FAILED:
                error_msg = GraphError(node_id=node_id, error=node.error)
                self.graph.errors.append(error_msg)
                logger.error(f"节点 {node.base_info()} 执行失败，错误信息: {node.error}")

        # 更新图状态
        if len(self.graph.errors) == 0:
            self.graph.status = GraphStatus.COMPLETED
            logger.info(f"流程图执行完成，所有（共 {len(executable_nodes)} 个）节点执行成功")
        else:
            self.graph.status = GraphStatus.FAILED
            logger.error(f"流程图执行失败，共 {len(self.graph.errors)} 个节点失败")

        return self.graph

    def _topological_sort(self) -> List[str]:
        """对图进行拓扑排序（确保依赖前置节点先执行）"""
        logger.debug("开始拓扑排序计算")

        in_degree = {node_id: 0 for node_id in self.graph.nodes}
        for edge in self.graph.edges:
            in_degree[edge.target_node_id] += 1

        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        sorted_ids = []

        logger.debug(f"初始入度为0的节点: {queue}")

        while queue:
            node_id = queue.pop(0)
            sorted_ids.append(node_id)
            logger.debug(f"处理节点 {node_id}，当前排序: {sorted_ids}")

            for edge in self.graph.get_downstream_edges(node_id):
                in_degree[edge.target_node_id] -= 1
                if in_degree[edge.target_node_id] == 0:
                    queue.append(edge.target_node_id)
                    logger.debug(f"节点 {edge.target_node_id} 入度减为0，加入队列")

        logger.debug(f"拓扑排序完成，结果: {sorted_ids}")
        return sorted_ids

    def _get_reachable_nodes(self, start_ids: List[str], sorted_ids: List[str]) -> List[str]:
        """获取从起始节点可达的所有节点（按拓扑顺序）"""
        logger.debug(f"开始计算可达节点，起始节点: {start_ids}")

        reachable = set(start_ids)
        logger.debug(f"初始可达集合: {reachable}")

        for node_id in sorted_ids:
            if node_id in reachable:
                logger.debug(f"节点 {node_id} 可达，处理其下游节点")
                # 将下游节点加入可达集合
                for edge in self.graph.get_downstream_edges(node_id):
                    reachable.add(edge.target_node_id)
                    logger.debug(f"添加下游节点 {edge.target_node_id} 到可达集合")

        # 按拓扑顺序返回
        result = [node_id for node_id in sorted_ids if node_id in reachable]
        logger.debug(f"最终可达节点: {result}")
        return result

    def get_node_results(self, mode: Union[Literal['json', 'python'], str] = 'python') -> Dict[str, Any]:
        """获取节点执行结果（按节点ID分组）"""
        logger.info(f"获取节点执行结果，模式: {mode}")

        result = {
            node_id: {
                "status": node.status,
                "result": node.result.model_dump(mode=mode) if node.result else None,
                "error": node.error
            }
            for node_id, node in self.graph.nodes.items()
        }

        logger.debug(f"结果数据构建完成，包含 {len(result)} 个节点")
        return result if mode == 'python' else json.dumps(result)

    def get_node_result(self, node_id: str) -> Result:
        return self.graph.get_node_result(node_id)
