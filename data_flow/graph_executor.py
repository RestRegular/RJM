import inspect
from typing import Optional, List, Dict, Any

from pydantic import BaseModel

from data_flow import *
from data_flow.result import ExecuteResult
from data_flow.node import Node
from data_flow.graph import Graph
from data_flow.execution_context import ExecutionContext


class GraphExecutor:
    def __init__(self, graph: Graph, context: ExecutionContext):
        self.graph = graph
        self.context = context

    async def _get_upstream_data(self, node_id: str) -> Dict[str, Any]:
        """获取上游节点传递到当前节点的数据（按输入端口分组）"""
        upstream_edges = self.graph.get_upstream_edges(node_id)
        input_data = {}
        for edge in upstream_edges:
            source_node = self.graph.nodes.get(edge.source_node_id)
            if not source_node or source_node.status != NodeStatus.SUCCESS:
                continue  # 上游节点未执行或失败，跳过
            # 从上游节点的输出结果中获取对应端口的数据
            source_port_data = source_node.result.get_data(edge.source_port_id)
            # 进行条件判断
            if edge.condition and not edge.condition(source_port_data):
                continue
            if source_port_data is not None:
                input_data[edge.target_port_id] = source_port_data
        return input_data

    async def _execute_node(self, node: Node) -> Node:
        """执行单个节点，使用新的执行器架构"""
        node.status = NodeStatus.RUNNING
        try:
            # 获取上游数据
            input_data = await self._get_upstream_data(node.id)

            node.set_config("data.input", input_data)

            # 通过工厂创建合适的执行器
            if not node.executor:
                node.executor = NodeExecutorFactory.create_executor(
                    node=node, context=self.context)

            result = await self._run_executor(node.executor, node=node, context=self.context)

            node.result = result
            node.status = NodeStatus.SUCCESS if result.success else NodeStatus.FAILED
        except Exception as e:
            node.status = NodeStatus.FAILED
            node.error = str(e)
        return node

    @staticmethod
    async def _run_executor(executor: NodeExecutor, **kwargs) -> ExecuteResult:
        """
        运行执行器，支持同步和异步执行器
        如果执行器的execute方法是异步的，使用await；否则直接调用
        """
        if inspect.iscoroutinefunction(executor.execute):
            # 异步执行
            return await executor.execute(**kwargs)
        else:
            # 同步执行
            return executor.execute(**kwargs)

    # async def _execute_node(self, node: Node) -> Node:
    #     """执行单个节点"""
    #     node.status = NodeStatus.RUNNING
    #     try:
    #         # 获取上游数据
    #         input_data = await self._get_upstream_data(node.id)
    #         # 校验必填输入端口
    #         for port in node.inputs:
    #             if port.required and port.id not in input_data:
    #                 raise ValueError(f"节点 {node.id} 缺少必填输入端口 {port.id} 的数据")
    #         # 执行节点处理函数
    #         handler = self._node_handlers.get(node.type)
    #         if not handler:
    #             raise ValueError(f"未注册节点类型 {node.type} 的处理函数")
    #         # 执行并获取结果（结果需按输出端口ID分组）
    #         result = await handler(node, input_data, self.context)
    #         node.result = result
    #         node.status = NodeStatus.SUCCESS
    #     except Exception as e:
    #         node.status = NodeStatus.FAILED
    #         node.error = str(e)
    #     return node

    async def run(self, start_node_ids: List[str]) -> Graph:
        """执行图（从指定的起始节点开始）"""
        # 拓扑排序（确保节点按依赖顺序执行）
        sorted_node_ids = self._topological_sort()
        # 过滤出需要执行的节点（从起始节点可达的节点）
        executable_nodes = self._get_reachable_nodes(start_node_ids, sorted_node_ids)
        # 依次执行节点
        for node_id in executable_nodes:
            node = self.graph.nodes.get(node_id)
            if not node:
                continue
            await self._execute_node(node)
        return self.graph

    def _topological_sort(self) -> List[str]:
        """对图进行拓扑排序（确保依赖前置节点先执行）"""
        # 简化实现：基于入度的拓扑排序
        in_degree = {node_id: 0 for node_id in self.graph.nodes}
        for edge in self.graph.edges:
            in_degree[edge.target_node_id] += 1
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        sorted_ids = []
        while queue:
            node_id = queue.pop(0)
            sorted_ids.append(node_id)
            for edge in self.graph.get_downstream_edges(node_id):
                in_degree[edge.target_node_id] -= 1
                if in_degree[edge.target_node_id] == 0:
                    queue.append(edge.target_node_id)
        return sorted_ids

    def _get_reachable_nodes(self, start_ids: List[str], sorted_ids: List[str]) -> List[str]:
        """获取从起始节点可达的所有节点（按拓扑顺序）"""
        reachable = set(start_ids)
        for node_id in sorted_ids:
            if node_id in reachable:
                # 将下游节点加入可达集合
                for edge in self.graph.get_downstream_edges(node_id):
                    reachable.add(edge.target_node_id)
        # 按拓扑顺序返回
        return [node_id for node_id in sorted_ids if node_id in reachable]