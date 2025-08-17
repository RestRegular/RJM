from typing import Tuple, Dict
from pydantic import BaseModel
from app.service.data_flow.domain.edge import Edge
from app.service.data_flow.domain.edges import Edges


class PortToPortEdges(BaseModel):
    """表示端口之间连接的边集合，按源端口进行组织"""

    # 字典结构，key为源端口名称，value为该端口对应的所有边集合
    edges: Dict[str, Edges] = {}

    def __iter__(self) -> Tuple[str, Edge, str]:
        """迭代遍历集合中的所有边

        生成：
            返回包含以下内容的元组：
            - start_port: 源端口名称
            - edge: 边对象
            - end_port: 目标端口名称
        """
        for start, edges in self.edges.items():  # type: str, Edges
            for edge in edges:  # type: Edge
                yield edge.source.param, edge, edge.target.param

    def get_enabled_edges(self) -> Tuple[str, Edge, str]:
        """生成器，只返回已启用的边

        生成：
            与__iter__相同的元组格式，但仅包含启用的边
        """
        for start_port, edge, end_port in self:
            if edge.enabled is True:
                yield start_port, edge, end_port

    def add(self, edge: Edge):
        """向集合中添加一条边

        参数：
            edge: 要添加的边对象
        """
        if edge.source.param not in self.edges:
            self.edges[edge.source.param] = set()
        self.edges[edge.source.param].add(edge)

    def dict(self, **kwargs):
        """将对象转换为字典，用于序列化（将集合转为列表）

        返回：
            对象的字典表示形式
        """
        for k, e in self.edges.items():
            self.edges[k] = list(self.edges[k])
        return super().model_dump(**kwargs)

    def get_start_ports(self):
        """获取所有有出边的源端口

        返回：
            边字典的所有key（源端口名称列表）
        """
        return self.edges.keys()

    def set_edges_on_port(self, port, enable: bool = True):
        """启用或禁用从指定端口出发的所有边

        参数：
            port: 要操作的源端口名称
            enable: True表示启用，False表示禁用
        """
        for start, edges in self.edges.items():
            for edge in edges:
                if edge.source.param == port:
                    edge.enabled = enable

    def set_edges(self, enable: bool = True):
        """启用或禁用集合中的所有边

        参数：
            enable: True表示启用，False表示禁用
        """
        for start, edges in self.edges.items():
            for edge in edges:
                edge.enabled = enable