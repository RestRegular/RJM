import re
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


# 图元素的数据类定义
@dataclass
class Port:
    id: str
    name: str = ""
    data_type: str = "LIST"


@dataclass
class Node:
    id: str
    name: str = ""
    type: str = "default"
    description: str = ""
    is_start: bool = False
    is_end: bool = False
    inputs: List[Port] = field(default_factory=list)
    outputs: List[Port] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Edge:
    from_node: str
    from_port: str
    to_node: str
    to_port: str
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Graph:
    name: str
    description: str = ""
    nodes: List[Node] = field(default_factory=list)
    edges: List[Edge] = field(default_factory=list)


class SymbolGraphParser:
    """解析基于符号表示的图定义"""

    def __init__(self):
        # 正则表达式模式
        self.node_pattern = re.compile(r'\[(?P<id>\w+)(\{(?P<props>.*?)})?]')
        self.port_pattern = re.compile(r'\((?P<id>\w+)(\{(?P<props>.*?)})?\)')
        self.edge_pattern = re.compile(
            r'\[(?P<from_node>\w+)](\.(?P<from_port>\w+))?\s*'
            r'-+(\{(?P<props>.*?)})?->\s*'
            r'\[(?P<to_node>\w+)](\.(?P<to_port>\w+))?'
        )
        self.prop_pattern = re.compile(r'(\w+)\s*=\s*(".*?"|\w+)')

    def parse_properties(self, prop_str: str) -> Dict[str, Any]:
        """解析属性字符串为字典"""
        props = {}
        if not prop_str:
            return props

        matches = self.prop_pattern.findall(prop_str)
        for key, value in matches:
            # 去除引号
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            # 转换布尔值
            elif value.lower() == 'true':
                value = True
            elif value.lower() == 'false':
                value = False
            props[key] = value
        return props

    def parse_nodes(self, node_strs: List[str]) -> List[Node]:
        """解析节点定义"""
        nodes = []
        for line in node_strs:
            line = line.strip()
            if not line:
                continue

            match = self.node_pattern.match(line)
            if match:
                node_id = match.group('id')
                props_str = match.group('props') or ''
                props = self.parse_properties(props_str)

                # 解析端口定义 (放在节点属性中)
                inputs = []
                outputs = []

                if 'inputs' in props:
                    for port_id in props['inputs'].split(','):
                        port_id = port_id.strip()
                        inputs.append(Port(id=port_id))
                    del props['inputs']

                if 'outputs' in props:
                    for port_id in props['outputs'].split(','):
                        port_id = port_id.strip()
                        outputs.append(Port(id=port_id))
                    del props['outputs']

                node = Node(
                    id=node_id,
                    name=props.get('name', node_id),
                    type=props.get('type', 'default'),
                    description=props.get('desc', ''),
                    is_start=props.get('start', False),
                    is_end=props.get('end', False),
                    inputs=inputs,
                    outputs=outputs,
                    config={k: v for k, v in props.items() if k not in ['name', 'type', 'desc', 'start', 'end']}
                )
                nodes.append(node)
        return nodes

    def parse_edges(self, edge_strs: List[str]) -> List[Edge]:
        """解析边定义"""
        edges = []
        for line in edge_strs:
            line = line.strip()
            if not line:
                continue

            match = self.edge_pattern.match(line)
            if match:
                from_node = match.group('from_node')
                from_port = match.group('from_port') or 'default'
                to_node = match.group('to_node')
                to_port = match.group('to_port') or 'default'
                props_str = match.group('props') or ''
                props = self.parse_properties(props_str)

                edge = Edge(
                    from_node=from_node,
                    from_port=from_port,
                    to_node=to_node,
                    to_port=to_port,
                    properties=props
                )
                edges.append(edge)
        return edges

    def parse(self, graph_def: str, name: str, description: str = "") -> Graph:
        """解析完整的图定义"""
        lines = [line.strip() for line in graph_def.split('\n')]

        # 简单分离节点和边定义（实际应用中可使用更复杂的逻辑）
        node_lines = []
        edge_lines = []
        for line in lines:
            if not line:
                continue
            if '->' in line:
                edge_lines.append(line)
            else:
                node_lines.append(line)

        nodes = self.parse_nodes(node_lines)
        edges = self.parse_edges(edge_lines)

        return Graph(
            name=name,
            description=description,
            nodes=nodes,
            edges=edges
        )


# 使用示例
if __name__ == "__main__":
    # 基于符号的图定义
    graph_def = """
    [input{name=图数据输入, type=input, start=true, outputs=graph_raw_data, desc=接收待导出的原始图数据}]
    [extract_base{name=图基本数据提取, type=mapper, inputs=graph_raw_data, outputs=graph_base_info}]
    [extract_nodes{name=图节点数据提取, type=mapper, inputs=graph_raw_data, outputs=graph_node_data}]
    [extract_node_info{name=图节点信息提取, type=mapper, inputs=graph_node_data, outputs=graph_node_info}]
    [extract_edges{name=图边数据提取, type=mapper, inputs=graph_raw_data, outputs=graph_edge_info}]
    [extract_config{name=提取节点配置信息, type=mapper, inputs=graph_node_data, outputs=graph_node_config_info}]
    [db_write{name=图数据入库, type=db_write, end=true, 
        inputs=graph_base_info,graph_node_info,graph_node_config_info,graph_edge_info,
        dbname=data_flow, user=root, password=197346285,
        mapping=graph_base_info:graph,graph_node_info:graph_node,
                graph_node_config_info:graph_node_config,graph_edge_info:graph_edge
    }]

    <[input].graph_raw_data -> [extract_base].graph_raw_data>
    [input].graph_raw_data -> [extract_nodes].graph_raw_data
    [input].graph_raw_data -> [extract_edges].graph_raw_data
    [extract_nodes].graph_node_data -> [extract_node_info].graph_node_data
    [extract_nodes].graph_node_data -> [extract_config].graph_node_data
    [extract_base].graph_base_info -> [db_write].graph_base_info
    [extract_node_info].graph_node_info -> [db_write].graph_node_info
    [extract_config].graph_node_config_info -> [db_write].graph_node_config_info
    [extract_edges].graph_edge_info -> [db_write].graph_edge_info
    """

    # 解析图定义
    parser = SymbolGraphParser()
    graph = parser.parse(graph_def, "图数据导出流程", "将流程图数据保存到MySQL数据库")

    # 打印解析结果
    print(f"图名称: {graph.name}")
    print(f"描述: {graph.description}")
    print(f"节点数量: {len(graph.nodes)}")
    print(f"边数量: {len(graph.edges)}")

    print("\n节点列表:")
    for node in graph.nodes:
        print(f"- {node.id}: {node.name} ({node.type})")

    print("\n边列表:")
    for edge in graph.edges:
        print(f"- [{edge.from_node}].{edge.from_port} -> [{edge.to_node}].{edge.to_port}")
