from data_flow import *
from data_flow.node import Node
from data_flow.port import Port
from data_flow.edge import Edge
from data_flow.graph import Graph
from data_flow.node_config import NodeConfig
from data_flow.graph_builder import GraphBuilder
from data_flow.graph_executor import GraphExecutor
from data_flow.execution_context import ExecutionContext
from data_flow.enum_data import BuiltinNodeType, DataType

from newrcc.CConsole import colorfulText
from newrcc.CColor import TextColor

id8 = GraphBuilder(name="数据处理流程图", description="数据处理流程图描述")
id1 = id8.port("input", "输入数据", "ANY")
id2 = id8.port("passed", "通过数据", "ANY")
id6 = id8.node("id5", "filter", [id1], [id2], FilterNodeConfig(
        filter_handler=lambda datas, **kwargs: [
            item
            for port_data in datas.values()
            for item in port_data
            if item['value'] > 5
        ],
    ), "数据过滤节点", False, False)
id0 = id8.port("output", "输出数据", "ANY")
id4 = id8.node("id3", "input", [], [id0], None, "数据输入节点", False, False)
id8.connect(id4, id0, id6, id1)
id8.build()
