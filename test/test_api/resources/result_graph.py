from typing import List

from data_flow.domain.executors.builtin_executors import *
from data_flow.domain.node_executor_factory import NodeExecutorFactory
from data_flow.domain.graph_builder import GraphBuilder
from data_flow.domain.graph import Graph

ResultNodeConfig = NodeExecutorFactory.get_module_member("result", "ResultNodeConfig")


def output_data_processor(port_datas, **kwargs):
    print(f"[{', '.join([str(data) for datas in port_datas.values() for data in datas])}]")
    return port_datas


def build_graphs() -> List[Graph]:
    id10 = GraphBuilder(name="数据过滤流程图", description="数据过滤流程图")
    id0 = id10.port("input_data", "输入数据端口", "ANY")
    result_port = id10.port("result_data", "结果端口", "ANY")
    id5 = id10.node("id4", "filter", [id0], [result_port], FilterNodeConfig(
        filter_handler=lambda port_datas, **kwargs: [
            data
            for datas in port_datas.values()
            for data in datas
            if data % 2 == 0
        ]
    ), "过滤数据节点", False, False)
    id1 = id10.port("filter_data", "过滤数据端口", "ANY")
    id7 = id10.node("id6", "output", [id1], [], OutputNodeConfig(
        data_processor=output_data_processor
    ), "输出数据节点", False, True)
    id0 = id10.port("input_data", "输入数据端口", "ANY")
    id3 = id10.node("id2", "input", [], [id0], InputNodeConfig(
        data_provider=lambda **kwargs: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    ), "输入数据节点", True, False)
    result_node = id10.node("result_node", "result", [result_port], [id1], ResultNodeConfig(
        result_handler=lambda port_datas, **kwargs: port_datas
    ), "结果节点")
    id10.connect(result_node, id1, id7, id1)
    id10.connect(id3, id0, id5, id0)
    id10.connect(id5, result_port, result_node, result_port)
    id10 = id10.build()
    return [id10]
