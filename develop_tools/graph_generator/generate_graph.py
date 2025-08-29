import os.path

from jinja2 import Environment, FileSystemLoader
import json
from typing import Dict, Any


def generate_flow_graph(output_path: str, context: Dict[str, Any]):
    """
    使用Jinja2模板生成流转图代码

    Args:
        output_path: 输出文件路径
        context: 模板渲染上下文
    """
    # 加载模板
    env = Environment(loader=FileSystemLoader(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "resource"))))
    template = env.get_template("graph_template.j2")

    # 渲染模板
    rendered_code = template.render(**context)

    # 写入输出文件
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered_code)


if __name__ == "__main__":
    # 示例上下文（可根据实际需求修改）
    example_context = {
        "graph_name": "图数据导出流程",
        "graph_description": "将流程图数据（节点、边、配置）保存到MySQL数据库",
        "log_level": "DEBUG",
        "ports": [
            {"id": "graph_raw_data", "name": "图数据", "data_type": "LIST", "required": True},
            {"id": "graph_base_info", "name": "图基本信息", "data_type": "LIST"},
            {"id": "graph_node_data", "name": "图节点数据", "data_type": "LIST"},
            {"id": "graph_node_info", "name": "图节点信息", "data_type": "LIST"},
            {"id": "graph_edge_info", "name": "图边信息", "data_type": "LIST"},
            {"id": "graph_node_config_info", "name": "图节点配置信息", "data_type": "LIST"}
        ],
        "nodes": [
            {
                "id": "input_node",
                "name": "图数据输入",
                "type": "input",
                "outputs": ["graph_raw_data"],
                "description": "接收待导出的原始图数据",
                "is_start": True
            },
            {
                "id": "extract_base_info",
                "name": "图基本数据提取",
                "type": "mapper",
                "inputs": ["graph_raw_data"],
                "outputs": ["graph_base_info"],
                "description": "提取图的基础属性",
                "config": "handler=lambda port_datas, **kwargs: [\n"
                          "    {\n"
                          "        \"id\": graph.id,\n"
                          "        \"name\": graph.name,\n"
                          "        \"description\": graph.description,\n"
                          "        \"status\": str(graph.status)\n"
                          "    }\n"
                          "    for port_data in port_datas.values()\n"
                          "    for graph in port_data\n"
                          "]"
            },
            {
                "id": "db_write_node",
                "name": "图数据入库",
                "type": "db_write",
                "inputs": ["graph_base_info", "graph_node_info", "graph_node_config_info", "graph_edge_info"],
                "description": "将数据写入数据库",
                "is_end": True,
                "config": "db_config=DBConnectionConfig(\n"
                          "    password=\"197346285\",\n"
                          "    dbname=\"data_flow\",\n"
                          "    user=\"root\"\n"
                          "),\n"
                          "port_table_mapping={\n"
                          "    graph_base_info.id: \"graph\",\n"
                          "    graph_node_info.id: \"graph_node\",\n"
                          "    graph_node_config_info.id: \"graph_node_config\",\n"
                          "    graph_edge_info.id: \"graph_edge\"\n"
                          "}"
            }
        ],
        "edges": [
            {
                "source_node": "input_node",
                "source_port": "graph_raw_data",
                "target_node": "extract_base_info",
                "target_port": "graph_raw_data"
            }
        ]
    }

    # 生成代码
    generate_flow_graph(
        output_path="generated_export_graph.py",
        context=example_context
    )
    print("流转图代码生成完成！")