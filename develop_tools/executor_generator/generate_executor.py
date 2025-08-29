import datetime

from jinja2 import Environment, FileSystemLoader
import os
from typing import Dict, Any
import ast


def check_syntax(code: str):
    """
    检查代码的语法是否正确
    :param code: 代码
    :return: 是否正确
    """
    ast.parse(code)


def generate_executor(
        output_dir_path: str,
        node_type_name: str,
        node_type: str,
        executor_class_name: str,
        config_class_name: str,
        handler_attr: str,
        default_handler: str,
        author: str,
        executor_description: str,
        need_input_check: bool = True,
        need_handler_check: bool = True,
        need_input_port_check: bool = True,
) -> None:
    """
    生成Executor模板代码

    :param output_dir_path: 输出模板代码文件的路径
    :param node_type_name: 节点类型名称（用于日志和错误信息）
    :param node_type: 节点类型枚举值
    :param executor_class_name: 执行器类名
    :param config_class_name: 配置类名
    :param handler_attr: 处理器属性名（如 filter_handler、map_handler）
    :param default_handler: 默认处理器逻辑（如 lambda port_datas, **kwargs: port_datas）
    :param author: 开发者标志
    :param executor_description: 执行器描述文档
    :param need_input_check: 是否需要输入数据检查
    :param need_handler_check: 是否需要处理器配置检查
    :param need_input_port_check: 是否需要输入端口检查
    """
    # 初始化Jinja2环境
    env = Environment(loader=FileSystemLoader(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "resource"))))
    template = env.get_template("executor_template.j2")

    output_path = os.path.join(output_dir_path, f"{node_type.lower()}_executor.py")
    output_path = os.path.abspath(output_path)

    try:
        # 检查 default_handler 是否符合语法
        check_syntax(default_handler)
    except Exception as e:
        raise ValueError(f"默认处理器代码包含语法错误：{e}") from e

    # 渲染模板
    rendered_code = template.render(
        node_type=node_type,
        node_type_name=node_type_name,
        builtin_node_type=node_type,
        executor_class_name=executor_class_name,
        config_class_name=config_class_name,
        handler_attr=handler_attr,
        default_handler=default_handler,
        executor_description=executor_description,
        author=author,
        date=datetime.datetime.now(),
        need_input_check=need_input_check,
        need_handler_check=need_handler_check,
        need_input_port_check=need_input_port_check
    )

    # 写入文件
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered_code)
    print(f"Executor代码已生成: {output_path}")


# 使用示例
if __name__ == "__main__":
    # 生成一个转换节点示例（类似Mapper）
    generate_executor(
        output_dir_path="./",
        node_type_name="转换",
        node_type="TRANSFORM",
        executor_class_name="TransformNodeExecutor",
        config_class_name="TransformNodeConfig",
        handler_attr="transform_handler",
        default_handler="lambda port_datas, **kwargs: port_datas",
        executor_description="转换节点执行器，负责数据格式转换",
        author="RestRegular",
        need_input_check=True,
        need_handler_check=True,
        need_input_port_check=True,

    )
