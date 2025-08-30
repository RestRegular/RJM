import os
import datetime
import subprocess
from typing import List

from data_flow.graph import Graph
from api.endpoints.service import *
from api.endpoints.service.dynamic_import_graph import dynamic_import_graph

GG_EXECUTOR_PATH = r"D:\repositories\Resume-JobMatcher\develop_tools\graph_generator\gg_compiler\cpp_src\cmake-build-debug\gg.exe"


def generate_new_gg_file_name():
    return f"{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_{get_cache_id()}"


def delete_cache_files(*filepaths: str):
    for filepath in filepaths:
        if os.path.exists(filepath):
            os.remove(filepath)


def process_gg_parse(content) -> List[Graph]:
    # 保存 gg 内容到缓存文件
    gg_file = generate_new_gg_file_name()
    gg_file_name = f"{gg_file}.gg"
    gg_save_path = os.path.join(os.path.dirname(__file__), "cache", gg_file_name)
    with open(gg_save_path, "w", encoding="utf-8") as f:
        f.write(content)
    archive_file_name = f"{gg_file}.py"
    archive_save_path = os.path.join(os.path.dirname(__file__), "cache", archive_file_name)
    command = [GG_EXECUTOR_PATH, "--t", gg_save_path, "--a", archive_save_path, "--ct", "B"]

    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        raise Exception(f"命令执行错误: {e.stderr}") from e

    graphs = dynamic_import_graph(gg_file)

    delete_cache_files(gg_save_path, archive_save_path)
    return graphs
