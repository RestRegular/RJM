import os
import datetime
import subprocess

from data_flow import CACHE_DIR_PATH
from data_flow.domain.graph import Graph
from data_flow.api.endpoints.service import *
from data_flow.api.endpoints.service.graph_manager import GraphManager
from data_flow.utils.file_manager import delete_cache_files

GG_EXECUTOR_PATH = "/Resume_Rag_Project/data_flow/develop_tools/graph_generator/gg_compiler/cpp_src/cmake-build-debug/GG"


def generate_new_gg_file_name():
    return f"{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}_{get_cache_id()}"


def process_gg_parse(content) -> Graph:
    # 保存 gg 内容到缓存文件
    os.makedirs(CACHE_DIR_PATH, exist_ok=True)
    gg_file = generate_new_gg_file_name()
    gg_file_name = f"{gg_file}.gg"
    gg_save_path = os.path.join(CACHE_DIR_PATH, gg_file_name)
    archive_file_name = f"{gg_file}.py"
    archive_save_path = os.path.join(CACHE_DIR_PATH, archive_file_name)
    try:
        with open(gg_save_path, "w", encoding="utf-8") as f:
            f.write(content)
        command = [GG_EXECUTOR_PATH, "--t", gg_save_path, "--a", archive_save_path, "--ct", "B"]
        subprocess.run(command, check=True)
        graph = GraphManager.dynamic_import_graph_from_cache(gg_file, archive_save_path)
    except subprocess.CalledProcessError as e:
        raise Exception(f"命令执行错误: {e.stderr}\n{e.stdout}\n{e}") from e
    finally:
        delete_cache_files(gg_save_path, archive_save_path)
    return graph
