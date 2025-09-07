from pathlib import Path
from typing import Union

from data_flow import CACHE_DIR_PATH


def delete_cache_files(*filepaths: Union[str, Path], cache_dir: Union[str, Path] = CACHE_DIR_PATH) -> None:
    """
    删除指定的缓存文件，如果文件不存在于当前路径，则尝试从缓存目录中查找并删除

    参数:
        *filepaths: 要删除的文件路径，可以是字符串或Path对象
        cache_dir: 缓存目录路径，默认为CACHE_DIR_PATH

    异常:
        FileNotFoundError: 当文件在两个位置都不存在时抛出
    """
    cache_dir_path = Path(cache_dir)

    def remove_file(path: Path) -> bool:
        """尝试删除文件，成功返回True，失败返回False"""
        if path.exists():
            path.unlink()
            return True
        return False

    for filepath in filepaths:
        path = Path(filepath)

        # 先尝试直接删除
        if remove_file(path):
            continue

        # 再尝试从缓存目录中删除
        cache_path = cache_dir_path / path
        if remove_file(cache_path):
            continue

        # 两个位置都找不到文件
        raise FileNotFoundError(
            f"文件 '{filepath}' 不存在，已尝试以下位置:\n"
            f"1. {path.absolute()}\n"
            f"2. {cache_path.absolute()}"
        )
