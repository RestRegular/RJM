from pathlib import Path
from typing import Union

from data_flow import CACHE_DIR_PATH


def delete_cache_files(*filepaths: Union[str, Path], cache_dir: Union[str, Path] = CACHE_DIR_PATH) -> None:
    """
    ɾ��ָ���Ļ����ļ�������ļ��������ڵ�ǰ·�������Դӻ���Ŀ¼�в��Ҳ�ɾ��

    ����:
        *filepaths: Ҫɾ�����ļ�·�����������ַ�����Path����
        cache_dir: ����Ŀ¼·����Ĭ��ΪCACHE_DIR_PATH

    �쳣:
        FileNotFoundError: ���ļ�������λ�ö�������ʱ�׳�
    """
    cache_dir_path = Path(cache_dir)

    def remove_file(path: Path) -> bool:
        """����ɾ���ļ����ɹ�����True��ʧ�ܷ���False"""
        if path.exists():
            path.unlink()
            return True
        return False

    for filepath in filepaths:
        path = Path(filepath)

        # �ȳ���ֱ��ɾ��
        if remove_file(path):
            continue

        # �ٳ��Դӻ���Ŀ¼��ɾ��
        cache_path = cache_dir_path / path
        if remove_file(cache_path):
            continue

        # ����λ�ö��Ҳ����ļ�
        raise FileNotFoundError(
            f"�ļ� '{filepath}' �����ڣ��ѳ�������λ��:\n"
            f"1. {path.absolute()}\n"
            f"2. {cache_path.absolute()}"
        )
