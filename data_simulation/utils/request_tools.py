from urllib.parse import urlencode, urlparse, urlunparse
from typing import Any, Dict, List

import requests


def request_and_get_result(url):
    result = requests.get(url)
    result.encoding = 'utf-8'
    return result.text


def join_url(base_url: str, *url_parts: str, **url_params: Any):
    """
    拼接基础URL、路径部分和查询参数，生成完整的URL

    参数:
        base_url: 基础URL
        *url_parts: 路径部分，可以是多个字符串
        **url_params: 查询参数，键值对形式

    返回:
        拼接后的完整URL

    异常:
        ValueError: 当路径部分包含多个带查询参数的片段时抛出
    """
    # 解析基础URL，处理可能已存在的路径和参数
    parsed_base = urlparse(base_url)

    # 收集所有路径部分
    path_segments: List[str] = []

    # 处理已存在的路径
    if parsed_base.path:
        path_segments.extend(parsed_base.path.strip('/').split('/'))

    # 处理额外的路径部分
    has_query_in_part = False
    extra_params: Dict[str, Any] = {}

    for part in url_parts:
        part_str = str(part).strip()
        if not part_str:
            continue

        # 检查是否包含查询参数
        if '?' in part_str:
            if has_query_in_part:
                raise ValueError("路径部分只能有一个片段包含查询参数")

            path_part, query_part = part_str.split('?', 1)
            # 解析路径部分
            if path_part:
                path_segments.extend(path_part.strip('/').split('/'))

            # 解析查询参数部分
            query_params_ = query_part.split('&')
            for param in query_params_:
                if '=' in param:
                    key, value = param.split('=', 1)
                    extra_params[key] = value
                elif param:  # 处理只有键没有值的参数
                    extra_params[param] = ''

            has_query_in_part = True
        else:
            # 纯路径部分
            path_segments.extend(part_str.strip('/').split('/'))

    # 组合所有路径部分
    full_path = '/' + '/'.join(filter(None, path_segments)) if path_segments else ''

    # 组合所有查询参数
    query_params: Dict[str, Any] = {}
    # 基础URL中的参数
    if parsed_base.query:
        base_params = parsed_base.query.split('&')
        for param in base_params:
            if '=' in param:
                key, value = param.split('=', 1)
                query_params[key] = value
            elif param:
                query_params[param] = ''

    # 添加路径部分中的参数
    query_params.update(extra_params)
    # 添加关键字参数中的参数
    query_params.update({key: value if value else '' for key, value in url_params.items()})

    # 构建完整查询字符串
    query_string = urlencode(query_params) if query_params else ''

    # 组合成完整URL
    return urlunparse((
        parsed_base.scheme,
        parsed_base.netloc,
        full_path,
        parsed_base.params,
        query_string,
        parsed_base.fragment
    ))



if __name__ == '__main__':
    print(join_url('https://www.baidu.com/', 'index.html', 'list.html', 'detail.html', id=1, name='test'))
