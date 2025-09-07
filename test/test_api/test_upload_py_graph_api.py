import requests

from test.test_api import *


def main():
    with open("./test/test_api/resources/result_graph.py", "r", encoding="utf-8") as f:
        files = {
            "file": ("result_graph.py", f)
        }
        try:
            response = requests.post(f"{BASE_URL}/graphs/upload_py_graph/", files=files)
            print("状态码: ", response.status_code)
            print("响应内容: ", response.json())
        except Exception as e:
            print("请求失败: ", str(e))



if __name__ == '__main__':
    main()
