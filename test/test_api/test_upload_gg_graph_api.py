import requests

from test.test_api import *


def main():
    with open("./test/test_api/resources/new_graph.gg", "r", encoding="utf-8") as f:
        files = {
            "file": ("new_graph.gg", f)
        }
        try:
            response = requests.post(f"{BASE_URL}/graphs/upload_gg_graph/", files=files)
            print("状态码: ", response.status_code)
            print("响应内容: ", response.json())
        except Exception as e:
            print("请求失败: ", str(e))



if __name__ == '__main__':
    main()
